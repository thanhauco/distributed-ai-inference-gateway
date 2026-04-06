package router

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/registry"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/resilience"
)

func fleet(t *testing.T, n int, models ...string) (*registry.Registry, []*registry.Worker) {
	t.Helper()
	reg := registry.New(registry.Options{
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Breaker: resilience.BreakerConfig{FailureThreshold: 1, OpenDuration: time.Hour},
	})
	var ws []*registry.Worker
	for i := range n {
		w, err := reg.Upsert(registry.Spec{ID: fmt.Sprintf("w%d", i), URL: fmt.Sprintf("http://w%d:8000", i), Models: models}, registry.SourceStatic)
		if err != nil {
			t.Fatal(err)
		}
		w.ApplyProbe(backend.ProbeResult{Healthy: true, Models: models}, nil, 1, time.Now())
		ws = append(ws, w)
	}
	return reg, ws
}

func TestModelAwareAndExclusion(t *testing.T) {
	reg, ws := fleet(t, 2, "llama3-8b")
	other, _ := reg.Upsert(registry.Spec{ID: "ollama", URL: "http://o:11434", Kind: backend.KindOllama}, registry.SourceStatic)
	other.ApplyProbe(backend.ProbeResult{Healthy: true, Models: []string{"phi3"}}, nil, 1, time.Now())
	r := New(reg, Config{})

	d, err := r.Select(Request{Model: "phi3"})
	if err != nil || d.Worker.ID != "ollama" || d.Reason != ReasonSingle {
		t.Fatalf("phi3 routed to %+v %v", d, err)
	}
	if _, err := r.Select(Request{Model: "gpt-9"}); !errors.Is(err, ErrNoWorkers) {
		t.Fatalf("expected ErrNoWorkers, got %v", err)
	}
	d, _ = r.Select(Request{Model: "llama3-8b", Exclude: map[string]bool{"w0": true}})
	if d.Worker.ID != "w1" {
		t.Fatalf("exclusion ignored: %s", d.Worker.ID)
	}
	ws[1].Breaker.RecordFailure() // opens
	d, err = r.Select(Request{Model: "llama3-8b", Exclude: map[string]bool{"w0": true}})
	if !errors.Is(err, ErrNoWorkers) {
		t.Fatalf("open breaker should remove candidate, got %+v", d)
	}
}

func TestP2CPrefersLessLoaded(t *testing.T) {
	reg, ws := fleet(t, 2, "m")
	for range 10 {
		ws[0].Acquire()
	}
	r := New(reg, Config{})
	for range 50 {
		d, _ := r.Select(Request{Model: "m"})
		if d.Worker.ID != "w1" || d.Reason != ReasonP2C {
			t.Fatalf("picked %s (%s)", d.Worker.ID, d.Reason)
		}
	}
}

func TestP2CPrefersWarmWorkers(t *testing.T) {
	reg, ws := fleet(t, 2, "phi3", "gemma2")
	ws[0].ApplyProbe(backend.ProbeResult{Healthy: true, Models: []string{"phi3", "gemma2"}, Loaded: []string{"gemma2"}}, nil, 1, time.Now())
	ws[1].ApplyProbe(backend.ProbeResult{Healthy: true, Models: []string{"phi3", "gemma2"}, Loaded: []string{"phi3"}}, nil, 1, time.Now())
	ws[1].Acquire()
	ws[1].Acquire()
	r := New(reg, Config{})
	d, _ := r.Select(Request{Model: "phi3"})
	if d.Worker.ID != "w1" {
		t.Fatalf("cold worker chosen over warm one with modest load: %s", d.Worker.ID)
	}
}
