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
	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
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

func conversation(system, user string, followups ...string) []openai.Message {
	msgs := []openai.Message{{Role: "system", Content: system}, {Role: "user", Content: user}}
	for _, f := range followups {
		msgs = append(msgs, openai.Message{Role: "assistant", Content: "ok"}, openai.Message{Role: "user", Content: f})
	}
	return msgs
}

func TestKeys(t *testing.T) {
	c1, g1 := Keys(conversation("You are a lawyer.", "Q1"), 4096)
	c2, g2 := Keys(conversation("You are a lawyer.", "Q1", "follow-up", "another"), 4096)
	c3, g3 := Keys(conversation("You are a lawyer.", "Q2"), 4096)
	c4, g4 := Keys([]openai.Message{{Role: "user", Content: "hi"}}, 4096)
	if c1 != c2 || g1 != g2 {
		t.Fatal("later turns must share keys with the first turn")
	}
	if c1 == c3 || g1 != g3 {
		t.Fatal("different conversations share group but not conv key")
	}
	if c4 != g4 {
		t.Fatal("without a system prompt group falls back to conv")
	}
	long := string(make([]byte, 10_000))
	a, _ := Keys(conversation(long+"A", "x"), 4096)
	b, _ := Keys(conversation(long+"B", "y"), 4096)
	if a != b {
		t.Fatal("keys must be bounded by maxChars")
	}
}

func TestAffinityIsStableAndSticky(t *testing.T) {
	reg, _ := fleet(t, 4, "m")
	r := New(reg, Config{PrefixAffinity: true, LoadFactor: 1.25})
	conv, group := Keys(conversation("shared system prompt", "hello"), 4096)

	first, _ := r.Select(Request{Model: "m", ConvKey: conv, GroupKey: group})
	if first.Reason != ReasonAffinityHash {
		t.Fatalf("reason %s", first.Reason)
	}
	for range 20 {
		d, _ := r.Select(Request{Model: "m", ConvKey: conv, GroupKey: group})
		if d.Worker != first.Worker || d.Reason != ReasonAffinitySticky {
			t.Fatalf("not sticky: %s %s", d.Worker.ID, d.Reason)
		}
	}
	// A new conversation with the same system prompt goes to the same owner.
	conv2, group2 := Keys(conversation("shared system prompt", "different question"), 4096)
	d, _ := r.Select(Request{Model: "m", ConvKey: conv2, GroupKey: group2})
	if d.Worker != first.Worker {
		t.Fatal("shared system prompt should co-locate")
	}
}

func TestAffinitySpillsUnderLoadInHashOrder(t *testing.T) {
	reg, ws := fleet(t, 4, "m")
	r := New(reg, Config{PrefixAffinity: true, LoadFactor: 1.25})
	_, group := Keys(conversation("hot system prompt", "q"), 4096)
	order := rank(group, ws)
	for range 20 {
		order[0].Acquire()
	}
	d, _ := r.Select(Request{Model: "m", GroupKey: group})
	if d.Worker != order[1] || d.Reason != ReasonAffinitySpill {
		t.Fatalf("expected spill to %s, got %s (%s)", order[1].ID, d.Worker.ID, d.Reason)
	}
}

func TestBoundedLoadBalancesHotPrefix(t *testing.T) {
	reg, ws := fleet(t, 4, "m")
	r := New(reg, Config{PrefixAffinity: true, LoadFactor: 1.25})
	_, group := Keys(conversation("everyone uses this prompt", "q"), 4096)
	// 40 concurrent requests sharing one prefix: no worker may exceed
	// ceil(1.25 × average) even though they all hash to the same owner.
	for i := range 40 {
		conv := SessionKey(fmt.Sprint(i))
		d, err := r.Select(Request{Model: "m", ConvKey: conv, GroupKey: group})
		if err != nil {
			t.Fatal(err)
		}
		d.Worker.Acquire()
	}
	for _, w := range ws {
		if w.InFlight() > 13 {
			t.Fatalf("%s holds %d of 40", w.ID, w.InFlight())
		}
	}
}

func TestRendezvousMinimalDisruption(t *testing.T) {
	_, ws := fleet(t, 5, "m")
	moved := 0
	for k := uint64(1); k <= 2000; k++ {
		before := rank(k, ws)[0]
		after := rank(k, ws[:4])[0]
		if before.ID != "w4" && before != after {
			moved++
		}
	}
	if moved != 0 {
		t.Fatalf("%d keys moved that were not owned by the removed worker", moved)
	}
}

func TestForgetAndLRU(t *testing.T) {
	tbl := newLRUTable(2)
	tbl.put(1, "a")
	tbl.put(2, "b")
	tbl.get(1)
	tbl.put(3, "c") // evicts 2
	if _, ok := tbl.get(2); ok || tbl.len() != 2 {
		t.Fatal("lru eviction wrong")
	}
	tbl.dropValue("a")
	if _, ok := tbl.get(1); ok {
		t.Fatal("dropValue failed")
	}
}
