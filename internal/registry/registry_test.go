package registry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/resilience"
	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func fakeVLLM(t *testing.T, models []string, healthy *atomic.Bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		var l openai.ModelList
		for _, m := range models {
			l.Data = append(l.Data, openai.Model{ID: m})
		}
		_ = json.NewEncoder(w).Encode(l)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "vllm:num_requests_running 2\nvllm:num_requests_waiting 3\n")
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestProbeLifecycle(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	srv := fakeVLLM(t, []string{"llama3-8b", "mistral-7b"}, &healthy)

	reg := New(Options{Logger: quiet()})
	if _, err := reg.Upsert(Spec{ID: "w1", URL: srv.URL, Kind: backend.KindVLLM}, SourceStatic); err != nil {
		t.Fatal(err)
	}
	if len(reg.Candidates("llama3-8b")) != 0 {
		t.Fatal("unprobed worker must not be a candidate")
	}
	reg.ProbeAll(context.Background(), time.Second, 2, nil)
	cands := reg.Candidates("mistral-7b")
	if len(cands) != 1 || cands[0].Load() != 5 {
		t.Fatalf("candidates %v", cands)
	}
	if got := reg.Models(); len(got) != 2 {
		t.Fatalf("models %v", got)
	}

	healthy.Store(false)
	reg.ProbeAll(context.Background(), time.Second, 2, nil)
	if !reg.Get("w1").Healthy() {
		t.Fatal("one failed probe should not flip a known-good worker")
	}
	reg.ProbeAll(context.Background(), time.Second, 2, nil)
	if reg.Get("w1").Healthy() {
		t.Fatal("two failed probes should mark unhealthy")
	}
	healthy.Store(true)
	reg.ProbeAll(context.Background(), time.Second, 2, nil)
	if !reg.Get("w1").Healthy() {
		t.Fatal("recovery should be immediate")
	}
}

func TestUpsertKeepsState(t *testing.T) {
	reg := New(Options{Logger: quiet()})
	w, _ := reg.Upsert(Spec{ID: "w", URL: "http://a:1", Models: []string{"m"}}, SourceRegistered)
	w.Acquire()
	w2, _ := reg.Upsert(Spec{ID: "w", URL: "http://b:1", Weight: 2}, SourceRegistered)
	if w != w2 || w2.InFlight() != 1 || w2.URL != "http://b:1" || w2.Weight != 2 || !w2.Serves("m") {
		t.Fatalf("upsert lost state: %+v", w2.Status())
	}
	if _, err := reg.Upsert(Spec{ID: "bad", URL: "ftp://x"}, SourceStatic); err == nil {
		t.Fatal("expected invalid url")
	}
	if _, err := reg.Upsert(Spec{ID: "bad", URL: "http://x", Kind: "tgi"}, SourceStatic); err == nil {
		t.Fatal("expected invalid kind")
	}
}

func TestEvictStaleOnlyRegistered(t *testing.T) {
	reg := New(Options{Logger: quiet()})
	now := time.Unix(1000, 0)
	reg.now = func() time.Time { return now }
	_, _ = reg.Upsert(Spec{ID: "static", URL: "http://s:1"}, SourceStatic)
	_, _ = reg.Upsert(Spec{ID: "dyn", URL: "http://d:1"}, SourceRegistered)
	removed := ""
	reg.onRemove = func(id string) { removed = id }

	now = now.Add(time.Minute)
	if got := reg.EvictStale(30 * time.Second); len(got) != 1 || got[0] != "dyn" || removed != "dyn" {
		t.Fatalf("evicted %v", got)
	}
	if reg.Get("static") == nil {
		t.Fatal("static worker must never be evicted")
	}
	if err := reg.Heartbeat("dyn", nil); !errors.Is(err, ErrUnknownWorker) {
		t.Fatal(err)
	}
}

func TestBreakerHook(t *testing.T) {
	var got string
	reg := New(Options{Logger: quiet(), Breaker: resilience.BreakerConfig{FailureThreshold: 1},
		OnBreakerChange: func(w *Worker, from, to resilience.State) { got = w.ID + ":" + to.String() }})
	w, _ := reg.Upsert(Spec{ID: "w", URL: "http://a:1"}, SourceStatic)
	w.Breaker.RecordFailure()
	if got != "w:open" {
		t.Fatalf("hook got %q", got)
	}
}

func TestLatencyWindow(t *testing.T) {
	lw := NewLatencyWindow(4)
	for _, ms := range []int{10, 20, 30, 40, 50} { // 10 is overwritten
		lw.Observe(time.Duration(ms) * time.Millisecond)
	}
	q := lw.Quantiles(0, 0.5, 1)
	if q[0] != 20*time.Millisecond || q[2] != 50*time.Millisecond {
		t.Fatalf("quantiles %v", q)
	}
	if NewLatencyWindow(3).Quantiles(0.5)[0] != 0 {
		t.Fatal("empty window")
	}
}

func TestKVPressureInflatesLoad(t *testing.T) {
	w := &Worker{TTFT: NewLatencyWindow(1), Breaker: resilience.NewBreaker(resilience.BreakerConfig{})}
	w.ApplyProbe(backend.ProbeResult{Healthy: true, Running: 4, KVCacheUsage: 0.95}, nil, 1, time.Now())
	if w.Load() != 6 {
		t.Fatalf("load %v", w.Load())
	}
}
