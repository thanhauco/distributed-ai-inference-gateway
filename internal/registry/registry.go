// Package registry is the gateway's in-memory view of the worker fleet:
// health, load, loaded models and latency, refreshed by health probes and
// worker heartbeats.
package registry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/resilience"
)

// ErrUnknownWorker is returned for an unknown worker ID.
var ErrUnknownWorker = errors.New("unknown worker")

// Registry holds every known worker.
type Registry struct {
	client     *http.Client
	breakerCfg resilience.BreakerConfig
	logger     *slog.Logger
	now        func() time.Time
	onBreaker  func(w *Worker, from, to resilience.State)
	onRemove   func(id string)
	mu         sync.RWMutex
	workers    map[string]*Worker
	generation uint64
}

// Options configures a Registry.
type Options struct {
	HTTPClient *http.Client
	Breaker    resilience.BreakerConfig
	Logger     *slog.Logger
	// OnBreakerChange is called on every worker breaker transition.
	OnBreakerChange func(w *Worker, from, to resilience.State)
	// OnRemove is called after a worker is removed.
	OnRemove func(id string)
}

// New returns an empty registry.
func New(opts Options) *Registry {
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Transport: http.DefaultTransport}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Registry{
		client: opts.HTTPClient, breakerCfg: opts.Breaker, logger: opts.Logger,
		onBreaker: opts.OnBreakerChange, onRemove: opts.OnRemove,
		now: time.Now, workers: map[string]*Worker{},
	}
}

// Upsert adds a worker or updates the mutable fields of an existing one.
// Updating keeps the breaker, latency history and in-flight counters.
func (r *Registry) Upsert(spec Spec, source Source) (*Worker, error) {
	if spec.ID == "" {
		return nil, errors.New("worker id is required")
	}
	u, err := url.Parse(spec.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("worker %s: invalid url %q", spec.ID, spec.URL)
	}
	if spec.Kind == "" {
		spec.Kind = backend.KindVLLM
	}
	adapter, err := backend.New(spec.Kind, r.client)
	if err != nil {
		return nil, err
	}
	if spec.Weight <= 0 {
		spec.Weight = 1
	}
	now := r.now()

	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.workers[spec.ID]; ok {
		w.mu.Lock()
		w.URL, w.Kind, w.Adapter, w.Weight, w.Labels = spec.URL, spec.Kind, adapter, spec.Weight, spec.Labels
		if len(spec.Models) > 0 {
			w.models = toSet(spec.Models)
		}
		w.lastHeartbeat = now
		if source == SourceStatic {
			w.Source = SourceStatic
		}
		w.mu.Unlock()
		r.generation++
		return w, nil
	}
	w := &Worker{
		ID: spec.ID, URL: spec.URL, Kind: spec.Kind, Weight: spec.Weight, Labels: spec.Labels,
		Source: source, Adapter: adapter, Breaker: resilience.NewBreaker(r.breakerCfg),
		TTFT: NewLatencyWindow(512), models: toSet(spec.Models), lastHeartbeat: now,
	}
	if r.onBreaker != nil {
		w.Breaker.OnStateChange(func(from, to resilience.State) { r.onBreaker(w, from, to) })
	}
	r.workers[spec.ID] = w
	r.generation++
	r.logger.Info("worker added", "worker", w.ID, "url", w.URL, "kind", w.Kind, "source", source, "models", spec.Models)
	return w, nil
}

// Remove deletes a worker.
func (r *Registry) Remove(id string) error {
	r.mu.Lock()
	if _, ok := r.workers[id]; !ok {
		r.mu.Unlock()
		return ErrUnknownWorker
	}
	delete(r.workers, id)
	r.generation++
	r.mu.Unlock()
	r.logger.Info("worker removed", "worker", id)
	if r.onRemove != nil {
		r.onRemove(id)
	}
	return nil
}

// Heartbeat refreshes a self-registered worker's liveness.
func (r *Registry) Heartbeat(id string, models []string) error {
	w := r.Get(id)
	if w == nil {
		return ErrUnknownWorker
	}
	w.heartbeat(r.now(), models)
	return nil
}

// Get returns a worker by ID or nil.
func (r *Registry) Get(id string) *Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.workers[id]
}

// List returns all workers sorted by ID.
func (r *Registry) List() []*Worker {
	r.mu.RLock()
	out := make([]*Worker, 0, len(r.workers))
	for _, w := range r.workers {
		out = append(out, w)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Candidates returns healthy workers serving model.
func (r *Registry) Candidates(model string) []*Worker {
	all := r.List()
	out := all[:0]
	for _, w := range all {
		if w.Healthy() && w.Serves(model) {
			out = append(out, w)
		}
	}
	return out
}

// Models returns the union of models served by healthy workers.
func (r *Registry) Models() []string {
	set := map[string]bool{}
	for _, w := range r.List() {
		if !w.Healthy() {
			continue
		}
		for _, m := range w.Models() {
			set[m] = true
		}
	}
	return sortedKeys(set)
}

// Generation increases whenever the worker set changes; routers use it to
// invalidate cached rankings.
func (r *Registry) Generation() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.generation
}

// ProbeAll probes every worker once, concurrently.
func (r *Registry) ProbeAll(ctx context.Context, timeout time.Duration, unhealthyAfter int, hook func(*Worker, backend.ProbeResult, error)) {
	var wg sync.WaitGroup
	for _, w := range r.List() {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, timeout)
			res, err := w.Adapter.Probe(pctx, w.URL)
			cancel()
			if ctx.Err() != nil {
				return
			}
			if changed := w.ApplyProbe(res, err, unhealthyAfter, r.now()); changed {
				if w.Healthy() {
					r.logger.Info("worker healthy", "worker", w.ID, "models", w.Models())
				} else {
					r.logger.Warn("worker unhealthy", "worker", w.ID, "error", err)
				}
			}
			if hook != nil {
				hook(w, res, err)
			}
		}(w)
	}
	wg.Wait()
}

// EvictStale removes self-registered workers whose heartbeat is older than
// after.
func (r *Registry) EvictStale(after time.Duration) []string {
	now := r.now()
	var stale []string
	for _, w := range r.List() {
		if w.stale(now, after) {
			stale = append(stale, w.ID)
		}
	}
	for _, id := range stale {
		_ = r.Remove(id)
	}
	return stale
}
