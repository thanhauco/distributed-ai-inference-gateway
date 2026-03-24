package registry

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/resilience"
)

// Source records how a worker joined the registry.
type Source string

// Worker sources.
const (
	SourceStatic     Source = "static"     // declared in the gateway config
	SourceRegistered Source = "registered" // self-registered via the admin API
)

// Spec describes a worker to add or update.
type Spec struct {
	ID     string            `json:"id"`
	URL    string            `json:"url"`
	Kind   backend.Kind      `json:"kind"`
	Models []string          `json:"models"`
	Weight float64           `json:"weight"`
	Labels map[string]string `json:"labels,omitempty"`
}

// Worker is the gateway's live view of one model server.
type Worker struct {
	ID      string
	URL     string
	Kind    backend.Kind
	Weight  float64
	Labels  map[string]string
	Source  Source
	Adapter backend.Adapter
	Breaker *resilience.Breaker
	TTFT    *LatencyWindow

	inflight atomic.Int64

	mu            sync.RWMutex
	healthy       bool
	everProbed    bool
	probeFailures int
	models        map[string]bool
	loaded        map[string]bool // nil: every served model is warm
	running       int
	waiting       int
	gpuUtil       float64
	kvUsage       float64
	prefixHit     float64
	lastProbe     time.Time
	lastHeartbeat time.Time
	lastError     string
}

// Status is a JSON-friendly snapshot of a worker.
type Status struct {
	ID            string            `json:"id"`
	URL           string            `json:"url"`
	Kind          backend.Kind      `json:"kind"`
	Source        Source            `json:"source"`
	Healthy       bool              `json:"healthy"`
	Breaker       string            `json:"breaker"`
	Models        []string          `json:"models"`
	Loaded        []string          `json:"loaded,omitempty"`
	Weight        float64           `json:"weight"`
	Labels        map[string]string `json:"labels,omitempty"`
	InFlight      int64             `json:"in_flight"`
	Running       int               `json:"running"`
	Waiting       int               `json:"queue_depth"`
	GPUUtil       float64           `json:"gpu_util"`
	KVCacheUsage  float64           `json:"kv_cache_usage"`
	PrefixHitRate float64           `json:"prefix_cache_hit_rate"`
	TTFTp50Ms     float64           `json:"ttft_p50_ms"`
	TTFTp99Ms     float64           `json:"ttft_p99_ms"`
	LastProbe     time.Time         `json:"last_probe"`
	LastError     string            `json:"last_error,omitempty"`
}

// Acquire marks a request as in flight on the worker.
func (w *Worker) Acquire() { w.inflight.Add(1) }

// Release marks an in-flight request as finished.
func (w *Worker) Release() { w.inflight.Add(-1) }

// InFlight returns the gateway-observed in-flight count.
func (w *Worker) InFlight() int64 { return w.inflight.Load() }

// Healthy reports the last probe verdict.
func (w *Worker) Healthy() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.healthy
}

// Serves reports whether the worker advertises model.
func (w *Worker) Serves(model string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.models[model]
}

// Warm reports whether model is resident in the worker's GPU memory.
func (w *Worker) Warm(model string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.loaded == nil || w.loaded[model]
}

// Load returns the effective queue the worker is facing. The worker-reported
// running+waiting count captures traffic from every gateway replica; the local
// in-flight count reacts instantly between probes. Taking the max of the two
// avoids double counting this replica's own requests.
func (w *Worker) Load() float64 {
	w.mu.RLock()
	reported := float64(w.running + w.waiting)
	kv := w.kvUsage
	w.mu.RUnlock()
	load := max(float64(w.inflight.Load()), reported)
	if kv > 0.9 {
		// Near-full KV cache means new sequences will queue or preempt.
		load *= 1.5
	}
	return load
}

// Models returns the advertised models, sorted.
func (w *Worker) Models() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return sortedKeys(w.models)
}

// Status returns a snapshot for APIs and metrics.
func (w *Worker) Status() Status {
	q := w.TTFT.Quantiles(0.5, 0.99)
	w.mu.RLock()
	defer w.mu.RUnlock()
	s := Status{
		ID: w.ID, URL: w.URL, Kind: w.Kind, Source: w.Source, Healthy: w.healthy,
		Breaker: w.Breaker.State().String(), Models: sortedKeys(w.models), Weight: w.Weight,
		Labels: w.Labels, InFlight: w.inflight.Load(), Running: w.running, Waiting: w.waiting,
		GPUUtil: w.gpuUtil, KVCacheUsage: w.kvUsage, PrefixHitRate: w.prefixHit,
		TTFTp50Ms: float64(q[0]) / float64(time.Millisecond), TTFTp99Ms: float64(q[1]) / float64(time.Millisecond),
		LastProbe: w.lastProbe, LastError: w.lastError,
	}
	if w.loaded != nil {
		s.Loaded = sortedKeys(w.loaded)
	}
	return s
}

// ApplyProbe folds a probe result into the worker state. It returns whether
// the health verdict changed.
func (w *Worker) ApplyProbe(res backend.ProbeResult, err error, unhealthyAfter int, now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	was := w.healthy
	w.lastProbe = now
	if err != nil || !res.Healthy {
		w.probeFailures++
		if err != nil {
			w.lastError = err.Error()
		}
		// A worker that has never answered is unhealthy immediately; a known
		// worker gets unhealthyAfter strikes to ride out a single blip.
		if !w.everProbed || w.probeFailures >= unhealthyAfter {
			w.healthy = false
		}
		return was != w.healthy
	}
	w.everProbed = true
	w.probeFailures = 0
	w.lastError = ""
	w.healthy = true
	if len(res.Models) > 0 {
		w.models = toSet(res.Models)
	}
	if res.Loaded != nil {
		w.loaded = toSet(res.Loaded)
	} else {
		w.loaded = nil
	}
	w.running, w.waiting = res.Running, res.Waiting
	w.gpuUtil, w.kvUsage, w.prefixHit = res.GPUUtil, res.KVCacheUsage, res.PrefixHit
	return was != w.healthy
}

func (w *Worker) heartbeat(now time.Time, models []string) {
	w.mu.Lock()
	w.lastHeartbeat = now
	if len(models) > 0 {
		w.models = toSet(models)
	}
	w.mu.Unlock()
}

func (w *Worker) stale(now time.Time, after time.Duration) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.Source == SourceRegistered && after > 0 && now.Sub(w.lastHeartbeat) > after
}

func toSet(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
