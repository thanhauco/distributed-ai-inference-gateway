package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every gateway metric. All metrics use the gateway_ prefix.
type Metrics struct {
	Registry *prometheus.Registry

	Requests        *prometheus.CounterVec   // model, status, cache
	RequestDuration *prometheus.HistogramVec // model, stream
	TTFT            *prometheus.HistogramVec // model, worker
	Tokens          *prometheus.CounterVec   // model, type=prompt|completion
	DecodeRate      *prometheus.HistogramVec // model, worker: completion tokens/s per request
	InFlight        prometheus.Gauge

	CacheLookups *prometheus.CounterVec // result
	RateLimited  *prometheus.CounterVec // reason
	Coalesced    prometheus.Counter

	BatchSize   *prometheus.HistogramVec // model
	BatchWait   prometheus.Histogram
	BatchTarget *prometheus.GaugeVec // model

	RouteDecisions *prometheus.CounterVec // model, reason
	Retries        *prometheus.CounterVec // model, cause
	UpstreamErrors *prometheus.CounterVec // worker, cause

	WorkerHealthy      *prometheus.GaugeVec   // worker
	WorkerInFlight     *prometheus.GaugeVec   // worker
	WorkerQueueDepth   *prometheus.GaugeVec   // worker
	WorkerRunning      *prometheus.GaugeVec   // worker
	WorkerGPUUtil      *prometheus.GaugeVec   // worker
	WorkerKVCache      *prometheus.GaugeVec   // worker
	WorkerPrefixHit    *prometheus.GaugeVec   // worker
	BreakerState       *prometheus.GaugeVec   // worker (0 closed, 1 half-open, 2 open)
	BreakerTransitions *prometheus.CounterVec // worker, to
}

var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 60, 120}

// NewMetrics registers all metrics on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	m := &Metrics{Registry: reg}

	m.Requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_requests_total", Help: "Chat completion requests by outcome."}, []string{"model", "status", "cache"})
	m.RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_request_duration_seconds", Help: "End-to-end request latency.", Buckets: latencyBuckets}, []string{"model", "stream"})
	m.TTFT = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_time_to_first_token_seconds", Help: "Time from request arrival to the first streamed token.", Buckets: latencyBuckets}, []string{"model", "worker"})
	m.Tokens = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_tokens_total", Help: "Tokens processed."}, []string{"model", "type"})
	m.DecodeRate = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_decode_tokens_per_second", Help: "Per-request completion throughput after the first token.", Buckets: []float64{5, 10, 20, 35, 50, 75, 100, 150, 250, 500, 1000}}, []string{"model", "worker"})
	m.InFlight = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_in_flight_requests", Help: "Requests currently being served."})

	m.CacheLookups = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_cache_lookups_total", Help: "Response cache lookups by result."}, []string{"result"})
	m.RateLimited = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_rate_limited_total", Help: "Requests rejected with 429."}, []string{"reason"})
	m.Coalesced = prometheus.NewCounter(prometheus.CounterOpts{Name: "gateway_coalesced_requests_total", Help: "Requests served by joining an identical in-flight generation."})

	m.BatchSize = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_batch_size", Help: "Dispatched micro-batch sizes.", Buckets: []float64{1, 2, 4, 8, 16, 32, 64}}, []string{"model"})
	m.BatchWait = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "gateway_batch_wait_seconds", Help: "Time the oldest request waited for its batch.", Buckets: []float64{.0005, .001, .0025, .005, .01, .015, .02, .03, .05}})
	m.BatchTarget = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gateway_batch_target_size", Help: "Adaptive power-of-two batch target."}, []string{"model"})

	m.RouteDecisions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_route_decisions_total", Help: "Routing decisions by reason."}, []string{"model", "reason"})
	m.Retries = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_retries_total", Help: "Upstream retries."}, []string{"model", "cause"})
	m.UpstreamErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_upstream_errors_total", Help: "Errors talking to workers."}, []string{"worker", "cause"})

	gauge := func(name, help string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, []string{"worker"})
	}
	m.WorkerHealthy = gauge("gateway_worker_healthy", "1 if the last health probe succeeded.")
	m.WorkerInFlight = gauge("gateway_worker_in_flight", "Requests this gateway has in flight on the worker.")
	m.WorkerQueueDepth = gauge("gateway_worker_queue_depth", "Requests waiting in the worker's scheduler queue.")
	m.WorkerRunning = gauge("gateway_worker_running", "Sequences the worker is decoding.")
	m.WorkerGPUUtil = gauge("gateway_worker_gpu_utilization", "Worker GPU utilization (0-1).")
	m.WorkerKVCache = gauge("gateway_worker_kv_cache_usage", "Fraction of the worker's KV cache in use (0-1).")
	m.WorkerPrefixHit = gauge("gateway_worker_prefix_cache_hit_rate", "Worker-reported prefix cache hit rate (0-1).")
	m.BreakerState = gauge("gateway_breaker_state", "Circuit breaker state: 0 closed, 1 half-open, 2 open.")
	m.BreakerTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_breaker_transitions_total", Help: "Circuit breaker state transitions."}, []string{"worker", "to"})

	for _, c := range []prometheus.Collector{
		m.Requests, m.RequestDuration, m.TTFT, m.Tokens, m.DecodeRate, m.InFlight,
		m.CacheLookups, m.RateLimited, m.Coalesced, m.BatchSize, m.BatchWait, m.BatchTarget,
		m.RouteDecisions, m.Retries, m.UpstreamErrors,
		m.WorkerHealthy, m.WorkerInFlight, m.WorkerQueueDepth, m.WorkerRunning, m.WorkerGPUUtil,
		m.WorkerKVCache, m.WorkerPrefixHit, m.BreakerState, m.BreakerTransitions,
	} {
		f(c)
	}
	return m
}

// Handler serves the registry in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}

// DeleteWorker drops every per-worker series for a removed worker.
func (m *Metrics) DeleteWorker(id string) {
	for _, g := range []*prometheus.GaugeVec{
		m.WorkerHealthy, m.WorkerInFlight, m.WorkerQueueDepth, m.WorkerRunning,
		m.WorkerGPUUtil, m.WorkerKVCache, m.WorkerPrefixHit, m.BreakerState,
	} {
		g.DeleteLabelValues(id)
	}
	m.TTFT.DeletePartialMatch(prometheus.Labels{"worker": id})
	m.DecodeRate.DeletePartialMatch(prometheus.Labels{"worker": id})
	m.UpstreamErrors.DeletePartialMatch(prometheus.Labels{"worker": id})
	m.BreakerTransitions.DeletePartialMatch(prometheus.Labels{"worker": id})
}
