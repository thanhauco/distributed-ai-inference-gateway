// Package gateway is the request pipeline:
//
//	auth → rate limit → cache → coalesce → batch → route → attempt (timeouts,
//	retries, breakers, failover) → stream
//
// It is transport-agnostic; the HTTP layer adapts it to OpenAI-compatible
// JSON and SSE through the Sink interface.
package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/auth"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/batcher"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/cache"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/catalog"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/config"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/observability"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/ratelimit"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/registry"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/resilience"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/router"
	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// defaultCompletionBudget is charged against token limits when a request
// does not set max_tokens and the model has no configured default.
const defaultCompletionBudget = 512

// CacheMode is the client's cache directive.
type CacheMode int

// Cache modes.
const (
	CacheDefault CacheMode = iota // cache deterministic requests (temperature ≤ max_temperature)
	CacheBypass                   // never read or write the cache
	CacheForce                    // cache regardless of sampling parameters
)

// Call is one chat completion invocation.
type Call struct {
	Key       *auth.Key // nil when authentication is disabled
	Request   *openai.ChatCompletionRequest
	SessionID string
	CacheMode CacheMode
}

// Meta describes how a request was served; the HTTP layer turns it into
// response headers.
type Meta struct {
	RequestID string
	Model     string
	Worker    string
	Route     router.Reason
	Cache     cache.Kind
	Coalesced bool
	Attempts  int
	RateLimit *ratelimit.Decision
}

// Sink receives the response. Start is called exactly once before the first
// Chunk; Finish is called once after the last chunk on success.
type Sink interface {
	Start(meta Meta) error
	Chunk(c backend.Chunk) error
	Finish(usage openai.Usage) error
}

// Outcome summarises a completed request.
type Outcome struct {
	Meta         Meta
	Usage        openai.Usage
	FinishReason string
}

// Options wires the pipeline's dependencies. Limiter and Cache may be nil.
type Options struct {
	Config   *config.Config
	Registry *registry.Registry
	Router   *router.Router
	Catalog  *catalog.Catalog
	Limiter  ratelimit.Limiter
	Cache    *cache.Cache
	Metrics  *observability.Metrics
	Logger   *slog.Logger
}

// Service is the gateway pipeline.
type Service struct {
	cfg     *config.Config
	reg     *registry.Registry
	router  *router.Router
	catalog *catalog.Catalog
	limiter ratelimit.Limiter
	cache   *cache.Cache
	metrics *observability.Metrics
	logger  *slog.Logger
	retry   resilience.RetryPolicy
	tracer  trace.Tracer

	batcher *batcher.Batcher[*job]
	flights *batcher.Group[backend.Chunk]
}

// New builds the pipeline.
func New(o Options) *Service {
	s := &Service{
		cfg: o.Config, reg: o.Registry, router: o.Router, catalog: o.Catalog,
		limiter: o.Limiter, cache: o.Cache, metrics: o.Metrics, logger: o.Logger,
		retry: resilience.RetryPolicy{
			MaxAttempts: o.Config.Resilience.MaxAttempts,
			BaseDelay:   o.Config.Resilience.BackoffBase,
			MaxDelay:    o.Config.Resilience.BackoffMax,
		},
		tracer:  observability.Tracer(),
		flights: batcher.NewGroup[backend.Chunk](),
	}
	if s.logger == nil {
		s.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if bc := o.Config.Batcher; bc.Enabled {
		s.batcher = batcher.New(batcher.Config{MinWait: bc.MinWait, MaxWait: bc.MaxWait, MaxBatchSize: bc.MaxBatchSize}, s.dispatchBatch)
	}
	return s
}

// Chat serves one chat completion.
func (s *Service) Chat(ctx context.Context, call Call, sink Sink) (out *Outcome, err error) {
	start := time.Now()
	req := call.Request
	out = &Outcome{Meta: Meta{RequestID: observability.RequestIDFrom(ctx), Cache: cache.Bypass}}

	ctx, span := s.tracer.Start(ctx, "gateway.chat", trace.WithAttributes(
		attribute.String("gen_ai.request.model", req.Model),
		attribute.Bool("gateway.stream", req.Stream),
	))
	defer span.End()
	if s.cfg.Resilience.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.Resilience.RequestTimeout)
		defer cancel()
	}
	s.metrics.InFlight.Inc()
	defer s.metrics.InFlight.Dec()

	defer func() {
		status := "200"
		if err != nil {
			ge := AsError(err)
			status = strconv.Itoa(ge.Status)
			span.RecordError(err)
			span.SetStatus(codes.Error, ge.Code)
		}
		model := out.Meta.Model
		if model == "" {
			model = "unknown"
		}
		s.metrics.Requests.WithLabelValues(model, status, string(out.Meta.Cache)).Inc()
		s.metrics.RequestDuration.WithLabelValues(model, boolLabel(req.Stream)).Observe(time.Since(start).Seconds())
		span.SetAttributes(
			attribute.String("gateway.worker", out.Meta.Worker),
			attribute.String("gateway.route", string(out.Meta.Route)),
			attribute.String("gateway.cache", string(out.Meta.Cache)),
			attribute.Int("gen_ai.usage.input_tokens", out.Usage.PromptTokens),
			attribute.Int("gen_ai.usage.output_tokens", out.Usage.CompletionTokens),
		)
	}()

	// Resolve aliases and authorize the model.
	requested := req.Model
	model, _ := s.catalog.Resolve(requested)
	req.Model = model.Name
	out.Meta.Model = model.Name
	ctx = observability.WithAttrs(ctx, slog.String("model", model.Name))
	if call.Key != nil && !call.Key.AllowsModel(model.Name) && !call.Key.AllowsModel(requested) {
		return out, &Error{Status: http.StatusForbidden, Type: "permission_error", Code: "model_not_allowed", Message: "this API key may not use model " + requested}
	}
	if !s.served(model.Name) {
		return out, &Error{Status: http.StatusNotFound, Type: "invalid_request_error", Code: "model_not_found", Message: "the model `" + requested + "` does not exist"}
	}
	if req.MaxTokens == nil && model.DefaultMaxTokens > 0 {
		v := model.DefaultMaxTokens
		req.MaxTokens = &v
	}

	// Token-level rate limiting.
	promptTokens := openai.PromptTokens(req)
	reserved, limiterKey := int64(0), ""
	if s.limiter != nil {
		budget := int64(promptTokens + openai.MaxCompletion(req, defaultCompletionBudget))
		dec, key, rerr := s.reserve(ctx, call.Key, budget)
		if rerr != nil {
			return out, rerr
		}
		out.Meta.RateLimit = dec
		if dec != nil && dec.Allowed {
			reserved, limiterKey = budget, key
		}
	}
	defer func() {
		if reserved == 0 {
			return
		}
		// Reconcile the estimate with actual usage (a refund on failure).
		actual := int64(out.Usage.TotalTokens)
		if out.Meta.Cache == cache.HitExact || out.Meta.Cache == cache.HitSemantic || out.Meta.Coalesced {
			actual = 0 // served without GPU work
		}
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if aerr := s.limiter.Adjust(actx, limiterKey, actual-reserved); aerr != nil {
			s.logger.WarnContext(ctx, "rate limit adjust failed", "error", aerr)
		}
	}()

	// Response cache.
	var plan *cache.Plan
	if s.cache != nil && s.cacheable(req, call.CacheMode) {
		plan = s.cache.Plan(req)
		lctx, lspan := s.tracer.Start(ctx, "cache.lookup")
		res, lerr := s.cache.Lookup(lctx, plan)
		lspan.SetAttributes(attribute.String("gateway.cache.result", string(res.Kind)), attribute.Float64("gateway.cache.similarity", res.Similarity))
		lspan.End()
		s.metrics.CacheLookups.WithLabelValues(string(res.Kind)).Inc()
		out.Meta.Cache = res.Kind
		if lerr != nil {
			s.logger.WarnContext(ctx, "cache lookup failed; continuing uncached", "error", lerr)
			out.Meta.Cache = cache.Miss
		}
		if res.Entry != nil && (res.Kind == cache.HitExact || res.Kind == cache.HitSemantic) {
			out.Meta.Worker = "cache"
			return out, s.replay(ctx, res.Entry, promptTokens, sink, out)
		}
	}

	// Coalesce identical in-flight requests.
	var flight *batcher.Flight[backend.Chunk]
	if plan != nil && s.cfg.Batcher.Coalesce {
		f, leader := s.flights.Join(plan.Key())
		if !leader {
			s.metrics.Coalesced.Inc()
			done, ferr := s.follow(ctx, f, promptTokens, sink, out)
			if done {
				return out, ferr
			}
			// The leader failed before producing anything: generate ourselves.
		} else {
			flight = f
			defer func() {
				if flight != nil {
					flight.Finish(err)
				}
			}()
		}
	}

	col := &collector{sink: sink, meta: &out.Meta, flight: flight}
	err = s.generate(ctx, req, call, col, start)
	if err != nil {
		return out, err
	}
	out.Usage = col.usage(promptTokens)
	out.FinishReason = col.finish
	if flight != nil {
		// Release followers as soon as the generation is complete.
		flight.Publish(backend.Chunk{Usage: &out.Usage})
		flight.Finish(nil)
		flight = nil
	}
	s.metrics.Tokens.WithLabelValues(model.Name, "prompt").Add(float64(out.Usage.PromptTokens))
	s.metrics.Tokens.WithLabelValues(model.Name, "completion").Add(float64(out.Usage.CompletionTokens))
	if !col.firstAt.IsZero() && out.Usage.CompletionTokens > 1 {
		if d := time.Since(col.firstAt).Seconds(); d > 0 {
			s.metrics.DecodeRate.WithLabelValues(model.Name, out.Meta.Worker).Observe(float64(out.Usage.CompletionTokens-1) / d)
		}
	}
	if err := sink.Finish(out.Usage); err != nil {
		return out, err
	}
	if plan != nil && col.content.Len() > 0 && (col.finish == "stop" || col.finish == "length") {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if serr := s.cache.Store(sctx, plan, col.content.String(), col.finish, &out.Usage); serr != nil {
			s.logger.WarnContext(ctx, "cache store failed", "error", serr)
		}
	}
	return out, nil
}

func (s *Service) served(model string) bool {
	for _, w := range s.reg.List() {
		if w.Serves(model) {
			return true
		}
	}
	return false
}

func (s *Service) cacheable(req *openai.ChatCompletionRequest, mode CacheMode) bool {
	switch mode {
	case CacheBypass:
		return false
	case CacheForce:
		return true
	}
	return req.Temperature != nil && *req.Temperature <= s.cfg.Cache.MaxTemperature
}

func (s *Service) reserve(ctx context.Context, key *auth.Key, tokens int64) (*ratelimit.Decision, string, error) {
	defaults := s.catalog.Limits()
	limits := ratelimit.Limits{Requests: defaults.RequestsPerMin, Tokens: defaults.TokensPerMin}
	id := "anonymous"
	if key != nil {
		id = key.ID
		if key.RequestsPerMin > 0 {
			limits.Requests = key.RequestsPerMin
		}
		if key.TokensPerMin > 0 {
			limits.Tokens = key.TokensPerMin
		}
	}
	rctx, span := s.tracer.Start(ctx, "ratelimit.reserve", trace.WithAttributes(attribute.Int64("gateway.ratelimit.tokens", tokens)))
	defer span.End()
	dec, err := s.limiter.Reserve(rctx, id, tokens, limits)
	if err != nil {
		// Fail open: an unavailable limiter backend must not take inference down.
		s.logger.WarnContext(ctx, "rate limiter unavailable; failing open", "error", err)
		return nil, id, nil
	}
	span.SetAttributes(attribute.Bool("gateway.ratelimit.allowed", dec.Allowed))
	if !dec.Allowed {
		s.metrics.RateLimited.WithLabelValues(dec.Reason).Inc()
		return &dec, id, &Error{
			Status: http.StatusTooManyRequests, Type: "rate_limit_error", Code: "rate_limit_exceeded",
			Message:    "rate limit exceeded: " + dec.Reason + " per minute",
			RetryAfter: dec.RetryAfter, RateLimit: &dec,
		}
	}
	return &dec, id, nil
}

// replay streams a cached entry in small pieces so streaming clients see the
// same shape as a live response.
func (s *Service) replay(ctx context.Context, e *cache.Entry, promptTokens int, sink Sink, out *Outcome) error {
	if err := sink.Start(out.Meta); err != nil {
		return err
	}
	for _, piece := range splitForReplay(e.Content) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sink.Chunk(backend.Chunk{Content: piece}); err != nil {
			return err
		}
	}
	if err := sink.Chunk(backend.Chunk{FinishReason: e.FinishReason}); err != nil {
		return err
	}
	completion := openai.EstimateTokens(e.Content)
	if e.Usage != nil {
		completion = e.Usage.CompletionTokens
	}
	out.Usage = openai.Usage{PromptTokens: promptTokens, CompletionTokens: completion, TotalTokens: promptTokens + completion}
	out.FinishReason = e.FinishReason
	return sink.Finish(out.Usage)
}

// follow subscribes to an identical in-flight generation. done is false when
// the leader failed before producing output and the caller should generate.
func (s *Service) follow(ctx context.Context, f *batcher.Flight[backend.Chunk], promptTokens int, sink Sink, out *Outcome) (done bool, err error) {
	out.Meta.Coalesced = true
	out.Meta.Worker = "coalesced"
	started := false
	var usage *openai.Usage
	var finish string
	err = f.Subscribe(ctx, func(c backend.Chunk) error {
		if c.Usage != nil {
			usage = c.Usage
			return nil
		}
		if !started {
			started = true
			if err := sink.Start(out.Meta); err != nil {
				return err
			}
		}
		if c.FinishReason != "" {
			finish = c.FinishReason
		}
		return sink.Chunk(c)
	})
	if err != nil {
		if !started && ctx.Err() == nil {
			out.Meta.Coalesced = false
			out.Meta.Worker = ""
			return false, nil
		}
		return true, err
	}
	if !started {
		if err := sink.Start(out.Meta); err != nil {
			return true, err
		}
	}
	if usage != nil {
		out.Usage = *usage
	} else {
		out.Usage = openai.Usage{PromptTokens: promptTokens, TotalTokens: promptTokens}
	}
	out.FinishReason = finish
	return true, sink.Finish(out.Usage)
}

// collector forwards worker chunks to the sink and the coalescing flight,
// and accumulates what the cache and usage accounting need.
type collector struct {
	sink    Sink
	meta    *Meta
	flight  *batcher.Flight[backend.Chunk]
	started bool
	firstAt time.Time
	content strings.Builder
	finish  string
	worker  *openai.Usage
}

func (c *collector) emit(ch backend.Chunk) error {
	if ch.Usage != nil {
		c.worker = ch.Usage
		if ch.Content == "" && ch.FinishReason == "" {
			return nil
		}
		ch.Usage = nil
	}
	if !c.started {
		c.started = true
		c.firstAt = time.Now()
		if err := c.sink.Start(*c.meta); err != nil {
			return err
		}
	}
	c.content.WriteString(ch.Content)
	if ch.FinishReason != "" {
		c.finish = ch.FinishReason
	}
	if c.flight != nil {
		c.flight.Publish(ch)
	}
	return c.sink.Chunk(ch)
}

func (c *collector) usage(promptTokens int) openai.Usage {
	if c.worker != nil && c.worker.TotalTokens > 0 {
		return *c.worker
	}
	completion := openai.EstimateTokens(c.content.String())
	return openai.Usage{PromptTokens: promptTokens, CompletionTokens: completion, TotalTokens: promptTokens + completion}
}

// job is a request waiting in the batcher for its routing decision.
type job struct {
	ctx context.Context
	req router.Request

	mu        sync.Mutex
	decision  router.Decision
	err       error
	routed    bool
	abandoned bool
}

// abandon marks the job as given up by its caller and releases any slot the
// dispatcher reserved for it concurrently.
func (j *job) abandon() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.abandoned = true
	if j.routed {
		j.decision.Worker.Release()
		j.routed = false
	}
}

// dispatchBatch routes a micro-batch. Jobs are routed in group-key order and
// each decision reserves a slot on its worker immediately, so the bounded-load
// check sees the batch's own requests and same-prefix requests land together
// until the owner is full, then spill in rendezvous order.
func (s *Service) dispatchBatch(b batcher.Batch[*job]) {
	s.metrics.BatchSize.WithLabelValues(b.Key).Observe(float64(len(b.Jobs)))
	s.metrics.BatchWait.Observe(b.Waited.Seconds())
	s.metrics.BatchTarget.WithLabelValues(b.Key).Set(float64(b.Target))
	jobs := append([]*job(nil), b.Jobs...)
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].req.GroupKey < jobs[j].req.GroupKey })
	for _, j := range jobs {
		j.mu.Lock()
		if j.abandoned || j.ctx.Err() != nil {
			j.err = context.Canceled
		} else {
			j.decision, j.err = s.route(j.req)
			j.routed = j.err == nil
		}
		j.mu.Unlock()
	}
}

// route selects a worker and reserves an in-flight slot on it; the attempt
// releases the slot.
func (s *Service) route(req router.Request) (router.Decision, error) {
	d, err := s.router.Select(req)
	if err != nil {
		return d, err
	}
	d.Worker.Acquire()
	s.metrics.RouteDecisions.WithLabelValues(req.Model, string(d.Reason)).Inc()
	return d, nil
}

func (s *Service) generate(ctx context.Context, req *openai.ChatCompletionRequest, call Call, col *collector, start time.Time) error {
	rreq := router.Request{Model: req.Model, Exclude: map[string]bool{}}
	if s.router.Config().PrefixAffinity {
		rreq.ConvKey, rreq.GroupKey = router.Keys(req.Messages, s.router.Config().PrefixChars)
		if call.SessionID != "" {
			rreq.ConvKey = router.SessionKey(call.SessionID)
		}
	}

	var first router.Decision
	var routeErr error
	if s.batcher != nil {
		bctx, bspan := s.tracer.Start(ctx, "batcher.wait")
		j := &job{ctx: bctx, req: rreq}
		err := s.batcher.Submit(bctx, req.Model, j)
		bspan.End()
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			j.abandon()
			return err
		}
		j.mu.Lock()
		first, routeErr = j.decision, j.err
		j.mu.Unlock()
	} else {
		first, routeErr = s.route(rreq)
	}

	var lastErr error
	for attempt := 0; attempt < max(s.retry.MaxAttempts, 1); attempt++ {
		d, err := first, routeErr
		if attempt > 0 {
			s.metrics.Retries.WithLabelValues(req.Model, cause(lastErr)).Inc()
			if serr := resilience.Sleep(ctx, s.retry.Backoff(attempt)); serr != nil {
				return serr
			}
			d, err = s.route(rreq)
		}
		if err != nil {
			if lastErr != nil && errors.Is(err, router.ErrNoWorkers) {
				return lastErr // failover exhausted; report the real failure
			}
			return err
		}
		committed, err := s.attempt(ctx, d, req, attempt, col, start)
		if err == nil {
			return nil
		}
		lastErr = err
		if committed || !resilience.IsRetryable(ctx, err) {
			return err
		}
		rreq.Exclude[d.Worker.ID] = true
		s.logger.WarnContext(ctx, "attempt failed; failing over", "worker", d.Worker.ID, "attempt", attempt+1, "error", err)
	}
	return lastErr
}

// attempt runs one request against one worker. committed reports whether any
// output reached the client, after which the request can no longer be retried.
func (s *Service) attempt(ctx context.Context, d router.Decision, req *openai.ChatCompletionRequest, n int, col *collector, reqStart time.Time) (committed bool, err error) {
	w := d.Worker
	defer w.Release()
	if !w.Breaker.Allow() {
		return false, resilience.ErrOpen
	}
	ctx, span := s.tracer.Start(ctx, "worker.attempt", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("gateway.worker", w.ID),
		attribute.String("gateway.worker.kind", string(w.Kind)),
		attribute.String("gateway.route", string(d.Reason)),
		attribute.Int("gateway.attempt", n+1),
	))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, cause(err))
		}
		span.End()
	}()

	actx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	attemptStart := time.Now()
	timer := time.AfterFunc(s.cfg.Resilience.FirstTokenTimeout, func() { cancel(resilience.ErrFirstTokenTimeout) })

	stream, err := w.Adapter.ChatStream(actx, w.URL, req)
	var chunk backend.Chunk
	if err == nil {
		chunk, err = stream.Recv()
	}
	timer.Stop()
	if err != nil {
		if stream != nil {
			stream.Close()
		}
		if errors.Is(context.Cause(actx), resilience.ErrFirstTokenTimeout) {
			err = resilience.ErrFirstTokenTimeout
		}
		s.recordFailure(ctx, w, err)
		return false, err
	}
	defer stream.Close()

	ttft := time.Since(attemptStart)
	w.Breaker.RecordSuccess(ttft)
	w.TTFT.Observe(ttft)
	s.metrics.TTFT.WithLabelValues(req.Model, w.ID).Observe(time.Since(reqStart).Seconds())
	span.AddEvent("first_token", trace.WithAttributes(attribute.Int64("gateway.ttft_ms", ttft.Milliseconds())))
	col.meta.Worker, col.meta.Route, col.meta.Attempts = w.ID, d.Reason, n+1

	chunks := 0
	for {
		if err := col.emit(chunk); err != nil {
			return true, err // client went away
		}
		chunks++
		chunk, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.recordFailure(ctx, w, err)
			return true, err
		}
	}
	span.AddEvent("stream_complete", trace.WithAttributes(attribute.Int("gateway.chunks", chunks)))
	return true, nil
}

func (s *Service) recordFailure(ctx context.Context, w *registry.Worker, err error) {
	if ctx.Err() != nil {
		w.Breaker.Release() // the client left; say nothing about the worker
		return
	}
	if workerFault(err) {
		w.Breaker.RecordFailure()
	} else {
		w.Breaker.Release()
	}
	s.metrics.UpstreamErrors.WithLabelValues(w.ID, cause(err)).Inc()
}

// ObserveProbe exports a worker's probe result as metrics.
func (s *Service) ObserveProbe(w *registry.Worker, res backend.ProbeResult, err error) {
	st := w.Status()
	s.metrics.WorkerHealthy.WithLabelValues(w.ID).Set(b2f(st.Healthy))
	s.metrics.WorkerInFlight.WithLabelValues(w.ID).Set(float64(st.InFlight))
	s.metrics.WorkerQueueDepth.WithLabelValues(w.ID).Set(float64(st.Waiting))
	s.metrics.WorkerRunning.WithLabelValues(w.ID).Set(float64(st.Running))
	s.metrics.WorkerGPUUtil.WithLabelValues(w.ID).Set(st.GPUUtil)
	s.metrics.WorkerKVCache.WithLabelValues(w.ID).Set(st.KVCacheUsage)
	s.metrics.WorkerPrefixHit.WithLabelValues(w.ID).Set(st.PrefixHitRate)
	s.metrics.BreakerState.WithLabelValues(w.ID).Set(breakerValue(w.Breaker.State()))
	if err != nil {
		s.metrics.UpstreamErrors.WithLabelValues(w.ID, "probe").Inc()
	}
}

// ObserveBreaker exports and logs a breaker transition.
func (s *Service) ObserveBreaker(w *registry.Worker, from, to resilience.State) {
	s.metrics.BreakerState.WithLabelValues(w.ID).Set(breakerValue(to))
	s.metrics.BreakerTransitions.WithLabelValues(w.ID, to.String()).Inc()
	s.logger.Warn("circuit breaker transition", "worker", w.ID, "from", from.String(), "to", to.String())
}

// Ready reports whether at least one worker is healthy.
func (s *Service) Ready() bool {
	for _, w := range s.reg.List() {
		if w.Healthy() {
			return true
		}
	}
	return false
}

func breakerValue(st resilience.State) float64 {
	switch st {
	case resilience.HalfOpen:
		return 1
	case resilience.Open:
		return 2
	}
	return 0
}

func splitForReplay(s string) []string {
	var out []string
	for len(s) > 0 {
		i := strings.IndexAny(s[1:], " \n")
		if i < 0 {
			out = append(out, s)
			break
		}
		out = append(out, s[:i+1])
		s = s[i+1:]
	}
	return out
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
