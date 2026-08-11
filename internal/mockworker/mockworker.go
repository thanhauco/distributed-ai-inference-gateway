// Package mockworker simulates a vLLM or Ollama GPU worker closely enough to
// exercise the gateway end to end without GPUs: prefill cost proportional to
// uncached prompt tokens, a block-level prefix (KV) cache, bounded
// concurrency with a waiting queue, per-sequence decode speed that degrades
// under load, cold model loads, and fault injection.
package mockworker

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// Config configures a simulated worker.
type Config struct {
	ID     string
	Kind   string // vllm | ollama
	Models []string

	PrefillTPS       float64       // prompt tokens/s for uncached blocks
	DecodeTPS        float64       // tokens/s per sequence when idle
	MaxConcurrency   int           // sequences decoded at once; the rest wait
	PrefixCacheBlock int           // characters per KV block (~16 tokens)
	PrefixCacheSize  int           // KV blocks kept in the prefix cache
	DefaultMaxTokens int           // completion length when max_tokens is unset
	ColdStart        time.Duration // time to load a non-resident model (ollama)
	MaxLoaded        int           // models resident at once (ollama)
	FailRate         float64       // probability of a 500 before the first token
	TimeScale        float64       // multiply every simulated delay (tests use ~0.01)
}

func (c *Config) defaults() {
	if c.Kind == "" {
		c.Kind = "vllm"
	}
	if c.PrefillTPS <= 0 {
		c.PrefillTPS = 4000
	}
	if c.DecodeTPS <= 0 {
		c.DecodeTPS = 60
	}
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 16
	}
	if c.PrefixCacheBlock <= 0 {
		c.PrefixCacheBlock = 64
	}
	if c.PrefixCacheSize <= 0 {
		c.PrefixCacheSize = 4096
	}
	if c.DefaultMaxTokens <= 0 {
		c.DefaultMaxTokens = 48
	}
	if c.ColdStart <= 0 {
		c.ColdStart = 2 * time.Second
	}
	if c.MaxLoaded <= 0 {
		c.MaxLoaded = 1
	}
	if c.TimeScale <= 0 {
		c.TimeScale = 1
	}
}

// Server is a simulated worker.
type Server struct {
	cfg Config
	sem chan struct{}

	running atomic.Int64
	waiting atomic.Int64
	healthy atomic.Bool
	failN   atomic.Int64 // fail the next N requests
	slowTTF atomic.Int64 // extra delay before first token, ns

	prefillCached   atomic.Int64
	prefillUncached atomic.Int64
	generated       atomic.Int64
	requests        atomic.Int64

	mu     sync.Mutex
	blocks *list.List
	index  map[uint64]*list.Element
	loaded *list.List // resident models, most recent first
}

// New returns a simulated worker.
func New(cfg Config) *Server {
	cfg.defaults()
	s := &Server{cfg: cfg, sem: make(chan struct{}, cfg.MaxConcurrency), blocks: list.New(), index: map[uint64]*list.Element{}, loaded: list.New()}
	s.healthy.Store(true)
	if len(cfg.Models) > 0 {
		s.loaded.PushFront(cfg.Models[0])
	}
	return s
}

// SetHealthy toggles the health endpoints.
func (s *Server) SetHealthy(ok bool) { s.healthy.Store(ok) }

// FailNext makes the next n chat requests fail with HTTP 500.
func (s *Server) FailNext(n int) { s.failN.Store(int64(n)) }

// SetFirstTokenDelay adds latency before every first token.
func (s *Server) SetFirstTokenDelay(d time.Duration) { s.slowTTF.Store(int64(d)) }

// Requests returns the number of chat requests received.
func (s *Server) Requests() int64 { return s.requests.Load() }

// PrefixHitRate returns the fraction of prompt tokens served from cache.
func (s *Server) PrefixHitRate() float64 {
	c, u := s.prefillCached.Load(), s.prefillUncached.Load()
	if c+u == 0 {
		return 0
	}
	return float64(c) / float64(c+u)
}

// Handler returns the worker's HTTP API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("POST /_mock/fail", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		s.FailNext(max(n, 1))
	})
	mux.HandleFunc("POST /_mock/health", func(w http.ResponseWriter, r *http.Request) {
		s.SetHealthy(r.URL.Query().Get("ok") != "false")
	})
	mux.HandleFunc("POST /_mock/slow", func(w http.ResponseWriter, r *http.Request) {
		d, _ := time.ParseDuration(r.URL.Query().Get("ttft"))
		s.SetFirstTokenDelay(d)
	})
	if s.cfg.Kind == "ollama" {
		mux.HandleFunc("GET /api/tags", s.ollamaTags)
		mux.HandleFunc("GET /api/ps", s.ollamaPS)
		mux.HandleFunc("POST /api/chat", s.ollamaChat)
		mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "Ollama is running") })
	} else {
		mux.HandleFunc("GET /health", s.health)
		mux.HandleFunc("GET /v1/models", s.models)
		mux.HandleFunc("POST /v1/chat/completions", s.chat)
	}
	return mux
}

func (s *Server) sleep(ctx context.Context, d time.Duration) error {
	d = time.Duration(float64(d) * s.cfg.TimeScale)
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *Server) serves(model string) bool {
	for _, m := range s.cfg.Models {
		if m == model {
			return true
		}
	}
	return false
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	if !s.healthy.Load() {
		http.Error(w, "unhealthy", http.StatusServiceUnavailable)
	}
}

func (s *Server) models(w http.ResponseWriter, _ *http.Request) {
	if !s.healthy.Load() {
		http.Error(w, "unhealthy", http.StatusServiceUnavailable)
		return
	}
	list := openai.ModelList{Object: "list"}
	for _, m := range s.cfg.Models {
		list.Data = append(list.Data, openai.Model{ID: m, Object: "model", OwnedBy: s.cfg.ID})
	}
	writeJSON(w, list)
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	kv := float64(s.blocks.Len()) / float64(s.cfg.PrefixCacheSize)
	s.mu.Unlock()
	running := s.running.Load()
	fmt.Fprintf(w, "# simulated worker %s (%s)\n", s.cfg.ID, s.cfg.Kind)
	fmt.Fprintf(w, "vllm:num_requests_running %d\n", running)
	fmt.Fprintf(w, "vllm:num_requests_waiting %d\n", s.waiting.Load())
	fmt.Fprintf(w, "vllm:gpu_cache_usage_perc %.4f\n", kv)
	fmt.Fprintf(w, "vllm:gpu_prefix_cache_hit_rate %.4f\n", s.PrefixHitRate())
	fmt.Fprintf(w, "gpu_utilization %.4f\n", min(1, 0.05+float64(running)/float64(s.cfg.MaxConcurrency)))
	fmt.Fprintf(w, "mock_prefill_tokens_total{cached=\"true\"} %d\n", s.prefillCached.Load())
	fmt.Fprintf(w, "mock_prefill_tokens_total{cached=\"false\"} %d\n", s.prefillUncached.Load())
	fmt.Fprintf(w, "mock_generated_tokens_total %d\n", s.generated.Load())
	fmt.Fprintf(w, "mock_requests_total %d\n", s.requests.Load())
}

// generation is one simulated completion.
type generation struct {
	promptTokens int
	cachedTokens int
	words        []string
	finish       string
}

// prepare runs admission, cold load and prefill; the caller must call
// s.finish when done decoding.
func (s *Server) prepare(ctx context.Context, req *openai.ChatCompletionRequest) (*generation, int, error) {
	s.requests.Add(1)
	if !s.serves(req.Model) {
		return nil, http.StatusNotFound, fmt.Errorf("model %q not found", req.Model)
	}
	if n := s.failN.Load(); n > 0 && s.failN.CompareAndSwap(n, n-1) {
		return nil, http.StatusInternalServerError, fmt.Errorf("injected failure")
	}
	if s.cfg.FailRate > 0 && rand.Float64() < s.cfg.FailRate {
		return nil, http.StatusInternalServerError, fmt.Errorf("simulated CUDA error")
	}

	s.waiting.Add(1)
	select {
	case s.sem <- struct{}{}:
		s.waiting.Add(-1)
	case <-ctx.Done():
		s.waiting.Add(-1)
		return nil, 499, ctx.Err()
	}
	s.running.Add(1)

	if s.cfg.Kind == "ollama" && !s.resident(req.Model) {
		if err := s.sleep(ctx, s.cfg.ColdStart); err != nil {
			s.finish()
			return nil, 499, err
		}
	}

	prompt := promptText(req)
	g := &generation{promptTokens: openai.PromptTokens(req)}
	cachedChars := s.prefixLookup(prompt)
	g.cachedTokens = min(cachedChars/4, g.promptTokens)
	uncached := g.promptTokens - g.cachedTokens
	s.prefillCached.Add(int64(g.cachedTokens))
	s.prefillUncached.Add(int64(uncached))
	delay := time.Duration(float64(uncached) / s.cfg.PrefillTPS * float64(time.Second))
	delay += time.Duration(s.slowTTF.Load())
	if err := s.sleep(ctx, delay); err != nil {
		s.finish()
		return nil, 499, err
	}

	n := s.cfg.DefaultMaxTokens
	if req.MaxTokens != nil {
		n = *req.MaxTokens
	}
	g.words, g.finish = completion(req.Model+"\x00"+prompt, n)
	return g, http.StatusOK, nil
}

func (s *Server) finish() {
	s.running.Add(-1)
	<-s.sem
}

// decodeDelay is the per-token latency; batch contention slows each sequence.
func (s *Server) decodeDelay() time.Duration {
	load := float64(s.running.Load()) / float64(s.cfg.MaxConcurrency)
	return time.Duration((1 + 0.75*load) / s.cfg.DecodeTPS * float64(time.Second))
}

// prefixLookup returns how many leading prompt characters were already in
// the KV cache, then inserts every block of the prompt.
func (s *Server) prefixLookup(prompt string) int {
	bs := s.cfg.PrefixCacheBlock
	h := fnv.New64a()
	var keys []uint64
	for i := 0; i+bs <= len(prompt); i += bs { // only full blocks are cacheable
		_, _ = h.Write([]byte(prompt[i : i+bs]))
		keys = append(keys, h.Sum64())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	hit := 0
	for _, k := range keys {
		if _, ok := s.index[k]; !ok {
			break
		}
		hit++
	}
	for _, k := range keys {
		if e, ok := s.index[k]; ok {
			s.blocks.MoveToFront(e)
			continue
		}
		s.index[k] = s.blocks.PushFront(k)
		if s.blocks.Len() > s.cfg.PrefixCacheSize {
			last := s.blocks.Back()
			s.blocks.Remove(last)
			delete(s.index, last.Value.(uint64))
		}
	}
	return hit * bs
}

func (s *Server) resident(model string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for e := s.loaded.Front(); e != nil; e = e.Next() {
		if e.Value.(string) == model {
			s.loaded.MoveToFront(e)
			return true
		}
	}
	s.loaded.PushFront(model)
	for s.loaded.Len() > s.cfg.MaxLoaded {
		s.loaded.Remove(s.loaded.Back())
	}
	return false
}

func promptText(req *openai.ChatCompletionRequest) string {
	var sb strings.Builder
	for _, m := range req.Messages {
		sb.WriteString("<|" + m.Role + "|>")
		sb.WriteString(m.Content)
	}
	return sb.String()
}

var vocabulary = strings.Fields(`the gateway routes each request to a worker that already holds the
prompt prefix in its KV cache so prefill is skipped and tokens start streaming sooner while
continuous batching keeps the GPU busy and the router balances load across replicas with power
of two choices bounded by a load factor circuit breakers isolate slow workers retries add jitter
and failover moves traffic to a healthy replica metrics traces and logs share one correlation id`)

// completion produces deterministic text for a prompt so identical requests
// produce identical answers (which the cache and coalescing rely on).
func completion(seed string, n int) ([]string, string) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	rng := rand.New(rand.NewPCG(h.Sum64(), 0x9e3779b97f4a7c15))
	length := n
	finish := "length"
	if natural := 16 + rng.IntN(48); natural < n {
		length, finish = natural, "stop"
	}
	words := make([]string, length)
	for i := range words {
		words[i] = vocabulary[rng.IntN(len(vocabulary))]
		if i == 0 {
			words[i] = strings.ToUpper(words[i][:1]) + words[i][1:]
		}
		if i < length-1 {
			words[i] += " "
		} else {
			words[i] += "."
		}
	}
	return words, finish
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
