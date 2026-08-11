package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/admin"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/auth"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/cache"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/catalog"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/config"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/gateway"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/mockworker"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/observability"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/ratelimit"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/registry"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/resilience"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/router"
	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

const testKey = "sk-test-key"

type harness struct {
	t       *testing.T
	srv     *httptest.Server
	reg     *registry.Registry
	keys    *auth.Store
	workers map[string]*mockworker.Server
	metrics *observability.Metrics
}

type harnessOpt func(*config.Config)

func newHarness(t *testing.T, opts ...harnessOpt) *harness {
	t.Helper()
	cfg := config.Default()
	cfg.Batcher.MinWait, cfg.Batcher.MaxWait = time.Millisecond, 2*time.Millisecond
	cfg.Resilience.FirstTokenTimeout = 300 * time.Millisecond
	cfg.Resilience.BackoffBase = time.Millisecond
	cfg.Resilience.Breaker.FailureThreshold = 2
	cfg.Resilience.Breaker.OpenDuration = time.Hour
	cfg.Models = []config.ModelConfig{{Name: "llama3-8b", Aliases: []string{"gpt-4o-mini"}}}
	for _, o := range opts {
		o(cfg)
	}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	h := &harness{t: t, workers: map[string]*mockworker.Server{}, metrics: observability.NewMetrics()}

	var svc *gateway.Service
	h.reg = registry.New(registry.Options{Logger: logger, Breaker: resilience.BreakerConfig{
		FailureThreshold: cfg.Resilience.Breaker.FailureThreshold, OpenDuration: cfg.Resilience.Breaker.OpenDuration,
	}, OnBreakerChange: func(w *registry.Worker, from, to resilience.State) {
		if svc != nil {
			svc.ObserveBreaker(w, from, to)
		}
	}})
	addWorker := func(id, kind string, models ...string) {
		mw := mockworker.New(mockworker.Config{ID: id, Kind: kind, Models: models, TimeScale: 0.02, ColdStart: time.Millisecond})
		ts := httptest.NewServer(mw.Handler())
		t.Cleanup(ts.Close)
		h.workers[id] = mw
		if _, err := h.reg.Upsert(registry.Spec{ID: id, URL: ts.URL, Kind: backend.Kind(kind), Models: models}, registry.SourceStatic); err != nil {
			t.Fatal(err)
		}
	}
	addWorker("vllm-1", "vllm", "llama3-8b", "mistral-7b")
	addWorker("vllm-2", "vllm", "llama3-8b")
	addWorker("ollama-1", "ollama", "phi3")
	h.probe()

	cat, _ := catalog.FromConfig(cfg)
	h.keys = auth.NewStore()
	h.keys.Add(testKey, auth.Spec{Name: "test"}, "config")
	rt := router.New(h.reg, router.Config{PrefixAffinity: cfg.Router.PrefixAffinity, PrefixChars: cfg.Router.PrefixChars, LoadFactor: cfg.Router.LoadFactor})
	var limiter ratelimit.Limiter
	if cfg.RateLimit.Enabled {
		limiter = ratelimit.NewMemory(ratelimit.Config{Window: cfg.RateLimit.Window, Buckets: cfg.RateLimit.Buckets})
	}
	var c *cache.Cache
	if cfg.Cache.Enabled {
		c = cache.New(cache.NewMemoryStore(100), nil, cache.Config{TTL: time.Minute, Semantic: true, SimilarityThreshold: cfg.Cache.SimilarityThreshold})
	}
	svc = gateway.New(gateway.Options{Config: cfg, Registry: h.reg, Router: rt, Catalog: cat, Limiter: limiter, Cache: c, Metrics: h.metrics, Logger: logger})
	adm := &admin.Handler{Token: "admin", Keys: h.keys, Registry: h.reg, Catalog: cat}
	h.srv = httptest.NewServer(NewHandler(Options{
		Service: svc, Keys: h.keys, AuthEnabled: true, Registry: h.reg, Catalog: cat,
		Metrics: h.metrics, Logger: logger, MaxBodyBytes: 1 << 20, Admin: adm,
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *harness) probe() { h.reg.ProbeAll(context.Background(), time.Second, 1, nil) }

func (h *harness) post(body any, headers ...string) *http.Response {
	h.t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/v1/chat/completions", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testKey)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

func decode[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func body(model string, msgs ...string) map[string]any {
	var m []map[string]string
	for i, c := range msgs {
		role := "user"
		if i == 0 && len(msgs) > 1 {
			role = "system"
		}
		m = append(m, map[string]string{"role": role, "content": c})
	}
	return map[string]any{"model": model, "messages": m, "max_tokens": 24}
}

func TestChatCompletionJSON(t *testing.T) {
	h := newHarness(t)
	resp := h.post(body("gpt-4o-mini", "You are terse.", "What is KV cache reuse?"))
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if w := resp.Header.Get("X-Gateway-Worker"); !strings.HasPrefix(w, "vllm-") {
		t.Fatalf("worker header %q", w)
	}
	if resp.Header.Get("X-Request-ID") == "" || resp.Header.Get("X-Ratelimit-Remaining-Tokens") == "" {
		t.Fatal("missing correlation or rate limit headers")
	}
	out := decode[openai.ChatCompletionResponse](t, resp)
	if out.Object != "chat.completion" || out.Model != "llama3-8b" || out.Choices[0].Message.Content == "" {
		t.Fatalf("bad response %+v", out)
	}
	if out.Usage == nil || out.Usage.CompletionTokens == 0 || out.Usage.TotalTokens != out.Usage.PromptTokens+out.Usage.CompletionTokens {
		t.Fatalf("bad usage %+v", out.Usage)
	}
}

func TestChatCompletionSSE(t *testing.T) {
	h := newHarness(t)
	b := body("llama3-8b", "stream please")
	b["stream"] = true
	b["stream_options"] = map[string]bool{"include_usage": true}
	resp := h.post(b)
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	var content strings.Builder
	var sawRole, sawFinish, sawUsage, sawDone bool
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			sawDone = true
			break
		}
		var c openai.ChatCompletionChunk
		if err := json.Unmarshal([]byte(data), &c); err != nil {
			t.Fatal(err)
		}
		if c.Object != "chat.completion.chunk" || c.ID == "" {
			t.Fatalf("bad chunk %s", data)
		}
		if c.Usage != nil {
			sawUsage = true
			continue
		}
		if c.Choices[0].Delta.Role == "assistant" {
			sawRole = true
		}
		content.WriteString(c.Choices[0].Delta.Content)
		if c.Choices[0].FinishReason != nil {
			sawFinish = true
		}
	}
	if !sawRole || !sawFinish || !sawUsage || !sawDone || content.Len() == 0 {
		t.Fatalf("role=%v finish=%v usage=%v done=%v content=%q", sawRole, sawFinish, sawUsage, sawDone, content.String())
	}
}

func TestOllamaWorkerPath(t *testing.T) {
	h := newHarness(t)
	resp := h.post(body("phi3", "hi from ollama"))
	out := decode[openai.ChatCompletionResponse](t, resp)
	if resp.Header.Get("X-Gateway-Worker") != "ollama-1" || out.Choices[0].Message.Content == "" || out.Usage.PromptTokens == 0 {
		t.Fatalf("ollama path: %s %+v", resp.Header.Get("X-Gateway-Worker"), out)
	}
}

func TestAuthAndModelErrors(t *testing.T) {
	h := newHarness(t)
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/v1/chat/completions", strings.NewReader(`{}`))
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != 401 {
		t.Fatalf("no key: %d", resp.StatusCode)
	}
	e := decode[openai.ErrorBody](t, resp)
	if e.Error.Code != "invalid_api_key" {
		t.Fatalf("error body %+v", e)
	}

	if resp := h.post(body("gpt-17", "x")); resp.StatusCode != 404 {
		t.Fatalf("unknown model: %d", resp.StatusCode)
	}
	if resp := h.post(map[string]any{"model": "llama3-8b", "messages": []any{}}); resp.StatusCode != 400 {
		t.Fatalf("empty messages: %d", resp.StatusCode)
	}

	raw, _, _ := h.keys.Create(auth.Spec{Name: "phi-only", Models: []string{"phi3"}})
	b, _ := json.Marshal(body("llama3-8b", "x"))
	req, _ = http.NewRequest(http.MethodPost, h.srv.URL+"/v1/chat/completions", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+raw)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 403 {
		t.Fatalf("model allow-list: %d", resp.StatusCode)
	}
}

func TestTokenRateLimit(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.RateLimit.DefaultTokensPerMin = 160 })
	// Budget = prompt (14) + max_tokens (24) = 38: four fit in 160; a large one does not.
	for i := range 4 {
		if resp := h.post(body("llama3-8b", "count tokens not requests")); resp.StatusCode != 200 {
			t.Fatalf("request %d: %d", i, resp.StatusCode)
		}
	}
	b := body("llama3-8b", "count tokens not requests")
	b["max_tokens"] = 120
	resp := h.post(b)
	if resp.StatusCode != 429 {
		t.Fatalf("expected 429, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" || resp.Header.Get("X-Ratelimit-Reset-Tokens") == "" {
		t.Fatalf("missing retry headers: %v", resp.Header)
	}
	if e := decode[openai.ErrorBody](t, resp); e.Error.Code != "rate_limit_exceeded" {
		t.Fatalf("error %+v", e)
	}
}

func TestExactAndSemanticCache(t *testing.T) {
	h := newHarness(t)
	ask := func(q string) (*http.Response, openai.ChatCompletionResponse) {
		b := body("llama3-8b", "You answer questions.", q)
		b["temperature"] = 0
		resp := h.post(b)
		return resp, decode[openai.ChatCompletionResponse](t, resp)
	}
	r1, a1 := ask("How do I reverse a list in Python?")
	if r1.Header.Get("X-Gateway-Cache") != "miss" {
		t.Fatalf("first: %s", r1.Header.Get("X-Gateway-Cache"))
	}
	before := h.workers["vllm-1"].Requests() + h.workers["vllm-2"].Requests()
	r2, a2 := ask("How do I reverse a list in Python?")
	r3, a3 := ask("how do i reverse a list in python")
	after := h.workers["vllm-1"].Requests() + h.workers["vllm-2"].Requests()
	if r2.Header.Get("X-Gateway-Cache") != "hit_exact" || r3.Header.Get("X-Gateway-Cache") != "hit_semantic" {
		t.Fatalf("cache headers %s %s", r2.Header.Get("X-Gateway-Cache"), r3.Header.Get("X-Gateway-Cache"))
	}
	if after != before {
		t.Fatal("cache hits must not reach workers")
	}
	if a1.Choices[0].Message.Content != a2.Choices[0].Message.Content || a2.Choices[0].Message.Content != a3.Choices[0].Message.Content {
		t.Fatal("cached content differs")
	}
	// Sampling with temperature > 0 bypasses the cache by default.
	b := body("llama3-8b", "You answer questions.", "How do I reverse a list in Python?")
	b["temperature"] = 0.8
	if resp := h.post(b); resp.Header.Get("X-Gateway-Cache") != "bypass" {
		t.Fatalf("sampled request: %s", resp.Header.Get("X-Gateway-Cache"))
	}
}

func TestCoalescingIdenticalRequests(t *testing.T) {
	h := newHarness(t)
	h.workers["vllm-1"].SetFirstTokenDelay(5 * time.Second) // ×0.02 = 100ms
	h.workers["vllm-2"].SetFirstTokenDelay(5 * time.Second)
	b := body("llama3-8b", "identical prompt from many clients")
	b["temperature"] = 0
	var wg sync.WaitGroup
	contents := make([]string, 6)
	coalesced := make([]bool, 6)
	for i := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := h.post(b, "X-Gateway-Cache", "force")
			coalesced[i] = resp.Header.Get("X-Gateway-Coalesced") == "true"
			contents[i] = decode[openai.ChatCompletionResponse](t, resp).Choices[0].Message.Content
		}()
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
	total := h.workers["vllm-1"].Requests() + h.workers["vllm-2"].Requests()
	if total != 1 {
		t.Fatalf("expected one upstream generation, got %d", total)
	}
	n := 0
	for i := range contents {
		if contents[i] != contents[0] || contents[i] == "" {
			t.Fatalf("follower %d got different content", i)
		}
		if coalesced[i] {
			n++
		}
	}
	if n != 5 {
		t.Fatalf("expected 5 coalesced followers, got %d", n)
	}
}

func TestFailoverAndCircuitBreaker(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.Router.PrefixAffinity = false })
	h.workers["vllm-1"].FailNext(1000)
	for i := range 10 {
		resp := h.post(body("llama3-8b", "resilience", string(rune('a'+i))))
		if resp.StatusCode != 200 {
			t.Fatalf("request %d failed: %d", i, resp.StatusCode)
		}
		if w := resp.Header.Get("X-Gateway-Worker"); w != "vllm-2" {
			t.Fatalf("request %d served by %s", i, w)
		}
		resp.Body.Close()
	}
	if st := h.reg.Get("vllm-1").Breaker.State(); st != resilience.Open {
		t.Fatalf("breaker should be open, is %v", st)
	}
	if h.workers["vllm-1"].Requests() > 2 {
		t.Fatalf("open breaker must stop traffic: vllm-1 saw %d requests", h.workers["vllm-1"].Requests())
	}
}

func TestFirstTokenTimeoutRetries(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.Router.PrefixAffinity = false })
	h.workers["vllm-1"].SetFirstTokenDelay(time.Minute) // ×0.02 = 1.2s ≫ 300ms
	sawRetry := false
	for i := range 6 {
		start := time.Now()
		resp := h.post(body("llama3-8b", "ttft", string(rune('a'+i))))
		if resp.StatusCode != 200 || resp.Header.Get("X-Gateway-Worker") != "vllm-2" {
			t.Fatalf("status %d worker %s", resp.StatusCode, resp.Header.Get("X-Gateway-Worker"))
		}
		if resp.Header.Get("X-Gateway-Attempts") == "2" {
			sawRetry = true
			if time.Since(start) > time.Second {
				t.Fatal("first-token timeout did not cut the slow attempt short")
			}
		}
		resp.Body.Close()
	}
	if !sawRetry {
		t.Fatal("expected at least one request to fail over after a first-token timeout")
	}
}

func TestPrefixAffinityKeepsConversationOnOneWorker(t *testing.T) {
	h := newHarness(t)
	system := strings.Repeat("You are a meticulous legal assistant. ", 40)
	msgs := []map[string]string{{"role": "system", "content": system}, {"role": "user", "content": "Summarise clause 4."}}
	var worker string
	for turn := range 5 {
		resp := h.post(map[string]any{"model": "llama3-8b", "messages": msgs, "max_tokens": 8})
		out := decode[openai.ChatCompletionResponse](t, resp)
		w := resp.Header.Get("X-Gateway-Worker")
		if turn == 0 {
			worker = w
		} else if w != worker {
			t.Fatalf("turn %d moved from %s to %s", turn, worker, w)
		}
		if turn > 0 && resp.Header.Get("X-Gateway-Route") != string(router.ReasonAffinitySticky) {
			t.Fatalf("turn %d route %s", turn, resp.Header.Get("X-Gateway-Route"))
		}
		msgs = append(msgs, map[string]string{"role": "assistant", "content": out.Choices[0].Message.Content},
			map[string]string{"role": "user", "content": "And clause " + string(rune('5'+turn)) + "?"})
	}
	if rate := h.workers[worker].PrefixHitRate(); rate < 0.5 {
		t.Fatalf("expected most prompt tokens served from the prefix cache, got %.2f", rate)
	}
}

func TestDynamicRegistrationAndModels(t *testing.T) {
	h := newHarness(t)
	mw := mockworker.New(mockworker.Config{ID: "vllm-3", Models: []string{"mistral-7b", "llama3-70b"}, TimeScale: 0.02})
	ts := httptest.NewServer(mw.Handler())
	defer ts.Close()

	b, _ := json.Marshal(registry.Spec{ID: "vllm-3", URL: ts.URL, Kind: "vllm"})
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/admin/v1/workers", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer admin")
	if resp, _ := http.DefaultClient.Do(req); resp.StatusCode != 200 {
		t.Fatalf("register: %d", resp.StatusCode)
	}
	h.probe()
	resp := h.post(body("llama3-70b", "new model, zero client changes"))
	if resp.StatusCode != 200 || resp.Header.Get("X-Gateway-Worker") != "vllm-3" {
		t.Fatalf("new worker not routed: %d %s", resp.StatusCode, resp.Header.Get("X-Gateway-Worker"))
	}

	req, _ = http.NewRequest(http.MethodGet, h.srv.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+testKey)
	r, _ := http.DefaultClient.Do(req)
	list := decode[openai.ModelList](t, r)
	ids := map[string]bool{}
	for _, m := range list.Data {
		ids[m.ID] = true
	}
	for _, want := range []string{"llama3-8b", "gpt-4o-mini", "mistral-7b", "phi3", "llama3-70b"} {
		if !ids[want] {
			t.Errorf("/v1/models missing %s: %v", want, ids)
		}
	}
}

func TestHealthReadinessMetrics(t *testing.T) {
	h := newHarness(t)
	for path, want := range map[string]int{"/healthz": 200, "/readyz": 200, "/nope": 404} {
		resp, _ := http.Get(h.srv.URL + path)
		if resp.StatusCode != want {
			t.Errorf("%s: %d", path, resp.StatusCode)
		}
	}
	h.post(body("llama3-8b", "metrics")).Body.Close()
	resp, _ := http.Get(h.srv.URL + "/metrics")
	raw, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"gateway_requests_total", "gateway_time_to_first_token_seconds", "gateway_tokens_total", "gateway_batch_size", "gateway_route_decisions_total"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("metrics missing %s", want)
		}
	}
	for _, w := range h.workers {
		w.SetHealthy(false)
	}
	h.probe()
	if resp, _ := http.Get(h.srv.URL + "/readyz"); resp.StatusCode != 503 {
		t.Fatalf("readyz with no healthy workers: %d", resp.StatusCode)
	}
	if resp := h.post(body("llama3-8b", "anyone?")); resp.StatusCode != 503 {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
}
