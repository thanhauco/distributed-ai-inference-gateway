package cache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

func stores(t *testing.T) map[string]Store {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return map[string]Store{"memory": NewMemoryStore(100), "redis": NewRedisStore(client)}
}

func chat(system, user string) *openai.ChatCompletionRequest {
	temp := 0.0
	return &openai.ChatCompletionRequest{
		Model:       "llama3-8b",
		Temperature: &temp,
		Messages:    []openai.Message{{Role: "system", Content: system}, {Role: "user", Content: user}},
	}
}

func TestExactAndSemanticTiers(t *testing.T) {
	for name, store := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c := New(store, nil, Config{TTL: time.Minute, Semantic: true, SimilarityThreshold: 0.95})

			p := c.Plan(chat("be helpful", "Explain KV cache reuse in vLLM"))
			if r, _ := c.Lookup(ctx, p); r.Kind != Miss {
				t.Fatalf("empty cache: %v", r.Kind)
			}
			if err := c.Store(ctx, p, "Reusing prefix blocks...", "stop", &openai.Usage{TotalTokens: 9}); err != nil {
				t.Fatal(err)
			}

			r, _ := c.Lookup(ctx, c.Plan(chat("be helpful", "Explain KV cache reuse in vLLM")))
			if r.Kind != HitExact || r.Entry.Content != "Reusing prefix blocks..." || r.Entry.Usage.TotalTokens != 9 {
				t.Fatalf("exact: %+v", r)
			}
			r, _ = c.Lookup(ctx, c.Plan(chat("be helpful", "explain KV-cache reuse in vLLM.")))
			if r.Kind != HitSemantic || r.Similarity < 0.95 {
				t.Fatalf("semantic: %+v", r)
			}
			r, _ = c.Lookup(ctx, c.Plan(chat("be helpful", "Explain speculative decoding in vLLM")))
			if r.Kind != Miss {
				t.Fatalf("different question must miss: %+v", r)
			}
			r, _ = c.Lookup(ctx, c.Plan(chat("be terse", "Explain KV cache reuse in vLLM")))
			if r.Kind != Miss {
				t.Fatalf("different system prompt must miss: %+v", r)
			}
		})
	}
}

func TestParametersPartitionCache(t *testing.T) {
	ctx := context.Background()
	c := New(NewMemoryStore(10), nil, Config{TTL: time.Minute, Semantic: true, SimilarityThreshold: 0.9})
	base := chat("s", "hello there")
	_ = c.Store(ctx, c.Plan(base), "hi", "stop", nil)

	other := chat("s", "hello there")
	n := 5
	other.MaxTokens = &n
	if r, _ := c.Lookup(ctx, c.Plan(other)); r.Kind != Miss {
		t.Fatalf("max_tokens must partition the cache: %v", r.Kind)
	}
	other = chat("s", "hello there")
	other.Model = "mistral-7b"
	if r, _ := c.Lookup(ctx, c.Plan(other)); r.Kind != Miss {
		t.Fatalf("model must partition the cache: %v", r.Kind)
	}
}

func TestTTL(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	store := NewMemoryStore(10)
	store.now = func() time.Time { return now }
	c := New(store, nil, Config{TTL: time.Minute, Semantic: true, SimilarityThreshold: 0.9})
	c.now = func() time.Time { return now }
	_ = c.Store(ctx, c.Plan(chat("s", "q")), "a", "stop", nil)
	now = now.Add(2 * time.Minute)
	if r, _ := c.Lookup(ctx, c.Plan(chat("s", "q"))); r.Kind != Miss {
		t.Fatalf("expired entry returned: %v", r.Kind)
	}
}

func TestBucketCap(t *testing.T) {
	s := NewMemoryStore(10)
	ctx := context.Background()
	for i := range 5 {
		_ = s.AddCandidate(ctx, "b", &Entry{Content: string(rune('a' + i))}, time.Minute, 3)
	}
	got, _ := s.Candidates(ctx, []string{"b"})
	if len(got) != 3 || got[0].Content != "e" {
		t.Fatalf("bucket cap: %d %v", len(got), got[0].Content)
	}
}

func TestMemoryEviction(t *testing.T) {
	s := NewMemoryStore(2)
	ctx := context.Background()
	_ = s.PutExact(ctx, "a", &Entry{}, time.Second)
	_ = s.PutExact(ctx, "b", &Entry{}, time.Minute)
	_ = s.PutExact(ctx, "c", &Entry{}, time.Minute)
	if e, _ := s.GetExact(ctx, "a"); e != nil {
		t.Fatal("soonest-expiring entry should be evicted")
	}
}

func TestVectorJSONRoundTrip(t *testing.T) {
	v := Vector{0.5, -1.25, 3}
	b, _ := json.Marshal(v)
	var out Vector
	if err := json.Unmarshal(b, &out); err != nil || len(out) != 3 || out[1] != -1.25 {
		t.Fatalf("round trip: %v %v", out, err)
	}
}

func TestLSHNeighboursShareBuckets(t *testing.T) {
	e := HashingEmbedder{Dims: 256}
	l := NewLSH(12, 256)
	a, _ := e.Embed(context.Background(), "How do I reverse a list in Python?")
	b, _ := e.Embed(context.Background(), "how do i reverse a list in python")
	sa, sb := l.Signature(a), l.Signature(b)
	found := false
	for _, p := range l.Probes(sa) {
		if p == sb {
			found = true
		}
	}
	if !found {
		t.Fatalf("near-duplicates not within one probe: %012b vs %012b", sa, sb)
	}
}

func TestRemoteEmbedder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"embedding":[3,4]}]}`))
	}))
	defer srv.Close()
	v, err := RemoteEmbedder{URL: srv.URL, Model: "bge"}.Embed(context.Background(), "x")
	if err != nil || v[0] != 0.6 || v[1] != 0.8 {
		t.Fatalf("remote: %v %v", v, err)
	}
}
