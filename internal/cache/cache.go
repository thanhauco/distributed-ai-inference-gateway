// Package cache implements the gateway's response cache: an exact-match tier
// keyed by a hash of the full request, and a semantic tier that matches the
// final user message by embedding similarity within an otherwise identical
// conversation and parameter set.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// Entry is a cached completion.
type Entry struct {
	Model        string        `json:"model"`
	Query        string        `json:"query"`
	Content      string        `json:"content"`
	FinishReason string        `json:"finish_reason"`
	Usage        *openai.Usage `json:"usage,omitempty"`
	Embedding    Vector        `json:"embedding,omitempty"`
	CreatedAt    time.Time     `json:"created_at"`
}

// Vector is a float32 slice serialized as base64 little-endian bytes, which
// is ~3× smaller than a JSON number array.
type Vector []float32

// MarshalJSON implements json.Marshaler.
func (v Vector) MarshalJSON() ([]byte, error) {
	b := make([]byte, 4*len(v))
	for i, x := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(x))
	}
	return json.Marshal(base64.StdEncoding.EncodeToString(b))
}

// UnmarshalJSON implements json.Unmarshaler.
func (v *Vector) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	if len(b)%4 != 0 {
		return fmt.Errorf("cache: vector length %d is not a multiple of 4", len(b))
	}
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	*v = out
	return nil
}

// Store persists entries.
type Store interface {
	GetExact(ctx context.Context, key string) (*Entry, error)
	PutExact(ctx context.Context, key string, e *Entry, ttl time.Duration) error
	// Candidates returns the entries of every bucket listed.
	Candidates(ctx context.Context, buckets []string) ([]*Entry, error)
	// AddCandidate prepends e to bucket, keeping at most maxEntries.
	AddCandidate(ctx context.Context, bucket string, e *Entry, ttl time.Duration, maxEntries int) error
}

// Config configures a Cache.
type Config struct {
	TTL                 time.Duration
	Semantic            bool
	SimilarityThreshold float64
	MaxBucketEntries    int
	LSHBits             int
	EmbeddingDims       int
}

// Kind classifies a lookup result.
type Kind string

// Lookup kinds.
const (
	Miss         Kind = "miss"
	HitExact     Kind = "hit_exact"
	HitSemantic  Kind = "hit_semantic"
	Bypass       Kind = "bypass"
	LookupFailed Kind = "error"
)

// Result is the outcome of Lookup.
type Result struct {
	Kind       Kind
	Entry      *Entry
	Similarity float64
}

// Cache combines the exact and semantic tiers.
type Cache struct {
	store    Store
	embedder Embedder
	lsh      *LSH
	cfg      Config
	now      func() time.Time
}

// New returns a Cache.
func New(store Store, embedder Embedder, cfg Config) *Cache {
	if cfg.LSHBits <= 0 {
		cfg.LSHBits = 12
	}
	if cfg.EmbeddingDims <= 0 {
		cfg.EmbeddingDims = 256
	}
	if cfg.MaxBucketEntries <= 0 {
		cfg.MaxBucketEntries = 64
	}
	if embedder == nil {
		embedder = HashingEmbedder{Dims: cfg.EmbeddingDims}
	}
	return &Cache{store: store, embedder: embedder, lsh: NewLSH(cfg.LSHBits, cfg.EmbeddingDims), cfg: cfg, now: time.Now}
}

// Plan holds the keys derived from one request so lookup and store do not
// recompute hashes or embeddings.
type Plan struct {
	Model     string
	Namespace string // model + params + conversation before the final user turn
	ExactKey  string
	Query     string // final user message
	semantic  bool
	vec       []float32
	embedErr  error
	embedded  bool
}

// Key returns the exact-match key; also used for request coalescing.
func (p *Plan) Key() string { return p.ExactKey }

// Plan derives cache keys for req.
func (c *Cache) Plan(req *openai.ChatCompletionRequest) *Plan {
	ns := sha256.New()
	write := func(parts ...string) {
		for _, s := range parts {
			ns.Write([]byte(s))
			ns.Write([]byte{0})
		}
	}
	f := func(p *float64) string {
		if p == nil {
			return "-"
		}
		return strconv.FormatFloat(*p, 'g', -1, 64)
	}
	maxTok := "-"
	if req.MaxTokens != nil {
		maxTok = strconv.Itoa(*req.MaxTokens)
	}
	seed := "-"
	if req.Seed != nil {
		seed = strconv.FormatInt(*req.Seed, 10)
	}
	write("v1", req.Model, f(req.Temperature), f(req.TopP), maxTok, seed, f(req.PresencePenalty), f(req.FrequencyPenalty), strings.Join(req.Stop, "\x01"))

	last := req.LastUserMessage()
	semantic := c.cfg.Semantic && last == len(req.Messages)-1
	for i, m := range req.Messages {
		if semantic && i == last {
			break
		}
		write(m.Role, m.Name, m.Content)
	}
	namespace := hex.EncodeToString(ns.Sum(nil))
	query := ""
	if last >= 0 {
		query = req.Messages[last].Content
	}
	ex := sha256.Sum256([]byte(namespace + "\x00" + strings.TrimSpace(query)))
	return &Plan{
		Model: req.Model, Namespace: namespace, ExactKey: hex.EncodeToString(ex[:]),
		Query: query, semantic: semantic,
	}
}

func (c *Cache) embed(ctx context.Context, p *Plan) ([]float32, error) {
	if !p.embedded {
		p.vec, p.embedErr = c.embedder.Embed(ctx, p.Query)
		p.embedded = true
	}
	return p.vec, p.embedErr
}

func (c *Cache) buckets(p *Plan, vec []float32, all bool) []string {
	sig := c.lsh.Signature(vec)
	sigs := []uint32{sig}
	if all {
		sigs = c.lsh.Probes(sig)
	}
	out := make([]string, len(sigs))
	for i, s := range sigs {
		out[i] = fmt.Sprintf("%s:%08x", p.Namespace, s)
	}
	return out
}

// Lookup checks the exact tier, then the semantic tier.
func (c *Cache) Lookup(ctx context.Context, p *Plan) (Result, error) {
	e, err := c.store.GetExact(ctx, p.ExactKey)
	if err != nil {
		return Result{Kind: LookupFailed}, err
	}
	if e != nil && c.fresh(e) {
		return Result{Kind: HitExact, Entry: e, Similarity: 1}, nil
	}
	if !p.semantic {
		return Result{Kind: Miss}, nil
	}
	vec, err := c.embed(ctx, p)
	if err != nil {
		return Result{Kind: LookupFailed}, err
	}
	cands, err := c.store.Candidates(ctx, c.buckets(p, vec, true))
	if err != nil {
		return Result{Kind: LookupFailed}, err
	}
	var best *Entry
	bestSim := -1.0
	for _, cand := range cands {
		if !c.fresh(cand) || cand.Model != p.Model {
			continue
		}
		if s := Cosine(vec, cand.Embedding); s > bestSim {
			best, bestSim = cand, s
		}
	}
	if best != nil && bestSim >= c.cfg.SimilarityThreshold {
		return Result{Kind: HitSemantic, Entry: best, Similarity: bestSim}, nil
	}
	return Result{Kind: Miss, Similarity: max(bestSim, 0)}, nil
}

// Store saves a completed response under the plan's keys.
func (c *Cache) Store(ctx context.Context, p *Plan, content, finishReason string, usage *openai.Usage) error {
	e := &Entry{Model: p.Model, Query: p.Query, Content: content, FinishReason: finishReason, Usage: usage, CreatedAt: c.now().UTC()}
	if err := c.store.PutExact(ctx, p.ExactKey, e, c.cfg.TTL); err != nil {
		return err
	}
	if !p.semantic {
		return nil
	}
	vec, err := c.embed(ctx, p)
	if err != nil {
		return err
	}
	withVec := *e
	withVec.Embedding = vec
	return c.store.AddCandidate(ctx, c.buckets(p, vec, false)[0], &withVec, c.cfg.TTL, c.cfg.MaxBucketEntries)
}

func (c *Cache) fresh(e *Entry) bool {
	return c.cfg.TTL <= 0 || c.now().Sub(e.CreatedAt) < c.cfg.TTL
}
