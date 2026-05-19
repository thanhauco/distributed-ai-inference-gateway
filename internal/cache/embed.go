package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"net/http"
	"strings"
	"unicode"
)

// Embedder turns text into a unit-length vector.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// HashingEmbedder is a dependency-free embedder based on feature hashing of
// words, word bigrams and character trigrams. It captures lexical similarity
// (rephrasings, casing, punctuation, small typos) rather than deep semantics,
// which is a conservative default for a response cache. Point the cache at a
// real embedding model via RemoteEmbedder for paraphrase-level matching.
type HashingEmbedder struct{ Dims int }

// Embed implements Embedder.
func (e HashingEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	dims := e.Dims
	if dims <= 0 {
		dims = 256
	}
	v := make([]float32, dims)
	add := func(feature string, weight float32) {
		h := fnv.New64a()
		_, _ = h.Write([]byte(feature))
		x := h.Sum64()
		idx := int(x % uint64(dims))
		if x&(1<<63) != 0 {
			weight = -weight
		}
		v[idx] += weight
	}
	words := tokenize(text)
	for i, w := range words {
		add("w:"+w, 1.0)
		if i > 0 {
			add("b:"+words[i-1]+" "+w, 0.7)
		}
		padded := "^" + w + "$"
		runes := []rune(padded)
		for j := 0; j+3 <= len(runes); j++ {
			add("c:"+string(runes[j:j+3]), 0.25)
		}
	}
	normalize(v)
	return v, nil
}

func tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

func normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}

// Cosine returns the cosine similarity of two unit vectors (their dot
// product); vectors of different dimension score 0.
func Cosine(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return dot
}

// RemoteEmbedder calls an OpenAI-compatible /v1/embeddings endpoint (vLLM
// with an embedding model, TEI, Ollama's OpenAI shim, ...).
type RemoteEmbedder struct {
	URL    string
	Model  string
	Client *http.Client
}

// Embed implements Embedder.
func (e RemoteEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	body, _ := json.Marshal(map[string]any{"model": e.Model, "input": text})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(e.URL, "/")+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := e.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embeddings endpoint returned %d", resp.StatusCode)
	}
	var out struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Data) == 0 {
		return nil, fmt.Errorf("embeddings endpoint returned no data")
	}
	v := out.Data[0].Embedding
	normalize(v)
	return v, nil
}
