// Package backend speaks the wire protocols of the model workers. Every
// adapter streams from the worker, even for non-streaming client requests, so
// the gateway has a single code path and can measure time-to-first-token.
package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/observability"
	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// Kind identifies a worker protocol.
type Kind string

// Supported worker kinds.
const (
	KindVLLM   Kind = "vllm"   // vLLM OpenAI-compatible server (+ Prometheus metrics)
	KindOllama Kind = "ollama" // Ollama native API
	KindOpenAI Kind = "openai" // any other OpenAI-compatible server (SGLang, TGI, llama.cpp)
)

// Chunk is a normalized piece of a streamed completion.
type Chunk struct {
	Content      string
	FinishReason string
	Usage        *openai.Usage
}

// Stream yields chunks until io.EOF.
type Stream interface {
	Recv() (Chunk, error)
	Close() error
}

// ProbeResult is a point-in-time view of a worker.
type ProbeResult struct {
	Healthy      bool
	Models       []string // models the worker can serve
	Loaded       []string // models resident in GPU memory (warm); nil means all
	Running      int      // sequences currently decoding
	Waiting      int      // sequences queued on the worker
	GPUUtil      float64  // 0..1, if the worker exports it
	KVCacheUsage float64  // 0..1 fraction of KV cache blocks in use
	PrefixHit    float64  // 0..1 prefix cache hit rate, if exported
}

// Adapter is a worker protocol client.
type Adapter interface {
	Kind() Kind
	ChatStream(ctx context.Context, baseURL string, req *openai.ChatCompletionRequest) (Stream, error)
	Probe(ctx context.Context, baseURL string) (ProbeResult, error)
}

// New returns the adapter for kind.
func New(kind Kind, client *http.Client) (Adapter, error) {
	if client == nil {
		client = http.DefaultClient
	}
	switch kind {
	case KindVLLM:
		return &openAIAdapter{client: client, kind: KindVLLM, scrapeMetrics: true}, nil
	case KindOpenAI:
		return &openAIAdapter{client: client, kind: KindOpenAI}, nil
	case KindOllama:
		return &ollamaAdapter{client: client}, nil
	default:
		return nil, fmt.Errorf("backend: unknown worker kind %q", kind)
	}
}

// StatusError is a non-2xx response from a worker.
type StatusError struct {
	StatusCode int
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("worker returned HTTP %d: %s", e.StatusCode, e.Body)
}

// Retryable reports whether another worker might succeed: server errors and
// overload signals are retryable, other client errors are not.
func (e *StatusError) Retryable() bool {
	return e.StatusCode >= 500 || e.StatusCode == http.StatusTooManyRequests || e.StatusCode == http.StatusRequestTimeout
}

// WorkerFault reports whether the error should count against the worker's
// circuit breaker.
func (e *StatusError) WorkerFault() bool { return e.StatusCode >= 500 }

// ErrStreamProtocol marks malformed stream data.
var ErrStreamProtocol = errors.New("backend: malformed stream")

func newRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if id := observability.RequestIDFrom(ctx); id != "" {
		req.Header.Set(observability.RequestIDHeader, id)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	return req, nil
}

func checkStatus(resp *http.Response) error {
	if resp.StatusCode/100 == 2 {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	return &StatusError{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(b))}
}

func joinURL(base, path string) string {
	return strings.TrimRight(base, "/") + path
}
