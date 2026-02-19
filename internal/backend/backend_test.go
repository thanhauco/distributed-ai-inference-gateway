package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/observability"
	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

func drain(t *testing.T, s Stream) (string, Chunk) {
	t.Helper()
	defer s.Close()
	var sb strings.Builder
	var last Chunk
	for {
		c, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return sb.String(), last
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		sb.WriteString(c.Content)
		if c.FinishReason != "" || c.Usage != nil {
			if c.FinishReason != "" {
				last.FinishReason = c.FinishReason
			}
			if c.Usage != nil {
				last.Usage = c.Usage
			}
		}
	}
}

func TestVLLMStreamAndHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(observability.RequestIDHeader) != "rid-7" {
			t.Errorf("request id not propagated")
		}
		var req openai.ChatCompletionRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if !req.Stream || req.StreamOptions == nil || !req.StreamOptions.IncludeUsage {
			t.Errorf("upstream request must stream with usage: %+v", req)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		stop := "stop"
		for _, piece := range []string{"Hel", "lo"} {
			b, _ := json.Marshal(openai.ChatCompletionChunk{Choices: []openai.ChunkChoice{{Delta: openai.Delta{Content: piece}}}})
			fmt.Fprintf(w, "data: %s\n\n", b)
		}
		b, _ := json.Marshal(openai.ChatCompletionChunk{Choices: []openai.ChunkChoice{{FinishReason: &stop}}})
		fmt.Fprintf(w, "data: %s\n\n", b)
		b, _ = json.Marshal(openai.ChatCompletionChunk{Usage: &openai.Usage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7}})
		fmt.Fprintf(w, ": keep-alive\n\ndata: %s\n\ndata: [DONE]\n\n", b)
	}))
	defer srv.Close()

	a, _ := New(KindVLLM, srv.Client())
	ctx := observability.WithRequestID(context.Background(), "rid-7")
	s, err := a.ChatStream(ctx, srv.URL, &openai.ChatCompletionRequest{Model: "m", Messages: []openai.Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	text, last := drain(t, s)
	if text != "Hello" || last.FinishReason != "stop" || last.Usage == nil || last.Usage.TotalTokens != 7 {
		t.Fatalf("got %q %+v", text, last)
	}
}

func TestStatusErrorClassification(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	a, _ := New(KindOpenAI, srv.Client())
	_, err := a.ChatStream(context.Background(), srv.URL, &openai.ChatCompletionRequest{Model: "m"})
	var se *StatusError
	if !errors.As(err, &se) || !se.Retryable() || !se.WorkerFault() {
		t.Fatalf("expected retryable worker fault, got %v", err)
	}
	if (&StatusError{StatusCode: 400}).Retryable() || (&StatusError{StatusCode: 429}).WorkerFault() {
		t.Fatal("misclassified 4xx")
	}
}

func TestTruncatedStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
	}))
	defer srv.Close()
	a, _ := New(KindOpenAI, srv.Client())
	s, err := a.ChatStream(context.Background(), srv.URL, &openai.ChatCompletionRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recv(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recv(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected unexpected EOF, got %v", err)
	}
}

func TestVLLMProbe(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(openai.ModelList{Data: []openai.Model{{ID: "llama3-8b"}, {ID: "mistral-7b"}}})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `# HELP vllm:num_requests_running x
vllm:num_requests_running{model_name="llama3-8b"} 3
vllm:num_requests_running{model_name="mistral-7b"} 1
vllm:num_requests_waiting{model_name="llama3-8b"} 2
vllm:gpu_cache_usage_perc{model_name="llama3-8b"} 0.42
gpu_utilization 0.8
`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	a, _ := New(KindVLLM, srv.Client())
	res, err := a.Probe(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Healthy || len(res.Models) != 2 || res.Running != 4 || res.Waiting != 2 || res.KVCacheUsage != 0.42 || res.GPUUtil != 0.8 {
		t.Fatalf("bad probe %+v", res)
	}
}

func TestOllamaStreamAndProbe(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/chat", func(w http.ResponseWriter, r *http.Request) {
		var req ollamaRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Options["num_predict"] != float64(16) || req.Options["temperature"] != float64(0) {
			t.Errorf("options not translated: %+v", req.Options)
		}
		fmt.Fprintln(w, `{"message":{"role":"assistant","content":"Hi "},"done":false}`)
		fmt.Fprintln(w, `{"message":{"role":"assistant","content":"there"},"done":false}`)
		fmt.Fprintln(w, `{"message":{"role":"assistant","content":""},"done":true,"done_reason":"length","prompt_eval_count":9,"eval_count":2}`)
	})
	mux.HandleFunc("/api/tags", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"models":[{"name":"phi3"},{"name":"gemma2"}]}`)
	})
	mux.HandleFunc("/api/ps", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"models":[{"name":"phi3"}]}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	a, _ := New(KindOllama, srv.Client())
	temp, maxTok := 0.0, 16
	s, err := a.ChatStream(context.Background(), srv.URL, &openai.ChatCompletionRequest{
		Model: "phi3", Messages: []openai.Message{{Role: "user", Content: "hi"}}, Temperature: &temp, MaxTokens: &maxTok,
	})
	if err != nil {
		t.Fatal(err)
	}
	text, last := drain(t, s)
	if text != "Hi there" || last.FinishReason != "length" || last.Usage.TotalTokens != 11 {
		t.Fatalf("got %q %+v", text, last)
	}
	res, err := a.Probe(context.Background(), srv.URL)
	if err != nil || len(res.Models) != 2 || len(res.Loaded) != 1 || res.Loaded[0] != "phi3" {
		t.Fatalf("probe: %+v %v", res, err)
	}
}

func TestOllamaInStreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"error":"model not loaded"}`)
	}))
	defer srv.Close()
	a, _ := New(KindOllama, srv.Client())
	s, err := a.ChatStream(context.Background(), srv.URL, &openai.ChatCompletionRequest{Model: "x"})
	if err != nil {
		t.Fatal(err)
	}
	var se *StatusError
	if _, err := s.Recv(); !errors.As(err, &se) {
		t.Fatalf("expected status error, got %v", err)
	}
}

func TestParsePromText(t *testing.T) {
	m := ParsePromText(strings.NewReader("a 1\na{x=\"}\"} 2\n# c 3\nb 4.5 1700000000\nbad\n"))
	if m["a"] != 3 || m["b"] != 4.5 || len(m) != 2 {
		t.Fatalf("got %v", m)
	}
}
