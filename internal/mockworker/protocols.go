package mockworker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// chat serves vLLM's OpenAI-compatible endpoint.
func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	var req openai.ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	g, status, err := s.prepare(r.Context(), &req)
	if err != nil {
		if status != 499 {
			http.Error(w, fmt.Sprintf(`{"object":"error","message":%q}`, err.Error()), status)
		}
		return
	}
	defer s.finish()

	id := fmt.Sprintf("cmpl-%s-%d", s.cfg.ID, time.Now().UnixNano())
	created := time.Now().Unix()
	usage := &openai.Usage{PromptTokens: g.promptTokens, CompletionTokens: len(g.words), TotalTokens: g.promptTokens + len(g.words)}

	if !req.Stream {
		var sb strings.Builder
		for _, word := range g.words {
			if s.sleep(r.Context(), s.decodeDelay()) != nil {
				return
			}
			sb.WriteString(word)
			s.generated.Add(1)
		}
		writeJSON(w, openai.ChatCompletionResponse{
			ID: id, Object: "chat.completion", Created: created, Model: req.Model, Usage: usage,
			Choices: []openai.Choice{{Message: openai.Message{Role: "assistant", Content: sb.String()}, FinishReason: g.finish}},
		})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Worker-ID", s.cfg.ID)
	flusher, _ := w.(http.Flusher)
	send := func(c openai.ChatCompletionChunk) {
		c.ID, c.Object, c.Created, c.Model = id, "chat.completion.chunk", created, req.Model
		b, _ := json.Marshal(c)
		fmt.Fprintf(w, "data: %s\n\n", b)
		if flusher != nil {
			flusher.Flush()
		}
	}
	send(openai.ChatCompletionChunk{Choices: []openai.ChunkChoice{{Delta: openai.Delta{Role: "assistant"}}}})
	for i, word := range g.words {
		if i > 0 && s.sleep(r.Context(), s.decodeDelay()) != nil {
			return
		}
		s.generated.Add(1)
		send(openai.ChatCompletionChunk{Choices: []openai.ChunkChoice{{Delta: openai.Delta{Content: word}}}})
	}
	fr := g.finish
	send(openai.ChatCompletionChunk{Choices: []openai.ChunkChoice{{FinishReason: &fr}}})
	if req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
		send(openai.ChatCompletionChunk{Choices: []openai.ChunkChoice{}, Usage: usage})
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}

type ollamaMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ollamaChat serves Ollama's native NDJSON chat endpoint.
func (s *Server) ollamaChat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Model    string         `json:"model"`
		Messages []ollamaMsg    `json:"messages"`
		Stream   *bool          `json:"stream"`
		Options  map[string]any `json:"options"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := &openai.ChatCompletionRequest{Model: body.Model}
	for _, m := range body.Messages {
		req.Messages = append(req.Messages, openai.Message{Role: m.Role, Content: m.Content})
	}
	if v, ok := body.Options["num_predict"].(float64); ok {
		n := int(v)
		req.MaxTokens = &n
	}
	g, status, err := s.prepare(r.Context(), req)
	if err != nil {
		if status != 499 {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		}
		return
	}
	defer s.finish()

	w.Header().Set("Content-Type", "application/x-ndjson")
	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	for i, word := range g.words {
		if i > 0 && s.sleep(r.Context(), s.decodeDelay()) != nil {
			return
		}
		s.generated.Add(1)
		_ = enc.Encode(map[string]any{"model": body.Model, "message": ollamaMsg{"assistant", word}, "done": false})
		if flusher != nil {
			flusher.Flush()
		}
	}
	_ = enc.Encode(map[string]any{
		"model": body.Model, "message": ollamaMsg{"assistant", ""}, "done": true, "done_reason": g.finish,
		"prompt_eval_count": g.promptTokens, "eval_count": len(g.words),
	})
}

func (s *Server) ollamaTags(w http.ResponseWriter, _ *http.Request) {
	if !s.healthy.Load() {
		http.Error(w, "unhealthy", http.StatusServiceUnavailable)
		return
	}
	var models []map[string]any
	for _, m := range s.cfg.Models {
		models = append(models, map[string]any{"name": m, "model": m})
	}
	writeJSON(w, map[string]any{"models": models})
}

func (s *Server) ollamaPS(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	var models []map[string]any
	for e := s.loaded.Front(); e != nil; e = e.Next() {
		models = append(models, map[string]any{"name": e.Value.(string), "model": e.Value.(string)})
	}
	s.mu.Unlock()
	writeJSON(w, map[string]any{"models": models})
}
