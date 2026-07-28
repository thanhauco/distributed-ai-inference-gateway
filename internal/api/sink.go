package api

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/gateway"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/ratelimit"
	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// setMetaHeaders exposes routing decisions for debugging and load tests.
func setMetaHeaders(h http.Header, m gateway.Meta) {
	if m.Worker != "" {
		h.Set("X-Gateway-Worker", m.Worker)
	}
	if m.Route != "" {
		h.Set("X-Gateway-Route", string(m.Route))
	}
	if m.Cache != "" {
		h.Set("X-Gateway-Cache", string(m.Cache))
	}
	if m.Attempts > 0 {
		h.Set("X-Gateway-Attempts", strconv.Itoa(m.Attempts))
	}
	if m.Coalesced {
		h.Set("X-Gateway-Coalesced", "true")
	}
	if m.RateLimit != nil {
		setRateLimitHeaders(h, *m.RateLimit)
	}
}

// setRateLimitHeaders uses OpenAI's header names so existing client retry
// logic works unchanged.
func setRateLimitHeaders(h http.Header, d ratelimit.Decision) {
	if d.LimitRequests > 0 {
		h.Set("X-Ratelimit-Limit-Requests", strconv.FormatInt(d.LimitRequests, 10))
		h.Set("X-Ratelimit-Remaining-Requests", strconv.FormatInt(d.RemainingRequests, 10))
	}
	if d.LimitTokens > 0 {
		h.Set("X-Ratelimit-Limit-Tokens", strconv.FormatInt(d.LimitTokens, 10))
		h.Set("X-Ratelimit-Remaining-Tokens", strconv.FormatInt(d.RemainingTokens, 10))
	}
	if !d.Allowed && d.RetryAfter > 0 {
		suffix := map[string]string{"requests": "Requests", "tokens": "Tokens"}[d.Reason]
		if suffix != "" {
			h.Set("X-Ratelimit-Reset-"+suffix, d.RetryAfter.Round(time.Millisecond).String())
		}
	}
}

func retryAfterSeconds(d time.Duration) string {
	return strconv.Itoa(int(math.Max(1, math.Ceil(d.Seconds()))))
}

// sseSink writes OpenAI-style chat.completion.chunk events.
type sseSink struct {
	c            *gin.Context
	id           string
	model        string
	created      int64
	includeUsage bool
	started      bool
}

func (s *sseSink) Start(meta gateway.Meta) error {
	s.started = true
	h := s.c.Writer.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
	setMetaHeaders(h, meta)
	s.model = meta.Model
	s.c.Status(http.StatusOK)
	return s.write(openai.ChatCompletionChunk{Choices: []openai.ChunkChoice{{Delta: openai.Delta{Role: "assistant"}}}})
}

func (s *sseSink) Chunk(c backend.Chunk) error {
	if c.Content == "" && c.FinishReason == "" {
		return nil
	}
	choice := openai.ChunkChoice{Delta: openai.Delta{Content: c.Content}}
	if c.FinishReason != "" {
		fr := c.FinishReason
		choice.FinishReason = &fr
	}
	return s.write(openai.ChatCompletionChunk{Choices: []openai.ChunkChoice{choice}})
}

func (s *sseSink) Finish(u openai.Usage) error {
	if s.includeUsage {
		if err := s.write(openai.ChatCompletionChunk{Choices: []openai.ChunkChoice{}, Usage: &u}); err != nil {
			return err
		}
	}
	return s.raw("[DONE]")
}

func (s *sseSink) write(chunk openai.ChatCompletionChunk) error {
	chunk.ID, chunk.Object, chunk.Created, chunk.Model = s.id, "chat.completion.chunk", s.created, s.model
	b, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	return s.raw(string(b))
}

func (s *sseSink) raw(data string) error {
	if _, err := fmt.Fprintf(s.c.Writer, "data: %s\n\n", data); err != nil {
		return err
	}
	s.c.Writer.Flush()
	return nil
}

// writeError emits an error event on an already-started stream.
func (s *sseSink) writeError(e *gateway.Error) {
	b, _ := json.Marshal(errorBody(e))
	_ = s.raw(string(b))
	_ = s.raw("[DONE]")
}

// jsonSink buffers a non-streaming response.
type jsonSink struct {
	meta    gateway.Meta
	content strings.Builder
	finish  string
	usage   openai.Usage
}

func (s *jsonSink) Start(meta gateway.Meta) error { s.meta = meta; return nil }

func (s *jsonSink) Chunk(c backend.Chunk) error {
	s.content.WriteString(c.Content)
	if c.FinishReason != "" {
		s.finish = c.FinishReason
	}
	return nil
}

func (s *jsonSink) Finish(u openai.Usage) error { s.usage = u; return nil }

func (s *jsonSink) response(id string, created int64) openai.ChatCompletionResponse {
	finish := s.finish
	if finish == "" {
		finish = "stop"
	}
	u := s.usage
	return openai.ChatCompletionResponse{
		ID: id, Object: "chat.completion", Created: created, Model: s.meta.Model,
		Choices: []openai.Choice{{Index: 0, Message: openai.Message{Role: "assistant", Content: s.content.String()}, FinishReason: finish}},
		Usage:   &u,
	}
}

func errorBody(e *gateway.Error) openai.ErrorBody {
	return openai.ErrorBody{Error: openai.ErrorDetail{Message: e.Message, Type: e.Type, Code: e.Code}}
}
