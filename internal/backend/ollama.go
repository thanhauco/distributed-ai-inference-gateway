package backend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// ollamaAdapter translates OpenAI chat requests to Ollama's native /api/chat
// NDJSON stream.
type ollamaAdapter struct{ client *http.Client }

func (a *ollamaAdapter) Kind() Kind { return KindOllama }

type ollamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaRequest struct {
	Model    string          `json:"model"`
	Messages []ollamaMessage `json:"messages"`
	Stream   bool            `json:"stream"`
	Options  map[string]any  `json:"options,omitempty"`
}

type ollamaChunk struct {
	Model           string        `json:"model"`
	Message         ollamaMessage `json:"message"`
	Done            bool          `json:"done"`
	DoneReason      string        `json:"done_reason"`
	PromptEvalCount int           `json:"prompt_eval_count"`
	EvalCount       int           `json:"eval_count"`
	Error           string        `json:"error"`
}

// ToOllamaRequest converts an OpenAI request into Ollama's format.
func ToOllamaRequest(req *openai.ChatCompletionRequest) ollamaRequest {
	out := ollamaRequest{Model: req.Model, Stream: true, Options: map[string]any{}}
	for _, m := range req.Messages {
		out.Messages = append(out.Messages, ollamaMessage{Role: m.Role, Content: m.Content})
	}
	if req.Temperature != nil {
		out.Options["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		out.Options["top_p"] = *req.TopP
	}
	if req.MaxTokens != nil {
		out.Options["num_predict"] = *req.MaxTokens
	}
	if req.Seed != nil {
		out.Options["seed"] = *req.Seed
	}
	if req.PresencePenalty != nil {
		out.Options["presence_penalty"] = *req.PresencePenalty
	}
	if req.FrequencyPenalty != nil {
		out.Options["frequency_penalty"] = *req.FrequencyPenalty
	}
	if len(req.Stop) > 0 {
		out.Options["stop"] = []string(req.Stop)
	}
	if len(out.Options) == 0 {
		out.Options = nil
	}
	return out
}

func (a *ollamaAdapter) ChatStream(ctx context.Context, baseURL string, req *openai.ChatCompletionRequest) (Stream, error) {
	body, err := json.Marshal(ToOllamaRequest(req))
	if err != nil {
		return nil, err
	}
	httpReq, err := newRequest(ctx, http.MethodPost, joinURL(baseURL, "/api/chat"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if err := checkStatus(resp); err != nil {
		return nil, err
	}
	return &ndjsonStream{body: resp.Body, sc: newScanner(resp.Body)}, nil
}

type ndjsonStream struct {
	body io.ReadCloser
	sc   *bufio.Scanner
	done bool
}

func (s *ndjsonStream) Recv() (Chunk, error) {
	if s.done {
		return Chunk{}, io.EOF
	}
	for s.sc.Scan() {
		line := bytes.TrimSpace(s.sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var c ollamaChunk
		if err := json.Unmarshal(line, &c); err != nil {
			return Chunk{}, fmt.Errorf("%w: %v", ErrStreamProtocol, err)
		}
		if c.Error != "" {
			return Chunk{}, &StatusError{StatusCode: http.StatusBadGateway, Body: c.Error}
		}
		out := Chunk{Content: c.Message.Content}
		if c.Done {
			s.done = true
			out.FinishReason = mapDoneReason(c.DoneReason)
			out.Usage = &openai.Usage{
				PromptTokens:     c.PromptEvalCount,
				CompletionTokens: c.EvalCount,
				TotalTokens:      c.PromptEvalCount + c.EvalCount,
			}
		}
		return out, nil
	}
	if err := s.sc.Err(); err != nil {
		return Chunk{}, err
	}
	return Chunk{}, io.ErrUnexpectedEOF
}

func (s *ndjsonStream) Close() error { return s.body.Close() }

func mapDoneReason(r string) string {
	switch r {
	case "length":
		return "length"
	default:
		return "stop"
	}
}

func (a *ollamaAdapter) Probe(ctx context.Context, baseURL string) (ProbeResult, error) {
	var res ProbeResult
	var tags struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := a.getJSON(ctx, joinURL(baseURL, "/api/tags"), &tags); err != nil {
		return res, err
	}
	for _, m := range tags.Models {
		res.Models = append(res.Models, m.Name)
	}
	var ps struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	// /api/ps lists models resident in VRAM. Routing to a worker that already
	// holds the model avoids a multi-second cold load.
	if err := a.getJSON(ctx, joinURL(baseURL, "/api/ps"), &ps); err == nil {
		for _, m := range ps.Models {
			res.Loaded = append(res.Loaded, m.Name)
		}
	} else {
		res.Loaded = res.Models
	}
	res.Healthy = true
	return res, nil
}

func (a *ollamaAdapter) getJSON(ctx context.Context, url string, into any) error {
	req, err := newRequest(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	if err := checkStatus(resp); err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(into)
}
