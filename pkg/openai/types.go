// Package openai defines the OpenAI-compatible wire types the gateway accepts
// and emits, plus request validation, normalization and token estimation.
package openai

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Message is a single chat message. Content accepts either a plain string or
// the array-of-parts form; text parts are flattened into Content.
type Message struct {
	Role       string `json:"role"`
	Content    string `json:"content"`
	Name       string `json:"name,omitempty"`
	ToolCallID string `json:"tool_call_id,omitempty"`
}

type contentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// UnmarshalJSON accepts string or array content.
func (m *Message) UnmarshalJSON(b []byte) error {
	var raw struct {
		Role       string          `json:"role"`
		Content    json.RawMessage `json:"content"`
		Name       string          `json:"name"`
		ToolCallID string          `json:"tool_call_id"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	m.Role, m.Name, m.ToolCallID = raw.Role, raw.Name, raw.ToolCallID
	m.Content = ""
	if len(raw.Content) == 0 || string(raw.Content) == "null" {
		return nil
	}
	if raw.Content[0] == '"' {
		return json.Unmarshal(raw.Content, &m.Content)
	}
	var parts []contentPart
	if err := json.Unmarshal(raw.Content, &parts); err != nil {
		return fmt.Errorf("content must be a string or an array of parts: %w", err)
	}
	var sb strings.Builder
	for _, p := range parts {
		if p.Type != "text" {
			return &RequestError{Param: "messages.content", Message: fmt.Sprintf("content part type %q is not supported", p.Type)}
		}
		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(p.Text)
	}
	m.Content = sb.String()
	return nil
}

// StopList accepts either a single string or an array of strings.
type StopList []string

// UnmarshalJSON accepts string or array.
func (s *StopList) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*s = nil
		return nil
	}
	if len(b) > 0 && b[0] == '"' {
		var one string
		if err := json.Unmarshal(b, &one); err != nil {
			return err
		}
		*s = StopList{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return err
	}
	*s = many
	return nil
}

// StreamOptions mirrors OpenAI's stream_options.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// ChatCompletionRequest is the body of POST /v1/chat/completions.
type ChatCompletionRequest struct {
	Model               string         `json:"model"`
	Messages            []Message      `json:"messages"`
	Stream              bool           `json:"stream,omitempty"`
	StreamOptions       *StreamOptions `json:"stream_options,omitempty"`
	Temperature         *float64       `json:"temperature,omitempty"`
	TopP                *float64       `json:"top_p,omitempty"`
	MaxTokens           *int           `json:"max_tokens,omitempty"`
	MaxCompletionTokens *int           `json:"max_completion_tokens,omitempty"`
	N                   *int           `json:"n,omitempty"`
	Stop                StopList       `json:"stop,omitempty"`
	PresencePenalty     *float64       `json:"presence_penalty,omitempty"`
	FrequencyPenalty    *float64       `json:"frequency_penalty,omitempty"`
	Seed                *int64         `json:"seed,omitempty"`
	User                string         `json:"user,omitempty"`
}

// Usage reports token accounting.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Choice is a non-streaming completion choice.
type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

// ChatCompletionResponse is the non-streaming response body.
type ChatCompletionResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

// Delta is the incremental message in a streaming chunk.
type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// ChunkChoice is a streaming choice.
type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

// ChatCompletionChunk is one SSE event of a streaming response.
type ChatCompletionChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`
}

// Model is an entry of GET /v1/models.
type Model struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// ModelList is the body of GET /v1/models.
type ModelList struct {
	Object string  `json:"object"`
	Data   []Model `json:"data"`
}

// ErrorBody is the OpenAI error envelope.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes an API error.
type ErrorDetail struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    string  `json:"code,omitempty"`
}

// RequestError is a client error detected during validation.
type RequestError struct {
	Param   string
	Message string
}

func (e *RequestError) Error() string {
	if e.Param == "" {
		return e.Message
	}
	return e.Param + ": " + e.Message
}

// ErrInvalidRequest is wrapped by validation failures.
var ErrInvalidRequest = errors.New("invalid request")

var validRoles = map[string]bool{"system": true, "user": true, "assistant": true, "tool": true}

// Normalize canonicalises a request in place: lower-cases roles, maps the
// "developer" role to "system", folds max_completion_tokens into max_tokens
// and trims surrounding whitespace from the model name.
func (r *ChatCompletionRequest) Normalize() {
	r.Model = strings.TrimSpace(r.Model)
	for i := range r.Messages {
		role := strings.ToLower(strings.TrimSpace(r.Messages[i].Role))
		if role == "developer" {
			role = "system"
		}
		r.Messages[i].Role = role
	}
	if r.MaxTokens == nil && r.MaxCompletionTokens != nil {
		v := *r.MaxCompletionTokens
		r.MaxTokens = &v
	}
	r.MaxCompletionTokens = nil
}

// Validate checks the request against the subset of the OpenAI schema the
// gateway supports. Call Normalize first.
func (r *ChatCompletionRequest) Validate() error {
	if r.Model == "" {
		return &RequestError{Param: "model", Message: "is required"}
	}
	if len(r.Messages) == 0 {
		return &RequestError{Param: "messages", Message: "must contain at least one message"}
	}
	for i, m := range r.Messages {
		if !validRoles[m.Role] {
			return &RequestError{Param: fmt.Sprintf("messages[%d].role", i), Message: fmt.Sprintf("unsupported role %q", m.Role)}
		}
	}
	if r.Temperature != nil && (*r.Temperature < 0 || *r.Temperature > 2) {
		return &RequestError{Param: "temperature", Message: "must be between 0 and 2"}
	}
	if r.TopP != nil && (*r.TopP <= 0 || *r.TopP > 1) {
		return &RequestError{Param: "top_p", Message: "must be in (0, 1]"}
	}
	if r.MaxTokens != nil && *r.MaxTokens < 1 {
		return &RequestError{Param: "max_tokens", Message: "must be >= 1"}
	}
	if r.N != nil && *r.N != 1 {
		return &RequestError{Param: "n", Message: "only n=1 is supported"}
	}
	if len(r.Stop) > 4 {
		return &RequestError{Param: "stop", Message: "at most 4 stop sequences are allowed"}
	}
	return nil
}

// LastUserMessage returns the index of the final user message or -1.
func (r *ChatCompletionRequest) LastUserMessage() int {
	for i := len(r.Messages) - 1; i >= 0; i-- {
		if r.Messages[i].Role == "user" {
			return i
		}
	}
	return -1
}

// Clone returns a deep copy of the request.
func (r *ChatCompletionRequest) Clone() *ChatCompletionRequest {
	c := *r
	c.Messages = append([]Message(nil), r.Messages...)
	c.Stop = append(StopList(nil), r.Stop...)
	if r.StreamOptions != nil {
		so := *r.StreamOptions
		c.StreamOptions = &so
	}
	return &c
}
