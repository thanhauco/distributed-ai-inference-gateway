package openai

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestMessageContentForms(t *testing.T) {
	var req ChatCompletionRequest
	body := `{"model":"m","messages":[
		{"role":"system","content":"be terse"},
		{"role":"user","content":[{"type":"text","text":"hello"},{"type":"text","text":"world"}]}
	],"stop":"END","max_completion_tokens":32}`
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	if req.Messages[1].Content != "hello\nworld" {
		t.Fatalf("parts not flattened: %q", req.Messages[1].Content)
	}
	if len(req.Stop) != 1 || req.Stop[0] != "END" {
		t.Fatalf("stop not parsed: %v", req.Stop)
	}
	req.Normalize()
	if req.MaxTokens == nil || *req.MaxTokens != 32 {
		t.Fatalf("max_completion_tokens not folded")
	}
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestImagePartsRejected(t *testing.T) {
	var req ChatCompletionRequest
	err := json.Unmarshal([]byte(`{"model":"m","messages":[{"role":"user","content":[{"type":"image_url"}]}]}`), &req)
	var re *RequestError
	if !errors.As(err, &re) {
		t.Fatalf("expected RequestError, got %v", err)
	}
}

func TestValidate(t *testing.T) {
	temp := 3.0
	n := 2
	cases := map[string]*ChatCompletionRequest{
		"no model":    {Messages: []Message{{Role: "user"}}},
		"no messages": {Model: "m"},
		"bad role":    {Model: "m", Messages: []Message{{Role: "wizard"}}},
		"bad temp":    {Model: "m", Messages: []Message{{Role: "user"}}, Temperature: &temp},
		"n>1":         {Model: "m", Messages: []Message{{Role: "user"}}, N: &n},
	}
	for name, req := range cases {
		req.Normalize()
		if err := req.Validate(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestDeveloperRoleMapsToSystem(t *testing.T) {
	r := &ChatCompletionRequest{Model: " m ", Messages: []Message{{Role: "Developer", Content: "x"}}}
	r.Normalize()
	if r.Messages[0].Role != "system" || r.Model != "m" {
		t.Fatalf("normalize failed: %+v", r)
	}
}

func TestEstimateTokens(t *testing.T) {
	if EstimateTokens("") != 0 {
		t.Fatal("empty should be 0")
	}
	got := EstimateTokens("The quick brown fox jumps over the lazy dog.")
	if got < 9 || got > 16 {
		t.Fatalf("estimate out of range: %d", got)
	}
	req := &ChatCompletionRequest{Messages: []Message{{Role: "user", Content: "hi"}}}
	if PromptTokens(req) < 5 {
		t.Fatalf("prompt tokens too small")
	}
}

func TestCloneIsDeep(t *testing.T) {
	r := &ChatCompletionRequest{Model: "m", Messages: []Message{{Role: "user", Content: "a"}}, StreamOptions: &StreamOptions{}}
	c := r.Clone()
	c.Messages[0].Content = "b"
	c.StreamOptions.IncludeUsage = true
	if r.Messages[0].Content != "a" || r.StreamOptions.IncludeUsage {
		t.Fatal("clone shares state")
	}
}
