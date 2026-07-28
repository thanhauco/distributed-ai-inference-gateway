package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/resilience"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/router"
)

func TestAsError(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{context.Canceled, StatusClientClosedRequest, ""},
		{context.DeadlineExceeded, http.StatusGatewayTimeout, "request_timeout"},
		{resilience.ErrFirstTokenTimeout, http.StatusGatewayTimeout, "first_token_timeout"},
		{router.ErrNoWorkers, http.StatusServiceUnavailable, "no_healthy_workers"},
		{&backend.StatusError{StatusCode: 400, Body: "context length exceeded"}, 400, "upstream_rejected"},
		{&backend.StatusError{StatusCode: 503}, http.StatusBadGateway, "worker_failed"},
		{fmt.Errorf("wrapped: %w", &Error{Status: 429, Code: "rate_limit_exceeded"}), 429, "rate_limit_exceeded"},
	}
	for _, c := range cases {
		got := AsError(c.err)
		if got.Status != c.status || got.Code != c.code {
			t.Errorf("%v → %d/%s, want %d/%s", c.err, got.Status, got.Code, c.status, c.code)
		}
	}
}

func TestWorkerFaultClassification(t *testing.T) {
	if !workerFault(&backend.StatusError{StatusCode: 502}) || workerFault(&backend.StatusError{StatusCode: 400}) {
		t.Fatal("status classification")
	}
	if workerFault(context.Canceled) || !workerFault(errors.New("connection refused")) {
		t.Fatal("error classification")
	}
	if cause(resilience.ErrFirstTokenTimeout) != "first_token_timeout" || cause(&backend.StatusError{StatusCode: 500}) != "http_500" {
		t.Fatal("cause labels")
	}
}

func TestSplitForReplay(t *testing.T) {
	in := "The gateway streams\ncached answers too."
	parts := splitForReplay(in)
	if strings.Join(parts, "") != in || len(parts) != 6 {
		t.Fatalf("parts %q", parts)
	}
	if len(splitForReplay("")) != 0 {
		t.Fatal("empty")
	}
}
