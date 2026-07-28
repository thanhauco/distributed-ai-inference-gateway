package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/ratelimit"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/resilience"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/router"
)

// Error is a request failure with an HTTP mapping in the OpenAI error format.
type Error struct {
	Status     int
	Type       string
	Code       string
	Message    string
	RetryAfter time.Duration
	RateLimit  *ratelimit.Decision
	Err        error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Err }

// StatusClientClosedRequest is the de-facto status for a client that went away.
const StatusClientClosedRequest = 499

// AsError maps any error from the pipeline to an *Error.
func AsError(err error) *Error {
	var ge *Error
	if errors.As(err, &ge) {
		return ge
	}
	var se *backend.StatusError
	switch {
	case errors.Is(err, context.Canceled):
		return &Error{Status: StatusClientClosedRequest, Type: "client_closed_request", Message: "client closed the request", Err: err}
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Status: http.StatusGatewayTimeout, Type: "timeout_error", Code: "request_timeout", Message: "request timed out", Err: err}
	case errors.Is(err, resilience.ErrFirstTokenTimeout):
		return &Error{Status: http.StatusGatewayTimeout, Type: "timeout_error", Code: "first_token_timeout", Message: "no worker produced a first token in time", Err: err}
	case errors.Is(err, router.ErrNoWorkers), errors.Is(err, resilience.ErrOpen):
		return &Error{Status: http.StatusServiceUnavailable, Type: "service_unavailable_error", Code: "no_healthy_workers", Message: "no healthy worker is available for this model", RetryAfter: time.Second, Err: err}
	case errors.As(err, &se) && se.StatusCode >= 400 && se.StatusCode < 500 && se.StatusCode != http.StatusTooManyRequests:
		return &Error{Status: se.StatusCode, Type: "invalid_request_error", Code: "upstream_rejected", Message: se.Body, Err: err}
	default:
		return &Error{Status: http.StatusBadGateway, Type: "upstream_error", Code: "worker_failed", Message: "all attempts to reach a worker failed", Err: err}
	}
}

// cause classifies an upstream error for metrics.
func cause(err error) string {
	var se *backend.StatusError
	switch {
	case errors.Is(err, resilience.ErrFirstTokenTimeout):
		return "first_token_timeout"
	case errors.Is(err, resilience.ErrOpen):
		return "breaker_open"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, backend.ErrStreamProtocol):
		return "protocol"
	case errors.As(err, &se):
		return fmt.Sprintf("http_%d", se.StatusCode)
	default:
		return "connection"
	}
}

// workerFault reports whether err should count against the worker's breaker.
func workerFault(err error) bool {
	var se *backend.StatusError
	if errors.As(err, &se) {
		return se.WorkerFault()
	}
	return !errors.Is(err, context.Canceled)
}
