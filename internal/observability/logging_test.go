package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestLoggerEnrichesFromContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "debug", "json")

	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid}))
	ctx = WithRequestID(ctx, "req-1")
	ctx = WithAttrs(ctx, slog.String("model", "llama3-8b"))

	logger.InfoContext(ctx, "hello", "k", "v")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("not json: %v (%s)", err, buf.String())
	}
	for k, want := range map[string]string{
		"request_id": "req-1",
		"trace_id":   tid.String(),
		"span_id":    sid.String(),
		"model":      "llama3-8b",
		"k":          "v",
	} {
		if rec[k] != want {
			t.Errorf("%s = %v, want %v", k, rec[k], want)
		}
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "warn", "json")
	logger.Info("dropped")
	if buf.Len() != 0 {
		t.Fatalf("info should be filtered at warn level: %s", buf.String())
	}
}

func TestNewRequestIDUnique(t *testing.T) {
	a, b := NewRequestID(), NewRequestID()
	if a == b || len(a) != 32 {
		t.Fatalf("bad ids %q %q", a, b)
	}
}
