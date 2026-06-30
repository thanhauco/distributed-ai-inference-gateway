package observability

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSetupTracingDisabledStillPropagates(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), TracingConfig{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	ctx, span := tp.Tracer("t").Start(context.Background(), "parent")
	defer span.End()

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if !strings.Contains(carrier["traceparent"], span.SpanContext().TraceID().String()) {
		t.Fatalf("traceparent not injected: %v", carrier)
	}
}
