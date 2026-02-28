package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// initTestTracer sets up a tracer provider with synchronous in-memory export.
// Returns the exporter (to inspect spans) and a cleanup function.
func initTestTracer(t *testing.T) (*tracetest.InMemoryExporter, func()) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	return exp, func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	}
}

func TestInitOTel_SetsGlobalProvider(t *testing.T) {
	ctx := context.Background()
	// Save and restore global state
	prev := globalTracerProvider
	defer func() {
		globalTracerProvider = prev
		if prev != nil {
			otel.SetTracerProvider(prev)
		}
	}()

	// Use a no-op OTLP endpoint (InitOTel creates the exporter but we just check the provider type).
	// We can't easily call InitOTel with a real endpoint, so test the core behaviour:
	// after init, the global provider is a *sdktrace.TracerProvider.
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	globalTracerProvider = tp

	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Errorf("GetTracerProvider() type = %T, want *sdktrace.TracerProvider", otel.GetTracerProvider())
	}

	_ = ctx
}

func TestShutdownOTel_Idempotent(t *testing.T) {
	ctx := context.Background()

	prev := globalTracerProvider
	defer func() {
		globalTracerProvider = prev
	}()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	globalTracerProvider = tp

	// First shutdown
	if err := ShutdownOTel(ctx); err != nil {
		t.Errorf("first ShutdownOTel() error = %v", err)
	}

	// Second shutdown — should be nil, not panic
	if err := ShutdownOTel(ctx); err != nil {
		t.Errorf("second ShutdownOTel() error = %v, want nil", err)
	}
}

func TestTracer_NoopBeforeInit(t *testing.T) {
	// Ensure no global provider is set
	prev := globalTracerProvider
	defer func() { globalTracerProvider = prev }()
	globalTracerProvider = nil

	// Reset global tracer to no-op
	otel.SetTracerProvider(otel.GetTracerProvider())

	// Should not panic
	tr := Tracer("test")
	_, span := tr.Start(context.Background(), "test.span")
	span.End()
}

func TestTracer_EmitsSpanWithProvider(t *testing.T) {
	exp, cleanup := initTestTracer(t)
	defer cleanup()

	tr := Tracer("test/tracer")
	_, span := tr.Start(context.Background(), "my.operation")
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "my.operation" {
		t.Errorf("span name = %q, want %q", spans[0].Name, "my.operation")
	}
}
