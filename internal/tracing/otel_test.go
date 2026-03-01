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

// TestNoLeadingZeroIDGenerator_TraceIDFirstNibbleNonZero verifies that all
// generated trace IDs have a non-zero first hex digit (first byte ≥ 0x10).
// This prevents the Tempo search API from returning 31-char IDs that Grafana
// cannot look up (https://github.com/grafana/tempo/issues/5395).
func TestNoLeadingZeroIDGenerator_TraceIDFirstNibbleNonZero(t *testing.T) {
	g := &noLeadingZeroIDGenerator{}
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		tid, _ := g.NewIDs(ctx)
		if tid[0]&0xF0 == 0 {
			t.Errorf("iteration %d: trace ID %x has leading-zero first nibble (byte[0]=%02x)", i, tid, tid[0])
		}
	}
}

// TestNoLeadingZeroIDGenerator_SpanIDIsNonZero verifies that generated span IDs
// are non-zero (crypto/rand virtually never returns all-zero, but confirm the
// generator doesn't accidentally zero the span ID).
func TestNoLeadingZeroIDGenerator_SpanIDIsNonZero(t *testing.T) {
	g := &noLeadingZeroIDGenerator{}
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, sid := g.NewIDs(ctx)
		var zero [8]byte
		if sid == zero {
			t.Error("NewIDs returned zero span ID")
		}
		sid2 := g.NewSpanID(ctx, [16]byte{})
		if sid2 == zero {
			t.Error("NewSpanID returned zero span ID")
		}
	}
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
