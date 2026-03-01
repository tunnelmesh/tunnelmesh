package testutil

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
)

// InitTestTracer sets up a synchronous in-memory OTel tracer for test span inspection.
// It uses the same ID generator as the production InitOTel so test trace IDs always
// start with a non-zero hex nibble (works around Tempo search bug #5395).
// Returns the exporter and a cleanup function that restores the previous provider.
func InitTestTracer(t *testing.T) (*tracetest.InMemoryExporter, func()) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithIDGenerator(tracing.NewIDGenerator()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	return exp, func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	}
}
