package tracing

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

// noLeadingZeroIDGenerator is a custom OpenTelemetry ID generator that ensures
// trace IDs never start with a leading-zero hex digit (i.e. first byte ≥ 0x10).
//
// Workaround for https://github.com/grafana/tempo/issues/5395: Tempo's search
// API returns trace IDs as plain integers, stripping any leading zero hex digit
// and producing a 31-char string instead of the required 32 chars. Grafana's
// trace-by-ID lookup then fails with "Not Found" because it passes the exact
// search string to Tempo's stricter trace-retrieval path.
//
// The upstream fix (Tempo PR #6489, config option read.left_pad_trace_ids) was
// merged 2026-02-18 and is not yet in a stable release. Until then, avoiding
// trace IDs whose first nibble is 0 eliminates the problem class entirely.
type noLeadingZeroIDGenerator struct{}

func (g *noLeadingZeroIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	var tid trace.TraceID
	var sid trace.SpanID
	if _, err := cryptorand.Read(tid[:]); err != nil {
		panic(fmt.Sprintf("tracing: trace ID generation failed: %v", err))
	}
	if _, err := cryptorand.Read(sid[:]); err != nil {
		panic(fmt.Sprintf("tracing: span ID generation failed: %v", err))
	}
	tid[0] |= 0x10 // force first hex digit to 1–f, never 0
	return tid, sid
}

func (g *noLeadingZeroIDGenerator) NewSpanID(_ context.Context, _ trace.TraceID) trace.SpanID {
	var sid trace.SpanID
	if _, err := cryptorand.Read(sid[:]); err != nil {
		panic(fmt.Sprintf("tracing: span ID generation failed: %v", err))
	}
	return sid
}

var globalTracerProvider *sdktrace.TracerProvider

// InitOTel initialises the OpenTelemetry TracerProvider with an OTLP HTTP exporter
// pointing at the given endpoint URL (e.g. "http://localhost:4318").
// It is safe to call multiple times; subsequent calls replace the provider.
// Call ShutdownOTel on program exit to flush pending spans.
func InitOTel(ctx context.Context, serviceName, serviceVersion, otlpEndpoint string) error {
	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(otlpEndpoint),
	)
	if err != nil {
		return fmt.Errorf("create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		// Non-fatal: fall back to default resource
		res = resource.Default()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithMaxExportBatchSize(512),
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		// Sample everything: connection-level events are low-frequency.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		// Use custom ID generator to avoid Tempo search bug with leading-zero trace IDs.
		sdktrace.WithIDGenerator(&noLeadingZeroIDGenerator{}),
	)

	if globalTracerProvider != nil {
		// Best-effort flush of previous provider before replacing.
		shutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = globalTracerProvider.Shutdown(shutCtx)
		cancel()
	}

	otel.SetTracerProvider(tp)
	globalTracerProvider = tp
	return nil
}

// ShutdownOTel flushes buffered spans and shuts down the TracerProvider.
// Should be called in a deferred cleanup near program exit.
func ShutdownOTel(ctx context.Context) error {
	if globalTracerProvider == nil {
		return nil
	}
	tp := globalTracerProvider
	globalTracerProvider = nil
	return tp.Shutdown(ctx)
}

// Tracer returns a named OTel tracer from the global provider.
// Returns a no-op tracer when OTel has not been initialised.
func Tracer(name string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name)
}
