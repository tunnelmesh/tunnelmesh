package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

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
