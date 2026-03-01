package tracing

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
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

// dockerSpanNameProcessor is an OTel SpanProcessor that rewrites Docker daemon
// API span names produced by the Docker Go client's built-in otelhttp transport.
// That transport names spans with the raw URL path, embedding the container /
// image / network ID (a 12- or 64-char hex string), creating a unique span name
// per resource and flooding the Tempo span-name index.
//
//	Before: GET /v1.51/containers/ce4144f1a8b9e86d.../stats
//	After:  GET /v1.51/containers/{id}/stats
//
// The processor fires in OnStart so the corrected name is recorded in Tempo.
type dockerSpanNameProcessor struct{}

func (dockerSpanNameProcessor) OnStart(_ context.Context, s sdktrace.ReadWriteSpan) {
	name := s.Name()
	// Quick pre-filter: Docker API paths look like "METHOD /v<digit>.<digit>/..."
	slashV := strings.Index(name, " /v")
	if slashV < 0 {
		return
	}
	method := name[:slashV]
	path := name[slashV+1:]
	parts := strings.Split(path, "/")
	changed := false
	for i, p := range parts {
		if isDockerHexID(p) {
			parts[i] = "{id}"
			changed = true
		}
	}
	if changed {
		s.SetName(method + " " + strings.Join(parts, "/"))
	}
}

func (dockerSpanNameProcessor) OnEnd(sdktrace.ReadOnlySpan)      {}
func (dockerSpanNameProcessor) Shutdown(context.Context) error   { return nil }
func (dockerSpanNameProcessor) ForceFlush(context.Context) error { return nil }

// isDockerHexID reports whether s is a Docker resource ID:
// a 12-char short ID or a full 64-char hex string.
func isDockerHexID(s string) bool {
	n := len(s)
	if n != 12 && n != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

var (
	globalTPMu           sync.Mutex
	globalTracerProvider *sdktrace.TracerProvider
)

// NewIDGenerator returns the production trace/span ID generator used by InitOTel.
// Exposed so test helpers (testutil.InitTestTracer) can match production behaviour
// and avoid leading-zero trace IDs that trip the Tempo search bug (#5395).
func NewIDGenerator() sdktrace.IDGenerator {
	return &noLeadingZeroIDGenerator{}
}

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
		// Rewrite Docker daemon API span names to remove per-resource hex IDs.
		sdktrace.WithSpanProcessor(dockerSpanNameProcessor{}),
	)

	globalTPMu.Lock()
	if globalTracerProvider != nil {
		// Best-effort flush of previous provider before replacing.
		shutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = globalTracerProvider.Shutdown(shutCtx)
		cancel()
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	globalTracerProvider = tp
	globalTPMu.Unlock()
	return nil
}

// ShutdownOTel flushes buffered spans and shuts down the TracerProvider.
// Should be called in a deferred cleanup near program exit.
func ShutdownOTel(ctx context.Context) error {
	globalTPMu.Lock()
	tp := globalTracerProvider
	globalTracerProvider = nil
	globalTPMu.Unlock()
	if tp == nil {
		return nil
	}
	return tp.Shutdown(ctx)
}

// Tracer returns a named OTel tracer from the global provider.
// Returns a no-op tracer when OTel has not been initialised.
func Tracer(name string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name)
}

// InjectHeaders writes W3C traceparent/tracestate from ctx into h.
// No-op when no active span or propagator not yet registered.
func InjectHeaders(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}

// ExtractContext returns a child of parent containing the remote span
// context from the W3C headers in h. Returns parent unchanged if headers
// are absent or invalid.
func ExtractContext(parent context.Context, h http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(parent, propagation.HeaderCarrier(h))
}
