package tracing

import "sync/atomic"

// otelEnabled tracks whether a real OTel exporter is configured.
// When false, TraceIDFromContext calls can be skipped to avoid
// the overhead of traversing the OTel context graph on every request.
var otelEnabled atomic.Bool

// SetEnabled sets whether OTel tracing is active. Called at startup when a
// real OTLP endpoint is configured, and reset to false on shutdown.
func SetEnabled(v bool) {
	otelEnabled.Store(v)
}

// IsEnabled reports whether OTel tracing is currently active.
// Use this to gate expensive OTel context traversals in hot paths.
func IsEnabled() bool {
	return otelEnabled.Load()
}
