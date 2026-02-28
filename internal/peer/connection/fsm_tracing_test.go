package connection

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// initFSMTestTracer sets up a synchronous in-memory tracer for span assertions.
func initFSMTestTracer(t *testing.T) (*tracetest.InMemoryExporter, func()) {
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

func TestTransitionTo_EmitsSpan(t *testing.T) {
	exp, cleanup := initFSMTestTracer(t)
	defer cleanup()

	pc := NewPeerConnection(PeerConnectionConfig{PeerName: "peer1", MeshIP: "10.0.0.1"})
	if err := pc.TransitionTo(context.Background(), StateConnecting, "test", nil); err != nil {
		t.Fatalf("TransitionTo() error = %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span, got 0")
	}

	var found bool
	for _, s := range spans {
		if s.Name == "connection.transition" {
			found = true
			break
		}
	}
	if !found {
		t.Error("connection.transition span not emitted")
	}
}

func TestTransitionTo_SpanAttributes(t *testing.T) {
	exp, cleanup := initFSMTestTracer(t)
	defer cleanup()

	pc := NewPeerConnection(PeerConnectionConfig{PeerName: "peer2", MeshIP: "10.0.0.2"})
	if err := pc.TransitionTo(context.Background(), StateConnecting, "dial initiated", nil); err != nil {
		t.Fatalf("TransitionTo() error = %v", err)
	}

	spans := exp.GetSpans()
	var span *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "connection.transition" {
			span = &spans[i]
			break
		}
	}
	if span == nil {
		t.Fatal("connection.transition span not found")
	}

	wantAttrs := map[string]string{
		"peer.name":         "peer2",
		"peer.mesh_ip":      "10.0.0.2",
		"transition.from":   "disconnected",
		"transition.to":     "connecting",
		"transition.reason": "dial initiated",
	}
	for _, attr := range span.Attributes {
		if want, ok := wantAttrs[string(attr.Key)]; ok {
			if attr.Value.AsString() != want {
				t.Errorf("attr %s = %q, want %q", attr.Key, attr.Value.AsString(), want)
			}
			delete(wantAttrs, string(attr.Key))
		}
	}
	for missing := range wantAttrs {
		t.Errorf("span missing attribute %q", missing)
	}
}

func TestTransitionTo_ErrorRecordedOnFailure(t *testing.T) {
	exp, cleanup := initFSMTestTracer(t)
	defer cleanup()

	pc := NewPeerConnection(PeerConnectionConfig{PeerName: "peer3", MeshIP: "10.0.0.3"})
	// Get to Connected first
	_ = pc.TransitionTo(context.Background(), StateConnecting, "dial", nil)
	exp.Reset() // clear connecting span

	connErr := errors.New("network unreachable")
	_ = pc.TransitionTo(context.Background(), StateDisconnected, "error", connErr)

	spans := exp.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == "connection.transition" && s.Status.Code == otelcodes.Error {
			found = true
			break
		}
	}
	if !found {
		t.Error("connection.transition span with Error status not found")
	}
}

func TestConnected_SpanIsChildOfParent(t *testing.T) {
	exp, cleanup := initFSMTestTracer(t)
	defer cleanup()

	// Create a parent span to simulate the peer.establish_tunnel context.
	parentCtx, parentSpan := otel.Tracer("test").Start(context.Background(), "parent.operation")
	parentSpanID := parentSpan.SpanContext().SpanID()

	pc := NewPeerConnection(PeerConnectionConfig{PeerName: "peer4", MeshIP: "10.0.0.4"})
	_ = pc.TransitionTo(context.Background(), StateConnecting, "dial", nil)

	tun := &mockTunnel{}
	if err := pc.Connected(parentCtx, tun, "udp", "transport negotiated"); err != nil {
		parentSpan.End()
		t.Fatalf("Connected() error = %v", err)
	}
	parentSpan.End()

	spans := exp.GetSpans()
	for _, s := range spans {
		if s.Name != "connection.transition" {
			continue
		}
		// Find the "Connecting → Connected" transition span
		var toConnected bool
		for _, attr := range s.Attributes {
			if attr.Key == "transition.to" && attr.Value.AsString() == "connected" {
				toConnected = true
			}
		}
		if !toConnected {
			continue
		}
		// This span should have parentSpanID as its parent.
		if s.Parent.SpanID() != parentSpanID {
			t.Errorf("connection.transition parent = %s, want %s", s.Parent.SpanID(), parentSpanID)
		}
		return
	}
	t.Error("Connected (Connecting→Connected) transition span not found")
}
