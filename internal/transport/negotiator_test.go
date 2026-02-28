package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// initTestTracer sets up a synchronous in-memory OTel tracer for test span inspection.
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

// mockConn is a minimal transport.Connection for testing.
type mockConn struct {
	peerName string
	typ      TransportType
}

func (m *mockConn) Read(p []byte) (int, error)  { return 0, nil }
func (m *mockConn) Write(p []byte) (int, error) { return len(p), nil }
func (m *mockConn) Close() error                { return nil }
func (m *mockConn) PeerName() string            { return m.peerName }
func (m *mockConn) Type() TransportType         { return m.typ }
func (m *mockConn) LocalAddr() net.Addr         { return nil }
func (m *mockConn) RemoteAddr() net.Addr        { return nil }
func (m *mockConn) IsHealthy() bool             { return true }

// mockTransport implements Transport for testing.
type mockTransport struct {
	typ     TransportType
	dialErr error
	dialFn  func(ctx context.Context, opts DialOptions) (Connection, error)
}

func (m *mockTransport) Type() TransportType { return m.typ }
func (m *mockTransport) Dial(ctx context.Context, opts DialOptions) (Connection, error) {
	if m.dialFn != nil {
		return m.dialFn(ctx, opts)
	}
	if m.dialErr != nil {
		return nil, m.dialErr
	}
	return &mockConn{peerName: opts.PeerName, typ: m.typ}, nil
}
func (m *mockTransport) Listen(ctx context.Context, opts ListenOptions) (Listener, error) {
	return nil, nil
}
func (m *mockTransport) Probe(ctx context.Context, opts ProbeOptions) (time.Duration, error) {
	return 0, nil
}
func (m *mockTransport) Close() error { return nil }

// buildNegotiator creates a negotiator with the given transports registered
// and MaxRetries=0 for fast tests.
func buildNegotiator(transports ...Transport) (*Negotiator, *Registry) {
	order := make([]TransportType, 0, len(transports))
	for _, t := range transports {
		order = append(order, t.Type())
	}
	reg := NewRegistry(RegistryConfig{DefaultOrder: order})
	for _, t := range transports {
		_ = reg.Register(t)
	}
	cfg := DefaultNegotiatorConfig()
	cfg.MaxRetries = 0
	cfg.EnableFallback = true
	cfg.ConnectionTimeout = 2 * time.Second
	return NewNegotiator(reg, cfg), reg
}

func TestNegotiate_EmitsNegotiateSpan(t *testing.T) {
	exp, cleanup := initTestTracer(t)
	defer cleanup()

	tr := &mockTransport{typ: TransportUDP}
	neg, _ := buildNegotiator(tr)

	_, err := neg.Negotiate(context.Background(), &PeerInfo{Name: "alice"}, DialOptions{})
	if err != nil {
		t.Fatalf("Negotiate() error = %v", err)
	}

	spans := exp.GetSpans()
	var negotiateSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "transport.negotiate" {
			negotiateSpan = &spans[i]
			break
		}
	}
	if negotiateSpan == nil {
		t.Fatal("transport.negotiate span not found")
	}

	// Verify peer.name attribute
	var found bool
	for _, attr := range negotiateSpan.Attributes {
		if attr.Key == "peer.name" && attr.Value.AsString() == "alice" {
			found = true
		}
	}
	if !found {
		t.Error("transport.negotiate span missing peer.name=alice attribute")
	}

	// Verify transport.order attribute is present
	found = false
	for _, attr := range negotiateSpan.Attributes {
		if attr.Key == "transport.order" {
			found = true
		}
	}
	if !found {
		t.Error("transport.negotiate span missing transport.order attribute")
	}
}

func TestNegotiate_EmitsAttemptSpans(t *testing.T) {
	exp, cleanup := initTestTracer(t)
	defer cleanup()

	tr := &mockTransport{typ: TransportUDP}
	neg, _ := buildNegotiator(tr)

	_, _ = neg.Negotiate(context.Background(), &PeerInfo{Name: "bob"}, DialOptions{})

	spans := exp.GetSpans()
	var attemptCount int
	for _, s := range spans {
		if s.Name == "transport.attempt" {
			attemptCount++
		}
	}
	if attemptCount == 0 {
		t.Error("expected at least one transport.attempt span, got 0")
	}
}

func TestNegotiate_AttemptsAreChildrenOfNegotiate(t *testing.T) {
	exp, cleanup := initTestTracer(t)
	defer cleanup()

	tr := &mockTransport{typ: TransportUDP}
	neg, _ := buildNegotiator(tr)

	_, _ = neg.Negotiate(context.Background(), &PeerInfo{Name: "carol"}, DialOptions{})

	spans := exp.GetSpans()
	var negotiateID [8]byte
	for _, s := range spans {
		if s.Name == "transport.negotiate" {
			negotiateID = [8]byte(s.SpanContext.SpanID())
			break
		}
	}
	if negotiateID == [8]byte{} {
		t.Fatal("transport.negotiate span not found")
	}

	for _, s := range spans {
		if s.Name == "transport.attempt" {
			parentID := [8]byte(s.Parent.SpanID())
			if parentID != negotiateID {
				t.Errorf("transport.attempt parent = %x, want %x", parentID, negotiateID)
			}
		}
	}
}

func TestNegotiate_AttemptSpanRecordsErrorOnFailure(t *testing.T) {
	exp, cleanup := initTestTracer(t)
	defer cleanup()

	dialErr := errors.New("connection refused")
	failing := &mockTransport{typ: TransportUDP, dialErr: dialErr}
	neg, _ := buildNegotiator(failing)

	_, _ = neg.Negotiate(context.Background(), &PeerInfo{Name: "dave"}, DialOptions{})

	spans := exp.GetSpans()
	for _, s := range spans {
		if s.Name == "transport.attempt" {
			if s.Status.Code != otelcodes.Error {
				t.Errorf("transport.attempt status = %v, want Error", s.Status.Code)
			}
			return
		}
	}
	t.Error("transport.attempt span not found")
}

func TestNegotiate_NegotiateSpanHasSelectedTransport(t *testing.T) {
	exp, cleanup := initTestTracer(t)
	defer cleanup()

	// First transport fails, second succeeds — exercises fallback path.
	failing := &mockTransport{typ: TransportUDP, dialErr: errors.New("fail")}
	succeeding := &mockTransport{typ: TransportSSH}
	neg, _ := buildNegotiator(failing, succeeding)

	result, err := neg.Negotiate(context.Background(), &PeerInfo{Name: "eve"}, DialOptions{})
	if err != nil {
		t.Fatalf("Negotiate() error = %v", err)
	}
	if result.Transport != TransportSSH {
		t.Errorf("result.Transport = %v, want SSH", result.Transport)
	}

	spans := exp.GetSpans()
	for _, s := range spans {
		if s.Name != "transport.negotiate" {
			continue
		}
		var found bool
		for _, attr := range s.Attributes {
			if attr.Key == attribute.Key("transport.selected") && attr.Value.AsString() == "ssh" {
				found = true
			}
		}
		if !found {
			t.Error("transport.negotiate span missing transport.selected=ssh attribute")
		}
		return
	}
	t.Error("transport.negotiate span not found")
}

func TestNegotiate_AllSpansEndWithinFunction(t *testing.T) {
	exp, cleanup := initTestTracer(t)
	defer cleanup()

	tr := &mockTransport{typ: TransportUDP}
	neg, _ := buildNegotiator(tr)

	_, _ = neg.Negotiate(context.Background(), &PeerInfo{Name: "frank"}, DialOptions{})

	for _, s := range exp.GetSpans() {
		if s.Name == "transport.negotiate" || s.Name == "transport.attempt" {
			if s.EndTime.IsZero() {
				t.Errorf("span %q has zero EndTime (not ended)", s.Name)
			}
		}
	}
}
