package udp

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	otelcodes "go.opentelemetry.io/otel/codes"

	"github.com/tunnelmesh/tunnelmesh/testutil"
)

// newHandshakeTestTransport returns a minimal UDP transport ready for unit tests.
// HandshakeTimeout is set low so tests complete quickly.
func newHandshakeTestTransport(t *testing.T) (*Transport, *net.UDPConn) {
	t.Helper()

	priv, pub, err := X25519KeyPair()
	if err != nil {
		t.Fatalf("X25519KeyPair: %v", err)
	}

	tr, err := New(Config{
		StaticPrivate:    priv,
		StaticPublic:     pub,
		HandshakeTimeout: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return tr, conn
}

// TestInitiateHandshake_EmitsSpan verifies that initiateHandshake emits a
// udp.handshake.initiate span, even when no peer responds (timeout path).
func TestInitiateHandshake_EmitsSpan(t *testing.T) {
	exp, cleanup := testutil.InitTestTracer(t)
	defer cleanup()

	tr, conn := newHandshakeTestTransport(t)

	_, peerPub, _ := X25519KeyPair()
	peerAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 59999}

	_, _ = tr.initiateHandshake(context.Background(), "test-peer", peerPub, peerAddr, conn)

	for _, s := range exp.GetSpans() {
		if s.Name == "udp.handshake.initiate" {
			return
		}
	}
	t.Error("udp.handshake.initiate span not emitted")
}

// TestInitiateHandshake_SpanHasPeerAttr verifies that the span has the
// peer.name attribute set to the peer name passed in.
func TestInitiateHandshake_SpanHasPeerAttr(t *testing.T) {
	exp, cleanup := testutil.InitTestTracer(t)
	defer cleanup()

	tr, conn := newHandshakeTestTransport(t)

	_, peerPub, _ := X25519KeyPair()
	peerAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 59998}

	_, _ = tr.initiateHandshake(context.Background(), "alice", peerPub, peerAddr, conn)

	for _, s := range exp.GetSpans() {
		if s.Name != "udp.handshake.initiate" {
			continue
		}
		for _, attr := range s.Attributes {
			if attr.Key == "peer.name" && attr.Value.AsString() == "alice" {
				return
			}
		}
		t.Error("udp.handshake.initiate span missing peer.name=alice attribute")
		return
	}
	t.Error("udp.handshake.initiate span not found")
}

// TestInitiateHandshake_TimeoutRecordsError verifies that when the handshake
// times out (no response), the span status is set to Error.
func TestInitiateHandshake_TimeoutRecordsError(t *testing.T) {
	exp, cleanup := testutil.InitTestTracer(t)
	defer cleanup()

	tr, conn := newHandshakeTestTransport(t)

	_, peerPub, _ := X25519KeyPair()
	peerAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 59997}

	_, err := tr.initiateHandshake(context.Background(), "bob", peerPub, peerAddr, conn)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	for _, s := range exp.GetSpans() {
		if s.Name == "udp.handshake.initiate" {
			if s.Status.Code != otelcodes.Error {
				t.Errorf("udp.handshake.initiate status = %v, want Error", s.Status.Code)
			}
			return
		}
	}
	t.Error("udp.handshake.initiate span not found")
}

// TestHandshake_ContextCancelled verifies that initiateHandshake returns
// immediately with context.Canceled when the context is already cancelled,
// well before the HandshakeTimeout fires.
func TestHandshake_ContextCancelled(t *testing.T) {
	tr, conn := newHandshakeTestTransport(t)

	_, peerPub, _ := X25519KeyPair()
	peerAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 59996}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel before calling

	start := time.Now()
	_, err := tr.initiateHandshake(ctx, "cancelled-peer", peerPub, peerAddr, conn)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	// Should return well before the 30 ms HandshakeTimeout.
	if elapsed >= 20*time.Millisecond {
		t.Errorf("context cancel took %v, expected < 20ms", elapsed)
	}
}

// TestHandshake_Timeout verifies that initiateHandshake returns an error
// when the context deadline is shorter than HandshakeTimeout and no response
// arrives.
func TestHandshake_Timeout(t *testing.T) {
	tr, conn := newHandshakeTestTransport(t)

	_, peerPub, _ := X25519KeyPair()
	peerAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 59995}

	// 10 ms deadline is shorter than the 30 ms HandshakeTimeout configured
	// in newHandshakeTestTransport, so timeout logic uses the context deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := tr.initiateHandshake(ctx, "timeout-peer", peerPub, peerAddr, conn)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	// The select may fire on ctx.Done() (DeadlineExceeded) or time.After
	// (handshake timeout) depending on scheduling; both are acceptable.
	if !errors.Is(err, context.DeadlineExceeded) && err.Error() != "handshake timeout" {
		t.Errorf("unexpected error: %v", err)
	}
}
