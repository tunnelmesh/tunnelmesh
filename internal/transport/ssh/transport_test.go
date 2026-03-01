package ssh

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	gossh "golang.org/x/crypto/ssh"

	"github.com/tunnelmesh/tunnelmesh/internal/transport"
)

func initSSHTestTracer(t *testing.T) (*tracetest.InMemoryExporter, func()) {
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

// newTestTransport creates an SSH transport with throwaway ed25519 keys.
func newTestTransport(t *testing.T) *Transport {
	t.Helper()

	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	hostSigner, err := gossh.NewSignerFromKey(hostPriv)
	if err != nil {
		t.Fatalf("host signer: %v", err)
	}

	_, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	clientSigner, err := gossh.NewSignerFromKey(clientPriv)
	if err != nil {
		t.Fatalf("client signer: %v", err)
	}

	tr, err := New(Config{HostKey: hostSigner, ClientSigner: clientSigner})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return tr
}

// TestSSHDial_EmitsSpan verifies that Dial emits an ssh.dial span with the
// peer.name and ssh.addr attributes, even when the connection is refused.
func TestSSHDial_EmitsSpan(t *testing.T) {
	exp, cleanup := initSSHTestTracer(t)
	defer cleanup()

	tr := newTestTransport(t)
	opts := transport.DialOptions{
		PeerName: "test-peer",
		PeerInfo: &transport.PeerInfo{
			Name:       "test-peer",
			PrivateIPs: []string{"127.0.0.1"},
			SSHPort:    1, // port 1 — connection refused almost immediately
		},
		Timeout: 2 * time.Second,
	}

	_, _ = tr.Dial(context.Background(), opts)

	var dialSpan *tracetest.SpanStub
	for i := range exp.GetSpans() {
		s := &exp.GetSpans()[i]
		if s.Name == "ssh.dial" {
			dialSpan = s
			break
		}
	}
	if dialSpan == nil {
		t.Fatal("ssh.dial span not emitted")
	}

	wantAttrs := map[string]string{
		"peer.name": "test-peer",
		"ssh.addr":  "127.0.0.1:1",
	}
	for _, attr := range dialSpan.Attributes {
		if want, ok := wantAttrs[string(attr.Key)]; ok {
			if attr.Value.AsString() != want {
				t.Errorf("attr %s = %q, want %q", attr.Key, attr.Value.AsString(), want)
			}
			delete(wantAttrs, string(attr.Key))
		}
	}
	for missing := range wantAttrs {
		t.Errorf("ssh.dial span missing attribute %q", missing)
	}
}

// TestSSHDial_SpanRecordsErrorOnFailure verifies that when the SSH connection
// is refused the span status is set to Error.
func TestSSHDial_SpanRecordsErrorOnFailure(t *testing.T) {
	exp, cleanup := initSSHTestTracer(t)
	defer cleanup()

	tr := newTestTransport(t)
	opts := transport.DialOptions{
		PeerName: "err-peer",
		PeerInfo: &transport.PeerInfo{
			Name:       "err-peer",
			PrivateIPs: []string{"127.0.0.1"},
			SSHPort:    1,
		},
		Timeout: 2 * time.Second,
	}

	_, _ = tr.Dial(context.Background(), opts)

	for _, s := range exp.GetSpans() {
		if s.Name == "ssh.dial" {
			if s.Status.Code != otelcodes.Error {
				t.Errorf("ssh.dial status = %v, want Error", s.Status.Code)
			}
			return
		}
	}
	t.Error("ssh.dial span not found")
}

// TestSSHDial_SpanEndsOnTimeout verifies that when the context deadline fires
// while the SSH handshake is in progress the span has Error status and ends.
func TestSSHDial_SpanEndsOnTimeout(t *testing.T) {
	exp, cleanup := initSSHTestTracer(t)
	defer cleanup()

	// Listen but never speak SSH so the handshake blocks.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Accept TCP but ignore — forces SSH handshake to block.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	tr := newTestTransport(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	opts := transport.DialOptions{
		PeerName: "timeout-peer",
		PeerInfo: &transport.PeerInfo{
			Name:       "timeout-peer",
			PrivateIPs: []string{"127.0.0.1"},
			SSHPort:    port,
		},
		// Timeout not set in opts; rely on ctx deadline.
	}

	_, _ = tr.Dial(ctx, opts)

	for _, s := range exp.GetSpans() {
		if s.Name == "ssh.dial" {
			if s.Status.Code != otelcodes.Error {
				t.Errorf("ssh.dial status = %v, want Error", s.Status.Code)
			}
			if s.EndTime.IsZero() {
				t.Error("ssh.dial span EndTime is zero (span not ended)")
			}
			return
		}
	}
	t.Error("ssh.dial span not found")
}
