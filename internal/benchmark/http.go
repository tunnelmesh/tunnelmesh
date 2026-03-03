package benchmark

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// BenchmarkPath is the HTTP endpoint registered on the admin server's mux.
	BenchmarkPath = "/benchmark"

	// upgradeProtocol is the value used in the HTTP Upgrade header to signal
	// that the connection should be switched to the benchmark binary protocol.
	upgradeProtocol = "tunnelmesh-benchmark"
)

// NewHTTPHandler returns an http.Handler that serves the benchmark protocol via
// HTTP connection hijacking. Register it on the admin server's mux at BenchmarkPath
// before calling Start.
func NewHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), upgradeProtocol) {
			http.Error(w, "expected Upgrade: "+upgradeProtocol, http.StatusBadRequest)
			return
		}

		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "connection hijacking not supported", http.StatusInternalServerError)
			return
		}

		conn, bufrw, err := hijacker.Hijack()
		if err != nil {
			log.Error().Err(err).Msg("benchmark: failed to hijack connection")
			return
		}
		defer func() { _ = conn.Close() }()

		// Respond with 101 Switching Protocols before handing off to serveConn.
		_, _ = fmt.Fprint(bufrw,
			"HTTP/1.1 101 Switching Protocols\r\n"+
				"Upgrade: "+upgradeProtocol+"\r\n"+
				"Connection: Upgrade\r\n"+
				"\r\n",
		)
		if err := bufrw.Flush(); err != nil {
			log.Error().Err(err).Msg("benchmark: failed to flush 101 response")
			return
		}

		// Pass the raw conn (not bufrw) to serveConn. bufrw.Reader may have
		// buffered bytes that the HTTP server pre-read, but in practice its
		// read buffer is empty here: the client sends no binary data until it
		// receives this 101 response, so Hijack() captures the conn at the
		// exact message boundary.
		log.Debug().Str("remote", conn.RemoteAddr().String()).Msg("benchmark: HTTP upgrade accepted")
		serveConn(r.Context(), conn)
	})
}

// dialBenchmarkHTTP connects to a benchmark server via HTTP upgrade on the admin
// server (TLS). addr is "host:port". The returned net.Conn carries the benchmark
// binary protocol.
func dialBenchmarkHTTP(ctx context.Context, addr string) (net.Conn, error) {
	// Connect with TLS. InsecureSkipVerify is intentional: the admin server uses
	// a mesh-internal self-signed certificate, and both endpoints are already
	// inside the encrypted mesh tunnel.
	tlsConfig := &tls.Config{InsecureSkipVerify: true} // #nosec G402
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 10 * time.Second},
		Config:    tlsConfig,
	}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial benchmark: %w", err)
	}

	host, _, _ := net.SplitHostPort(addr)

	// Send HTTP upgrade request.
	req := "GET " + BenchmarkPath + " HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Upgrade: " + upgradeProtocol + "\r\n" +
		"Connection: Upgrade\r\n" +
		"\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write upgrade request: %w", err)
	}

	// Read and verify the 101 Switching Protocols response.
	if err := readUpgradeResponse(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}

// readUpgradeResponse reads the HTTP response headers and verifies a 101 status.
// The bufio.Reader used here may buffer bytes beyond the response headers, but
// in practice the server sends no binary data until it receives MsgStart from
// the client, so the reader never captures application-layer bytes.
func readUpgradeResponse(conn net.Conn) error {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	reader := bufio.NewReader(conn)
	// Read the status line.
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read upgrade response: %w", err)
	}
	statusLine = strings.TrimSpace(statusLine)
	if !strings.HasPrefix(statusLine, "HTTP/1.1 101") {
		return fmt.Errorf("expected 101 Switching Protocols, got: %q", statusLine)
	}
	// Drain the remaining headers.
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read upgrade headers: %w", err)
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}
	return nil
}
