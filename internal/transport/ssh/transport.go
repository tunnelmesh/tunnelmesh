// Package ssh implements the SSH transport for tunnelmesh.
package ssh

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	gossh "golang.org/x/crypto/ssh"

	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"github.com/tunnelmesh/tunnelmesh/internal/transport"
	"github.com/tunnelmesh/tunnelmesh/internal/tunnel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
)

// Transport implements the SSH transport.
type Transport struct {
	sshServer *tunnel.SSHServer
	sshClient *tunnel.SSHClient
	listener  net.Listener
	closed    atomic.Bool
}

// Config holds SSH transport configuration.
type Config struct {
	HostKey        gossh.Signer
	ClientSigner   gossh.Signer
	AuthorizedKeys []gossh.PublicKey
	ListenPort     int
}

// New creates a new SSH transport.
func New(cfg Config) (*Transport, error) {
	if cfg.HostKey == nil {
		return nil, fmt.Errorf("host key is required")
	}
	if cfg.ClientSigner == nil {
		return nil, fmt.Errorf("client signer is required")
	}

	return &Transport{
		sshServer: tunnel.NewSSHServer(cfg.HostKey, cfg.AuthorizedKeys),
		sshClient: tunnel.NewSSHClient(cfg.ClientSigner, nil),
	}, nil
}

// Type returns the transport type.
func (t *Transport) Type() transport.TransportType {
	return transport.TransportSSH
}

// Dial creates an outbound SSH connection to the peer.
func (t *Transport) Dial(ctx context.Context, opts transport.DialOptions) (transport.Connection, error) {
	if t.closed.Load() {
		return nil, fmt.Errorf("transport is closed")
	}
	if opts.PeerInfo == nil {
		return nil, fmt.Errorf("peer info is required")
	}

	// Determine address to connect to (prefer private IPs for lower latency)
	addr := t.selectAddress(opts.PeerInfo)
	if addr == "" {
		return nil, fmt.Errorf("no connectable address for peer %s", opts.PeerName)
	}

	ctx, span := tracing.Tracer("tunnelmesh/ssh").Start(ctx, "ssh.dial")
	defer span.End()
	span.SetAttributes(
		attribute.String("peer.name", opts.PeerName),
		attribute.String("peer.id", opts.PeerInfo.PublicKey),
		attribute.String("ssh.addr", addr),
	)

	log.Debug().
		Str("peer", opts.PeerName).
		Str("addr", addr).
		Msg("dialing SSH")

	// Connect with timeout
	dialCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	// SSH dial doesn't support context directly, so we use a goroutine
	type result struct {
		client *gossh.Client
		err    error
	}
	ch := make(chan result, 1)

	go func() {
		client, err := t.sshClient.Connect(addr)
		ch <- result{client, err}
	}()

	select {
	case <-dialCtx.Done():
		dialErr := dialCtx.Err()
		span.RecordError(dialErr)
		span.SetStatus(otelcodes.Error, dialErr.Error())
		sc := span.SpanContext()
		log.Debug().Err(dialErr).
			Str("peer", opts.PeerName).
			Str("addr", addr).
			Str("trace_id", sc.TraceID().String()).
			Str("span_id", sc.SpanID().String()).
			Msg("SSH dial timeout")
		return nil, dialErr
	case r := <-ch:
		if r.err != nil {
			span.RecordError(r.err)
			span.SetStatus(otelcodes.Error, r.err.Error())
			sc := span.SpanContext()
			log.Debug().Err(r.err).
				Str("peer", opts.PeerName).
				Str("addr", addr).
				Str("trace_id", sc.TraceID().String()).
				Str("span_id", sc.SpanID().String()).
				Msg("SSH dial failed")
			return nil, r.err
		}

		// Open the tunnelmesh data channel, sending our identity as extra data
		channel, reqs, err := r.client.OpenChannel(tunnel.ChannelType, []byte(opts.LocalName))
		if err != nil {
			_ = r.client.Close()
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
			return nil, fmt.Errorf("open channel: %w", err)
		}

		// Discard channel requests
		go gossh.DiscardRequests(reqs)

		span.SetStatus(otelcodes.Ok, "")
		conn := &Connection{
			channel:    channel,
			sshClient:  r.client,
			peerName:   opts.PeerName,
			localAddr:  r.client.LocalAddr(),
			remoteAddr: r.client.RemoteAddr(),
		}
		conn.initActivity()
		return conn, nil
	}
}

// selectAddress chooses the best address to connect to.
// Returns "" if the peer is not listening on SSH (SSHPort == 0).
func (t *Transport) selectAddress(info *transport.PeerInfo) string {
	port := info.SSHPort
	if port == 0 {
		return "" // Peer explicitly registered with no SSH listener
	}

	// Prefer private IPs (same LAN)
	for _, ip := range info.PrivateIPs {
		return net.JoinHostPort(ip, fmt.Sprint(port))
	}

	// Fall back to public IPs
	for _, ip := range info.PublicIPs {
		return net.JoinHostPort(ip, fmt.Sprint(port))
	}

	return ""
}

// Listen starts listening for incoming SSH connections.
func (t *Transport) Listen(ctx context.Context, opts transport.ListenOptions) (transport.Listener, error) {
	addr := opts.Address
	if addr == "" {
		addr = fmt.Sprintf(":%d", opts.Port)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	t.listener = listener

	return &Listener{
		transport: t,
		listener:  listener,
	}, nil
}

// Probe tests if the peer is reachable via SSH.
func (t *Transport) Probe(ctx context.Context, opts transport.ProbeOptions) (time.Duration, error) {
	if opts.PeerInfo == nil {
		return 0, fmt.Errorf("peer info is required")
	}

	addr := t.selectAddress(opts.PeerInfo)
	if addr == "" {
		return 0, fmt.Errorf("no connectable address")
	}

	// Simple TCP probe with timing
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	start := time.Now()
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return 0, err
	}
	latency := time.Since(start)
	_ = conn.Close()

	return latency, nil
}

// AddAuthorizedKey adds a public key to the authorized keys.
func (t *Transport) AddAuthorizedKey(key gossh.PublicKey) {
	t.sshServer.AddAuthorizedKey(key)
}

// Close shuts down the transport.
func (t *Transport) Close() error {
	if t.closed.Swap(true) {
		return nil // Already closed
	}
	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}

// Listener implements the transport.Listener interface for SSH.
type Listener struct {
	transport *Transport
	listener  net.Listener
}

// Accept waits for and returns the next connection.
func (l *Listener) Accept(ctx context.Context) (transport.Connection, error) {
	for {
		// Accept with context support
		type acceptResult struct {
			conn net.Conn
			err  error
		}
		ch := make(chan acceptResult, 1)

		go func() {
			conn, err := l.listener.Accept()
			ch <- acceptResult{conn, err}
		}()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r := <-ch:
			if r.err != nil {
				return nil, r.err
			}

			// Perform SSH handshake
			sshConn, err := l.transport.sshServer.Accept(r.conn)
			if err != nil {
				log.Warn().Err(err).Str("remote", r.conn.RemoteAddr().String()).Msg("SSH handshake failed")
				_ = r.conn.Close()
				continue // Try next connection
			}

			// Wait for channel
			for newChannel := range sshConn.Channels {
				if newChannel.ChannelType() != tunnel.ChannelType {
					_ = newChannel.Reject(gossh.UnknownChannelType, "unknown channel type")
					continue
				}

				// Accept the channel
				channel, reqs, err := newChannel.Accept()
				if err != nil {
					log.Debug().Err(err).Msg("failed to accept channel")
					continue
				}

				// Discard channel requests
				go gossh.DiscardRequests(reqs)

				// Get peer name from channel extra data
				peerName := string(newChannel.ExtraData())

				conn := &Connection{
					channel:    channel,
					sshServer:  sshConn.Conn, // Keep reference to prevent GC from closing connection
					peerName:   peerName,
					localAddr:  r.conn.LocalAddr(),
					remoteAddr: r.conn.RemoteAddr(),
				}
				conn.initActivity()
				return conn, nil
			}
		}
	}
}

// Addr returns the listener's network address.
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}

// Close stops listening.
func (l *Listener) Close() error {
	return l.listener.Close()
}

// Connection wraps an SSH channel as a transport.Connection.
type Connection struct {
	channel    gossh.Channel
	sshClient  *gossh.Client     // For outgoing connections - prevents GC
	sshServer  *gossh.ServerConn // For incoming connections - prevents GC
	peerName   string
	localAddr  net.Addr
	remoteAddr net.Addr
	mu         sync.Mutex
	closed     bool

	// Activity tracking for timeout detection
	lastRecv atomic.Value // time.Time - last time data was received
}

// Read reads data from the connection.
func (c *Connection) Read(p []byte) (int, error) {
	n, err := c.channel.Read(p)
	if n > 0 {
		c.lastRecv.Store(time.Now())
	}
	return n, err
}

// Write writes data to the connection.
func (c *Connection) Write(p []byte) (int, error) {
	return c.channel.Write(p)
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	err := c.channel.Close()
	if c.sshClient != nil {
		if clientErr := c.sshClient.Close(); clientErr != nil && err == nil {
			err = clientErr
		}
	}
	if c.sshServer != nil {
		if serverErr := c.sshServer.Close(); serverErr != nil && err == nil {
			err = serverErr
		}
	}
	return err
}

// PeerName returns the peer name.
func (c *Connection) PeerName() string {
	return c.peerName
}

// Type returns the transport type.
func (c *Connection) Type() transport.TransportType {
	return transport.TransportSSH
}

// LocalAddr returns the local network address.
func (c *Connection) LocalAddr() net.Addr {
	return c.localAddr
}

// RemoteAddr returns the remote network address.
func (c *Connection) RemoteAddr() net.Addr {
	return c.remoteAddr
}

// IsHealthy returns true if the SSH channel is open and ready for data.
func (c *Connection) IsHealthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed
}

// LastActivity returns the time of the last received data.
// Used for activity timeout detection.
func (c *Connection) LastActivity() time.Time {
	if v := c.lastRecv.Load(); v != nil {
		return v.(time.Time)
	}
	return time.Time{}
}

// initActivity initializes the last activity timestamp.
// Should be called when the connection is established.
func (c *Connection) initActivity() {
	c.lastRecv.Store(time.Now())
}
