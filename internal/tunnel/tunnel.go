// Package tunnel implements SSH tunnel management for peer-to-peer connections.
package tunnel

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

const (
	// ChannelType is the SSH channel type for mesh data.
	ChannelType = "tunnelmesh-data"
)

// handleSSHRequests handles out-of-band SSH requests, responding to keepalives.
// This replaces ssh.DiscardRequests to properly handle keepalive requests
// which expect a response to maintain the connection.
func handleSSHRequests(reqs <-chan *ssh.Request) {
	for req := range reqs {
		if req == nil {
			return
		}

		// Log non-keepalive requests for debugging
		if req.Type != "keepalive@openssh.com" {
			log.Debug().
				Str("type", req.Type).
				Bool("want_reply", req.WantReply).
				Msg("received SSH request")
		}

		// Respond to requests that want a reply (like keepalive)
		if req.WantReply {
			if err := req.Reply(true, nil); err != nil {
				log.Debug().Err(err).Str("type", req.Type).Msg("failed to reply to SSH request")
			}
		}
	}
}

// TunnelConnection represents a bidirectional tunnel connection.
// Both SSH tunnels and relay tunnels implement this interface.
// nolint:revive // TunnelConnection name kept for clarity despite stuttering
type TunnelConnection interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
	PeerName() string
}

// TransportConn is a minimal interface for transport connections.
type TransportConn interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
	PeerName() string
}

// HealthChecker is an optional interface for connections that can report health.
type HealthChecker interface {
	IsHealthy() bool
}

// NewTunnelFromTransport creates a tunnel from a transport connection.
func NewTunnelFromTransport(conn TransportConn) *Tunnel {
	return &Tunnel{
		transportConn: conn,
		peerName:      conn.PeerName(),
	}
}

// SSHServer handles incoming SSH connections.
type SSHServer struct {
	config         *ssh.ServerConfig
	authorizedKeys []ssh.PublicKey
	keysMu         sync.RWMutex
}

// SSHConnection represents an established SSH connection.
type SSHConnection struct {
	Conn       *ssh.ServerConn
	Channels   <-chan ssh.NewChannel
	Requests   <-chan *ssh.Request
	PeerName   string
	RemoteAddr net.Addr
}

// NewSSHServer creates a new SSH server.
func NewSSHServer(hostKey ssh.Signer, authorizedKeys []ssh.PublicKey) *SSHServer {
	s := &SSHServer{
		authorizedKeys: authorizedKeys,
	}

	// nolint:revive // conn required by interface signature but not used
	config := &ssh.ServerConfig{
		PublicKeyCallback: func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			s.keysMu.RLock()
			defer s.keysMu.RUnlock()

			keyBytes := key.Marshal()
			for _, authorized := range s.authorizedKeys {
				if string(keyBytes) == string(authorized.Marshal()) {
					return &ssh.Permissions{
						Extensions: map[string]string{
							"pubkey-fp": ssh.FingerprintSHA256(key),
						},
					}, nil
				}
			}
			return nil, fmt.Errorf("unknown public key")
		},
	}
	config.AddHostKey(hostKey)
	s.config = config

	return s
}

// Accept accepts an incoming connection and performs SSH handshake.
func (s *SSHServer) Accept(conn net.Conn) (*SSHConnection, error) {
	sshConn, chans, reqs, err := ssh.NewServerConn(conn, s.config)
	if err != nil {
		return nil, fmt.Errorf("SSH handshake failed: %w", err)
	}

	// Handle out-of-band requests (keepalive, etc.)
	// Respond to keepalive requests instead of discarding them
	go handleSSHRequests(reqs)

	log.Info().
		Str("remote", conn.RemoteAddr().String()).
		Str("user", sshConn.User()).
		Msg("SSH connection established")

	return &SSHConnection{
		Conn:       sshConn,
		Channels:   chans,
		Requests:   reqs,
		RemoteAddr: conn.RemoteAddr(),
	}, nil
}

// AddAuthorizedKey adds a public key to the authorized keys.
func (s *SSHServer) AddAuthorizedKey(key ssh.PublicKey) {
	s.keysMu.Lock()
	defer s.keysMu.Unlock()

	// Check if key already exists
	keyBytes := key.Marshal()
	for _, existing := range s.authorizedKeys {
		if string(existing.Marshal()) == string(keyBytes) {
			return // Already authorized
		}
	}

	s.authorizedKeys = append(s.authorizedKeys, key)
	log.Debug().
		Str("fingerprint", ssh.FingerprintSHA256(key)).
		Msg("authorized key added")
}

// SSHClient handles outgoing SSH connections.
type SSHClient struct {
	signer  ssh.Signer
	hostKey ssh.PublicKey
}

// NewSSHClient creates a new SSH client.
func NewSSHClient(signer ssh.Signer, hostKey ssh.PublicKey) *SSHClient {
	return &SSHClient{
		signer:  signer,
		hostKey: hostKey,
	}
}

// Connect establishes an SSH connection to the given address.
func (c *SSHClient) Connect(addr string) (*ssh.Client, error) {
	config := &ssh.ClientConfig{
		User: "tunnelmesh",
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(c.signer),
		},
		HostKeyCallback: c.hostKeyCallback(),
		Timeout:         30 * time.Second,
	}

	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, fmt.Errorf("SSH dial failed: %w", err)
	}

	log.Info().
		Str("addr", addr).
		Msg("SSH connection established")

	return client, nil
}

func (c *SSHClient) hostKeyCallback() ssh.HostKeyCallback {
	if c.hostKey == nil {
		// Insecure mode - accept any host key
		return ssh.InsecureIgnoreHostKey()
		// nolint:revive // hostname required by interface signature but not used
	}

	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		if string(key.Marshal()) != string(c.hostKey.Marshal()) {
			return fmt.Errorf("host key mismatch")
		}
		return nil
	}
}

// Tunnel represents a bidirectional data tunnel over SSH or transport connection.
type Tunnel struct {
	channel       ssh.Channel   // For SSH-based tunnels
	transportConn TransportConn // For transport-based tunnels (UDP, etc.)
	sshClient     *ssh.Client   // Keep reference to prevent GC from closing connection
	peerName      string
	mu            sync.Mutex
	closed        bool
}

// NewTunnel creates a tunnel from an SSH channel.
func NewTunnel(channel ssh.Channel, peerName string) *Tunnel {
	return &Tunnel{
		channel:  channel,
		peerName: peerName,
	}
}

// NewTunnelWithClient creates a tunnel that holds a reference to the SSH client.
// This prevents the SSH connection from being garbage collected while the tunnel is active.
func NewTunnelWithClient(channel ssh.Channel, peerName string, client *ssh.Client) *Tunnel {
	return &Tunnel{
		channel:   channel,
		sshClient: client,
		peerName:  peerName,
	}
}

// Read reads data from the tunnel.
func (t *Tunnel) Read(p []byte) (int, error) {
	if t.transportConn != nil {
		return t.transportConn.Read(p)
	}
	return t.channel.Read(p)
}

// Write writes data to the tunnel.
func (t *Tunnel) Write(p []byte) (int, error) {
	if t.transportConn != nil {
		return t.transportConn.Write(p)
	}
	return t.channel.Write(p)
}

// Close closes the tunnel and its underlying SSH connection if present.
func (t *Tunnel) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	var err error

	// Close transport connection if present
	if t.transportConn != nil {
		err = t.transportConn.Close()
		return err
	}

	// Close the SSH channel
	if t.channel != nil {
		err = t.channel.Close()
	}

	// Close the SSH client if we own it
	if t.sshClient != nil {
		if closeErr := t.sshClient.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}

// PeerName returns the name of the peer at the other end.
func (t *Tunnel) PeerName() string {
	return t.peerName
}

// IsHealthy returns true if the tunnel is ready for data transmission.
// For transport-based tunnels, this delegates to the underlying connection's health check.
// For SSH-based tunnels, this checks if the tunnel hasn't been closed.
func (t *Tunnel) IsHealthy() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return false
	}

	// Check transport connection health if available
	if t.transportConn != nil {
		if hc, ok := t.transportConn.(HealthChecker); ok {
			return hc.IsHealthy()
		}
		// Transport connection without health checker is assumed healthy if not closed
		return true
	}

	// SSH channel is healthy if tunnel isn't closed
	return t.channel != nil
	// nolint:revive // TunnelManager name kept for clarity despite stuttering
}

// TunnelManager manages multiple tunnels to peers.
type TunnelManager struct {
	tunnels map[string]TunnelConnection
	mu      sync.RWMutex
}

// NewTunnelManager creates a new tunnel manager.
func NewTunnelManager() *TunnelManager {
	return &TunnelManager{
		tunnels: make(map[string]TunnelConnection),
	}
}

// Add adds a tunnel to the manager.
func (m *TunnelManager) Add(name string, tunnel TunnelConnection) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close existing tunnel if present
	if existing, ok := m.tunnels[name]; ok {
		_ = existing.Close()
	}

	m.tunnels[name] = tunnel
	log.Debug().Str("peer", name).Msg("tunnel added")
}

// Get returns the tunnel for a peer.
func (m *TunnelManager) Get(name string) (TunnelConnection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tunnel, ok := m.tunnels[name]
	return tunnel, ok
}

// Remove removes and closes a tunnel.
func (m *TunnelManager) Remove(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tunnel, ok := m.tunnels[name]; ok {
		_ = tunnel.Close()
		delete(m.tunnels, name)
		log.Debug().Str("peer", name).Msg("tunnel removed")
	}
}

// List returns the names of all active tunnels.
func (m *TunnelManager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.tunnels))
	for name := range m.tunnels {
		names = append(names, name)
	}
	return names
}

// CloseAll closes all tunnels.
func (m *TunnelManager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, tunnel := range m.tunnels {
		_ = tunnel.Close()
		delete(m.tunnels, name)
	}
	log.Debug().Msg("all tunnels closed")
}

// ConnectionAdapter wraps an io.ReadWriteCloser (like transport.Connection)
// to implement the TunnelConnection interface.
type ConnectionAdapter struct {
	conn     ReadWriteCloserWithName
	peerName string
	mu       sync.Mutex
	closed   bool
}

// ReadWriteCloserWithName is an interface for connections with a peer name.
type ReadWriteCloserWithName interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
	PeerName() string
}

// NewConnectionAdapter creates a ConnectionAdapter from any ReadWriteCloserWithName.
func NewConnectionAdapter(conn ReadWriteCloserWithName, peerName string) *ConnectionAdapter {
	return &ConnectionAdapter{
		conn:     conn,
		peerName: peerName,
	}
}

// Read reads data from the connection.
func (a *ConnectionAdapter) Read(p []byte) (int, error) {
	return a.conn.Read(p)
}

// Write writes data to the connection.
func (a *ConnectionAdapter) Write(p []byte) (int, error) {
	return a.conn.Write(p)
}

// Close closes the connection.
func (a *ConnectionAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}
	a.closed = true
	return a.conn.Close()
}

// PeerName returns the peer name.
func (a *ConnectionAdapter) PeerName() string {
	return a.peerName
}

// IsHealthy implements HealthChecker by delegating to the underlying connection
// if it supports health checking. Returns false if the adapter is closed.
func (a *ConnectionAdapter) IsHealthy() bool {
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return false
	}
	if hc, ok := a.conn.(HealthChecker); ok {
		return hc.IsHealthy()
	}
	// Underlying connection has no health check — assume healthy while not closed.
	return true
}
