package udp

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/portmap"
	"github.com/tunnelmesh/tunnelmesh/internal/portmap/client"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"github.com/tunnelmesh/tunnelmesh/internal/transport"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
)

// PacketQueueSize is the buffer size for the packet processing queue.
// When the queue is full, packets are dropped rather than spawning unbounded goroutines.
const PacketQueueSize = 1024

// packetWork represents a packet to be processed by a worker.
type packetWork struct {
	data       []byte
	remoteAddr *net.UDPAddr
	conn       *net.UDPConn
}

// handshakeResponse is used to pass handshake responses from receiveLoop to initiators.
type handshakeResponse struct {
	data       []byte
	remoteAddr *net.UDPAddr
}

// Transport implements encrypted UDP transport.
type Transport struct {
	mu sync.RWMutex

	// Configuration
	config Config

	// Network - dual-stack support with separate IPv4 and IPv6 sockets
	conn     *net.UDPConn // IPv4 socket (or single socket if dual-stack disabled)
	conn6    *net.UDPConn // IPv6 socket (nil if not available)
	listener *Listener

	// Identity
	staticPrivate [32]byte
	staticPublic  [32]byte

	// Our discovered external addresses (set after registration)
	externalAddr  string // IPv4 external address
	externalAddr6 string // IPv6 external address

	// Timestamp of last network change (for invalidating stale sessions)
	lastNetworkChange time.Time

	// Port mapper for PCP/NAT-PMP/UPnP
	portMapper *portmap.PortMapper

	// Sessions (indexed by local index)
	sessions map[uint32]*Session

	// Peer lookup (peer name -> session)
	peerSessions map[string]*Session

	// Pending handshakes (local index -> response channel)
	pendingHandshakes map[uint32]chan handshakeResponse

	// Pending outbound handshakes by peer name (for crossing handshake detection)
	pendingOutboundPeers map[string]uint32 // peer name -> local index

	// Rate limiting for rekey-required messages (source addr -> last sent time)
	rekeyRateLimit map[string]time.Time

	// Callback for session invalidation (called when we receive rekey-required)
	onSessionInvalid func(peerName string)

	// Callback for pong responses (used for latency measurement)
	onPong func(peerName string, rtt time.Duration)

	// Worker pool for packet processing (avoids per-packet goroutine spawning)
	packetQueue chan packetWork

	// State
	running atomic.Bool
	closeCh chan struct{}

	// HTTP client for coordination server requests
	httpClient *http.Client

	// onDropQueueFull is called when a packet is dropped because the queue is full.
	// Stored as atomic.Value to allow lock-free reads on the hot receive path.
	onDropQueueFull atomic.Value // stores func()
}

// Config holds UDP transport configuration.
type Config struct {
	// ListenAddr is the UDP address to listen on
	ListenAddr string

	// Port is the UDP port (if ListenAddr not specified)
	Port int

	// LocalPeerName is our own peer name (used for hole-punch coordination)
	LocalPeerName string

	// StaticPrivate is our X25519 private key
	StaticPrivate [32]byte

	// StaticPublic is our X25519 public key
	StaticPublic [32]byte

	// CoordServerURL is the coordination server URL for hole-punching
	CoordServerURL string

	// AuthToken is the shared auth token for coordination server endpoints
	AuthToken string

	// JWTToken for authenticating with relay (not used for hole-punch)
	JWTToken string

	// KeepaliveInterval for NAT keepalive
	KeepaliveInterval time.Duration

	// HandshakeTimeout for establishing sessions
	HandshakeTimeout time.Duration

	// HolePunchTimeout for NAT traversal
	HolePunchTimeout time.Duration

	// SessionTimeout is the duration after which a session is considered dead
	// if no packets have been received. This handles stale sessions where the
	// peer has restarted and our rekey-required messages don't reach us (NAT timeout).
	// Default: 75 seconds (~3 keepalive intervals worth of missed packets)
	SessionTimeout time.Duration

	// PeerResolver looks up peer name from their X25519 public key
	// Returns empty string if peer is unknown
	PeerResolver func(pubKey [32]byte) string

	// HolePunchRetries is the number of hole-punch attempts
	HolePunchRetries int

	// EnablePortMapping enables PCP/NAT-PMP/UPnP port mapping.
	// When enabled, the transport will attempt to create a port mapping
	// on the gateway for better NAT traversal. Default: true (opportunistic)
	EnablePortMapping *bool

	// HTTPClient is the HTTP client to use for coordination server requests.
	// If nil, a new client with sensible defaults will be created.
	// Providing a managed client allows proper cleanup during network changes.
	HTTPClient *http.Client
}

// SetOnDropQueueFull sets a callback invoked each time a packet is dropped because
// the processing queue is full. Uses atomic storage so it is safe to call from any
// goroutine, including after Start(), without lock contention on the hot receive path.
func (t *Transport) SetOnDropQueueFull(fn func()) {
	t.onDropQueueFull.Store(fn)
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Port:              51820,
		KeepaliveInterval: 25 * time.Second,
		HandshakeTimeout:  10 * time.Second,
		HolePunchTimeout:  10 * time.Second,
		SessionTimeout:    75 * time.Second,
		HolePunchRetries:  5,
	}
}

// New creates a new UDP transport.
func New(cfg Config) (*Transport, error) {
	if cfg.KeepaliveInterval == 0 {
		cfg.KeepaliveInterval = 25 * time.Second
	}
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	if cfg.HolePunchTimeout == 0 {
		cfg.HolePunchTimeout = 10 * time.Second
	}
	if cfg.HolePunchRetries == 0 {
		cfg.HolePunchRetries = 5
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 75 * time.Second
	}

	// Use provided HTTP client or create a new one with sensible defaults
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	t := &Transport{
		config:               cfg,
		staticPrivate:        cfg.StaticPrivate,
		staticPublic:         cfg.StaticPublic,
		sessions:             make(map[uint32]*Session),
		peerSessions:         make(map[string]*Session),
		pendingHandshakes:    make(map[uint32]chan handshakeResponse),
		pendingOutboundPeers: make(map[string]uint32),
		rekeyRateLimit:       make(map[string]time.Time),
		closeCh:              make(chan struct{}),
		httpClient:           httpClient,
	}

	return t, nil
}

// Type returns the transport type.
func (t *Transport) Type() transport.TransportType {
	return transport.TransportUDP
}

// Implement portmap.Observer interface

// OnPortMapStateChanged is called when the port mapper transitions between states.
func (t *Transport) OnPortMapStateChanged(_ *portmap.PortMapper, oldState portmap.State, newState portmap.State) {
	log.Debug().
		Str("old", oldState.String()).
		Str("new", newState.String()).
		Msg("UDP portmap state changed")
}

// OnMappingAcquired is called when a port mapping is successfully acquired or refreshed.
func (t *Transport) OnMappingAcquired(pm *portmap.PortMapper, mapping *portmap.Mapping) {
	t.mu.Lock()
	// Store the PCP-mapped external address
	// This will be used during registration with the coord server
	t.externalAddr = mapping.ExternalAddr()
	t.mu.Unlock()

	log.Info().
		Str("external", mapping.ExternalAddr()).
		Str("client", mapping.ClientType).
		Dur("lifetime", mapping.Lifetime).
		Msg("UDP PCP/NAT-PMP mapping acquired")
}

// OnMappingLost is called when a previously active mapping is lost.
func (t *Transport) OnMappingLost(_ *portmap.PortMapper, reason error) {
	t.mu.Lock()
	// Clear the PCP-mapped address, will fall back to STUN discovery
	t.externalAddr = ""
	t.mu.Unlock()

	log.Warn().
		Err(reason).
		Msg("UDP PCP/NAT-PMP mapping lost, falling back to STUN discovery")
}

// HasPCPMapping returns true if we have an active PCP/NAT-PMP port mapping.
func (t *Transport) HasPCPMapping() bool {
	return t.portMapper != nil && t.portMapper.IsActive()
}

// PCPMapping returns the current PCP/NAT-PMP mapping, or nil if not active.
func (t *Transport) PCPMapping() *portmap.Mapping {
	if t.portMapper == nil {
		return nil
	}
	return t.portMapper.Mapping()
}

// rekeyRateLimitInterval is the minimum time between rekey-required messages to the same source.
const rekeyRateLimitInterval = 5 * time.Second

// SetSessionInvalidCallback sets the callback for session invalidation.
// This is called when we receive a rekey-required message indicating our session is stale.
func (t *Transport) SetSessionInvalidCallback(cb func(peerName string)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onSessionInvalid = cb
}

// SetPongCallback sets the callback for pong responses.
// This is called when we receive a pong response, providing the peer name and RTT.
func (t *Transport) SetPongCallback(cb func(peerName string, rtt time.Duration)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onPong = cb
}

// SendPingToPeer sends a ping packet to the specified peer for latency measurement.
// Returns an error if the peer doesn't have an active session.
func (t *Transport) SendPingToPeer(peerName string) error {
	t.mu.RLock()
	session, ok := t.peerSessions[peerName]
	t.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no session for peer %s", peerName)
	}

	_, err := session.SendPing()
	return err
}

// ListConnectedPeers returns a list of peer names with active UDP sessions.
func (t *Transport) ListConnectedPeers() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	peers := make([]string, 0, len(t.peerSessions))
	for name, session := range t.peerSessions {
		if session.IsEstablished() {
			peers = append(peers, name)
		}
	}

	// Debug: always log session counts for diagnostics
	log.Debug().
		Int("total_sessions", len(t.peerSessions)).
		Int("established", len(peers)).
		Msg("ListConnectedPeers called")

	return peers
}

// RegisterPendingOutbound pre-registers intent to connect to a peer.
// This should be called BEFORE spawning the connection goroutine to ensure
// crossing handshake detection works correctly. The pendingOutboundPeers map
// is checked in handleHandshakeInit to detect and handle simultaneous connections.
// Call ClearPendingOutbound when the connection attempt completes (success or failure).
func (t *Transport) RegisterPendingOutbound(peerName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Use 0 as placeholder - the real index will be set in initiateHandshake
	t.pendingOutboundPeers[peerName] = 0
	log.Debug().Str("peer", peerName).Msg("pre-registered pending outbound for crossing handshake detection")
}

// ClearPendingOutbound removes the pending outbound registration for a peer.
// This should be called when the connection attempt completes or if the peer
// won't actually be connected via UDP (e.g., if UDP transport is skipped).
func (t *Transport) ClearPendingOutbound(peerName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pendingOutboundPeers, peerName)
	log.Debug().Str("peer", peerName).Msg("cleared pending outbound registration")
}

// selectSocketForPeer returns the appropriate socket for communicating with the peer.
// Returns the IPv4 socket for IPv4 addresses, IPv6 socket for IPv6 addresses.
// Returns nil if no suitable socket is available (e.g., no IPv6 socket for IPv6 peer).
func (t *Transport) selectSocketForPeer(peerAddr *net.UDPAddr) *net.UDPConn {
	if peerAddr.IP.To4() != nil {
		// Peer is IPv4
		return t.conn
	}
	// Peer is IPv6 - must have IPv6 socket (IPv4 socket cannot reach IPv6 addresses)
	return t.conn6
}

// Start starts the UDP transport (listening for incoming packets).
// Creates both IPv4 and IPv6 sockets for dual-stack support.
func (t *Transport) Start() error {
	if t.running.Load() {
		return fmt.Errorf("transport already running")
	}

	port := t.config.Port
	if t.config.ListenAddr != "" {
		// If explicit address provided, use it
		udpAddr, err := net.ResolveUDPAddr("udp", t.config.ListenAddr)
		if err != nil {
			return fmt.Errorf("resolve address: %w", err)
		}
		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			return fmt.Errorf("listen UDP: %w", err)
		}
		t.conn = conn
	} else {
		// Create IPv4 socket
		udpAddr4 := &net.UDPAddr{IP: net.IPv4zero, Port: port}
		conn4, err := net.ListenUDP("udp4", udpAddr4)
		if err != nil {
			log.Warn().Err(err).Msg("failed to create IPv4 UDP socket")
		} else {
			t.conn = conn4
			log.Debug().Str("addr", conn4.LocalAddr().String()).Msg("IPv4 UDP socket created")
		}

		// Create IPv6 socket
		udpAddr6 := &net.UDPAddr{IP: net.IPv6zero, Port: port}
		conn6, err := net.ListenUDP("udp6", udpAddr6)
		if err != nil {
			log.Warn().Err(err).Msg("failed to create IPv6 UDP socket")
		} else {
			t.conn6 = conn6
			log.Debug().Str("addr", conn6.LocalAddr().String()).Msg("IPv6 UDP socket created")
		}

		// At least one socket must succeed
		if t.conn == nil && t.conn6 == nil {
			return fmt.Errorf("failed to create any UDP socket")
		}
	}

	t.running.Store(true)

	// Initialize worker pool for packet processing
	t.packetQueue = make(chan packetWork, PacketQueueSize)
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		go t.packetWorker()
	}
	log.Debug().Int("workers", numWorkers).Int("queue_size", PacketQueueSize).Msg("UDP packet worker pool started")

	// Start packet receivers for each socket
	if t.conn != nil {
		go t.receiveLoop(t.conn)
	}
	if t.conn6 != nil {
		go t.receiveLoop(t.conn6)
	}

	// Start keepalive sender
	go t.keepaliveLoop()

	addrs := []string{}
	if t.conn != nil {
		addrs = append(addrs, t.conn.LocalAddr().String())
	}
	if t.conn6 != nil {
		addrs = append(addrs, t.conn6.LocalAddr().String())
	}
	log.Info().
		Strs("addrs", addrs).
		Msg("UDP transport started")

	// Start port mapper if enabled (default: enabled)
	portMappingEnabled := t.config.EnablePortMapping == nil || *t.config.EnablePortMapping
	if portMappingEnabled {
		if err := t.startPortMapper(); err != nil {
			// Port mapping is opportunistic - log but don't fail
			log.Debug().Err(err).Msg("port mapping not available, using STUN fallback")
		}
	}

	return nil
}

// startPortMapper initializes and starts the PCP/NAT-PMP/UPnP port mapper.
func (t *Transport) startPortMapper() error {
	// Detect gateway and local IP
	gateway, localIP, err := portmap.DetectGateway()
	if err != nil {
		return fmt.Errorf("detect gateway: %w", err)
	}

	log.Debug().
		Str("gateway", gateway.String()).
		Str("local_ip", localIP.String()).
		Msg("detected gateway for port mapping")

	// Create discovery client that tries PCP, then NAT-PMP
	pmClient := client.NewDiscoveryClient(gateway, localIP, 0)

	// Create port mapper FSM
	t.portMapper = portmap.New(portmap.Config{
		Protocol:  portmap.ProtocolUDP,
		LocalPort: t.config.Port,
		Client:    pmClient,
		Observers: []portmap.Observer{t},
	})

	// Start the FSM (runs in background)
	if err := t.portMapper.Start(); err != nil {
		return fmt.Errorf("start port mapper: %w", err)
	}

	return nil
}

// receiveLoop processes incoming UDP packets from the given connection.
func (t *Transport) receiveLoop(conn *net.UDPConn) {
	buf := make([]byte, 2000) // Larger than MTU

	for t.running.Load() {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if t.running.Load() {
				log.Debug().Err(err).Msg("UDP read error")
			}
			continue
		}

		// Minimum 1 byte needed to check packet type
		// Individual handlers validate their specific size requirements
		// Note: RekeyRequired packets are only 5 bytes, smaller than data packets
		if n < 1 {
			continue // Too short
		}

		packet := make([]byte, n)
		copy(packet, buf[:n])

		// Dispatch to worker pool instead of spawning goroutine per packet
		select {
		case t.packetQueue <- packetWork{data: packet, remoteAddr: remoteAddr, conn: conn}:
			// Packet queued for processing
		default:
			// Queue full - drop packet (better than unbounded goroutines)
			log.Debug().Msg("packet queue full, dropping packet")
			if fn, ok := t.onDropQueueFull.Load().(func()); ok {
				fn()
			}
		}
	}
}

// packetWorker processes packets from the queue.
// Multiple workers run concurrently to handle packets.
// Each iteration has panic recovery so a single malformed packet cannot crash the worker.
func (t *Transport) packetWorker() {
	for work := range t.packetQueue {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().
						Interface("panic", r).
						Str("from", work.remoteAddr.String()).
						Bytes("stack", debug.Stack()).
						Msg("packet worker panicked - skipping packet")
				}
			}()
			t.handlePacket(work.data, work.remoteAddr, work.conn)
		}()
	}
}

// handlePacket processes a single incoming packet.
// The conn parameter is the socket the packet was received on.
func (t *Transport) handlePacket(data []byte, remoteAddr *net.UDPAddr, conn *net.UDPConn) {
	if len(data) < 1 {
		return
	}

	// Check packet type from first byte
	packetType := data[0]

	switch packetType {
	case PacketTypeHandshakeInit:
		// Handshake packets have 1-byte type prefix + handshake message
		t.handleHandshakeInit(data[1:], remoteAddr, conn)
	case PacketTypeHandshakeResponse:
		t.handleHandshakeResponse(data[1:], remoteAddr)
	case PacketTypeData, PacketTypeKeepalive:
		// Data/keepalive packets have 16-byte header
		header, err := UnmarshalHeader(data)
		if err != nil {
			return
		}
		if packetType == PacketTypeData {
			t.handleDataPacket(header, data[HeaderSize:], remoteAddr)
		} else {
			t.handleKeepalive(header, remoteAddr)
		}
	case PacketTypeRekeyRequired:
		t.handleRekeyRequired(data, remoteAddr)
	case PacketTypePing:
		t.handlePing(data, remoteAddr)
	case PacketTypePong:
		t.handlePong(data, remoteAddr)
	}
}

// handleDataPacket processes an incoming data packet.
func (t *Transport) handleDataPacket(header *PacketHeader, ciphertext []byte, remoteAddr *net.UDPAddr) {
	t.mu.RLock()
	session, ok := t.sessions[header.Receiver]
	t.mu.RUnlock()

	if !ok {
		log.Debug().
			Uint32("receiver_index", header.Receiver).
			Str("from", remoteAddr.String()).
			Int("ciphertext_len", len(ciphertext)).
			Msg("data packet for unknown session")

		// Send rekey-required message (rate limited)
		t.sendRekeyRequired(header.Receiver, remoteAddr)
		return
	}

	log.Debug().
		Str("peer", session.PeerName()).
		Uint32("receiver_index", header.Receiver).
		Int("ciphertext_len", len(ciphertext)).
		Msg("UDP data packet received")

	// Update remote address atomically (NAT roaming)
	session.UpdateRemoteAddrIfChanged(remoteAddr)

	if err := session.HandlePacket(header, ciphertext); err != nil {
		log.Debug().
			Err(err).
			Str("peer", session.PeerName()).
			Msg("packet handling error")
	}
}

// handleKeepalive processes a keepalive packet.
func (t *Transport) handleKeepalive(header *PacketHeader, remoteAddr *net.UDPAddr) {
	t.mu.RLock()
	session, ok := t.sessions[header.Receiver]
	t.mu.RUnlock()

	if ok {
		session.UpdateRemoteAddrIfChanged(remoteAddr)
		session.UpdateLastReceive()
	}
}

// handlePing processes a ping packet and sends a pong response.
func (t *Transport) handlePing(data []byte, remoteAddr *net.UDPAddr) {
	ping, err := UnmarshalPing(data)
	if err != nil {
		log.Debug().Err(err).Msg("failed to parse ping packet")
		return
	}

	t.mu.RLock()
	session, ok := t.sessions[ping.Receiver]
	t.mu.RUnlock()

	if !ok {
		log.Debug().
			Uint32("receiver_index", ping.Receiver).
			Str("from", remoteAddr.String()).
			Msg("ping for unknown session")
		return
	}

	// Update remote address for NAT roaming
	session.UpdateRemoteAddrIfChanged(remoteAddr)

	// Send pong response
	if err := session.SendPong(ping.Timestamp); err != nil {
		log.Debug().Err(err).Str("peer", session.PeerName()).Msg("failed to send pong")
	}
}

// handlePong processes a pong packet and calculates RTT.
func (t *Transport) handlePong(data []byte, remoteAddr *net.UDPAddr) {
	pong, err := UnmarshalPong(data)
	if err != nil {
		log.Debug().Err(err).Msg("failed to parse pong packet")
		return
	}

	t.mu.RLock()
	session, ok := t.sessions[pong.Receiver]
	cb := t.onPong
	t.mu.RUnlock()

	if !ok {
		log.Debug().
			Uint32("receiver_index", pong.Receiver).
			Str("from", remoteAddr.String()).
			Msg("pong for unknown session")
		return
	}

	// Calculate RTT
	rtt := time.Duration(time.Now().UnixNano() - pong.Timestamp)

	log.Debug().
		Str("peer", session.PeerName()).
		Dur("rtt", rtt).
		Msg("received pong, calculated RTT")

	// Notify callback
	if cb != nil {
		cb(session.PeerName(), rtt)
	}
}

// sendRekeyRequired sends a rekey-required message to tell the peer their session is stale.
// This is rate-limited to prevent flooding.
func (t *Transport) sendRekeyRequired(unknownIndex uint32, remoteAddr *net.UDPAddr) {
	addrKey := remoteAddr.String()

	// Check rate limit
	t.mu.Lock()
	lastSent, exists := t.rekeyRateLimit[addrKey]
	if exists && time.Since(lastSent) < rekeyRateLimitInterval {
		t.mu.Unlock()
		return
	}
	t.rekeyRateLimit[addrKey] = time.Now()
	t.mu.Unlock()

	// Build and send rekey-required packet
	pkt := NewRekeyRequiredPacket(unknownIndex)
	data := pkt.Marshal()

	// Select the appropriate socket based on address family
	conn := t.selectSocketForAddr(remoteAddr)
	if conn == nil {
		log.Debug().Str("addr", addrKey).Msg("no socket available for rekey-required")
		return
	}

	if _, err := conn.WriteToUDP(data, remoteAddr); err != nil {
		log.Debug().Err(err).Str("addr", addrKey).Msg("failed to send rekey-required")
		return
	}

	log.Debug().
		Uint32("unknown_index", unknownIndex).
		Str("to", addrKey).
		Msg("sent rekey-required")
}

// selectSocketForAddr returns the appropriate socket for an address.
func (t *Transport) selectSocketForAddr(addr *net.UDPAddr) *net.UDPConn {
	if addr.IP.To4() != nil {
		// IPv4 address
		if t.conn != nil {
			return t.conn
		}
		return t.conn6 // Fallback to IPv6 socket if no IPv4
	}
	// IPv6 address
	if t.conn6 != nil {
		return t.conn6
	}
	return t.conn // Fallback to IPv4 socket
}

// handleRekeyRequired processes a rekey-required message from a peer.
// This indicates our session is stale on their side and we need to re-handshake.
func (t *Transport) handleRekeyRequired(data []byte, remoteAddr *net.UDPAddr) {
	pkt, err := UnmarshalRekeyRequired(data)
	if err != nil {
		log.Debug().Err(err).Msg("failed to parse rekey-required")
		return
	}

	log.Debug().
		Uint32("unknown_index", pkt.UnknownIndex).
		Str("from", remoteAddr.String()).
		Msg("received rekey-required from peer")

	// Find session by the index the peer says is unknown.
	// The unknownIndex is the receiver index from packets we sent to them,
	// which is our REMOTE index (the peer's local index from their perspective).
	// We need to search by remoteIndex, not localIndex.
	t.mu.Lock()
	var session *Session
	var localIndex uint32
	for idx, s := range t.sessions {
		log.Trace().
			Uint32("session_local_idx", idx).
			Uint32("session_remote_idx", s.RemoteIndex()).
			Str("peer", s.PeerName()).
			Msg("checking session for rekey match")
		if s.RemoteIndex() == pkt.UnknownIndex {
			session = s
			localIndex = idx
			break
		}
	}
	if session == nil {
		t.mu.Unlock()
		log.Debug().
			Uint32("unknown_index", pkt.UnknownIndex).
			Str("from", remoteAddr.String()).
			Int("session_count", len(t.sessions)).
			Msg("rekey-required for unknown remote index (already removed?)")
		return
	}

	peerName := session.PeerName()

	// Remove the stale session
	delete(t.sessions, localIndex)
	// Only delete from peerSessions if it still points to this session
	// (a new session may have already replaced it during reconnection)
	var hasNewSession bool
	if existing, ok := t.peerSessions[peerName]; ok {
		if existing.LocalIndex() == localIndex {
			delete(t.peerSessions, peerName)
		} else {
			hasNewSession = true
		}
	}

	// Get callback while holding lock, then release before calling
	cb := t.onSessionInvalid
	t.mu.Unlock()

	log.Info().
		Str("peer", peerName).
		Uint32("local_index", localIndex).
		Uint32("remote_index", pkt.UnknownIndex).
		Str("from", remoteAddr.String()).
		Bool("has_new_session", hasNewSession).
		Msg("session invalidated by peer, re-handshake needed")

	// Close the old session
	_ = session.Close()

	// Only notify upper layer if there's no new session already established
	// If a new session exists, don't trigger reconnection - we're already connected
	if cb != nil && !hasNewSession {
		cb(peerName)
	}
}

// handleHandshakeInit processes an incoming handshake initiation.
// The conn parameter is the socket the init was received on, used for responding.
func (t *Transport) handleHandshakeInit(data []byte, remoteAddr *net.UDPAddr, conn *net.UDPConn) {
	if len(data) < 128 {
		return
	}

	// Create responder handshake state
	// Responder doesn't know initiator's static key until they decrypt it from the message
	hs, err := NewResponderHandshake(t.staticPrivate, t.staticPublic)
	if err != nil {
		log.Debug().Err(err).Msg("failed to create handshake state")
		return
	}

	if err := hs.ConsumeInitiation(data); err != nil {
		log.Debug().Err(err).Msg("failed to consume initiation")
		return
	}

	// Look up peer name from public key BEFORE responding
	// We need to check for existing active sessions before committing to this handshake
	peerPubKey := hs.PeerStaticPublic()
	peerName := ""
	if t.config.PeerResolver != nil {
		peerName = t.config.PeerResolver(peerPubKey)
	}
	if peerName == "" {
		// If we can't identify the peer, log and return
		log.Debug().
			Hex("peer_pubkey", peerPubKey[:8]).
			Str("remote", remoteAddr.String()).
			Msg("unknown peer public key, rejecting connection")
		return
	}

	// Check for crossing handshakes: if we have a pending outbound handshake to this peer,
	// use public key comparison as tie-breaker. The node with the "lower" public key wins
	// (its outgoing handshake is kept). This ensures both sides agree on which handshake to use.
	t.mu.RLock()
	_, hasPendingOutbound := t.pendingOutboundPeers[peerName]
	t.mu.RUnlock()

	if hasPendingOutbound {
		// Compare public keys: lower wins (their outgoing handshake is kept)
		cmp := bytes.Compare(t.staticPublic[:], peerPubKey[:])
		if cmp < 0 {
			// Our public key is lower, we win - ignore their init, our outgoing handshake will complete
			log.Debug().
				Str("peer", peerName).
				Str("remote", remoteAddr.String()).
				Msg("crossing handshake detected, our pubkey is lower - ignoring incoming init")
			return
		}
		// Their public key is lower (or equal, unlikely), they win - we'll respond to their init
		// Our outgoing handshake will fail (no response) or be superseded
		log.Debug().
			Str("peer", peerName).
			Str("remote", remoteAddr.String()).
			Msg("crossing handshake detected, their pubkey is lower - responding to their init")
	}

	// Check if we already have an active established session for this peer BEFORE responding.
	// If so, don't respond - the peer is probably retrying due to packet loss.
	// By not responding, the peer will either:
	// 1. Receive packets from our existing session and realize it's still valid
	// 2. Timeout and try again (at which point our session may be stale)
	t.mu.RLock()
	existingSession, hasExisting := t.peerSessions[peerName]
	lastNetChange := t.lastNetworkChange
	if hasExisting && existingSession.IsEstablished() {
		// Check if the existing session is still active (received data recently)
		lastRecv := existingSession.LastReceive()
		sessionAge := time.Since(lastRecv)
		t.mu.RUnlock()

		// Session is active if:
		// 1. We received data in the last 30 seconds, AND
		// 2. The last receive was AFTER any network change (session uses current network)
		sessionActive := sessionAge < 30*time.Second && lastRecv.After(lastNetChange)
		if sessionActive {
			log.Debug().
				Str("peer", peerName).
				Str("remote", remoteAddr.String()).
				Dur("session_age", sessionAge).
				Msg("ignoring handshake init, active session exists (not responding)")
			return
		}
		log.Debug().
			Str("peer", peerName).
			Dur("session_age", sessionAge).
			Bool("pre_network_change", lastRecv.Before(lastNetChange)).
			Msg("replacing stale session with new handshake")
	} else {
		t.mu.RUnlock()
	}

	// Create and send response
	response, err := hs.CreateResponse()
	if err != nil {
		log.Debug().Err(err).Msg("failed to create response")
		return
	}

	// Prepend packet type header
	packet := make([]byte, 1+len(response))
	packet[0] = PacketTypeHandshakeResponse
	copy(packet[1:], response)

	// Respond on the same socket that received the init
	if _, err := conn.WriteToUDP(packet, remoteAddr); err != nil {
		log.Debug().Err(err).Msg("failed to send response")
		return
	}

	// Derive keys (DeriveKeys handles initiator/responder key ordering)
	sendKey, recvKey, err := hs.DeriveKeys()
	if err != nil {
		log.Debug().Err(err).Msg("failed to derive keys")
		return
	}

	crypto, err := NewCryptoState(sendKey, recvKey)
	if err != nil {
		log.Debug().Err(err).Msg("failed to create crypto state")
		return
	}

	// Create session using the same socket that received the handshake
	session := NewSession(SessionConfig{
		LocalIndex: hs.LocalIndex(),
		PeerName:   peerName,
		PeerPublic: peerPubKey,
		RemoteAddr: remoteAddr,
		Conn:       conn,
	})
	session.SetCrypto(crypto, hs.RemoteIndex())
	session.SetOnClose(t.removeSession)

	// Register session, keeping any existing active session for this peer
	if !t.registerSession(session) {
		// Session was discarded because there's already an active session
		// The handshake response was already sent, but we're keeping the old session
		return
	}

	log.Debug().
		Str("remote", remoteAddr.String()).
		Str("peer", peerName).
		Uint32("local_index", session.LocalIndex()).
		Msg("incoming handshake completed")

	// Push connection to listener if active
	if t.listener != nil {
		conn := &Connection{
			session: session,
			readBuf: new(bytes.Buffer),
		}
		if t.listener.trySend(conn) {
			log.Debug().Str("peer", peerName).Msg("incoming connection queued for accept")
		} else if !t.listener.closed.Load() {
			log.Warn().Str("peer", peerName).Msg("listener accept channel full, dropping connection")
		}
	}
}

// handleHandshakeResponse processes an incoming handshake response.
func (t *Transport) handleHandshakeResponse(data []byte, remoteAddr *net.UDPAddr) {
	if len(data) < 72 {
		return
	}

	// Extract receiver index (bytes 4-8) - this is the initiator's local index
	receiverIndex := binary.LittleEndian.Uint32(data[4:8])

	t.mu.RLock()
	respChan, ok := t.pendingHandshakes[receiverIndex]
	t.mu.RUnlock()

	if ok {
		// Non-blocking send - if channel is full or closed, we drop the response
		select {
		case respChan <- handshakeResponse{data: data, remoteAddr: remoteAddr}:
			log.Debug().
				Uint32("receiver_index", receiverIndex).
				Str("remote", remoteAddr.String()).
				Msg("handshake response routed to initiator")
		default:
			log.Debug().
				Uint32("receiver_index", receiverIndex).
				Msg("handshake response channel full or closed")
		}
	} else {
		log.Debug().
			Uint32("receiver_index", receiverIndex).
			Msg("no pending handshake for response")
	}
}

// keepaliveLoop sends periodic keepalives to maintain NAT mappings.
func (t *Transport) keepaliveLoop() {
	ticker := time.NewTicker(t.config.KeepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.sendKeepalives()
		case <-t.closeCh:
			return
		}
	}
}

// sendKeepalives sends keepalive packets to all active sessions and checks for dead sessions.
func (t *Transport) sendKeepalives() {
	t.mu.RLock()
	sessions := make([]*Session, 0, len(t.sessions))
	for _, s := range t.sessions {
		if s.State() == SessionStateEstablished {
			sessions = append(sessions, s)
		}
	}
	sessionTimeout := t.config.SessionTimeout
	cb := t.onSessionInvalid
	t.mu.RUnlock()

	now := time.Now()
	var deadSessions []*Session

	for _, s := range sessions {
		// Check if session has timed out (no packets received for too long)
		lastRecv := s.LastReceive()
		if !lastRecv.IsZero() && now.Sub(lastRecv) > sessionTimeout {
			log.Warn().
				Str("peer", s.PeerName()).
				Dur("since_last_recv", now.Sub(lastRecv)).
				Dur("timeout", sessionTimeout).
				Msg("session timed out, no packets received")
			deadSessions = append(deadSessions, s)
			continue
		}

		if err := s.SendKeepalive(); err != nil {
			log.Debug().
				Err(err).
				Str("peer", s.PeerName()).
				Msg("keepalive failed")
		}
	}

	// Clean up dead sessions
	for _, s := range deadSessions {
		peerName := s.PeerName()
		localIndex := s.LocalIndex()

		// Remove from transport's session maps
		t.mu.Lock()
		delete(t.sessions, localIndex)
		if existing, ok := t.peerSessions[peerName]; ok && existing.LocalIndex() == localIndex {
			delete(t.peerSessions, peerName)
		}
		t.mu.Unlock()

		// Close the session
		_ = s.Close()

		log.Info().
			Str("peer", peerName).
			Uint32("local_index", localIndex).
			Msg("removed dead session due to receive timeout")

		// Trigger reconnection
		if cb != nil {
			cb(peerName)
		}
	}
}

// Dial creates an outbound connection to a peer.
func (t *Transport) Dial(ctx context.Context, opts transport.DialOptions) (transport.Connection, error) {
	log.Debug().
		Str("peer", opts.PeerName).
		Bool("has_peer_info", opts.PeerInfo != nil).
		Msg("UDP Dial called")

	if !t.running.Load() {
		// Start transport if not already running
		if err := t.Start(); err != nil {
			return nil, err
		}
	}

	// Get peer's current external endpoint from coordination server
	// This is the authoritative source for where the peer currently is
	peerEndpoint, err := t.getPeerEndpoint(ctx, opts.PeerName)
	if err != nil {
		return nil, fmt.Errorf("get peer endpoint: %w", err)
	}

	// Check if peer is on the same network (same public IP = same NAT)
	// If so, use private IPs to avoid NAT hairpinning issues
	// IMPORTANT: Use the fresh external address from coordination, not cached PeerInfo
	if opts.PeerInfo != nil && len(opts.PeerInfo.PrivateIPs) > 0 && opts.PeerInfo.UDPPort > 0 {
		// Extract peer's current public IP from external endpoint (host:port format)
		peerExternalHost, _, _ := net.SplitHostPort(peerEndpoint)
		if peerExternalHost != "" {
			// Get our current public IPs freshly to handle network changes
			ourPublicIPs, _, _ := proto.GetLocalIPs()
			for _, ourPublicIP := range ourPublicIPs {
				if ourPublicIP == peerExternalHost {
					// Same public IP means same LAN - use private IP to avoid hairpinning
					privateEndpoint := net.JoinHostPort(opts.PeerInfo.PrivateIPs[0], fmt.Sprint(opts.PeerInfo.UDPPort))
					log.Debug().
						Str("peer", opts.PeerName).
						Str("our_public_ip", ourPublicIP).
						Str("peer_external_ip", peerExternalHost).
						Str("using_private_addr", privateEndpoint).
						Msg("detected same-network peer, using private IP to avoid NAT hairpinning")
					peerEndpoint = privateEndpoint
					break
				}
			}
		}
	}

	// Resolve peer address
	peerAddr, err := net.ResolveUDPAddr("udp", peerEndpoint)
	if err != nil {
		return nil, fmt.Errorf("resolve peer address: %w", err)
	}

	// Select the appropriate socket based on peer's address family
	conn := t.selectSocketForPeer(peerAddr)
	if conn == nil {
		peerIsIPv4 := peerAddr.IP.To4() != nil
		return nil, fmt.Errorf("no %s socket available for peer %s",
			map[bool]string{true: "IPv4", false: "IPv6"}[peerIsIPv4],
			opts.PeerName)
	}

	// Get peer's X25519 public key from ED25519 public key
	var peerPublic [32]byte
	if opts.PeerInfo != nil && opts.PeerInfo.PublicKey != "" {
		sshPubKey, err := config.DecodePublicKey(opts.PeerInfo.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("decode peer public key: %w", err)
		}

		// Extract the raw ED25519 key and convert to X25519
		ed25519Key := sshPubKey.Marshal()
		// SSH format: 4 bytes length + "ssh-ed25519" + 4 bytes length + 32 bytes key
		// Skip the SSH wire format header to get raw ED25519 key
		keyLen := len(ed25519Key)
		if keyLen < 32 {
			return nil, fmt.Errorf("invalid ED25519 key length: %d", keyLen)
		}
		rawED25519 := ed25519Key[keyLen-32:]

		log.Debug().
			Int("ssh_key_len", keyLen).
			Hex("raw_ed25519", rawED25519).
			Str("key_type", sshPubKey.Type()).
			Msg("extracting ED25519 public key for X25519 conversion")

		x25519Key, err := config.ED25519PublicToX25519(rawED25519)
		if err != nil {
			return nil, fmt.Errorf("convert ED25519 to X25519: %w", err)
		}

		log.Debug().
			Hex("x25519_pub", x25519Key).
			Str("peer", opts.PeerName).
			Msg("converted ED25519 to X25519 public key")

		copy(peerPublic[:], x25519Key)
	} else {
		return nil, fmt.Errorf("peer public key not available")
	}

	// Check if we can skip hole-punching
	// When both peers have PCP/NAT-PMP mappings, their external addresses
	// are directly reachable without hole-punching
	weHavePCP := t.HasPCPMapping()
	peerHasPCP := opts.PeerInfo != nil && opts.PeerInfo.PCPMapped

	if weHavePCP && peerHasPCP {
		log.Debug().
			Str("peer", opts.PeerName).
			Msg("both peers have PCP mapping, skipping hole-punch")
	} else {
		// Attempt hole-punch if needed
		if err := t.holePunch(ctx, opts.PeerName, peerAddr, conn); err != nil {
			log.Debug().Err(err).Str("peer", opts.PeerName).Msg("hole-punch failed, trying direct")
		}
	}

	// Perform handshake
	session, err := t.initiateHandshake(ctx, opts.PeerName, peerPublic, peerAddr, conn)
	if err != nil {
		return nil, fmt.Errorf("handshake failed: %w", err)
	}

	// Create connection wrapper
	return &Connection{
		session: session,
		readBuf: new(bytes.Buffer),
	}, nil
}

// initiateHandshake performs the Noise IK handshake as initiator.
func (t *Transport) initiateHandshake(ctx context.Context, peerName string, peerPublic [32]byte, peerAddr *net.UDPAddr, conn *net.UDPConn) (*Session, error) {
	ctx, span := tracing.Tracer("tunnelmesh/udp").Start(ctx, "udp.handshake.initiate")
	defer span.End()
	span.SetAttributes(
		attribute.String("peer.name", peerName),
		attribute.String("peer.addr", peerAddr.String()),
	)
	// Register intent to connect BEFORE creating handshake state.
	// This closes a race window where an incoming init could be processed
	// before pendingOutboundPeers is set, bypassing the crossing handshake
	// tie-breaker logic in handleHandshakeInit.
	// We use 0 as a placeholder index until the real index is known.
	t.mu.Lock()
	t.pendingOutboundPeers[peerName] = 0
	t.mu.Unlock()

	hs, err := NewInitiatorHandshake(t.staticPrivate, t.staticPublic, peerPublic)
	if err != nil {
		t.mu.Lock()
		delete(t.pendingOutboundPeers, peerName)
		t.mu.Unlock()
		return nil, err
	}

	// Create initiation message
	initMsg, err := hs.CreateInitiation()
	if err != nil {
		t.mu.Lock()
		delete(t.pendingOutboundPeers, peerName)
		t.mu.Unlock()
		return nil, err
	}

	// Register pending handshake before sending
	respChan := make(chan handshakeResponse, 1)
	localIndex := hs.LocalIndex()

	t.mu.Lock()
	t.pendingHandshakes[localIndex] = respChan
	t.pendingOutboundPeers[peerName] = localIndex // Update with real index
	t.mu.Unlock()

	// Clean up pending handshake when done
	defer func() {
		t.mu.Lock()
		delete(t.pendingHandshakes, localIndex)
		delete(t.pendingOutboundPeers, peerName)
		t.mu.Unlock()
	}()

	// Prepend packet type header
	packet := make([]byte, 1+len(initMsg))
	packet[0] = PacketTypeHandshakeInit
	copy(packet[1:], initMsg)

	// Send initiation
	if _, err := conn.WriteToUDP(packet, peerAddr); err != nil {
		return nil, err
	}

	// Wait for response with timeout
	timeout := t.config.HandshakeTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}

	// Wait for response via channel (routed by receiveLoop)
	select {
	case <-ctx.Done():
		span.RecordError(ctx.Err())
		span.SetStatus(otelcodes.Error, ctx.Err().Error())
		return nil, ctx.Err()
	case <-time.After(timeout):
		timeoutErr := fmt.Errorf("handshake timeout")
		span.RecordError(timeoutErr)
		span.SetStatus(otelcodes.Error, "handshake timeout")
		return nil, timeoutErr
	case resp := <-respChan:
		// Verify it's from the expected peer
		if resp.remoteAddr.String() != peerAddr.String() {
			return nil, fmt.Errorf("response from unexpected peer: %s", resp.remoteAddr)
		}

		// Process response
		if err := hs.ConsumeResponse(resp.data); err != nil {
			return nil, fmt.Errorf("invalid response: %w", err)
		}
	}

	// Derive keys
	sendKey, recvKey, err := hs.DeriveKeys()
	if err != nil {
		return nil, err
	}

	crypto, err := NewCryptoState(sendKey, recvKey)
	if err != nil {
		return nil, err
	}

	// Create session using the same socket used for handshake
	session := NewSession(SessionConfig{
		LocalIndex: hs.LocalIndex(),
		PeerName:   peerName,
		PeerPublic: peerPublic,
		RemoteAddr: peerAddr,
		Conn:       conn,
	})
	session.SetCrypto(crypto, hs.RemoteIndex())
	session.SetOnClose(t.removeSession)

	// Register session, keeping any existing active session for this peer
	if !t.registerSession(session) {
		// Session was discarded because there's already an active session
		// Return the existing session instead
		t.mu.RLock()
		existingSession := t.peerSessions[peerName]
		t.mu.RUnlock()

		if existingSession != nil {
			log.Debug().
				Str("peer", peerName).
				Str("addr", peerAddr.String()).
				Msg("handshake completed but using existing active session")
			// Return a connection wrapping the existing session
			// Note: We return a new Connection wrapper but it points to the existing session
			return existingSession, nil
		}
		// This shouldn't happen - registerSession rejected our session but there's no existing one
		return nil, fmt.Errorf("session registration rejected but no existing session found")
	}

	sc := span.SpanContext()
	log.Debug().
		Str("peer", peerName).
		Str("addr", peerAddr.String()).
		Str("trace_id", sc.TraceID().String()).
		Str("span_id", sc.SpanID().String()).
		Msg("handshake completed")

	span.SetStatus(otelcodes.Ok, "")
	return session, nil
}

// holePunch attempts to establish NAT traversal with the peer.
func (t *Transport) holePunch(ctx context.Context, peerName string, peerAddr *net.UDPAddr, conn *net.UDPConn) error {
	if t.config.CoordServerURL == "" {
		return nil // No coordination server, skip hole-punch
	}

	// Get our stored external address for the appropriate address family
	t.mu.RLock()
	externalAddr := t.externalAddr
	if peerAddr.IP.To4() == nil && t.externalAddr6 != "" {
		externalAddr = t.externalAddr6
	}
	t.mu.RUnlock()

	// Request hole-punch coordination from server
	reqBody := map[string]string{
		"from_peer":     t.config.LocalPeerName,
		"to_peer":       peerName,
		"local_addr":    conn.LocalAddr().String(),
		"external_addr": externalAddr,
	}

	log.Debug().
		Str("from", t.config.LocalPeerName).
		Str("to", peerName).
		Str("our_external", externalAddr).
		Str("peer_addr", peerAddr.String()).
		Msg("initiating hole-punch coordination")

	bodyBytes, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, "POST",
		t.config.CoordServerURL+"/api/v1/udp/holepunch",
		bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if t.config.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+t.config.AuthToken)
	}
	tracing.InjectHeaders(ctx, req.Header)

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	// Parse hole-punch response
	var hpResp struct {
		OK            bool   `json:"ok"`
		Ready         bool   `json:"ready"`
		PeerAddr      string `json:"peer_addr"`
		PeerLocalAddr string `json:"peer_local_addr"`
		Message       string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&hpResp); err != nil {
		return fmt.Errorf("decode hole-punch response: %w", err)
	}

	if !hpResp.Ready {
		log.Debug().
			Str("peer", peerName).
			Str("message", hpResp.Message).
			Msg("peer not ready for hole-punch")
		return fmt.Errorf("peer not ready: %s", hpResp.Message)
	}

	log.Debug().
		Str("peer", peerName).
		Str("peer_external", hpResp.PeerAddr).
		Msg("hole-punch coordination successful, sending packets")

	// Send hole-punch packets
	for i := 0; i < t.config.HolePunchRetries; i++ {
		// Send a small packet to punch the hole
		_, _ = conn.WriteToUDP([]byte{0}, peerAddr)
		time.Sleep(100 * time.Millisecond)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

// getPeerEndpoint retrieves the peer's UDP endpoint from coordination server.
// Returns the endpoint address that matches local connectivity (IPv4 or IPv6).
func (t *Transport) getPeerEndpoint(ctx context.Context, peerName string) (string, error) {
	if t.config.CoordServerURL == "" {
		return "", fmt.Errorf("coordination server URL not configured")
	}

	url := t.config.CoordServerURL + "/api/v1/udp/endpoint/" + peerName
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}

	if t.config.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+t.config.AuthToken)
	}
	tracing.InjectHeaders(ctx, req.Header)

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get endpoint: %s", resp.Status)
	}

	var endpoint struct {
		ExternalAddr4 string `json:"external_addr4"` // IPv4 address
		ExternalAddr6 string `json:"external_addr6"` // IPv6 address
		LocalAddr     string `json:"local_addr"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&endpoint); err != nil {
		return "", err
	}

	// Select address based on local connectivity
	// Prefer IPv4 if we have an IPv4 socket (more reliable NAT traversal)
	if t.conn != nil && endpoint.ExternalAddr4 != "" {
		log.Debug().
			Str("peer", peerName).
			Str("addr", endpoint.ExternalAddr4).
			Msg("selected IPv4 endpoint for peer")
		return endpoint.ExternalAddr4, nil
	}

	// Use IPv6 if we have an IPv6 socket, successfully registered our IPv6, and peer has IPv6
	t.mu.RLock()
	haveIPv6External := t.externalAddr6 != ""
	t.mu.RUnlock()
	if t.conn6 != nil && haveIPv6External && endpoint.ExternalAddr6 != "" {
		log.Debug().
			Str("peer", peerName).
			Str("addr", endpoint.ExternalAddr6).
			Msg("selected IPv6 endpoint for peer")
		return endpoint.ExternalAddr6, nil
	}

	// Fall back to local address
	if endpoint.LocalAddr != "" {
		return endpoint.LocalAddr, nil
	}

	return "", fmt.Errorf("no compatible endpoint for peer %s (have IPv4: %v, have IPv6: %v, peer IPv4: %v, peer IPv6: %v)",
		peerName, t.conn != nil, t.conn6 != nil, endpoint.ExternalAddr4 != "", endpoint.ExternalAddr6 != "")
}

// Listen returns a listener for incoming connections.
func (t *Transport) Listen(ctx context.Context, opts transport.ListenOptions) (transport.Listener, error) {
	if !t.running.Load() {
		if err := t.Start(); err != nil {
			return nil, err
		}
	}

	t.listener = &Listener{
		transport: t,
		acceptCh:  make(chan *Connection, 16),
	}

	return t.listener, nil
}

// Probe tests if the peer is reachable via UDP.
func (t *Transport) Probe(ctx context.Context, opts transport.ProbeOptions) (time.Duration, error) {
	// For UDP, we need to check if the peer has registered their endpoint
	if t.config.CoordServerURL == "" {
		return 0, fmt.Errorf("coordination server not configured")
	}

	if opts.PeerInfo == nil {
		return 0, fmt.Errorf("peer info required")
	}

	start := time.Now()

	// Check if peer has UDP endpoint registered
	_, err := t.getPeerEndpoint(ctx, opts.PeerInfo.Name)
	if err != nil {
		return 0, err
	}

	// Return estimated latency (we don't actually probe the peer yet)
	// A real implementation would send a probe packet and measure RTT
	return time.Since(start) + 10*time.Millisecond, nil
}

// Close shuts down the transport.
func (t *Transport) Close() error {
	if !t.running.Load() {
		return nil
	}

	t.running.Store(false)
	close(t.closeCh)

	// Stop port mapper if running
	if t.portMapper != nil {
		_ = t.portMapper.Stop()
	}

	// Close packet queue to signal workers to exit
	if t.packetQueue != nil {
		close(t.packetQueue)
	}

	t.mu.Lock()
	for _, s := range t.sessions {
		_ = s.Close()
	}
	t.sessions = make(map[uint32]*Session)
	t.peerSessions = make(map[string]*Session)
	t.mu.Unlock()

	if t.conn != nil {
		_ = t.conn.Close()
	}
	if t.conn6 != nil {
		_ = t.conn6.Close()
	}

	return nil
}

// removeSession removes a session from the transport's maps.
// This is called by the session's onClose callback.
func (t *Transport) removeSession(localIndex uint32, peerName string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.sessions, localIndex)
	// Only delete from peerSessions if it still points to this session
	// (a new session may have already replaced it)
	if existing, ok := t.peerSessions[peerName]; ok {
		if existing.LocalIndex() == localIndex {
			delete(t.peerSessions, peerName)
		}
	}
}

// registerSession adds a session to the transport's maps, closing any existing
// session for the same peer to prevent orphaned sessions and memory leaks.
// If there's an active session (received data recently), it will be kept and
// the new session will be discarded to prevent disrupting ongoing communication.
// Returns true if the session was registered, false if it was discarded.
// Must be called WITHOUT holding t.mu.
func (t *Transport) registerSession(session *Session) bool {
	peerName := session.PeerName()

	t.mu.Lock()
	// Check for existing session to this peer
	var oldSession *Session
	var discardNew bool
	lastNetChange := t.lastNetworkChange
	if existing, ok := t.peerSessions[peerName]; ok {
		if existing.LocalIndex() != session.LocalIndex() {
			// Check if existing session is active (received data recently)
			if existing.IsEstablished() {
				lastRecv := existing.LastReceive()
				sessionAge := time.Since(lastRecv)
				// Session is active if we received data recently AND after any network change
				sessionActive := sessionAge < 30*time.Second && lastRecv.After(lastNetChange)
				if sessionActive {
					// Existing session is active - discard the new one
					log.Debug().
						Str("peer", peerName).
						Uint32("existing_index", existing.LocalIndex()).
						Uint32("new_index", session.LocalIndex()).
						Dur("session_age", sessionAge).
						Msg("keeping active session, discarding new handshake")
					discardNew = true
				}
			}
			if !discardNew {
				oldSession = existing
				// Remove old session from sessions map
				delete(t.sessions, existing.LocalIndex())
			}
		}
	}

	if discardNew {
		t.mu.Unlock()
		// Close the new session since we're keeping the old one
		_ = session.Close()
		return false
	}

	t.sessions[session.LocalIndex()] = session
	t.peerSessions[peerName] = session
	t.mu.Unlock()

	// Close old session outside lock to avoid deadlock
	if oldSession != nil {
		log.Debug().
			Str("peer", peerName).
			Uint32("old_index", oldSession.LocalIndex()).
			Uint32("new_index", session.LocalIndex()).
			Msg("replacing existing session for peer")
		_ = oldSession.Close()
	}

	return true
}

// ClearNetworkState clears cached network state such as STUN-discovered external addresses.
// This should be called after network changes to ensure fresh address discovery.
// Also records the time of network change to invalidate stale sessions.
func (t *Transport) ClearNetworkState() {
	t.mu.Lock()
	log.Debug().
		Str("old_ipv4", t.externalAddr).
		Str("old_ipv6", t.externalAddr6).
		Msg("clearing cached UDP external addresses")

	t.externalAddr = ""
	t.externalAddr6 = ""
	t.lastNetworkChange = time.Now()
	t.mu.Unlock()

	// Close idle HTTP connections to force fresh connections on new network
	t.httpClient.CloseIdleConnections()

	// Signal network change to port mapper (will re-discover and re-map)
	if t.portMapper != nil {
		t.portMapper.NetworkChanged()
	}
}

// RefreshEndpoint re-registers the UDP endpoint with the coordination server.
// Implements the transport.EndpointRefresher interface.
func (t *Transport) RefreshEndpoint(ctx context.Context, peerName string) error {
	return t.RegisterUDPEndpoint(ctx, peerName)
}

// Connection wraps a UDP session as a transport.Connection.
type Connection struct {
	session *Session
	readBuf *bytes.Buffer
	mu      sync.Mutex
}

// Read reads data from the connection.
func (c *Connection) Read(p []byte) (int, error) {
	// Check buffer first (with lock)
	c.mu.Lock()
	if c.readBuf.Len() > 0 {
		n, err := c.readBuf.Read(p)
		c.mu.Unlock()
		return n, err
	}
	c.mu.Unlock()

	// Wait for new data without holding lock to avoid deadlock
	data, ok := <-c.session.Recv()
	if !ok {
		return 0, io.EOF
	}

	// Write to buffer and read (with lock)
	c.mu.Lock()
	c.readBuf.Write(data)
	n, err := c.readBuf.Read(p)
	c.mu.Unlock()
	return n, err
}

// Write writes data to the connection.
func (c *Connection) Write(p []byte) (int, error) {
	if err := c.session.Send(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close closes the connection.
func (c *Connection) Close() error {
	return c.session.Close()
}

// PeerName returns the peer name.
func (c *Connection) PeerName() string {
	return c.session.PeerName()
}

// Type returns the transport type.
func (c *Connection) Type() transport.TransportType {
	return transport.TransportUDP
}

// LocalAddr returns the local address.
func (c *Connection) LocalAddr() net.Addr {
	return nil // UDP doesn't have a specific local address per connection
}

// RemoteAddr returns the remote address.
func (c *Connection) RemoteAddr() net.Addr {
	return c.session.RemoteAddr()
}

// IsHealthy returns true if the UDP session is established and receiving keepalives.
func (c *Connection) IsHealthy() bool {
	if c.session.State() != SessionStateEstablished {
		return false
	}
	// Session is healthy if keepalives (or data) have been received recently.
	// Use the session timeout (75s) as the threshold — if we haven't heard
	// anything in that long, the UDP session itself will time out.
	return time.Since(c.session.LastReceive()) < 75*time.Second
}

// Listener accepts incoming UDP connections.
type Listener struct {
	transport *Transport
	acceptCh  chan *Connection
	closed    atomic.Bool
	mu        sync.Mutex // Protects channel operations to prevent send-on-closed-channel panic
}

// Accept waits for and returns the next connection.
func (l *Listener) Accept(ctx context.Context) (transport.Connection, error) {
	select {
	case conn, ok := <-l.acceptCh:
		if !ok {
			return nil, fmt.Errorf("listener closed")
		}
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Addr returns the listener's address.
func (l *Listener) Addr() net.Addr {
	if l.transport.conn != nil {
		return l.transport.conn.LocalAddr()
	}
	return nil
}

// Close closes the listener.
func (l *Listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed.Load() {
		return nil
	}
	l.closed.Store(true)
	close(l.acceptCh)
	return nil
}

// trySend attempts to send a connection to the accept channel.
// Returns true if sent, false if listener is closed or channel is full.
func (l *Listener) trySend(conn *Connection) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed.Load() {
		return false
	}
	select {
	case l.acceptCh <- conn:
		return true
	default:
		return false
	}
}

// RegisterUDPEndpoint registers our UDP endpoint with the coordination server.
// Attempts to register via both IPv4 and IPv6 to support dual-stack connectivity.
func (t *Transport) RegisterUDPEndpoint(ctx context.Context, peerName string) error {
	if t.config.CoordServerURL == "" {
		return fmt.Errorf("coordination server URL not configured")
	}

	// Get local port
	port := t.config.Port
	if t.conn != nil {
		if addr, ok := t.conn.LocalAddr().(*net.UDPAddr); ok {
			port = addr.Port
		}
	}

	var lastErr error
	registered := false

	// Try to register via IPv4 if we have an IPv4 socket
	if t.conn != nil {
		localAddr := t.conn.LocalAddr().String()
		if err := t.registerEndpointVia(ctx, peerName, localAddr, port, "tcp4"); err != nil {
			log.Debug().Err(err).Msg("IPv4 UDP registration failed")
			lastErr = err
		} else {
			registered = true
		}
	}

	// Try to register via IPv6 if we have an IPv6 socket
	if t.conn6 != nil {
		localAddr := t.conn6.LocalAddr().String()
		if err := t.registerEndpointVia(ctx, peerName, localAddr, port, "tcp6"); err != nil {
			log.Debug().Err(err).Msg("IPv6 UDP registration failed")
			if lastErr == nil {
				lastErr = err
			}
		} else {
			registered = true
		}
	}

	if !registered {
		if lastErr != nil {
			return lastErr
		}
		return fmt.Errorf("no UDP sockets available for registration")
	}

	return nil
}

// registerEndpointVia registers endpoint via specific network (tcp4 or tcp6).
func (t *Transport) registerEndpointVia(ctx context.Context, peerName, localAddr string, port int, network string) error {
	reqBody := map[string]interface{}{
		"peer_name":  peerName,
		"local_addr": localAddr,
		"udp_port":   port,
		"pcp_mapped": t.HasPCPMapping(),
	}

	bodyBytes, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, "POST",
		t.config.CoordServerURL+"/api/v1/udp/register",
		bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if t.config.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+t.config.AuthToken)
	}

	// Create HTTP client that forces specific network
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				d := net.Dialer{}
				return d.DialContext(ctx, network, addr)
			},
		},
		Timeout: 10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register failed: %s - %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var result struct {
		OK           bool   `json:"ok"`
		ExternalAddr string `json:"external_addr"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	// Store our external address for hole-punching
	t.mu.Lock()
	// Parse to determine address family and store in appropriate field
	host, _, parseErr := net.SplitHostPort(result.ExternalAddr)
	if parseErr == nil {
		ip := net.ParseIP(host)
		if ip != nil && ip.To4() != nil {
			t.externalAddr = result.ExternalAddr
		} else if ip != nil {
			t.externalAddr6 = result.ExternalAddr
		}
	}
	t.mu.Unlock()

	log.Debug().
		Str("network", network).
		Str("external_addr", result.ExternalAddr).
		Msg("UDP endpoint registered")

	return nil
}
