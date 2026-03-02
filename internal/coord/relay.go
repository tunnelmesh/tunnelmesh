package coord

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
)

// relayPacketPool pools relay packet buffers to reduce GC pressure.
// Max packet size is ~65KB, plus header overhead.
var relayPacketPool = sync.Pool{
	New: func() interface{} {
		// Allocate buffer for max relay message: type(1) + name_len(1) + name(255) + data(65535)
		buf := make([]byte, 65792)
		return &buf
	},
}

// relayConn represents a peer's relay connection.
type relayConn struct {
	peerName   string
	targetPeer string
	conn       *websocket.Conn
	paired     chan struct{} // closed when paired
}

// persistentConn represents a peer's persistent relay connection (DERP-like).
type persistentConn struct {
	peerName  string
	conn      *websocket.Conn
	writeChan chan []byte   // buffered channel for async writes
	closeChan chan struct{} // signals writer goroutine to stop
	closed    bool
	closeMu   sync.Mutex
}

// relayManager handles relay connections between peers.
type relayManager struct {
	pending    map[string]*relayConn      // key: "peerA->peerB" (legacy pairing)
	persistent map[string]*persistentConn // key: peerName (DERP-like routing)
	mu         sync.Mutex

	// Request tracking (for QueryFilterRules)
	apiRequests   map[uint32]chan []byte // reqID -> response channel
	apiRequestsMu sync.Mutex
	nextReqID     uint32
}

// Persistent relay message types
const (
	MsgTypeSendPacket      byte = 0x01 // Client -> Server: send packet to target peer
	MsgTypeRecvPacket      byte = 0x02 // Server -> Client: received packet from source peer
	MsgTypePing            byte = 0x03 // Keepalive ping
	MsgTypePong            byte = 0x04 // Keepalive pong
	MsgTypePeerReconnected byte = 0x05 // Server -> Client: peer reconnected (tunnel may be stale)

	// Heartbeat and push notification message types
	MsgTypeHeartbeat       byte = 0x20 // Client -> Server: stats update
	MsgTypeHeartbeatAck    byte = 0x21 // Server -> Client: heartbeat acknowledged
	MsgTypeRelayNotify     byte = 0x22 // Server -> Client: relay request notification
	MsgTypeHolePunchNotify byte = 0x23 // Server -> Client: hole-punch request notification

	// Packet filter message types
	MsgTypeFilterRulesSync   byte = 0x30 // Server -> Client: full sync of coordinator rules
	MsgTypeFilterRuleAdd     byte = 0x31 // Server -> Client: add single temporary rule
	MsgTypeFilterRuleRemove  byte = 0x32 // Server -> Client: remove single rule
	MsgTypeServicePortNotify byte = 0x33 // Server -> Client: coordinator service port announcement
	MsgTypeFilterRulesQuery  byte = 0x34 // Server -> Client: request current filter rules
	MsgTypeFilterRulesReply  byte = 0x35 // Client -> Server: response with filter rules

	// Coordinator discovery message types
	MsgTypeCoordListUpdate byte = 0x36 // Server -> Client: updated coordinator IP list
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  16384,
	WriteBufferSize: 16384,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for relay connections
	},
}

// newRelayManager creates a new relay manager.
// ctx is the server lifecycle context used for background operations.
func newRelayManager(ctx context.Context) *relayManager {
	// Note: ctx parameter kept for future use (e.g., initializing background workers)
	// but not stored in struct per Go best practices
	_ = ctx
	return &relayManager{
		pending:     make(map[string]*relayConn),
		persistent:  make(map[string]*persistentConn),
		apiRequests: make(map[uint32]chan []byte),
	}
}

// QueryFilterRules sends a filter rules query to a peer and waits for response.
// Returns the JSON-encoded filter rules or error if timeout/peer not connected.
func (r *relayManager) QueryFilterRules(ctx context.Context, peerName string, timeout time.Duration) ([]byte, error) {
	r.mu.Lock()
	pc, ok := r.persistent[peerName]
	r.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("peer not connected: %s", peerName)
	}

	// Allocate request ID and response channel (reuse apiRequests map)
	r.apiRequestsMu.Lock()
	reqID := r.nextReqID
	r.nextReqID++
	respChan := make(chan []byte, 1)
	r.apiRequests[reqID] = respChan
	r.apiRequestsMu.Unlock()

	defer func() {
		r.apiRequestsMu.Lock()
		delete(r.apiRequests, reqID)
		r.apiRequestsMu.Unlock()
	}()

	// Build request: [MsgTypeFilterRulesQuery][reqID:4]
	msg := make([]byte, 5)
	msg[0] = MsgTypeFilterRulesQuery
	msg[1] = byte(reqID >> 24)
	msg[2] = byte(reqID >> 16)
	msg[3] = byte(reqID >> 8)
	msg[4] = byte(reqID)

	// Send request via async write channel
	select {
	case pc.writeChan <- msg:
		// Message queued
	default:
		return nil, fmt.Errorf("query filter rules: write channel full")
	}

	// Combine parent context with timeout
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Wait for response
	select {
	case resp := <-respChan:
		return resp, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("query filter rules from %s: %w", peerName, ctx.Err())
	}
}

// handleFilterRulesReply processes a filter rules response from a peer.
func (r *relayManager) handleFilterRulesReply(data []byte) {
	if len(data) < 5 {
		log.Debug().Msg("filter rules reply too short")
		return
	}

	reqID := uint32(data[1])<<24 | uint32(data[2])<<16 | uint32(data[3])<<8 | uint32(data[4])
	body := data[5:]

	r.apiRequestsMu.Lock()
	respChan, ok := r.apiRequests[reqID]
	r.apiRequestsMu.Unlock()

	if ok {
		select {
		case respChan <- body:
		default:
			log.Debug().Uint32("req_id", reqID).Msg("filter rules reply channel full")
		}
	} else {
		log.Debug().Uint32("req_id", reqID).Msg("filter rules reply for unknown request")
	}
}

// RegisterPersistent registers a peer's persistent relay connection.
func (r *relayManager) RegisterPersistent(peerName string, conn *websocket.Conn) *persistentConn {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Close existing connection if any
	if existing, ok := r.persistent[peerName]; ok {
		existing.Close()
	}

	pc := &persistentConn{
		peerName:  peerName,
		conn:      conn,
		writeChan: make(chan []byte, 128), // Buffered channel to prevent blocking
		closeChan: make(chan struct{}),
	}
	r.persistent[peerName] = pc

	// Start the writer goroutine
	go pc.writeLoop()

	return pc
}

// writeLoop processes queued writes to avoid blocking senders.
func (pc *persistentConn) writeLoop() {
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-pc.closeChan:
			return
		case <-pingTicker.C:
			if err := pc.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
				log.Debug().Err(err).Str("peer", pc.peerName).Msg("persistent relay ping failed")
				return
			}
		case data := <-pc.writeChan:
			if err := pc.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				log.Debug().Err(err).Str("peer", pc.peerName).Msg("persistent relay write failed")
				// Only return buffer to pool if it's a pool-sized buffer
				// (small buffers like heartbeat ack are allocated inline)
				if cap(data) >= 65792 {
					relayPacketPool.Put(&data)
				}
				return
			}
			// Only return buffer to pool if it's a pool-sized buffer
			if cap(data) >= 65792 {
				relayPacketPool.Put(&data)
			}
		}
	}
}

// Close closes the persistent connection and stops the writer goroutine.
func (pc *persistentConn) Close() {
	pc.closeMu.Lock()
	defer pc.closeMu.Unlock()
	if pc.closed {
		return
	}
	pc.closed = true
	close(pc.closeChan)
	_ = pc.conn.Close()
}

// UnregisterPersistent removes a peer's persistent relay connection.
func (r *relayManager) UnregisterPersistent(peerName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.persistent, peerName)
}

// GetPersistent returns a peer's persistent connection if connected.
func (r *relayManager) GetPersistent(peerName string) (*persistentConn, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pc, ok := r.persistent[peerName]
	return pc, ok
}

// GetConnectedPeerNames returns the names of all connected peers.
func (r *relayManager) GetConnectedPeerNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.persistent))
	for name := range r.persistent {
		names = append(names, name)
	}
	return names
}

// BroadcastPeerReconnected notifies all other connected peers that a peer has reconnected.
// Clients may use this to re-evaluate their connection state to that peer.
func (r *relayManager) BroadcastPeerReconnected(reconnectedPeer string) {
	r.mu.Lock()
	peers := make([]*persistentConn, 0, len(r.persistent))
	for name, pc := range r.persistent {
		if name != reconnectedPeer {
			peers = append(peers, pc)
		}
	}
	r.mu.Unlock()

	for _, pc := range peers {
		// Get buffer from pool for each peer (they'll be returned after write)
		bufPtr := relayPacketPool.Get().(*[]byte)
		buf := *bufPtr

		// Build message: [MsgTypePeerReconnected][name len][name]
		msgLen := 2 + len(reconnectedPeer)
		msg := buf[:msgLen]
		msg[0] = MsgTypePeerReconnected
		msg[1] = byte(len(reconnectedPeer))
		copy(msg[2:], reconnectedPeer)

		// Non-blocking send to write channel
		select {
		case pc.writeChan <- msg:
			log.Debug().
				Str("target", pc.peerName).
				Str("reconnected", reconnectedPeer).
				Msg("queued peer reconnected notification")
		default:
			relayPacketPool.Put(bufPtr)
			log.Debug().
				Str("target", pc.peerName).
				Str("reconnected", reconnectedPeer).
				Msg("failed to queue peer reconnected notification (channel full)")
		}
	}
}

// SendToPeer sends a packet to a peer via their persistent connection.
// Returns true if the peer is connected and the packet was sent.
func (pc *persistentConn) SendPacket(sourcePeer string, data []byte) error {
	pc.closeMu.Lock()
	if pc.closed {
		pc.closeMu.Unlock()
		return io.EOF
	}
	pc.closeMu.Unlock()

	// Get buffer from pool
	bufPtr := relayPacketPool.Get().(*[]byte)
	buf := *bufPtr

	// Build message: [MsgTypeRecvPacket][source name len][source name][data]
	msgLen := 2 + len(sourcePeer) + len(data)
	if msgLen > len(buf) {
		// Message too large for pooled buffer, allocate new one
		relayPacketPool.Put(bufPtr)
		buf = make([]byte, msgLen)
		bufPtr = &buf
	}

	msg := buf[:msgLen]
	msg[0] = MsgTypeRecvPacket
	msg[1] = byte(len(sourcePeer))
	copy(msg[2:], sourcePeer)
	copy(msg[2+len(sourcePeer):], data)

	// Send to write channel (non-blocking with timeout)
	select {
	case pc.writeChan <- msg:
		return nil
	default:
		// Channel full - drop packet to avoid blocking
		relayPacketPool.Put(bufPtr)
		log.Warn().Str("peer", pc.peerName).Msg("relay write channel full, dropping packet")
		return nil
	}
}

// GetPendingRequestsFor returns the list of peers waiting on relay for the given peer.
// For example, if "oldie" is waiting on relay for "roku", calling GetPendingRequestsFor("roku")
// returns ["oldie"].
func (r *relayManager) GetPendingRequestsFor(peerName string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var requests []string
	suffix := "->" + peerName
	for key, conn := range r.pending {
		if len(key) > len(suffix) && key[len(key)-len(suffix):] == suffix {
			requests = append(requests, conn.peerName)
		}
	}
	return requests
}

// NotifyRelayRequest pushes a relay notification to a peer via their persistent connection.
// This tells the peer that other peers are waiting for relay connections.
// nolint:dupl // Forward and reverse relay setup follow similar patterns
func (r *relayManager) NotifyRelayRequest(peerName string, waitingPeers []string) {
	if len(waitingPeers) == 0 {
		return
	}

	r.mu.Lock()
	pc, ok := r.persistent[peerName]
	r.mu.Unlock()

	if !ok {
		log.Debug().Str("peer", peerName).Msg("cannot notify relay request: peer not connected")
		return
	}

	// Build message: [MsgTypeRelayNotify][count:1][name_len:1][name]...
	msgLen := 2 // type + count
	for _, p := range waitingPeers {
		msgLen += 1 + len(p) // name_len + name
	}

	msg := make([]byte, msgLen)
	msg[0] = MsgTypeRelayNotify
	msg[1] = byte(len(waitingPeers))
	offset := 2
	for _, p := range waitingPeers {
		msg[offset] = byte(len(p))
		copy(msg[offset+1:], p)
		offset += 1 + len(p)
	}

	// Non-blocking send to write channel
	select {
	case pc.writeChan <- msg:
		log.Debug().
			Str("target", peerName).
			Strs("waiting", waitingPeers).
			Msg("sent relay notification")
	default:
		log.Debug().
			Str("target", peerName).
			Msg("failed to send relay notification: channel full")
	}
}

// NotifyHolePunch pushes a hole-punch notification to a peer via their persistent connection.
// This tells the peer that other peers want to hole-punch with them.
// nolint:dupl // Companion relay method
func (r *relayManager) NotifyHolePunch(peerName string, requestingPeers []string) {
	if len(requestingPeers) == 0 {
		return
	}

	r.mu.Lock()
	pc, ok := r.persistent[peerName]
	r.mu.Unlock()

	if !ok {
		log.Debug().Str("peer", peerName).Msg("cannot notify hole-punch: peer not connected")
		return
	}

	// Build message: [MsgTypeHolePunchNotify][count:1][name_len:1][name]...
	msgLen := 2 // type + count
	for _, p := range requestingPeers {
		msgLen += 1 + len(p) // name_len + name
	}

	msg := make([]byte, msgLen)
	msg[0] = MsgTypeHolePunchNotify
	msg[1] = byte(len(requestingPeers))
	offset := 2
	for _, p := range requestingPeers {
		msg[offset] = byte(len(p))
		copy(msg[offset+1:], p)
		offset += 1 + len(p)
	}

	// Non-blocking send to write channel
	select {
	case pc.writeChan <- msg:
		log.Debug().
			Str("target", peerName).
			Strs("requesting", requestingPeers).
			Msg("sent hole-punch notification")
	default:
		log.Debug().
			Str("target", peerName).
			Msg("failed to send hole-punch notification: channel full")
	}
}

// FilterRuleWire is the wire format for a filter rule.
type FilterRuleWire struct {
	Port       uint16 `json:"port"`
	Protocol   string `json:"protocol"`    // "tcp" or "udp"
	Action     string `json:"action"`      // "allow" or "deny"
	SourcePeer string `json:"source_peer"` // Source peer (empty = any peer)
}

// PushFilterRules sends the full set of coordinator filter rules to a peer.
// This is called when a peer connects to sync the global rules.
func (r *relayManager) PushFilterRules(peerName string, rules []config.FilterRule) {
	r.mu.Lock()
	pc, ok := r.persistent[peerName]
	r.mu.Unlock()

	if !ok {
		log.Debug().Str("peer", peerName).Msg("cannot push filter rules: peer not connected")
		return
	}

	// Convert to wire format
	wireRules := make([]FilterRuleWire, len(rules))
	for i, r := range rules {
		wireRules[i] = FilterRuleWire{
			Port:       r.Port,
			Protocol:   r.Protocol,
			Action:     r.Action,
			SourcePeer: r.SourcePeer,
		}
	}

	// Build message: [MsgTypeFilterRulesSync][rules_len:2][rules JSON]
	rulesJSON, err := json.Marshal(wireRules)
	if err != nil {
		log.Error().Err(err).Str("peer", peerName).Msg("failed to marshal filter rules")
		return
	}

	msg := make([]byte, 3+len(rulesJSON))
	msg[0] = MsgTypeFilterRulesSync
	msg[1] = byte(len(rulesJSON) >> 8)
	msg[2] = byte(len(rulesJSON))
	copy(msg[3:], rulesJSON)

	// Non-blocking send to write channel
	select {
	case pc.writeChan <- msg:
		log.Debug().
			Str("peer", peerName).
			Int("rules", len(rules)).
			Msg("pushed filter rules to peer")
	default:
		log.Debug().
			Str("peer", peerName).
			Msg("failed to push filter rules: channel full")
	}
}

// PushFilterRuleAdd sends a single filter rule add to a peer.
// Used when admin panel adds a temporary rule.
func (r *relayManager) PushFilterRuleAdd(peerName string, port uint16, protocol, action, sourcePeer string) {
	r.mu.Lock()
	pc, ok := r.persistent[peerName]
	r.mu.Unlock()

	if !ok {
		log.Debug().Str("peer", peerName).Msg("cannot push filter rule add: peer not connected")
		return
	}

	rule := FilterRuleWire{
		Port:       port,
		Protocol:   protocol,
		Action:     action,
		SourcePeer: sourcePeer,
	}

	ruleJSON, err := json.Marshal(rule)
	if err != nil {
		log.Error().Err(err).Str("peer", peerName).Msg("failed to marshal filter rule")
		return
	}

	// Build message: [MsgTypeFilterRuleAdd][rule_len:2][rule JSON]
	msg := make([]byte, 3+len(ruleJSON))
	msg[0] = MsgTypeFilterRuleAdd
	msg[1] = byte(len(ruleJSON) >> 8)
	msg[2] = byte(len(ruleJSON))
	copy(msg[3:], ruleJSON)

	select {
	case pc.writeChan <- msg:
		log.Debug().
			Str("peer", peerName).
			Uint16("port", port).
			Str("protocol", protocol).
			Str("action", action).
			Str("source_peer", sourcePeer).
			Msg("pushed filter rule add to peer")
	default:
		log.Debug().
			Str("peer", peerName).
			Msg("failed to push filter rule add: channel full")
	}
}

// PushFilterRuleRemove sends a filter rule removal to a peer.
func (r *relayManager) PushFilterRuleRemove(peerName string, port uint16, protocol, sourcePeer string) {
	r.mu.Lock()
	pc, ok := r.persistent[peerName]
	r.mu.Unlock()

	if !ok {
		log.Debug().Str("peer", peerName).Msg("cannot push filter rule remove: peer not connected")
		return
	}

	// Build message: [MsgTypeFilterRuleRemove][port:2][protocol_len:1][protocol][source_peer_len:1][source_peer]
	msg := make([]byte, 5+len(protocol)+len(sourcePeer))
	msg[0] = MsgTypeFilterRuleRemove
	msg[1] = byte(port >> 8)
	msg[2] = byte(port)
	msg[3] = byte(len(protocol))
	copy(msg[4:4+len(protocol)], protocol)
	msg[4+len(protocol)] = byte(len(sourcePeer))
	copy(msg[5+len(protocol):], sourcePeer)

	select {
	case pc.writeChan <- msg:
		log.Debug().
			Str("peer", peerName).
			Uint16("port", port).
			Str("protocol", protocol).
			Str("source_peer", sourcePeer).
			Msg("pushed filter rule remove to peer")
	default:
		log.Debug().
			Str("peer", peerName).
			Msg("failed to push filter rule remove: channel full")
	}
}

// BroadcastCoordListUpdate sends the updated coordinator IP list to all connected peers.
// Format: [MsgTypeCoordListUpdate][count:1][ip_len:1][ip]...
func (r *relayManager) BroadcastCoordListUpdate(ips []string) {
	r.mu.Lock()
	peers := make([]*persistentConn, 0, len(r.persistent))
	for _, pc := range r.persistent {
		peers = append(peers, pc)
	}
	r.mu.Unlock()

	if len(peers) == 0 {
		return
	}

	// Build message: [MsgTypeCoordListUpdate][count:1][ip_len:1][ip]...
	msgLen := 2 // type + count
	for _, ip := range ips {
		msgLen += 1 + len(ip) // ip_len + ip
	}

	for _, pc := range peers {
		msg := make([]byte, msgLen)
		msg[0] = MsgTypeCoordListUpdate
		msg[1] = byte(len(ips))
		offset := 2
		for _, ip := range ips {
			msg[offset] = byte(len(ip))
			copy(msg[offset+1:], ip)
			offset += 1 + len(ip)
		}

		select {
		case pc.writeChan <- msg:
			log.Debug().
				Str("target", pc.peerName).
				Int("coordinators", len(ips)).
				Msg("sent coordinator list update")
		default:
			log.Debug().
				Str("target", pc.peerName).
				Msg("failed to send coordinator list update: channel full")
		}
	}
}

// PushServicePorts sends coordinator service port announcements to a peer.
// This tells the peer which ports the coordinator exposes (admin, metrics, etc).
func (r *relayManager) PushServicePorts(peerName string, ports []uint16) {
	r.mu.Lock()
	pc, ok := r.persistent[peerName]
	r.mu.Unlock()

	if !ok {
		log.Debug().Str("peer", peerName).Msg("cannot push service ports: peer not connected")
		return
	}

	// Build message: [MsgTypeServicePortNotify][count:1][port:2]...
	msg := make([]byte, 2+len(ports)*2)
	msg[0] = MsgTypeServicePortNotify
	msg[1] = byte(len(ports))
	for i, port := range ports {
		msg[2+i*2] = byte(port >> 8)
		msg[2+i*2+1] = byte(port)
	}

	select {
	case pc.writeChan <- msg:
		log.Debug().
			Str("peer", peerName).
			Int("count", len(ports)).
			Msg("pushed service ports to peer")
	default:
		log.Debug().
			Str("peer", peerName).
			Msg("failed to push service ports: channel full")
	}
}

// setupRelayRoutes registers the relay WebSocket endpoints on the public mux.
// These endpoints are also registered on adminMux (in setupAdminRoutes) for mesh-only access.
// Public mux: Used for initial connections and peers without mesh access.
// Admin mux: Preferred by peers with mesh connectivity for better security.
func (s *Server) setupRelayRoutes(ctx context.Context) {
	if s.relay == nil {
		s.relay = newRelayManager(ctx)
	}
	s.mux.HandleFunc("/api/v1/relay/persistent", s.handlePersistentRelay)
	s.mux.HandleFunc("/api/v1/relay/", s.handleRelay)
	s.mux.HandleFunc("/api/v1/relay-status", s.handleRelayStatus)
}

// handlePersistentRelay handles persistent (DERP-like) relay connections.
// Peers connect once and stay connected. Packets are routed by peer name.
func (s *Server) handlePersistentRelay(w http.ResponseWriter, r *http.Request) {
	// Authenticate via JWT
	auth := r.Header.Get("Authorization")
	if auth == "" {
		http.Error(w, "missing authorization header", http.StatusUnauthorized)
		return
	}

	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		http.Error(w, "invalid authorization header", http.StatusUnauthorized)
		return
	}

	claims, err := s.ValidateToken(parts[1])
	if err != nil {
		log.Debug().Err(err).Msg("persistent relay auth failed")
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	peerName := claims.PeerName

	// Verify peer is registered
	s.peersMu.RLock()
	_, exists := s.peers[peerName]
	s.peersMu.RUnlock()

	if !exists {
		http.Error(w, "peer not registered", http.StatusNotFound)
		return
	}

	// Upgrade to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Str("peer", peerName).Msg("persistent relay websocket upgrade failed")
		return
	}

	log.Info().Str("peer", peerName).Msg("persistent relay connection established")

	// Register the connection (starts writer goroutine with ping ticker)
	pc := s.relay.RegisterPersistent(peerName, conn)
	if s.coordMetrics != nil {
		s.coordMetrics.OnlinePeers.Inc()
	}
	defer func() {
		s.relay.UnregisterPersistent(peerName)
		if s.coordMetrics != nil {
			s.coordMetrics.OnlinePeers.Dec()
			// Prevent cardinality leak: remove series for disconnected peer
			s.coordMetrics.PeerRTTSeconds.DeleteLabelValues(peerName)
			s.coordMetrics.PeerLatencySeconds.DeletePartialMatch(prometheus.Labels{"source": peerName})
			s.coordMetrics.PeerLatencySeconds.DeletePartialMatch(prometheus.Labels{"target": peerName})
		}
		pc.Close()
		log.Info().Str("peer", peerName).Msg("persistent relay connection closed")
	}()

	// Notify other peers that this peer has (re)connected
	// This allows them to invalidate stale direct tunnels
	go s.relay.BroadcastPeerReconnected(peerName)

	// Push coordinator filter rules to the peer
	if len(s.cfg.Filter.Rules) > 0 {
		go s.relay.PushFilterRules(peerName, s.cfg.Filter.Rules)
	}

	// Push coordinator service ports (so peers auto-allow access to admin, metrics, etc.)
	if servicePorts := s.getServicePorts(); len(servicePorts) > 0 {
		go s.relay.PushServicePorts(peerName, servicePorts)
	}

	// Set up ping/pong handlers for keepalive
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	// Read loop - handle messages directly (writes go through buffered channel)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Debug().Err(err).Str("peer", peerName).Msg("persistent relay read error")
			}
			return
		}
		s.handlePersistentRelayMessage(r.Context(), peerName, data)
	}
}

// handlePersistentRelayMessage processes a message from a persistent relay connection.
// nolint:gocyclo // Relay message handler processes multiple message types
func (s *Server) handlePersistentRelayMessage(ctx context.Context, sourcePeer string, data []byte) {
	if len(data) < 1 {
		log.Debug().Str("peer", sourcePeer).Msg("persistent relay message too short")
		return
	}

	msgType := data[0]

	switch msgType {
	case MsgTypeHeartbeat:
		// Format: [MsgTypeHeartbeat][stats_len:2][stats JSON]
		if len(data) < 3 {
			log.Debug().Str("peer", sourcePeer).Msg("heartbeat message too short")
			return
		}
		statsLen := int(data[1])<<8 | int(data[2])
		if len(data) < 3+statsLen {
			log.Debug().Str("peer", sourcePeer).Int("expected", statsLen).Int("got", len(data)-3).Msg("heartbeat stats truncated")
			return
		}
		statsJSON := data[3 : 3+statsLen]

		// Span wraps the heartbeat processing (parse + store + metrics + ack).
		// Using background context: heartbeats arrive on a hot loop but are low-frequency
		// per-peer (~every 30s), so overhead is negligible.
		_, hbSpan := tracing.Tracer("tunnelmesh/coord").Start(ctx, "coord.heartbeat")
		hbSpan.SetAttributes(attribute.String("peer.name", sourcePeer))

		// Parse stats
		var stats proto.PeerStats
		if err := json.Unmarshal(statsJSON, &stats); err != nil {
			hbSpan.RecordError(err)
			hbSpan.SetStatus(otelcodes.Error, err.Error())
			log.Debug().Err(err).Str("peer", sourcePeer).Msg("failed to parse heartbeat stats")
		}

		// Update peer info and record stats history
		s.peersMu.Lock()
		if peer, exists := s.peers[sourcePeer]; exists {
			now := time.Now()
			peer.peer.LastSeen = now
			peer.prevStats = peer.stats // Save current as previous BEFORE updating
			peer.prevStatsTime = peer.lastStatsTime
			peer.stats = &stats
			peer.lastStatsTime = now
			peer.heartbeatCount++
			// Update location from heartbeat (keeps map positions after coordinator restart)
			if s.cfg.Coordinator.Locations && stats.Location != nil && stats.Location.IsSet() {
				peer.peer.Location = stats.Location
			}
			// Store reported latency metrics (only update if peer reported a value)
			if stats.CoordinatorRTTMs > 0 {
				peer.coordinatorRTT = stats.CoordinatorRTTMs
				log.Debug().Str("peer", sourcePeer).Int64("rtt_ms", stats.CoordinatorRTTMs).Msg("updated peer coordinator RTT")
			}
			if stats.PeerLatencies != nil {
				peer.peerLatencies = stats.PeerLatencies
			}
		}
		atomic.AddUint64(&s.serverStats.totalHeartbeats, 1)
		s.peersMu.Unlock()

		// Update Prometheus metrics
		if s.coordMetrics != nil {
			s.coordMetrics.TotalHeartbeats.Inc()
			if stats.CoordinatorRTTMs > 0 {
				// Convert milliseconds to seconds for Prometheus
				s.coordMetrics.PeerRTTSeconds.WithLabelValues(sourcePeer).Set(float64(stats.CoordinatorRTTMs) / 1000.0)
			}
			if len(stats.PeerLatencies) > 0 {
				log.Debug().
					Str("source", sourcePeer).
					Int("latency_count", len(stats.PeerLatencies)).
					Msg("received peer latencies in heartbeat")
			}
			for targetPeer, latencyUs := range stats.PeerLatencies {
				// Convert microseconds to seconds for Prometheus
				s.coordMetrics.PeerLatencySeconds.WithLabelValues(sourcePeer, targetPeer).Set(float64(latencyUs) / 1e6)
			}
		}

		// Send ack via the persistent connection
		// Format: [MsgTypeHeartbeatAck][timestamp:8]
		if pc, ok := s.relay.GetPersistent(sourcePeer); ok {
			ackMsg := make([]byte, 9)
			ackMsg[0] = MsgTypeHeartbeatAck
			binary.BigEndian.PutUint64(ackMsg[1:], uint64(stats.HeartbeatSentAt))
			select {
			case pc.writeChan <- ackMsg:
				log.Debug().Str("peer", sourcePeer).Int("ack_len", len(ackMsg)).Msg("sent heartbeat ack")
			default:
				log.Debug().Str("peer", sourcePeer).Msg("failed to send heartbeat ack: channel full")
			}
		}

		// End heartbeat span and inject trace_id into log for Loki → Tempo correlation.
		hbSpan.SetStatus(otelcodes.Ok, "")
		sc := hbSpan.SpanContext()
		hbSpan.End()
		log.Debug().
			Str("peer", sourcePeer).
			Str("trace_id", sc.TraceID().String()).
			Str("span_id", sc.SpanID().String()).
			Msg("heartbeat processed")

		// Notify SSE clients of heartbeat
		s.notifyHeartbeat(sourcePeer)

	case MsgTypeSendPacket:
		// Format: [MsgTypeSendPacket][target name len][target name][packet data]
		if len(data) < 3 {
			log.Debug().Str("peer", sourcePeer).Msg("persistent relay send packet too short")
			return
		}
		targetLen := int(data[1])
		if len(data) < 2+targetLen {
			log.Debug().Str("peer", sourcePeer).Msg("persistent relay send packet too short")
			return
		}
		targetPeer := string(data[2 : 2+targetLen])
		packetData := data[2+targetLen:]

		// Route to target peer
		targetConn, ok := s.relay.GetPersistent(targetPeer)
		if !ok {
			log.Debug().
				Str("source", sourcePeer).
				Str("target", targetPeer).
				Msg("persistent relay target not connected")
			return
		}

		if err := targetConn.SendPacket(sourcePeer, packetData); err != nil {
			log.Debug().Err(err).
				Str("source", sourcePeer).
				Str("target", targetPeer).
				Msg("persistent relay forward failed")
		} else {
			log.Debug().
				Str("source", sourcePeer).
				Str("target", targetPeer).
				Int("len", len(packetData)).
				Msg("persistent relay forwarded packet")
		}

	case MsgTypePing:
		// Respond with pong (handled at protocol level via SetPongHandler)
		log.Debug().Str("peer", sourcePeer).Msg("persistent relay received ping")

	case MsgTypeFilterRulesReply:
		// Filter rules response from peer
		s.relay.handleFilterRulesReply(data)

	default:
		log.Debug().Str("peer", sourcePeer).Uint8("type", msgType).Msg("unknown persistent relay message type")
	}
}

// handleRelayStatus returns pending relay requests for the authenticated peer.
// This allows peers to poll for relay requests without waiting for the heartbeat interval.
func (s *Server) handleRelayStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Authenticate via JWT
	auth := r.Header.Get("Authorization")
	if auth == "" {
		s.jsonError(w, "missing authorization header", http.StatusUnauthorized)
		return
	}

	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		s.jsonError(w, "invalid authorization header", http.StatusUnauthorized)
		return
	}

	claims, err := s.ValidateToken(parts[1])
	if err != nil {
		log.Debug().Err(err).Msg("relay-status auth failed")
		s.jsonError(w, "invalid token", http.StatusUnauthorized)
		return
	}

	peerName := claims.PeerName

	// Get pending relay requests for this peer
	requests := s.relay.GetPendingRequestsFor(peerName)

	resp := proto.RelayStatusResponse{RelayRequests: requests}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleRelay handles WebSocket relay connections.
// URL format: /api/v1/relay/{target_peer}
// The connecting peer is identified via JWT token in Authorization header.
func (s *Server) handleRelay(w http.ResponseWriter, r *http.Request) {
	// Extract target peer from path
	targetPeer := strings.TrimPrefix(r.URL.Path, "/api/v1/relay/")
	if targetPeer == "" {
		http.Error(w, "target peer required", http.StatusBadRequest)
		return
	}

	// Authenticate via JWT
	auth := r.Header.Get("Authorization")
	if auth == "" {
		http.Error(w, "missing authorization header", http.StatusUnauthorized)
		return
	}

	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		http.Error(w, "invalid authorization header", http.StatusUnauthorized)
		return
	}

	claims, err := s.ValidateToken(parts[1])
	if err != nil {
		log.Debug().Err(err).Msg("relay auth failed")
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	fromPeer := claims.PeerName

	// Verify both peers are registered
	s.peersMu.RLock()
	_, fromExists := s.peers[fromPeer]
	_, toExists := s.peers[targetPeer]
	s.peersMu.RUnlock()

	if !fromExists || !toExists {
		http.Error(w, "peer not registered", http.StatusNotFound)
		return
	}

	// Upgrade to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Msg("websocket upgrade failed")
		return
	}

	log.Info().
		Str("from", fromPeer).
		Str("to", targetPeer).
		Msg("relay connection established")

	// Try to pair with existing connection
	s.relay.mu.Lock()

	// Check if there's a matching pending connection (targetPeer wanting to connect to fromPeer)
	reverseKey := targetPeer + "->" + fromPeer
	if pending, ok := s.relay.pending[reverseKey]; ok {
		// Found a match - pair them
		delete(s.relay.pending, reverseKey)
		s.relay.mu.Unlock()

		// Signal the waiting peer that pairing is complete
		close(pending.paired)

		// Bridge the connections (only this goroutine does it, not the waiting one)
		s.bridgeConnections(pending.conn, conn, pending.peerName, fromPeer)
		return
	}

	// No match found - wait for peer to connect
	forwardKey := fromPeer + "->" + targetPeer
	rc := &relayConn{
		peerName:   fromPeer,
		targetPeer: targetPeer,
		conn:       conn,
		paired:     make(chan struct{}),
	}
	s.relay.pending[forwardKey] = rc
	s.relay.mu.Unlock()

	// Push notification to target peer that someone is waiting for relay
	s.relay.NotifyRelayRequest(targetPeer, []string{fromPeer})

	// Wait for peer with hardcoded timeout (relay always enabled, timeout not configurable)
	const pairTimeout = 60 * time.Second

	select {
	case <-rc.paired:
		// Successfully paired - the other goroutine handles bridging
		// Just return, our connection is being handled
		return

	case <-time.After(pairTimeout):
		// Timeout - clean up
		s.relay.mu.Lock()
		delete(s.relay.pending, forwardKey)
		s.relay.mu.Unlock()

		log.Debug().
			Str("from", fromPeer).
			Str("to", targetPeer).
			Msg("relay pairing timeout")

		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "pairing timeout"),
			time.Now().Add(5*time.Second))
		_ = conn.Close()
	}
}

// bridgeConnections relays data between two WebSocket connections.
func (s *Server) bridgeConnections(conn1, conn2 *websocket.Conn, peer1, peer2 string) {
	log.Info().
		Str("peer1", peer1).
		Str("peer2", peer2).
		Msg("relay bridge started")

	var wg sync.WaitGroup

	// conn1 -> conn2
	wg.Go(func() {
		s.relayMessages(conn1, conn2)
	})

	// conn2 -> conn1
	wg.Go(func() {
		s.relayMessages(conn2, conn1)
	})

	wg.Wait()

	_ = conn1.Close()
	_ = conn2.Close()

	log.Info().
		Str("peer1", peer1).
		Str("peer2", peer2).
		Msg("relay bridge closed")
}

// relayMessages copies WebSocket messages from src to dst.
func (s *Server) relayMessages(src, dst *websocket.Conn) {
	for {
		messageType, data, err := src.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Debug().Err(err).Msg("relay read error")
			}
			// Signal the other direction to stop by closing the write side
			_ = dst.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				time.Now().Add(5*time.Second))
			return
		}

		if err := dst.WriteMessage(messageType, data); err != nil {
			if !errors.Is(err, io.EOF) {
				log.Debug().Err(err).Msg("relay write error")
			}
			return
		}
	}
}
