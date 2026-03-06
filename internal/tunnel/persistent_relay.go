package tunnel

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/coord"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
)

// httpToWSURL converts an HTTP(S) URL to a WebSocket URL.
func httpToWSURL(httpURL string) (string, error) {
	u, err := url.Parse(httpURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	return u.String(), nil
}

// ErrNotConnected is returned when an operation requires a relay connection but none exists.
var ErrNotConnected = errors.New("not connected to relay")

// FilterRuleWire is the wire format for a filter rule (matches coord/relay.go).
type FilterRuleWire struct {
	Port       uint16 `json:"port"`
	Protocol   string `json:"protocol"`    // "tcp" or "udp"
	Action     string `json:"action"`      // "allow" or "deny"
	SourcePeer string `json:"source_peer"` // Source peer (empty = any peer)
}

// FilterRuleWithSourceWire is a filter rule with its origin source for display.
type FilterRuleWithSourceWire struct {
	Port       uint16 `json:"port"`
	Protocol   string `json:"protocol"`    // "tcp" or "udp"
	Action     string `json:"action"`      // "allow" or "deny"
	SourcePeer string `json:"source_peer"` // Source peer (empty = any peer)
	Source     string `json:"source"`      // "coordinator", "config", "temporary", or "service"
	Expires    int64  `json:"expires"`     // Unix timestamp, 0=permanent
}

// relayPacketPool pools relay packet buffers to reduce GC pressure.
var relayPacketPool = sync.Pool{
	New: func() interface{} {
		// Allocate buffer for max relay message: type(1) + name_len(1) + name(255) + data(65535)
		buf := make([]byte, 65792)
		return &buf
	},
}

// Persistent relay message types (must match server)
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

// PersistentRelay maintains a persistent connection to the coordination server
// for DERP-like packet relay. It can route packets to any peer without
// the per-peer pairing required by the old relay mechanism.
type PersistentRelay struct {
	serverURL  string
	jwtToken   string
	conn       *websocket.Conn
	mu         sync.RWMutex
	connected  bool
	closed     bool
	closedChan chan struct{}

	// Async write channel to prevent blocking on slow writes
	writeChan     chan writeRequest
	writeLoopDone chan struct{}

	// Packet routing
	incomingMu sync.Mutex
	incoming   map[string]*peerQueue // peerName -> queue of incoming packets

	// Callbacks (protected by mu)
	onPacket           func(sourcePeer string, data []byte)
	onPeerReconnected  func(peerName string)              // Called when server notifies a peer reconnected
	onRelayNotify      func(waitingPeers []string)        // Called when server notifies of relay requests
	onHolePunchNotify  func(requestingPeers []string)     // Called when server notifies of hole-punch requests
	onReconnectError   func(err error)                    // Called when reconnection fails (for re-registration)
	onFilterRulesSync  func(rules []FilterRuleWire)       // Called when server syncs coordinator filter rules
	onFilterRuleAdd    func(rule FilterRuleWire)          // Called when server pushes a single rule add
	onFilterRuleRemove func(port uint16, protocol string) // Called when server removes a rule
	onServicePorts     func(ports []uint16)               // Called when server announces service ports
	getFilterRules     func() []FilterRuleWithSourceWire  // Returns all filter rules with their sources
	onCoordListUpdate  func(coordIPs []string)            // Called when server sends updated coordinator IP list

	// Reconnection control
	reconnecting bool // Prevents concurrent autoReconnect goroutines

	// RTT measurement
	latencyMu       sync.RWMutex
	lastRTT         time.Duration
	heartbeatSentAt int64 // atomic - Unix nano when last heartbeat sent
}

// writeRequest represents a pending write to the relay connection.
type writeRequest struct {
	data   []byte
	pooled bool // if true, return to pool after write
}

// peerQueue holds incoming packets from a specific peer.
type peerQueue struct {
	packets chan []byte
}

// NewPersistentRelay creates a new persistent relay connection.
func NewPersistentRelay(serverURL, jwtToken string) *PersistentRelay {
	return &PersistentRelay{
		serverURL:     serverURL,
		jwtToken:      jwtToken,
		closedChan:    make(chan struct{}),
		writeChan:     make(chan writeRequest, 256), // Buffered to prevent blocking
		writeLoopDone: make(chan struct{}),
		incoming:      make(map[string]*peerQueue),
	}
}

// Connect establishes the persistent relay connection to the server.
func (p *PersistentRelay) Connect(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("relay is closed")
	}
	if p.connected {
		p.mu.Unlock()
		return nil // Already connected
	}
	// Capture jwtToken under lock to avoid a data race with UpdateJWTToken.
	jwtToken := p.jwtToken
	p.mu.Unlock()

	wsURL, err := httpToWSURL(p.serverURL)
	if err != nil {
		return fmt.Errorf("convert URL: %w", err)
	}

	relayURL := wsURL + "/api/v1/relay/persistent"

	log.Debug().Str("url", relayURL).Msg("connecting to persistent relay")

	dialer := websocket.Dialer{
		HandshakeTimeout: 30 * time.Second,
	}

	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+jwtToken)

	conn, resp, err := dialer.DialContext(ctx, relayURL, headers)
	if err != nil {
		if resp != nil {
			defer func() { _ = resp.Body.Close() }()
			body := make([]byte, 256)
			n, _ := resp.Body.Read(body)
			// Return sentinel errors for specific status codes so callers can react appropriately
			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("%w: %s", coord.ErrPeerNotFound, string(body[:n]))
			}
			if resp.StatusCode == http.StatusUnauthorized {
				return fmt.Errorf("%w: %s", coord.ErrUnauthorized, string(body[:n]))
			}
			return fmt.Errorf("persistent relay connection failed: %s - %s", resp.Status, string(body[:n]))
		}
		return fmt.Errorf("persistent relay connection failed: %w", err)
	}

	p.mu.Lock()
	p.conn = conn
	p.connected = true
	// Reset write channel and done signal for new connection
	p.writeChan = make(chan writeRequest, 256)
	p.writeLoopDone = make(chan struct{})
	p.mu.Unlock()

	log.Info().Msg("persistent relay connected")

	// Start write loop (handles outgoing packets without blocking callers)
	go p.writeLoop()

	// Start message reader
	go p.readLoop()

	return nil
}

// maxConsecutiveWriteFailures is the number of consecutive write failures before
// the writeLoop closes the connection to force a reconnect via the readLoop.
const maxConsecutiveWriteFailures = 3

// writeLoop processes queued writes to prevent blocking on slow network writes.
func (p *PersistentRelay) writeLoop() {
	defer close(p.writeLoopDone)

	// Capture closedChan under lock to avoid race with Close()
	p.mu.RLock()
	closedChan := p.closedChan
	writeChan := p.writeChan
	p.mu.RUnlock()

	consecutiveFailures := 0

	for {
		select {
		case <-closedChan:
			return
		case req, ok := <-writeChan:
			if !ok {
				return
			}
			p.mu.RLock()
			conn := p.conn
			p.mu.RUnlock()

			if conn == nil {
				if req.pooled {
					relayPacketPool.Put(&req.data)
				}
				continue
			}

			if err := conn.WriteMessage(websocket.BinaryMessage, req.data); err != nil {
				consecutiveFailures++
				log.Debug().Err(err).Int("failures", consecutiveFailures).Msg("persistent relay write failed")
				if req.pooled {
					relayPacketPool.Put(&req.data)
				}
				if consecutiveFailures >= maxConsecutiveWriteFailures {
					log.Warn().Int("failures", consecutiveFailures).Msg("persistent relay closing connection after repeated write failures")
					_ = conn.Close()
					return
				}
				continue
			}

			consecutiveFailures = 0
			if req.pooled {
				relayPacketPool.Put(&req.data)
			}
		}
	}
}

// readLoop reads messages from the server and dispatches them.
func (p *PersistentRelay) readLoop() {
	shouldReconnect := false

	defer func() {
		p.mu.Lock()
		p.connected = false
		if p.conn != nil {
			_ = p.conn.Close()
			p.conn = nil
		}
		// Close write channel to stop write loop
		if p.writeChan != nil {
			close(p.writeChan)
			p.writeChan = nil
		}
		closed := p.closed
		p.mu.Unlock()

		log.Debug().Msg("persistent relay read loop exited")

		// Auto-reconnect if not explicitly closed
		if shouldReconnect && !closed {
			// Prevent concurrent reconnection attempts
			p.mu.Lock()
			if !p.reconnecting {
				p.reconnecting = true
				go p.autoReconnect()
			}
			p.mu.Unlock()
		}
	}()

	for {
		p.mu.RLock()
		conn := p.conn
		closed := p.closed
		p.mu.RUnlock()

		if closed || conn == nil {
			return
		}

		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			p.mu.RLock()
			wasClosed := p.closed
			p.mu.RUnlock()
			if !wasClosed {
				log.Warn().Err(err).Msg("persistent relay read error, will reconnect")
				shouldReconnect = true
			}
			return
		}

		p.handleMessage(data)
	}
}

// autoReconnect attempts to reconnect with exponential backoff.
func (p *PersistentRelay) autoReconnect() {
	defer func() {
		p.mu.Lock()
		p.reconnecting = false
		p.mu.Unlock()
	}()

	backoff := time.Second
	maxBackoff := 30 * time.Second

	for attempt := 1; ; attempt++ {
		p.mu.RLock()
		closed := p.closed
		p.mu.RUnlock()

		if closed {
			return
		}

		log.Info().Int("attempt", attempt).Dur("backoff", backoff).Msg("attempting relay reconnection")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := p.Connect(ctx)
		cancel()

		if err == nil {
			log.Info().Int("attempt", attempt).Msg("relay reconnected successfully")
			return
		}

		log.Warn().Err(err).Int("attempt", attempt).Msg("relay reconnection failed")

		// Notify caller of error (e.g., to trigger re-registration)
		p.mu.RLock()
		errorHandler := p.onReconnectError
		p.mu.RUnlock()
		if errorHandler != nil {
			errorHandler(err)
		}

		// Wait before next attempt
		p.mu.RLock()
		closed = p.closed
		p.mu.RUnlock()
		if closed {
			return
		}

		select {
		case <-p.closedChan:
			return
		case <-time.After(backoff):
		}

		// Exponential backoff
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// handleMessage processes an incoming message from the server.
// nolint:gocyclo // Message handler processes multiple relay message types
func (p *PersistentRelay) handleMessage(data []byte) {
	if len(data) < 1 {
		log.Debug().Msg("persistent relay: empty message")
		return
	}

	msgType := data[0]

	switch msgType {
	case MsgTypeRecvPacket:
		// Format: [MsgTypeRecvPacket][source name len][source name][packet data]
		if len(data) < 3 {
			log.Debug().Int("len", len(data)).Msg("persistent relay: recv packet too short")
			return
		}
		sourceLen := int(data[1])
		if len(data) < 2+sourceLen {
			log.Debug().Int("len", len(data)).Int("source_len", sourceLen).Msg("persistent relay: malformed packet")
			return
		}
		sourcePeer := string(data[2 : 2+sourceLen])
		packetData := data[2+sourceLen:]

		// Dispatch to callback or queue (read callback with lock)
		p.mu.RLock()
		handler := p.onPacket
		p.mu.RUnlock()

		if handler != nil {
			handler(sourcePeer, packetData)
		} else {
			p.queuePacket(sourcePeer, packetData)
		}

	case MsgTypePeerReconnected:
		// Format: [MsgTypePeerReconnected][peer name len][peer name]
		if len(data) < 2 {
			return
		}
		peerLen := int(data[1])
		if len(data) < 2+peerLen {
			log.Debug().Int("len", len(data)).Int("peer_len", peerLen).Msg("persistent relay: malformed peer reconnected")
			return
		}
		peerName := string(data[2 : 2+peerLen])

		log.Debug().Str("peer", peerName).Msg("received peer reconnected notification")

		// Dispatch to callback
		p.mu.RLock()
		handler := p.onPeerReconnected
		p.mu.RUnlock()

		if handler != nil {
			handler(peerName)
		}

	case MsgTypePong:
		// Server pong - connection is alive
		log.Debug().Msg("persistent relay received pong")

	case MsgTypeHeartbeatAck:
		// Server acknowledged our heartbeat - connection is alive
		// Format: [MsgTypeHeartbeatAck][timestamp:8] echoes our sent timestamp
		if len(data) >= 9 {
			sentAt := int64(binary.BigEndian.Uint64(data[1:9]))
			rtt := time.Duration(time.Now().UnixNano() - sentAt)
			p.latencyMu.Lock()
			p.lastRTT = rtt
			p.latencyMu.Unlock()
			log.Debug().Dur("rtt", rtt).Msg("measured coordinator RTT")
		}

	case MsgTypeRelayNotify:
		// Format: [MsgTypeRelayNotify][count:1][name_len:1][name]...
		if len(data) < 2 {
			log.Debug().Int("len", len(data)).Msg("persistent relay: relay notify too short")
			return
		}
		count := int(data[1])
		peers := make([]string, 0, count)
		offset := 2
		for i := 0; i < count; i++ {
			if offset >= len(data) {
				break
			}
			nameLen := int(data[offset])
			if offset+1+nameLen > len(data) {
				break
			}
			peers = append(peers, string(data[offset+1:offset+1+nameLen]))
			offset += 1 + nameLen
		}

		log.Debug().Strs("peers", peers).Msg("received relay notification")

		// Dispatch to callback
		p.mu.RLock()
		handler := p.onRelayNotify
		p.mu.RUnlock()

		if handler != nil {
			handler(peers)
		}

	case MsgTypeHolePunchNotify:
		// Format: [MsgTypeHolePunchNotify][count:1][name_len:1][name]...
		if len(data) < 2 {
			log.Debug().Int("len", len(data)).Msg("persistent relay: hole-punch notify too short")
			return
		}
		count := int(data[1])
		peers := make([]string, 0, count)
		offset := 2
		for i := 0; i < count; i++ {
			if offset >= len(data) {
				break
			}
			nameLen := int(data[offset])
			if offset+1+nameLen > len(data) {
				break
			}
			peers = append(peers, string(data[offset+1:offset+1+nameLen]))
			offset += 1 + nameLen
		}

		log.Debug().Strs("peers", peers).Msg("received hole-punch notification")

		// Dispatch to callback
		p.mu.RLock()
		handler := p.onHolePunchNotify
		p.mu.RUnlock()

		if handler != nil {
			handler(peers)
		}

	case MsgTypeFilterRulesSync:
		// Format: [MsgTypeFilterRulesSync][rules_len:2][rules JSON]
		if len(data) < 3 {
			log.Debug().Int("len", len(data)).Msg("persistent relay: filter rules sync too short")
			return
		}
		rulesLen := int(data[1])<<8 | int(data[2])
		if len(data) < 3+rulesLen {
			log.Debug().Int("len", len(data)).Int("expected", rulesLen).Msg("persistent relay: filter rules truncated")
			return
		}
		rulesJSON := data[3 : 3+rulesLen]

		var rules []FilterRuleWire
		if err := json.Unmarshal(rulesJSON, &rules); err != nil {
			log.Debug().Err(err).Msg("persistent relay: failed to unmarshal filter rules")
			return
		}

		log.Debug().Int("rules", len(rules)).Msg("received coordinator filter rules sync")

		// Dispatch to callback
		p.mu.RLock()
		handler := p.onFilterRulesSync
		p.mu.RUnlock()

		if handler != nil {
			handler(rules)
		}

	case MsgTypeFilterRuleAdd:
		// Format: [MsgTypeFilterRuleAdd][rule_len:2][rule JSON]
		if len(data) < 3 {
			log.Debug().Int("len", len(data)).Msg("persistent relay: filter rule add too short")
			return
		}
		ruleLen := int(data[1])<<8 | int(data[2])
		if len(data) < 3+ruleLen {
			log.Debug().Int("len", len(data)).Int("expected", ruleLen).Msg("persistent relay: filter rule add truncated")
			return
		}
		ruleJSON := data[3 : 3+ruleLen]

		var rule FilterRuleWire
		if err := json.Unmarshal(ruleJSON, &rule); err != nil {
			log.Debug().Err(err).Msg("persistent relay: failed to unmarshal filter rule")
			return
		}

		log.Debug().
			Uint16("port", rule.Port).
			Str("protocol", rule.Protocol).
			Str("action", rule.Action).
			Msg("received filter rule add")

		// Dispatch to callback
		p.mu.RLock()
		handler := p.onFilterRuleAdd
		p.mu.RUnlock()

		if handler != nil {
			handler(rule)
		}

	case MsgTypeFilterRuleRemove:
		// Format: [MsgTypeFilterRuleRemove][port:2][protocol_len:1][protocol]
		if len(data) < 4 {
			log.Debug().Int("len", len(data)).Msg("persistent relay: filter rule remove too short")
			return
		}
		port := uint16(data[1])<<8 | uint16(data[2])
		protoLen := int(data[3])
		if len(data) < 4+protoLen {
			log.Debug().Int("len", len(data)).Int("proto_len", protoLen).Msg("persistent relay: filter rule remove truncated")
			return
		}
		protocol := string(data[4 : 4+protoLen])

		log.Debug().Uint16("port", port).Str("protocol", protocol).Msg("received filter rule remove")

		// Dispatch to callback
		p.mu.RLock()
		handler := p.onFilterRuleRemove
		p.mu.RUnlock()

		if handler != nil {
			handler(port, protocol)
		}

	case MsgTypeServicePortNotify:
		// Format: [MsgTypeServicePortNotify][count:1][port:2]...
		if len(data) < 2 {
			log.Debug().Int("len", len(data)).Msg("persistent relay: service port notify too short")
			return
		}
		count := int(data[1])
		if len(data) < 2+count*2 {
			log.Debug().Int("len", len(data)).Int("count", count).Msg("persistent relay: service port notify truncated")
			return
		}
		ports := make([]uint16, count)
		for i := 0; i < count; i++ {
			ports[i] = uint16(data[2+i*2])<<8 | uint16(data[2+i*2+1])
		}

		log.Debug().Int("count", len(ports)).Msg("received coordinator service ports")

		// Dispatch to callback
		p.mu.RLock()
		handler := p.onServicePorts
		p.mu.RUnlock()

		if handler != nil {
			handler(ports)
		}

	case MsgTypeCoordListUpdate:
		// Format: [MsgTypeCoordListUpdate][count:1][ip_len:1][ip]...
		if len(data) < 2 {
			log.Debug().Int("len", len(data)).Msg("persistent relay: coord list update too short")
			return
		}
		count := int(data[1])
		coordIPs := make([]string, 0, count)
		offset := 2
		for i := 0; i < count; i++ {
			if offset >= len(data) {
				break
			}
			ipLen := int(data[offset])
			if offset+1+ipLen > len(data) {
				break
			}
			coordIPs = append(coordIPs, string(data[offset+1:offset+1+ipLen]))
			offset += 1 + ipLen
		}

		log.Debug().Strs("coord_ips", coordIPs).Msg("received coordinator list update")

		p.mu.RLock()
		handler := p.onCoordListUpdate
		p.mu.RUnlock()

		if handler != nil {
			handler(coordIPs)
		}

	case MsgTypeFilterRulesQuery:
		// Format: [MsgTypeFilterRulesQuery][reqID:4]
		if len(data) < 5 {
			log.Debug().Int("len", len(data)).Msg("persistent relay: filter rules query too short")
			return
		}
		reqID := uint32(data[1])<<24 | uint32(data[2])<<16 | uint32(data[3])<<8 | uint32(data[4])

		log.Debug().Uint32("req_id", reqID).Msg("received filter rules query")

		// Get all filter rules via callback
		p.mu.RLock()
		handler := p.getFilterRules
		p.mu.RUnlock()

		var rules []FilterRuleWithSourceWire
		if handler != nil {
			rules = handler()
		}

		// Marshal and send response
		rulesJSON, err := json.Marshal(rules)
		if err != nil {
			log.Error().Err(err).Msg("failed to marshal filter rules for query response")
			return
		}

		// Build response: [MsgTypeFilterRulesReply][reqID:4][rules JSON]
		reply := make([]byte, 5+len(rulesJSON))
		reply[0] = MsgTypeFilterRulesReply
		reply[1] = byte(reqID >> 24)
		reply[2] = byte(reqID >> 16)
		reply[3] = byte(reqID >> 8)
		reply[4] = byte(reqID)
		copy(reply[5:], rulesJSON)

		// Send reply
		p.mu.RLock()
		writeChan := p.writeChan
		p.mu.RUnlock()

		if writeChan != nil {
			select {
			case writeChan <- writeRequest{data: reply}:
				log.Debug().Uint32("req_id", reqID).Int("rules", len(rules)).Msg("sent filter rules reply")
			default:
				log.Debug().Uint32("req_id", reqID).Msg("failed to send filter rules reply: channel full")
			}
		}
	}
}

// queuePacket adds a packet to the incoming queue for a peer.
func (p *PersistentRelay) queuePacket(sourcePeer string, data []byte) {
	p.incomingMu.Lock()
	defer p.incomingMu.Unlock()

	q, ok := p.incoming[sourcePeer]
	if !ok {
		q = &peerQueue{packets: make(chan []byte, 64)}
		p.incoming[sourcePeer] = q
	}

	select {
	case q.packets <- data:
	default:
		// Queue full, drop oldest packet to make room
		log.Warn().Str("peer", sourcePeer).Int("data_len", len(data)).Msg("relay queue full, dropping oldest packet")
		select {
		case <-q.packets:
		default:
		}
		q.packets <- data
	}
}

// SendTo sends a packet to the target peer via the relay.
func (p *PersistentRelay) SendTo(targetPeer string, data []byte) error {
	p.mu.RLock()
	connected := p.connected
	writeChan := p.writeChan
	p.mu.RUnlock()

	if !connected || writeChan == nil {
		return ErrNotConnected
	}

	// Get buffer from pool
	bufPtr := relayPacketPool.Get().(*[]byte)
	buf := *bufPtr

	// Build message: [MsgTypeSendPacket][target name len][target name][data]
	msgLen := 2 + len(targetPeer) + len(data)
	if msgLen > len(buf) {
		// Message too large for pooled buffer, allocate new one
		relayPacketPool.Put(bufPtr)
		buf = make([]byte, msgLen)
		// Send non-pooled (won't be returned to pool)
		msg := buf[:msgLen]
		msg[0] = MsgTypeSendPacket
		msg[1] = byte(len(targetPeer))
		copy(msg[2:], targetPeer)
		copy(msg[2+len(targetPeer):], data)

		select {
		case writeChan <- writeRequest{data: msg, pooled: false}:
			return nil
		default:
			return fmt.Errorf("relay write channel full")
		}
	}

	msg := buf[:msgLen]
	msg[0] = MsgTypeSendPacket
	msg[1] = byte(len(targetPeer))
	copy(msg[2:], targetPeer)
	copy(msg[2+len(targetPeer):], data)

	// Non-blocking send to write channel
	select {
	case writeChan <- writeRequest{data: msg, pooled: true}:
		return nil
	default:
		relayPacketPool.Put(bufPtr)
		return fmt.Errorf("relay write channel full")
	}
}

// SetPacketHandler sets a callback for incoming packets.
// If set, packets are dispatched to this callback instead of being queued.
func (p *PersistentRelay) SetPacketHandler(handler func(sourcePeer string, data []byte)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onPacket = handler
}

// SetPeerReconnectedHandler sets a callback for peer reconnection notifications.
// This is called when the server notifies us that another peer has reconnected.
// The handler decides what action to take (if any) based on current connection state.
func (p *PersistentRelay) SetPeerReconnectedHandler(handler func(peerName string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onPeerReconnected = handler
}

// SetRelayNotifyHandler sets a callback for relay request notifications.
// This is called when the server notifies us that other peers are waiting
// to relay through us.
func (p *PersistentRelay) SetRelayNotifyHandler(handler func(waitingPeers []string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onRelayNotify = handler
}

// SetHolePunchNotifyHandler sets a callback for hole-punch request notifications.
// This is called when the server notifies us that other peers want to
// hole-punch with us.
func (p *PersistentRelay) SetHolePunchNotifyHandler(handler func(requestingPeers []string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onHolePunchNotify = handler
}

// SetReconnectErrorHandler sets a callback for reconnection errors.
// This is called when autoReconnect fails to connect, allowing the caller
// to handle specific errors like "peer not registered" by re-registering.
func (p *PersistentRelay) SetReconnectErrorHandler(handler func(err error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onReconnectError = handler
}

// UpdateJWTToken replaces the JWT token used for relay authentication.
// Call this after re-registering with the coordinator so the next reconnect
// attempt uses the fresh token rather than the expired one.
func (p *PersistentRelay) UpdateJWTToken(token string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.jwtToken = token
}

// SetFilterRulesSyncHandler sets a callback for coordinator filter rules sync.
// This is called when the coordinator sends its global filter rules on connect.
func (p *PersistentRelay) SetFilterRulesSyncHandler(handler func(rules []FilterRuleWire)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onFilterRulesSync = handler
}

// SetFilterRuleAddHandler sets a callback for filter rule additions.
// This is called when the admin panel adds a temporary rule.
func (p *PersistentRelay) SetFilterRuleAddHandler(handler func(rule FilterRuleWire)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onFilterRuleAdd = handler
}

// SetFilterRuleRemoveHandler sets a callback for filter rule removals.
// This is called when the admin panel removes a rule.
func (p *PersistentRelay) SetFilterRuleRemoveHandler(handler func(port uint16, protocol string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onFilterRuleRemove = handler
}

// SetServicePortsHandler sets a callback for coordinator service port announcements.
// This is called when the coordinator announces which ports it exposes.
func (p *PersistentRelay) SetServicePortsHandler(handler func(ports []uint16)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onServicePorts = handler
}

// SetGetFilterRulesHandler sets a callback to retrieve all filter rules with their sources.
// This is called when the coordinator queries for the peer's current filter rules.
func (p *PersistentRelay) SetGetFilterRulesHandler(handler func() []FilterRuleWithSourceWire) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.getFilterRules = handler
}

// SetCoordListUpdateHandler sets a callback for coordinator list updates.
// This is called when the server notifies of coordinator join/leave events.
func (p *PersistentRelay) SetCoordListUpdateHandler(handler func(coordIPs []string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onCoordListUpdate = handler
}

// SendHeartbeat sends a heartbeat with stats to the coordination server.
func (p *PersistentRelay) SendHeartbeat(stats *proto.PeerStats) error {
	p.mu.RLock()
	connected := p.connected
	writeChan := p.writeChan
	p.mu.RUnlock()

	if !connected || writeChan == nil {
		return ErrNotConnected
	}

	// Set HeartbeatSentAt for RTT measurement
	sentAt := time.Now().UnixNano()
	atomic.StoreInt64(&p.heartbeatSentAt, sentAt)
	stats.HeartbeatSentAt = sentAt

	// Encode stats as JSON
	statsJSON, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("marshal stats: %w", err)
	}

	// Build message: [MsgTypeHeartbeat][stats_len:2][stats JSON]
	msgLen := 1 + 2 + len(statsJSON)
	msg := make([]byte, msgLen)
	msg[0] = MsgTypeHeartbeat
	msg[1] = byte(len(statsJSON) >> 8)
	msg[2] = byte(len(statsJSON))
	copy(msg[3:], statsJSON)

	// Non-blocking send to write channel
	select {
	case writeChan <- writeRequest{data: msg, pooled: false}:
		return nil
	default:
		return fmt.Errorf("relay write channel full")
	}
}

// GetLastRTT returns the last measured round-trip time to the coordinator.
// Returns 0 if no RTT has been measured yet (e.g., before first heartbeat ack
// or when connected to an old coordinator that doesn't echo timestamps).
func (p *PersistentRelay) GetLastRTT() time.Duration {
	p.latencyMu.RLock()
	defer p.latencyMu.RUnlock()
	return p.lastRTT
}

// IsConnected returns true if the relay is connected.
func (p *PersistentRelay) IsConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connected
}

// Close closes the persistent relay connection.
func (p *PersistentRelay) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.connected = false
	close(p.closedChan)
	conn := p.conn
	p.conn = nil
	writeChan := p.writeChan
	p.writeChan = nil
	writeLoopDone := p.writeLoopDone
	p.mu.Unlock()

	// Close write channel and wait for write loop to finish
	if writeChan != nil {
		close(writeChan)
	}
	if writeLoopDone != nil {
		<-writeLoopDone
	}

	// Close all peer queues to unblock readers
	p.incomingMu.Lock()
	for peerName, q := range p.incoming {
		close(q.packets)
		delete(p.incoming, peerName)
	}
	p.incomingMu.Unlock()

	if conn != nil {
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(5*time.Second),
		)
		return conn.Close()
	}

	return nil
}

// PeerTunnel wraps a PersistentRelay to implement io.ReadWriteCloser for a specific peer.
// This allows using the persistent relay as a tunnel to a specific peer.
type PeerTunnel struct {
	relay    *PersistentRelay
	peerName string
	readBuf  []byte
	readPos  int
	mu       sync.Mutex
	closed   bool
}

// NewPeerTunnel creates a tunnel to a specific peer via the persistent relay.
func (p *PersistentRelay) NewPeerTunnel(peerName string) *PeerTunnel {
	return &PeerTunnel{
		relay:    p,
		peerName: peerName,
	}
}

// Read reads data from the peer via the relay.
func (pt *PeerTunnel) Read(buf []byte) (int, error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.closed {
		return 0, io.EOF
	}

	// If we have buffered data, return it
	if pt.readPos < len(pt.readBuf) {
		n := copy(buf, pt.readBuf[pt.readPos:])
		pt.readPos += n
		if pt.readPos >= len(pt.readBuf) {
			pt.readBuf = nil
			pt.readPos = 0
		}
		return n, nil
	}

	// Get queue for this peer
	pt.relay.incomingMu.Lock()
	q, ok := pt.relay.incoming[pt.peerName]
	if !ok {
		q = &peerQueue{packets: make(chan []byte, 64)}
		pt.relay.incoming[pt.peerName] = q
	}
	pt.relay.incomingMu.Unlock()

	// Wait for packet (with mutex unlocked)
	pt.mu.Unlock()
	select {
	case data, ok := <-q.packets:
		pt.mu.Lock()
		if !ok {
			return 0, io.EOF
		}
		n := copy(buf, data)
		if n < len(data) {
			pt.readBuf = data
			pt.readPos = n
		}
		return n, nil
	case <-pt.relay.closedChan:
		pt.mu.Lock()
		return 0, io.EOF
	}
}

// Write writes data to the peer via the relay.
func (pt *PeerTunnel) Write(buf []byte) (int, error) {
	pt.mu.Lock()
	if pt.closed {
		pt.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	pt.mu.Unlock()

	if err := pt.relay.SendTo(pt.peerName, buf); err != nil {
		return 0, err
	}
	return len(buf), nil
}

// Close closes the peer tunnel (but not the underlying relay).
func (pt *PeerTunnel) Close() error {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.closed = true
	return nil
}

// PeerName returns the name of the peer this tunnel connects to.
func (pt *PeerTunnel) PeerName() string {
	return pt.peerName
}

// IsClosed returns true if the tunnel has been explicitly closed.
func (pt *PeerTunnel) IsClosed() bool {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return pt.closed
}
