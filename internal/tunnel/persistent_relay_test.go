package tunnel

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/coord"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
)

var testUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// mockRelayServer simulates the coordination server's persistent relay endpoint.
type mockRelayServer struct {
	t           *testing.T
	server      *httptest.Server
	connections map[string]*websocket.Conn
	mu          sync.Mutex
	received    chan relayMessage
	heartbeats  chan heartbeatMessage
}

type heartbeatMessage struct {
	peerName string
	data     []byte
}

type relayMessage struct {
	source string
	target string
	data   []byte
}

func newMockRelayServer(t *testing.T) *mockRelayServer {
	m := &mockRelayServer{
		t:           t,
		connections: make(map[string]*websocket.Conn),
		received:    make(chan relayMessage, 10),
		heartbeats:  make(chan heartbeatMessage, 10),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/relay/persistent", m.handlePersistentRelay)
	m.server = httptest.NewServer(mux)

	return m
}

func (m *mockRelayServer) handlePersistentRelay(w http.ResponseWriter, r *http.Request) {
	// Extract peer name from auth header (simplified)
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	peerName := strings.TrimPrefix(auth, "Bearer ")

	conn, err := testUpgrader.Upgrade(w, r, nil)
	if err != nil {
		m.t.Logf("upgrade failed: %v", err)
		return
	}

	m.mu.Lock()
	m.connections[peerName] = conn
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.connections, peerName)
		m.mu.Unlock()
		_ = conn.Close()
	}()

	// Read and route messages
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		if len(data) < 1 {
			continue
		}

		msgType := data[0]
		switch msgType {
		case MsgTypeSendPacket:
			if len(data) < 3 {
				continue
			}
			targetLen := int(data[1])
			if len(data) < 2+targetLen {
				continue
			}
			targetPeer := string(data[2 : 2+targetLen])
			packetData := data[2+targetLen:]

			m.received <- relayMessage{
				source: peerName,
				target: targetPeer,
				data:   packetData,
			}

			// Route to target if connected
			m.mu.Lock()
			targetConn, ok := m.connections[targetPeer]
			m.mu.Unlock()

			if ok {
				// Build recv message
				msg := make([]byte, 2+len(peerName)+len(packetData))
				msg[0] = MsgTypeRecvPacket
				msg[1] = byte(len(peerName))
				copy(msg[2:], peerName)
				copy(msg[2+len(peerName):], packetData)
				_ = targetConn.WriteMessage(websocket.BinaryMessage, msg)
			}

		case MsgTypeHeartbeat:
			// Record the heartbeat
			m.heartbeats <- heartbeatMessage{
				peerName: peerName,
				data:     data[1:], // Skip message type byte
			}
			// Send extended ack with echoed timestamp
			ack := make([]byte, 9)
			ack[0] = MsgTypeHeartbeatAck
			// Parse stats to get HeartbeatSentAt
			if len(data) >= 4 {
				statsLen := int(data[1])<<8 | int(data[2])
				if len(data) >= 3+statsLen {
					var stats proto.PeerStats
					if err := json.Unmarshal(data[3:3+statsLen], &stats); err == nil {
						binary.BigEndian.PutUint64(ack[1:], uint64(stats.HeartbeatSentAt))
					}
				}
			}
			_ = conn.WriteMessage(websocket.BinaryMessage, ack)
		}
	}
}

// sendRelayNotify sends a relay notification to a connected peer.
func (m *mockRelayServer) sendRelayNotify(peerName string, waitingPeers []string) error {
	m.mu.Lock()
	conn, ok := m.connections[peerName]
	m.mu.Unlock()

	if !ok {
		return nil // Peer not connected
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

	return conn.WriteMessage(websocket.BinaryMessage, msg)
}

// sendHolePunchNotify sends a hole-punch notification to a connected peer.
func (m *mockRelayServer) sendHolePunchNotify(peerName string, requestingPeers []string) error {
	m.mu.Lock()
	conn, ok := m.connections[peerName]
	m.mu.Unlock()

	if !ok {
		return nil // Peer not connected
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

	return conn.WriteMessage(websocket.BinaryMessage, msg)
}

func (m *mockRelayServer) URL() string {
	return m.server.URL
}

func (m *mockRelayServer) Close() {
	m.server.Close()
}

func TestPersistentRelay_Connect(t *testing.T) {
	server := newMockRelayServer(t)
	defer server.Close()

	relay := NewPersistentRelay(server.URL(), "peer1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.NoError(t, err)
	assert.True(t, relay.IsConnected())

	_ = relay.Close()
	assert.False(t, relay.IsConnected())
}

func TestPersistentRelay_SendTo(t *testing.T) {
	server := newMockRelayServer(t)
	defer server.Close()

	relay := NewPersistentRelay(server.URL(), "peer1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay.Close() }()

	// Send a packet
	testData := []byte("hello world")
	err = relay.SendTo("peer2", testData)
	require.NoError(t, err)

	// Verify server received it
	select {
	case msg := <-server.received:
		assert.Equal(t, "peer1", msg.source)
		assert.Equal(t, "peer2", msg.target)
		assert.Equal(t, testData, msg.data)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestPersistentRelay_ReceivePacket(t *testing.T) {
	server := newMockRelayServer(t)
	defer server.Close()

	// Connect two peers
	relay1 := NewPersistentRelay(server.URL(), "peer1")
	relay2 := NewPersistentRelay(server.URL(), "peer2")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay1.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay1.Close() }()

	err = relay2.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay2.Close() }()

	// Set up packet handler on peer2
	received := make(chan []byte, 1)
	relay2.SetPacketHandler(func(sourcePeer string, data []byte) {
		assert.Equal(t, "peer1", sourcePeer)
		received <- data
	})

	// Give time for both connections to be established
	time.Sleep(100 * time.Millisecond)

	// Send from peer1 to peer2
	testData := []byte("hello from peer1")
	err = relay1.SendTo("peer2", testData)
	require.NoError(t, err)

	// Verify peer2 received it
	select {
	case data := <-received:
		assert.Equal(t, testData, data)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for packet")
	}
}

func TestPersistentRelay_SendNotConnected(t *testing.T) {
	relay := NewPersistentRelay("http://localhost:9999", "peer1")

	err := relay.SendTo("peer2", []byte("test"))
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotConnected))
}

func TestPeerTunnel_ReadWrite(t *testing.T) {
	server := newMockRelayServer(t)
	defer server.Close()

	// Connect two peers
	relay1 := NewPersistentRelay(server.URL(), "peer1")
	relay2 := NewPersistentRelay(server.URL(), "peer2")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay1.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay1.Close() }()

	err = relay2.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay2.Close() }()

	// Give time for connections
	time.Sleep(100 * time.Millisecond)

	// Create peer tunnels
	tunnel1to2 := relay1.NewPeerTunnel("peer2")
	tunnel2to1 := relay2.NewPeerTunnel("peer1")

	// Write from tunnel1 to tunnel2
	testData := []byte("hello via peer tunnel")
	n, err := tunnel1to2.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)

	// Read on tunnel2
	buf := make([]byte, 100)
	n, err = tunnel2to1.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, testData, buf[:n])
}

func TestPeerTunnel_Close(t *testing.T) {
	relay := NewPersistentRelay("http://localhost:9999", "peer1")
	tunnel := relay.NewPeerTunnel("peer2")

	assert.False(t, tunnel.IsClosed())

	err := tunnel.Close()
	require.NoError(t, err)

	assert.True(t, tunnel.IsClosed())

	// Write should fail after close
	_, err = tunnel.Write([]byte("test"))
	assert.Error(t, err)
}

// --- Tests for heartbeat and push notification message types ---

func TestPersistentRelay_SendHeartbeat(t *testing.T) {
	server := newMockRelayServer(t)
	defer server.Close()

	relay := NewPersistentRelay(server.URL(), "peer1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay.Close() }()

	// Send a heartbeat with stats
	stats := &proto.PeerStats{
		PacketsSent:     100,
		PacketsReceived: 50,
		BytesSent:       5000,
		BytesReceived:   2500,
		ActiveTunnels:   2,
	}
	err = relay.SendHeartbeat(stats)
	require.NoError(t, err)

	// Verify server received the heartbeat
	select {
	case msg := <-server.heartbeats:
		assert.Equal(t, "peer1", msg.peerName)
		// Data should be JSON-encoded stats
		assert.True(t, len(msg.data) > 0)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for heartbeat")
	}
}

func TestPersistentRelay_SendHeartbeat_NotConnected(t *testing.T) {
	relay := NewPersistentRelay("http://localhost:9999", "peer1")

	stats := &proto.PeerStats{PacketsSent: 100}
	err := relay.SendHeartbeat(stats)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotConnected))
}

func TestPersistentRelay_ReceiveRelayNotify(t *testing.T) {
	server := newMockRelayServer(t)
	defer server.Close()

	relay := NewPersistentRelay(server.URL(), "peer1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay.Close() }()

	// Set up callback
	received := make(chan []string, 1)
	relay.SetRelayNotifyHandler(func(peers []string) {
		received <- peers
	})

	// Give time for connection to be ready
	time.Sleep(100 * time.Millisecond)

	// Server pushes relay notification
	err = server.sendRelayNotify("peer1", []string{"peer2", "peer3"})
	require.NoError(t, err)

	// Verify callback received it
	select {
	case peers := <-received:
		assert.Equal(t, []string{"peer2", "peer3"}, peers)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for relay notify")
	}
}

func TestPersistentRelay_ReceiveHolePunchNotify(t *testing.T) {
	server := newMockRelayServer(t)
	defer server.Close()

	relay := NewPersistentRelay(server.URL(), "peer1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay.Close() }()

	// Set up callback
	received := make(chan []string, 1)
	relay.SetHolePunchNotifyHandler(func(peers []string) {
		received <- peers
	})

	// Give time for connection to be ready
	time.Sleep(100 * time.Millisecond)

	// Server pushes hole-punch notification
	err = server.sendHolePunchNotify("peer1", []string{"peer4"})
	require.NoError(t, err)

	// Verify callback received it
	select {
	case peers := <-received:
		assert.Equal(t, []string{"peer4"}, peers)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for hole-punch notify")
	}
}

func TestPersistentRelay_ReceiveRelayNotify_NoHandler(t *testing.T) {
	server := newMockRelayServer(t)
	defer server.Close()

	relay := NewPersistentRelay(server.URL(), "peer1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay.Close() }()

	// No handler set - should not panic
	time.Sleep(100 * time.Millisecond)

	err = server.sendRelayNotify("peer1", []string{"peer2"})
	require.NoError(t, err)

	// Give time for message to be processed
	time.Sleep(100 * time.Millisecond)
	// Test passes if no panic
}

// --- Tests for RTT measurement ---

// mockRelayServerWithRTT extends mockRelayServer to echo timestamps in heartbeat acks.
type mockRelayServerWithRTT struct {
	*mockRelayServer
}

func newMockRelayServerWithRTT(t *testing.T) *mockRelayServerWithRTT {
	m := &mockRelayServerWithRTT{
		mockRelayServer: &mockRelayServer{
			t:           t,
			connections: make(map[string]*websocket.Conn),
			received:    make(chan relayMessage, 10),
			heartbeats:  make(chan heartbeatMessage, 10),
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/relay/persistent", m.handlePersistentRelayWithRTT)
	m.server = httptest.NewServer(mux)

	return m
}

func (m *mockRelayServerWithRTT) handlePersistentRelayWithRTT(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	peerName := strings.TrimPrefix(auth, "Bearer ")

	conn, err := testUpgrader.Upgrade(w, r, nil)
	if err != nil {
		m.t.Logf("upgrade failed: %v", err)
		return
	}

	m.mu.Lock()
	m.connections[peerName] = conn
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.connections, peerName)
		m.mu.Unlock()
		_ = conn.Close()
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		if len(data) < 1 {
			continue
		}

		msgType := data[0]
		switch msgType {
		case MsgTypeHeartbeat:
			// Parse stats to extract HeartbeatSentAt
			if len(data) < 3 {
				continue
			}
			statsLen := int(data[1])<<8 | int(data[2])
			if len(data) < 3+statsLen {
				continue
			}
			statsJSON := data[3 : 3+statsLen]

			// Record heartbeat
			m.heartbeats <- heartbeatMessage{
				peerName: peerName,
				data:     statsJSON,
			}

			// Parse to get HeartbeatSentAt and echo it back
			var stats proto.PeerStats
			if err := json.Unmarshal(statsJSON, &stats); err == nil {
				// Small delay to ensure measurable RTT on Windows (15.6ms timer resolution)
				time.Sleep(20 * time.Millisecond)
				// Send extended ack with echoed timestamp: [MsgTypeHeartbeatAck][timestamp:8]
				ack := make([]byte, 9)
				ack[0] = MsgTypeHeartbeatAck
				binary.BigEndian.PutUint64(ack[1:], uint64(stats.HeartbeatSentAt))
				_ = conn.WriteMessage(websocket.BinaryMessage, ack)
			}

		case MsgTypeSendPacket:
			// Handle same as base mock
			if len(data) < 3 {
				continue
			}
			targetLen := int(data[1])
			if len(data) < 2+targetLen {
				continue
			}
			targetPeer := string(data[2 : 2+targetLen])
			packetData := data[2+targetLen:]

			m.received <- relayMessage{
				source: peerName,
				target: targetPeer,
				data:   packetData,
			}

			m.mu.Lock()
			targetConn, ok := m.connections[targetPeer]
			m.mu.Unlock()

			if ok {
				msg := make([]byte, 2+len(peerName)+len(packetData))
				msg[0] = MsgTypeRecvPacket
				msg[1] = byte(len(peerName))
				copy(msg[2:], peerName)
				copy(msg[2+len(peerName):], packetData)
				_ = targetConn.WriteMessage(websocket.BinaryMessage, msg)
			}
		}
	}
}

func TestPersistentRelay_GetLastRTT_Initial(t *testing.T) {
	relay := NewPersistentRelay("http://localhost:9999", "peer1")

	// Before any heartbeat, RTT should be 0
	rtt := relay.GetLastRTT()
	assert.Equal(t, time.Duration(0), rtt)
}

func TestPersistentRelay_RTTCalculation(t *testing.T) {
	server := newMockRelayServerWithRTT(t)
	defer server.Close()

	relay := NewPersistentRelay(server.URL(), "peer1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay.Close() }()

	// Before heartbeat, RTT should be 0
	assert.Equal(t, time.Duration(0), relay.GetLastRTT())

	// Send a heartbeat - the mock server will echo the timestamp
	stats := &proto.PeerStats{
		PacketsSent:   100,
		ActiveTunnels: 2,
	}
	err = relay.SendHeartbeat(stats)
	require.NoError(t, err)

	// Wait for heartbeat to be received by server
	select {
	case <-server.heartbeats:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for heartbeat")
	}

	// Wait a bit for the ack to be processed
	time.Sleep(200 * time.Millisecond)

	// RTT should now be non-zero and reasonable (< 1 second for local test)
	rtt := relay.GetLastRTT()
	assert.Greater(t, rtt, time.Duration(0), "RTT should be positive after heartbeat")
	assert.Less(t, rtt, time.Second, "RTT should be less than 1 second for local test")
}

func TestPersistentRelay_HeartbeatIncludesSentAt(t *testing.T) {
	server := newMockRelayServerWithRTT(t)
	defer server.Close()

	relay := NewPersistentRelay(server.URL(), "peer1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay.Close() }()

	// Send a heartbeat
	stats := &proto.PeerStats{
		PacketsSent:   100,
		ActiveTunnels: 2,
	}
	beforeSend := time.Now().UnixNano()
	err = relay.SendHeartbeat(stats)
	require.NoError(t, err)
	afterSend := time.Now().UnixNano()

	// Receive heartbeat and check HeartbeatSentAt is set
	select {
	case msg := <-server.heartbeats:
		var receivedStats proto.PeerStats
		err := json.Unmarshal(msg.data, &receivedStats)
		require.NoError(t, err)

		// HeartbeatSentAt should be set and within the send window
		assert.NotZero(t, receivedStats.HeartbeatSentAt, "HeartbeatSentAt should be set")
		assert.GreaterOrEqual(t, receivedStats.HeartbeatSentAt, beforeSend, "HeartbeatSentAt should be >= beforeSend")
		assert.LessOrEqual(t, receivedStats.HeartbeatSentAt, afterSend, "HeartbeatSentAt should be <= afterSend")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for heartbeat")
	}
}

// TestPersistentRelay_Connect_401_ReturnsErrUnauthorized verifies that a 401
// response from the server is wrapped as coord.ErrUnauthorized.
func TestPersistentRelay_Connect_401_ReturnsErrUnauthorized(t *testing.T) {
	// Serve a plain HTTP server that always returns 401 (no WS upgrade)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "token expired", http.StatusUnauthorized)
	}))
	defer srv.Close()

	relay := NewPersistentRelay(srv.URL, "expired-token")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, coord.ErrUnauthorized), "expected ErrUnauthorized, got: %v", err)
}

// TestPersistentRelay_UpdateJWTToken verifies that UpdateJWTToken replaces the
// stored token so the next Connect call sends the new value in the Authorization header.
// The test creates a single relay, calls UpdateJWTToken before Connect, then verifies
// the server sees the updated token — no ordering dependency, no background goroutines
// racing against the assertion.
func TestPersistentRelay_UpdateJWTToken(t *testing.T) {
	var receivedToken string
	var mu sync.Mutex
	connected := make(chan struct{}, 1)

	// Server records the token and signals when a connection arrives
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		receivedToken = token
		mu.Unlock()
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		select {
		case connected <- struct{}{}:
		default:
		}
		// Keep open until client closes
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Set placeholder at construction, then update before connecting
	relay := NewPersistentRelay(srv.URL, "placeholder-token")
	relay.UpdateJWTToken("new-token")

	err := relay.Connect(ctx)
	require.NoError(t, err)
	defer func() { _ = relay.Close() }()

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for connection")
	}

	mu.Lock()
	got := receivedToken
	mu.Unlock()

	assert.Equal(t, "new-token", got, "Connect should use the token set by UpdateJWTToken, not the constructor value")
}

// TestPersistentRelay_WriteLoop_ClosesAfterConsecutiveFailures verifies that
// the relay becomes disconnected when the server drops the TCP connection
// without a clean WebSocket close frame. This exercises the readLoop failure
// path (shouldReconnect=true) and, if writes were in flight, the writeLoop's
// consecutive-failure detection (maxConsecutiveWriteFailures).
//
// The server rejects all reconnection attempts (503) so that autoReconnect's
// Connect() fails before creating a new writeChan. This avoids the race
// between readLoop closing the old writeChan and a concurrent SendTo sending
// to the newly created replacement — a race that would occur if the server
// accepted the reconnect and the new readLoop immediately exited.
func TestPersistentRelay_WriteLoop_ClosesAfterConsecutiveFailures(t *testing.T) {
	var mu sync.Mutex
	firstHandled := false
	serverConns := make(chan *websocket.Conn, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		isFirst := !firstHandled
		firstHandled = true
		mu.Unlock()

		if !isFirst {
			// Reject reconnection attempts so Connect() returns an error without
			// creating a new writeChan, keeping p.writeChan nil and race-free.
			http.Error(w, "no reconnect", http.StatusServiceUnavailable)
			return
		}

		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		select {
		case serverConns <- conn:
		default:
		}
		// Drain reads so the client write buffer does not stall.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	relay := NewPersistentRelay(srv.URL, "peer1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := relay.Connect(ctx)
	require.NoError(t, err)
	assert.True(t, relay.IsConnected())

	// Wait for the server goroutine to see the connection.
	var sc *websocket.Conn
	select {
	case sc = <-serverConns:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server connection")
	}

	// Drop the TCP connection without sending a WebSocket close frame. This
	// forces the client's ReadMessage to return an error (not a clean close),
	// so readLoop sets shouldReconnect=true and triggers autoReconnect. It
	// also causes any pending WriteMessage calls in writeLoop to fail.
	_ = sc.UnderlyingConn().Close()

	// The relay should detect the broken connection and transition to disconnected.
	assert.Eventually(t, func() bool {
		return !relay.IsConnected()
	}, 2*time.Second, 25*time.Millisecond, "relay should disconnect after connection drop")

	// Close the relay to stop any ongoing reconnection attempts.
	_ = relay.Close()
}
