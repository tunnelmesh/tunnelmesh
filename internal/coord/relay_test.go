package coord

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
	"golang.org/x/time/rate"
)

// Helper to create a WebSocket connection to the relay endpoint
func connectRelay(t *testing.T, serverURL, peerName, jwtToken string) *websocket.Conn {
	// Convert http:// to ws://
	wsURL := strings.Replace(serverURL, "http://", "ws://", 1) + "/api/v1/relay/persistent"

	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
	}

	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+jwtToken)

	conn, _, err := dialer.Dial(wsURL, headers)
	require.NoError(t, err, "failed to connect to relay")

	// Drain exactly the startup messages sent immediately on connect.
	// getServicePorts() unconditionally returns [443, 9000, ...], so one
	// MsgTypeServicePortNotify is always sent. Filter rules are only sent when
	// cfg.Filter.Rules is non-empty — newTestConfig leaves it empty, so exactly
	// one message arrives. We read that one message to prevent tests from
	// consuming it accidentally.
	//
	// NOTE: gorilla/websocket permanently poisons a connection's internal readErr
	// on any deadline timeout, making all subsequent reads fail. A loop-drain
	// would always timeout on the final iteration and poison the connection.
	// Reading the exact expected count avoids this.
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, _, _ = conn.ReadMessage()
	_ = conn.SetReadDeadline(time.Time{})

	return conn
}

func TestRelayManager_HandleHeartbeat(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Start test server
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Register a peer first to get a JWT token
	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	// Connect to persistent relay
	conn := connectRelay(t, ts.URL, peerName, jwtToken)
	defer func() { _ = conn.Close() }()

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Send heartbeat with stats
	stats := &proto.PeerStats{
		PacketsSent:     100,
		PacketsReceived: 50,
		BytesSent:       5000,
		BytesReceived:   2500,
		ActiveTunnels:   2,
	}
	statsJSON, _ := json.Marshal(stats)

	// Build message: [MsgTypeHeartbeat][stats_len:2][stats JSON]
	msg := make([]byte, 1+2+len(statsJSON))
	msg[0] = MsgTypeHeartbeat
	msg[1] = byte(len(statsJSON) >> 8)
	msg[2] = byte(len(statsJSON))
	copy(msg[3:], statsJSON)

	err = conn.WriteMessage(websocket.BinaryMessage, msg)
	require.NoError(t, err)

	// Read heartbeat ack
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, ackData, err := conn.ReadMessage()
	require.NoError(t, err)

	assert.Equal(t, MsgTypeHeartbeatAck, ackData[0], "should receive heartbeat ack")

	// Verify peer stats were updated
	srv.peersMu.RLock()
	peer, exists := srv.peers[peerName]
	srv.peersMu.RUnlock()

	require.True(t, exists, "peer should exist")
	assert.WithinDuration(t, time.Now(), peer.peer.LastSeen, 2*time.Second, "LastSeen should be updated")
	if peer.stats != nil {
		assert.Equal(t, uint64(100), peer.stats.PacketsSent, "stats should be updated")
	}
}

func TestRelayManager_NotifyRelayRequest(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Register peer
	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	// Connect to persistent relay
	conn := connectRelay(t, ts.URL, peerName, jwtToken)
	defer func() { _ = conn.Close() }()

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Call NotifyRelayRequest on the server
	waitingPeers := []string{"peer2", "peer3"}
	srv.relay.NotifyRelayRequest(peerName, waitingPeers)

	// Read notification
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, data, err := conn.ReadMessage()
	require.NoError(t, err)

	// Parse notification
	assert.Equal(t, MsgTypeRelayNotify, data[0], "should receive relay notify")
	count := int(data[1])
	assert.Equal(t, 2, count, "should have 2 peers")

	// Parse peer names
	peers := parsePeerList(data[2:], count)
	assert.Equal(t, []string{"peer2", "peer3"}, peers)
}

func TestRelayManager_NotifyHolePunch(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Register peer
	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	// Connect to persistent relay
	conn := connectRelay(t, ts.URL, peerName, jwtToken)
	defer func() { _ = conn.Close() }()

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Call NotifyHolePunch
	requestingPeers := []string{"peer4"}
	srv.relay.NotifyHolePunch(peerName, requestingPeers)

	// Read notification
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, data, err := conn.ReadMessage()
	require.NoError(t, err)

	assert.Equal(t, MsgTypeHolePunchNotify, data[0], "should receive hole-punch notify")
	count := int(data[1])
	assert.Equal(t, 1, count, "should have 1 peer")

	peers := parsePeerList(data[2:], count)
	assert.Equal(t, []string{"peer4"}, peers)
}

func TestRelayManager_NotifyPeerNotConnected(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Notify a peer that's not connected - should not panic
	srv.relay.NotifyRelayRequest("nonexistent-peer", []string{"peer2"})
	srv.relay.NotifyHolePunch("nonexistent-peer", []string{"peer2"})
	// Test passes if no panic
}

func TestRelayManager_HeartbeatUpdatesStats(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	conn := connectRelay(t, ts.URL, peerName, jwtToken)
	defer func() { _ = conn.Close() }()

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Send multiple heartbeats with different stats
	for i := 1; i <= 3; i++ {
		stats := &proto.PeerStats{
			PacketsSent: uint64(i * 100),
			BytesSent:   uint64(i * 1000),
		}
		statsJSON, _ := json.Marshal(stats)

		msg := make([]byte, 1+2+len(statsJSON))
		msg[0] = MsgTypeHeartbeat
		msg[1] = byte(len(statsJSON) >> 8)
		msg[2] = byte(len(statsJSON))
		copy(msg[3:], statsJSON)

		err = conn.WriteMessage(websocket.BinaryMessage, msg)
		require.NoError(t, err)

		// Read ack
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
		_, _, err = conn.ReadMessage()
		require.NoError(t, err)
	}

	// Verify final stats
	srv.peersMu.RLock()
	peer := srv.peers[peerName]
	srv.peersMu.RUnlock()

	if peer.stats != nil {
		assert.Equal(t, uint64(300), peer.stats.PacketsSent, "stats should reflect latest heartbeat")
	}
}

// --- Helper functions ---

func registerPeerAndGetToken(t *testing.T, serverURL, peerName, authToken string) string {
	regReq := proto.RegisterRequest{
		Name:       peerName,
		PublicKey:  "SHA256:abc123",
		PublicIPs:  []string{"1.2.3.4"},
		PrivateIPs: []string{"192.168.1.100"},
		SSHPort:    2222,
	}
	body, _ := json.Marshal(regReq)

	req, _ := http.NewRequest(http.MethodPost, serverURL+"/api/v1/register", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var regResp proto.RegisterResponse
	err = json.NewDecoder(resp.Body).Decode(&regResp)
	require.NoError(t, err)

	return regResp.Token
}

func parsePeerList(data []byte, count int) []string {
	peers := make([]string, 0, count)
	offset := 0
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
	return peers
}

// Ensure sync.WaitGroup is used (for compiler)
var _ = sync.WaitGroup{}

// --- RTT and latency tests ---

func TestRelayManager_HeartbeatAckEchoesTimestamp(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	conn := connectRelay(t, ts.URL, peerName, jwtToken)
	defer func() { _ = conn.Close() }()

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Send heartbeat with HeartbeatSentAt timestamp
	sentAt := time.Now().UnixNano()
	stats := &proto.PeerStats{
		PacketsSent:     100,
		ActiveTunnels:   2,
		HeartbeatSentAt: sentAt,
	}
	statsJSON, _ := json.Marshal(stats)

	msg := make([]byte, 1+2+len(statsJSON))
	msg[0] = MsgTypeHeartbeat
	msg[1] = byte(len(statsJSON) >> 8)
	msg[2] = byte(len(statsJSON))
	copy(msg[3:], statsJSON)

	err = conn.WriteMessage(websocket.BinaryMessage, msg)
	require.NoError(t, err)

	// Read heartbeat ack
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, ackData, err := conn.ReadMessage()
	require.NoError(t, err)

	// Ack should be 9 bytes: [MsgTypeHeartbeatAck][timestamp:8]
	assert.Equal(t, MsgTypeHeartbeatAck, ackData[0], "should receive heartbeat ack")
	require.Len(t, ackData, 9, "ack should include echoed timestamp")

	// Parse echoed timestamp
	echoedTimestamp := int64(binary.BigEndian.Uint64(ackData[1:9]))
	assert.Equal(t, sentAt, echoedTimestamp, "echoed timestamp should match sent timestamp")
}

func TestRelayManager_HeartbeatAckWithoutTimestamp(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	conn := connectRelay(t, ts.URL, peerName, jwtToken)
	defer func() { _ = conn.Close() }()

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Send heartbeat with HeartbeatSentAt for RTT measurement
	sentAt := time.Now().UnixMicro()
	stats := &proto.PeerStats{
		PacketsSent:     100,
		ActiveTunnels:   2,
		HeartbeatSentAt: sentAt,
	}
	statsJSON, _ := json.Marshal(stats)

	msg := make([]byte, 1+2+len(statsJSON))
	msg[0] = MsgTypeHeartbeat
	msg[1] = byte(len(statsJSON) >> 8)
	msg[2] = byte(len(statsJSON))
	copy(msg[3:], statsJSON)

	err = conn.WriteMessage(websocket.BinaryMessage, msg)
	require.NoError(t, err)

	// Read heartbeat ack - always extended format [type][timestamp:8]
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, ackData, err := conn.ReadMessage()
	require.NoError(t, err)

	assert.Equal(t, MsgTypeHeartbeatAck, ackData[0], "should receive heartbeat ack")
	assert.Len(t, ackData, 9, "ack should be 9 bytes with echoed timestamp")
	echoedTS := int64(binary.BigEndian.Uint64(ackData[1:]))
	assert.Equal(t, sentAt, echoedTS, "echoed timestamp should match sent timestamp")
}

func TestRelayManager_QueryFilterRules(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	conn := connectRelay(t, ts.URL, peerName, jwtToken)
	defer func() { _ = conn.Close() }()

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Query filter rules in a goroutine (it blocks waiting for response)
	responseChan := make(chan []byte)
	errChan := make(chan error)
	go func() {
		rules, err := srv.relay.QueryFilterRules(context.Background(), peerName, 5*time.Second)
		if err != nil {
			errChan <- err
			return
		}
		responseChan <- rules
	}()

	// Read the query request
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, data, err := conn.ReadMessage()
	require.NoError(t, err)

	// Verify query format: [MsgTypeFilterRulesQuery][reqID:4]
	assert.Equal(t, MsgTypeFilterRulesQuery, data[0], "should receive filter rules query")
	require.Len(t, data, 5, "query should be 5 bytes")
	reqID := uint32(data[1])<<24 | uint32(data[2])<<16 | uint32(data[3])<<8 | uint32(data[4])

	// Send mock response with filter rules
	mockRules := []struct {
		Port       uint16 `json:"port"`
		Protocol   string `json:"protocol"`
		Action     string `json:"action"`
		SourcePeer string `json:"source_peer"`
		Source     string `json:"source"`
	}{
		{Port: 22, Protocol: "tcp", Action: "allow", SourcePeer: "", Source: "coordinator"},
		{Port: 80, Protocol: "tcp", Action: "allow", SourcePeer: "", Source: "config"},
		{Port: 443, Protocol: "tcp", Action: "allow", SourcePeer: "", Source: "service"},
	}
	rulesJSON, _ := json.Marshal(mockRules)

	// Build reply: [MsgTypeFilterRulesReply][reqID:4][rules JSON]
	reply := make([]byte, 5+len(rulesJSON))
	reply[0] = MsgTypeFilterRulesReply
	reply[1] = byte(reqID >> 24)
	reply[2] = byte(reqID >> 16)
	reply[3] = byte(reqID >> 8)
	reply[4] = byte(reqID)
	copy(reply[5:], rulesJSON)

	err = conn.WriteMessage(websocket.BinaryMessage, reply)
	require.NoError(t, err)

	// Wait for response
	select {
	case rules := <-responseChan:
		// Verify the response matches what we sent
		assert.Equal(t, rulesJSON, rules, "should receive the same rules JSON")
	case err := <-errChan:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for filter rules response")
	}
}

func TestRelayManager_QueryFilterRules_Timeout(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	conn := connectRelay(t, ts.URL, peerName, jwtToken)
	defer func() { _ = conn.Close() }()

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Query filter rules with short timeout - don't respond
	_, err = srv.relay.QueryFilterRules(context.Background(), peerName, 100*time.Millisecond)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deadline exceeded")
}

func TestRelayManager_QueryFilterRules_PeerNotConnected(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Query filter rules for non-existent peer
	_, err = srv.relay.QueryFilterRules(context.Background(), "nonexistent-peer", 1*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

// TestRelayManager_QueryFilterRules_ConnectionClosed verifies that QueryFilterRules
// returns an error containing "closed" when the persistentConn is marked closed
// but still present in the relay manager's map.
func TestRelayManager_QueryFilterRules_ConnectionClosed(t *testing.T) {
	rm := newRelayManager()

	// Inject a fake persistentConn that is already closed, without a real WebSocket.
	// We create it with a non-nil writeChan so the struct is valid but mark it closed
	// directly so that QueryFilterRules hits the closed-check branch before attempting
	// to enqueue to the write channel.
	pc := &persistentConn{
		peerName:  "closed-peer",
		writeChan: make(chan []byte, 1),
		closeChan: make(chan struct{}),
		closed:    true,
	}

	rm.mu.Lock()
	rm.persistent["closed-peer"] = pc
	rm.mu.Unlock()

	_, err := rm.QueryFilterRules(context.Background(), "closed-peer", 1*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestRelayManager_StoresReportedLatency(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	conn := connectRelay(t, ts.URL, peerName, jwtToken)
	defer func() { _ = conn.Close() }()

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Send heartbeat with RTT and peer latencies
	stats := &proto.PeerStats{
		PacketsSent:      100,
		ActiveTunnels:    2,
		HeartbeatSentAt:  time.Now().UnixNano(),
		CoordinatorRTTMs: 42,
		PeerLatencies: map[string]int64{
			"peer-a": 15,
			"peer-b": 28,
		},
	}
	statsJSON, _ := json.Marshal(stats)

	msg := make([]byte, 1+2+len(statsJSON))
	msg[0] = MsgTypeHeartbeat
	msg[1] = byte(len(statsJSON) >> 8)
	msg[2] = byte(len(statsJSON))
	copy(msg[3:], statsJSON)

	err = conn.WriteMessage(websocket.BinaryMessage, msg)
	require.NoError(t, err)

	// Read ack
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, _, err = conn.ReadMessage()
	require.NoError(t, err)

	// Verify peer info stores the latency data
	srv.peersMu.RLock()
	peer := srv.peers[peerName]
	srv.peersMu.RUnlock()

	require.NotNil(t, peer, "peer should exist")
	assert.Equal(t, int64(42), peer.coordinatorRTT, "coordinator RTT should be stored")
	require.NotNil(t, peer.peerLatencies, "peer latencies should be stored")
	assert.Equal(t, int64(15), peer.peerLatencies["peer-a"])
	assert.Equal(t, int64(28), peer.peerLatencies["peer-b"])
}

// TestPeerDisconnectCleansMetrics verifies that Prometheus gauge series for a peer
// are deleted when the peer disconnects, preventing unbounded cardinality growth.
func TestPeerDisconnectCleansMetrics(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Inject test-specific metrics (unregistered, just for this test — bypasses singleton
	// so we get a clean slate and don't pollute the shared default registry).
	srv.coordMetrics = &CoordMetrics{
		PeerRTTSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_disconnect_peer_rtt_seconds",
		}, []string{"peer"}),
		PeerLatencySeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_disconnect_peer_latency_seconds",
		}, []string{"source", "target"}),
		OnlinePeers:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_disconnect_online_peers"}),
		TotalHeartbeats: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_disconnect_heartbeats_total"}),
	}

	ts := httptest.NewServer(srv)
	defer ts.Close()

	peerName := "test-peer"
	jwtToken := registerPeerAndGetToken(t, ts.URL, peerName, cfg.AuthToken)

	conn := connectRelay(t, ts.URL, peerName, jwtToken)

	// Wait for server to register the connection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] != nil
	})
	require.NoError(t, err, "connection not registered")

	// Send heartbeat with RTT and peer latencies to populate metric series
	stats := &proto.PeerStats{
		CoordinatorRTTMs: 42,
		PeerLatencies: map[string]int64{
			"peer-b": 15000,
			"peer-c": 28000,
		},
	}
	statsJSON, _ := json.Marshal(stats)

	msg := make([]byte, 1+2+len(statsJSON))
	msg[0] = MsgTypeHeartbeat
	msg[1] = byte(len(statsJSON) >> 8)
	msg[2] = byte(len(statsJSON))
	copy(msg[3:], statsJSON)

	err = conn.WriteMessage(websocket.BinaryMessage, msg)
	require.NoError(t, err)

	// Read ack to ensure heartbeat was processed before we close
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, _, err = conn.ReadMessage()
	require.NoError(t, err)

	// Verify metric series were created before disconnect
	chRTT := make(chan prometheus.Metric, 10)
	srv.coordMetrics.PeerRTTSeconds.Collect(chRTT)
	close(chRTT)
	require.Len(t, drainMetricChan(chRTT), 1, "expected 1 RTT metric series before disconnect")

	chLatency := make(chan prometheus.Metric, 10)
	srv.coordMetrics.PeerLatencySeconds.Collect(chLatency)
	close(chLatency)
	require.Len(t, drainMetricChan(chLatency), 2, "expected 2 latency metric series before disconnect")

	// Disconnect the peer
	_ = conn.Close()

	// Wait for disconnect to be processed (defer cleanup runs)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	err = waitFor(ctx2, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerName] == nil
	})
	require.NoError(t, err, "peer should be unregistered after disconnect")

	// Verify RTT metric series were cleaned up
	chRTTAfter := make(chan prometheus.Metric, 10)
	srv.coordMetrics.PeerRTTSeconds.Collect(chRTTAfter)
	close(chRTTAfter)
	assert.Empty(t, drainMetricChan(chRTTAfter), "RTT metric series should be deleted after peer disconnect")

	// Verify latency metric series were cleaned up
	chLatencyAfter := make(chan prometheus.Metric, 10)
	srv.coordMetrics.PeerLatencySeconds.Collect(chLatencyAfter)
	close(chLatencyAfter)
	assert.Empty(t, drainMetricChan(chLatencyAfter), "latency metric series should be deleted after peer disconnect")
}

// drainMetricChan collects all metrics from a closed channel into a slice.
func drainMetricChan(ch <-chan prometheus.Metric) []prometheus.Metric {
	var result []prometheus.Metric
	for m := range ch {
		result = append(result, m)
	}
	return result
}

// readUntilType reads WebSocket messages until msgType is found or deadline expires.
func readUntilType(t *testing.T, conn *websocket.Conn, msgType byte, deadline time.Duration) ([]byte, bool) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(deadline))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return nil, false
		}
		if len(data) > 0 && data[0] == msgType {
			return data, true
		}
	}
}

// TestRelayManager_SendPacket_RoutingAuth verifies relay routing authorization:
// packets to registered peers are forwarded, packets to unregistered peers are dropped.
func TestRelayManager_SendPacket_RoutingAuth(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	peerA, peerB := "relay-auth-a", "relay-auth-b"
	tokenA := registerPeerAndGetToken(t, ts.URL, peerA, cfg.AuthToken)
	tokenB := registerPeerAndGetToken(t, ts.URL, peerB, cfg.AuthToken)

	connA := connectRelay(t, ts.URL, peerA, tokenA)
	defer func() { _ = connA.Close() }()
	connB := connectRelay(t, ts.URL, peerB, tokenB)
	defer func() { _ = connB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerA] != nil && srv.relay.persistent[peerB] != nil
	})
	require.NoError(t, err, "both connections must be registered")

	t.Run("registered target receives packet", func(t *testing.T) {
		payload := []byte("hello-registered")
		msg := make([]byte, 2+len(peerB)+len(payload))
		msg[0] = MsgTypeSendPacket
		msg[1] = byte(len(peerB))
		copy(msg[2:], peerB)
		copy(msg[2+len(peerB):], payload)

		require.NoError(t, connA.WriteMessage(websocket.BinaryMessage, msg))

		data, found := readUntilType(t, connB, MsgTypeRecvPacket, 2*time.Second)
		require.True(t, found, "peerB should receive the forwarded packet")
		assert.Equal(t, MsgTypeRecvPacket, data[0])
	})

	t.Run("unregistered target is dropped", func(t *testing.T) {
		target := "nonexistent-peer-xyz"
		payload := []byte("should-be-dropped")
		msg := make([]byte, 2+len(target)+len(payload))
		msg[0] = MsgTypeSendPacket
		msg[1] = byte(len(target))
		copy(msg[2:], target)
		copy(msg[2+len(target):], payload)

		require.NoError(t, connA.WriteMessage(websocket.BinaryMessage, msg))

		// peerB should not receive anything (short deadline → expect timeout)
		_ = connB.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		_, _, readErr := connB.ReadMessage()
		assert.Error(t, readErr, "no packet should be forwarded when target is unregistered")
		_ = connB.SetReadDeadline(time.Time{})
	})
}

// TestRelayManager_QueryFilterRules_Limit verifies that QueryFilterRules fails fast
// when the pending API requests map is full.
func TestRelayManager_QueryFilterRules_Limit(t *testing.T) {
	rm := newRelayManager()

	// Pre-fill the apiRequests map to the limit
	rm.apiRequestsMu.Lock()
	for i := 0; i < maxPendingAPIRequests; i++ {
		rm.apiRequests[uint32(i)] = make(chan []byte, 1)
	}
	rm.apiRequestsMu.Unlock()

	// Insert a fake persistentConn so the "peer not connected" check passes
	pc := &persistentConn{
		peerName:  "limit-peer",
		writeChan: make(chan []byte, 1),
		closeChan: make(chan struct{}),
	}
	rm.mu.Lock()
	rm.persistent["limit-peer"] = pc
	rm.mu.Unlock()

	_, err := rm.QueryFilterRules(context.Background(), "limit-peer", 5*time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many pending relay API requests")
}

// TestRelayManager_SendPacket_RateLimit verifies that per-peer relay rate limiting
// drops packets when the rate limiter is exhausted.
func TestRelayManager_SendPacket_RateLimit(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	ts := httptest.NewServer(srv)
	defer ts.Close()

	peerA, peerB := "rate-limit-a", "rate-limit-b"
	tokenA := registerPeerAndGetToken(t, ts.URL, peerA, cfg.AuthToken)
	tokenB := registerPeerAndGetToken(t, ts.URL, peerB, cfg.AuthToken)

	connA := connectRelay(t, ts.URL, peerA, tokenA)
	defer func() { _ = connA.Close() }()
	connB := connectRelay(t, ts.URL, peerB, tokenB)
	defer func() { _ = connB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		srv.relay.mu.Lock()
		defer srv.relay.mu.Unlock()
		return srv.relay.persistent[peerA] != nil && srv.relay.persistent[peerB] != nil
	})
	require.NoError(t, err, "both connections must be registered")

	// Replace peerA's rate limiter with a very restrictive one (1 pkt/s, burst 1)
	srv.relay.mu.Lock()
	srv.relay.persistent[peerA].rateLimiter = rate.NewLimiter(1, 1)
	srv.relay.mu.Unlock()

	// Send 10 packets rapidly from A to B
	payload := []byte("rate-test")
	for i := 0; i < 10; i++ {
		msg := make([]byte, 2+len(peerB)+len(payload))
		msg[0] = MsgTypeSendPacket
		msg[1] = byte(len(peerB))
		copy(msg[2:], peerB)
		copy(msg[2+len(peerB):], payload)
		require.NoError(t, connA.WriteMessage(websocket.BinaryMessage, msg))
	}

	// Count packets received by peerB within a short window
	received := 0
	_ = connB.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	for {
		_, data, readErr := connB.ReadMessage()
		if readErr != nil {
			break
		}
		if len(data) > 0 && data[0] == MsgTypeRecvPacket {
			received++
		}
	}

	// With burst=1 rate limiter, only 1 packet should pass
	assert.Equal(t, 1, received, "rate limiter should allow only 1 packet (burst=1)")
}
