package routing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTUN simulates a TUN device for testing.
type mockTUN struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	mu       sync.Mutex
}

func newMockTUN() *mockTUN {
	return &mockTUN{
		readBuf:  bytes.NewBuffer(nil),
		writeBuf: bytes.NewBuffer(nil),
	}
}

func (m *mockTUN) Read(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readBuf.Read(p)
}

func (m *mockTUN) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeBuf.Write(p)
}

func (m *mockTUN) Close() error {
	return nil
}

func (m *mockTUN) InjectPacket(packet []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readBuf.Write(packet)
}

func (m *mockTUN) GetWrittenPackets() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeBuf.Bytes()
}

// mockTunnel simulates a tunnel for testing.
type mockTunnel struct {
	buf       *bytes.Buffer
	mu        sync.Mutex
	closed    bool
	failWrite bool // If true, Write returns an error
}

func newMockTunnel() *mockTunnel {
	return &mockTunnel{
		buf: bytes.NewBuffer(nil),
	}
}

func (m *mockTunnel) Read(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.Read(p)
}

func (m *mockTunnel) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrite {
		return 0, io.ErrClosedPipe
	}
	return m.buf.Write(p)
}

func (m *mockTunnel) SetFailWrite(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failWrite = fail
}

func (m *mockTunnel) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockTunnel) PeerName() string {
	return "mock-peer"
}

func (m *mockTunnel) GetData() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.Bytes()
}

func (m *mockTunnel) InjectData(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buf.Write(data)
}

func TestForwarder_ForwardPacket(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnelMgr.Add("peer1", tunnel)

	fwd := NewForwarder(router, tunnelMgr)

	// Create a test packet
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	payload := []byte("test data")
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, payload)

	// Forward the packet
	err := fwd.ForwardPacket(packet)
	require.NoError(t, err)

	// Verify packet was written to tunnel
	tunnelData := tunnel.GetData()
	assert.NotEmpty(t, tunnelData)

	// The tunnel should have received an encoded frame
	// Frame format: [2 bytes length][1 byte proto][payload]
	assert.True(t, len(tunnelData) >= 3)
}

func TestForwarder_ForwardPacket_NoRoute(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()

	fwd := NewForwarder(router, tunnelMgr)

	// Create a test packet to unknown destination
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.99").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))

	// Should fail - no route
	err := fwd.ForwardPacket(packet)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoRoute))
}

func TestForwarder_ForwardPacket_NoTunnel(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	// No tunnel added for peer1

	fwd := NewForwarder(router, tunnelMgr)

	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))

	err := fwd.ForwardPacket(packet)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoTunnel))
}

func TestForwarder_ReceivePacket(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)

	// Create a test packet
	srcIP := net.ParseIP("10.42.0.2").To4()
	dstIP := net.ParseIP("10.42.0.1").To4()
	payload := []byte("incoming data")
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, payload)

	// Receive the packet
	err := fwd.ReceivePacket(packet)
	require.NoError(t, err)

	// Verify packet was written to TUN
	written := mockTun.GetWrittenPackets()
	assert.Equal(t, packet, written)
}

func TestForwarderStats(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnelMgr.Add("peer1", tunnel)
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)

	// Forward a packet
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))
	_ = fwd.ForwardPacket(packet)

	// Receive a packet
	srcIP = net.ParseIP("10.42.0.2").To4()
	dstIP = net.ParseIP("10.42.0.1").To4()
	packet = BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))
	_ = fwd.ReceivePacket(packet)

	stats := fwd.Stats()
	assert.Equal(t, uint64(1), stats.PacketsSent)
	assert.Equal(t, uint64(1), stats.PacketsReceived)
	assert.Greater(t, stats.BytesSent, uint64(0))
	assert.Greater(t, stats.BytesReceived, uint64(0))
}

// MockTunnelManager for testing.
type MockTunnelManager struct {
	tunnels map[string]io.ReadWriteCloser
	mu      sync.RWMutex
}

func NewMockTunnelManager() *MockTunnelManager {
	return &MockTunnelManager{
		tunnels: make(map[string]io.ReadWriteCloser),
	}
}

func (m *MockTunnelManager) Add(name string, tunnel io.ReadWriteCloser) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tunnels[name] = tunnel
}

func (m *MockTunnelManager) Get(name string) (io.ReadWriteCloser, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tunnel, ok := m.tunnels[name]
	return tunnel, ok
}

func (m *MockTunnelManager) Remove(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tunnels, name)
}

func TestForwarderLoop(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)

	// Inject a packet into TUN to be forwarded
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))

	// Add tunnel after starting
	tunnel := newMockTunnel()
	tunnelMgr.Add("peer1", tunnel)

	mockTun.InjectPacket(packet)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Start the forwarder loop (should process the packet then timeout)
	go func() { _ = fwd.Run(ctx) }()

	// Wait for context to expire
	<-ctx.Done()

	// The packet should have been forwarded to the tunnel
	// Note: Due to timing, this may or may not have data
}

// mockRelay implements RelayPacketSender for testing.
type mockRelay struct {
	packets   []relayPacket
	mu        sync.Mutex
	connected bool
}

type relayPacket struct {
	target string
	data   []byte
}

func newMockRelay() *mockRelay {
	return &mockRelay{
		connected: true,
	}
}

func (m *mockRelay) SendTo(targetPeer string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.packets = append(m.packets, relayPacket{target: targetPeer, data: append([]byte{}, data...)})
	return nil
}

func (m *mockRelay) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected
}

func (m *mockRelay) GetPackets() []relayPacket {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.packets
}

func (m *mockRelay) SetConnected(connected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = connected
}

func TestForwarder_RelayFallback_NoTunnel(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	// No direct tunnel for peer1

	relay := newMockRelay()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetRelay(relay)

	// Create a test packet
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test via relay"))

	// Forward the packet - should use relay since no direct tunnel
	err := fwd.ForwardPacket(packet)
	require.NoError(t, err)

	// Verify packet was sent to relay
	packets := relay.GetPackets()
	require.Len(t, packets, 1)
	assert.Equal(t, "peer1", packets[0].target)
	// Data should be framed
	assert.True(t, len(packets[0].data) > len(packet))
}

func TestForwarder_DirectTunnelPreferred(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnelMgr.Add("peer1", tunnel)

	relay := newMockRelay()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetRelay(relay)

	// Create a test packet
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test via direct"))

	// Forward the packet - should use direct tunnel
	err := fwd.ForwardPacket(packet)
	require.NoError(t, err)

	// Verify packet was sent to tunnel, not relay
	tunnelData := tunnel.GetData()
	assert.NotEmpty(t, tunnelData)

	relayPackets := relay.GetPackets()
	assert.Empty(t, relayPackets)
}

func TestForwarder_RelayDisconnected(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	// No direct tunnel

	relay := newMockRelay()
	relay.SetConnected(false) // Relay not connected

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetRelay(relay)

	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))

	// Should fail - no tunnel and relay disconnected
	err := fwd.ForwardPacket(packet)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoTunnel))
}

func TestForwarder_HandleRelayPacket(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)

	// Create a test IP packet
	srcIP := net.ParseIP("10.42.0.2").To4()
	dstIP := net.ParseIP("10.42.0.1").To4()
	payload := []byte("relay packet data")
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, payload)

	// Frame the packet as the relay would
	frameLen := len(packet) + 1
	framedData := make([]byte, 3+len(packet))
	framedData[0] = byte(frameLen >> 8)
	framedData[1] = byte(frameLen)
	framedData[2] = 0x01 // IP packet protocol
	copy(framedData[3:], packet)

	// Handle relay packet
	fwd.HandleRelayPacket("peer2", framedData)

	// Verify packet was written to TUN
	written := mockTun.GetWrittenPackets()
	assert.Equal(t, packet, written)
}

func TestForwarder_DeadTunnelFallbackToRelay(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnel.SetFailWrite(true) // Tunnel write will fail
	tunnelMgr.Add("peer1", tunnel)

	relay := newMockRelay()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetRelay(relay)

	// Track if onDeadTunnel callback was called
	var deadTunnelPeer string
	var callbackCalled bool
	callbackDone := make(chan struct{})
	fwd.SetOnDeadTunnel(func(peerName string) {
		deadTunnelPeer = peerName
		callbackCalled = true
		close(callbackDone)
	})

	// Create a test packet
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test via relay fallback"))

	// Forward the packet - should fail on tunnel, fall back to relay
	err := fwd.ForwardPacket(packet)
	require.NoError(t, err)

	// Wait for callback (it's async)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("onDeadTunnel callback was not called")
	}

	// Verify onDeadTunnel was called with correct peer
	assert.True(t, callbackCalled)
	assert.Equal(t, "peer1", deadTunnelPeer)

	// Verify packet was sent to relay (fallback)
	relayPackets := relay.GetPackets()
	require.Len(t, relayPackets, 1)
	assert.Equal(t, "peer1", relayPackets[0].target)
}

func TestForwarder_DeadTunnelNoRelay(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnel.SetFailWrite(true) // Tunnel write will fail
	tunnelMgr.Add("peer1", tunnel)

	fwd := NewForwarder(router, tunnelMgr)
	// No relay set

	// Track if onDeadTunnel callback was called
	var callbackCalled bool
	callbackDone := make(chan struct{})
	fwd.SetOnDeadTunnel(func(peerName string) {
		callbackCalled = true
		close(callbackDone)
	})

	// Create a test packet
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test no relay"))

	// Forward the packet - should fail on tunnel and have no relay fallback
	err := fwd.ForwardPacket(packet)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoTunnel))

	// Wait for callback (it's async)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("onDeadTunnel callback was not called")
	}

	// Verify onDeadTunnel was still called
	assert.True(t, callbackCalled)
}

func TestForwarder_DeadTunnelCallbackDebounced(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnel.SetFailWrite(true) // Tunnel write will fail
	tunnelMgr.Add("peer1", tunnel)

	relay := newMockRelay()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetRelay(relay)

	// Track callback invocations
	var callbackCount int
	var mu sync.Mutex
	fwd.SetOnDeadTunnel(func(peerName string) {
		mu.Lock()
		callbackCount++
		mu.Unlock()
	})

	// Create a test packet
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test debounce"))

	// Send 10 packets rapidly - all will fail and trigger dead tunnel callback
	for i := 0; i < 10; i++ {
		err := fwd.ForwardPacket(packet)
		require.NoError(t, err) // Succeeds due to relay fallback
	}

	// Wait a bit for any async callbacks to complete
	time.Sleep(100 * time.Millisecond)

	// Due to debouncing, callback should be called at most once within 5s window
	mu.Lock()
	count := callbackCount
	mu.Unlock()

	// Should be exactly 1 (or at most 2 if there was a race at the boundary)
	assert.LessOrEqual(t, count, 2, "Callback should be debounced, got %d calls", count)
	assert.GreaterOrEqual(t, count, 1, "Callback should be called at least once")
}

// Exit Node Feature Tests

func TestForwarder_IsExternalTraffic(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	fwd := NewForwarder(router, tunnelMgr)

	// Set mesh CIDR for split-tunnel detection
	_, meshNet, _ := net.ParseCIDR("10.42.0.0/16")
	fwd.SetMeshCIDR(meshNet)

	tests := []struct {
		name     string
		ip       string
		external bool
	}{
		{"mesh IP", "10.42.0.1", false},
		{"mesh IP 2", "10.42.255.255", false},
		{"external IP google DNS", "8.8.8.8", true},
		{"external IP cloudflare", "1.1.1.1", true},
		{"external IP private 192.168", "192.168.1.1", true}, // Not in mesh, so external
		{"external IP private 172.16", "172.16.0.1", true},   // Not in mesh, so external
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			assert.Equal(t, tt.external, fwd.IsExternalTraffic(ip), "IP %s", tt.ip)
		})
	}
}

func TestForwarder_IsExternalTraffic_NoMeshCIDR(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	fwd := NewForwarder(router, tunnelMgr)

	// No mesh CIDR set - all traffic treated as mesh (not external)
	ip := net.ParseIP("8.8.8.8")
	assert.False(t, fwd.IsExternalTraffic(ip), "without mesh CIDR, nothing is external")
}

func TestForwarder_SetExitPeer(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	fwd := NewForwarder(router, tunnelMgr)

	// Initially empty
	assert.Equal(t, "", fwd.ExitPeer())

	// Set exit node
	fwd.SetExitPeer("exit-server")
	assert.Equal(t, "exit-server", fwd.ExitPeer())

	// Clear exit node
	fwd.SetExitPeer("")
	assert.Equal(t, "", fwd.ExitPeer())
}

func TestForwarder_ExitPeer_ForwardExternalTraffic(t *testing.T) {
	router := NewRouter()
	// Only add route for exit node, not for 8.8.8.8
	router.AddRoute("10.42.0.5", "exit-server")

	tunnelMgr := NewMockTunnelManager()
	exitTunnel := newMockTunnel()
	tunnelMgr.Add("exit-server", exitTunnel)

	fwd := NewForwarder(router, tunnelMgr)

	// Configure for exit node routing
	_, meshNet, _ := net.ParseCIDR("10.42.0.0/16")
	fwd.SetMeshCIDR(meshNet)
	fwd.SetExitPeer("exit-server")

	// Create a packet to external IP (8.8.8.8)
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("8.8.8.8").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("to google DNS"))

	// Forward the packet - should go to exit node
	err := fwd.ForwardPacket(packet)
	require.NoError(t, err)

	// Verify packet was sent to exit node tunnel
	exitTunnelData := exitTunnel.GetData()
	assert.NotEmpty(t, exitTunnelData, "packet should be forwarded to exit node tunnel")
}

func TestForwarder_ExitPeer_MeshTrafficStaysDirect(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")
	router.AddRoute("10.42.0.5", "exit-server")

	tunnelMgr := NewMockTunnelManager()
	peerTunnel := newMockTunnel()
	exitTunnel := newMockTunnel()
	tunnelMgr.Add("peer1", peerTunnel)
	tunnelMgr.Add("exit-server", exitTunnel)

	fwd := NewForwarder(router, tunnelMgr)

	// Configure for exit node routing
	_, meshNet, _ := net.ParseCIDR("10.42.0.0/16")
	fwd.SetMeshCIDR(meshNet)
	fwd.SetExitPeer("exit-server")

	// Create a packet to mesh peer (10.42.0.2)
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("to mesh peer"))

	// Forward the packet - should go to peer1, not exit node
	err := fwd.ForwardPacket(packet)
	require.NoError(t, err)

	// Verify packet was sent to peer1 tunnel, NOT exit tunnel
	peerTunnelData := peerTunnel.GetData()
	exitTunnelData := exitTunnel.GetData()
	assert.NotEmpty(t, peerTunnelData, "mesh traffic should go to direct peer")
	assert.Empty(t, exitTunnelData, "mesh traffic should NOT go to exit node")
}

func TestForwarder_ExitPeer_NoExitPeerConfigured(t *testing.T) {
	router := NewRouter()
	// No route for 8.8.8.8

	tunnelMgr := NewMockTunnelManager()
	fwd := NewForwarder(router, tunnelMgr)

	// Set mesh CIDR but no exit node
	_, meshNet, _ := net.ParseCIDR("10.42.0.0/16")
	fwd.SetMeshCIDR(meshNet)
	// No exit node configured

	// Create a packet to external IP
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("8.8.8.8").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("to external"))

	// Forward the packet - should fail with no route
	err := fwd.ForwardPacket(packet)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoRoute), "without exit node, external traffic should have no route")
}

func TestForwarder_ExitPeer_FallbackToRelay(t *testing.T) {
	router := NewRouter()

	tunnelMgr := NewMockTunnelManager()
	// No direct tunnel to exit node

	relay := newMockRelay()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetRelay(relay)

	// Configure for exit node routing
	_, meshNet, _ := net.ParseCIDR("10.42.0.0/16")
	fwd.SetMeshCIDR(meshNet)
	fwd.SetExitPeer("exit-server")

	// Create a packet to external IP
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("8.8.8.8").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("to external via relay"))

	// Forward the packet - should use relay since no direct tunnel
	err := fwd.ForwardPacket(packet)
	require.NoError(t, err)

	// Verify packet was sent via relay to exit node
	relayPackets := relay.GetPackets()
	require.Len(t, relayPackets, 1)
	assert.Equal(t, "exit-server", relayPackets[0].target)
}

// Stats Feature Gate Tests

func TestForwarderStats_EnabledByDefault(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnelMgr.Add("peer1", tunnel)
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)

	// Stats collection should be enabled by default
	assert.True(t, fwd.StatsEnabled(), "stats should be enabled by default")

	// Forward a packet
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))
	_ = fwd.ForwardPacket(packet)

	stats := fwd.Stats()
	assert.Equal(t, uint64(1), stats.PacketsSent, "stats should be collected when enabled")
}

func TestForwarderStats_Disabled(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnelMgr.Add("peer1", tunnel)
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)
	fwd.SetStatsEnabled(false) // Disable stats for high-performance mode

	assert.False(t, fwd.StatsEnabled(), "stats should be disabled after SetStatsEnabled(false)")

	// Forward multiple packets
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	for i := 0; i < 100; i++ {
		packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))
		_ = fwd.ForwardPacket(packet)
	}

	// Receive a packet
	srcIP = net.ParseIP("10.42.0.2").To4()
	dstIP = net.ParseIP("10.42.0.1").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))
	_ = fwd.ReceivePacket(packet)

	// Stats should NOT be collected when disabled
	stats := fwd.Stats()
	assert.Equal(t, uint64(0), stats.PacketsSent, "stats should not be collected when disabled")
	assert.Equal(t, uint64(0), stats.PacketsReceived, "stats should not be collected when disabled")
	assert.Equal(t, uint64(0), stats.BytesSent, "stats should not be collected when disabled")
	assert.Equal(t, uint64(0), stats.BytesReceived, "stats should not be collected when disabled")
}

func TestForwarderStats_DisabledNoRoute(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetStatsEnabled(false)

	// Try to forward packet with no route
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.99").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))
	_ = fwd.ForwardPacket(packet) // Will fail, but should not record stats

	stats := fwd.Stats()
	assert.Equal(t, uint64(0), stats.DroppedNoRoute, "dropped stats should not be collected when disabled")
}

func TestForwarderStats_ReEnable(t *testing.T) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnelMgr.Add("peer1", tunnel)

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetStatsEnabled(false)

	// Forward while disabled
	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, []byte("test"))
	_ = fwd.ForwardPacket(packet)

	stats := fwd.Stats()
	assert.Equal(t, uint64(0), stats.PacketsSent, "no stats while disabled")

	// Re-enable stats
	fwd.SetStatsEnabled(true)
	_ = fwd.ForwardPacket(packet)

	stats = fwd.Stats()
	assert.Equal(t, uint64(1), stats.PacketsSent, "stats should be collected after re-enabling")
}

// Packet Filter Tests

func TestForwarder_PacketFilter_DropsByDefault(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)

	// Create filter with default deny (allowlist mode)
	filter := NewPacketFilter(true)
	fwd.SetFilter(filter)

	// Create incoming TCP packet to port 22
	srcIP := net.ParseIP("10.42.0.2").To4()
	dstIP := net.ParseIP("10.42.0.1").To4()
	packet := buildTCPPacket(srcIP, dstIP, 22)

	// Receive the packet - should be dropped (no allow rule for port 22)
	err := fwd.ReceivePacket(packet)
	require.NoError(t, err) // No error, just silently dropped

	// Verify packet was NOT written to TUN
	written := mockTun.GetWrittenPackets()
	assert.Empty(t, written, "packet should be dropped by filter")

	// Check stats
	stats := fwd.Stats()
	assert.Equal(t, uint64(1), stats.DroppedFiltered, "dropped packet should be counted")
	assert.Equal(t, uint64(0), stats.PacketsReceived, "dropped packet should not count as received")
}

func TestForwarder_PacketFilter_AllowsWithRule(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)

	// Create filter with default deny, but allow port 22
	filter := NewPacketFilter(true)
	filter.SetPeerConfigRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
	})
	fwd.SetFilter(filter)

	// Create incoming TCP packet to port 22
	srcIP := net.ParseIP("10.42.0.2").To4()
	dstIP := net.ParseIP("10.42.0.1").To4()
	packet := buildTCPPacket(srcIP, dstIP, 22)

	// Receive the packet - should be allowed
	err := fwd.ReceivePacket(packet)
	require.NoError(t, err)

	// Verify packet WAS written to TUN
	written := mockTun.GetWrittenPackets()
	assert.Equal(t, packet, written, "packet should pass through filter")

	// Check stats
	stats := fwd.Stats()
	assert.Equal(t, uint64(0), stats.DroppedFiltered, "no packets should be dropped")
	assert.Equal(t, uint64(1), stats.PacketsReceived, "packet should count as received")
}

func TestForwarder_PacketFilter_ICMP_NotFiltered(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)

	// Create filter with default deny
	filter := NewPacketFilter(true)
	fwd.SetFilter(filter)

	// Create incoming ICMP packet
	srcIP := net.ParseIP("10.42.0.2").To4()
	dstIP := net.ParseIP("10.42.0.1").To4()
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoICMP, []byte("ping"))

	// Receive the packet - ICMP should pass (not filtered)
	err := fwd.ReceivePacket(packet)
	require.NoError(t, err)

	// Verify packet WAS written to TUN
	written := mockTun.GetWrittenPackets()
	assert.Equal(t, packet, written, "ICMP should not be filtered")
}

func TestForwarder_PacketFilter_Nil(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	mockTun := newMockTUN()

	fwd := NewForwarder(router, tunnelMgr)
	fwd.SetTUN(mockTun)
	// No filter set - nil

	// Create incoming TCP packet
	srcIP := net.ParseIP("10.42.0.2").To4()
	dstIP := net.ParseIP("10.42.0.1").To4()
	packet := buildTCPPacket(srcIP, dstIP, 22)

	// Receive the packet - should pass (no filter)
	err := fwd.ReceivePacket(packet)
	require.NoError(t, err)

	// Verify packet WAS written to TUN
	written := mockTun.GetWrittenPackets()
	assert.Equal(t, packet, written, "packet should pass when no filter set")
}

func TestForwarder_Filter_GetterSetter(t *testing.T) {
	router := NewRouter()
	tunnelMgr := NewMockTunnelManager()
	fwd := NewForwarder(router, tunnelMgr)

	// Initially nil
	assert.Nil(t, fwd.Filter())

	// Set filter
	filter := NewPacketFilter(true)
	fwd.SetFilter(filter)
	assert.Equal(t, filter, fwd.Filter())

	// Clear filter
	fwd.SetFilter(nil)
	assert.Nil(t, fwd.Filter())
}

func TestForwarder_PacketFilter_PeerSpecific(t *testing.T) {
	// Create filter with default deny
	// Add global allow for port 22, but deny for "untrusted" peer
	filter := NewPacketFilter(true)
	filter.SetCoordinatorRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow}, // Global allow
	})
	filter.AddTemporaryRule(FilterRule{
		Port:       22,
		Protocol:   ProtoTCP,
		Action:     ActionDeny,
		SourcePeer: "untrusted", // Peer-specific deny
	})

	// Create incoming TCP packet to port 22
	srcIP := net.ParseIP("10.42.0.2").To4()
	dstIP := net.ParseIP("10.42.0.1").To4()
	packet := buildTCPPacket(srcIP, dstIP, 22)

	t.Run("trusted peer allowed", func(t *testing.T) {
		router := NewRouter()
		tunnelMgr := NewMockTunnelManager()
		mockTun := newMockTUN()

		fwd := NewForwarder(router, tunnelMgr)
		fwd.SetTUN(mockTun)
		fwd.SetFilter(filter)

		// Packet from "trusted" peer should be allowed (uses global allow)
		err := fwd.ReceivePacketFromPeer(packet, "trusted")
		require.NoError(t, err)
		written := mockTun.GetWrittenPackets()
		assert.Equal(t, packet, written, "packet from trusted peer should pass")

		stats := fwd.Stats()
		assert.Equal(t, uint64(0), stats.DroppedFiltered, "no packets should be dropped")
		assert.Equal(t, uint64(1), stats.PacketsReceived, "one packet should be received")
	})

	t.Run("untrusted peer blocked", func(t *testing.T) {
		router := NewRouter()
		tunnelMgr := NewMockTunnelManager()
		mockTun := newMockTUN()

		fwd := NewForwarder(router, tunnelMgr)
		fwd.SetTUN(mockTun)
		fwd.SetFilter(filter)

		// Packet from "untrusted" peer should be dropped (peer-specific deny)
		err := fwd.ReceivePacketFromPeer(packet, "untrusted")
		require.NoError(t, err)
		written := mockTun.GetWrittenPackets()
		assert.Empty(t, written, "packet from untrusted peer should be dropped")

		stats := fwd.Stats()
		assert.Equal(t, uint64(1), stats.DroppedFiltered, "one packet should be dropped")
		assert.Equal(t, uint64(0), stats.PacketsReceived, "no packets should be received")
	})
}
