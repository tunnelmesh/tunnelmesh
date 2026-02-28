package connection

import (
	"context"
	"io"
	"sync"
	"testing"
)

// mockTunnelProvider implements TunnelProvider for testing.
type mockTunnelProvider struct {
	mu      sync.Mutex
	tunnels map[string]io.ReadWriteCloser
}

func newMockTunnelProvider() *mockTunnelProvider {
	return &mockTunnelProvider{
		tunnels: make(map[string]io.ReadWriteCloser),
	}
}

func (t *mockTunnelProvider) Add(name string, tunnel io.ReadWriteCloser) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tunnels[name] = tunnel
}

func (t *mockTunnelProvider) Remove(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tunnels, name)
}

func (t *mockTunnelProvider) Get(name string) (io.ReadWriteCloser, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tun, ok := t.tunnels[name]
	return tun, ok
}

func (t *mockTunnelProvider) Has(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.tunnels[name]
	return ok
}

func TestNewLifecycleManager(t *testing.T) {
	tunnels := newMockTunnelProvider()

	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	if lm == nil {
		t.Fatal("NewLifecycleManager() returned nil")
	}

	if len(lm.List()) != 0 {
		t.Errorf("New manager should have no connections, got %d", len(lm.List()))
	}
}

func TestLifecycleManager_GetOrCreate(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	// Create new connection
	pc1 := lm.GetOrCreate("peer1", "10.0.0.1")
	if pc1 == nil {
		t.Fatal("GetOrCreate() returned nil")
	}
	if pc1.PeerName() != "peer1" {
		t.Errorf("PeerName() = %q, want %q", pc1.PeerName(), "peer1")
	}
	if pc1.MeshIP() != "10.0.0.1" {
		t.Errorf("MeshIP() = %q, want %q", pc1.MeshIP(), "10.0.0.1")
	}

	// Get existing connection
	pc2 := lm.GetOrCreate("peer1", "10.0.0.1")
	if pc1 != pc2 {
		t.Error("GetOrCreate() should return the same connection for same peer")
	}

	// Create another connection
	pc3 := lm.GetOrCreate("peer2", "10.0.0.2")
	if pc3 == pc1 {
		t.Error("GetOrCreate() should create different connections for different peers")
	}

	// List should have 2 peers
	list := lm.List()
	if len(list) != 2 {
		t.Errorf("List() should have 2 peers, got %d", len(list))
	}
}

func TestLifecycleManager_Get(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	// Get non-existent
	pc := lm.Get("peer1")
	if pc != nil {
		t.Error("Get() should return nil for non-existent peer")
	}

	// Create and get
	lm.GetOrCreate("peer1", "10.0.0.1")
	pc = lm.Get("peer1")
	if pc == nil {
		t.Error("Get() should return the connection after creation")
	}
}

func TestLifecycleManager_OnConnectCallback(t *testing.T) {
	tunnels := newMockTunnelProvider()

	var connectedPeers []string
	var mu sync.Mutex

	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
		OnConnect: func(peerName string) {
			mu.Lock()
			connectedPeers = append(connectedPeers, peerName)
			mu.Unlock()
		},
	})

	pc := lm.GetOrCreate("peer1", "10.0.0.1")

	// No callback yet
	mu.Lock()
	if len(connectedPeers) != 0 {
		t.Error("OnConnect should not be called before connect")
	}
	mu.Unlock()

	// Connect - callback should be called
	tunnel := &mockTunnel{}
	_ = pc.Connected(context.Background(), tunnel, "test", "test")

	mu.Lock()
	if len(connectedPeers) != 1 || connectedPeers[0] != "peer1" {
		t.Errorf("OnConnect should be called with peer1, got %v", connectedPeers)
	}
	mu.Unlock()

	// Disconnect and reconnect - callback should be called again
	_ = pc.Disconnect(context.Background(), "test", nil)
	_ = pc.Connected(context.Background(), &mockTunnel{}, "test", "reconnect")

	mu.Lock()
	if len(connectedPeers) != 2 {
		t.Errorf("OnConnect should be called twice, got %d calls", len(connectedPeers))
	}
	mu.Unlock()
}

func TestLifecycleManager_OnDisconnectCallback(t *testing.T) {
	tunnels := newMockTunnelProvider()

	var disconnectedPeers []string
	var mu sync.Mutex

	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
		OnDisconnect: func(peerName string) {
			mu.Lock()
			disconnectedPeers = append(disconnectedPeers, peerName)
			mu.Unlock()
		},
	})

	pc := lm.GetOrCreate("peer1", "10.0.0.1")

	// Connect
	tunnel := &mockTunnel{}
	_ = pc.Connected(context.Background(), tunnel, "test", "test")

	// No callback yet
	mu.Lock()
	if len(disconnectedPeers) != 0 {
		t.Error("OnDisconnect should not be called before disconnect")
	}
	mu.Unlock()

	// Disconnect - callback should be called
	_ = pc.Disconnect(context.Background(), "test", nil)

	mu.Lock()
	if len(disconnectedPeers) != 1 || disconnectedPeers[0] != "peer1" {
		t.Errorf("OnDisconnect should be called with peer1, got %v", disconnectedPeers)
	}
	mu.Unlock()
}

func TestLifecycleManager_TunnelLifecycle(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	pc := lm.GetOrCreate("peer1", "10.0.0.1")

	// Initially no tunnel
	if tunnels.Has("peer1") {
		t.Error("Should have no tunnel before Connected()")
	}

	// Connect - tunnel should be added
	tunnel := &mockTunnel{}
	_ = pc.Connected(context.Background(), tunnel, "test", "test")

	if !tunnels.Has("peer1") {
		t.Error("Tunnel should be added after Connected()")
	}
	gotTunnel, _ := tunnels.Get("peer1")
	if gotTunnel != tunnel {
		t.Error("TunnelProvider should have the correct tunnel")
	}

	// Disconnect - tunnel should be removed
	_ = pc.Disconnect(context.Background(), "test", nil)

	if tunnels.Has("peer1") {
		t.Error("Tunnel should be removed after Disconnect()")
	}
}

func TestLifecycleManager_ReconnectingRemovesTunnel(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	pc := lm.GetOrCreate("peer1", "10.0.0.1")

	// Connect
	tunnel := &mockTunnel{}
	_ = pc.Connected(context.Background(), tunnel, "test", "test")

	if !tunnels.Has("peer1") {
		t.Error("Tunnel should exist after Connected()")
	}

	// Start reconnecting - tunnel should be removed
	_ = pc.StartReconnecting("network error", nil)

	if tunnels.Has("peer1") {
		t.Error("Tunnel should be removed during Reconnecting")
	}

	// Reconnect - tunnel should be added back
	tunnel2 := &mockTunnel{}
	_ = pc.Connected(context.Background(), tunnel2, "test", "reconnected")

	if !tunnels.Has("peer1") {
		t.Error("Tunnel should exist after reconnect")
	}
}

func TestLifecycleManager_CloseRemovesTunnel(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	pc := lm.GetOrCreate("peer1", "10.0.0.1")

	// Connect
	tunnel := &mockTunnel{}
	_ = pc.Connected(context.Background(), tunnel, "test", "test")

	// Close - tunnel should be removed
	_ = pc.Close()

	if tunnels.Has("peer1") {
		t.Error("Tunnel should be removed after Close()")
	}
}

func TestLifecycleManager_Remove(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	pc := lm.GetOrCreate("peer1", "10.0.0.1")
	tunnel := &mockTunnel{}
	_ = pc.Connected(context.Background(), tunnel, "test", "test")

	// Remove from manager
	lm.Remove("peer1")

	// Connection should be gone
	if lm.Get("peer1") != nil {
		t.Error("Connection should be removed from manager")
	}

	// Tunnel should be cleaned up
	if tunnels.Has("peer1") {
		t.Error("Tunnel should be removed after Remove()")
	}
}

func TestLifecycleManager_CloseAll(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	// Create multiple connections
	pc1 := lm.GetOrCreate("peer1", "10.0.0.1")
	pc2 := lm.GetOrCreate("peer2", "10.0.0.2")

	tunnel1 := &mockTunnel{}
	tunnel2 := &mockTunnel{}
	_ = pc1.Connected(context.Background(), tunnel1, "test", "test")
	_ = pc2.Connected(context.Background(), tunnel2, "test", "test")

	// Close all
	lm.CloseAll()

	// All connections should be gone
	if len(lm.List()) != 0 {
		t.Errorf("All connections should be removed, got %d", len(lm.List()))
	}

	// All tunnels should be cleaned up
	if tunnels.Has("peer1") || tunnels.Has("peer2") {
		t.Error("All tunnels should be removed after CloseAll()")
	}
}

func TestLifecycleManager_DisconnectAll(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	// Create multiple connections
	pc1 := lm.GetOrCreate("peer1", "10.0.0.1")
	pc2 := lm.GetOrCreate("peer2", "10.0.0.2")

	tunnel1 := &mockTunnel{}
	tunnel2 := &mockTunnel{}
	_ = pc1.Connected(context.Background(), tunnel1, "test", "test")
	_ = pc2.Connected(context.Background(), tunnel2, "test", "test")

	// Verify connections are in Connected state
	if pc1.State() != StateConnected || pc2.State() != StateConnected {
		t.Error("Both connections should be in Connected state before DisconnectAll")
	}

	// Disconnect all (non-terminal - connections remain in manager)
	lm.DisconnectAll("network change")

	// Connections should still be in manager but in Disconnected state
	if len(lm.List()) != 2 {
		t.Errorf("Connections should remain in manager after DisconnectAll, got %d", len(lm.List()))
	}

	// Both connections should be in Disconnected state (reusable)
	if pc1.State() != StateDisconnected {
		t.Errorf("peer1 should be Disconnected after DisconnectAll, got %v", pc1.State())
	}
	if pc2.State() != StateDisconnected {
		t.Errorf("peer2 should be Disconnected after DisconnectAll, got %v", pc2.State())
	}

	// All tunnels should be cleaned up
	if tunnels.Has("peer1") || tunnels.Has("peer2") {
		t.Error("All tunnels should be removed after DisconnectAll()")
	}

	// Verify tunnels were closed
	if !tunnel1.closed || !tunnel2.closed {
		t.Error("Tunnels should be closed after DisconnectAll()")
	}

	// Connections can be reused - try reconnecting
	newTunnel := &mockTunnel{}
	err := pc1.Connected(context.Background(), newTunnel, "test", "reconnection")
	if err != nil {
		t.Errorf("Should be able to reconnect after DisconnectAll: %v", err)
	}
	if pc1.State() != StateConnected {
		t.Errorf("peer1 should be Connected after reconnection, got %v", pc1.State())
	}
}

func TestLifecycleManager_ListByState(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	// Create connections in different states
	pc1 := lm.GetOrCreate("peer1", "10.0.0.1")
	pc2 := lm.GetOrCreate("peer2", "10.0.0.2")
	_ = lm.GetOrCreate("peer3", "10.0.0.3") // stays disconnected

	_ = pc1.StartConnecting("test")
	_ = pc2.Connected(context.Background(), &mockTunnel{}, "test", "test")

	connecting := lm.ListByState(StateConnecting)
	if len(connecting) != 1 || connecting[0] != "peer1" {
		t.Errorf("ListByState(Connecting) = %v, want [peer1]", connecting)
	}

	connected := lm.ListByState(StateConnected)
	if len(connected) != 1 || connected[0] != "peer2" {
		t.Errorf("ListByState(Connected) = %v, want [peer2]", connected)
	}

	disconnected := lm.ListByState(StateDisconnected)
	if len(disconnected) != 1 || disconnected[0] != "peer3" {
		t.Errorf("ListByState(Disconnected) = %v, want [peer3]", disconnected)
	}
}

func TestLifecycleManager_CountByState(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	// Create connections in different states
	pc1 := lm.GetOrCreate("peer1", "10.0.0.1")
	pc2 := lm.GetOrCreate("peer2", "10.0.0.2")
	_ = lm.GetOrCreate("peer3", "10.0.0.3") // stays disconnected

	_ = pc1.Connected(context.Background(), &mockTunnel{}, "test", "test")
	_ = pc2.Connected(context.Background(), &mockTunnel{}, "test", "test")

	if lm.CountByState(StateConnected) != 2 {
		t.Errorf("CountByState(Connected) = %d, want 2", lm.CountByState(StateConnected))
	}
	if lm.CountByState(StateDisconnected) != 1 {
		t.Errorf("CountByState(Disconnected) = %d, want 1", lm.CountByState(StateDisconnected))
	}
	if lm.CountByState(StateConnecting) != 0 {
		t.Errorf("CountByState(Connecting) = %d, want 0", lm.CountByState(StateConnecting))
	}
}

func TestLifecycleManager_IsConnecting(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{})

	// Non-existent peer
	if lm.IsConnecting("peer1") {
		t.Error("IsConnecting() should return false for non-existent peer")
	}

	pc := lm.GetOrCreate("peer1", "10.0.0.1")

	// Disconnected
	if lm.IsConnecting("peer1") {
		t.Error("IsConnecting() should return false for disconnected peer")
	}

	// Connecting
	_ = pc.StartConnecting("test")
	if !lm.IsConnecting("peer1") {
		t.Error("IsConnecting() should return true for connecting peer")
	}

	// Connected
	_ = pc.Connected(context.Background(), &mockTunnel{}, "test", "test")
	if lm.IsConnecting("peer1") {
		t.Error("IsConnecting() should return false for connected peer")
	}
}

func TestLifecycleManager_IsConnected(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{})

	// Non-existent peer
	if lm.IsConnected("peer1") {
		t.Error("IsConnected() should return false for non-existent peer")
	}

	pc := lm.GetOrCreate("peer1", "10.0.0.1")

	// Disconnected
	if lm.IsConnected("peer1") {
		t.Error("IsConnected() should return false for disconnected peer")
	}

	// Connected
	_ = pc.Connected(context.Background(), &mockTunnel{}, "test", "test")
	if !lm.IsConnected("peer1") {
		t.Error("IsConnected() should return true for connected peer")
	}
}

func TestLifecycleManager_State(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{})

	// Non-existent peer returns Disconnected
	if lm.State("peer1") != StateDisconnected {
		t.Errorf("State() = %v, want Disconnected for non-existent peer", lm.State("peer1"))
	}

	pc := lm.GetOrCreate("peer1", "10.0.0.1")
	_ = pc.Connected(context.Background(), &mockTunnel{}, "test", "test")

	if lm.State("peer1") != StateConnected {
		t.Errorf("State() = %v, want Connected", lm.State("peer1"))
	}
}

func TestLifecycleManager_AllInfo(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{})

	pc1 := lm.GetOrCreate("peer1", "10.0.0.1")
	_ = lm.GetOrCreate("peer2", "10.0.0.2") // stays disconnected

	_ = pc1.Connected(context.Background(), &mockTunnel{}, "test", "test")

	infos := lm.AllInfo()
	if len(infos) != 2 {
		t.Fatalf("AllInfo() should return 2 infos, got %d", len(infos))
	}

	// Find peer1's info
	var peer1Info *ConnectionInfo
	for i := range infos {
		if infos[i].PeerName == "peer1" {
			peer1Info = &infos[i]
			break
		}
	}
	if peer1Info == nil {
		t.Fatal("AllInfo() should include peer1")
	}
	if peer1Info.State != StateConnected {
		t.Errorf("peer1 state = %v, want Connected", peer1Info.State)
	}
}

func TestLifecycleManager_AddObserver(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{})

	recorder := &transitionRecorder{}
	lm.AddObserver(recorder)

	// New connections should have the observer
	pc := lm.GetOrCreate("peer1", "10.0.0.1")
	_ = pc.Connected(context.Background(), &mockTunnel{}, "test", "test")

	transitions := recorder.Transitions()
	if len(transitions) != 1 {
		t.Fatalf("Observer should receive 1 transition, got %d", len(transitions))
	}
	if transitions[0].To != StateConnected {
		t.Errorf("Transition.To = %v, want Connected", transitions[0].To)
	}
}

func TestLifecycleManager_ConcurrentAccess(t *testing.T) {
	tunnels := newMockTunnelProvider()
	lm := NewLifecycleManager(LifecycleConfig{
		Tunnels: tunnels,
	})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		n := i
		wg.Go(func() {
			peerName := "peer" + string(rune('a'+n%26))
			meshIP := "10.0.0." + string(rune('1'+n%9))

			pc := lm.GetOrCreate(peerName, meshIP)
			_ = pc.StartConnecting("test")
			_ = pc.Connected(context.Background(), &mockTunnel{}, "test", "test")
			lm.List()
			lm.Get(peerName)
			lm.IsConnected(peerName)
			lm.State(peerName)
			_ = pc.Disconnect(context.Background(), "test", nil)
		})
	}

	wg.Wait()

	// Should not panic and should have valid state
	list := lm.List()
	t.Logf("Final connection count: %d", len(list))
}
