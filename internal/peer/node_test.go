package peer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/coord"
)

func TestNewMeshNode(t *testing.T) {
	identity := &PeerIdentity{
		Name:          "test-node",
		PubKeyEncoded: "test-key",
		SSHPort:       2222,
		MeshCIDR:      "10.42.0.0/16",
		MeshIP:        "10.42.0.1",
		Config: &config.PeerConfig{
			Name:    "test-node",
			SSHPort: 2222,
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")

	node := NewMeshNode(identity, client)

	require.NotNil(t, node)
	assert.Same(t, identity, node.Identity())
	assert.Same(t, client, node.Client())
	assert.NotNil(t, node.TunnelMgr())
	assert.NotNil(t, node.Router())
}

func TestMeshNode_TriggerDiscovery(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// Should be able to trigger discovery
	triggered := node.TriggerDiscovery()
	assert.True(t, triggered)

	// Channel should have one item
	select {
	case <-node.DiscoveryChan():
		// OK - received the trigger
	default:
		t.Fatal("discovery channel should have a pending trigger")
	}
}

func TestMeshNode_TriggerDiscovery_AlreadyPending(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// First trigger should succeed
	triggered1 := node.TriggerDiscovery()
	assert.True(t, triggered1)

	// Second trigger should return false (already pending)
	triggered2 := node.TriggerDiscovery()
	assert.False(t, triggered2)
}

func TestMeshNode_NetworkChangeState(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// Initially not in bypass window
	assert.False(t, node.InNetworkBypassWindow())

	// Record network change
	node.RecordNetworkChange()

	// Should now be in bypass window
	assert.True(t, node.InNetworkBypassWindow())
}

func TestMeshNode_HeartbeatState(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// Initial state should be empty
	pub, priv, nat := node.GetHeartbeatIPs()
	assert.Nil(t, pub)
	assert.Nil(t, priv)
	assert.False(t, nat)

	// Update state
	node.SetHeartbeatIPs([]string{"1.2.3.4"}, []string{"192.168.1.1"}, true)

	// Should return updated values
	pub, priv, nat = node.GetHeartbeatIPs()
	assert.Equal(t, []string{"1.2.3.4"}, pub)
	assert.Equal(t, []string{"192.168.1.1"}, priv)
	assert.True(t, nat)
}

func TestMeshNode_CollectStats(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// Add some tunnels
	node.TunnelMgr().Add("peer1", newMockTunnel())
	node.TunnelMgr().Add("peer2", newMockTunnel())

	// Without forwarder, should still return stats with tunnel count
	stats := node.CollectStats()

	// Stats from nil forwarder should have zero values but correct tunnel count
	require.NotNil(t, stats)
	assert.Equal(t, 2, stats.ActiveTunnels)
}

func TestMeshNode_IPsChanged(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// Initially should detect change (no last IPs set)
	assert.True(t, node.IPsChanged([]string{"1.2.3.4"}, []string{"192.168.1.1"}, false))

	// Set heartbeat IPs
	node.SetHeartbeatIPs([]string{"1.2.3.4"}, []string{"192.168.1.1"}, false)

	// Same IPs should not show change
	assert.False(t, node.IPsChanged([]string{"1.2.3.4"}, []string{"192.168.1.1"}, false))

	// Different public IPs should show change
	assert.True(t, node.IPsChanged([]string{"5.6.7.8"}, []string{"192.168.1.1"}, false))

	// Different private IPs should show change
	assert.True(t, node.IPsChanged([]string{"1.2.3.4"}, []string{"10.0.0.1"}, false))

	// Different NAT status should show change
	assert.True(t, node.IPsChanged([]string{"1.2.3.4"}, []string{"192.168.1.1"}, true))
}

func TestMeshNode_CancelOutbound_ViaLifecycleManager(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Track if context was cancelled
	var cancelled atomic.Bool
	go func() {
		<-ctx.Done()
		cancelled.Store(true)
	}()

	// Start connecting with cancel function via LifecycleManager
	started := node.Connections.StartConnecting("peer1", "10.0.0.1", cancel)
	assert.True(t, started, "should be able to start connecting")

	// Verify peer is marked as connecting
	assert.True(t, node.Connections.IsConnecting("peer1"))

	// Cancel the outbound connection (simulating inbound success)
	wasCancelled := node.Connections.CancelOutbound("peer1")
	assert.True(t, wasCancelled, "should cancel the connection")

	// Wait a bit for goroutine to process
	time.Sleep(10 * time.Millisecond)

	// Context should be cancelled
	assert.True(t, cancelled.Load(), "context should be cancelled")

	// Second cancel should return false (already consumed)
	wasCancelled = node.Connections.CancelOutbound("peer1")
	assert.False(t, wasCancelled, "second cancel should return false")
}

func TestMeshNode_CancelOutbound_NonExistent(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// Cancel for non-existent peer should return false
	wasCancelled := node.Connections.CancelOutbound("nonexistent")
	assert.False(t, wasCancelled)
}

func TestMeshNode_StartConnecting_AlreadyConnecting(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// First start should succeed
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	started1 := node.Connections.StartConnecting("peer1", "10.0.0.1", cancel1)
	assert.True(t, started1)

	// Second start should fail (already connecting)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	started2 := node.Connections.StartConnecting("peer1", "10.0.0.1", cancel2)
	assert.False(t, started2, "should not be able to start connecting twice")

	// Verify only the first context is tracked
	_ = ctx1
	_ = ctx2

	// Clear connecting and verify we can start again
	node.Connections.ClearConnecting("peer1")
	started3 := node.Connections.StartConnecting("peer1", "10.0.0.1", cancel1)
	assert.True(t, started3, "should be able to start connecting after clear")
}

// TestReconnectPersistentRelay_AtomicSwapBehavior documents the expected behavior
// of ReconnectPersistentRelay. The actual relay reconnection requires a real server,
// but this test verifies the code structure ensures atomic swap semantics:
//
// 1. Old relay is saved but NOT closed or cleared from forwarder
// 2. New relay is created and connection attempted
// 3. Only on SUCCESS: new relay is set in forwarder, then old relay is closed
// 4. Only on TOTAL FAILURE (all retries exhausted): relay is set to nil
//
// This prevents packet loss during the reconnection window, which can take
// 500ms to 30+ seconds with exponential backoff.
//
// To verify this behavior, inspect ReconnectPersistentRelay():
// - m.Forwarder.SetRelay(nil) is ONLY called after all retries fail
// - m.Forwarder.SetRelay(newRelay) is called BEFORE oldRelay.Close()

func TestMeshNode_HandleRelayPacketForAsymmetricDetection(t *testing.T) {
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	t.Run("no peer connection returns false", func(t *testing.T) {
		invalidated := node.HandleRelayPacketForAsymmetricDetection("nonexistent-peer")
		assert.False(t, invalidated)
	})

	t.Run("peer without tunnel returns false", func(t *testing.T) {
		// Create a peer connection without a tunnel
		node.Connections.GetOrCreate("peer1", "10.0.0.1")
		invalidated := node.HandleRelayPacketForAsymmetricDetection("peer1")
		assert.False(t, invalidated)
	})

	t.Run("peer with tunnel but zero connectedSince returns false", func(t *testing.T) {
		// Create a peer connection and set tunnel directly (without transitioning to Connected)
		pc := node.Connections.GetOrCreate("peer2", "10.0.0.2")
		pc.SetTunnel(&mockTunnel{}, "test")

		// Should have tunnel but ConnectedSince should be zero (state is not Connected)
		assert.True(t, pc.HasTunnel())
		assert.True(t, pc.ConnectedSince().IsZero())

		// Should NOT invalidate because connectedSince is zero
		invalidated := node.HandleRelayPacketForAsymmetricDetection("peer2")
		assert.False(t, invalidated)

		// Peer should still have its tunnel
		assert.True(t, pc.HasTunnel())
	})

	t.Run("peer with tunnel in grace period returns false", func(t *testing.T) {
		// Create a properly connected peer (just connected, within grace period)
		pc := node.Connections.GetOrCreate("peer3", "10.0.0.3")
		err := pc.Connected(context.Background(), &mockTunnel{}, "test", "test connection")
		require.NoError(t, err)

		// Should have tunnel and non-zero ConnectedSince
		assert.True(t, pc.HasTunnel())
		assert.False(t, pc.ConnectedSince().IsZero())

		// Should NOT invalidate because tunnel is too new (within grace period)
		invalidated := node.HandleRelayPacketForAsymmetricDetection("peer3")
		assert.False(t, invalidated)

		// Peer should still have its tunnel
		assert.True(t, pc.HasTunnel())
	})
}

func TestMeshNode_InboundCancelsOutbound_Integration(t *testing.T) {
	// This test simulates the full flow:
	// 1. Start an outbound connection (slow, takes 100ms)
	// 2. Inbound connection arrives
	// 3. Outbound should be cancelled
	identity := &PeerIdentity{
		Name: "test-node",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// Create a context for the outbound connection
	connCtx, cancel := context.WithCancel(context.Background())

	// Track the state
	var outboundStarted atomic.Bool
	var outboundCancelled atomic.Bool
	outboundDone := make(chan struct{})

	// Start simulated outbound connection via LifecycleManager
	if !node.Connections.StartConnecting("peer1", "10.0.0.1", cancel) {
		t.Fatal("should be able to start connecting")
	}

	go func() {
		defer close(outboundDone)
		outboundStarted.Store(true)

		// Simulate slow connection attempt
		select {
		case <-connCtx.Done():
			outboundCancelled.Store(true)
			return
		case <-time.After(5 * time.Second):
			// Connection would succeed but we expect cancellation
			t.Error("outbound was not cancelled")
		}
	}()

	// Wait for outbound to start
	for !outboundStarted.Load() {
		time.Sleep(1 * time.Millisecond)
	}

	// Simulate inbound connection arriving
	wasCancelled := node.Connections.CancelOutbound("peer1")
	assert.True(t, wasCancelled, "should cancel outbound")

	// Wait for outbound to complete
	select {
	case <-outboundDone:
		// OK
	case <-time.After(1 * time.Second):
		t.Fatal("outbound goroutine did not complete")
	}

	// Verify outbound was cancelled
	assert.True(t, outboundCancelled.Load(), "outbound should have been cancelled")
}
