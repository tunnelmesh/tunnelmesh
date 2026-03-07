package peer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/coord"
)

func TestMeshNode_RunHeartbeat_FastPhase(t *testing.T) {
	// This test verifies that the fast phase runs with 5s interval
	// We can't easily test timing, but we can verify the loop exits on context cancel
	identity := &PeerIdentity{
		Name:     "test-node",
		MeshCIDR: "10.42.0.0/16",
		Config: &config.PeerConfig{
			Name: "test-node",
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	ctx, cancel := context.WithCancel(context.Background())

	// Should exit when context is cancelled without panicking
	done := make(chan struct{})
	go func() {
		node.RunHeartbeat(ctx)
		close(done)
	}()

	// Let the heartbeat start, then cancel
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// OK - exited properly
	case <-time.After(30 * time.Second):
		t.Fatal("heartbeat loop did not exit on context cancel")
	}
}

func TestMeshNode_PerformHeartbeat_FirstRun(t *testing.T) {
	identity := &PeerIdentity{
		Name:          "test-node",
		PubKeyEncoded: "test-key",
		SSHPort:       2222,
		MeshCIDR:      "10.42.0.0/16",
		Config: &config.PeerConfig{
			Name:    "test-node",
			SSHPort: 2222,
		},
	}
	// This will fail to connect but we're testing the logic, not the connection
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	ctx := context.Background()

	// First heartbeat should record IPs (even if server call fails)
	node.PerformHeartbeat(ctx)

	// IPs should now be set (though heartbeat likely failed)
	pub, priv, _ := node.GetHeartbeatIPs()
	// We should have at least recorded some IPs
	assert.NotNil(t, pub, "should have recorded public IPs or empty slice")
	_ = priv // Private IPs depend on local network
}

func TestMeshNode_HandleIPChange(t *testing.T) {
	identity := &PeerIdentity{
		Name:          "test-node",
		PubKeyEncoded: "test-key",
		SSHPort:       2222,
		MeshCIDR:      "10.42.0.0/16",
		Config: &config.PeerConfig{
			Name:    "test-node",
			SSHPort: 2222,
		},
	}
	client := coord.NewClient("http://localhost:8080", "test-token")
	node := NewMeshNode(identity, client)

	// Add a tunnel
	node.TunnelMgr().Add("peer1", newMockTunnel())

	// Set initial IPs
	node.SetHeartbeatIPs([]string{"1.2.3.4"}, []string{"192.168.1.1"}, false)

	// Handle IP change - should close tunnels and trigger discovery
	newPublic := []string{"5.6.7.8"}
	newPrivate := []string{"10.0.0.1"}
	node.HandleIPChange(context.Background(), newPublic, newPrivate, true)

	// Tunnels should be closed
	assert.Empty(t, node.TunnelMgr().List())

	// IPs should be updated
	pub, priv, nat := node.GetHeartbeatIPs()
	assert.Equal(t, newPublic, pub)
	assert.Equal(t, newPrivate, priv)
	assert.True(t, nat)
}
