package metrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/peer"
	"github.com/tunnelmesh/tunnelmesh/internal/peer/connection"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
)

// Mock implementations for testing

type mockForwarder struct {
	mu       sync.Mutex
	stats    routing.ForwarderStats
	exitNode string
}

func (m *mockForwarder) Stats() routing.ForwarderStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats
}

func (m *mockForwarder) SetStats(stats routing.ForwarderStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats = stats
}

func (m *mockForwarder) ExitPeer() string {
	return m.exitNode
}

type mockTunnelMgr struct {
	tunnels      []string
	healthyCount int
}

func (m *mockTunnelMgr) List() []string {
	return m.tunnels
}

func (m *mockTunnelMgr) CountHealthy() int {
	return m.healthyCount
}

type mockConnectionMgr struct {
	infos []connection.ConnectionInfo
}

func (m *mockConnectionMgr) AllInfo() []connection.ConnectionInfo {
	return m.infos
}

type mockRelay struct {
	connected bool
}

func (m *mockRelay) IsConnected() bool {
	return m.connected
}

type mockFilter struct {
	defaultDeny bool
	counts      routing.RuleCounts
}

func (m *mockFilter) IsDefaultDeny() bool {
	return m.defaultDeny
}

func (m *mockFilter) RuleCountBySource() routing.RuleCounts {
	return m.counts
}

func TestCollector_CollectForwarderStats(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	fwd := &mockForwarder{
		stats: routing.ForwarderStats{
			PacketsSent:     100,
			PacketsReceived: 80,
			BytesSent:       10000,
			BytesReceived:   8000,
			DroppedNoRoute:  5,
			DroppedNoTunnel: 3,
			DroppedNonIPv4:  2,
			Errors:          1,
		},
	}

	c := NewCollector(m, CollectorConfig{
		Forwarder: fwd,
	})

	// First collection
	c.Collect()

	// Gather and verify
	mfs, err := Registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	expected := map[string]float64{
		"tunnelmesh_packets_sent_total":      100,
		"tunnelmesh_packets_received_total":  80,
		"tunnelmesh_bytes_sent_total":        10000,
		"tunnelmesh_bytes_received_total":    8000,
		"tunnelmesh_dropped_no_route_total":  5,
		"tunnelmesh_dropped_no_tunnel_total": 3,
		"tunnelmesh_dropped_non_ipv4_total":  2,
		"tunnelmesh_forwarder_errors_total":  1,
	}

	for _, mf := range mfs {
		if exp, ok := expected[mf.GetName()]; ok {
			if len(mf.GetMetric()) == 0 {
				t.Errorf("No metrics for %s", mf.GetName())
				continue
			}
			val := mf.GetMetric()[0].GetCounter().GetValue()
			if val != exp {
				t.Errorf("%s: expected %f, got %f", mf.GetName(), exp, val)
			}
		}
	}

	// Second collection with increased stats
	fwd.stats.PacketsSent = 150
	fwd.stats.BytesSent = 15000
	c.Collect()

	mfs, _ = Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_packets_sent_total" {
			val := mf.GetMetric()[0].GetCounter().GetValue()
			if val != 150 { // 100 + 50 delta
				t.Errorf("Expected cumulative packets_sent=150, got %f", val)
			}
		}
	}
}

func TestCollector_CollectTunnelStats(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	tunnelMgr := &mockTunnelMgr{
		tunnels:      []string{"peer-a", "peer-b", "peer-c"},
		healthyCount: 2,
	}

	c := NewCollector(m, CollectorConfig{
		TunnelMgr: tunnelMgr,
	})

	c.Collect()

	mfs, _ := Registry.Gather()
	for _, mf := range mfs {
		switch mf.GetName() {
		case "tunnelmesh_active_tunnels":
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != 3 {
				t.Errorf("Expected active_tunnels=3, got %f", val)
			}
		case "tunnelmesh_healthy_tunnels":
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != 2 {
				t.Errorf("Expected healthy_tunnels=2, got %f", val)
			}
		}
	}
}

func TestCollector_CollectConnectionStats(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	connMgr := &mockConnectionMgr{
		infos: []connection.ConnectionInfo{
			{PeerName: "peer-a", State: connection.StateConnected, TransportType: "ssh"},
			{PeerName: "peer-b", State: connection.StateConnecting, TransportType: "udp"},
			{PeerName: "peer-c", State: connection.StateDisconnected, TransportType: ""},
		},
	}

	c := NewCollector(m, CollectorConfig{
		Connections: connMgr,
	})

	c.Collect()

	mfs, _ := Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_connection_state" {
			if len(mf.GetMetric()) != 3 {
				t.Errorf("Expected 3 connection states, got %d", len(mf.GetMetric()))
			}
			// Verify each connection state
			for _, metric := range mf.GetMetric() {
				labels := make(map[string]string)
				for _, l := range metric.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				val := metric.GetGauge().GetValue()
				switch labels["target_peer"] {
				case "peer-a":
					if val != float64(connection.StateConnected) {
						t.Errorf("peer-a: expected state=%d, got %f", connection.StateConnected, val)
					}
					if labels["transport"] != "ssh" {
						t.Errorf("peer-a: expected transport=ssh, got %s", labels["transport"])
					}
				case "peer-b":
					if val != float64(connection.StateConnecting) {
						t.Errorf("peer-b: expected state=%d, got %f", connection.StateConnecting, val)
					}
				case "peer-c":
					if val != float64(connection.StateDisconnected) {
						t.Errorf("peer-c: expected state=%d, got %f", connection.StateDisconnected, val)
					}
					if labels["transport"] != "none" {
						t.Errorf("peer-c: expected transport=none, got %s", labels["transport"])
					}
				}
			}
		}
	}
}

func TestCollector_CollectRelayStats(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	relay := &mockRelay{connected: true}

	c := NewCollector(m, CollectorConfig{
		Relay: relay,
	})

	c.Collect()

	mfs, _ := Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_relay_connected" {
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != 1 {
				t.Errorf("Expected relay_connected=1, got %f", val)
			}
		}
	}

	// Test disconnected
	relay.connected = false
	c.Collect()

	mfs, _ = Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_relay_connected" {
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != 0 {
				t.Errorf("Expected relay_connected=0, got %f", val)
			}
		}
	}
}

func TestCollector_CollectExitPeerStats(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	fwd := &mockForwarder{
		exitNode: "exit-node-1",
	}

	c := NewCollector(m, CollectorConfig{
		Forwarder:  fwd,
		AllowsExit: true,
	})

	c.Collect()

	mfs, _ := Registry.Gather()
	for _, mf := range mfs {
		switch mf.GetName() {
		case "tunnelmesh_exit_node_configured":
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != 1 {
				t.Errorf("Expected exit_node_configured=1, got %f", val)
			}
		case "tunnelmesh_allows_exit_traffic":
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != 1 {
				t.Errorf("Expected allows_exit_traffic=1, got %f", val)
			}
		case "tunnelmesh_exit_node_info":
			if len(mf.GetMetric()) == 0 {
				t.Error("Expected exit_node_info metric")
				continue
			}
			labels := make(map[string]string)
			for _, l := range mf.GetMetric()[0].GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["exit_node"] != "exit-node-1" {
				t.Errorf("Expected exit_node=exit-node-1, got %s", labels["exit_node"])
			}
		}
	}
}

func TestCollector_CollectGeolocationStats(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	identity := &peer.PeerIdentity{
		Name:   "test-peer",
		MeshIP: "10.42.0.1",
		Location: &proto.GeoLocation{
			Latitude:  37.7749,
			Longitude: -122.4194,
			City:      "San Francisco",
		},
	}

	c := NewCollector(m, CollectorConfig{
		Identity: identity,
	})

	c.Collect()

	mfs, _ := Registry.Gather()
	for _, mf := range mfs {
		switch mf.GetName() {
		case "tunnelmesh_peer_latitude":
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != 37.7749 {
				t.Errorf("Expected latitude=37.7749, got %f", val)
			}
		case "tunnelmesh_peer_longitude":
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != -122.4194 {
				t.Errorf("Expected longitude=-122.4194, got %f", val)
			}
		case "tunnelmesh_peer_location_info":
			if len(mf.GetMetric()) == 0 {
				t.Error("Expected peer_location_info metric")
				continue
			}
			labels := make(map[string]string)
			for _, l := range mf.GetMetric()[0].GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["city"] != "San Francisco" {
				t.Errorf("Expected city=San Francisco, got %s", labels["city"])
			}
		}
	}
}

func TestCollector_Run(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	fwd := &mockForwarder{
		stats: routing.ForwarderStats{PacketsSent: 10},
	}

	c := NewCollector(m, CollectorConfig{
		Forwarder: fwd,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Run collector in background
	done := make(chan struct{})
	go func() {
		c.Run(ctx, 50*time.Millisecond)
		close(done)
	}()

	// Wait for a few collection cycles
	time.Sleep(150 * time.Millisecond)

	// Increase stats
	fwd.SetStats(routing.ForwarderStats{PacketsSent: 30})

	// Wait for another cycle
	time.Sleep(100 * time.Millisecond)

	// Stop collector
	cancel()
	<-done

	// Verify metrics were collected
	mfs, _ := Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_packets_sent_total" {
			val := mf.GetMetric()[0].GetCounter().GetValue()
			if val < 10 {
				t.Errorf("Expected packets_sent >= 10, got %f", val)
			}
		}
	}
}

func TestCollector_ReconnectObserver(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	c := NewCollector(m, CollectorConfig{})

	observer := c.ReconnectObserver()

	// Simulate reconnect transitions
	observer.OnTransition(connection.Transition{
		PeerName: "peer-a",
		From:     connection.StateReconnecting,
		To:       connection.StateConnected,
	})
	observer.OnTransition(connection.Transition{
		PeerName: "peer-a",
		From:     connection.StateReconnecting,
		To:       connection.StateConnected,
	})
	observer.OnTransition(connection.Transition{
		PeerName: "peer-b",
		From:     connection.StateReconnecting,
		To:       connection.StateConnected,
	})

	// This shouldn't count (not a reconnect)
	observer.OnTransition(connection.Transition{
		PeerName: "peer-c",
		From:     connection.StateConnecting,
		To:       connection.StateConnected,
	})

	mfs, _ := Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_reconnects_total" {
			// Should have 2 entries: peer-a with 2 reconnects, peer-b with 1
			if len(mf.GetMetric()) != 2 {
				t.Errorf("Expected 2 reconnect metrics, got %d", len(mf.GetMetric()))
			}
			for _, metric := range mf.GetMetric() {
				labels := make(map[string]string)
				for _, l := range metric.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				val := metric.GetCounter().GetValue()
				switch labels["target_peer"] {
				case "peer-a":
					if val != 2 {
						t.Errorf("peer-a: expected 2 reconnects, got %f", val)
					}
				case "peer-b":
					if val != 1 {
						t.Errorf("peer-b: expected 1 reconnect, got %f", val)
					}
				}
			}
		}
	}
}

func TestCollector_DeltaCalculation(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	fwd := &mockForwarder{
		stats: routing.ForwarderStats{PacketsSent: 100},
	}

	c := NewCollector(m, CollectorConfig{
		Forwarder: fwd,
	})

	// First collection
	c.Collect()

	// Verify initial value
	mfs, _ := Registry.Gather()
	var initialVal float64
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_packets_sent_total" {
			initialVal = mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	if initialVal != 100 {
		t.Errorf("Expected initial value 100, got %f", initialVal)
	}

	// Simulate counter reset (e.g., peer restart)
	fwd.stats.PacketsSent = 50 // Lower than before

	c.Collect()

	// Value should not decrease (counter reset protection)
	mfs, _ = Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_packets_sent_total" {
			val := mf.GetMetric()[0].GetCounter().GetValue()
			// Our implementation only adds positive deltas, so value stays at 100
			if val < 100 {
				t.Errorf("Counter should not decrease, got %f", val)
			}
		}
	}
}

func TestCollector_NilComponents(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	// Create collector with all nil components
	c := NewCollector(m, CollectorConfig{})

	// Should not panic
	c.Collect()

	// Verify default values
	mfs, _ := Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_relay_connected" {
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != 0 {
				t.Errorf("Expected relay_connected=0 with nil relay, got %f", val)
			}
		}
	}
}

// mockRTTProvider for testing latency collection.
type mockRTTProvider struct {
	rtt time.Duration
}

func (m *mockRTTProvider) GetLastRTT() time.Duration {
	return m.rtt
}

func TestCollector_LatencyStats(t *testing.T) {
	// Create fresh registry to avoid conflicts
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	rttProvider := &mockRTTProvider{rtt: 42 * time.Millisecond}

	c := NewCollector(m, CollectorConfig{
		RTTProvider: rttProvider,
	})

	c.Collect()

	// Verify latency metric
	mfs, err := Registry.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_peer_coordinator_rtt_seconds" {
			found = true
			val := mf.GetMetric()[0].GetGauge().GetValue()
			assert.Equal(t, float64(0.042), val, "Expected peer_coordinator_rtt_seconds=0.042")
		}
	}
	assert.True(t, found, "Expected to find tunnelmesh_peer_coordinator_rtt_seconds metric")
}

func TestCollector_LatencyStats_ZeroRTT(t *testing.T) {
	// Create fresh registry to avoid conflicts
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	// Zero RTT (no measurement yet)
	rttProvider := &mockRTTProvider{rtt: 0}

	c := NewCollector(m, CollectorConfig{
		RTTProvider: rttProvider,
	})

	c.Collect()

	// Verify metric is not set when RTT is zero
	mfs, err := Registry.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_peer_coordinator_rtt_ms" {
			val := mf.GetMetric()[0].GetGauge().GetValue()
			// Gauge defaults to 0, so this just confirms we don't set it when RTT is 0
			assert.Equal(t, float64(0), val, "Expected peer_coordinator_rtt_ms=0 when no RTT measured")
		}
	}
}

func TestCollector_CollectFilterStats(t *testing.T) {
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	filter := &mockFilter{
		defaultDeny: true,
		counts: routing.RuleCounts{
			Coordinator: 3,
			PeerConfig:  2,
			Temporary:   1,
		},
	}

	c := NewCollector(m, CollectorConfig{
		Filter: filter,
	})

	c.Collect()

	mfs, err := Registry.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		switch mf.GetName() {
		case "tunnelmesh_filter_default_deny":
			val := mf.GetMetric()[0].GetGauge().GetValue()
			assert.Equal(t, float64(1), val, "Expected filter_default_deny=1")
		case "tunnelmesh_filter_rules_total":
			// Check each source label
			for _, metric := range mf.GetMetric() {
				labels := make(map[string]string)
				for _, l := range metric.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				val := metric.GetGauge().GetValue()
				switch labels["source"] {
				case "coordinator":
					assert.Equal(t, float64(3), val, "Expected coordinator rules=3")
				case "config":
					assert.Equal(t, float64(2), val, "Expected config rules=2")
				case "temporary":
					assert.Equal(t, float64(1), val, "Expected temporary rules=1")
				}
			}
		}
	}
}

func TestCollector_CollectFilterStats_NilFilter(t *testing.T) {
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	// No filter configured
	c := NewCollector(m, CollectorConfig{})

	c.Collect()

	mfs, err := Registry.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		switch mf.GetName() {
		case "tunnelmesh_filter_default_deny":
			val := mf.GetMetric()[0].GetGauge().GetValue()
			assert.Equal(t, float64(0), val, "Expected filter_default_deny=0 when no filter")
		case "tunnelmesh_filter_rules_total":
			// All sources should be 0
			for _, metric := range mf.GetMetric() {
				val := metric.GetGauge().GetValue()
				assert.Equal(t, float64(0), val, "Expected rules=0 when no filter")
			}
		}
	}
}

func TestCollector_CollectConnectionStats_StaleLabelsCleared(t *testing.T) {
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	connMgr := &mockConnectionMgr{
		infos: []connection.ConnectionInfo{
			{PeerName: "peer-a", State: connection.StateConnecting, TransportType: ""},
		},
	}

	c := NewCollector(m, CollectorConfig{
		Connections: connMgr,
	})

	// First collect: peer-a is Connecting with transport="none".
	c.Collect()

	mfs, err := Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_connection_state" {
			require.Len(t, mf.GetMetric(), 1, "expected exactly 1 series after first collect")
			labels := make(map[string]string)
			for _, l := range mf.GetMetric()[0].GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			assert.Equal(t, "none", labels["transport"])
		}
	}

	// Transition: peer-a is now Connected with transport="udp".
	connMgr.infos = []connection.ConnectionInfo{
		{PeerName: "peer-a", State: connection.StateConnected, TransportType: "udp"},
	}

	c.Collect()

	mfs, err = Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_connection_state" {
			// After Reset(), only the new series must exist — no stale transport="none".
			require.Len(t, mf.GetMetric(), 1, "stale transport label series must be cleared on re-collect")
			labels := make(map[string]string)
			for _, l := range mf.GetMetric()[0].GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			assert.Equal(t, "udp", labels["transport"], "expected transport=udp after transition")
			assert.Equal(t, float64(connection.StateConnected), mf.GetMetric()[0].GetGauge().GetValue())
		}
	}
}

func TestCollector_CollectFilteredDrops(t *testing.T) {
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	fwd := &mockForwarder{
		stats: routing.ForwarderStats{
			DroppedFilteredTCP: 100,
			DroppedFilteredUDP: 50,
		},
	}

	c := NewCollector(m, CollectorConfig{
		Forwarder: fwd,
	})

	c.Collect()

	mfs, err := Registry.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_dropped_filtered_total" {
			for _, metric := range mf.GetMetric() {
				labels := make(map[string]string)
				for _, l := range metric.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				val := metric.GetCounter().GetValue()
				switch labels["protocol"] {
				case "tcp":
					assert.Equal(t, float64(100), val, "Expected TCP filtered drops=100")
				case "udp":
					assert.Equal(t, float64(50), val, "Expected UDP filtered drops=50")
				}
			}
		}
	}
}

func TestCollector_CleanupPeer(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")
	c := NewCollector(m, CollectorConfig{})

	// Populate per-peer series
	c.TrackReconnect("peer-a")
	c.TrackReconnect("peer-b")
	c.metrics.PeerLatencySeconds.WithLabelValues("peer-a").Set(0.01)
	c.metrics.PeerLatencySeconds.WithLabelValues("peer-b").Set(0.02)
	c.TrackFilterDrop(6, "peer-a")
	c.TrackFilterDrop(17, "peer-a")
	c.TrackFilterDrop(6, "peer-b")

	// Verify series exist before cleanup
	mfs, _ := Registry.Gather()
	reconnectsBefore := 0
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_reconnects_total" {
			reconnectsBefore = len(mf.GetMetric())
		}
	}
	assert.Equal(t, 2, reconnectsBefore, "Expected 2 reconnect series before cleanup")

	// Clean up peer-a
	c.CleanupPeer("peer-a")

	mfs, _ = Registry.Gather()
	for _, mf := range mfs {
		switch mf.GetName() {
		case "tunnelmesh_reconnects_total":
			assert.Equal(t, 1, len(mf.GetMetric()), "Expected 1 reconnect series after cleanup")
			labels := make(map[string]string)
			for _, l := range mf.GetMetric()[0].GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			assert.Equal(t, "peer-b", labels["target_peer"])
		case "tunnelmesh_peer_udp_latency_seconds":
			assert.Equal(t, 1, len(mf.GetMetric()), "Expected 1 latency series after cleanup")
			labels := make(map[string]string)
			for _, l := range mf.GetMetric()[0].GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			assert.Equal(t, "peer-b", labels["target_peer"])
		case "tunnelmesh_dropped_filtered_total":
			for _, metric := range mf.GetMetric() {
				labels := make(map[string]string)
				for _, l := range metric.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				assert.NotEqual(t, "peer-a", labels["source_peer"], "peer-a series should be deleted")
			}
		}
	}
}

func TestCollector_CleanupObserver(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")
	c := NewCollector(m, CollectorConfig{})

	// Populate reconnect series
	c.TrackReconnect("peer-x")
	c.TrackReconnect("peer-y")

	observer := c.CleanupObserver()

	// Non-terminal transition should not clean up
	observer.OnTransition(connection.Transition{
		PeerName: "peer-x",
		From:     connection.StateConnected,
		To:       connection.StateReconnecting,
	})

	mfs, _ := Registry.Gather()
	reconnectsAfterNonTerminal := 0
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_reconnects_total" {
			reconnectsAfterNonTerminal = len(mf.GetMetric())
		}
	}
	assert.Equal(t, 2, reconnectsAfterNonTerminal, "Non-terminal transition should not remove series")

	// StateClosed transition should clean up
	observer.OnTransition(connection.Transition{
		PeerName: "peer-x",
		From:     connection.StateReconnecting,
		To:       connection.StateClosed,
	})

	mfs, _ = Registry.Gather()
	reconnectsAfterClose := 0
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_reconnects_total" {
			reconnectsAfterClose = len(mf.GetMetric())
		}
	}
	assert.Equal(t, 1, reconnectsAfterClose, "StateClosed should remove peer series")
}
