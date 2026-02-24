package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func TestInitMetrics(t *testing.T) {
	// Create a fresh registry for this test
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	// Re-register standard collectors
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")
	if m == nil {
		t.Fatal("InitMetrics returned nil")
	}

	// Verify all metrics are initialized
	tests := []struct {
		name   string
		metric interface{}
	}{
		{"PacketsSent", m.PacketsSent},
		{"PacketsReceived", m.PacketsReceived},
		{"BytesSent", m.BytesSent},
		{"BytesReceived", m.BytesReceived},
		{"DroppedNoRoute", m.DroppedNoRoute},
		{"DroppedNoTunnel", m.DroppedNoTunnel},
		{"DroppedNonIPv4", m.DroppedNonIPv4},
		{"ForwarderErrors", m.ForwarderErrors},
		{"ExitPacketsSent", m.ExitPacketsSent},
		{"ExitBytesSent", m.ExitBytesSent},
		{"ConnectionState", m.ConnectionState},
		{"ActiveTunnels", m.ActiveTunnels},
		{"HealthyTunnels", m.HealthyTunnels},
		{"ReconnectCount", m.ReconnectCount},
		{"RelayConnected", m.RelayConnected},
		{"ExitPeerConfigured", m.ExitPeerConfigured},
		{"ExitPeerInfo", m.ExitPeerInfo},
		{"AllowsExitTraffic", m.AllowsExitTraffic},
		{"PeerLatitude", m.PeerLatitude},
		{"PeerLongitude", m.PeerLongitude},
		{"PeerLocationInfo", m.PeerLocationInfo},
		{"PeerInfo", m.PeerInfo},
	}

	for _, tt := range tests {
		if tt.metric == nil {
			t.Errorf("%s is nil", tt.name)
		}
	}
}

func TestMetricsCounterIncrement(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	// Test counter increments
	m.PacketsSent.Add(100)
	m.BytesSent.Add(1024)

	// Gather metrics and verify
	mfs, err := Registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Find and verify PacketsSent
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_packets_sent_total" {
			found = true
			if len(mf.GetMetric()) == 0 {
				t.Error("No metrics found for tunnelmesh_packets_sent_total")
				continue
			}
			val := mf.GetMetric()[0].GetCounter().GetValue()
			if val != 100 {
				t.Errorf("Expected PacketsSent=100, got %f", val)
			}
		}
	}
	if !found {
		t.Error("tunnelmesh_packets_sent_total not found in gathered metrics")
	}
}

func TestMetricsGaugeSet(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	// Test gauge sets
	m.ActiveTunnels.Set(5)
	m.HealthyTunnels.Set(4)
	m.RelayConnected.Set(1)

	mfs, err := Registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Verify ActiveTunnels
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_active_tunnels" {
			if len(mf.GetMetric()) == 0 {
				t.Error("No metrics found for tunnelmesh_active_tunnels")
				continue
			}
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if val != 5 {
				t.Errorf("Expected ActiveTunnels=5, got %f", val)
			}
		}
	}
}

func TestMetricsLabels(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := InitMetrics("test-peer", "10.42.0.1", "1.0.0")

	// Test labeled metrics
	m.ConnectionState.WithLabelValues("peer-a", "ssh").Set(2)
	m.ConnectionState.WithLabelValues("peer-b", "udp").Set(2)
	m.ReconnectCount.WithLabelValues("peer-a").Inc()

	mfs, err := Registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Verify ConnectionState has correct labels
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_connection_state" {
			if len(mf.GetMetric()) != 2 {
				t.Errorf("Expected 2 connection_state metrics, got %d", len(mf.GetMetric()))
			}
			for _, m := range mf.GetMetric() {
				labels := m.GetLabel()
				hasTargetPeer := false
				hasTransport := false
				hasPeer := false
				for _, l := range labels {
					switch l.GetName() {
					case "target_peer":
						hasTargetPeer = true
					case "transport":
						hasTransport = true
					case "peer":
						hasPeer = true
						if l.GetValue() != "test-peer" {
							t.Errorf("Expected peer=test-peer, got %s", l.GetValue())
						}
					}
				}
				if !hasTargetPeer || !hasTransport || !hasPeer {
					t.Error("Missing expected labels on connection_state metric")
				}
			}
		}
	}
}

func TestPeerInfoMetric(t *testing.T) {
	oldRegistry := Registry
	Registry = prometheus.NewRegistry()
	defer func() { Registry = oldRegistry }()

	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	_ = InitMetrics("my-node", "10.42.1.5", "2.0.0")

	mfs, err := Registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Verify PeerInfo
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "tunnelmesh_peer_info" {
			found = true
			if len(mf.GetMetric()) != 1 {
				t.Errorf("Expected 1 peer_info metric, got %d", len(mf.GetMetric()))
				continue
			}
			m := mf.GetMetric()[0]
			if m.GetGauge().GetValue() != 1 {
				t.Errorf("Expected peer_info value=1, got %f", m.GetGauge().GetValue())
			}
			labels := make(map[string]string)
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["peer"] != "my-node" {
				t.Errorf("Expected peer=my-node, got %s", labels["peer"])
			}
			if labels["mesh_ip"] != "10.42.1.5" {
				t.Errorf("Expected mesh_ip=10.42.1.5, got %s", labels["mesh_ip"])
			}
			if labels["version"] != "2.0.0" {
				t.Errorf("Expected version=2.0.0, got %s", labels["version"])
			}
		}
	}
	if !found {
		t.Error("tunnelmesh_peer_info not found")
	}
}
