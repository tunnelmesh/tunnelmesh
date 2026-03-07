// Package metrics provides Prometheus metrics for tunnelmesh peers.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Registry is the Prometheus registry for all tunnelmesh metrics.
var Registry = prometheus.NewRegistry()

// PeerMetrics holds all Prometheus metrics for a tunnelmesh peer.
type PeerMetrics struct {
	// Forwarder stats (counters)
	PacketsSent     prometheus.Counter
	PacketsReceived prometheus.Counter
	BytesSent       prometheus.Counter
	BytesReceived   prometheus.Counter
	DroppedNoRoute  prometheus.Counter
	DroppedNoTunnel prometheus.Counter
	DroppedNonIPv4  prometheus.Counter
	ForwarderErrors prometheus.Counter

	// UDP transport stats
	DroppedQueueFull  prometheus.Counter     // Packets dropped due to full packet-processing queue
	HandshakeFailures *prometheus.CounterVec // udp_handshake_failures_total{reason}
	SessionRejected   prometheus.Counter     // Duplicate sessions discarded

	// Packet filter stats (counters, labeled by protocol and source peer)
	DroppedFiltered *prometheus.CounterVec // labels: protocol (tcp, udp), source_peer

	// Packet filter gauges
	FilterRulesTotal  *prometheus.GaugeVec // labels: source (coordinator, config, temporary)
	FilterDefaultDeny prometheus.Gauge     // 1 if default-deny mode, 0 if default-allow

	// Exit node stats (counters)
	ExitPacketsSent prometheus.Counter
	ExitBytesSent   prometheus.Counter

	// Connection gauges (labeled by peer and transport)
	ConnectionState *prometheus.GaugeVec // state per peer
	ActiveTunnels   prometheus.Gauge
	HealthyTunnels  prometheus.Gauge
	ReconnectCount  *prometheus.CounterVec // per target peer

	// Relay metrics
	RelayConnected prometheus.Gauge // 1 if connected, 0 if not

	// Exit node info
	ExitPeerConfigured prometheus.Gauge     // 1 if using exit node
	ExitPeerInfo       *prometheus.GaugeVec // labels: exit_node
	AllowsExitTraffic  prometheus.Gauge     // 1 if this node is an exit node

	// Geolocation metrics
	PeerLatitude     prometheus.Gauge
	PeerLongitude    prometheus.Gauge
	PeerLocationInfo *prometheus.GaugeVec // labels: city

	// Peer info (constant labels exposed as a gauge)
	PeerInfo *prometheus.GaugeVec // labels: mesh_ip, version

	// Latency metrics
	CoordinatorRTTSeconds prometheus.Gauge     // RTT to coordinator in seconds
	PeerLatencySeconds    *prometheus.GaugeVec // Latency to other peers (seconds), labels: target_peer

	// Connection setup duration histogram with exemplar support for Prometheus → Tempo linking.
	// Labels: transport (udp, ssh, relay), result (success, failure)
	ConnectionSetupDuration *prometheus.HistogramVec // tunnelmesh_connection_setup_duration_seconds
}

func init() {
	// Register standard Go metrics
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

// InitMetrics initializes all metrics with the given peer name as a constant label.
func InitMetrics(peerName, meshIP, version string) *PeerMetrics {
	constLabels := prometheus.Labels{
		"peer": peerName,
	}

	m := &PeerMetrics{
		// Forwarder counters
		PacketsSent: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_packets_sent_total",
			Help:        "Total packets sent through the forwarder",
			ConstLabels: constLabels,
		}),
		PacketsReceived: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_packets_received_total",
			Help:        "Total packets received through the forwarder",
			ConstLabels: constLabels,
		}),
		BytesSent: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_bytes_sent_total",
			Help:        "Total bytes sent through the forwarder",
			ConstLabels: constLabels,
		}),
		BytesReceived: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_bytes_received_total",
			Help:        "Total bytes received through the forwarder",
			ConstLabels: constLabels,
		}),
		DroppedNoRoute: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_dropped_no_route_total",
			Help:        "Packets dropped due to no route",
			ConstLabels: constLabels,
		}),
		DroppedNoTunnel: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_dropped_no_tunnel_total",
			Help:        "Packets dropped due to no tunnel",
			ConstLabels: constLabels,
		}),
		DroppedNonIPv4: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_dropped_non_ipv4_total",
			Help:        "Packets dropped due to non-IPv4",
			ConstLabels: constLabels,
		}),
		ForwarderErrors: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_forwarder_errors_total",
			Help:        "Total forwarder errors",
			ConstLabels: constLabels,
		}),

		// UDP transport stats
		DroppedQueueFull: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_dropped_queue_full_total",
			Help:        "Packets dropped because the UDP packet processing queue was full",
			ConstLabels: constLabels,
		}),
		HandshakeFailures: promauto.With(Registry).NewCounterVec(prometheus.CounterOpts{
			Name:        "tunnelmesh_udp_handshake_failures_total",
			Help:        "UDP handshake failures by reason",
			ConstLabels: constLabels,
		}, []string{"reason"}),
		SessionRejected: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_udp_session_rejected_total",
			Help:        "New sessions discarded because an active session already exists",
			ConstLabels: constLabels,
		}),

		// Packet filter counters (labeled by protocol and source peer)
		DroppedFiltered: promauto.With(Registry).NewCounterVec(prometheus.CounterOpts{
			Name:        "tunnelmesh_dropped_filtered_total",
			Help:        "Packets dropped by packet filter",
			ConstLabels: constLabels,
		}, []string{"protocol", "source_peer"}),

		// Packet filter gauges
		FilterRulesTotal: promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
			Name:        "tunnelmesh_filter_rules_total",
			Help:        "Number of active filter rules",
			ConstLabels: constLabels,
		}, []string{"source"}),
		FilterDefaultDeny: promauto.With(Registry).NewGauge(prometheus.GaugeOpts{
			Name:        "tunnelmesh_filter_default_deny",
			Help:        "Whether the filter is in default-deny mode (1) or default-allow (0)",
			ConstLabels: constLabels,
		}),

		// Exit node counters
		ExitPacketsSent: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_exit_packets_sent_total",
			Help:        "Total packets sent through exit node",
			ConstLabels: constLabels,
		}),
		ExitBytesSent: promauto.With(Registry).NewCounter(prometheus.CounterOpts{
			Name:        "tunnelmesh_exit_bytes_sent_total",
			Help:        "Total bytes sent through exit node",
			ConstLabels: constLabels,
		}),

		// Connection gauges
		ConnectionState: promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
			Name:        "tunnelmesh_connection_state",
			Help:        "Connection state per peer (0=disconnected, 1=connecting, 2=connected, 3=reconnecting, 4=closed)",
			ConstLabels: constLabels,
		}, []string{"target_peer", "transport"}),

		ActiveTunnels: promauto.With(Registry).NewGauge(prometheus.GaugeOpts{
			Name:        "tunnelmesh_active_tunnels",
			Help:        "Number of active tunnels",
			ConstLabels: constLabels,
		}),
		HealthyTunnels: promauto.With(Registry).NewGauge(prometheus.GaugeOpts{
			Name:        "tunnelmesh_healthy_tunnels",
			Help:        "Number of healthy tunnels",
			ConstLabels: constLabels,
		}),
		ReconnectCount: promauto.With(Registry).NewCounterVec(prometheus.CounterOpts{
			Name:        "tunnelmesh_reconnects_total",
			Help:        "Total reconnection attempts per peer",
			ConstLabels: constLabels,
		}, []string{"target_peer"}),

		// Relay metrics
		RelayConnected: promauto.With(Registry).NewGauge(prometheus.GaugeOpts{
			Name:        "tunnelmesh_relay_connected",
			Help:        "Whether the persistent relay is connected (1) or not (0)",
			ConstLabels: constLabels,
		}),

		// Exit node info
		ExitPeerConfigured: promauto.With(Registry).NewGauge(prometheus.GaugeOpts{
			Name:        "tunnelmesh_exit_node_configured",
			Help:        "Whether an exit node is configured (1) or not (0)",
			ConstLabels: constLabels,
		}),
		ExitPeerInfo: promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
			Name:        "tunnelmesh_exit_node_info",
			Help:        "Exit node information (value is always 1 when exit node is configured)",
			ConstLabels: constLabels,
		}, []string{"exit_node"}),
		AllowsExitTraffic: promauto.With(Registry).NewGauge(prometheus.GaugeOpts{
			Name:        "tunnelmesh_allows_exit_traffic",
			Help:        "Whether this node allows exit traffic from other peers (1) or not (0)",
			ConstLabels: constLabels,
		}),

		// Geolocation metrics
		PeerLatitude: promauto.With(Registry).NewGauge(prometheus.GaugeOpts{
			Name:        "tunnelmesh_peer_latitude",
			Help:        "Peer geographic latitude",
			ConstLabels: constLabels,
		}),
		PeerLongitude: promauto.With(Registry).NewGauge(prometheus.GaugeOpts{
			Name:        "tunnelmesh_peer_longitude",
			Help:        "Peer geographic longitude",
			ConstLabels: constLabels,
		}),
		PeerLocationInfo: promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
			Name:        "tunnelmesh_peer_location_info",
			Help:        "Peer location information (value is always 1)",
			ConstLabels: constLabels,
		}, []string{"city"}),

		// Peer info gauge
		PeerInfo: promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
			Name: "tunnelmesh_peer_info",
			Help: "Peer information (value is always 1)",
		}, []string{"peer", "mesh_ip", "version"}),

		// Latency metrics
		CoordinatorRTTSeconds: promauto.With(Registry).NewGauge(prometheus.GaugeOpts{
			Name:        "tunnelmesh_peer_coordinator_rtt_seconds",
			Help:        "Round-trip time to coordinator in seconds",
			ConstLabels: constLabels,
		}),
		PeerLatencySeconds: promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
			Name:        "tunnelmesh_peer_udp_latency_seconds",
			Help:        "UDP tunnel RTT to other peers in seconds (only available for direct UDP connections)",
			ConstLabels: constLabels,
		}, []string{"target_peer"}),

		// Connection setup duration: low-frequency (once per connection), supports
		// exemplars so a slow bucket in Grafana links directly to the offending trace.
		ConnectionSetupDuration: promauto.With(Registry).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "tunnelmesh_connection_setup_duration_seconds",
			Help:                            "Time to establish a connection to a peer, from dial to connected state",
			Buckets:                         []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Minute,
			ConstLabels:                     constLabels,
		}, []string{"transport", "result"}),
	}

	// Set peer info
	m.PeerInfo.WithLabelValues(peerName, meshIP, version).Set(1)

	return m
}

// RecordConnectionSetup records the duration of a connection setup attempt with an exemplar.
// traceID should be the hex trace ID from the active OTel span (empty string disables exemplar).
func (m *PeerMetrics) RecordConnectionSetup(transport, result string, durationSeconds float64, traceID string) {
	obs := m.ConnectionSetupDuration.WithLabelValues(transport, result)
	if traceID != "" {
		if ex, ok := obs.(prometheus.ExemplarObserver); ok {
			ex.ObserveWithExemplar(durationSeconds, prometheus.Labels{"traceID": traceID})
			return
		}
	}
	obs.Observe(durationSeconds)
}
