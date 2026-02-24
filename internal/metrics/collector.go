package metrics

import (
	"context"
	"time"

	"github.com/tunnelmesh/tunnelmesh/internal/peer"
	"github.com/tunnelmesh/tunnelmesh/internal/peer/connection"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
	"github.com/tunnelmesh/tunnelmesh/internal/tunnel"
)

// ForwarderSnapshot holds the last-seen forwarder stats for delta calculation.
type ForwarderSnapshot struct {
	PacketsSent     uint64
	PacketsReceived uint64
	BytesSent       uint64
	BytesReceived   uint64
	DroppedNoRoute  uint64
	DroppedNoTunnel uint64
	DroppedNonIPv4  uint64
	DroppedFiltered uint64 // Total drops (for logging, not per-peer)
	Errors          uint64
	ExitPacketsSent uint64
	ExitBytesSent   uint64
}

// Collector periodically collects metrics from the MeshNode.
type Collector struct {
	metrics *PeerMetrics
	config  CollectorConfig

	// References to collect from
	forwarder           ForwarderStats
	tunnelMgr           TunnelManager
	connections         ConnectionManager
	relay               RelayStatus
	rttProvider         RTTProvider
	peerLatencyProvider PeerLatencyProvider
	identity            *peer.PeerIdentity
	allowsExit          bool
	filter              FilterStatus

	// Last snapshot for delta calculation
	lastForwarder ForwarderSnapshot
}

// ForwarderStats interface for getting forwarder statistics.
type ForwarderStats interface {
	Stats() routing.ForwarderStats
	ExitPeer() string
}

// TunnelManager interface for getting tunnel statistics.
type TunnelManager interface {
	List() []string
	CountHealthy() int
}

// ConnectionManager interface for getting connection information.
type ConnectionManager interface {
	AllInfo() []connection.ConnectionInfo
}

// RelayStatus interface for getting relay status.
type RelayStatus interface {
	IsConnected() bool
}

// RTTProvider interface for getting RTT measurements.
type RTTProvider interface {
	GetLastRTT() time.Duration
}

// PeerLatencyProvider interface for getting peer-to-peer latency measurements.
type PeerLatencyProvider interface {
	GetLatencies() map[string]int64
}

// FilterStatus interface for getting packet filter status.
type FilterStatus interface {
	IsDefaultDeny() bool
	RuleCountBySource() routing.RuleCounts
}

// CollectorConfig holds configuration for the collector.
type CollectorConfig struct {
	Forwarder           ForwarderStats
	TunnelMgr           TunnelManager
	Connections         ConnectionManager
	Relay               RelayStatus
	RTTProvider         RTTProvider
	PeerLatencyProvider PeerLatencyProvider
	Identity            *peer.PeerIdentity
	AllowsExit          bool
	Filter              FilterStatus
}

// NewCollector creates a new metrics collector.
func NewCollector(m *PeerMetrics, cfg CollectorConfig) *Collector {
	return &Collector{
		metrics:             m,
		config:              cfg,
		forwarder:           cfg.Forwarder,
		tunnelMgr:           cfg.TunnelMgr,
		connections:         cfg.Connections,
		relay:               cfg.Relay,
		rttProvider:         cfg.RTTProvider,
		peerLatencyProvider: cfg.PeerLatencyProvider,
		identity:            cfg.Identity,
		allowsExit:          cfg.AllowsExit,
		filter:              cfg.Filter,
	}
}

// Collect updates all metrics from the current state.
func (c *Collector) Collect() {
	c.collectForwarderStats()
	c.collectTunnelStats()
	c.collectConnectionStats()
	c.collectRelayStats()
	c.collectLatencyStats()
	c.collectExitPeerStats()
	c.collectGeolocationStats()
	c.collectFilterStats()
}

func (c *Collector) collectForwarderStats() {
	if c.forwarder == nil {
		return
	}

	stats := c.forwarder.Stats()

	// Calculate deltas and add to counters
	if stats.PacketsSent > c.lastForwarder.PacketsSent {
		c.metrics.PacketsSent.Add(float64(stats.PacketsSent - c.lastForwarder.PacketsSent))
	}
	if stats.PacketsReceived > c.lastForwarder.PacketsReceived {
		c.metrics.PacketsReceived.Add(float64(stats.PacketsReceived - c.lastForwarder.PacketsReceived))
	}
	if stats.BytesSent > c.lastForwarder.BytesSent {
		c.metrics.BytesSent.Add(float64(stats.BytesSent - c.lastForwarder.BytesSent))
	}
	if stats.BytesReceived > c.lastForwarder.BytesReceived {
		c.metrics.BytesReceived.Add(float64(stats.BytesReceived - c.lastForwarder.BytesReceived))
	}
	if stats.DroppedNoRoute > c.lastForwarder.DroppedNoRoute {
		c.metrics.DroppedNoRoute.Add(float64(stats.DroppedNoRoute - c.lastForwarder.DroppedNoRoute))
	}
	if stats.DroppedNoTunnel > c.lastForwarder.DroppedNoTunnel {
		c.metrics.DroppedNoTunnel.Add(float64(stats.DroppedNoTunnel - c.lastForwarder.DroppedNoTunnel))
	}
	if stats.DroppedNonIPv4 > c.lastForwarder.DroppedNonIPv4 {
		c.metrics.DroppedNonIPv4.Add(float64(stats.DroppedNonIPv4 - c.lastForwarder.DroppedNonIPv4))
	}
	if stats.Errors > c.lastForwarder.Errors {
		c.metrics.ForwarderErrors.Add(float64(stats.Errors - c.lastForwarder.Errors))
	}

	// Note: Packet filter drops by protocol/peer are tracked via callback (TrackFilterDrop)
	// The forwarder calls SetOnFilterDrop with a callback that uses TrackFilterDrop

	// Exit traffic stats
	if stats.ExitPacketsSent > c.lastForwarder.ExitPacketsSent {
		c.metrics.ExitPacketsSent.Add(float64(stats.ExitPacketsSent - c.lastForwarder.ExitPacketsSent))
	}
	if stats.ExitBytesSent > c.lastForwarder.ExitBytesSent {
		c.metrics.ExitBytesSent.Add(float64(stats.ExitBytesSent - c.lastForwarder.ExitBytesSent))
	}

	// Store current for next delta
	c.lastForwarder = ForwarderSnapshot{
		PacketsSent:     stats.PacketsSent,
		PacketsReceived: stats.PacketsReceived,
		BytesSent:       stats.BytesSent,
		BytesReceived:   stats.BytesReceived,
		DroppedNoRoute:  stats.DroppedNoRoute,
		DroppedNoTunnel: stats.DroppedNoTunnel,
		DroppedNonIPv4:  stats.DroppedNonIPv4,
		DroppedFiltered: stats.DroppedFiltered,
		Errors:          stats.Errors,
		ExitPacketsSent: stats.ExitPacketsSent,
		ExitBytesSent:   stats.ExitBytesSent,
	}
}

func (c *Collector) collectTunnelStats() {
	if c.tunnelMgr == nil {
		return
	}

	tunnels := c.tunnelMgr.List()
	c.metrics.ActiveTunnels.Set(float64(len(tunnels)))
	c.metrics.HealthyTunnels.Set(float64(c.tunnelMgr.CountHealthy()))
}

func (c *Collector) collectConnectionStats() {
	if c.connections == nil {
		return
	}

	for _, info := range c.connections.AllInfo() {
		transport := info.TransportType
		if transport == "" {
			transport = "none"
		}
		c.metrics.ConnectionState.WithLabelValues(info.PeerName, transport).Set(float64(info.State))
	}
}

func (c *Collector) collectRelayStats() {
	if c.relay == nil {
		c.metrics.RelayConnected.Set(0)
		return
	}

	if c.relay.IsConnected() {
		c.metrics.RelayConnected.Set(1)
	} else {
		c.metrics.RelayConnected.Set(0)
	}
}

func (c *Collector) collectLatencyStats() {
	// Coordinator RTT
	if c.rttProvider != nil {
		rtt := c.rttProvider.GetLastRTT()
		if rtt > 0 {
			c.metrics.CoordinatorRTTSeconds.Set(rtt.Seconds())
		}
	}

	// Peer-to-peer latencies (UDP tunnel RTT)
	if c.peerLatencyProvider != nil {
		latencies := c.peerLatencyProvider.GetLatencies()
		for peerName, latencyUs := range latencies {
			// Convert microseconds to seconds
			c.metrics.PeerLatencySeconds.WithLabelValues(peerName).Set(float64(latencyUs) / 1e6)
		}
	}
}

func (c *Collector) collectExitPeerStats() {
	// Set allows exit traffic flag
	if c.allowsExit {
		c.metrics.AllowsExitTraffic.Set(1)
	} else {
		c.metrics.AllowsExitTraffic.Set(0)
	}

	// Check if exit node is configured
	if c.forwarder == nil {
		c.metrics.ExitPeerConfigured.Set(0)
		return
	}

	exitNode := c.forwarder.ExitPeer()
	if exitNode != "" {
		c.metrics.ExitPeerConfigured.Set(1)
		c.metrics.ExitPeerInfo.WithLabelValues(exitNode).Set(1)
	} else {
		c.metrics.ExitPeerConfigured.Set(0)
	}
}

func (c *Collector) collectGeolocationStats() {
	if c.identity == nil || c.identity.Location == nil {
		return
	}

	loc := c.identity.Location
	c.metrics.PeerLatitude.Set(loc.Latitude)
	c.metrics.PeerLongitude.Set(loc.Longitude)

	if loc.City != "" {
		c.metrics.PeerLocationInfo.WithLabelValues(loc.City).Set(1)
	}
}

func (c *Collector) collectFilterStats() {
	if c.filter == nil {
		// No filter configured - set defaults
		c.metrics.FilterDefaultDeny.Set(0)
		c.metrics.FilterRulesTotal.WithLabelValues("coordinator").Set(0)
		c.metrics.FilterRulesTotal.WithLabelValues("config").Set(0)
		c.metrics.FilterRulesTotal.WithLabelValues("temporary").Set(0)
		c.metrics.FilterRulesTotal.WithLabelValues("service").Set(0)
		return
	}

	// Default deny mode
	if c.filter.IsDefaultDeny() {
		c.metrics.FilterDefaultDeny.Set(1)
	} else {
		c.metrics.FilterDefaultDeny.Set(0)
	}

	// Rule counts by source
	counts := c.filter.RuleCountBySource()
	c.metrics.FilterRulesTotal.WithLabelValues("coordinator").Set(float64(counts.Coordinator))
	c.metrics.FilterRulesTotal.WithLabelValues("config").Set(float64(counts.PeerConfig))
	c.metrics.FilterRulesTotal.WithLabelValues("temporary").Set(float64(counts.Temporary))
	c.metrics.FilterRulesTotal.WithLabelValues("service").Set(float64(counts.Service))
}

// Run starts periodic metric collection.
func (c *Collector) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Collect immediately on start
	c.Collect()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.Collect()
		}
	}
}

// RelayWrapper wraps a PersistentRelay to implement RelayStatus and RTTProvider interfaces.
type RelayWrapper struct {
	relay *tunnel.PersistentRelay
}

// NewRelayWrapper creates a new relay wrapper.
func NewRelayWrapper(relay *tunnel.PersistentRelay) *RelayWrapper {
	if relay == nil {
		return nil
	}
	return &RelayWrapper{relay: relay}
}

func (w *RelayWrapper) IsConnected() bool {
	return w.relay != nil && w.relay.IsConnected()
}

func (w *RelayWrapper) GetLastRTT() time.Duration {
	if w.relay == nil {
		return 0
	}
	return w.relay.GetLastRTT()
}

// TrackReconnect is called by the connection observer when a reconnect happens.
func (c *Collector) TrackReconnect(targetPeer string) {
	c.metrics.ReconnectCount.WithLabelValues(targetPeer).Inc()
}

// TrackFilterDrop increments the dropped packet counter for the given protocol and source peer.
// Protocol should be 6 for TCP, 17 for UDP.
// sourcePeer is always available since all incoming paths provide peer context:
// - HandleRelayPacket has sourcePeer from the relay message
// - HandleTunnel has peerName from the tunnel association
func (c *Collector) TrackFilterDrop(protocol uint8, sourcePeer string) {
	c.metrics.DroppedFiltered.WithLabelValues(routing.ProtocolToString(protocol), sourcePeer).Inc()
}

// FilterDropCallback returns a callback function for the forwarder to call on filter drops.
func (c *Collector) FilterDropCallback() func(protocol uint8, sourcePeer string) {
	return c.TrackFilterDrop
}

// ReconnectObserver returns a connection observer that tracks reconnects.
func (c *Collector) ReconnectObserver() connection.Observer {
	return connection.ObserverFunc(func(t connection.Transition) {
		// Track reconnections: transitioning from Reconnecting to Connected
		if t.From == connection.StateReconnecting && t.To == connection.StateConnected {
			c.TrackReconnect(t.PeerName)
		}
	})
}
