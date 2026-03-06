package peer

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/coord"
	"github.com/tunnelmesh/tunnelmesh/internal/dns"
	"github.com/tunnelmesh/tunnelmesh/internal/peer/connection"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"github.com/tunnelmesh/tunnelmesh/internal/transport"
	sshtransport "github.com/tunnelmesh/tunnelmesh/internal/transport/ssh"
	udptransport "github.com/tunnelmesh/tunnelmesh/internal/transport/udp"
	"github.com/tunnelmesh/tunnelmesh/internal/tunnel"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
)

// networkBypassWindow is the duration after a network change during which
// we consider the node to be in a "recovery" state. Used for logging and debugging.
const networkBypassWindow = 10 * time.Second

// asymmetricDetectionGracePeriod is the duration after tunnel establishment during which
// we ignore relay packets for asymmetric failure detection. This prevents tearing down
// freshly established tunnels due to stale relay packets that were in-flight before
// the peer knew the direct tunnel was ready.
const asymmetricDetectionGracePeriod = 5 * time.Second

// MeshNode coordinates all mesh networking operations for a peer.
// It owns all the managers and handles lifecycle of connections.
type MeshNode struct {
	// Identity (immutable after construction)
	identity *PeerIdentity

	// Coordination server client
	client *coord.Client

	// Transport layer
	TransportRegistry   *transport.Registry
	TransportNegotiator *transport.Negotiator
	SSHTransport        *sshtransport.Transport // For incoming SSH and key management
	UDPTransport        *udptransport.Transport // For crossing handshake pre-registration

	// Persistent relay for DERP-like instant connectivity
	PersistentRelay *tunnel.PersistentRelay

	// Tunnel and routing
	tunnelMgr *TunnelAdapter
	router    *routing.Router
	Forwarder *routing.Forwarder

	// Connection lifecycle management (FSM-based)
	Connections *connection.LifecycleManager

	// Signals
	triggerDiscovery chan struct{}

	// Network change state
	lastNetworkChange atomic.Value // stores time.Time

	// Heartbeat state
	heartbeatMu       sync.RWMutex
	lastPublicIPs     []string
	lastPrivateIPs    []string
	lastBehindNAT     bool
	heartbeatIPsIsSet bool

	// Peer cache for use when coord server unreachable (e.g., during network transitions)
	peerCacheMu sync.RWMutex
	peerCache   map[string]proto.Peer // peer name -> full peer info

	// Optional components
	Resolver *dns.Resolver

	// Latency measurement
	LatencyProber *LatencyProber

	// OnConnectionSetup is an optional callback invoked after a connection is established or fails.
	// transport: transport type used ("udp", "ssh", "relay", "unknown")
	// result: "success" or "failure"
	// durationSeconds: wall-clock time from dial to connected/failed
	// traceID: hex OTel trace ID for Prometheus exemplar linking (empty if tracing disabled)
	OnConnectionSetup func(transport, result string, durationSeconds float64, traceID string)
}

// NewMeshNode creates a new MeshNode with the given identity and client.
func NewMeshNode(identity *PeerIdentity, client *coord.Client) *MeshNode {
	router := routing.NewRouter()
	tunnelMgr := NewTunnelAdapter()

	node := &MeshNode{
		identity:         identity,
		client:           client,
		tunnelMgr:        tunnelMgr,
		router:           router,
		triggerDiscovery: make(chan struct{}, 1),
		peerCache:        make(map[string]proto.Peer),
	}

	// Initialize connection lifecycle manager with tunnel integration
	// Routes are managed separately by discovery (via UpdateRoutes) to stay in sync
	// with the coordination server's peer list
	node.Connections = connection.NewLifecycleManager(connection.LifecycleConfig{
		Tunnels: tunnelMgr,
		OnConnect: func(peerName string) {
			// Trigger discovery to refresh routes immediately on connect
			// This ensures routes are current even if discovery hasn't run recently
			log.Debug().Str("peer", peerName).Msg("peer connected, triggering discovery for route refresh")
			node.TriggerDiscovery()
		},
		OnDisconnect: func(peerName string) {
			// Trigger discovery to refresh routes and attempt reconnection
			log.Debug().Str("peer", peerName).Msg("peer disconnected, triggering discovery")
			node.TriggerDiscovery()
		},
	})

	node.lastNetworkChange.Store(time.Time{})
	return node
}

// Identity returns the peer's identity.
func (m *MeshNode) Identity() *PeerIdentity {
	return m.identity
}

// Client returns the coordination client.
func (m *MeshNode) Client() *coord.Client {
	return m.client
}

// TunnelMgr returns the tunnel adapter.
func (m *MeshNode) TunnelMgr() *TunnelAdapter {
	return m.tunnelMgr
}

// Router returns the routing table.
func (m *MeshNode) Router() *routing.Router {
	return m.router
}

// DiscoveryChan returns the channel used to trigger peer discovery.
func (m *MeshNode) DiscoveryChan() <-chan struct{} {
	return m.triggerDiscovery
}

// TriggerDiscovery triggers a peer discovery cycle.
// Returns true if the trigger was sent, false if a discovery is already pending.
func (m *MeshNode) TriggerDiscovery() bool {
	select {
	case m.triggerDiscovery <- struct{}{}:
		return true
	default:
		return false
	}
}

// RecordNetworkChange records that a network change has occurred.
// This is used to track network recovery state for logging and debugging.
func (m *MeshNode) RecordNetworkChange() {
	m.lastNetworkChange.Store(time.Now())
}

// InNetworkBypassWindow returns true if we're within the recovery window
// after a network change. Used for logging and debugging purposes.
func (m *MeshNode) InNetworkBypassWindow() bool {
	lastChange := m.lastNetworkChange.Load().(time.Time)
	if lastChange.IsZero() {
		return false
	}
	return time.Since(lastChange) < networkBypassWindow
}

// GetHeartbeatIPs returns the last known IPs from heartbeat detection.
func (m *MeshNode) GetHeartbeatIPs() (publicIPs, privateIPs []string, behindNAT bool) {
	m.heartbeatMu.RLock()
	defer m.heartbeatMu.RUnlock()
	return m.lastPublicIPs, m.lastPrivateIPs, m.lastBehindNAT
}

// SetHeartbeatIPs updates the last known IPs from heartbeat detection.
func (m *MeshNode) SetHeartbeatIPs(publicIPs, privateIPs []string, behindNAT bool) {
	m.heartbeatMu.Lock()
	defer m.heartbeatMu.Unlock()
	m.lastPublicIPs = publicIPs
	m.lastPrivateIPs = privateIPs
	m.lastBehindNAT = behindNAT
	m.heartbeatIPsIsSet = true
}

// CachePeer caches the full peer info for use when coord server is unreachable.
func (m *MeshNode) CachePeer(peer proto.Peer) {
	m.peerCacheMu.Lock()
	defer m.peerCacheMu.Unlock()
	m.peerCache[peer.Name] = peer
}

// GetCachedPeer returns the cached peer info.
func (m *MeshNode) GetCachedPeer(peerName string) (proto.Peer, bool) {
	m.peerCacheMu.RLock()
	defer m.peerCacheMu.RUnlock()
	peer, ok := m.peerCache[peerName]
	return peer, ok
}

// ClearPeerCache clears all cached peer info.
// Called on network change to prevent using stale IPs/endpoints.
func (m *MeshNode) ClearPeerCache() {
	m.peerCacheMu.Lock()
	defer m.peerCacheMu.Unlock()
	m.peerCache = make(map[string]proto.Peer)
	log.Debug().Msg("cleared peer cache due to network change")
}

// IPsChanged returns true if the provided IPs differ from the last known IPs.
// If no IPs have been set yet (first heartbeat), returns true only if we have
// recorded IPs before (to avoid re-registering on first heartbeat).
func (m *MeshNode) IPsChanged(publicIPs, privateIPs []string, behindNAT bool) bool {
	m.heartbeatMu.RLock()
	defer m.heartbeatMu.RUnlock()

	if !m.heartbeatIPsIsSet {
		// First time - not a "change" per se, but caller should set initial IPs
		return true
	}

	return !slicesEqual(publicIPs, m.lastPublicIPs) ||
		!slicesEqual(privateIPs, m.lastPrivateIPs) ||
		behindNAT != m.lastBehindNAT
}

// CollectStats collects stats from the forwarder and tunnel manager.
func (m *MeshNode) CollectStats() *proto.PeerStats {
	stats := &proto.PeerStats{
		ActiveTunnels: m.tunnelMgr.CountHealthy(),
		Location:      m.identity.Location, // Include location in every heartbeat
		Connections:   m.getConnectionTypes(),
	}

	if m.Forwarder != nil {
		fwdStats := m.Forwarder.Stats()
		stats.PacketsSent = fwdStats.PacketsSent
		stats.PacketsReceived = fwdStats.PacketsReceived
		stats.BytesSent = fwdStats.BytesSent
		stats.BytesReceived = fwdStats.BytesReceived
		stats.DroppedNoRoute = fwdStats.DroppedNoRoute
		stats.DroppedNoTunnel = fwdStats.DroppedNoTunnel
		stats.Errors = fwdStats.Errors
	}

	// Include coordinator RTT from last heartbeat ack
	if m.PersistentRelay != nil {
		if rtt := m.PersistentRelay.GetLastRTT(); rtt > 0 {
			stats.CoordinatorRTTMs = rtt.Milliseconds()
		}
	}

	// Include peer latencies from latency prober
	if m.LatencyProber != nil {
		stats.PeerLatencies = m.LatencyProber.GetLatencies()
		if len(stats.PeerLatencies) > 0 {
			log.Debug().Int("count", len(stats.PeerLatencies)).Msg("collected peer latencies for heartbeat")
		}
	}

	return stats
}

// getConnectionTypes returns a map of peer name to transport type for all connected peers.
func (m *MeshNode) getConnectionTypes() map[string]string {
	if m.Connections == nil {
		return nil
	}

	connections := make(map[string]string)
	for _, info := range m.Connections.AllInfo() {
		if info.State == connection.StateConnected && info.TransportType != "" {
			connections[info.PeerName] = info.TransportType
		}
	}

	if len(connections) == 0 {
		return nil
	}
	return connections
}

// slicesEqual compares two string slices for equality.
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// HandleRelayPacketForAsymmetricDetection checks if we should invalidate a direct tunnel
// when receiving relay packets from a peer. This handles detection of asymmetric path failures
// where we can reach the peer directly but they can't reach us.
//
// Returns true if the tunnel was invalidated, false otherwise.
func (m *MeshNode) HandleRelayPacketForAsymmetricDetection(sourcePeer string) bool {
	pc := m.Connections.Get(sourcePeer)
	if pc == nil {
		return false
	}

	if !pc.HasTunnel() {
		return false
	}

	connectedSince := pc.ConnectedSince()
	// If connectedSince is zero (state not Connected), skip invalidation
	// to avoid false positives from time.Since(zero) returning huge duration
	if connectedSince.IsZero() {
		log.Debug().
			Str("peer", sourcePeer).
			Msg("ignoring relay packet: connection state not Connected (connectedSince is zero)")
		return false
	}

	tunnelAge := time.Since(connectedSince)
	if tunnelAge < asymmetricDetectionGracePeriod {
		log.Debug().
			Str("peer", sourcePeer).
			Dur("tunnel_age", tunnelAge).
			Msg("ignoring relay packet during grace period after tunnel establishment")
		return false
	}

	log.Info().
		Str("peer", sourcePeer).
		Dur("tunnel_age", tunnelAge).
		Msg("received relay packet from peer with active tunnel, invalidating stale tunnel")
	_ = pc.Disconnect(context.Background(), "peer using relay (asymmetric tunnel failure)", nil)
	return true
}

// setupRelayHandlers configures the packet and peer reconnection handlers for a relay.
// This is extracted to ensure consistent behavior between initial connect and reconnect.
func (m *MeshNode) setupRelayHandlers(relay *tunnel.PersistentRelay) {
	// Set up packet handler to route incoming relay packets to forwarder
	relay.SetPacketHandler(func(sourcePeer string, data []byte) {
		if m.Forwarder != nil {
			log.Debug().Str("source", sourcePeer).Int("len", len(data)).Msg("routing relay packet to forwarder")
			m.Forwarder.HandleRelayPacket(sourcePeer, data)
		} else {
			log.Warn().Str("source", sourcePeer).Int("len", len(data)).Msg("dropping relay packet: forwarder not set")
		}

		// Check for asymmetric path failure (we can reach peer but they can't reach us)
		m.HandleRelayPacketForAsymmetricDetection(sourcePeer)
	})

	// Set up handler for peer reconnection notifications.
	// When a peer reconnects to the relay, they may have changed networks (new IPs).
	// We fetch their fresh info from the server and compare IPs - if changed,
	// our direct tunnel is likely broken and needs to be reconnected.
	relay.SetPeerReconnectedHandler(func(peerName string) {
		// Get our cached info for this peer (if any)
		oldPeer, hadCached := m.GetCachedPeer(peerName)

		// Fetch fresh peer info from coordination server
		peers, err := m.client.ListPeers()
		if err != nil {
			log.Debug().Err(err).Str("peer", peerName).Msg("failed to fetch peers after reconnect notification")
			return
		}

		// Find the peer that reconnected and update our cache
		var newPeer *proto.Peer
		for _, p := range peers {
			m.CachePeer(p) // Update cache for all peers
			if p.Name == peerName {
				newPeer = &p
			}
		}

		if newPeer == nil {
			log.Debug().Str("peer", peerName).Msg("peer not found on server after reconnect notification")
			return
		}

		// Check if IPs changed (only if we had cached info to compare)
		if hadCached {
			oldPublicIPs := make(map[string]bool)
			for _, ip := range oldPeer.PublicIPs {
				oldPublicIPs[ip] = true
			}
			ipsChanged := false
			for _, ip := range newPeer.PublicIPs {
				if !oldPublicIPs[ip] {
					ipsChanged = true
					break
				}
			}
			if len(oldPeer.PublicIPs) != len(newPeer.PublicIPs) {
				ipsChanged = true
			}

			if !ipsChanged {
				log.Debug().
					Str("peer", peerName).
					Msg("peer reconnected but IPs unchanged - no action needed")
				return
			}

			log.Info().
				Str("peer", peerName).
				Strs("old_ips", oldPeer.PublicIPs).
				Strs("new_ips", newPeer.PublicIPs).
				Msg("peer IPs changed after reconnect notification")
		} else {
			log.Debug().
				Str("peer", peerName).
				Msg("peer reconnected (no cached info to compare)")
		}

		// If we have a direct tunnel to this peer, disconnect it
		// (the peer's IPs changed, so our direct tunnel is broken)
		if pc := m.Connections.Get(peerName); pc != nil && pc.HasTunnel() {
			log.Info().
				Str("peer", peerName).
				Str("transport", pc.TransportType()).
				Msg("disconnecting tunnel due to peer IP change")
			_ = pc.Disconnect(context.Background(), "peer IP changed", nil)
		}

		// Trigger discovery to reconnect with new IPs
		m.TriggerDiscovery()
	})

	// Set up handler for reconnection errors to detect when we need to re-register.
	// Handles both 404 (peer deregistered) and 401 (JWT expired) by re-registering
	// and pushing the fresh token into the relay so the next attempt succeeds.
	relay.SetReconnectErrorHandler(func(err error) {
		needsReregister := errors.Is(err, coord.ErrPeerNotFound) || errors.Is(err, coord.ErrUnauthorized)
		if needsReregister {
			if errors.Is(err, coord.ErrUnauthorized) {
				log.Info().Msg("relay JWT expired, re-registering to refresh token...")
			} else {
				log.Info().Msg("peer not registered on server, re-registering...")
			}
			publicIPs, privateIPs, behindNAT := m.identity.GetLocalIPs()
			hasMonitoring := m.identity.Config.Coordinator.Enabled &&
				(m.identity.Config.Coordinator.Monitoring.PrometheusURL != "" || m.identity.Config.Coordinator.Monitoring.GrafanaURL != "")
			if _, regErr := m.client.Register(
				m.identity.Name, m.identity.PubKeyEncoded,
				publicIPs, privateIPs, m.identity.SSHPort, m.identity.UDPPort, behindNAT, m.identity.Version, nil,
				m.identity.Config.ExitPeer, m.identity.Config.AllowExitTraffic, m.identity.Config.DNS.Aliases,
				m.identity.Config.Coordinator.Enabled, hasMonitoring,
			); regErr != nil {
				log.Error().Err(regErr).Msg("failed to re-register after relay auth failure")
			} else {
				log.Info().Msg("re-registered with coordination server")
				m.SetHeartbeatIPs(publicIPs, privateIPs, behindNAT)
				// Push fresh JWT into relay so the next autoReconnect attempt uses it
				relay.UpdateJWTToken(m.client.JWTToken())
			}
		}
	})

	// Set up push notification handlers for relay and hole-punch requests.
	// These must be set here (not just in RunHeartbeat) to ensure they're
	// re-registered after relay reconnection.
	// Note: Uses background context since handlers spawn their own goroutines
	// and should continue processing even during shutdown.
	relay.SetRelayNotifyHandler(func(peers []string) {
		log.Debug().Strs("peers", peers).Msg("received relay notification via WebSocket")
		m.HandleRelayRequests(context.Background(), peers)
	})
	relay.SetHolePunchNotifyHandler(func(peers []string) {
		log.Debug().Strs("peers", peers).Msg("received hole-punch notification via WebSocket")
		m.HandleHolePunchRequests(context.Background(), peers)
	})
}

// ConnectPersistentRelay establishes the persistent relay connection.
// This should be called after registration when we have a JWT token.
func (m *MeshNode) ConnectPersistentRelay(ctx context.Context) error {
	jwtToken := m.client.JWTToken()
	if jwtToken == "" {
		log.Warn().Msg("no JWT token available for persistent relay")
		return nil
	}

	m.PersistentRelay = tunnel.NewPersistentRelay(m.client.BaseURL(), jwtToken)

	// Set up handlers BEFORE connecting
	m.setupRelayHandlers(m.PersistentRelay)

	if err := m.PersistentRelay.Connect(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to connect persistent relay")
		return err
	}

	// Update forwarder with new relay reference
	if m.Forwarder != nil {
		m.Forwarder.SetRelay(m.PersistentRelay)
	}

	log.Info().Msg("persistent relay connected for DERP-like routing")
	return nil
}

// ReconnectPersistentRelay reconnects the persistent relay after a network change.
// It uses atomic swap to keep the old relay active until the new one connects,
// preventing packet loss during the reconnection window.
func (m *MeshNode) ReconnectPersistentRelay(ctx context.Context) {
	ctx, span := tracing.Tracer("tunnelmesh/peer").Start(ctx, "peer.relay.reconnect")
	defer span.End()
	span.SetAttributes(attribute.String("relay.url", m.client.BaseURL()))

	oldRelay := m.PersistentRelay
	// Note: We intentionally do NOT close or clear the old relay here.
	// The forwarder continues using it until the new relay is ready.
	// We start reconnection immediately to minimize the window where
	// the old relay's connection might die before new one is ready.

	backoff := 200 * time.Millisecond // Start with fast retries
	maxBackoff := 10 * time.Second    // Cap backoff lower for faster recovery
	maxAttempts := 15                 // More attempts with faster backoff

	// advanceBackoff doubles the backoff, adds jitter, then caps at maxBackoff.
	// Defined once here to avoid duplicating the formula across multiple retry sites.
	advanceBackoff := func(d time.Duration) time.Duration {
		d *= 2
		d += time.Duration(rand.Int63n(int64(d / 4))) // jitter before cap
		if d > maxBackoff {
			d = maxBackoff
		}
		return d
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			span.SetStatus(otelcodes.Error, "context cancelled")
			return
		default:
		}

		span.SetAttributes(attribute.Int("relay.attempt", attempt))

		// Get fresh JWT token for new connection
		jwtToken := m.client.JWTToken()
		if jwtToken == "" {
			log.Warn().Int("attempt", attempt).Msg("no JWT token for relay reconnection")
			if attempt < maxAttempts {
				select {
				case <-ctx.Done():
					span.SetStatus(otelcodes.Error, "context cancelled")
					return
				case <-time.After(backoff):
				}
				backoff = advanceBackoff(backoff)
			}
			continue
		}

		// Create new relay (don't touch old yet)
		newRelay := tunnel.NewPersistentRelay(m.client.BaseURL(), jwtToken)

		// Set up handlers BEFORE connecting (uses shared handler setup)
		m.setupRelayHandlers(newRelay)

		if err := newRelay.Connect(ctx); err != nil {
			span.RecordError(err)
			log.Warn().Err(err).Int("attempt", attempt).Msg("relay reconnection failed")
			_ = newRelay.Close()

			if attempt < maxAttempts {
				select {
				case <-ctx.Done():
					span.SetStatus(otelcodes.Error, "context cancelled")
					return
				case <-time.After(backoff):
				}
				backoff = advanceBackoff(backoff)
			}
			continue
		}

		// SUCCESS! Atomically swap relay references
		// Forwarder will immediately start using new relay for outbound packets
		m.PersistentRelay = newRelay
		if m.Forwarder != nil {
			m.Forwarder.SetRelay(newRelay)
		}

		// NOW close old relay (packets already routing to new)
		if oldRelay != nil {
			_ = oldRelay.Close()
		}

		span.SetStatus(otelcodes.Ok, "")
		span.SetAttributes(attribute.Int("relay.attempts_total", attempt))
		log.Info().Int("attempt", attempt).Msg("persistent relay reconnected after network change")
		return
	}

	// All attempts failed - now clear references as last resort
	if oldRelay != nil {
		_ = oldRelay.Close()
	}
	m.PersistentRelay = nil
	if m.Forwarder != nil {
		m.Forwarder.SetRelay(nil)
	}
	span.SetStatus(otelcodes.Error, "max attempts exceeded")
	log.Error().Msg("gave up reconnecting persistent relay after max attempts")
}

// IsPersistentRelayConnected returns true if the persistent relay is connected.
func (m *MeshNode) IsPersistentRelayConnected() bool {
	return m.PersistentRelay != nil && m.PersistentRelay.IsConnected()
}
