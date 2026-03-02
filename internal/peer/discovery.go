package peer

import (
	"context"
	"math/rand"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"github.com/tunnelmesh/tunnelmesh/internal/transport"
	"github.com/tunnelmesh/tunnelmesh/internal/tunnel"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
)

// RunPeerDiscovery periodically discovers peers and establishes tunnels.
func (m *MeshNode) RunPeerDiscovery(ctx context.Context) {
	// Add random jitter (0-3 seconds) before initial discovery
	jitter := time.Duration(rand.Intn(3000)) * time.Millisecond
	log.Debug().Dur("jitter", jitter).Msg("waiting before initial peer discovery")
	select {
	case <-ctx.Done():
		return
	case <-time.After(jitter):
	}

	// Initial peer discovery
	m.DiscoverAndConnectPeers(ctx)

	// Fast retry phase: discover every 5 seconds for the first 30 seconds
	fastTicker := time.NewTicker(5 * time.Second)
	fastPhaseEnd := time.After(30 * time.Second)

fastLoop:
	for {
		select {
		case <-ctx.Done():
			fastTicker.Stop()
			return
		case <-fastPhaseEnd:
			fastTicker.Stop()
			break fastLoop
		case <-fastTicker.C:
			m.DiscoverAndConnectPeers(ctx)
			m.CheckAndHandleRelayRequests(ctx)
		case <-m.triggerDiscovery:
			log.Debug().Msg("peer discovery triggered by network change")
			m.DiscoverAndConnectPeers(ctx)
			m.CheckAndHandleRelayRequests(ctx)
		}
	}

	// Normal phase: discover every 60 seconds
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.DiscoverAndConnectPeers(ctx)
		case <-m.triggerDiscovery:
			log.Debug().Msg("peer discovery triggered by network change")
			m.DiscoverAndConnectPeers(ctx)
		}
	}
}

// DiscoverAndConnectPeers lists peers from the coordination server and establishes tunnels.
func (m *MeshNode) DiscoverAndConnectPeers(ctx context.Context) {
	peers, err := m.client.ListPeers()
	if err != nil {
		log.Warn().Err(err).Msg("failed to list peers")
		return
	}

	existingTunnels := m.tunnelMgr.List()
	existingSet := make(map[string]bool)
	for _, name := range existingTunnels {
		existingSet[name] = true
	}

	// Build routes map for atomic update - only routes for peers known to coord server
	routes := make(map[string]string, len(peers))

	for _, peer := range peers {
		if peer.Name == m.identity.Name {
			continue // Skip self
		}

		// Add peer's public key to authorized keys for incoming connections
		if peer.PublicKey != "" && m.SSHTransport != nil {
			pubKey, err := config.DecodePublicKey(peer.PublicKey)
			if err != nil {
				log.Warn().Err(err).Str("peer", peer.Name).Msg("failed to decode peer public key")
			} else {
				m.SSHTransport.AddAuthorizedKey(pubKey)
			}
		}

		// Collect route for atomic update
		routes[peer.MeshIP] = peer.Name
		// Cache full peer info for use when coord server is unreachable
		m.CachePeer(peer)

		// Skip if tunnel already exists
		if existingSet[peer.Name] {
			continue
		}

		// Skip peers with no inbound transport endpoints — they cannot accept tunnels
		if peer.SSHPort == 0 && peer.UDPPort == 0 {
			log.Debug().Str("peer", peer.Name).Msg("skipping peer with no transport endpoints")
			continue
		}

		// Skip if connection attempt already in progress
		if m.Connections.IsConnecting(peer.Name) {
			log.Debug().Str("peer", peer.Name).Msg("connection attempt already in progress, skipping")
			continue
		}

		// Try to establish tunnel
		go m.EstablishTunnel(ctx, peer)
	}

	// Atomically update all routes - this adds new peers and removes stale ones
	m.router.UpdateRoutes(routes)
}

// shouldInitiateConnection determines if we should be the initiator for a connection
// to the given peer. Uses public key comparison as a deterministic tie-breaker:
// the peer with the lexicographically "lower" public key initiates.
// This prevents both peers from simultaneously connecting with different transports.
func (m *MeshNode) shouldInitiateConnection(peer proto.Peer) bool {
	ourKey := m.identity.PubKeyEncoded
	peerKey := peer.PublicKey

	// Both keys must be present for deterministic tie-breaking
	if ourKey == "" || peerKey == "" {
		log.Debug().
			Str("peer", peer.Name).
			Bool("our_key_empty", ourKey == "").
			Bool("peer_key_empty", peerKey == "").
			Msg("skipping connection: missing public key for tie-breaking")
		return false
	}

	// Compare keys: lower key initiates
	return strings.Compare(ourKey, peerKey) < 0
}

// EstablishTunnel negotiates and establishes a tunnel to a peer.
// Uses public key comparison to determine which peer should initiate:
// the peer with the "lower" public key initiates, the other waits for incoming.
// This prevents asymmetric transport selection where peers use different transports.
func (m *MeshNode) EstablishTunnel(ctx context.Context, peer proto.Peer) {
	m.establishTunnelWithOptions(ctx, peer, false)
}

// EstablishTunnelForced initiates a tunnel regardless of public key comparison.
// Used for hole-punch requests where we must respond to enable NAT traversal.
func (m *MeshNode) EstablishTunnelForced(ctx context.Context, peer proto.Peer) {
	m.establishTunnelWithOptions(ctx, peer, true)
}

// establishTunnelWithOptions is the internal implementation of tunnel establishment.
// If forceInitiate is true, bypasses the public key comparison (for hole-punch).
func (m *MeshNode) establishTunnelWithOptions(ctx context.Context, peer proto.Peer, forceInitiate bool) {
	// Determine if we should be the initiator based on public key comparison
	// This prevents both peers from connecting simultaneously with different transports
	// Exception: hole-punch requests bypass this check to enable NAT traversal
	if !forceInitiate && !m.shouldInitiateConnection(peer) {
		log.Debug().
			Str("peer", peer.Name).
			Msg("not initiating connection (peer has lower pubkey, they will initiate)")
		return
	}

	// Start a trace span covering the entire tunnel setup (negotiate + transition).
	ctx, setupSpan := tracing.Tracer("tunnelmesh/peer").Start(ctx, "peer.establish_tunnel")
	defer setupSpan.End()
	setupSpan.SetAttributes(
		attribute.String("peer.name", peer.Name),
		attribute.String("peer.mesh_ip", peer.MeshIP),
		attribute.Bool("force_initiate", forceInitiate),
	)
	setupStart := time.Now()

	// Create a cancellable context for this outbound connection
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Mark as connecting with cancel function - if already connecting, bail out
	if !m.Connections.StartConnecting(peer.Name, peer.MeshIP, cancel) {
		log.Debug().Str("peer", peer.Name).Msg("connection attempt already in progress")
		setupSpan.SetStatus(otelcodes.Ok, "already connecting")
		return
	}
	defer m.Connections.ClearConnecting(peer.Name)

	// Check if transport negotiator is available
	if m.TransportNegotiator == nil {
		log.Debug().Str("peer", peer.Name).Msg("no transport negotiator available, skipping connection attempt")
		setupSpan.SetStatus(otelcodes.Ok, "no transport negotiator")
		return
	}

	// Build peer info for negotiation
	peerInfo := m.buildTransportPeerInfo(peer)

	// Log detailed peer info for debugging
	log.Debug().
		Str("peer", peer.Name).
		Strs("public_ips", peerInfo.PublicIPs).
		Strs("private_ips", peerInfo.PrivateIPs).
		Int("ssh_port", peerInfo.SSHPort).
		Int("udp_port", peerInfo.UDPPort).
		Bool("connectable", peerInfo.Connectable).
		Msg("attempting to establish tunnel")

	// Set up dial options
	dialOpts := transport.DialOptions{
		LocalName: m.identity.Name,
		PeerName:  peer.Name,
		ServerURL: m.client.BaseURL(),
		JWTToken:  m.client.JWTToken(),
	}

	// Try to negotiate a connection using the transport layer
	result, err := m.TransportNegotiator.Negotiate(connCtx, peerInfo, dialOpts)
	if err != nil {
		// Check if we were cancelled due to inbound connection
		if connCtx.Err() == context.Canceled {
			log.Debug().Str("peer", peer.Name).Msg("outbound connection cancelled (inbound connection established)")
			setupSpan.SetStatus(otelcodes.Ok, "cancelled by inbound connection")
			return
		}
		setupSpan.RecordError(err)
		setupSpan.SetStatus(otelcodes.Error, err.Error())
		sc := setupSpan.SpanContext()
		if m.OnConnectionSetup != nil {
			m.OnConnectionSetup("unknown", "failure", time.Since(setupStart).Seconds(), sc.TraceID().String())
		}
		log.Warn().Err(err).
			Str("peer", peer.Name).
			Str("trace_id", sc.TraceID().String()).
			Msg("transport negotiation failed")
		return
	}

	// Check if a tunnel was established while we were negotiating (by the other peer)
	if _, exists := m.tunnelMgr.Get(peer.Name); exists {
		log.Debug().Str("peer", peer.Name).Msg("tunnel already established by peer, closing our connection")
		_ = result.Connection.Close()
		setupSpan.SetStatus(otelcodes.Ok, "tunnel already established by peer")
		return
	}

	// Wrap the transport.Connection in a tunnel adapter
	tun := tunnel.NewConnectionAdapter(result.Connection, peer.Name)

	// Transition to Connected state (this adds tunnel via LifecycleManager observer)
	pc := m.Connections.Get(peer.Name)
	if pc == nil {
		log.Warn().Str("peer", peer.Name).Msg("peer connection not found after negotiation")
		_ = tun.Close()
		setupSpan.SetStatus(otelcodes.Ok, "peer connection not found")
		return
	}
	if err := pc.Connected(connCtx, tun, string(result.Transport), "transport negotiated: "+string(result.Transport)); err != nil {
		log.Warn().Err(err).Str("peer", peer.Name).Msg("failed to transition to connected state")
		_ = tun.Close()
		setupSpan.RecordError(err)
		setupSpan.SetStatus(otelcodes.Error, err.Error())
		return
	}

	// Record successful connection setup with exemplar linking to this trace.
	setupSpan.SetAttributes(attribute.String("transport", string(result.Transport)))
	setupSpan.SetStatus(otelcodes.Ok, "")
	sc := setupSpan.SpanContext()
	setupDuration := time.Since(setupStart).Seconds()
	if m.OnConnectionSetup != nil {
		m.OnConnectionSetup(string(result.Transport), "success", setupDuration, sc.TraceID().String())
	}

	log.Info().
		Str("peer", peer.Name).
		Str("transport", string(result.Transport)).
		Str("trace_id", sc.TraceID().String()).
		Str("span_id", sc.SpanID().String()).
		Float64("setup_duration_s", setupDuration).
		Msg("tunnel established via transport layer")

	// End the setup span now — HandleTunnel blocks for the entire tunnel lifetime
	// (hours or days), so defer would never fire while the peer is connected.
	// The defer above is still the safety net for all early-return error paths.
	setupSpan.End()

	// Handle incoming packets from this tunnel
	if m.Forwarder != nil {
		m.Forwarder.HandleTunnel(connCtx, peer.Name, tun)
	}

	// Disconnect when tunnel handler exits (this removes tunnel via LifecycleManager observer).
	// Use Background context — the connCtx is cancelled by this point.
	_ = pc.Disconnect(context.Background(), "tunnel handler exited", nil)
}

// buildTransportPeerInfo builds a transport.PeerInfo from a proto.Peer.
func (m *MeshNode) buildTransportPeerInfo(peer proto.Peer) *transport.PeerInfo {
	info := &transport.PeerInfo{
		Name:             peer.Name,
		PublicIPs:        peer.PublicIPs,
		PrivateIPs:       peer.PrivateIPs,
		SSHPort:          peer.SSHPort,
		UDPPort:          peer.UDPPort,
		Connectable:      peer.Connectable,
		BehindNAT:        !peer.Connectable,
		PublicKey:        peer.PublicKey,
		ExternalEndpoint: peer.ExternalEndpoint,
	}

	return info
}

// ConnectToPeerByName establishes a tunnel to a peer by name.
// This is used when we're notified that a peer wants to connect to us (e.g., hole-punch notification).
// Uses forced initiation to bypass pubkey comparison, as hole-punch requires both sides to send.
// Uses cached peer info if available, falls back to API if not in cache.
func (m *MeshNode) ConnectToPeerByName(ctx context.Context, peerName string) {
	// Clear pre-registered outbound intent when function exits.
	// Note: initiateHandshake will re-register with the real index, and clean up on completion.
	// This ensures the pre-registration from HandleHolePunchRequests doesn't leak if we bail early.
	defer m.ClearUDPOutbound(peerName)

	// Skip if tunnel already exists
	if _, exists := m.tunnelMgr.Get(peerName); exists {
		log.Debug().Str("peer", peerName).Msg("tunnel already exists, skipping connection attempt")
		return
	}

	// Try cache first (works during network transitions when API is unreachable)
	if cachedPeer, ok := m.GetCachedPeer(peerName); ok {
		log.Debug().Str("peer", peerName).Msg("using cached peer info for connection")
		m.EstablishTunnelForced(ctx, cachedPeer)
		return
	}

	// Fall back to API if not in cache
	peers, err := m.client.ListPeers()
	if err != nil {
		log.Warn().Err(err).Str("peer", peerName).Msg("failed to fetch peer info for connection (not in cache)")
		return
	}

	// Find the target peer and cache it
	var targetPeer *proto.Peer
	for _, p := range peers {
		m.CachePeer(p) // Cache all peers while we're at it
		if p.Name == peerName {
			targetPeer = &p
		}
	}

	if targetPeer == nil {
		log.Warn().Str("peer", peerName).Msg("peer not found on coordination server")
		return
	}

	// Establish tunnel with forced initiation (needed for hole-punch to work)
	m.EstablishTunnelForced(ctx, *targetPeer)
}

// RefreshAuthorizedKeys fetches peer keys from coordination server and adds them to SSH transport.
func (m *MeshNode) RefreshAuthorizedKeys() {
	if m.SSHTransport == nil {
		return
	}

	peers, err := m.client.ListPeers()
	if err != nil {
		log.Warn().Err(err).Msg("failed to refresh peer keys")
		return
	}

	// Build routes map for atomic update
	routes := make(map[string]string, len(peers))

	for _, peer := range peers {
		if peer.Name == m.identity.Name {
			continue // Skip self
		}
		if peer.PublicKey != "" {
			pubKey, err := config.DecodePublicKey(peer.PublicKey)
			if err != nil {
				log.Warn().Err(err).Str("peer", peer.Name).Msg("failed to decode peer public key")
			} else {
				m.SSHTransport.AddAuthorizedKey(pubKey)
			}
		}
		routes[peer.MeshIP] = peer.Name
		m.CachePeer(peer)
	}

	// Atomically update all routes
	m.router.UpdateRoutes(routes)
	log.Debug().Int("peers", len(peers)).Msg("refreshed authorized keys and routes")
}
