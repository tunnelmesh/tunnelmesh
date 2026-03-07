package peer

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

// RunHeartbeat starts the heartbeat loop that maintains presence with the coordination server.
// Uses WebSocket-based heartbeat via PersistentRelay with push notifications for relay/hole-punch.
// Note: Push notification handlers (relay/hole-punch) are set in setupRelayHandlers to ensure
// they're re-registered after relay reconnection.
func (m *MeshNode) RunHeartbeat(ctx context.Context) {
	// Parse heartbeat interval from config (default: 10s)
	interval := 10 * time.Second
	if m.identity.Config != nil && m.identity.Config.HeartbeatInterval != "" {
		if parsed, err := time.ParseDuration(m.identity.Config.HeartbeatInterval); err == nil {
			interval = parsed
		}
	}

	log.Debug().Dur("interval", interval).Msg("starting heartbeat loop")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Perform initial heartbeat immediately
	m.PerformHeartbeat(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.PerformHeartbeat(ctx)
		}
	}
}

// PerformHeartbeat performs a single heartbeat cycle.
func (m *MeshNode) PerformHeartbeat(ctx context.Context) {
	// Check if IPs have changed
	publicIPs, privateIPs, behindNAT := m.identity.GetLocalIPs()

	if m.IPsChanged(publicIPs, privateIPs, behindNAT) {
		oldPub, oldPriv, _ := m.GetHeartbeatIPs()
		if oldPub != nil {
			// IPs changed - handle the change
			log.Info().
				Strs("old_public", oldPub).
				Strs("new_public", publicIPs).
				Strs("old_private", oldPriv).
				Strs("new_private", privateIPs).
				Msg("IP addresses changed, re-registering...")

			m.HandleIPChange(ctx, publicIPs, privateIPs, behindNAT)

			// Re-register with server (pass nil location to retain existing)
			hasMonitoring := m.identity.Config.Coordinator.Enabled &&
				(m.identity.Config.Coordinator.Monitoring.PrometheusURL != "" || m.identity.Config.Coordinator.Monitoring.GrafanaURL != "")
			if _, err := m.client.Register(
				m.identity.Name, m.identity.PubKeyEncoded,
				publicIPs, privateIPs, m.identity.SSHPort, m.identity.UDPPort, behindNAT, m.identity.Version, nil,
				m.identity.Config.ExitPeer, m.identity.Config.AllowExitTraffic, m.identity.Config.DNS.Aliases,
				m.identity.Config.Coordinator.Enabled, hasMonitoring,
			); err != nil {
				log.Error().Err(err).Msg("failed to re-register after IP change")
			} else {
				log.Info().Msg("re-registered with new IP addresses")
			}
		} else {
			// First heartbeat - just record the IPs
			m.SetHeartbeatIPs(publicIPs, privateIPs, behindNAT)
		}
	}

	// Collect stats
	stats := m.CollectStats()

	// Send heartbeat via WebSocket if PersistentRelay is connected
	if m.PersistentRelay != nil && m.PersistentRelay.IsConnected() {
		if err := m.PersistentRelay.SendHeartbeat(stats); err != nil {
			log.Debug().Err(err).Msg("WebSocket heartbeat failed, relay notifications may be delayed")
		}
	}

	// Sync DNS (still uses HTTP, but infrequent)
	m.syncDNS()
}

// HandleIPChange handles an IP address change by closing stale connections
// and triggering discovery.
func (m *MeshNode) HandleIPChange(ctx context.Context, publicIPs, privateIPs []string, behindNAT bool) {
	// Clear cached network state in transports (e.g., STUN-discovered addresses)
	if m.TransportRegistry != nil {
		m.TransportRegistry.ClearNetworkState()
	}

	// Close stale HTTP connections
	m.client.CloseIdleConnections()

	// Clear peer cache to prevent using stale IPs/endpoints for reconnection
	m.ClearPeerCache()

	// Disconnect all peers (tunnels may be using stale IPs)
	// Use DisconnectAll to properly transition FSM states and trigger observers
	m.Connections.DisconnectAll("IP change")
	// Also close any orphaned tunnels not tracked by FSM (belt and suspenders)
	m.tunnelMgr.CloseAll()
	log.Debug().Msg("disconnected all peers due to IP change")

	// Update stored IPs
	m.SetHeartbeatIPs(publicIPs, privateIPs, behindNAT)

	// Re-register UDP endpoint with new external address (non-blocking)
	// This ensures peers can reach us at our new address before discovery completes
	if m.TransportRegistry != nil {
		go m.TransportRegistry.RefreshEndpoints(ctx, m.identity.Name)
	}

	// Reconnect persistent relay (non-blocking)
	if m.PersistentRelay != nil {
		go m.ReconnectPersistentRelay(ctx)
	}

	// Trigger discovery
	m.TriggerDiscovery()
}

// HandleRelayRequests handles notifications that peers want to communicate via relay.
// The PersistentRelay already handles packet routing - this just cancels any direct
// connection attempts since the peer can only reach us via relay.
func (m *MeshNode) HandleRelayRequests(ctx context.Context, relayRequests []string) {
	if len(relayRequests) == 0 {
		return
	}

	for _, peerName := range relayRequests {
		log.Info().Str("peer", peerName).Msg("peer wants to communicate via relay")

		// Cancel any outbound connection attempt to this peer since they can only reach us via relay
		m.Connections.CancelOutbound(peerName)

		// Ensure route exists for this peer so forwarder can route packets via relay
		if peer, ok := m.GetCachedPeer(peerName); ok && peer.MeshIP != "" {
			m.router.AddRoute(peer.MeshIP, peerName)
		}
	}
}

// syncDNS syncs DNS records from the coordination server.
func (m *MeshNode) syncDNS() {
	if m.Resolver == nil {
		return
	}

	records, err := m.client.GetDNSRecords()
	if err != nil {
		log.Warn().Err(err).Msg("DNS sync failed")
		return
	}

	recordMap := make(map[string]string, len(records))
	for _, r := range records {
		recordMap[r.Hostname] = r.MeshIP
	}

	m.Resolver.UpdateRecords(recordMap)
}

// CheckAndHandleRelayRequests polls for relay requests and handles them.
func (m *MeshNode) CheckAndHandleRelayRequests(ctx context.Context) {
	relayRequests, err := m.client.CheckRelayRequests()
	if err != nil {
		log.Debug().Err(err).Msg("failed to check relay requests")
		return
	}
	m.HandleRelayRequests(ctx, relayRequests)
}

// HandleHolePunchRequests initiates hole-punching to peers that have requested it.
// When peer A tries to hole-punch to peer B, the server notifies B so it can
// simultaneously punch back to A, enabling NAT traversal.
func (m *MeshNode) HandleHolePunchRequests(ctx context.Context, holePunchRequests []string) {
	if len(holePunchRequests) == 0 {
		return
	}

	existingTunnels := m.tunnelMgr.List()
	existingSet := make(map[string]bool)
	for _, t := range existingTunnels {
		existingSet[t] = true
	}

	for _, peerName := range holePunchRequests {
		// Skip if we already have a tunnel to this peer
		if existingSet[peerName] {
			log.Debug().Str("peer", peerName).Msg("skipping hole-punch request, tunnel already exists")
			continue
		}

		// Skip if we're already connecting to this peer
		if m.Connections.IsConnecting(peerName) {
			log.Debug().Str("peer", peerName).Msg("skipping hole-punch request, already connecting")
			continue
		}

		log.Info().Str("peer", peerName).Msg("peer wants to hole-punch with us, initiating connection")

		// Pre-register outbound intent BEFORE spawning the goroutine.
		// This ensures crossing handshake detection works correctly even if
		// the other peer's init arrives before our goroutine starts.
		m.PreRegisterUDPOutbound(peerName)

		// Get the peer info and start a connection attempt
		// This will use the negotiator which will try UDP hole-punching
		go m.ConnectToPeerByName(ctx, peerName)
	}
}
