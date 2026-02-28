package peer

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/transport"
	udptransport "github.com/tunnelmesh/tunnelmesh/internal/transport/udp"
)

// HandleIncomingUDP accepts incoming UDP connections from the listener.
func (m *MeshNode) HandleIncomingUDP(ctx context.Context, listener transport.Listener) {
	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error().Err(err).Msg("UDP accept error")
			continue
		}

		go m.handleIncomingConnection(ctx, conn, "UDP")
	}
}

// HandleUDPSessionInvalidated is called when a UDP session is invalidated by the remote peer
// (e.g., when we receive a rekey-required message). This removes the stale tunnel and
// triggers reconnection.
func (m *MeshNode) HandleUDPSessionInvalidated(peerName string) {
	log.Info().Str("peer", peerName).Msg("UDP session invalidated by peer, removing tunnel and triggering reconnection")

	// Disconnect the peer (removes tunnel via LifecycleManager observer).
	// This is called from the UDP receive loop — use Background context.
	pc := m.Connections.Get(peerName)
	if pc != nil {
		_ = pc.Disconnect(context.Background(), "session invalidated by peer", nil)
	}

	// Trigger peer discovery to reconnect
	m.TriggerDiscovery()
}

// SetupUDPSessionInvalidCallback wires up the UDP transport's session invalid callback
// to handle remote session invalidation (rekey-required messages).
// Also stores the UDP transport reference for crossing handshake pre-registration.
func (m *MeshNode) SetupUDPSessionInvalidCallback(udpTransport *udptransport.Transport) {
	m.UDPTransport = udpTransport
	udpTransport.SetSessionInvalidCallback(m.HandleUDPSessionInvalidated)
}

// PreRegisterUDPOutbound pre-registers intent to connect to a peer via UDP.
// This should be called BEFORE spawning the connection goroutine to ensure
// crossing handshake detection works correctly during hole-punching.
func (m *MeshNode) PreRegisterUDPOutbound(peerName string) {
	if m.UDPTransport != nil {
		m.UDPTransport.RegisterPendingOutbound(peerName)
	}
}

// ClearUDPOutbound clears the pre-registration for a peer.
// Called when the connection attempt completes (success or failure).
func (m *MeshNode) ClearUDPOutbound(peerName string) {
	if m.UDPTransport != nil {
		m.UDPTransport.ClearPendingOutbound(peerName)
	}
}
