package coord

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// UDPEndpoint represents a peer's UDP endpoint information.
// Stores both IPv4 and IPv6 addresses separately for dual-stack support.
type UDPEndpoint struct {
	PeerName      string    `json:"peer_name"`
	LocalAddr     string    `json:"local_addr"`         // Local UDP address (e.g., "0.0.0.0:51820")
	ExternalAddr4 string    `json:"external_addr4"`     // IPv4 external address
	ExternalAddr6 string    `json:"external_addr6"`     // IPv6 external address
	LastSeen4     time.Time `json:"last_seen4"`         // Last time IPv4 was updated
	LastSeen6     time.Time `json:"last_seen6"`         // Last time IPv6 was updated
	NATType       string    `json:"nat_type,omitempty"` // "none", "full_cone", "restricted", "symmetric"
	PCPMapped     bool      `json:"pcp_mapped"`         // Whether endpoint has PCP/NAT-PMP mapping
}

// BestExternalAddr returns the best available external address, preferring IPv4.
func (ep *UDPEndpoint) BestExternalAddr() string {
	if ep.ExternalAddr4 != "" {
		return ep.ExternalAddr4
	}
	return ep.ExternalAddr6
}

// HolePunchRequest is sent by a peer to initiate hole-punching.
type HolePunchRequest struct {
	FromPeer     string `json:"from_peer"`
	ToPeer       string `json:"to_peer"`
	LocalAddr    string `json:"local_addr"`    // Peer's local UDP address
	ExternalAddr string `json:"external_addr"` // Peer's external address (if known)
}

// HolePunchResponse contains the target peer's endpoint information.
type HolePunchResponse struct {
	OK            bool   `json:"ok"`
	PeerAddr      string `json:"peer_addr,omitempty"`       // Target peer's external address
	PeerLocalAddr string `json:"peer_local_addr,omitempty"` // Target peer's local address
	Ready         bool   `json:"ready"`                     // Whether peer has registered
	Message       string `json:"message,omitempty"`
}

// RegisterUDPRequest is sent by a peer to register its UDP endpoint.
type RegisterUDPRequest struct {
	PeerName  string `json:"peer_name"`
	LocalAddr string `json:"local_addr"` // Local UDP listen address
	UDPPort   int    `json:"udp_port"`
	PCPMapped bool   `json:"pcp_mapped,omitempty"` // Whether endpoint has PCP/NAT-PMP mapping
}

// RegisterUDPResponse contains the discovered external address.
type RegisterUDPResponse struct {
	OK           bool   `json:"ok"`
	ExternalAddr string `json:"external_addr"` // Discovered external IP:port
	Message      string `json:"message,omitempty"`
}

// holePunchManager manages UDP endpoint registration and hole-punch coordination.
type holePunchManager struct {
	endpoints          map[string]*UDPEndpoint         // peer name -> endpoint
	pendingHolePunches map[string]map[string]time.Time // target peer -> (requesting peer -> request time)
	mu                 sync.RWMutex
}

func newHolePunchManager() *holePunchManager {
	return &holePunchManager{
		endpoints:          make(map[string]*UDPEndpoint),
		pendingHolePunches: make(map[string]map[string]time.Time),
	}
}

// RegisterEndpoint registers or updates a peer's UDP endpoint.
// The external address is stored in the appropriate field based on address family.
func (m *holePunchManager) RegisterEndpoint(peerName, localAddr, externalAddr string, pcpMapped bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// Get or create endpoint
	ep, ok := m.endpoints[peerName]
	if !ok {
		ep = &UDPEndpoint{
			PeerName: peerName,
		}
		m.endpoints[peerName] = ep
	}

	ep.LocalAddr = localAddr
	ep.PCPMapped = pcpMapped

	// Parse the external address to determine if it's IPv4 or IPv6
	host, _, err := net.SplitHostPort(externalAddr)
	if err != nil {
		// Try without port
		host = externalAddr
	}

	ip := net.ParseIP(host)
	if ip != nil {
		if ip.To4() != nil {
			// IPv4 address
			ep.ExternalAddr4 = externalAddr
			ep.LastSeen4 = now
			log.Debug().
				Str("peer", peerName).
				Str("external_ipv4", externalAddr).
				Bool("pcp_mapped", pcpMapped).
				Msg("UDP IPv4 endpoint registered")
		} else {
			// IPv6 address
			ep.ExternalAddr6 = externalAddr
			ep.LastSeen6 = now
			log.Debug().
				Str("peer", peerName).
				Str("external_ipv6", externalAddr).
				Bool("pcp_mapped", pcpMapped).
				Msg("UDP IPv6 endpoint registered")
		}
	} else {
		log.Debug().
			Str("peer", peerName).
			Str("external", externalAddr).
			Bool("pcp_mapped", pcpMapped).
			Msg("UDP endpoint registered (unknown address family)")
	}
}

// GetEndpoint returns a peer's UDP endpoint.
// Returns the endpoint if at least one address (IPv4 or IPv6) is still fresh.
func (m *holePunchManager) GetEndpoint(peerName string) (*UDPEndpoint, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ep, ok := m.endpoints[peerName]
	if !ok {
		return nil, false
	}

	// Check if at least one address is fresh (not older than 5 minutes)
	staleThreshold := 5 * time.Minute
	ipv4Fresh := ep.ExternalAddr4 != "" && time.Since(ep.LastSeen4) <= staleThreshold
	ipv6Fresh := ep.ExternalAddr6 != "" && time.Since(ep.LastSeen6) <= staleThreshold

	if !ipv4Fresh && !ipv6Fresh {
		return nil, false
	}

	return ep, true
}

// RemoveEndpoint removes a peer's UDP endpoint.
func (m *holePunchManager) RemoveEndpoint(peerName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.endpoints, peerName)
}

// CleanupStale removes stale endpoints and hole-punch requests.
func (m *holePunchManager) CleanupStale() {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-5 * time.Minute)
	for name, ep := range m.endpoints {
		// Clear stale individual addresses
		if ep.ExternalAddr4 != "" && ep.LastSeen4.Before(cutoff) {
			ep.ExternalAddr4 = ""
			log.Debug().Str("peer", name).Msg("cleared stale IPv4 UDP endpoint")
		}
		if ep.ExternalAddr6 != "" && ep.LastSeen6.Before(cutoff) {
			ep.ExternalAddr6 = ""
			log.Debug().Str("peer", name).Msg("cleared stale IPv6 UDP endpoint")
		}

		// Remove entire endpoint if both addresses are stale
		if ep.ExternalAddr4 == "" && ep.ExternalAddr6 == "" {
			delete(m.endpoints, name)
			log.Debug().Str("peer", name).Msg("removed stale UDP endpoint")
		}
	}

	// Clean up stale hole-punch requests (older than 30 seconds)
	holePunchCutoff := time.Now().Add(-30 * time.Second)
	for targetPeer, requests := range m.pendingHolePunches {
		for fromPeer, requestTime := range requests {
			if requestTime.Before(holePunchCutoff) {
				delete(requests, fromPeer)
			}
		}
		if len(requests) == 0 {
			delete(m.pendingHolePunches, targetPeer)
		}
	}
}

// RecordHolePunchRequest records that fromPeer wants to hole-punch to toPeer.
// This allows toPeer to be notified and initiate hole-punching back.
func (m *holePunchManager) RecordHolePunchRequest(fromPeer, toPeer string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pendingHolePunches[toPeer] == nil {
		m.pendingHolePunches[toPeer] = make(map[string]time.Time)
	}
	m.pendingHolePunches[toPeer][fromPeer] = time.Now()

	log.Debug().
		Str("from", fromPeer).
		Str("to", toPeer).
		Msg("recorded hole-punch request for notification")
}

// GetPendingHolePunches returns and clears the list of peers wanting to hole-punch
// with the given peer.
func (m *holePunchManager) GetPendingHolePunches(peerName string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	requests, ok := m.pendingHolePunches[peerName]
	if !ok || len(requests) == 0 {
		return nil
	}

	// Collect peer names and clear the pending list
	result := make([]string, 0, len(requests))
	for fromPeer := range requests {
		result = append(result, fromPeer)
	}
	delete(m.pendingHolePunches, peerName)

	return result
}

// setupHolePunchRoutes registers the hole-punch API routes (no goroutines).
func (s *Server) setupHolePunchRoutes() {
	s.holePunch = newHolePunchManager()
	s.mux.HandleFunc("/api/v1/udp/register", s.withAuth(s.handleUDPRegister))
	s.mux.HandleFunc("/api/v1/udp/holepunch", s.withAuth(s.handleHolePunch))
	s.mux.HandleFunc("/api/v1/udp/endpoint/", s.withAuth(s.handleGetEndpoint))
}

// startHolePunchCleanup starts the periodic stale-endpoint cleanup goroutine.
// Called from StartPeriodicCleanup where a context is available.
// An optional interval overrides the default 1-minute tick (useful in tests).
func (s *Server) startHolePunchCleanup(ctx context.Context, interval ...time.Duration) {
	d := time.Minute
	if len(interval) > 0 {
		d = interval[0]
	}
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.holePunch.CleanupStale()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// handleUDPRegister handles UDP endpoint registration.
// It discovers the peer's external address from the request.
func (s *Server) handleUDPRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RegisterUDPRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Discover external address from the request
	externalIP := getClientIP(r)

	// If the detected IP is localhost or a private IP (peer is on same machine
	// or connecting via VPC/internal network), use the peer's registered public IP
	if isLocalOrPrivateIP(externalIP) {
		s.peersMu.RLock()
		if info, ok := s.peers[req.PeerName]; ok && len(info.peer.PublicIPs) > 0 {
			externalIP = info.peer.PublicIPs[0]
		}
		s.peersMu.RUnlock()
	}

	externalAddr := net.JoinHostPort(externalIP, "0") // Port unknown at this point

	// If peer provided their local port, we can estimate external port
	// (though NAT may change it)
	if req.UDPPort > 0 {
		externalAddr = net.JoinHostPort(externalIP, strconv.Itoa(req.UDPPort))
	}

	s.holePunch.RegisterEndpoint(req.PeerName, req.LocalAddr, externalAddr, req.PCPMapped)

	// Also update the peer info with UDP port and PCP status
	s.peersMu.Lock()
	if info, ok := s.peers[req.PeerName]; ok {
		info.peer.UDPPort = req.UDPPort
		info.peer.PCPMapped = req.PCPMapped
	}
	s.peersMu.Unlock()

	log.Info().
		Str("peer", req.PeerName).
		Str("external_ip", externalIP).
		Int("udp_port", req.UDPPort).
		Bool("pcp_mapped", req.PCPMapped).
		Msg("UDP endpoint registered")

	resp := RegisterUDPResponse{
		OK:           true,
		ExternalAddr: externalAddr,
		Message:      "UDP endpoint registered",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleHolePunch handles hole-punch coordination requests.
func (s *Server) handleHolePunch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req HolePunchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Update the requester's endpoint
	externalIP := getClientIP(r)
	if req.ExternalAddr == "" {
		req.ExternalAddr = externalIP
	}
	// Note: HolePunch requests don't update PCPMapped status (keep existing)
	s.holePunch.RegisterEndpoint(req.FromPeer, req.LocalAddr, req.ExternalAddr, false)

	// Record this request so the target peer can be notified to hole-punch back
	if req.FromPeer != "" && req.ToPeer != "" {
		s.holePunch.RecordHolePunchRequest(req.FromPeer, req.ToPeer)

		// Push notification to target peer via WebSocket if connected
		s.relay.NotifyHolePunch(req.ToPeer, []string{req.FromPeer})
	}

	// Get target peer's endpoint
	targetEp, ok := s.holePunch.GetEndpoint(req.ToPeer)
	if !ok {
		// Target peer hasn't registered their UDP endpoint yet
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(HolePunchResponse{
			OK:      false,
			Ready:   false,
			Message: "target peer UDP endpoint not registered",
		})
		return
	}

	peerAddr := targetEp.BestExternalAddr()
	log.Debug().
		Str("from", req.FromPeer).
		Str("to", req.ToPeer).
		Str("target_addr", peerAddr).
		Msg("hole-punch coordination")

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(HolePunchResponse{
		OK:            true,
		PeerAddr:      peerAddr,
		PeerLocalAddr: targetEp.LocalAddr,
		Ready:         true,
	})
}

// handleGetEndpoint returns a specific peer's UDP endpoint.
func (s *Server) handleGetEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract peer name from path: /api/v1/udp/endpoint/{name}
	peerName := strings.TrimPrefix(r.URL.Path, "/api/v1/udp/endpoint/")
	if peerName == "" {
		s.jsonError(w, "peer name required", http.StatusBadRequest)
		return
	}

	ep, ok := s.holePunch.GetEndpoint(peerName)
	if !ok {
		s.jsonError(w, "endpoint not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ep)
}

// getClientIP extracts the client's IP address from the request.
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header (for proxies)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in the chain
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}

	// Fall back to RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// isLocalOrPrivateIP returns true if the IP is localhost or a private IP.
// Used to detect when peer is connecting via internal/VPC network.
func isLocalOrPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if ip.IsPrivate() {
		return true
	}
	return false
}
