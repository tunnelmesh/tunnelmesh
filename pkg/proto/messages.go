// Package proto defines shared protocol messages for tunnelmesh.
package proto

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// GeoLocation represents geographic coordinates with source metadata.
type GeoLocation struct {
	Latitude  float64   `json:"latitude,omitempty"`
	Longitude float64   `json:"longitude,omitempty"`
	Accuracy  float64   `json:"accuracy,omitempty"`   // Accuracy in meters (IP ~50000, manual ~0)
	Source    string    `json:"source,omitempty"`     // "manual" or "ip"
	City      string    `json:"city,omitempty"`       // From IP lookup
	Region    string    `json:"region,omitempty"`     // From IP lookup
	Country   string    `json:"country,omitempty"`    // From IP lookup
	UpdatedAt time.Time `json:"updated_at,omitempty"` // When location was last updated
}

// Validate checks if the geolocation coordinates are within valid ranges.
func (g *GeoLocation) Validate() error {
	if g.Latitude < -90 || g.Latitude > 90 {
		return fmt.Errorf("latitude must be between -90 and 90, got %f", g.Latitude)
	}
	if g.Longitude < -180 || g.Longitude > 180 {
		return fmt.Errorf("longitude must be between -180 and 180, got %f", g.Longitude)
	}
	return nil
}

// IsSet returns true if the location has valid coordinates set.
// A location is considered set if it has a source (to distinguish 0,0 from unset).
func (g *GeoLocation) IsSet() bool {
	if g.Source != "" {
		return true
	}
	// If no source but both lat/long are non-zero, consider it set
	return g.Latitude != 0 && g.Longitude != 0
}

// Peer represents a node in the mesh network.
type Peer struct {
	Name              string       `json:"name"`
	PublicKey         string       `json:"public_key"`                    // SSH public key (base64 encoded wire format)
	PublicIPs         []string     `json:"public_ips"`                    // Externally reachable IPs
	PrivateIPs        []string     `json:"private_ips"`                   // Internal network IPs
	SSHPort           int          `json:"ssh_port"`                      // SSH server port
	UDPPort           int          `json:"udp_port,omitempty"`            // UDP transport port
	MeshIP            string       `json:"mesh_ip"`                       // Assigned mesh network IP (10.42.x.x)
	LastSeen          time.Time    `json:"last_seen"`                     // Last heartbeat time
	Connectable       bool         `json:"connectable"`                   // Can accept incoming connections
	BehindNAT         bool         `json:"behind_nat"`                    // Public IP was fetched externally (behind NAT)
	ExternalEndpoint  string       `json:"external_endpoint,omitempty"`   // STUN-discovered external address for UDP
	Version           string       `json:"version,omitempty"`             // Application version
	PCPMapped         bool         `json:"pcp_mapped,omitempty"`          // Whether peer has PCP/NAT-PMP port mapping
	Location          *GeoLocation `json:"location,omitempty"`            // Geographic location
	AllowsExitTraffic bool         `json:"allows_exit_traffic,omitempty"` // Can act as exit node for other peers
	ExitPeer          string       `json:"exit_node,omitempty"`           // Name of peer used as exit node
	IsCoordinator     bool         `json:"is_coordinator,omitempty"`      // True if peer is running coordinator services
}

// RegisterRequest is sent by a peer to join the mesh.
type RegisterRequest struct {
	Name              string       `json:"name"`
	PublicKey         string       `json:"public_key"`
	PublicIPs         []string     `json:"public_ips"`
	PrivateIPs        []string     `json:"private_ips"`
	SSHPort           int          `json:"ssh_port"`
	UDPPort           int          `json:"udp_port,omitempty"`            // UDP transport port
	BehindNAT         bool         `json:"behind_nat"`                    // True if public IP was fetched from external service
	Version           string       `json:"version,omitempty"`             // Application version
	Location          *GeoLocation `json:"location,omitempty"`            // Geographic location (manual config)
	AllowsExitTraffic bool         `json:"allows_exit_traffic,omitempty"` // Allow this node to act as exit node
	ExitPeer          string       `json:"exit_node,omitempty"`           // Name of peer to use as exit node
	Aliases           []string     `json:"aliases,omitempty"`             // Custom DNS aliases for this peer
	IsCoordinator     bool         `json:"is_coordinator,omitempty"`      // True if peer is running coordinator services
	HasMonitoring     bool         `json:"has_monitoring,omitempty"`      // True if coordinator has monitoring (Prometheus/Grafana) configured
}

// RegisterResponse is returned after successful registration.
type RegisterResponse struct {
	MeshIP           string            `json:"mesh_ip"`                     // Assigned mesh IP address
	MeshCIDR         string            `json:"mesh_cidr"`                   // Full mesh CIDR for routing
	Domain           string            `json:"domain"`                      // Domain suffix (e.g., ".tunnelmesh")
	Token            string            `json:"token"`                       // JWT token for relay authentication
	TLSCert          string            `json:"tls_cert,omitempty"`          // PEM-encoded TLS certificate signed by mesh CA
	TLSKey           string            `json:"tls_key,omitempty"`           // PEM-encoded TLS private key
	CoordMeshIPs     []string          `json:"coord_mesh_ips,omitempty"`    // All coordinator mesh IPs for DNS round-robin
	ServerVersion    string            `json:"server_version,omitempty"`    // Server version for compatibility check
	PeerName         string            `json:"peer_name,omitempty"`         // Assigned peer name (may differ from request if renamed)
	PeerID           string            `json:"peer_id,omitempty"`           // Derived peer ID for RBAC (SHA256(pubkey)[:8] hex)
	IsAdmin          bool              `json:"is_admin,omitempty"`          // True if peer is in admin_peers list (coordinator services enabled)
	Coordinators     []string          `json:"coordinators,omitempty"`      // List of coordinator mesh IPs for immediate replication setup
	CoordinatorNames map[string]string `json:"coordinator_names,omitempty"` // Map of mesh IP → coordinator name for replication ownership
}

// PeerStats contains traffic statistics reported by peers.
// Stats are now sent via WebSocket heartbeat (see internal/tunnel/persistent_relay.go).
type PeerStats struct {
	PacketsSent     uint64            `json:"packets_sent"`
	PacketsReceived uint64            `json:"packets_received"`
	BytesSent       uint64            `json:"bytes_sent"`
	BytesReceived   uint64            `json:"bytes_received"`
	DroppedNoRoute  uint64            `json:"dropped_no_route"`
	DroppedNoTunnel uint64            `json:"dropped_no_tunnel"`
	Errors          uint64            `json:"errors"`
	ActiveTunnels   int               `json:"active_tunnels"`
	Location        *GeoLocation      `json:"location,omitempty"`    // Geographic location (sent with every heartbeat)
	Connections     map[string]string `json:"connections,omitempty"` // Active connections: peerName -> transport type ("ssh", "udp", "relay")

	// Latency metrics
	HeartbeatSentAt  int64            `json:"heartbeat_sent_at,omitempty"`  // Unix nano timestamp when heartbeat was sent
	CoordinatorRTTMs int64            `json:"coordinator_rtt_ms,omitempty"` // Last measured RTT to coordinator in milliseconds
	PeerLatencies    map[string]int64 `json:"peer_latencies,omitempty"`     // Peer name -> latency in microseconds
}

// Note: HeartbeatRequest and HeartbeatResponse removed.
// Heartbeats are now sent via WebSocket using binary message format.
// See internal/tunnel/persistent_relay.go (MsgTypeHeartbeat) and
// internal/coord/relay.go for implementation.
// Relay and hole-punch notifications are pushed instantly via WebSocket
// (MsgTypeRelayNotify, MsgTypeHolePunchNotify).

// RelayStatusResponse contains pending relay requests for a peer.
type RelayStatusResponse struct {
	RelayRequests []string `json:"relay_requests"`
}

// PeerListResponse contains the list of all mesh peers.
type PeerListResponse struct {
	Peers []Peer `json:"peers"`
}

// DNSRecord represents a hostname to IP mapping.
type DNSRecord struct {
	Hostname string `json:"hostname"`
	MeshIP   string `json:"mesh_ip"`
}

// DNSUpdateNotification is sent when DNS records change.
type DNSUpdateNotification struct {
	Records []DNSRecord `json:"records"`
}

// ConnectionHint suggests how to connect to a peer.
type ConnectionHint struct {
	PeerName   string `json:"peer_name"`
	Strategy   string `json:"strategy"`    // "direct_public", "direct_private", "reverse"
	Address    string `json:"address"`     // IP:port to connect to
	RequireNAT bool   `json:"require_nat"` // True if NAT traversal needed
}

// ErrorResponse represents an API error.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// GetLocalIPs returns the local IP addresses of the machine.
// The behindNAT return value is true if the public IP was fetched from an external service.
func GetLocalIPs() (public []string, private []string, behindNAT bool) {
	return GetLocalIPsExcluding("")
}

// GetLocalIPsExcluding returns the local IP addresses, excluding IPs in the given CIDR.
// This is useful for excluding mesh network IPs from the advertised addresses.
// The behindNAT return value is true if the public IP was fetched from an external service.
func GetLocalIPsExcluding(excludeCIDR string) (public []string, private []string, behindNAT bool) {
	var excludeNet *net.IPNet
	if excludeCIDR != "" {
		_, excludeNet, _ = net.ParseCIDR(excludeCIDR)
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, nil, false
	}

	for _, iface := range interfaces {
		// Skip loopback and down interfaces
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		// Skip TUN/TAP interfaces (mesh network interfaces)
		// On macOS: utun*, tun*
		// On Linux: tun*, tap*
		ifName := iface.Name
		if strings.HasPrefix(ifName, "utun") || strings.HasPrefix(ifName, "tun") || strings.HasPrefix(ifName, "tap") {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			// Only consider IPv4 for now
			ip4 := ip.To4()
			if ip4 == nil {
				continue
			}

			// Skip IPs in the excluded CIDR (e.g., mesh network)
			if excludeNet != nil && excludeNet.Contains(ip4) {
				continue
			}

			ipStr := ip4.String()
			if isPrivateIP(ip4) {
				private = append(private, ipStr)
			} else {
				public = append(public, ipStr)
			}
		}
	}

	// If no public IPs found locally (behind NAT), try to get external IP
	if len(public) == 0 {
		if externalIP := GetExternalIP(); externalIP != "" {
			public = append(public, externalIP)
			behindNAT = true
		}
	}

	return public, private, behindNAT
}

// isPrivateIP checks if an IP is in a private range.
func isPrivateIP(ip net.IP) bool {
	private := []struct {
		start net.IP
		end   net.IP
	}{
		{net.IPv4(10, 0, 0, 0), net.IPv4(10, 255, 255, 255)},
		{net.IPv4(172, 16, 0, 0), net.IPv4(172, 31, 255, 255)},
		{net.IPv4(192, 168, 0, 0), net.IPv4(192, 168, 255, 255)},
	}

	for _, r := range private {
		if bytesGreaterOrEqual(ip, r.start) && bytesGreaterOrEqual(r.end, ip) {
			return true
		}
	}
	return false
}

func bytesGreaterOrEqual(a, b net.IP) bool {
	a = a.To4()
	b = b.To4()
	if a == nil || b == nil {
		return false
	}
	for i := 0; i < 4; i++ {
		if a[i] > b[i] {
			return true
		}
		if a[i] < b[i] {
			return false
		}
	}
	return true
}

// publicIPServices is a list of services that return the public IP as plain text.
var publicIPServices = []string{
	"https://ifconfig.me/ip",
	"https://ipinfo.io/ip",
	"https://api.ipify.org",
	"https://icanhazip.com",
}

// GetExternalIP fetches the public IP address from an external service.
// This is useful when behind NAT where local interfaces don't show the public IP.
// Returns empty string if the public IP cannot be determined.
func GetExternalIP() string {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	for _, service := range publicIPServices {
		resp, err := client.Get(service)
		if err != nil {
			continue
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			continue
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
		if err != nil {
			continue
		}

		ip := strings.TrimSpace(string(body))
		// Validate it's a valid IPv4 address
		if parsed := net.ParseIP(ip); parsed != nil && parsed.To4() != nil {
			return ip
		}
	}

	return ""
}
