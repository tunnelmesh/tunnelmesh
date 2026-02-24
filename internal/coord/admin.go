package coord

import (
	"bytes"
	"io/fs"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"encoding/json"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/web"
	"github.com/tunnelmesh/tunnelmesh/internal/mesh"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
)

// adminStatusRecorder wraps http.ResponseWriter to capture the status code for metrics.
type adminStatusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *adminStatusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
		r.ResponseWriter.WriteHeader(code)
	}
}

func (r *adminStatusRecorder) getStatus() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// withS3AdminMetrics wraps an admin S3 handler with metrics instrumentation.
// Uses the shared S3 metrics singleton so admin API requests appear in the same
// Prometheus metrics as direct S3 API requests.
func (s *Server) withS3AdminMetrics(w http.ResponseWriter, operation string, fn func(http.ResponseWriter)) {
	m := s3.GetS3Metrics()
	startTime := time.Now()
	rec := &adminStatusRecorder{ResponseWriter: w}
	defer func() {
		if m != nil {
			duration := time.Since(startTime).Seconds()
			status := s3.ClassifyS3Status(rec.getStatus())
			m.RecordRequest(operation, status, duration)
		}
	}()
	fn(rec)
}

// redirectToCanonicalDomain returns middleware that redirects .tm and .mesh requests
// to the canonical .tunnelmesh domain.
func redirectToCanonicalDomain(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		// Strip port if present
		if colonIdx := strings.LastIndex(host, ":"); colonIdx != -1 {
			host = host[:colonIdx]
		}

		// Check if using an alias suffix
		for _, alias := range []string{mesh.AliasTM, mesh.AliasMesh} {
			if strings.HasSuffix(host, alias) {
				// Redirect to canonical domain
				canonical := strings.TrimSuffix(host, alias) + mesh.DomainSuffix
				scheme := "https"
				if r.TLS == nil {
					scheme = "http"
				}
				targetURL := scheme + "://" + canonical + r.URL.RequestURI()
				http.Redirect(w, r, targetURL, http.StatusMovedPermanently)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// AdminOverview is the response for the admin overview endpoint.
type AdminOverview struct {
	ServerUptime     string          `json:"server_uptime"`
	ServerVersion    string          `json:"server_version"`
	TotalPeers       int             `json:"total_peers"`
	OnlinePeers      int             `json:"online_peers"`
	TotalHeartbeats  uint64          `json:"total_heartbeats"`
	MeshCIDR         string          `json:"mesh_cidr"`
	DomainSuffix     string          `json:"domain_suffix"`
	LocationsEnabled bool            `json:"locations_enabled"` // Whether node location tracking is enabled
	Peers            []AdminPeerInfo `json:"peers"`
}

// AdminPeerInfo contains peer information for the admin UI.
type AdminPeerInfo struct {
	Name                string             `json:"name"`
	MeshIP              string             `json:"mesh_ip"`
	PublicIPs           []string           `json:"public_ips"`
	PrivateIPs          []string           `json:"private_ips"`
	SSHPort             int                `json:"ssh_port"`
	UDPPort             int                `json:"udp_port"`
	UDPExternalAddr4    string             `json:"udp_external_addr4,omitempty"`
	UDPExternalAddr6    string             `json:"udp_external_addr6,omitempty"`
	LastSeen            time.Time          `json:"last_seen"`
	Online              bool               `json:"online"`
	Connectable         bool               `json:"connectable"`
	BehindNAT           bool               `json:"behind_nat"`
	RegisteredAt        time.Time          `json:"registered_at"`
	HeartbeatCount      uint64             `json:"heartbeat_count"`
	Stats               *proto.PeerStats   `json:"stats,omitempty"`
	BytesSentRate       float64            `json:"bytes_sent_rate"`
	BytesReceivedRate   float64            `json:"bytes_received_rate"`
	PacketsSentRate     float64            `json:"packets_sent_rate"`
	PacketsReceivedRate float64            `json:"packets_received_rate"`
	Version             string             `json:"version,omitempty"`
	Location            *proto.GeoLocation `json:"location,omitempty"`
	// Exit node info
	AllowsExitTraffic bool     `json:"allows_exit_traffic,omitempty"`
	ExitPeer          string   `json:"exit_node,omitempty"`
	ExitClients       []string `json:"exit_clients,omitempty"`
	// Connection info (peer -> transport type)
	Connections map[string]string `json:"connections,omitempty"`
	// DNS aliases for this peer
	Aliases []string `json:"aliases,omitempty"`
	// Latency metrics
	CoordinatorRTTMs int64            `json:"coordinator_rtt_ms,omitempty"`
	PeerLatencies    map[string]int64 `json:"peer_latencies,omitempty"`
}

// handleAdminOverview returns the admin overview data.
//
// nolint:gocyclo // Admin overview aggregates data from multiple sources
func (s *Server) handleAdminOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.peersMu.RLock()
	defer s.peersMu.RUnlock()

	now := time.Now()
	onlineThreshold := 2 * time.Minute

	overview := AdminOverview{
		ServerUptime:     time.Since(s.serverStats.startTime).Round(time.Second).String(),
		ServerVersion:    s.version,
		TotalPeers:       len(s.peers),
		TotalHeartbeats:  s.serverStats.totalHeartbeats,
		MeshCIDR:         mesh.CIDR,
		DomainSuffix:     mesh.DomainSuffix,
		LocationsEnabled: s.cfg.Coordinator.Locations,
		Peers:            make([]AdminPeerInfo, 0, len(s.peers)),
	}

	// Build exit client map (which clients use which exit node)
	exitClients := make(map[string][]string) // exitNodeName -> [clientNames]
	for _, info := range s.peers {
		if info.peer.ExitPeer != "" {
			exitClients[info.peer.ExitPeer] = append(exitClients[info.peer.ExitPeer], info.peer.Name)
		}
	}

	for _, info := range s.peers {
		online := now.Sub(info.peer.LastSeen) < onlineThreshold
		if online {
			overview.OnlinePeers++
		}

		peerInfo := AdminPeerInfo{
			Name:              info.peer.Name,
			MeshIP:            info.peer.MeshIP,
			PublicIPs:         info.peer.PublicIPs,
			PrivateIPs:        info.peer.PrivateIPs,
			SSHPort:           info.peer.SSHPort,
			UDPPort:           info.peer.UDPPort,
			LastSeen:          info.peer.LastSeen,
			Online:            online,
			Connectable:       info.peer.Connectable,
			BehindNAT:         info.peer.BehindNAT,
			RegisteredAt:      info.registeredAt,
			HeartbeatCount:    info.heartbeatCount,
			Stats:             info.stats,
			Version:           info.peer.Version,
			AllowsExitTraffic: info.peer.AllowsExitTraffic,
			ExitPeer:          info.peer.ExitPeer,
			Aliases:           info.aliases,
		}

		// Include exit clients if this peer allows exit traffic
		if info.peer.AllowsExitTraffic {
			peerInfo.ExitClients = exitClients[info.peer.Name]
		}

		// Include connection types from stats (peer -> transport type)
		if info.stats != nil && len(info.stats.Connections) > 0 {
			peerInfo.Connections = info.stats.Connections
		}

		// Include latency metrics
		peerInfo.CoordinatorRTTMs = info.coordinatorRTT
		if len(info.peerLatencies) > 0 {
			peerInfo.PeerLatencies = info.peerLatencies
		}

		// Only include location if the feature is enabled
		if s.cfg.Coordinator.Locations {
			peerInfo.Location = info.peer.Location
		}

		// Get UDP endpoint addresses if available
		if s.holePunch != nil {
			if ep, ok := s.holePunch.GetEndpoint(info.peer.Name); ok {
				peerInfo.UDPExternalAddr4 = ep.ExternalAddr4
				peerInfo.UDPExternalAddr6 = ep.ExternalAddr6
			}
		}

		// Calculate rates if we have previous stats
		if info.prevStats != nil && info.stats != nil && !info.lastStatsTime.IsZero() {
			// Use actual time delta for rate calculation (more accurate than fixed interval)
			delta := info.lastStatsTime.Sub(info.prevStatsTime).Seconds()
			if delta > 0 {
				peerInfo.BytesSentRate = float64(info.stats.BytesSent-info.prevStats.BytesSent) / delta
				peerInfo.BytesReceivedRate = float64(info.stats.BytesReceived-info.prevStats.BytesReceived) / delta
				peerInfo.PacketsSentRate = float64(info.stats.PacketsSent-info.prevStats.PacketsSent) / delta
				peerInfo.PacketsReceivedRate = float64(info.stats.PacketsReceived-info.prevStats.PacketsReceived) / delta
			}
		}

		overview.Peers = append(overview.Peers, peerInfo)
	}

	// Sort peers by mesh IP for consistent ordering
	sort.Slice(overview.Peers, func(i, j int) bool {
		ipI := net.ParseIP(overview.Peers[i].MeshIP)
		ipJ := net.ParseIP(overview.Peers[j].MeshIP)
		if ipI == nil || ipJ == nil {
			return overview.Peers[i].MeshIP < overview.Peers[j].MeshIP
		}
		return bytes.Compare(ipI.To16(), ipJ.To16()) < 0
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(overview)
}

// setupAdminRoutes registers the admin API routes and static file server.
// Note: Admin routes have no authentication - access is controlled by network binding.
// Admin is only accessible from inside the mesh via HTTPS on mesh IP (https://this.tunnelmesh/)
func (s *Server) setupAdminRoutes() {
	// Serve embedded static files
	staticFS, _ := fs.Sub(web.Assets, ".")
	fileServer := http.FileServer(http.FS(staticFS))

	// Create separate adminMux for HTTPS server on mesh IP
	// Serve at root - dedicated server doesn't need /admin/ prefix
	s.adminMux = http.NewServeMux()

	s.adminMux.HandleFunc("/api/overview", s.handleAdminOverview)
	s.adminMux.HandleFunc("/api/events", s.handleSSE)

	// Filter rule management
	s.adminMux.HandleFunc("/api/filter/rules", s.handleFilterRules)

	// Group management API
	s.adminMux.HandleFunc("/api/groups", s.handleGroups)
	s.adminMux.HandleFunc("/api/groups/", s.handleGroupByName)

	// File share management API
	s.adminMux.HandleFunc("/api/shares", s.handleShares)
	s.adminMux.HandleFunc("/api/shares/", s.handleShareByName)

	// Peer management API
	s.adminMux.HandleFunc("/api/users", s.handlePeersMgmt)

	// Role binding management API
	s.adminMux.HandleFunc("/api/bindings", s.handleBindings)
	s.adminMux.HandleFunc("/api/bindings/", s.handleBindingByName)

	// DNS records
	s.adminMux.HandleFunc("/api/dns", s.handleDNS)

	// S3 bucket management API (specific routes before proxy catch-all)
	s.adminMux.HandleFunc("/api/s3/buckets", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.withS3AdminMetrics(w, "listBuckets", func(w http.ResponseWriter) {
				s.handleListBuckets(w, r)
			})
		case http.MethodPost:
			s.withS3AdminMetrics(w, "createBucket", func(w http.ResponseWriter) {
				s.handleCreateBucket(w, r)
			})
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	s.adminMux.HandleFunc("/api/s3/buckets/", func(w http.ResponseWriter, r *http.Request) {
		// Extract bucket name from path
		path := strings.TrimPrefix(r.URL.Path, "/api/s3/buckets/")
		path = strings.TrimSuffix(path, "/")

		// Handle per-bucket recycle bin purge: DELETE /api/s3/buckets/{bucket}/recyclebin
		// Only intercept DELETE; GET (list) and GET /{key} (content) are handled by handleS3Proxy.
		if strings.HasSuffix(path, "/recyclebin") && r.Method == http.MethodDelete {
			bucket := strings.TrimSuffix(path, "/recyclebin")
			s.withS3AdminMetrics(w, "purgeRecycleBin", func(w http.ResponseWriter) {
				s.handlePurgeBucketRecycleBin(w, r, bucket)
			})
			return
		}

		// If path contains additional segments (e.g., "mybucket/objects"),
		// delegate to S3 proxy handler instead of bucket management
		if strings.Contains(path, "/") {
			s.handleS3Proxy(w, r)
			return
		}

		bucket := path

		switch r.Method {
		case http.MethodGet:
			s.withS3AdminMetrics(w, "getBucket", func(w http.ResponseWriter) {
				s.handleGetBucket(w, r, bucket)
			})
		case http.MethodPatch:
			s.withS3AdminMetrics(w, "updateBucket", func(w http.ResponseWriter) {
				s.handleUpdateBucket(w, r, bucket)
			})
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// S3 garbage collection (on-demand)
	s.adminMux.HandleFunc("/api/s3/gc", s.handleS3GC)

	// S3 proxy for explorer
	s.adminMux.HandleFunc("/api/s3/", s.handleS3Proxy)

	// Docker orchestration API (when coordinator has Docker enabled via join_mesh)
	s.adminMux.HandleFunc("/api/docker/containers", s.handleDockerContainers)
	s.adminMux.HandleFunc("/api/docker/containers/", func(w http.ResponseWriter, r *http.Request) {
		// Route to inspect or control based on path suffix
		if strings.HasSuffix(r.URL.Path, "/control") {
			s.handleDockerControl(w, r)
		} else {
			s.handleDockerContainerInspect(w, r)
		}
	})

	// Panel management API
	s.adminMux.HandleFunc("/api/panels", s.handlePanels)
	s.adminMux.HandleFunc("/api/panels/", s.handlePanelByID)
	s.adminMux.HandleFunc("/api/user/permissions", s.handlePeerPermissions)

	// System health check
	s.adminMux.HandleFunc("/api/system/health", s.handleSystemHealth)

	// Coordinator replication endpoint (mesh-only, used by other coordinators)
	// This MUST be on adminMux (not public mux) to ensure replication only happens within the mesh
	if s.replicator != nil {
		s.adminMux.HandleFunc("/api/replication/message", s.handleReplicationMessage)
	}

	// Relay endpoints on admin mux (mesh-only heartbeats and relay fallback)
	// Also registered on public mux for initial connections before mesh join.
	s.adminMux.HandleFunc("/api/v1/relay/persistent", s.handlePersistentRelay)
	s.adminMux.HandleFunc("/api/v1/relay/", s.handleRelay)
	s.adminMux.HandleFunc("/api/v1/relay-status", s.handleRelayStatus)

	// Expose metrics on admin interface for Prometheus scraping via mesh IP.
	// Uses a lazy handler because metricsRegistry is nil at setup time and
	// only set later via SetMetricsRegistry() after mesh join.
	s.adminMux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if gatherer, ok := s.metricsRegistry.(prometheus.Gatherer); ok {
			promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}).ServeHTTP(w, r)
		} else {
			promhttp.Handler().ServeHTTP(w, r)
		}
	})
	// Expose debug trace endpoint on admin interface only (mesh-only access)
	s.adminMux.HandleFunc("/debug/trace", s.handleTrace)

	// Peer site hosting (serves files from peer shares as web pages)
	s.adminMux.HandleFunc("/peers/", s.handlePeerSite)

	// Serve admin dashboard at /admin/ prefix
	s.adminMux.Handle("/admin/", http.StripPrefix("/admin/", fileServer))

	// Landing page at root, 404 for unknown paths
	s.adminMux.HandleFunc("/", s.handleLanding)
}

// handleLanding serves the landing page at "/" or returns 404 for unknown paths.
// If a custom landing page is configured via LandingPage config, it serves that file.
// Otherwise, it serves a built-in default landing page with a redirect to /admin/.
func (s *Server) handleLanding(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	// Custom landing page from config
	if s.cfg.Coordinator.LandingPage != "" {
		data, err := os.ReadFile(s.cfg.Coordinator.LandingPage)
		if err != nil {
			log.Error().Err(err).Str("path", s.cfg.Coordinator.LandingPage).Msg("failed to read custom landing page")
			if os.IsNotExist(err) {
				http.Error(w, "landing page not found", http.StatusNotFound)
			} else {
				http.Error(w, "failed to read landing page", http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
		return
	}

	// Default built-in landing page
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(defaultLandingPage))
}

const defaultLandingPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="5;url=/admin/">
<title>tunnelmesh</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#c9d1d9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;overflow:hidden}
.container{text-align:center;z-index:2;position:relative}
h1{font-size:2.4rem;font-weight:600;color:#58a6ff;margin-bottom:.5rem;letter-spacing:-.02em}
p{font-size:1.1rem;color:#8b949e;margin-bottom:2rem}
a{color:#58a6ff;text-decoration:none;border:1px solid #30363d;padding:.6rem 1.6rem;font-size:.95rem;transition:all .2s}
a:hover{background:#58a6ff;color:#0d1117;border-color:#58a6ff}
.meta{margin-top:2rem;font-size:.8rem;color:#484f58}
.grid{position:fixed;top:0;left:0;width:100%;height:100%;z-index:1;opacity:.08}
.grid svg{width:100%;height:100%}
</style>
</head>
<body>
<div class="grid"><svg xmlns="http://www.w3.org/2000/svg" width="100%" height="100%"><defs><pattern id="g" width="40" height="40" patternUnits="userSpaceOnUse"><path d="M40 0H0v40" fill="none" stroke="#58a6ff" stroke-width=".5"/></pattern></defs><rect width="100%" height="100%" fill="url(#g)"/></svg></div>
<div class="container">
<h1>tunnelmesh</h1>
<p>Welcome to your secured mesh network</p>
<a href="/admin/">Enter Dashboard</a>
<div class="meta">Redirecting in 5 seconds&hellip;</div>
</div>
</body>
</html>`
