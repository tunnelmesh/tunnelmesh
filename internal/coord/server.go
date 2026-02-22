// Package coord implements the coordination server for tunnelmesh.
package coord

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/nfs"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/replication"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/wireguard"
	"github.com/tunnelmesh/tunnelmesh/internal/docker"
	"github.com/tunnelmesh/tunnelmesh/internal/mesh"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
)

// peerInfo wraps a peer with stats and metadata for admin UI.
type peerInfo struct {
	peer           *proto.Peer
	stats          *proto.PeerStats
	prevStats      *proto.PeerStats
	heartbeatCount uint64
	registeredAt   time.Time
	lastStatsTime  time.Time
	prevStatsTime  time.Time
	aliases        []string // DNS aliases registered by this peer
	peerID         string   // Peer ID derived from peer's public key (SHA256[:8] hex)
	hasMonitoring  bool     // True if coordinator has monitoring (Prometheus/Grafana) configured

	// Latency metrics reported by peer
	coordinatorRTT int64            // Peer's reported RTT to coordinator (ms)
	peerLatencies  map[string]int64 // Peer's reported latencies to other peers (ms)
}

// serverStats tracks server-level statistics.
type serverStats struct {
	startTime       time.Time
	totalHeartbeats uint64
}

// Server is the coordination server that manages peer registration and discovery.
//
// The server uses two separate HTTP muxes for security isolation:
//   - mux (port 8080): Public API for peer registration, exposed to internet
//   - adminMux (port 443 on mesh IP): Private admin interface, only accessible from within mesh
//
// This separation ensures the admin interface (dashboards, monitoring, config) is never
// exposed to the public internet, while the coordination API remains accessible for peers.
type Server struct {
	cancel             context.CancelFunc // Cancel function for server lifecycle (stops background operations)
	wg                 sync.WaitGroup     // Tracks background goroutines for clean shutdown
	cfg                *config.PeerConfig
	mux                *http.ServeMux // Public coordination API (peer registration, relay/heartbeats)
	adminMux           *http.ServeMux // Private admin interface (dashboards, monitoring, relay/heartbeats) - mesh-only
	adminServer        *http.Server   // HTTPS server for adminMux, bound to mesh IP only
	peers              map[string]*peerInfo
	coordinators       map[string]*peerInfo // Subset of peers that are coordinators, for O(1) lookups
	peersMu            sync.RWMutex
	ipAlloc            *ipAllocator
	dnsCache           map[string]string // hostname -> mesh IP
	aliasOwner         map[string]string // alias -> peer name (reverse lookup for ownership)
	serverStats        serverStats
	relay              *relayManager
	holePunch          *holePunchManager
	wgStore            *wireguard.Store             // WireGuard client storage
	ca                 *CertificateAuthority        // Internal CA for mesh TLS certs
	version            string                       // Server version for admin display
	sseHub             *sseHub                      // SSE hub for real-time dashboard updates
	ipGeoCache         *IPGeoCache                  // IP geolocation cache for location fallback
	coordIPs           atomic.Pointer[coordIPSet]   // Coordinator mesh IPs (original + sorted) for DNS and write forwarding
	s3ForwardTransport *http.Transport              // Shared transport for S3 write forwarding (reuses connections)
	peerListings       atomic.Pointer[peerListings] // Pre-merged peer listing data (from system store)
	localListingIndex  atomic.Pointer[listingIndex] // This coordinator's own listing index (in-memory)
	listingIndexDirty  atomic.Bool                  // Whether local index needs persisting
	listingIndexNotify chan struct{}                // Signal for immediate persist+load cycle
	coordMetrics       *CoordMetrics                // Prometheus metrics for coordinator
	metricsRegistry    prometheus.Registerer        // Prometheus registry for metrics (shared with peer metrics)
	// S3 storage
	s3Store             *s3.Store            // S3 file-based storage
	s3Server            *s3.Server           // S3 HTTP server
	s3Authorizer        *auth.Authorizer     // RBAC authorizer for S3
	s3Credentials       *s3.CredentialStore  // S3 credential store
	s3SystemStore       *s3.SystemStore      // System bucket accessor
	fileShareMgr        *s3.FileShareManager // File share manager
	builtinBindingsOnce sync.Once            // Ensures builtin group bindings are initialized only once
	// NFS server
	nfsServer *nfs.Server // NFS server for file shares
	// Packet filter
	filter       *routing.PacketFilter // Global packet filter
	filterSaveMu sync.Mutex            // Protects SaveFilterRules from concurrent calls
	filterTimer  *time.Timer           // Timer for debouncing filter saves
	// S3 GC serialization
	gcMu sync.Mutex // Prevents concurrent GC runs via /api/s3/gc
	// Docker orchestration (when coordinator joins mesh)
	dockerMgr *docker.Manager // Docker manager (nil if Docker not enabled)
	// Peer name cache for owner display (cached to avoid LoadPeers() on every request)
	peerNameCache atomic.Pointer[map[string]string] // Peer ID -> name mapping
	// Callback for coordinator list changes (updates local DNS resolver)
	coordIPsCb func([]string)
	// Replication for multi-coordinator setup
	replicator       *replication.Replicator    // S3 replication engine (nil if not enabled)
	meshTransport    *replication.MeshTransport // Transport for replication messages
	capacityRegistry *s3.CapacityRegistry       // Storage capacity tracking for replication
}

// coordIPSet holds both the original and sorted coordinator IP lists as a single
// atomic unit to prevent inconsistent reads during updates.
type coordIPSet struct {
	original []string // Self is always first
	sorted   []string // Deterministic ordering for hash-based routing
}

// ipAllocator manages IP address allocation from the mesh CIDR.
// It uses deterministic allocation based on peer name hash for consistency.
// State is persisted to S3 for recovery across coordinator restarts.
type ipAllocator struct {
	network     *net.IPNet
	used        map[string]bool
	peerToIP    map[string]string // peer name -> allocated IP (for consistency)
	next        uint32
	mu          sync.Mutex
	systemStore *s3.SystemStore // For persisting allocations to S3
}

func newIPAllocator(cidr string, systemStore *s3.SystemStore) (*ipAllocator, error) {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, fmt.Errorf("parse CIDR: %w", err)
	}

	allocator := &ipAllocator{
		network:     network,
		used:        make(map[string]bool),
		peerToIP:    make(map[string]string),
		next:        1, // Start from .1, skip .0 (network address)
		systemStore: systemStore,
	}

	// Load existing allocations from S3 if available
	if systemStore != nil {
		if err := allocator.loadFromS3(); err != nil {
			log.Warn().Err(err).Msg("Failed to load IP allocations from S3, starting fresh")
			// Continue with empty allocator - not a fatal error
		}
	}

	return allocator, nil
}

// allocateForPeer allocates an IP for a specific peer, using deterministic
// hashing to ensure the same peer always gets the same IP.
func (a *ipAllocator) allocateForPeer(peerName string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if we already allocated an IP for this peer
	if ip, exists := a.peerToIP[peerName]; exists {
		return ip, nil
	}

	// Get the network address as uint32
	ip := a.network.IP.To4()
	if ip == nil {
		return "", fmt.Errorf("only IPv4 supported")
	}

	base := uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])

	ones, bits := a.network.Mask.Size()
	maxHosts := uint32(1<<(bits-ones)) - 2 // Subtract network and broadcast

	// Use hash of peer name to get a deterministic starting point
	// This ensures the same peer always gets the same IP (if available)
	h := fnv.New32a()
	h.Write([]byte(peerName))
	hashOffset := h.Sum32() % maxHosts
	if hashOffset == 0 {
		hashOffset = 1 // Skip .0
	}

	// Try the hash-based IP first, then fall back to sequential search
	for i := uint32(0); i < maxHosts; i++ {
		offset := (hashOffset + i) % maxHosts
		if offset == 0 {
			offset = 1 // Skip .0
		}
		candidate := base + offset

		candidateIP := net.IPv4(
			byte(candidate>>24),
			byte(candidate>>16),
			byte(candidate>>8),
			byte(candidate),
		)

		ipStr := candidateIP.String()
		if !a.used[ipStr] {
			a.used[ipStr] = true
			a.peerToIP[peerName] = ipStr

			// Persist to S3 (async, non-blocking)
			// Copy data while holding lock to avoid race condition
			if a.systemStore != nil {
				data := a.copyDataLocked()
				go a.saveToS3Async(data)
			}

			return ipStr, nil
		}
	}

	return "", fmt.Errorf("no available IP addresses")
}

func (a *ipAllocator) release(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.used, ip)

	// Persist to S3 (async, non-blocking)
	// Copy data while holding lock to avoid race condition
	if a.systemStore != nil {
		data := a.copyDataLocked()
		go a.saveToS3Async(data)
	}
}

// loadFromS3 loads IP allocations from S3 storage.
// Called during initialization. Errors are logged but not fatal.
func (a *ipAllocator) loadFromS3() error {
	data, err := a.systemStore.LoadIPAllocations(context.Background())
	if err != nil {
		return fmt.Errorf("load IP allocations: %w", err)
	}

	if data == nil {
		// No existing data - fresh start
		return nil
	}

	// Restore state
	a.used = data.Used
	a.peerToIP = data.PeerToIP
	a.next = data.Next

	log.Info().
		Int("allocated_ips", len(a.used)).
		Int("peer_mappings", len(a.peerToIP)).
		Msg("Loaded IP allocations from S3")

	return nil
}

// copyDataLocked creates a deep copy of IP allocation data.
// Must be called while holding a.mu lock.
func (a *ipAllocator) copyDataLocked() s3.IPAllocationsData {
	// Deep copy maps to avoid data races
	usedCopy := make(map[string]bool, len(a.used))
	for k, v := range a.used {
		usedCopy[k] = v
	}

	peerToIPCopy := make(map[string]string, len(a.peerToIP))
	for k, v := range a.peerToIP {
		peerToIPCopy[k] = v
	}

	return s3.IPAllocationsData{
		Used:     usedCopy,
		PeerToIP: peerToIPCopy,
		Next:     a.next,
	}
}

// saveToS3Async persists IP allocations to S3 storage asynchronously.
// Called after each allocation/release. Errors are logged but don't fail the operation.
// Takes a copy of the data to avoid holding locks during I/O.
func (a *ipAllocator) saveToS3Async(data s3.IPAllocationsData) {
	if err := a.systemStore.SaveIPAllocations(context.Background(), data); err != nil {
		log.Error().Err(err).Msg("Failed to save IP allocations to S3")
	}
}

// NewServer creates a new coordination server.
func NewServer(ctx context.Context, cfg *config.PeerConfig) (*Server, error) {
	// Create a cancellable context for server lifecycle
	// This allows us to stop all background operations on shutdown
	ctx, cancel := context.WithCancel(ctx)

	srv := &Server{
		cancel:       cancel,
		cfg:          cfg,
		mux:          http.NewServeMux(),
		peers:        make(map[string]*peerInfo),
		coordinators: make(map[string]*peerInfo),
		dnsCache:     make(map[string]string),
		aliasOwner:   make(map[string]string),
		serverStats: serverStats{
			startTime: time.Now(),
		},
		sseHub:       newSSEHub(),
		coordMetrics: nil, // Initialized lazily when SetMetricsRegistry is called
	}

	// Initialize IP geolocation cache if locations feature is enabled
	if cfg.Coordinator.Locations {
		srv.ipGeoCache = NewIPGeoCache("") // Use default ip-api.com URL
		log.Info().Msg("node location tracking enabled (uses external IP geolocation API)")
	}

	// Set peer expiration days from config
	if cfg.Coordinator.UserExpirationDays > 0 {
		auth.SetPeerExpirationDays(cfg.Coordinator.UserExpirationDays)
	}

	// Initialize WireGuard client store if enabled
	if cfg.Coordinator.WireGuardServer.Enabled {
		srv.wgStore = wireguard.NewStore(mesh.CIDR)
		log.Info().Msg("WireGuard client management enabled")
	}

	// Initialize S3 storage (always enabled, must be before IP allocator)
	if err := srv.initS3Storage(ctx, cfg); err != nil {
		return nil, fmt.Errorf("initialize S3 storage: %w", err)
	}

	// Initialize IP allocator (after S3 so it can load persisted allocations)
	ipAlloc, err := newIPAllocator(mesh.CIDR, srv.s3SystemStore)
	if err != nil {
		return nil, fmt.Errorf("create IP allocator: %w", err)
	}
	srv.ipAlloc = ipAlloc

	// Initialize Certificate Authority for mesh TLS
	ca, err := NewCertificateAuthority(cfg.Coordinator.DataDir, mesh.DomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("initialize CA: %w", err)
	}
	srv.ca = ca

	// Shared transport for forwarding S3 writes to other coordinators.
	// TLS config is set later by SetMeshTLS() with the registration CA.
	srv.s3ForwardTransport = &http.Transport{
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true, // S3 objects are often already compressed
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		ResponseHeaderTimeout: 30 * time.Second,
	}

	// Initialize packet filter
	srv.filter = routing.NewPacketFilter(cfg.Coordinator.Filter.IsDefaultDeny())

	// Load coordinator config rules
	coordRules := make([]routing.FilterRule, len(cfg.Coordinator.Filter.Rules))
	for i, r := range cfg.Coordinator.Filter.Rules {
		coordRules[i] = routing.FilterRule{
			Port:       r.Port,
			Protocol:   r.ProtocolNumber(),
			Action:     routing.ParseFilterAction(r.Action),
			SourcePeer: r.SourcePeer,
		}
	}
	srv.filter.SetCoordinatorRules(coordRules)

	// Set service rules from config
	serviceRules := make([]routing.FilterRule, len(cfg.Coordinator.ServicePorts))
	for i, port := range cfg.Coordinator.ServicePorts {
		serviceRules[i] = routing.FilterRule{
			Port:     port,
			Protocol: routing.ProtoTCP,
			Action:   routing.ActionAllow,
		}
	}
	srv.filter.SetServiceRules(serviceRules)

	// Load persisted filter rules (temporary and service overrides)
	if err := srv.recoverFilterRules(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to load filter rules, starting fresh")
	}

	// Initialize S3 replication for multi-coordinator deployments
	// Coordinators discover each other through the peer list using is_coordinator flag
	if srv.s3Store != nil {
		// Initialize capacity registry for storage capacity tracking
		srv.capacityRegistry = s3.NewCapacityRegistry()

		// Create mesh transport for replication messages (HTTPS between coordinators).
		// TLS config is set later by SetMeshTLS() with the registration CA.
		srv.meshTransport = replication.NewMeshTransport(log.Logger, nil)

		// Create S3 store adapter for replication
		s3Adapter := replication.NewS3StoreAdapter(srv.s3Store)

		// Build node ID from coordinator name
		nodeID := cfg.Name
		if nodeID == "" {
			nodeID = "coordinator"
		}

		// Initialize chunk registry for chunk-level replication (avoids buffering full objects)
		chunkRegistry := replication.NewChunkRegistry(nodeID, nil)
		srv.s3Store.SetChunkRegistry(chunkRegistry)
		srv.s3Store.SetCoordinatorID(nodeID)

		// Initialize replicator
		srv.replicator = replication.NewReplicator(replication.Config{
			NodeID:               nodeID,
			Transport:            srv.meshTransport,
			S3Store:              s3Adapter,
			ChunkRegistry:        chunkRegistry,
			CapacityChecker:      srv.capacityRegistry,
			Logger:               log.Logger,
			Context:              ctx, // Pass server context for proper cancellation
			AckTimeout:           10 * time.Second,
			RetryInterval:        30 * time.Second,
			MaxPendingOperations: 10000,
			ChunkPipelineWindow:  5,               // Send up to 5 chunks concurrently per object
			AutoSyncInterval:     5 * time.Minute, // Re-enqueue all objects every 5 minutes
		})

		// Wire replicator into S3 store for distributed reads (fetching remote chunks)
		srv.s3Store.SetReplicator(srv.replicator)

		// Initialize rebalancer for automatic data redistribution on topology changes
		rebalancer := replication.NewRebalancer(srv.replicator, s3Adapter, chunkRegistry, log.Logger)
		rebalancer.OnCycleComplete = func(stats replication.RebalancerStats) {
			if m := s3.GetS3Metrics(); m != nil {
				m.RebalanceRunsTotal.Add(float64(stats.RunsTotal))
				m.RebalanceChunksMovedTotal.Add(float64(stats.ChunksRedistributed))
				m.RebalanceObjectsEnqueuedTotal.Add(float64(stats.ObjectsEnqueued))
				m.RebalanceBytesTransferred.Add(float64(stats.BytesTransferred))
			}
		}
		srv.replicator.SetRebalancer(rebalancer)

		log.Info().
			Str("node_id", nodeID).
			Msg("replication engine initialized - coordinators will discover each other via peer list")
	}

	srv.setupRoutes(ctx)
	return srv, nil
}

// SetMeshTLS configures the TLS settings for inter-coordinator communication.
// The TLS config should trust the mesh CA (from registration) and include the
// coordinator's own certificate for client authentication.
// Must be called before any replication traffic is sent.
func (s *Server) SetMeshTLS(cfg *tls.Config) {
	if s.meshTransport != nil {
		s.meshTransport.SetTLSConfig(cfg)
	}
	if s.s3ForwardTransport != nil {
		s.s3ForwardTransport.TLSClientConfig = cfg
	}
}

// refreshPeerNameCache reloads the peer ID -> name mapping from storage.
// This is called during server startup and when peers are added/updated.
func (s *Server) refreshPeerNameCache() error {
	peers, err := s.s3SystemStore.LoadPeers(context.Background())
	if err != nil {
		log.Warn().Err(err).Msg("failed to load peers for name cache")
		return fmt.Errorf("load peers: %w", err)
	}

	nameMap := make(map[string]string, len(peers))
	for _, peer := range peers {
		nameMap[peer.ID] = peer.Name
	}
	s.peerNameCache.Store(&nameMap)

	log.Debug().Int("count", len(nameMap)).Msg("refreshed peer name cache")
	return nil
}

// getPeerName returns the name for a peer ID, falling back to the ID if not found.
// This uses the cached peer name map to avoid repeated LoadPeers() calls.
func (s *Server) getPeerName(peerID string) string {
	cache := s.peerNameCache.Load()
	if cache == nil || peerID == "" {
		return peerID
	}
	if name := (*cache)[peerID]; name != "" {
		return name
	}
	return peerID // Fallback to ID if peer not found (may have been deleted)
}

// saveDNSData persists DNS cache and aliases to S3.
// This is called asynchronously after peer registration changes.
func (s *Server) saveDNSData() {
	// Copy data while holding lock to avoid blocking peer operations during S3 writes
	s.peersMu.RLock()
	dnsCache := make(map[string]string, len(s.dnsCache))
	for k, v := range s.dnsCache {
		dnsCache[k] = v
	}

	peerAliases := make(map[string][]string)
	for peerName, info := range s.peers {
		if len(info.aliases) > 0 {
			// Copy slice to avoid sharing references
			peerAliases[peerName] = append([]string{}, info.aliases...)
		}
	}

	aliasOwner := make(map[string]string, len(s.aliasOwner))
	for k, v := range s.aliasOwner {
		aliasOwner[k] = v
	}
	s.peersMu.RUnlock()

	// Perform S3 operations without holding lock
	if err := s.s3SystemStore.SaveDNSCache(context.Background(), dnsCache); err != nil {
		log.Warn().Err(err).Msg("failed to persist DNS cache")
	} else {
		log.Debug().Int("entries", len(dnsCache)).Msg("persisted DNS cache to S3")
	}

	if err := s.s3SystemStore.SaveDNSAliases(context.Background(), aliasOwner, peerAliases); err != nil {
		log.Warn().Err(err).Msg("failed to persist DNS aliases")
	} else {
		log.Debug().Int("aliases", len(aliasOwner)).Msg("persisted DNS aliases to S3")
	}
}

// recoverFilterRules loads filter rules from S3.
func (s *Server) recoverFilterRules(ctx context.Context) error {
	data, err := s.s3SystemStore.LoadFilterRules(ctx)
	if err != nil {
		return err
	}

	// Load temporary rules (skip expired)
	now := time.Now().Unix()
	tempRules := make([]routing.FilterRule, 0, len(data.Temporary))
	for _, r := range data.Temporary {
		if r.Expires > 0 && r.Expires <= now {
			log.Debug().Uint16("port", r.Port).Str("protocol", r.Protocol).
				Msg("skipping expired temporary filter rule")
			continue
		}
		tempRules = append(tempRules, routing.FilterRule{
			Port:       r.Port,
			Protocol:   routing.ProtocolFromString(r.Protocol),
			Action:     routing.ParseFilterAction(r.Action),
			Expires:    r.Expires,
			SourcePeer: r.SourcePeer,
		})
	}

	// Apply loaded temporary rules
	// Note: Service rules are not persisted - they always come from config
	for _, rule := range tempRules {
		s.filter.AddTemporaryRule(rule)
	}

	log.Info().Int("temporary", len(tempRules)).
		Msg("loaded filter rules from S3")
	return nil
}

// SaveFilterRules persists filter rules to S3 or does nothing if S3 is disabled.
// Filters out expired rules before saving to prevent storage bloat.
func (s *Server) SaveFilterRules(ctx context.Context) error {
	// Mutex prevents concurrent saves from racing
	s.filterSaveMu.Lock()
	defer s.filterSaveMu.Unlock()

	// Get all current rules
	allRules := s.filter.ListRules()

	data := s3.FilterRulesData{
		Temporary: make([]s3.FilterRulePersisted, 0),
	}

	// Only persist non-expired temporary rules (CLI/API added)
	// Service rules always come from config and are not persisted
	// Expired rules are filtered out to prevent storage bloat
	now := time.Now().Unix()
	for _, r := range allRules {
		if r.Source == routing.SourceTemporary {
			// Skip expired rules
			if r.Rule.Expires > 0 && r.Rule.Expires <= now {
				continue
			}

			persisted := s3.FilterRulePersisted{
				Port:       r.Rule.Port,
				Protocol:   routing.ProtocolToString(r.Rule.Protocol),
				Action:     r.Rule.Action.String(),
				Expires:    r.Rule.Expires,
				SourcePeer: r.Rule.SourcePeer,
			}
			data.Temporary = append(data.Temporary, persisted)
		}
	}

	if err := s.s3SystemStore.SaveFilterRules(ctx, data); err != nil {
		return err
	}

	log.Debug().Int("temporary", len(data.Temporary)).
		Msg("saved filter rules to S3")
	return nil
}

// saveFilterRulesAsync saves filter rules asynchronously with debouncing.
// Uses a timer that resets on each call to coalesce rapid changes.
func (s *Server) saveFilterRulesAsync() {
	s.filterSaveMu.Lock()
	defer s.filterSaveMu.Unlock()

	// Stop existing timer if it exists
	if s.filterTimer != nil {
		s.filterTimer.Stop()
	}

	// Create new timer that saves after 100ms of inactivity
	s.filterTimer = time.AfterFunc(100*time.Millisecond, func() {
		if err := s.SaveFilterRules(context.Background()); err != nil {
			log.Error().Err(err).Msg("failed to save filter rules")
		}
	})
}

// Shutdown gracefully shuts down the server, persisting state.
// Returns an error if any persistence operations fail.
func (s *Server) Shutdown(ctx context.Context) error {
	// Cancel server lifecycle context to stop all background operations
	if s.cancel != nil {
		s.cancel()
	}

	log.Info().Msg("saving data before shutdown")

	var errs []error

	// Stop admin server if running
	if s.adminServer != nil {
		log.Info().Msg("stopping admin server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.adminServer.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msg("failed to stop admin server")
			errs = append(errs, fmt.Errorf("stop admin server: %w", err))
		}
	}

	// Save DNS data (cache and aliases)
	// This ensures final state is persisted even if async saves are in-flight
	s.saveDNSData()

	// Stop filter save timer and ensure final save happens
	s.filterSaveMu.Lock()
	if s.filterTimer != nil {
		s.filterTimer.Stop()
		s.filterTimer = nil
	}
	s.filterSaveMu.Unlock()

	// Save filter rules (final save after stopping timer)
	if err := s.SaveFilterRules(ctx); err != nil {
		log.Error().Err(err).Msg("failed to save filter rules")
		errs = append(errs, fmt.Errorf("save filter rules: %w", err))
	}

	// Stop Docker manager if running
	if s.dockerMgr != nil {
		log.Info().Msg("stopping Docker manager")
		if err := s.dockerMgr.Stop(); err != nil {
			log.Error().Err(err).Msg("failed to stop Docker manager")
			errs = append(errs, fmt.Errorf("stop docker manager: %w", err))
		}
	}

	// Stop replicator if running
	if s.replicator != nil {
		log.Info().Msg("stopping replicator")
		if err := s.replicator.Stop(); err != nil {
			log.Error().Err(err).Msg("failed to stop replicator")
			errs = append(errs, fmt.Errorf("stop replicator: %w", err))
		}
	}

	// Wait for all background goroutines to finish (with timeout)
	log.Info().Msg("waiting for background goroutines to complete")
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info().Msg("all background goroutines completed")
	case <-time.After(10 * time.Second):
		log.Warn().Msg("timeout waiting for background goroutines to complete")
		errs = append(errs, fmt.Errorf("shutdown timeout: goroutines did not complete within 10s"))
	}

	// Flush S3 store to ensure all filesystem operations complete
	if s.s3Store != nil {
		log.Info().Msg("flushing S3 store")
		if err := s.s3Store.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close S3 store")
			errs = append(errs, fmt.Errorf("close S3 store: %w", err))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// StartPeriodicSave starts a goroutine that periodically saves stats history.
// The goroutine stops when the context is cancelled.
func (s *Server) StartPeriodicSave(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.SaveFilterRules(ctx); err != nil {
					log.Warn().Err(err).Msg("failed to save filter rules")
				}
			}
		}
	}()
}

// StartPeriodicCleanup starts the background cleanup goroutine for S3 storage.
// gcStaggerDelay computes a deterministic stagger delay based on the coordinator
// name hash, to avoid all coordinators running GC simultaneously.
// Returns 0 to 149 seconds (0–2m29s), keeping the window under 50% of the
// 5-minute GC interval to ensure predictable cleanup cadence.
func gcStaggerDelay(coordName string) time.Duration {
	if coordName == "" {
		coordName = "coordinator"
	}
	h := fnv.New32a()
	h.Write([]byte(coordName))
	return time.Duration(h.Sum32()%150) * time.Second
}

// StartPeriodicCleanup launches a background goroutine for S3 storage maintenance.
// It periodically:
//   - Purges recycled objects past their retention period
//   - Purges content in expired file shares
//   - Runs garbage collection on versions and orphaned chunks
//   - Updates CAS metrics (dedup ratio, chunk count, etc.)
func (s *Server) StartPeriodicCleanup(ctx context.Context) {
	if s.s3Store == nil {
		return
	}

	stagger := gcStaggerDelay(s.cfg.Name)

	log.Info().Str("coordinator", s.cfg.Name).Dur("stagger", stagger).Msg("GC stagger delay computed")

	// GC cycle body — shared between first run and ticker runs
	runGCCycle := func() {
		// Purge recycled objects past retention period
		if purged := s.s3Store.PurgeRecycleBin(ctx); purged > 0 {
			log.Info().Int("count", purged).Msg("purged recycled S3 objects")
		}

		// Purge content in expired file shares
		if s.fileShareMgr != nil {
			if purged := s.fileShareMgr.PurgeExpiredShareContents(ctx); purged > 0 {
				log.Info().Int("count", purged).Msg("purged expired file share content")
			}
		}

		// Purge orphaned file share buckets (shares deleted on another coordinator)
		if s.fileShareMgr != nil {
			if purged := s.fileShareMgr.PurgeOrphanedFileShareBuckets(ctx); purged > 0 {
				log.Info().Int("count", purged).Msg("purged orphaned file share buckets")
			}
		}

		// Serialize with manual GC — skip this cycle if manual GC is running
		if !s.gcMu.TryLock() {
			log.Debug().Msg("skipping periodic GC: manual GC in progress")
		} else {
			// Run garbage collection on versions and orphaned chunks
			gcStart := time.Now()
			gcStats := s.s3Store.RunGarbageCollection(ctx)
			gcDuration := time.Since(gcStart).Seconds()
			s.gcMu.Unlock()

			// Always log GC at Info level so operators can confirm it's running
			log.Info().
				Str("coordinator", s.cfg.Name).
				Int("versions_pruned", gcStats.VersionsPruned).
				Int("chunks_deleted", gcStats.ChunksDeleted).
				Int64("bytes_reclaimed", gcStats.BytesReclaimed).
				Int("objects_scanned", gcStats.ObjectsScanned).
				Int("chunks_skipped_grace_period", gcStats.ChunksSkippedGracePeriod).
				Float64("duration_seconds", gcDuration).
				Msg("periodic S3 garbage collection completed")

			// Record GC metrics
			if metrics := s3.GetS3Metrics(); metrics != nil {
				metrics.RecordGCRun(gcStats.VersionsPruned, gcStats.ChunksDeleted, gcStats.BytesReclaimed, gcDuration)
			}

			// Update S3 metrics after GC
			s.updateS3Metrics()
		}

		// Collect and persist local capacity, then load peer snapshots
		s.collectAndPersistCapacity(ctx)
		s.loadPeerCapacitySnapshots(ctx)
	}

	// Initialize listing index channel and start background indexer
	s.listingIndexNotify = make(chan struct{}, 1)
	s.reconcileLocalIndex(ctx) // Populate index from disk on startup
	s.loadPeerIndexes(ctx)     // Load any existing replicated peer data
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runListingIndexer(ctx)
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Update metrics on startup
		s.updateS3Metrics()
		s.collectAndPersistCapacity(ctx)

		// Wait for stagger delay before first GC run
		select {
		case <-ctx.Done():
			return
		case <-time.After(stagger):
		}

		// Run first GC cycle immediately after stagger
		runGCCycle()

		// Create ticker AFTER stagger so the cycle is properly offset.
		// Each coordinator's cycle: stagger, stagger+5m, stagger+10m, ...
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runGCCycle()
			}
		}
	}()

	// Refresh storage/CAS gauges every 60s so dashboards stay current
	// between the hourly GC cycles.
	metricsTicker := time.NewTicker(60 * time.Second)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer metricsTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-metricsTicker.C:
				s.updateS3Metrics()
			}
		}
	}()
}

// StartReplicator starts the replication engine if enabled.
func (s *Server) StartReplicator() error {
	if s.replicator == nil {
		return nil // Replication not enabled
	}

	if err := s.replicator.Start(); err != nil {
		return fmt.Errorf("start replicator: %w", err)
	}

	// Discover existing coordinator peers and add them to replication targets
	// Use coordinators map for O(1) lookups instead of iterating all peers
	s.peersMu.RLock()
	for name, info := range s.coordinators {
		// Don't add self to replication targets
		if name != s.cfg.Name {
			s.replicator.AddPeer(info.peer.MeshIP, name)
			log.Debug().
				Str("peer", name).
				Str("mesh_ip", info.peer.MeshIP).
				Msg("discovered existing coordinator for replication")
		}
	}
	s.peersMu.RUnlock()

	log.Info().
		Int("coordinator_count", len(s.replicator.GetPeers())).
		Msg("replicator started with discovered coordinators")
	return nil
}

// updateS3Metrics collects CAS stats once and updates all S3-related Prometheus
// gauges (chunks, objects, storage bytes, quota, registered users).
func (s *Server) updateS3Metrics() {
	if s.s3Store == nil {
		return
	}
	metrics := s3.GetS3Metrics()
	if metrics == nil {
		return
	}

	casStats := s.s3Store.GetCASStats()
	metrics.UpdateCASMetrics(
		casStats.ChunkCount,
		casStats.ChunkBytes,
		casStats.LogicalBytes,
		casStats.VersionBytes,
		casStats.RecycledBytes,
		casStats.VersionCount,
	)

	buckets, err := s.s3Store.ListBuckets(context.Background())
	if err != nil {
		return
	}

	// Use physical chunk bytes (post-dedup) as the headline storage metric.
	// NOTE: This is intentionally the same value as tunnelmesh_s3_chunk_storage_bytes
	// (set by UpdateCASMetrics above). storage_bytes is the headline/dashboard metric;
	// chunk_storage_bytes lives in the CAS namespace for dedup analysis.
	storageBytes := casStats.ChunkBytes

	var quotaBytes int64
	if qs := s.s3Store.QuotaStats(); qs != nil {
		quotaBytes = qs.MaxBytes
	}

	metrics.UpdateStorageMetrics(len(buckets), casStats.ObjectCount, storageBytes, quotaBytes)

	if s.s3Credentials != nil {
		metrics.SetRegisteredUsers(s.s3Credentials.UserCount())
	}
}

// collectAndPersistCapacity collects local volume/quota stats, updates the local
// capacity registry, updates Prometheus metrics, and persists to the system bucket.
func (s *Server) collectAndPersistCapacity(ctx context.Context) {
	if s.s3Store == nil || s.capacityRegistry == nil {
		return
	}

	total, used, available, err := s.s3Store.VolumeStats()
	if err != nil {
		log.Warn().Err(err).Msg("failed to collect volume stats")
		return
	}

	snap := &s3.CapacitySnapshot{
		Timestamp:            time.Now(),
		CoordinatorName:      s.cfg.Name,
		VolumeTotalBytes:     total,
		VolumeUsedBytes:      used,
		VolumeAvailableBytes: available,
	}

	// Add quota info if available
	if qs := s.s3Store.QuotaStats(); qs != nil {
		snap.QuotaMaxBytes = qs.MaxBytes
		snap.QuotaUsedBytes = qs.UsedBytes
		snap.QuotaAvailBytes = qs.AvailableBytes
	} else {
		snap.QuotaAvailBytes = -1 // unlimited
	}

	// Update local registry
	s.capacityRegistry.Update(snap)

	// Update Prometheus metrics
	if metrics := s3.GetS3Metrics(); metrics != nil {
		metrics.UpdateVolumeMetrics(total, used, available)
	}

	// Persist to system bucket
	if s.s3SystemStore != nil {
		key := "stats/" + s.cfg.Name + ".capacity.json"
		if err := s.s3SystemStore.SaveJSON(ctx, key, snap); err != nil {
			log.Warn().Err(err).Msg("failed to persist capacity snapshot")
		}
	}
}

// loadPeerCapacitySnapshots loads capacity snapshots from the system bucket
// for all coordinators and populates the capacity registry.
func (s *Server) loadPeerCapacitySnapshots(ctx context.Context) {
	if s.s3Store == nil || s.s3SystemStore == nil || s.capacityRegistry == nil {
		return
	}

	objects, _, _, err := s.s3Store.ListObjects(ctx, "_tunnelmesh", "stats/", "", 1000)
	if err != nil {
		log.Warn().Err(err).Msg("failed to list capacity snapshots")
		return
	}

	loaded := 0
	for _, obj := range objects {
		key := obj.Key
		if !strings.HasSuffix(key, ".capacity.json") {
			continue
		}

		var snap s3.CapacitySnapshot
		if err := s.s3SystemStore.LoadJSON(ctx, key, &snap); err != nil {
			log.Warn().Err(err).Str("key", key).Msg("failed to load capacity snapshot")
			continue
		}
		s.capacityRegistry.Update(&snap)
		loaded++
	}

	if loaded > 0 {
		log.Info().Int("count", loaded).Msg("loaded peer capacity snapshots")
	}
}

// SetVersion sets the server version for admin display.
func (s *Server) SetVersion(version string) {
	s.version = version
}

func (s *Server) setupRoutes(ctx context.Context) {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/ca.crt", s.handleCACert) // CA cert for mesh TLS (no auth)
	s.mux.HandleFunc("/api/v1/register", s.withAuth(s.handleRegister))
	s.mux.HandleFunc("/api/v1/peers", s.withAuth(s.handlePeers))
	s.mux.HandleFunc("/api/v1/peers/", s.withAuth(s.handlePeerByName))
	// Note: HTTP heartbeat endpoint removed - heartbeats now sent via WebSocket in relay.go
	s.mux.HandleFunc("/api/v1/dns", s.withAuth(s.handleDNS))

	// Setup relay routes (JWT auth handled internally)
	// Always setup relay routes (relay always enabled for coordinators)
	s.setupRelayRoutes(ctx)

	// Always setup admin routes (admin always enabled for coordinators)
	s.setupAdminRoutes()

	// Always setup monitoring proxies (local proxy or forward to remote coordinator)
	s.SetupMonitoringProxies(MonitoringProxyConfig{
		PrometheusURL: s.cfg.Coordinator.Monitoring.PrometheusURL,
		GrafanaURL:    s.cfg.Coordinator.Monitoring.GrafanaURL,
	})

	// Setup UDP hole-punch coordination routes
	s.setupHolePunchRoutes()

	// Setup WireGuard concentrator sync endpoint (JWT auth)
	if s.cfg.Coordinator.WireGuardServer.Enabled {
		s.setupWireGuardRoutes()
	}

	// Note: Replication endpoint moved to adminMux (see admin.go) to ensure
	// replication only happens within the mesh network, not from public internet
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// NFSPort is the standard NFS port used for file share access.
const NFSPort = 2049

// getServicePorts returns the list of service ports the coordinator exposes.
// These ports are pushed to peers so they can auto-allow access through the packet filter.
func (s *Server) getServicePorts() []uint16 {
	var ports []uint16

	// Coordinator services always enabled with hardcoded ports
	ports = append(ports, 443)  // Admin (HTTPS)
	ports = append(ports, 9000) // S3 API

	// NFS port (only when there are active file shares)
	if s.fileShareMgr != nil && len(s.fileShareMgr.List()) > 0 {
		ports = append(ports, NFSPort)
	}

	// Configured service ports (includes metrics port 9443 by default)
	ports = append(ports, s.cfg.Coordinator.ServicePorts...)

	return ports
}

// loadOrCreateCASKey loads or creates the master key for CAS encryption.
// The key is stored in the S3 data directory as cas.key.
func (s *Server) loadOrCreateCASKey(dataDir string) ([32]byte, error) {
	keyPath := filepath.Join(dataDir, "cas.key")
	var masterKey [32]byte

	// Try to load existing key
	data, err := os.ReadFile(keyPath)
	if err == nil && len(data) == 32 {
		copy(masterKey[:], data)
		return masterKey, nil
	}

	// Create new key
	if _, err := rand.Read(masterKey[:]); err != nil {
		return masterKey, fmt.Errorf("generate CAS key: %w", err)
	}

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return masterKey, fmt.Errorf("create data directory: %w", err)
	}

	// Save key to disk
	if err := os.WriteFile(keyPath, masterKey[:], 0o600); err != nil {
		return masterKey, fmt.Errorf("save CAS key: %w", err)
	}

	log.Info().Str("path", keyPath).Msg("generated new CAS encryption key")
	return masterKey, nil
}

// initS3Storage initializes the S3 storage subsystem.
func (s *Server) initS3Storage(ctx context.Context, cfg *config.PeerConfig) error {
	// Require max_size to be configured for quota enforcement
	if cfg.Coordinator.S3.MaxSize.Bytes() <= 0 {
		return fmt.Errorf("s3.max_size must be configured (e.g., 10Gi) for quota enforcement")
	}
	quota := s3.NewQuotaManager(cfg.Coordinator.S3.MaxSize.Bytes())

	// Load or create master key for CAS encryption
	masterKey, err := s.loadOrCreateCASKey(cfg.Coordinator.S3.DataDir)
	if err != nil {
		return fmt.Errorf("initialize CAS key: %w", err)
	}

	// Create store with CAS for content-addressed storage
	store, err := s3.NewStoreWithCAS(cfg.Coordinator.S3.DataDir, quota, masterKey)
	if err != nil {
		return fmt.Errorf("create S3 store: %w", err)
	}
	s.s3Store = store

	// Set expiry defaults from config
	store.SetDefaultObjectExpiryDays(cfg.Coordinator.S3.ObjectExpiryDays)
	store.SetDefaultShareExpiryDays(cfg.Coordinator.S3.ShareExpiryDays)

	// Set recycle bin retention config
	store.SetRecycleBinRetentionDays(cfg.Coordinator.S3.RecycleBinRetentionDays)

	// Set version retention config
	store.SetVersionRetentionDays(cfg.Coordinator.S3.VersionRetentionDays)
	store.SetMaxVersionsPerObject(cfg.Coordinator.S3.MaxVersionsPerObject)
	store.SetVersionRetentionPolicy(s3.VersionRetentionPolicy{
		RecentDays:    cfg.Coordinator.S3.VersionRetention.RecentDays,
		WeeklyWeeks:   cfg.Coordinator.S3.VersionRetention.WeeklyWeeks,
		MonthlyMonths: cfg.Coordinator.S3.VersionRetention.MonthlyMonths,
	})

	// Create authorizer with group support
	s.s3Authorizer = auth.NewAuthorizerWithGroups()

	// Create credential store
	s.s3Credentials = s3.NewCredentialStore()

	// Create RBAC authorizer for S3
	rbacAuth := s3.NewRBACAuthorizer(s.s3Credentials, s.s3Authorizer)

	// Create S3 server (metrics are initialized later in SetMetricsRegistry
	// when the correct Prometheus registry is available)
	s.s3Server = s3.NewServer(store, rbacAuth, nil)

	// Create system store for internal coordinator data
	// Use a service peer ID for the coordinator
	servicePeerID := auth.ServicePeerPrefix + "coordinator"
	systemStore, err := s3.NewSystemStore(store, servicePeerID)
	if err != nil {
		return fmt.Errorf("create system store: %w", err)
	}
	s.s3SystemStore = systemStore

	// Recover peers and credentials from previous runs
	if peers, err := systemStore.LoadPeers(ctx); err == nil && len(peers) > 0 {
		log.Info().Int("count", len(peers)).Msg("recovering registered peers")
		for _, peer := range peers {
			if _, _, err := s.s3Credentials.RegisterUser(peer.ID, peer.PublicKey); err != nil {
				log.Warn().Err(err).Str("peer", peer.ID).Msg("failed to recover peer credentials")
			}
		}
	}

	// Initialize peer name cache for owner lookups
	if err := s.refreshPeerNameCache(); err != nil {
		log.Warn().Err(err).Msg("failed to initialize peer name cache")
	}

	// Recover role bindings
	if bindings, err := systemStore.LoadBindings(ctx); err == nil && len(bindings) > 0 {
		log.Info().Int("count", len(bindings)).Msg("recovering role bindings")
		for _, binding := range bindings {
			s.s3Authorizer.Bindings.Add(binding)
		}
	}

	// Recover groups
	if groups, err := systemStore.LoadGroups(ctx); err == nil && len(groups) > 0 {
		log.Info().Int("count", len(groups)).Msg("recovering groups")
		s.s3Authorizer.Groups.LoadGroups(groups)
	}

	// Recover group bindings
	if groupBindings, err := systemStore.LoadGroupBindings(ctx); err == nil && len(groupBindings) > 0 {
		log.Info().Int("count", len(groupBindings)).Msg("recovering group bindings")
		s.s3Authorizer.GroupBindings.LoadBindings(groupBindings)
	}

	// Recover external panels
	if panels, err := systemStore.LoadPanels(ctx); err == nil && len(panels) > 0 {
		log.Info().Int("count", len(panels)).Msg("recovering external panels")
		for _, panel := range panels {
			if err := s.s3Authorizer.PanelRegistry.Register(*panel); err != nil {
				log.Warn().Err(err).Str("panel_id", panel.ID).Msg("failed to recover panel")
			}
		}
	}

	// Set up built-in group bindings if not already present
	s.ensureBuiltinGroupBindings()

	// Initialize file share manager
	s.fileShareMgr = s3.NewFileShareManager(store, systemStore, s.s3Authorizer)
	s.s3Server.SetBucketRecoverer(s.fileShareMgr)
	s.s3Server.SetRequestForwarder(s)
	s.s3Server.SetDeleteNotifier(s)
	log.Info().Int("shares", len(s.fileShareMgr.List())).Msg("file share manager initialized")

	// Recover coordinator state from S3
	s.recoverCoordinatorState(ctx, cfg, systemStore)

	// Register service peer credentials (derived from a fixed key for now)
	// In production, this would be derived from the CA private key
	if _, _, err := s.s3Credentials.RegisterUser(servicePeerID, servicePeerID); err != nil {
		return fmt.Errorf("register service peer credentials: %w", err)
	}

	log.Info().
		Str("data_dir", cfg.Coordinator.S3.DataDir).
		Int("port", 9000).
		Str("max_size", cfg.Coordinator.S3.MaxSize.String()).
		Msg("S3 storage initialized")

	return nil
}

// recoverCoordinatorState recovers ephemeral coordinator state from S3.
// This includes WG concentrator assignment and DNS cache/aliases.
func (s *Server) recoverCoordinatorState(ctx context.Context, cfg *config.PeerConfig, systemStore *s3.SystemStore) {
	// Recover WireGuard concentrator assignment
	if concentrator, err := systemStore.LoadWGConcentrator(ctx); err == nil && concentrator != "" {
		log.Info().Str("peer", concentrator).Msg("recovering WireGuard concentrator assignment")
		// Store in relay manager - will be validated when peer reconnects
		s.relay.RecoverWGConcentrator(concentrator)
	}

	// Recover DNS cache if available
	if dnsCache, err := systemStore.LoadDNSCache(ctx); err == nil && len(dnsCache) > 0 {
		log.Info().Int("entries", len(dnsCache)).Msg("recovering DNS cache")
		s.peersMu.Lock()
		for hostname, meshIP := range dnsCache {
			s.dnsCache[hostname] = meshIP
		}
		s.peersMu.Unlock()
	}

	// Recover DNS aliases if available
	if _, aliasOwner, err := systemStore.LoadDNSAliases(ctx); err == nil && len(aliasOwner) > 0 {
		log.Info().Int("aliases", len(aliasOwner)).Msg("recovering DNS aliases")
		s.peersMu.Lock()
		for alias, owner := range aliasOwner {
			s.aliasOwner[alias] = owner
			// Also add to dnsCache if we have the peer's IP
			if meshIP, ok := s.dnsCache[owner]; ok {
				s.dnsCache[alias] = meshIP
			}
		}
		// Note: peerInfo.aliases will be updated when peers reconnect
		s.peersMu.Unlock()
	}

	// Recover coordinator IPs so full list is available before all coordinators re-register
	if coordIPs, err := systemStore.LoadCoordinatorIPs(ctx); err == nil && len(coordIPs) > 0 {
		log.Info().Strs("ips", coordIPs).Msg("recovering coordinator IPs")
		s.storeCoordIPs(coordIPs)
		if s.coordIPsCb != nil {
			s.coordIPsCb(coordIPs)
		}
	}

	// Pre-populate capacity registry from replicated system bucket
	s.loadPeerCapacitySnapshots(ctx)
}

// ensureBuiltinGroupBindings sets up the built-in group bindings if not already present.
// - admins group gets admin role (unscoped)
// - everyone group gets panel-viewer for default peer panels
// - admins group gets panel-viewer for admin-only panels
//
// Uses sync.Once to prevent race conditions during concurrent coordinator startups.
func (s *Server) ensureBuiltinGroupBindings() {
	s.builtinBindingsOnce.Do(func() {
		modified := false

		adminBindings := s.s3Authorizer.GroupBindings.GetForGroup(auth.GroupAdmins)
		if len(adminBindings) == 0 {
			s.s3Authorizer.GroupBindings.Add(auth.NewGroupBinding(
				auth.GroupAdmins,
				auth.RoleAdmin,
				"", // Unscoped - admin has access to all buckets
			))
			modified = true
		}

		// Add default panel bindings for everyone group
		everyoneBindings := s.s3Authorizer.GroupBindings.GetForGroup(auth.GroupEveryone)
		everyonePanels := make(map[string]bool)
		for _, b := range everyoneBindings {
			if b.RoleName == auth.RolePanelViewer && b.PanelScope != "" {
				everyonePanels[b.PanelScope] = true
			}
		}
		for _, panelID := range auth.DefaultPeerPanels() {
			if !everyonePanels[panelID] {
				s.s3Authorizer.GroupBindings.Add(auth.NewGroupBindingForPanel(
					auth.GroupEveryone,
					panelID,
				))
				modified = true
				log.Info().Str("group", auth.GroupEveryone).Str("panel", panelID).Msg("added default panel binding")
			}
		}

		// Add admin-only panel bindings for admins group
		adminPanels := make(map[string]bool)
		for _, b := range adminBindings {
			if b.RoleName == auth.RolePanelViewer && b.PanelScope != "" {
				adminPanels[b.PanelScope] = true
			}
		}
		// Re-fetch admin bindings since we may have added admin role binding above
		adminBindings = s.s3Authorizer.GroupBindings.GetForGroup(auth.GroupAdmins)
		for _, b := range adminBindings {
			if b.RoleName == auth.RolePanelViewer && b.PanelScope != "" {
				adminPanels[b.PanelScope] = true
			}
		}
		for _, panelID := range auth.DefaultAdminPanels() {
			if !adminPanels[panelID] {
				s.s3Authorizer.GroupBindings.Add(auth.NewGroupBindingForPanel(
					auth.GroupAdmins,
					panelID,
				))
				modified = true
				log.Info().Str("group", auth.GroupAdmins).Str("panel", panelID).Msg("added default panel binding")
			}
		}

		// Persist if we added any bindings
		if modified {
			if err := s.s3SystemStore.SaveGroupBindings(context.Background(), s.s3Authorizer.GroupBindings.List()); err != nil {
				log.Warn().Err(err).Msg("failed to persist builtin group bindings")
			}
		}
	})
}

// StartS3Server starts the S3 API server on the specified address.
// This should be called after join_mesh completes to bind to the mesh IP.
func (s *Server) StartS3Server(addr string, tlsCert *tls.Certificate) error {
	if s.s3Server == nil {
		return fmt.Errorf("S3 storage not initialized")
	}

	// Create listener with SO_REUSEADDR
	lc := net.ListenConfig{
		Control: setReuseAddr,
	}

	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	server := &http.Server{
		Handler: s.s3Server.Handler(),
	}

	if tlsCert != nil {
		server.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{*tlsCert},
			MinVersion:   tls.VersionTLS12,
		}
		log.Info().Str("addr", addr).Msg("starting S3 server (HTTPS)")
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := server.ServeTLS(ln, "", ""); err != nil && err != http.ErrServerClosed {
				log.Error().Err(err).Msg("S3 server error")
			}
		}()
	} else {
		log.Info().Str("addr", addr).Msg("starting S3 server (HTTP)")
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				log.Error().Err(err).Msg("S3 server error")
			}
		}()
	}

	return nil
}

// StartNFSServer starts the NFS server on the specified address.
// This should be called after join_mesh completes to bind to the mesh IP.
func (s *Server) StartNFSServer(addr string, tlsCert *tls.Certificate) error {
	if s.s3Store == nil || s.fileShareMgr == nil {
		return fmt.Errorf("S3 storage not initialized")
	}

	// Create NFS password store
	passwords := nfs.NewPasswordStore()

	// Create TLS config if certificate provided
	var tlsConfig *tls.Config
	if tlsCert != nil {
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{*tlsCert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	// Create and start NFS server
	s.nfsServer = nfs.NewServer(
		s.s3Store,
		s.fileShareMgr,
		s.s3Authorizer,
		passwords,
		nfs.Config{
			Address:   addr,
			TLSConfig: tlsConfig,
		},
	)

	if err := s.nfsServer.Start(); err != nil {
		return fmt.Errorf("start NFS server: %w", err)
	}

	return nil
}

// RegisterS3User registers a peer for S3 access.
// Returns the peer's S3 access key and secret key.
func (s *Server) RegisterS3User(peerID, publicKey string, roles []string) (accessKey, secretKey string, err error) {
	if s.s3Credentials == nil {
		return "", "", fmt.Errorf("S3 storage not initialized")
	}

	// Register credentials
	accessKey, secretKey, err = s.s3Credentials.RegisterUser(peerID, publicKey)
	if err != nil {
		return "", "", fmt.Errorf("register S3 credentials: %w", err)
	}

	// Bind roles
	for _, role := range roles {
		s.s3Authorizer.Bindings.Add(&auth.RoleBinding{
			Name:     fmt.Sprintf("%s-%s", peerID, role),
			PeerID:   peerID,
			RoleName: role,
		})
	}

	return accessKey, secretKey, nil
}

// S3SystemStore returns the system store for internal coordinator data.
// Returns nil if S3 is not enabled.
func (s *Server) S3SystemStore() *s3.SystemStore {
	return s.s3SystemStore
}

func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			s.jsonError(w, "missing authorization header", http.StatusUnauthorized)
			return
		}

		// Expect "Bearer <token>"
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" {
			s.jsonError(w, "invalid authorization header", http.StatusUnauthorized)
			return
		}

		if parts[1] != s.cfg.AuthToken {
			s.jsonError(w, "invalid token", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleTrace returns a runtime trace snapshot.
// The output is compatible with `go tool trace`.
func (s *Server) handleTrace(w http.ResponseWriter, _ *http.Request) {
	if !tracing.Enabled() {
		http.Error(w, "tracing not enabled (use --enable-tracing flag)", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=trace.out")

	if err := tracing.Snapshot(w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// validateAliases checks if all aliases are valid and available for the requesting peer.
// Returns an error if any alias conflicts. Must be called with peersMu held.
func (s *Server) validateAliases(aliases []string, requestingPeer string) error {
	for _, alias := range aliases {
		// Can't use another peer's name
		if _, exists := s.peers[alias]; exists && alias != requestingPeer {
			return fmt.Errorf("alias %q conflicts with existing peer name", alias)
		}

		// Can't use another peer's alias
		if owner, exists := s.aliasOwner[alias]; exists && owner != requestingPeer {
			return fmt.Errorf("alias %q is already registered by peer %q", alias, owner)
		}
	}
	return nil
}

// reservedPeerNames contains names that cannot be used for peer registration.
var reservedPeerNames = map[string]bool{
	"admin":         true,
	"administrator": true,
	"super":         true,
	"supervisor":    true,
	"peers":         true,
	"api":           true,
	"prometheus":    true,
	"grafana":       true,
	"health":        true,
	"assets":        true,
	"static":        true,
}

// isReservedPeerName checks if a peer name is reserved.
func isReservedPeerName(name string) bool {
	return reservedPeerNames[strings.ToLower(name)]
}

// nolint:gocyclo // Peer registration handles many edge cases and states
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req proto.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Reject reserved peer names
	if isReservedPeerName(req.Name) {
		s.jsonError(w, fmt.Sprintf("peer name %q is reserved", req.Name), http.StatusForbidden)
		return
	}

	// Load persisted peers BEFORE acquiring peersMu to avoid holding the lock
	// during S3 I/O. Peer registration is idempotent, so a slightly stale read
	// is safe — worst case is a missed name collision caught on next registration.
	persistedPeers, _ := s.s3SystemStore.LoadPeers(context.Background())

	s.peersMu.Lock()
	defer s.peersMu.Unlock()

	// Check for hostname collision with different public key
	// Important: Check both active peers AND persisted peers to prevent admin spoofing
	// An attacker could register with a privileged name when the legitimate peer is offline
	originalName := req.Name
	needsRename := false

	// Check active peers first
	if existing, exists := s.peers[req.Name]; exists {
		if existing.peer.PublicKey != req.PublicKey {
			needsRename = true
		}
	}

	// Also check persisted peers in S3 (critical for security)
	if !needsRename && persistedPeers != nil {
		for _, peer := range persistedPeers {
			if peer.Name == req.Name && peer.PublicKey != req.PublicKey {
				needsRename = true
				log.Warn().
					Str("name", req.Name).
					Str("existing_peer_id", peer.ID).
					Str("new_public_key", req.PublicKey).
					Msg("peer name already registered with different key in S3")
				break
			}
		}
	}

	if needsRename {
		// Different device trying to use same name - find unique suffix
		baseName := req.Name
		for i := 2; ; i++ {
			candidateName := fmt.Sprintf("%s-%d", baseName, i)
			// Check both active and persisted peers for uniqueness
			activeTaken := false
			if _, exists := s.peers[candidateName]; exists {
				activeTaken = true
			}
			persistedTaken := false
			if !activeTaken && persistedPeers != nil {
				for _, peer := range persistedPeers {
					if peer.Name == candidateName {
						persistedTaken = true
						break
					}
				}
			}
			if !activeTaken && !persistedTaken {
				req.Name = candidateName
				log.Info().
					Str("original", originalName).
					Str("assigned", req.Name).
					Msg("hostname conflict - assigned unique name")
				break
			}
		}
	}

	// Validate aliases first (before any state changes)
	if len(req.Aliases) > 0 {
		if err := s.validateAliases(req.Aliases, req.Name); err != nil {
			s.jsonError(w, err.Error(), http.StatusConflict)
			return
		}
	}

	// Allocate IP deterministically based on peer name
	// This ensures the same peer always gets the same IP
	meshIP, err := s.ipAlloc.allocateForPeer(req.Name)
	if err != nil {
		s.jsonError(w, "failed to allocate IP: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Preserve existing location if re-registering without new location
	// Only process locations if the feature is enabled
	var location *proto.GeoLocation
	var needsGeoLookup bool
	var geoLookupIP string

	existing, isExisting := s.peers[req.Name]

	if s.cfg.Coordinator.Locations {
		switch {
		case req.Location != nil:
			// Manual location provided - use it
			location = req.Location
		case isExisting && existing.peer.Location != nil:
			// Existing peer has location - check if we should keep it
			existingLoc := existing.peer.Location
			switch existingLoc.Source {
			case "manual":
				// Always preserve manual locations
				location = existingLoc
			case "ip":
				if len(req.PublicIPs) > 0 && len(existing.peer.PublicIPs) > 0 {
					// IP-based location - keep if IP hasn't changed
					if req.PublicIPs[0] == existing.peer.PublicIPs[0] {
						location = existingLoc
					} else {
						// IP changed - need new lookup
						needsGeoLookup = true
						geoLookupIP = req.PublicIPs[0]
					}
				} else {
					location = existingLoc
				}
			default:
				// Keep existing location as fallback
				location = existingLoc
			}
		case len(req.PublicIPs) > 0:
			// New peer with public IPs - need geolocation lookup
			needsGeoLookup = true
			geoLookupIP = req.PublicIPs[0]
		}
	}

	peer := &proto.Peer{
		Name:              req.Name,
		PublicKey:         req.PublicKey,
		PublicIPs:         req.PublicIPs,
		PrivateIPs:        req.PrivateIPs,
		SSHPort:           req.SSHPort,
		UDPPort:           req.UDPPort,
		MeshIP:            meshIP,
		LastSeen:          time.Now(),
		Connectable:       len(req.PublicIPs) > 0 && !req.BehindNAT,
		BehindNAT:         req.BehindNAT,
		Version:           req.Version,
		Location:          location,
		AllowsExitTraffic: req.AllowsExitTraffic,
		ExitPeer:          req.ExitPeer,
		IsCoordinator:     req.IsCoordinator,
	}

	// Preserve registeredAt for existing peers
	registeredAt := time.Now()
	if isExisting {
		registeredAt = existing.registeredAt
		// Clean up old aliases before registering new ones
		for _, oldAlias := range existing.aliases {
			delete(s.aliasOwner, oldAlias)
			delete(s.dnsCache, oldAlias)
		}
	}

	// Register new aliases
	for _, alias := range req.Aliases {
		s.aliasOwner[alias] = req.Name
		s.dnsCache[alias] = meshIP
	}

	// Compute peer ID from peer's SSH public key for RBAC purposes
	var peerID string
	if isExisting && existing.peerID != "" {
		// Preserve existing peer ID
		peerID = existing.peerID
	} else if req.PublicKey != "" {
		// Derive peer ID from SSH public key
		if edPubKey, err := config.DecodeED25519PublicKey(req.PublicKey); err == nil {
			peerID = auth.ComputePeerID(edPubKey)
		} else {
			log.Debug().Err(err).Str("peer", req.Name).Msg("failed to derive peer ID from public key")
		}
	}

	info := &peerInfo{
		peer:          peer,
		registeredAt:  registeredAt,
		aliases:       req.Aliases,
		peerID:        peerID,
		hasMonitoring: req.HasMonitoring,
	}
	s.peers[req.Name] = info
	s.dnsCache[req.Name] = meshIP

	// Add to coordinators index for O(1) lookups
	if peer.IsCoordinator {
		s.coordinators[req.Name] = info

		// Broadcast updated coordinator list to all connected peers
		go s.broadcastCoordinatorList()
	}

	// Persist DNS data to S3 (async to avoid blocking registration)
	// Tracked by WaitGroup so Shutdown waits for completion before cleanup.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.saveDNSData()
	}()

	// Update replicator peer list if this is a coordinator
	if peer.IsCoordinator && s.replicator != nil {
		// SECURITY FIX #4: Don't add self to replication targets
		// Check peer name instead of mesh IP to avoid race condition with storeCoordIPs()
		// which happens asynchronously after registration completes
		if req.Name != s.cfg.Name {
			s.replicator.AddPeer(meshIP, req.Name)
			log.Debug().Str("peer", req.Name).Str("mesh_ip", meshIP).Msg("added coordinator to replication targets")
		} else {
			log.Debug().Str("peer", req.Name).Msg("skipping self-replication (coordinator registering itself)")
		}
	}

	// Auto-register peer: add to "everyone" group and create peer record on first registration
	// Peer identity is derived from peer's SSH key - no separate registration needed
	isAdmin := false
	if peerID != "" && s.s3Authorizer != nil && s.s3Authorizer.Groups != nil {
		isNewPeer := !s.s3Authorizer.Groups.IsMember(auth.GroupEveryone, peerID)
		if isNewPeer {
			groupsModified := false

			// Check if this peer should be added to admins group (via admin_peers config)
			// Supports both peer names (for convenience) and peer IDs (for security)
			// Peer IDs are preferred as they're immutable (derived from SSH public key)
			isAdminPeer := false
			for _, adminEntry := range s.cfg.Coordinator.AdminPeers {
				// Check if entry matches peer ID (SHA256 hash of SSH key) - most secure
				if peerID != "" && adminEntry == peerID {
					isAdminPeer = true
					log.Info().Str("peer", req.Name).Str("peer_id", peerID).Msg("matched admin_peers entry by peer ID (secure)")
					break
				}
				// Fallback to name matching with glob pattern support (less secure due to mutability)
				// path.Match supports *, ?, [...] shell-style glob patterns - no regex injection risk
				if matched, err := path.Match(adminEntry, req.Name); err != nil {
					log.Warn().Err(err).Str("pattern", adminEntry).Msg("invalid admin_peers glob pattern, skipping")
				} else if matched {
					isAdminPeer = true
					log.Warn().Str("peer", req.Name).Str("peer_id", peerID).Str("pattern", adminEntry).Msg("matched admin_peers by name pattern (peer ID matching is more secure)")
					break
				}
			}
			if isAdminPeer {
				if err := s.s3Authorizer.Groups.AddMember(auth.GroupAdmins, peerID); err != nil {
					log.Warn().Err(err).Str("peer_id", peerID).Str("peer", req.Name).Msg("failed to add peer to admins group")
				} else {
					log.Info().Str("peer", req.Name).Str("peer_id", peerID).Msg("peer added to admins group via admin_peers config")
					groupsModified = true
					isAdmin = true
				}
			}

			// Add to everyone group
			if err := s.s3Authorizer.Groups.AddMember(auth.GroupEveryone, peerID); err != nil {
				log.Warn().Err(err).Str("peer", req.Name).Str("peer_id", peerID).Msg("failed to add peer to everyone group")
			} else {
				log.Debug().Str("peer", req.Name).Str("peer_id", peerID).Msg("added peer to everyone group")
				groupsModified = true
			}

			// Save groups once after all modifications (prevents redundant S3 writes)
			if groupsModified {
				if err := s.s3SystemStore.SaveGroups(context.Background(), s.s3Authorizer.Groups.List()); err != nil {
					log.Error().Err(err).Msg("failed to persist groups after peer registration")
				} else {
					log.Debug().Str("peer", req.Name).Msg("persisted group changes to S3")
				}
			}
		}

		// Create or update peer record in peer store
		s.updatePeerRecord(peerID, req.Name, req.PublicKey, isNewPeer)

		// Auto-create peer share on first registration
		// Tracked by WaitGroup so Shutdown waits for completion before cleanup.
		if isNewPeer && s.fileShareMgr != nil {
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.createPeerShare(peerID, req.Name)
			}()
		}

		// Check if peer is admin (for both new and existing peers)
		if s.s3Authorizer.Groups.IsMember(auth.GroupAdmins, peerID) {
			isAdmin = true
		}
	}

	// Generate JWT token for relay authentication
	token, err := s.GenerateToken(req.Name, meshIP)
	if err != nil {
		s.jsonError(w, "failed to generate token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	logEvent := log.Info().
		Str("name", req.Name).
		Str("mesh_ip", meshIP)
	if len(req.Aliases) > 0 {
		logEvent = logEvent.Strs("aliases", req.Aliases)
	}
	logEvent.Msg("peer registered")

	// Trigger IP geolocation only for new peers or when IP has changed (if locations enabled)
	if needsGeoLookup && s.cfg.Coordinator.Locations && s.ipGeoCache != nil {
		go s.lookupPeerLocation(req.Name, geoLookupIP)
	}

	// Get coordinator mesh IPs safely (uses atomic.Pointer).
	// May be empty during coordinator self-registration at startup — the coordinator's
	// DNS resolver reads directly from the server's atomic via GetCoordMeshIPs() instead.
	coordIPs := s.GetCoordMeshIPs()

	// If registering peer is a coordinator, include list of all known coordinators
	// This enables immediate bidirectional replication without separate ListPeers() call
	var coordinators []string
	var coordinatorNames map[string]string
	if peer.IsCoordinator {
		for _, peerInfo := range s.peers {
			if peerInfo.peer.IsCoordinator && peerInfo.peer.MeshIP != meshIP {
				coordinators = append(coordinators, peerInfo.peer.MeshIP)
				if coordinatorNames == nil {
					coordinatorNames = make(map[string]string)
				}
				coordinatorNames[peerInfo.peer.MeshIP] = peerInfo.peer.Name
			}
		}
		// Include self so the registering coordinator discovers THIS coordinator as a
		// replication peer. Without this, getPeerName(msg.From) returns "" on the
		// registrant and all manifest-based purges are silently skipped.
		// Guards: skip if self-registering (would cause coordinator to replicate to itself),
		// or if own mesh IP not yet known (SetCoordMeshIP not yet called).
		if req.Name != s.cfg.Name && len(coordIPs) > 0 {
			selfMeshIP := coordIPs[0] // Self is always first in GetCoordMeshIPs()
			if coordinatorNames == nil {
				coordinatorNames = make(map[string]string)
			}
			coordinators = append(coordinators, selfMeshIP)
			coordinatorNames[selfMeshIP] = s.cfg.Name
		}
	}

	resp := proto.RegisterResponse{
		MeshIP:           meshIP,
		MeshCIDR:         mesh.CIDR,
		Domain:           mesh.DomainSuffix,
		Token:            token,
		CoordMeshIPs:     coordIPs, // All coordinator IPs for DNS round-robin
		ServerVersion:    s.version,
		PeerName:         req.Name, // May differ from original request if renamed
		PeerID:           peerID,
		IsAdmin:          isAdmin,
		Coordinators:     coordinators,     // For immediate replication setup
		CoordinatorNames: coordinatorNames, // For replication ownership tracking
	}

	// Generate TLS certificate for the peer
	if s.ca != nil {
		certPEM, keyPEM, err := s.ca.GeneratePeerCert(req.Name, mesh.DomainSuffix, meshIP)
		if err != nil {
			log.Warn().Err(err).Str("peer", req.Name).Msg("failed to generate TLS cert")
		} else {
			resp.TLSCert = string(certPEM)
			resp.TLSKey = string(keyPEM)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.peersMu.RLock()
	defer s.peersMu.RUnlock()

	peers := make([]proto.Peer, 0, len(s.peers))
	for _, info := range s.peers {
		peers = append(peers, *info.peer)
	}

	resp := proto.PeerListResponse{Peers: peers}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePeerByName(w http.ResponseWriter, r *http.Request) {
	// Extract peer name from path
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/peers/")
	if name == "" {
		s.jsonError(w, "peer name required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.peersMu.RLock()
		info, exists := s.peers[name]
		s.peersMu.RUnlock()

		if !exists {
			s.jsonError(w, "peer not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(info.peer)

	case http.MethodDelete:
		s.peersMu.Lock()
		info, exists := s.peers[name]
		if exists {
			s.ipAlloc.release(info.peer.MeshIP)
			// Clean up aliases
			for _, alias := range info.aliases {
				delete(s.aliasOwner, alias)
				delete(s.dnsCache, alias)
			}
			delete(s.peers, name)
			delete(s.dnsCache, name)

			// Remove from coordinators index
			wasCoordinator := info.peer.IsCoordinator
			if wasCoordinator {
				delete(s.coordinators, name)
			}

			// Remove from replicator if this was a coordinator
			if wasCoordinator && s.replicator != nil {
				s.replicator.RemovePeer(info.peer.MeshIP)
				log.Debug().Str("peer", name).Str("mesh_ip", info.peer.MeshIP).Msg("removed coordinator from replication targets")
			}

			// Broadcast updated coordinator list if a coordinator was removed
			if wasCoordinator {
				go s.broadcastCoordinatorList()
			}
		}
		s.peersMu.Unlock()

		// Also remove UDP endpoint
		if s.holePunch != nil {
			s.holePunch.RemoveEndpoint(name)
		}

		if !exists {
			s.jsonError(w, "peer not found", http.StatusNotFound)
			return
		}

		log.Info().Str("name", name).Msg("peer deregistered")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	default:
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleDNS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.peersMu.RLock()
	defer s.peersMu.RUnlock()

	records := make([]proto.DNSRecord, 0, len(s.dnsCache))
	for hostname, ip := range s.dnsCache {
		records = append(records, proto.DNSRecord{
			Hostname: hostname,
			MeshIP:   ip,
		})
	}

	resp := proto.DNSUpdateNotification{Records: records}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleReplicationMessage handles incoming replication messages from other coordinators.
func (s *Server) handleReplicationMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// SECURITY: Verify this is a replication message
	if r.Header.Get("X-Replication-Protocol") != "v1" {
		s.jsonError(w, "invalid replication protocol version", http.StatusBadRequest)
		return
	}

	// Authenticate the sender via TLS client cert or known mesh IP.
	// We don't additionally check coordinator status because:
	// 1. This endpoint is only reachable within the mesh (admin server binds to mesh IP)
	// 2. Only coordinators have the code to send replication messages
	// 3. Coordinator discovery is itself distributed via replication, creating a
	//    chicken-and-egg problem if we require coordinator verification here
	// 4. The replication protocol has its own integrity checks (checksums, versioning)
	peerName := s.getRequestOwner(r)
	if peerName == "" {
		log.Warn().Str("remote_addr", r.RemoteAddr).Msg("replication request from unauthenticated peer")
		s.jsonError(w, "authentication required", http.StatusUnauthorized)
		return
	}

	from, _, _ := net.SplitHostPort(r.RemoteAddr)

	// SECURITY FIX #2: Limit message size to prevent OOM attacks (100MB max)
	const maxReplicationMessageSize = 100 * 1024 * 1024 // 100MB
	if r.ContentLength < 0 {
		s.jsonError(w, "content-length required", http.StatusBadRequest)
		return
	}
	if r.ContentLength > maxReplicationMessageSize {
		log.Warn().
			Int64("size", r.ContentLength).
			Int64("max", maxReplicationMessageSize).
			Str("peer", peerName).
			Msg("replication message exceeds size limit")
		s.jsonError(w, "message too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Read message data with bounded allocation
	data := make([]byte, r.ContentLength)
	if _, err := io.ReadFull(r.Body, data); err != nil {
		s.jsonError(w, "failed to read message", http.StatusBadRequest)
		return
	}

	// Handle via mesh transport (from was determined during auth above)
	if s.meshTransport != nil {
		if err := s.meshTransport.HandleIncomingMessage(from, data); err != nil {
			log.Warn().Err(err).Str("from", from).Str("peer", peerName).Msg("failed to handle replication message")
			s.jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusAccepted)
}

// updatePeerRecord creates or updates a peer record in the peer store.
// This is called during peer registration to ensure the peer exists in the peer list.
func (s *Server) updatePeerRecord(peerID, peerName, publicKey string, isNewPeer bool) {
	peers, err := s.s3SystemStore.LoadPeers(context.Background())
	if err != nil {
		log.Warn().Err(err).Str("peer_id", peerID).Msg("failed to load peers for update")
		peers = []*auth.Peer{}
	}

	now := time.Now()
	var found bool
	for _, u := range peers {
		if u.ID == peerID {
			// Update existing peer: refresh last seen time and update name if this is the same device
			u.LastSeen = now
			// Update name if current peer provides one and peer name is empty or matches this peer
			if peerName != "" && (u.Name == "" || u.Name == peerName) {
				u.Name = peerName
			}
			found = true
			break
		}
	}

	if !found {
		// Create new peer
		peers = append(peers, &auth.Peer{
			ID:        peerID,
			Name:      peerName,
			PublicKey: publicKey,
			CreatedAt: now,
			LastSeen:  now,
		})
		log.Debug().Str("peer_id", peerID).Str("name", peerName).Msg("created peer record")
	}

	if err := s.s3SystemStore.SavePeers(context.Background(), peers); err != nil {
		log.Warn().Err(err).Str("peer_id", peerID).Msg("failed to save peers")
	} else {
		// Refresh peer name cache after adding new peer
		if err := s.refreshPeerNameCache(); err != nil {
			log.Warn().Err(err).Msg("failed to refresh peer name cache")
		}
	}
}

// createPeerShare creates a default file share for a newly registered peer.
// This is called in a background goroutine to avoid blocking registration.
func (s *Server) createPeerShare(peerID, peerName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shareName := peerName + "_share"

	quota := s.cfg.Coordinator.S3.DefaultShareQuota.Bytes()
	opts := &s3.FileShareOptions{
		GuestRead:         true,
		GuestReadSet:      true,
		ReplicationFactor: 2,
	}

	share, err := s.fileShareMgr.Create(ctx, shareName, "Personal share", peerID, quota, opts)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return // Share already exists (re-registration), nothing to do
		}
		log.Warn().Err(err).Str("peer", peerName).Str("share", shareName).Msg("failed to auto-create peer share")
		return
	}

	// Persist bindings
	if s.s3SystemStore != nil {
		if err := s.s3SystemStore.SaveGroupBindings(ctx, s.s3Authorizer.GroupBindings.List()); err != nil {
			log.Warn().Err(err).Msg("failed to persist group bindings for auto-created share")
		}
		if err := s.s3SystemStore.SaveBindings(ctx, s.s3Authorizer.Bindings.List()); err != nil {
			log.Warn().Err(err).Msg("failed to persist role bindings for auto-created share")
		}
	}

	// Trigger immediate replication of file share metadata so other coordinators
	// learn about the new share before PurgeOrphanedFileShareBuckets runs.
	if s.replicator != nil {
		s.replicator.EnqueueReplication(s3.SystemBucket, s3.FileSharesPath, "put")
	}

	log.Info().Str("peer", peerName).Str("share", share.Name).Msg("auto-created peer share")
}

func (s *Server) jsonError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(proto.ErrorResponse{
		Error:   http.StatusText(code),
		Code:    code,
		Message: message,
	})
}

// lookupPeerLocation performs IP geolocation lookup for a peer and updates their location.
// This is called in a background goroutine to avoid blocking registration.
func (s *Server) lookupPeerLocation(peerName, ip string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	location, err := s.ipGeoCache.Lookup(ctx, ip)
	if err != nil {
		log.Debug().Err(err).Str("peer", peerName).Str("ip", ip).Msg("IP geolocation lookup failed")
		return
	}

	if location == nil {
		// IP could not be geolocated (e.g., private IP)
		return
	}

	// Update peer's location
	s.peersMu.Lock()
	if info, ok := s.peers[peerName]; ok && info.peer.Location == nil {
		info.peer.Location = location
		log.Info().
			Str("peer", peerName).
			Str("city", location.City).
			Str("country", location.Country).
			Msg("peer location updated via IP geolocation")
	}
	s.peersMu.Unlock()
}

// ListenAndServe starts the coordination server.
func (s *Server) ListenAndServe() error {
	log.Info().Str("listen", s.cfg.Coordinator.Listen).Msg("starting coordination server")
	return http.ListenAndServe(s.cfg.Coordinator.Listen, s)
}

// GetCoordMeshIPs returns the current list of coordinator mesh IPs.
func (s *Server) GetCoordMeshIPs() []string {
	if set := s.coordIPs.Load(); set != nil {
		return set.original
	}
	return nil
}

// getSortedCoordIPs returns the cached sorted coordinator IP list for deterministic hashing.
func (s *Server) getSortedCoordIPs() []string {
	if set := s.coordIPs.Load(); set != nil {
		return set.sorted
	}
	return nil
}

// storeCoordIPs stores both the original and sorted coordinator IP lists as a single
// atomic unit, preventing inconsistent reads between the two lists during updates.
func (s *Server) storeCoordIPs(ips []string) {
	sorted := make([]string, len(ips))
	copy(sorted, ips)
	sort.Strings(sorted)
	s.coordIPs.Store(&coordIPSet{original: ips, sorted: sorted})
}

// OnCoordIPsChanged registers a callback that fires whenever the coordinator IP list changes.
// Used to keep the local DNS resolver in sync.
func (s *Server) OnCoordIPsChanged(fn func([]string)) {
	s.coordIPsCb = fn
}

// SetCoordMeshIP sets the coordinator's own mesh IP for "this.tunnelmesh" resolution.
// This is called after join_mesh completes so other peers can resolve "this" to the coordinator.
// The full list of coordinator IPs is built dynamically from registered coordinators.
func (s *Server) SetCoordMeshIP(ip string) {
	// Build full list: self + all other coordinators
	s.peersMu.RLock()
	ips := []string{ip}
	for _, ci := range s.coordinators {
		if ci.peer.MeshIP != ip {
			ips = append(ips, ci.peer.MeshIP)
		}
	}
	s.peersMu.RUnlock()
	s.storeCoordIPs(ips)
	log.Info().Strs("ips", ips).Msg("coordinator mesh IPs set for 'this.tunnelmesh' resolution")
	if s.coordIPsCb != nil {
		s.coordIPsCb(ips)
	}

	// Persist coordinator IPs to system store (replicated to all coordinators)
	if s.s3SystemStore != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.s3SystemStore.SaveCoordinatorIPs(context.Background(), ips); err != nil {
				log.Warn().Err(err).Msg("failed to persist coordinator IPs")
			}
		}()
	}
}

// broadcastCoordinatorList sends the updated coordinator IP list to all connected peers.
// This is called when a coordinator joins or leaves the mesh.
func (s *Server) broadcastCoordinatorList() {
	// Build IP list from coordinators map + self, store atomically while holding the lock
	// to prevent races between list building and storage.
	s.peersMu.RLock()
	var ips []string
	// Include self first (original list always starts with self)
	if existing := s.GetCoordMeshIPs(); len(existing) > 0 {
		ips = append(ips, existing[0]) // Self IP is always first
	}
	for _, ci := range s.coordinators {
		// Skip self (already included above)
		if len(ips) > 0 && ci.peer.MeshIP == ips[0] {
			continue
		}
		ips = append(ips, ci.peer.MeshIP)
	}
	if len(ips) > 0 {
		s.storeCoordIPs(ips)
	}
	s.peersMu.RUnlock()

	if len(ips) == 0 {
		return
	}

	// Persist coordinator IPs to system store (replicated to all coordinators)
	if s.s3SystemStore != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.s3SystemStore.SaveCoordinatorIPs(context.Background(), ips); err != nil {
				log.Warn().Err(err).Msg("failed to persist coordinator IPs")
			}
		}()
	}

	// Update local DNS resolver
	if s.coordIPsCb != nil {
		s.coordIPsCb(ips)
	}

	// Broadcast to all connected peers via relay
	if s.relay != nil {
		s.relay.BroadcastCoordListUpdate(ips)
	}

	log.Info().Strs("ips", ips).Msg("broadcast coordinator list update")
}

// SetMetricsRegistry initializes coordinator metrics with the given registry.
// This should be called after the server is created if you want coordinator
// metrics to be exposed on a specific registry (e.g., metrics.Registry for
// the peer /metrics endpoint).
func (s *Server) SetMetricsRegistry(registry prometheus.Registerer) {
	s.metricsRegistry = registry
	s.coordMetrics = InitCoordMetrics(registry)

	// Initialize S3 metrics on the correct registry and wire into the S3 server.
	// This must happen here (not in initS3Storage) because the registry is not
	// available during server construction.
	s3Metrics := s3.InitS3Metrics(registry)
	if s.s3Server != nil {
		s.s3Server.SetMetrics(s3Metrics)
	}

	// Refresh storage metrics now that metrics are initialized.
	// StartPeriodicCleanup may have already run its startup update before
	// the registry was set, so those updates would have been silently skipped.
	go func() {
		s.updateS3Metrics()
		s.collectAndPersistCapacity(context.Background())
	}()

	log.Debug().Msg("coordinator metrics initialized")
}

// SetDockerManager sets the Docker manager for the coordinator.
// This is called when the coordinator joins the mesh and has Docker enabled.
func (s *Server) SetDockerManager(mgr *docker.Manager) {
	s.dockerMgr = mgr
	log.Info().Msg("Docker manager initialized on coordinator")
}

// GetSystemStore returns the S3 system store for the coordinator.
// Returns nil if S3 is not enabled.
func (s *Server) GetSystemStore() *s3.SystemStore {
	return s.s3SystemStore
}

// GetReplicator returns the replicator for multi-coordinator deployments.
// Returns nil if replication is not enabled.
func (s *Server) GetReplicator() *replication.Replicator {
	return s.replicator
}

// StartAdminServer starts the admin interface on the specified address.
// If tlsCert is provided, the server uses HTTPS; otherwise HTTP.
// This is called after join_mesh completes to bind admin to the mesh IP.
// Uses SO_REUSEADDR to allow binding to mesh IP even when main server uses wildcard.
func (s *Server) StartAdminServer(addr string, tlsCert *tls.Certificate) error {
	if s.adminMux == nil {
		return fmt.Errorf("admin routes not initialized")
	}

	// Create listener with SO_REUSEADDR to allow binding to specific IP
	// even when main server is bound to 0.0.0.0 on the same port
	lc := net.ListenConfig{
		Control: setReuseAddr,
	}

	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	s.adminServer = &http.Server{
		Handler: redirectToCanonicalDomain(s.adminMux),
	}

	if tlsCert != nil {
		s.adminServer.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{*tlsCert},
			MinVersion:   tls.VersionTLS12,
			// Request client certs for user identification, but don't require them
			// This allows getRequestOwner() to identify users for operations like share creation
			ClientAuth: tls.RequestClientCert,
		}
		log.Info().Str("addr", addr).Msg("starting admin server (HTTPS)")
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.adminServer.ServeTLS(ln, "", ""); err != nil && err != http.ErrServerClosed {
				log.Error().Err(err).Msg("admin server error")
			}
		}()
	} else {
		log.Info().Str("addr", addr).Msg("starting admin server (HTTP)")
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.adminServer.Serve(ln); err != nil && err != http.ErrServerClosed {
				log.Error().Err(err).Msg("admin server error")
			}
		}()
	}

	return nil
}

// GetCA returns the server's certificate authority.
func (s *Server) GetCA() *CertificateAuthority {
	return s.ca
}

// setupWireGuardRoutes registers the WireGuard API routes for concentrator sync.
func (s *Server) setupWireGuardRoutes() {
	// Concentrator fetches client list (JWT auth)
	s.mux.HandleFunc("/api/v1/wireguard/clients", s.handleWireGuardClients)
	// Concentrator reports handshakes (JWT auth)
	s.mux.HandleFunc("/api/v1/wireguard/handshake", s.handleWireGuardHandshake)
}

// handleWireGuardClients returns the list of WireGuard clients for the concentrator.
// Uses JWT authentication.
func (s *Server) handleWireGuardClients(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate JWT token
	auth := r.Header.Get("Authorization")
	if auth == "" {
		s.jsonError(w, "missing authorization header", http.StatusUnauthorized)
		return
	}

	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		s.jsonError(w, "invalid authorization header", http.StatusUnauthorized)
		return
	}

	_, err := s.ValidateToken(parts[1])
	if err != nil {
		s.jsonError(w, "invalid token", http.StatusUnauthorized)
		return
	}

	// Return only enabled clients
	allClients := s.wgStore.List()
	enabledClients := make([]wireguard.Client, 0)
	for _, c := range allClients {
		if c.Enabled {
			enabledClients = append(enabledClients, c)
		}
	}

	resp := wireguard.ClientListResponse{
		Clients: enabledClients,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleWireGuardHandshake updates the last seen time for a WireGuard client.
// Called by the concentrator when it detects a client handshake.
func (s *Server) handleWireGuardHandshake(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate JWT token
	auth := r.Header.Get("Authorization")
	if auth == "" {
		s.jsonError(w, "missing authorization header", http.StatusUnauthorized)
		return
	}

	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		s.jsonError(w, "invalid authorization header", http.StatusUnauthorized)
		return
	}

	_, err := s.ValidateToken(parts[1])
	if err != nil {
		s.jsonError(w, "invalid token", http.StatusUnauthorized)
		return
	}

	var req wireguard.HandshakeReport
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.wgStore.UpdateLastSeen(req.ClientID, req.HandshakeAt); err != nil {
		s.jsonError(w, "client not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
