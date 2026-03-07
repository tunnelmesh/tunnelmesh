package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"golang.org/x/time/rate"
)

const (
	// maxPendingFetchRequests is the maximum number of concurrent chunk fetch requests
	// to prevent unbounded memory growth from DoS attacks
	maxPendingFetchRequests = 10000
)

// Transport defines the interface for sending replication messages.
// This will be implemented by the mesh transport layer (UDP/SSH/Relay).
type Transport interface {
	// SendToCoordinator sends a message to another coordinator by mesh IP
	SendToCoordinator(ctx context.Context, coordMeshIP string, data []byte) error

	// RegisterHandler registers a handler for incoming replication messages.
	// The context carries the W3C trace context extracted from the HTTP request headers,
	// allowing the receiving coordinator to create child spans under the sender's trace.
	RegisterHandler(handler func(ctx context.Context, from string, data []byte) error)
}

// ObjectMeta represents S3 object metadata including chunk information.
// This is a minimal representation used by replication - full definition in s3 package.
type ObjectMeta struct {
	Key           string
	Size          int64
	ContentType   string
	Metadata      map[string]string
	Chunks        []string                  // Ordered list of chunk hashes
	ChunkMetadata map[string]*ChunkMetadata // Per-chunk metadata
	VersionVector map[string]uint64         // File-level version vector
}

// ChunkMetadata represents metadata for a single chunk.
type ChunkMetadata struct {
	Hash          string
	Size          int64
	VersionVector map[string]uint64
}

// CapacityChecker checks whether a coordinator has sufficient storage capacity.
type CapacityChecker interface {
	HasCapacityFor(coordinatorName string, bytes int64) bool
}

// S3Store defines the interface for S3 storage operations.
type S3Store interface {
	// Get retrieves an object from S3
	Get(ctx context.Context, bucket, key string) (data []byte, metadata map[string]string, err error)

	// Put stores an object in S3
	Put(ctx context.Context, bucket, key string, data []byte, contentType string, metadata map[string]string) error

	// Delete removes an object from S3
	Delete(ctx context.Context, bucket, key string) error

	// List lists all objects in a bucket
	List(ctx context.Context, bucket string) ([]string, error)

	// ListBuckets lists all buckets
	ListBuckets(ctx context.Context) ([]string, error)

	// Chunk-level operations (added in Phase 4)

	// GetObjectMeta retrieves object metadata without loading chunk data
	GetObjectMeta(ctx context.Context, bucket, key string) (*ObjectMeta, error)

	// ChunkExists checks if a chunk exists in CAS without reading its data.
	ChunkExists(ctx context.Context, hash string) bool

	// ReadChunk reads a chunk from CAS by hash (returns decrypted plaintext)
	ReadChunk(ctx context.Context, hash string) ([]byte, error)

	// ReadChunkRaw reads the raw on-disk (encrypted+compressed) bytes for a chunk.
	// Used by the replication sender to avoid decrypt+decompress overhead.
	ReadChunkRaw(ctx context.Context, hash string) ([]byte, error)

	// WriteChunkDirect writes plaintext chunk data directly to CAS (for replication receiver)
	WriteChunkDirect(ctx context.Context, hash string, data []byte) error

	// WriteChunkDirectRaw writes raw (encrypted+compressed) bytes directly to CAS.
	// Used by the replication receiver when the sender set RawTransfer=true.
	WriteChunkDirectRaw(ctx context.Context, hash string, raw []byte) error

	// ImportObjectMeta writes object metadata directly (for replication receiver).
	// bucketOwner is used when auto-creating the bucket (empty = "system").
	// Returns chunk hashes from pruned old versions that may now be unreferenced.
	ImportObjectMeta(ctx context.Context, bucket, key string, metaJSON []byte, bucketOwner string) ([]string, error)

	// DeleteUnreferencedChunks checks each chunk hash and deletes unreferenced ones.
	// Returns total bytes freed.
	DeleteUnreferencedChunks(ctx context.Context, chunkHashes []string) int64

	// DeleteChunk removes a chunk from CAS by hash (for cleanup after replication)
	DeleteChunk(ctx context.Context, hash string) error

	// GetBucketReplicationFactor returns the replication factor for a bucket (0 if unknown)
	GetBucketReplicationFactor(ctx context.Context, bucket string) int

	// PurgeObject permanently removes an object, its versions, and unreferenced chunks.
	// Used for replicated deletes where tombstoning is unnecessary.
	PurgeObject(ctx context.Context, bucket, key string) error

	// GetVersionHistory returns archived version entries for an object (version ID + full meta JSON).
	// The current (live) version is not included; only entries from the versions/ directory.
	GetVersionHistory(ctx context.Context, bucket, key string) ([]VersionEntry, error)

	// ImportVersionHistory imports version entries, skipping any that already exist (dedup by versionID).
	// After importing, prunes expired versions to enforce local retention policy.
	// Returns count of newly imported versions and chunk hashes from pruned versions for GC.
	ImportVersionHistory(ctx context.Context, bucket, key string, versions []VersionEntry) (int, []string, error)

	// GetAllObjectKeys returns all object keys grouped by bucket.
	// Used by the rebalancer to iterate all objects for redistribution.
	GetAllObjectKeys(ctx context.Context) (map[string][]string, error)

	// GetBucketOwner returns the owner of a bucket (empty string if bucket not found).
	// Used by manifest reconciliation to validate sender ownership.
	GetBucketOwner(ctx context.Context, bucket string) string

	// GetActiveVersionIDs returns the set of all active version IDs across all buckets.
	// Used by CleanupOrphanedShards to identify EC shards whose parent version is gone.
	GetActiveVersionIDs(ctx context.Context) (map[string]bool, error)
}

// ChunkRegistryInterface defines operations for chunk ownership tracking.
type ChunkRegistryInterface interface {
	RegisterChunk(hash string, size int64) error
	UnregisterChunk(hash string) error
	GetOwners(hash string) ([]string, error)
	GetChunksOwnedBy(coordID string) ([]string, error)
	AddOwner(hash string, coordID string) error
	// CleanupOrphanedShards removes registry entries for EC shards whose parent
	// file version ID is no longer present in the active set. Returns count removed.
	CleanupOrphanedShards(activeFileIDs map[string]bool) int
	// CleanupStaleOwners removes coordinator entries that have not been refreshed
	// within maxAge. Returns the total count of ownership entries removed.
	CleanupStaleOwners(maxAge time.Duration) int
}

// Replicator manages replication of S3 data between coordinators.
type Replicator struct {
	nodeID               string
	transport            Transport
	s3                   S3Store
	state                *State
	chunkRegistry        ChunkRegistryInterface // Distributed chunk ownership tracking (Phase 4)
	capacityChecker      CapacityChecker        // Optional capacity pre-flight checker
	logger               zerolog.Logger
	maxPendingOperations int // Maximum pending ACKs (0 = unlimited)

	// Timeouts
	ackTimeout      time.Duration
	applyTimeout    time.Duration
	ackSendTimeout  time.Duration
	chunkAckTimeout time.Duration // Timeout for chunk-level ACKs (Phase 4)

	// Peer coordinators
	mu        sync.RWMutex
	peers     map[string]bool   // map[coordMeshIP]true
	peerNames map[string]string // map[coordMeshIP]coordinatorName

	// Pending ACKs (file-level)
	pendingMu sync.RWMutex
	pending   map[string]*pendingReplication // map[messageID]*pendingReplication

	// Pending chunk ACKs (chunk-level, Phase 4)
	pendingChunksMu        sync.RWMutex
	pendingChunks          map[string]*pendingChunkReplication        // map[messageID]*pendingChunkReplication
	pendingFetchRequests   map[string]chan *FetchChunkResponsePayload // map[requestID]chan (Phase 5)
	pendingFetchTimestamps map[string]time.Time                       // map[requestID]timestamp for TTL cleanup

	// Async ACK sending
	ackSendSem chan struct{} // Semaphore to bound concurrent async ACK sends

	// Outbound send concurrency control
	sendSem chan struct{} // Semaphore to limit concurrent outbound replication sends

	// Rate limiting
	rateLimiter *rate.Limiter // Limits incoming replication messages per second

	// Metrics (lock-free via atomics)
	sentCount        atomic.Uint64
	receivedCount    atomic.Uint64
	conflictCount    atomic.Uint64
	errorCount       atomic.Uint64
	droppedCount     atomic.Uint64 // Operations dropped due to pending limit
	rateLimited      atomic.Uint64 // Messages dropped due to rate limiting
	rateLimitLastLog atomic.Int64  // Unix nanoseconds of last rate-limit warn, for throttling

	// Rebalancer (nil if not configured)
	rebalancer *Rebalancer

	// Metadata import concurrency limiter — bounds concurrent ImportObjectMeta
	// calls to prevent writer queue buildup on Store.mu (see lock contention fix).
	metaImportSem chan struct{}

	// Deferred chunk cleanup — coalesces chunk hashes from metadata imports
	// and processes them in batches via a background worker, avoiding expensive
	// full-filesystem walks (buildChunkReferenceSet) in the HTTP handler path.
	deferredChunksMu sync.Mutex
	deferredChunks   map[string]struct{}
	deferredNotify   chan struct{} // buffered(1)

	// Replication queue (replaces per-PutObject goroutine approach)
	replQueue   chan struct{} // Notification channel (buffer=1)
	replPending sync.Map      // map["bucket\x00key"]*replQueueEntry

	// Chunk pipelining
	chunkPipelineWindow int

	// Auto-sync interval (0 = disabled)
	autoSyncInterval     time.Duration
	autoSyncInitialDelay time.Duration

	// Chunk registry periodic cleanup
	chunkRegistryCleanupInterval time.Duration
	chunkRegistryMaxOwnerAge     time.Duration

	// On-demand manifest sync trigger (buffered 1 — deduplicated).
	manifestSyncCh chan struct{}

	// preSyncHook is called at the start of each runAutoSyncCycle. Nil in production.
	// Tests set this to block the worker at a deterministic point.
	preSyncHook func()

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// pendingReplication tracks a replication operation awaiting ACK.
type pendingReplication struct {
	bucket  string
	key     string
	sentAt  time.Time
	ackChan chan *AckPayload
	timeout *time.Timer
}

// pendingChunkReplication tracks a chunk replication operation awaiting ACK (Phase 4).
type pendingChunkReplication struct {
	bucket     string
	key        string
	chunkHash  string
	chunkIndex int
	sentAt     time.Time
	ackChan    chan *ChunkAckPayload
	timeout    *time.Timer
}

// Config contains configuration for the replicator.
type Config struct {
	NodeID                       string
	Transport                    Transport
	S3Store                      S3Store
	ChunkRegistry                ChunkRegistryInterface // Optional: distributed chunk ownership tracking (Phase 4)
	CapacityChecker              CapacityChecker        // Optional: nil = no capacity checks
	Logger                       zerolog.Logger
	AckTimeout                   time.Duration   // How long to wait for ACK before retrying (default: 10s)
	RetryInterval                time.Duration   // How long to wait before retrying failed replication (default: 30s)
	MaxPendingOperations         int             // Maximum number of pending ACKs to track (0 = unlimited, default: 10k)
	RateLimit                    int             // Maximum incoming messages per second (0 = unlimited, default: 5000)
	RateBurst                    int             // Maximum burst size for rate limiter (default: 500)
	ApplyTimeout                 time.Duration   // Timeout for applying replication to S3 (default: 30s)
	AckSendTimeout               time.Duration   // Timeout for sending ACK messages (default: 5s)
	ChunkAckTimeout              time.Duration   // Timeout for chunk-level ACKs (default: 30s, Phase 4)
	MaxConcurrentSends           int             // Maximum concurrent outbound replication sends (default: 20)
	ChunkPipelineWindow          int             // Maximum concurrent chunk sends per ReplicateObject (default: 5)
	AutoSyncInterval             time.Duration   // How often to re-enqueue all objects for replication (default: 5min, 0 = disabled)
	AutoSyncInitialDelay         time.Duration   // Initial delay before first auto-sync (default: 2min). 0 uses default.
	ChunkRegistryCleanupInterval time.Duration   // How often to evict stale chunk owners (default: 1h, 0 uses default)
	ChunkRegistryMaxOwnerAge     time.Duration   // Max age for a coordinator's ownership entry (default: 7 days)
	Context                      context.Context // SECURITY FIX #3: Parent context for proper cancellation propagation
}

// NewReplicator creates a new replicator instance.
func NewReplicator(config Config) *Replicator {
	if config.AckTimeout == 0 {
		config.AckTimeout = 10 * time.Second
	}
	if config.RetryInterval == 0 {
		config.RetryInterval = 30 * time.Second
	}
	if config.MaxPendingOperations == 0 {
		config.MaxPendingOperations = 10000 // Default: 10k pending operations
	}
	if config.RateLimit == 0 {
		config.RateLimit = 5000 // Default: 5000 messages/second
	}
	if config.RateBurst == 0 {
		config.RateBurst = 500 // Default: burst of 500 messages
	}
	if config.ApplyTimeout == 0 {
		config.ApplyTimeout = 30 * time.Second
	}
	if config.AckSendTimeout == 0 {
		config.AckSendTimeout = 5 * time.Second
	}
	if config.ChunkAckTimeout == 0 {
		config.ChunkAckTimeout = 30 * time.Second // Default: 30s for chunk ACKs
	}
	if config.MaxConcurrentSends == 0 {
		config.MaxConcurrentSends = 20 // Default: 20 concurrent outbound replication sends
	}
	if config.ChunkPipelineWindow == 0 {
		config.ChunkPipelineWindow = 5 // Default: 5 concurrent chunk sends per object
	}
	if config.AutoSyncInterval == 0 {
		config.AutoSyncInterval = 5 * time.Minute // Default: 5 minutes
	}
	if config.AutoSyncInitialDelay == 0 {
		config.AutoSyncInitialDelay = 2 * time.Minute
	}
	if config.ChunkRegistryCleanupInterval == 0 {
		config.ChunkRegistryCleanupInterval = 1 * time.Hour
	}
	if config.ChunkRegistryMaxOwnerAge == 0 {
		config.ChunkRegistryMaxOwnerAge = 7 * 24 * time.Hour
	}

	// SECURITY FIX #3: Use provided context or create a new one
	// This allows proper context propagation and cancellation from parent
	parentCtx := config.Context
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(parentCtx)

	r := &Replicator{
		nodeID:               config.NodeID,
		transport:            config.Transport,
		s3:                   config.S3Store,
		state:                NewState(config.NodeID),
		chunkRegistry:        config.ChunkRegistry, // Optional (nil if not using chunk-level replication)
		capacityChecker:      config.CapacityChecker,
		logger:               config.Logger.With().Str("component", "replicator").Logger(),
		maxPendingOperations: config.MaxPendingOperations,
		ackTimeout:           config.AckTimeout,
		applyTimeout:         config.ApplyTimeout,
		ackSendTimeout:       config.AckSendTimeout,
		chunkAckTimeout:      config.ChunkAckTimeout,
		// Semaphore to bound concurrent async ACK sends. Cap of 100 allows
		// high parallelism for typical replication bursts while limiting
		// goroutine memory overhead (~8KB stack each). When full, callers
		// fall back to synchronous send rather than queueing indefinitely.
		ackSendSem:                   make(chan struct{}, 100),
		sendSem:                      make(chan struct{}, config.MaxConcurrentSends),
		rateLimiter:                  rate.NewLimiter(rate.Limit(config.RateLimit), config.RateBurst),
		peers:                        make(map[string]bool),
		peerNames:                    make(map[string]string),
		pending:                      make(map[string]*pendingReplication),
		pendingChunks:                make(map[string]*pendingChunkReplication),
		pendingFetchRequests:         make(map[string]chan *FetchChunkResponsePayload),
		pendingFetchTimestamps:       make(map[string]time.Time),
		metaImportSem:                make(chan struct{}, 3),
		deferredChunks:               make(map[string]struct{}),
		deferredNotify:               make(chan struct{}, 1),
		replQueue:                    make(chan struct{}, 1),
		manifestSyncCh:               make(chan struct{}, 1),
		chunkPipelineWindow:          config.ChunkPipelineWindow,
		autoSyncInterval:             config.AutoSyncInterval,
		autoSyncInitialDelay:         config.AutoSyncInitialDelay,
		chunkRegistryCleanupInterval: config.ChunkRegistryCleanupInterval,
		chunkRegistryMaxOwnerAge:     config.ChunkRegistryMaxOwnerAge,
		ctx:                          ctx,
		cancel:                       cancel,
	}

	// Register message handler
	config.Transport.RegisterHandler(r.handleIncomingMessage)

	return r
}

// SetRebalancer attaches a rebalancer to this replicator.
func (r *Replicator) SetRebalancer(rb *Rebalancer) {
	r.rebalancer = rb
}

// TriggerManifestSync signals the auto-sync worker to immediately broadcast the
// current object manifest to all peers. This is used after local bucket deletion
// so replicas can purge orphaned objects without waiting for the next scheduled cycle.
// Non-blocking and deduplicated: if a sync is already queued, this is a no-op.
func (r *Replicator) TriggerManifestSync() {
	select {
	case r.manifestSyncCh <- struct{}{}:
	default:
	}
}

// Start starts the replicator background tasks.
func (r *Replicator) Start() error {
	r.logger.Info().Msg("Starting replicator")

	// Start ACK timeout cleanup goroutine
	r.wg.Add(1)
	go r.ackTimeoutWorker()

	// Start fetch request TTL cleanup goroutine
	r.wg.Add(1)
	go r.fetchRequestCleanupWorker()

	// Start replication queue worker
	r.wg.Add(1)
	go r.runReplicationQueueWorker()

	// Start auto-sync worker if enabled
	if r.autoSyncInterval > 0 {
		r.wg.Add(1)
		go r.runAutoSyncWorker()
	}

	// Start deferred chunk cleanup worker
	r.wg.Add(1)
	go r.runDeferredChunkCleanupWorker()

	// Start chunk registry stale-owner cleanup worker
	if r.chunkRegistry != nil {
		r.wg.Add(1)
		go r.chunkRegistryCleanupWorker()
	}

	// Start rebalancer if configured
	if r.rebalancer != nil {
		r.rebalancer.Start()
	}

	return nil
}

// Stop stops the replicator and waits for background tasks to finish.
func (r *Replicator) Stop() error {
	r.logger.Info().Msg("Stopping replicator")

	// Stop rebalancer first
	if r.rebalancer != nil {
		r.rebalancer.Stop()
	}

	// Final drain of the replication queue before cancelling context
	r.drainReplicationQueueFinal()

	r.cancel()
	r.wg.Wait()
	return nil
}

// acquireSendSlot acquires a slot from the outbound send semaphore.
// Returns nil on success, or the context error if the context is cancelled.
func (r *Replicator) acquireSendSlot(ctx context.Context) error {
	select {
	case r.sendSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseSendSlot releases a slot back to the outbound send semaphore.
func (r *Replicator) releaseSendSlot() {
	<-r.sendSem
}

// AddPeer adds a coordinator peer to replicate to.
func (r *Replicator) AddPeer(coordMeshIP, name string) {
	r.mu.Lock()
	added := false
	if !r.peers[coordMeshIP] {
		r.logger.Info().Str("peer", coordMeshIP).Str("name", name).Msg("Added replication peer")
		r.peers[coordMeshIP] = true
		added = true
	}
	r.peerNames[coordMeshIP] = name
	r.mu.Unlock()

	if added && r.rebalancer != nil {
		r.rebalancer.NotifyTopologyChange()
	}
}

// RemovePeer removes a coordinator peer.
func (r *Replicator) RemovePeer(coordMeshIP string) {
	r.mu.Lock()
	removed := false
	if r.peers[coordMeshIP] {
		r.logger.Info().Str("peer", coordMeshIP).Msg("Removed replication peer")
		delete(r.peers, coordMeshIP)
		delete(r.peerNames, coordMeshIP)
		removed = true
	}
	r.mu.Unlock()

	if removed && r.rebalancer != nil {
		r.rebalancer.NotifyTopologyChange()
	}
}

// HasPeer reports whether a coordinator is already an active replication target.
func (r *Replicator) HasPeer(coordMeshIP string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.peers[coordMeshIP]
}

// GetPeerName returns the coordinator name for a given mesh IP, or the mesh IP
// itself if no name is known.
func (r *Replicator) GetPeerName(meshIP string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if name := r.peerNames[meshIP]; name != "" {
		return name
	}
	return meshIP
}

// GetPeerNames returns a snapshot of the mesh-IP → coordinator-name mapping.
// Callers that need to look up multiple names should prefer this over repeated
// GetPeerName calls to avoid acquiring the read lock once per peer.
func (r *Replicator) GetPeerNames() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot := make(map[string]string, len(r.peerNames))
	for ip, name := range r.peerNames {
		snapshot[ip] = name
	}
	return snapshot
}

// GetPeers returns a copy of the current peer list.
func (r *Replicator) GetPeers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peers := make([]string, 0, len(r.peers))
	for peer := range r.peers {
		peers = append(peers, peer)
	}
	return peers
}

// getPeerNodeIDs returns the set of coordinator names for all known peers.
// This is used for ownership checks where bucket owners are stored as
// coordinator names (e.g. "coordinator-1") rather than mesh IPs.
func (r *Replicator) getPeerNodeIDs() map[string]struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make(map[string]struct{}, len(r.peerNames))
	for _, name := range r.peerNames {
		names[name] = struct{}{}
	}
	return names
}

// getPeerName returns the coordinator name for a given mesh IP.
func (r *Replicator) getPeerName(meshIP string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.peerNames[meshIP]
}

// ReplicateDelete replicates a delete operation to all coordinator peers.
// Deletes are represented as tombstones (empty data with delete marker).
func (r *Replicator) ReplicateDelete(ctx context.Context, bucket, key string) error {
	// Update local version vector
	vv := r.state.Update(bucket, key)

	r.logger.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Str("version_vector", vv.String()).
		Msg("Replicating delete operation")

	// Get peers to replicate to
	peers := r.GetPeers()
	if len(peers) == 0 {
		r.logger.Debug().Msg("No peers to replicate to")
		return nil
	}

	// Create replicate payload with empty data to signal delete
	payload := ReplicatePayload{
		Bucket:        bucket,
		Key:           key,
		Data:          nil, // Empty data signals delete
		VersionVector: vv,
		ContentType:   "",
		Metadata:      map[string]string{"_deleted": "true"}, // Tombstone marker
	}

	// Send to all peers in parallel to avoid one slow peer blocking others
	// Use semaphore to limit concurrency to 10 concurrent sends
	semaphore := make(chan struct{}, 10)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var lastErr error

	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := r.sendReplicateMessage(ctx, p, payload); err != nil {
				r.logger.Error().Err(err).Str("peer", p).Msg("Failed to send delete replicate message")
				mu.Lock()
				lastErr = err
				mu.Unlock()
				r.incrementErrorCount()
			} else {
				r.incrementSentCount()
			}
		}(peer)
	}

	wg.Wait()
	return lastErr
}

// sendReplicateMessage sends a replication message to a peer.
func (r *Replicator) sendReplicateMessage(ctx context.Context, peer string, payload ReplicatePayload) error {
	msgID := uuid.New().String()

	msg, err := NewReplicateMessage(msgID, r.nodeID, payload)
	if err != nil {
		return fmt.Errorf("create replicate message: %w", err)
	}

	data, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Track pending ACK - returns false if dropped due to limit
	if !r.trackPendingACK(msgID, payload.Bucket, payload.Key) {
		return fmt.Errorf("dropped: pending operations limit reached")
	}

	// Acquire send slot to limit concurrent outbound HTTP requests
	if err := r.acquireSendSlot(ctx); err != nil {
		r.removePendingACK(msgID)
		return fmt.Errorf("acquire send slot: %w", err)
	}
	err = r.transport.SendToCoordinator(ctx, peer, data)
	r.releaseSendSlot()
	if err != nil {
		r.removePendingACK(msgID)
		return fmt.Errorf("send to coordinator: %w", err)
	}

	return nil
}

// handleIncomingMessage processes incoming replication messages.
func (r *Replicator) handleIncomingMessage(ctx context.Context, from string, data []byte) (err error) {
	_, span := tracing.Tracer("tunnelmesh/replication").Start(ctx, "replication.receive")
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
		} else {
			span.SetStatus(otelcodes.Ok, "")
		}
	}()
	span.SetAttributes(
		attribute.String("replication.from", from),
		attribute.Int("replication.size", len(data)),
	)

	// Apply rate limiting to prevent abuse
	if !r.rateLimiter.Allow() {
		r.incrementRateLimitedCount()
		// Emit at most one Warn per second to avoid flooding logs.
		// The rateLimited counter records every drop for observability.
		now := time.Now().UnixNano()
		last := r.rateLimitLastLog.Load()
		if now-last > int64(time.Second) && r.rateLimitLastLog.CompareAndSwap(last, now) {
			r.logger.Warn().
				Str("from", from).
				Uint64("total_dropped", r.rateLimited.Load()).
				Msg("Rate limit exceeded, dropping replication messages (suppressed to 1/sec)")
		}
		return fmt.Errorf("rate limit exceeded")
	}

	msg, err := UnmarshalMessage(data)
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to unmarshal message")
		r.incrementErrorCount()
		return fmt.Errorf("unmarshal message: %w", err)
	}

	// Override msg.From with the authenticated sender's mesh IP.
	// msg.From contains the node ID (e.g., "coordinator-1") which is a
	// hostname, but SendToCoordinator needs a mesh IP for the URL.
	if from != "" {
		msg.From = from
	}

	span.SetAttributes(attribute.String("replication.msg_type", string(msg.Type)))
	r.logger.Debug().
		Str("type", string(msg.Type)).
		Str("from", from).
		Str("id", msg.ID).
		Msg("Received replication message")

	switch msg.Type {
	case MessageTypeReplicate:
		return r.handleReplicate(msg)
	case MessageTypeAck:
		return r.handleAck(msg)
	case MessageTypeReplicateChunk:
		return r.handleReplicateChunk(msg)
	case MessageTypeChunkAck:
		return r.handleChunkAck(msg)
	case MessageTypeFetchChunk:
		return r.handleFetchChunk(msg)
	case MessageTypeFetchChunkResponse:
		return r.handleFetchChunkResponse(msg)
	case MessageTypeReplicateObjectMeta:
		return r.handleReplicateObjectMeta(msg)
	case MessageTypeObjectManifest:
		return r.handleObjectManifest(msg)
	default:
		r.logger.Warn().Str("type", string(msg.Type)).Msg("Unknown message type")
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// handleReplicate processes an incoming replication operation.
func (r *Replicator) handleReplicate(msg *Message) error {
	payload, err := msg.DecodeReplicatePayload()
	if err != nil {
		return fmt.Errorf("decode replicate payload: %w", err)
	}

	r.incrementReceivedCount()

	r.logger.Debug().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("from", msg.From).
		Str("version_vector", payload.VersionVector.String()).
		Msg("Processing replicate message")

	// Check for conflicts
	rel, needsResolution := r.state.CheckConflict(payload.Bucket, payload.Key, payload.VersionVector)

	if needsResolution {
		r.incrementConflictCount()
		r.logger.Warn().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Str("relationship", fmt.Sprintf("%d", rel)).
			Msg("Conflict detected")

		// Resolve conflict
		localVV := r.state.Get(payload.Bucket, payload.Key)
		winner := ResolveConflict(localVV, payload.VersionVector)

		// If remote won, apply the update
		if winner.Equal(payload.VersionVector) {
			if err := r.applyReplication(payload); err != nil {
				// Error ACKs stay synchronous — handler is already returning error
				return r.sendAck(msg.ID, msg.From, false, err.Error(), localVV)
			}
		}
		// Merge version vectors (for both local and remote wins)
		// This ensures both coordinators converge to the same VV after conflict
		mergedVV, _ := r.state.Merge(payload.Bucket, payload.Key, payload.VersionVector)
		r.asyncSendAck(msg.ID, msg.From, true, "", mergedVV)
		return nil
	}

	// No conflict - check if we should apply
	if rel == VectorBefore || rel == VectorEqual {
		// Remote is newer or same, apply it
		if err := r.applyReplication(payload); err != nil {
			localVV := r.state.Get(payload.Bucket, payload.Key)
			return r.sendAck(msg.ID, msg.From, false, err.Error(), localVV)
		}
	}

	// Merge version vectors
	mergedVV, _ := r.state.Merge(payload.Bucket, payload.Key, payload.VersionVector)

	// Send ACK asynchronously to avoid blocking this handler
	r.asyncSendAck(msg.ID, msg.From, true, "", mergedVV)
	return nil
}

// applyReplication applies a replication operation to local S3.
func (r *Replicator) applyReplication(payload *ReplicatePayload) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.applyTimeout)
	defer cancel()

	// Check if this is a delete operation (nil data or tombstone marker)
	isDelete := len(payload.Data) == 0 || (payload.Metadata != nil && payload.Metadata["_deleted"] == "true")

	if isDelete {
		// Purge immediately — replicated deletes don't need tombstoning because
		// the primary already purged the object and won't re-send it during sync.
		if err := r.s3.PurgeObject(ctx, payload.Bucket, payload.Key); err != nil {
			r.logger.Error().Err(err).
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Msg("Failed to apply delete replication to S3")
			r.incrementErrorCount()
			return fmt.Errorf("purge from s3: %w", err)
		}

		r.logger.Info().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Msg("Successfully applied delete replication (purged)")
	} else {
		// Apply put operation
		if err := r.s3.Put(ctx, payload.Bucket, payload.Key, payload.Data, payload.ContentType, payload.Metadata); err != nil {
			r.logger.Error().Err(err).
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Msg("Failed to apply replication to S3")
			r.incrementErrorCount()
			return fmt.Errorf("put to s3: %w", err)
		}

		r.logger.Info().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Msg("Successfully applied replication")
	}

	return nil
}

// sendAck sends an acknowledgment message.
func (r *Replicator) sendAck(replicateID, to string, success bool, errorMsg string, vv VersionVector) error {
	ackPayload := AckPayload{
		ReplicateID:   replicateID,
		Success:       success,
		ErrorMessage:  errorMsg,
		VersionVector: vv,
	}

	msgID := uuid.New().String()
	msg, err := NewAckMessage(msgID, r.nodeID, ackPayload)
	if err != nil {
		return fmt.Errorf("create ack message: %w", err)
	}

	data, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("marshal ack: %w", err)
	}

	ctx, cancel := context.WithTimeout(r.ctx, r.ackSendTimeout)
	defer cancel()

	if err := r.transport.SendToCoordinator(ctx, to, data); err != nil {
		r.logger.Error().Err(err).Str("to", to).Msg("Failed to send ACK")
		return fmt.Errorf("send ack: %w", err)
	}

	return nil
}

// asyncSendAck sends an ACK in a background goroutine bounded by the ACK send
// semaphore. This prevents the handler from blocking on the ACK send — which
// would create a bidirectional HTTP dependency between coordinators and lead to
// convoy/deadlock under sustained load.
func (r *Replicator) asyncSendAck(replicateID, to string, success bool, errorMsg string, vv VersionVector) {
	select {
	case r.ackSendSem <- struct{}{}:
		// Acquired semaphore slot — send asynchronously
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer func() { <-r.ackSendSem }()

			if err := r.sendAck(replicateID, to, success, errorMsg, vv); err != nil {
				r.logger.Warn().Err(err).
					Str("to", to).
					Str("replicate_id", replicateID).
					Msg("Failed to send async ACK")
			}
		}()
	default:
		// Semaphore full — fall back to synchronous send (still better than not sending)
		if err := r.sendAck(replicateID, to, success, errorMsg, vv); err != nil {
			r.logger.Warn().Err(err).
				Str("to", to).
				Str("replicate_id", replicateID).
				Msg("Failed to send sync-fallback ACK")
		}
	}
}

// asyncSendChunkAck sends a chunk ACK in a background goroutine bounded by the
// ACK send semaphore.
func (r *Replicator) asyncSendChunkAck(replicateID, to, bucket, key, chunkHash string, chunkIndex int, success bool, errorMsg string) {
	select {
	case r.ackSendSem <- struct{}{}:
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer func() { <-r.ackSendSem }()

			if err := r.sendChunkAck(replicateID, to, bucket, key, chunkHash, chunkIndex, success, errorMsg); err != nil {
				r.logger.Warn().Err(err).
					Str("to", to).
					Str("chunk", truncateHashForLog(chunkHash)).
					Msg("Failed to send async chunk ACK")
			}
		}()
	default:
		if err := r.sendChunkAck(replicateID, to, bucket, key, chunkHash, chunkIndex, success, errorMsg); err != nil {
			r.logger.Warn().Err(err).
				Str("to", to).
				Str("chunk", truncateHashForLog(chunkHash)).
				Msg("Failed to send sync-fallback chunk ACK")
		}
	}
}

// handleAck processes an incoming ACK message.
func (r *Replicator) handleAck(msg *Message) error {
	payload, err := msg.DecodeAckPayload()
	if err != nil {
		return fmt.Errorf("decode ack payload: %w", err)
	}

	r.logger.Debug().
		Str("replicate_id", payload.ReplicateID).
		Bool("success", payload.Success).
		Msg("Received ACK")

	// SECURITY FIX #5 & #6: Notify pending operation with lock held to prevent race
	// The channel could be closed by timeout between lookup and send if we release the lock early
	// Also stop the timer immediately when ACK is received to prevent resource leak
	r.pendingMu.RLock()
	pending, exists := r.pending[payload.ReplicateID]
	r.pendingMu.RUnlock()

	if exists {
		// Merge the returned version vector to prevent divergence after conflicts
		// The receiver may have merged version vectors during conflict resolution
		if payload.Success && len(payload.VersionVector) > 0 {
			r.state.Merge(pending.bucket, pending.key, payload.VersionVector)
			r.logger.Debug().
				Str("bucket", pending.bucket).
				Str("key", pending.key).
				Str("version_vector", payload.VersionVector.String()).
				Msg("Merged version vector from ACK")
		}

		delivered := false
		select {
		case pending.ackChan <- payload:
			// ACK delivered successfully
			delivered = true
		default:
			// Channel full or closed (timeout fired while we held the lock)
		}

		// SECURITY FIX #6: Stop timer immediately when ACK received to prevent resource leak
		// Don't wait for the 10-second timeout to clean up
		if delivered {
			r.removePendingACK(payload.ReplicateID)
		}
	}

	return nil
}

// trackPendingACK tracks a pending ACK for a replication operation.
// Returns false if the operation was dropped due to pending limit.
func (r *Replicator) trackPendingACK(msgID, bucket, key string) bool {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	// Check if we've reached the pending operations limit
	if len(r.pending) >= r.maxPendingOperations {
		r.logger.Warn().
			Int("pending", len(r.pending)).
			Int("limit", r.maxPendingOperations).
			Str("bucket", bucket).
			Str("key", key).
			Msg("dropping replication operation: pending limit reached")
		r.incrementDroppedCount()
		return false
	}

	r.pending[msgID] = &pendingReplication{
		bucket:  bucket,
		key:     key,
		sentAt:  time.Now(),
		ackChan: make(chan *AckPayload, 1),
		timeout: time.AfterFunc(r.ackTimeout, func() {
			r.removePendingACK(msgID)
		}),
	}
	return true
}

// removePendingACK removes a pending ACK entry.
func (r *Replicator) removePendingACK(msgID string) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	if pending, exists := r.pending[msgID]; exists {
		pending.timeout.Stop()
		close(pending.ackChan)
		delete(r.pending, msgID)
	}
}

// ackTimeoutWorker periodically cleans up timed-out ACKs.
func (r *Replicator) ackTimeoutWorker() {
	defer r.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			// Cleanup is handled by individual timeouts
		}
	}
}

// fetchRequestCleanupWorker periodically removes stale fetch requests (TTL-based cleanup).
func (r *Replicator) fetchRequestCleanupWorker() {
	defer r.wg.Done()

	ticker := time.NewTicker(30 * time.Second) // Cleanup every 30 seconds
	defer ticker.Stop()

	const fetchRequestTTL = 2 * time.Minute // Remove requests older than 2 minutes

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			r.pendingChunksMu.Lock()

			var cleaned int
			for requestID, timestamp := range r.pendingFetchTimestamps {
				if now.Sub(timestamp) > fetchRequestTTL {
					delete(r.pendingFetchRequests, requestID)
					delete(r.pendingFetchTimestamps, requestID)
					cleaned++
				}
			}

			r.pendingChunksMu.Unlock()

			if cleaned > 0 {
				r.logger.Debug().
					Int("count", cleaned).
					Dur("ttl", fetchRequestTTL).
					Msg("Cleaned up stale fetch requests")
			}
		}
	}
}

// chunkRegistryCleanupWorker periodically evicts stale coordinator entries from the
// chunk registry. After the primary fix (UnregisterChunk in DeleteUnreferencedChunks),
// valid chunks are regularly re-registered; orphaned entries become stale over time
// and are removed here, acting as a backstop for any deletion paths we may have missed.
func (r *Replicator) chunkRegistryCleanupWorker() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.chunkRegistryCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			n := r.chunkRegistry.CleanupStaleOwners(r.chunkRegistryMaxOwnerAge)
			if n > 0 {
				r.logger.Info().Int("cleaned", n).Msg("chunk registry stale owner cleanup")
			}
		}
	}
}

// GetStats returns replication statistics.
type Stats struct {
	SentCount     uint64 `json:"sent_count"`
	ReceivedCount uint64 `json:"received_count"`
	ConflictCount uint64 `json:"conflict_count"`
	ErrorCount    uint64 `json:"error_count"`
	DroppedCount  uint64 `json:"dropped_count"`
	RateLimited   uint64 `json:"rate_limited"` // Messages dropped due to rate limiting
	PeerCount     int    `json:"peer_count"`
	PendingACKs   int    `json:"pending_acks"`
}

func (r *Replicator) GetStats() Stats {
	r.pendingMu.RLock()
	pendingCount := len(r.pending)
	r.pendingMu.RUnlock()

	r.mu.RLock()
	peerCount := len(r.peers)
	r.mu.RUnlock()

	return Stats{
		SentCount:     r.sentCount.Load(),
		ReceivedCount: r.receivedCount.Load(),
		ConflictCount: r.conflictCount.Load(),
		ErrorCount:    r.errorCount.Load(),
		DroppedCount:  r.droppedCount.Load(),
		RateLimited:   r.rateLimited.Load(),
		PeerCount:     peerCount,
		PendingACKs:   pendingCount,
	}
}

func (r *Replicator) incrementSentCount()        { r.sentCount.Add(1) }
func (r *Replicator) incrementReceivedCount()    { r.receivedCount.Add(1) }
func (r *Replicator) incrementConflictCount()    { r.conflictCount.Add(1) }
func (r *Replicator) incrementErrorCount()       { r.errorCount.Add(1) }
func (r *Replicator) incrementDroppedCount()     { r.droppedCount.Add(1) }
func (r *Replicator) incrementRateLimitedCount() { r.rateLimited.Add(1) }

// GetState returns the replication state tracker.
func (r *Replicator) GetState() *State {
	return r.state
}

// ==== Chunk Striping Policy ====

// StripingPolicy distributes chunks across coordinators using round-robin.
// This ensures parallel reads can fetch different chunks from different peers.
type StripingPolicy struct {
	Peers []string // Sorted coordinator IDs (including self)
}

// NewStripingPolicy creates a striping policy from the given peer list.
// Peers are sorted for deterministic assignment.
func NewStripingPolicy(peers []string) *StripingPolicy {
	sorted := make([]string, len(peers))
	copy(sorted, peers)
	sort.Strings(sorted)
	return &StripingPolicy{Peers: sorted}
}

// PrimaryOwner returns the coordinator that should own a given chunk index.
// Uses round-robin: chunkIndex % len(peers).
func (sp *StripingPolicy) PrimaryOwner(chunkIndex int) string {
	if len(sp.Peers) == 0 {
		return ""
	}
	return sp.Peers[chunkIndex%len(sp.Peers)]
}

// AssignedChunks returns the chunk indices assigned to a specific peer.
func (sp *StripingPolicy) AssignedChunks(peerID string, totalChunks int) []int {
	if len(sp.Peers) == 0 {
		return nil
	}

	// Find peer index
	peerIdx := -1
	for i, p := range sp.Peers {
		if p == peerID {
			peerIdx = i
			break
		}
	}
	if peerIdx < 0 {
		return nil
	}

	var assigned []int
	for i := 0; i < totalChunks; i++ {
		if i%len(sp.Peers) == peerIdx {
			assigned = append(assigned, i)
		}
	}
	return assigned
}

// ChunksForPeer returns the chunk indices that should be replicated to a peer,
// respecting the replication factor. Each chunk goes to its primary owner plus
// the next (replicationFactor-1) peers in order.
func (sp *StripingPolicy) ChunksForPeer(peerID string, totalChunks int, replicationFactor int) []int {
	if len(sp.Peers) == 0 {
		return nil
	}

	// Find peer index
	peerIdx := -1
	for i, p := range sp.Peers {
		if p == peerID {
			peerIdx = i
			break
		}
	}
	if peerIdx < 0 {
		return nil
	}

	// Cap replication factor to number of peers
	rf := replicationFactor
	if rf > len(sp.Peers) {
		rf = len(sp.Peers)
	}

	var chunks []int
	for i := 0; i < totalChunks; i++ {
		primary := i % len(sp.Peers)
		// Check if this peer is within the replication window for this chunk
		for r := 0; r < rf; r++ {
			replica := (primary + r) % len(sp.Peers)
			if replica == peerIdx {
				chunks = append(chunks, i)
				break
			}
		}
	}
	return chunks
}

// ShardDistributionForPeer returns which data and parity shards should be stored on a peer
// for optimal erasure coding fault tolerance. Ensures data and parity shards are distributed
// across different coordinators to maximize recoverability.
//
// Strategy: Round-robin with separation guarantee
// - Data shards: distributed round-robin across all coordinators
// - Parity shards: distributed round-robin, avoiding coordinators that already have too many data shards
//
// Example: 3 coordinators, 10 data + 3 parity shards:
//
//	Coord A: data[0,3,6,9], parity[0]       -> 4 data + 1 parity = 5 total
//	Coord B: data[1,4,7],   parity[1,2]     -> 3 data + 2 parity = 5 total
//	Coord C: data[2,5,8]                    -> 3 data + 0 parity = 3 total
func (sp *StripingPolicy) ShardDistributionForPeer(peerID string, dataShards, parityShards int) (dataIndices, parityIndices []int) {
	if len(sp.Peers) == 0 {
		return nil, nil
	}

	// Find peer index
	peerIdx := -1
	for i, p := range sp.Peers {
		if p == peerID {
			peerIdx = i
			break
		}
	}
	if peerIdx < 0 {
		return nil, nil
	}

	// Distribute data shards round-robin
	for i := 0; i < dataShards; i++ {
		if i%len(sp.Peers) == peerIdx {
			dataIndices = append(dataIndices, i)
		}
	}

	// Distribute parity shards round-robin (starting after data shards to spread load)
	// Use a different offset to avoid concentrating parity shards on the same coordinator
	// that has the most data shards
	for i := 0; i < parityShards; i++ {
		// Add dataShards to offset to start parity distribution at a different point
		owner := (dataShards + i) % len(sp.Peers)
		if owner == peerIdx {
			parityIndices = append(parityIndices, i)
		}
	}

	return dataIndices, parityIndices
}

// GetShardOwner returns the coordinator that should own a specific shard (data or parity).
// For data shards, uses round-robin. For parity shards, uses offset round-robin to spread load.
func (sp *StripingPolicy) GetShardOwner(shardIndex int, isParityShard bool, dataShards int) string {
	if len(sp.Peers) == 0 {
		return ""
	}

	if isParityShard {
		// Parity shards: use offset to avoid concentration
		return sp.Peers[(dataShards+shardIndex)%len(sp.Peers)]
	}

	// Data shards: simple round-robin
	return sp.Peers[shardIndex%len(sp.Peers)]
}

// ==== Phase 4: Chunk-Level Replication Functions ====

// ReplicateObject replicates a file to a specific peer by sending individual chunks.
// Only chunks not already owned by the peer are sent (bandwidth optimization).
// When multiple peers exist, uses striping to distribute chunks so different
// coordinators hold different primary chunks, enabling parallel reads.
// Enables resume capability: failed transfers can be resumed from the last successful chunk.
func (r *Replicator) ReplicateObject(ctx context.Context, bucket, key, peerID string) error {
	// Chunk registry is required for chunk-level replication
	if r.chunkRegistry == nil {
		return fmt.Errorf("chunk registry not configured: chunk-level replication requires a chunk registry")
	}

	// Get file metadata (includes chunk list)
	meta, err := r.s3.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("get object metadata: %w", err)
	}

	// Query which chunks the remote peer already has
	remoteChunks, err := r.chunkRegistry.GetChunksOwnedBy(peerID)
	if err != nil {
		r.logger.Warn().Err(err).Str("peer", peerID).Msg("Failed to query remote chunks, replicating all")
		remoteChunks = []string{} // Fallback: replicate everything
	}

	// Build set for fast lookup
	remoteChunkSet := make(map[string]bool, len(remoteChunks))
	for _, hash := range remoteChunks {
		remoteChunkSet[hash] = true
	}

	// Build striping policy to determine which chunks this peer should own.
	// All coordinators (self + peers) participate in striping. The local coordinator
	// already has all chunks via PutObject, so we only replicate to remote peers.
	peers := r.GetPeers()
	allCoords := make([]string, 0, len(peers)+1)
	allCoords = append(allCoords, r.nodeID)
	allCoords = append(allCoords, peers...)

	// Determine which chunks need replication to this peer
	var chunksToReplicate []string

	if len(allCoords) > 1 {
		// Multi-coordinator: use striping policy with bucket's replication factor
		replicationFactor := r.s3.GetBucketReplicationFactor(ctx, bucket)
		if replicationFactor < 1 {
			replicationFactor = 2 // Fallback if bucket RF is unknown
		}

		sp := NewStripingPolicy(allCoords)
		assignedIndices := sp.ChunksForPeer(peerID, len(meta.Chunks), replicationFactor)

		// Build set of assigned indices for fast lookup
		assignedSet := make(map[int]bool, len(assignedIndices))
		for _, idx := range assignedIndices {
			assignedSet[idx] = true
		}

		// Only replicate chunks that are both assigned to this peer AND not already there
		for idx, chunkHash := range meta.Chunks {
			if assignedSet[idx] && !remoteChunkSet[chunkHash] {
				chunksToReplicate = append(chunksToReplicate, chunkHash)
			}
		}

		r.logger.Debug().
			Str("peer", peerID).
			Int("assigned_chunks", len(assignedIndices)).
			Int("total_chunks", len(meta.Chunks)).
			Int("replication_factor", replicationFactor).
			Msg("Using striped replication")
	} else {
		// Single coordinator or no peers: replicate all missing chunks
		for _, chunkHash := range meta.Chunks {
			if !remoteChunkSet[chunkHash] {
				chunksToReplicate = append(chunksToReplicate, chunkHash)
			}
		}
	}

	// Pre-flight capacity check
	if err := r.checkPeerCapacity(peerID, bucket, key, chunksToReplicate, meta); err != nil {
		return err
	}

	if len(chunksToReplicate) == 0 {
		// Still send metadata even if all chunks were already there
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal object metadata: %w", err)
		}
		if err := r.sendReplicateObjectMeta(ctx, peerID, bucket, key, metaJSON); err != nil {
			r.logger.Warn().Err(err).
				Str("peer", peerID).
				Msg("Failed to send object metadata")
		}

		r.logger.Info().
			Str("bucket", bucket).
			Str("key", key).
			Str("peer", peerID).
			Int("total_chunks", len(meta.Chunks)).
			Msg("All chunks already replicated, sent metadata only")
		return nil
	}

	r.logger.Info().
		Str("bucket", bucket).
		Str("key", key).
		Str("peer", peerID).
		Int("chunks_to_replicate", len(chunksToReplicate)).
		Int("total_chunks", len(meta.Chunks)).
		Int("already_replicated", len(meta.Chunks)-len(chunksToReplicate)).
		Msg("Starting chunk-level replication")

	// Replicate chunks with pipelining for throughput
	if err := r.replicateChunksPipelined(ctx, peerID, bucket, key, chunksToReplicate, meta); err != nil {
		return err
	}

	// Send object metadata so the remote peer can serve reads
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal object metadata: %w", err)
	}

	if err := r.sendReplicateObjectMeta(ctx, peerID, bucket, key, metaJSON); err != nil {
		r.logger.Warn().Err(err).
			Str("peer", peerID).
			Str("bucket", bucket).
			Str("key", key).
			Msg("Failed to send object metadata (chunks were sent successfully)")
		// Don't fail the whole operation — chunks are there, metadata can be retried
	}

	r.logger.Info().
		Str("bucket", bucket).
		Str("key", key).
		Str("peer", peerID).
		Int("chunks_replicated", len(chunksToReplicate)).
		Msg("Completed chunk-level replication with metadata")

	return nil
}

// replicateChunksPipelined sends chunks with windowed concurrency for throughput.
// Instead of sending chunks one at a time (N × RTT), this sends up to
// chunkPipelineWindow chunks concurrently.
func (r *Replicator) replicateChunksPipelined(ctx context.Context, peerID, bucket, key string, chunksToReplicate []string, meta *ObjectMeta) error {
	window := r.chunkPipelineWindow
	if window <= 1 || len(chunksToReplicate) <= 1 {
		// Fall back to sequential for single chunk or window=1
		for i, chunkHash := range chunksToReplicate {
			if err := r.replicateSingleChunk(ctx, peerID, bucket, key, chunkHash, meta); err != nil {
				return fmt.Errorf("send chunk %s (%d/%d): %w", chunkHash, i+1, len(chunksToReplicate), err)
			}
		}
		return nil
	}

	inflight := make(chan struct{}, window)
	type chunkResult struct {
		index int
		hash  string
		err   error
	}
	resultCh := make(chan chunkResult, len(chunksToReplicate))

	for i, chunkHash := range chunksToReplicate {
		select {
		case <-ctx.Done():
			// Collect already-sent results before returning
			goto collect
		case inflight <- struct{}{}:
		}

		go func(idx int, hash string) {
			defer func() { <-inflight }()
			err := r.replicateSingleChunk(ctx, peerID, bucket, key, hash, meta)
			resultCh <- chunkResult{index: idx, hash: hash, err: err}
		}(i, chunkHash)
	}

collect:
	// Collect all results
	var firstErr error
	collected := 0
	for collected < len(chunksToReplicate) {
		select {
		case res := <-resultCh:
			collected++
			if res.err != nil && firstErr == nil {
				firstErr = fmt.Errorf("send chunk %s (%d/%d): %w",
					res.hash, res.index+1, len(chunksToReplicate), res.err)
			}
			if res.err == nil {
				r.logger.Debug().
					Str("bucket", bucket).
					Str("key", key).
					Str("chunk", truncateHashForLog(res.hash)).
					Int("progress", collected).
					Int("total", len(chunksToReplicate)).
					Msg("Replicated chunk")
			}
		case <-ctx.Done():
			if firstErr == nil {
				firstErr = fmt.Errorf("replication canceled: %w", ctx.Err())
			}
			return firstErr
		}
	}

	return firstErr
}

// replicateSingleChunk reads and sends a single chunk to a peer.
// Preferentially sends raw on-disk bytes (encrypted+compressed) to avoid double
// crypto overhead on sender and receiver. Falls back to plaintext if the chunk is
// missing locally and must be fetched from a peer.
func (r *Replicator) replicateSingleChunk(ctx context.Context, peerID, bucket, key, chunkHash string, meta *ObjectMeta) error {
	// Try to read raw (encrypted+compressed) bytes from local CAS first.
	// This avoids the decrypt+decompress (sender) + compress+encrypt (receiver) cycle.
	chunkData, err := r.s3.ReadChunkRaw(ctx, chunkHash)
	rawTransfer := err == nil

	if err != nil {
		// Chunk missing locally (e.g. GC cleaned it up) — fall back to peer fetch.
		// Peer fetch returns plaintext, so the receiver will use WriteChunkDirect.
		localErr := err
		chunkData, err = r.fetchChunkFromPeers(ctx, chunkHash)
		if err != nil {
			return fmt.Errorf("read chunk %s: local: %w, peers: %w", chunkHash, localErr, err)
		}
	}

	// Get chunk metadata
	chunkMeta := meta.ChunkMetadata[chunkHash]
	if chunkMeta == nil {
		chunkMeta = &ChunkMetadata{
			Hash: chunkHash,
			Size: int64(len(chunkData)),
		}
	}

	// Find chunk index in file (for ordering)
	chunkIndex := -1
	for idx, hash := range meta.Chunks {
		if hash == chunkHash {
			chunkIndex = idx
			break
		}
	}

	payload := ReplicateChunkPayload{
		Bucket:        bucket,
		Key:           key,
		ChunkHash:     chunkHash,
		ChunkData:     chunkData,
		ChunkIndex:    chunkIndex,
		TotalChunks:   len(meta.Chunks),
		ChunkSize:     chunkMeta.Size,
		VersionVector: VersionVector(chunkMeta.VersionVector),
		RawTransfer:   rawTransfer,
	}

	return r.sendReplicateChunk(ctx, peerID, payload)
}

// fetchChunkFromPeers tries to fetch a chunk from any available peer.
// Used as a fallback when a chunk is missing locally (e.g. cleaned up by GC).
func (r *Replicator) fetchChunkFromPeers(ctx context.Context, chunkHash string) ([]byte, error) {
	peers := r.GetPeers()
	if len(peers) == 0 {
		return nil, fmt.Errorf("chunk not found locally and no peers available")
	}

	r.logger.Debug().
		Str("chunk", truncateHashForLog(chunkHash)).
		Int("peers", len(peers)).
		Msg("Chunk not found locally during replication, fetching from peers")

	var lastErr error
	for _, peerID := range peers {
		data, err := r.FetchChunk(ctx, peerID, chunkHash)
		if err != nil {
			r.logger.Debug().Err(err).
				Str("peer", peerID).
				Str("chunk", truncateHashForLog(chunkHash)).
				Msg("Peer chunk fetch failed, trying next")
			lastErr = err
			continue
		}
		// Cache locally for future reads
		if werr := r.s3.WriteChunkDirect(ctx, chunkHash, data); werr != nil {
			r.logger.Warn().Err(werr).
				Str("chunk", truncateHashForLog(chunkHash)).
				Msg("Failed to cache remotely-fetched chunk locally")
		}
		return data, nil
	}
	return nil, fmt.Errorf("chunk %s not found on any peer: %w", chunkHash, lastErr)
}

// sendReplicateChunk sends a chunk replication message and waits for ACK.
func (r *Replicator) sendReplicateChunk(ctx context.Context, peerID string, payload ReplicateChunkPayload) error {
	msgID := uuid.New().String()

	// Create message
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal chunk payload: %w", err)
	}

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeReplicateChunk,
		ID:      msgID,
		From:    r.nodeID,
		Payload: json.RawMessage(payloadJSON),
	}

	data, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Track pending ACK
	if !r.trackPendingChunkACK(msgID, payload.Bucket, payload.Key, payload.ChunkHash, payload.ChunkIndex) {
		return fmt.Errorf("dropped: pending chunk operations limit reached")
	}

	// Acquire send slot to limit concurrent outbound HTTP requests
	if err := r.acquireSendSlot(ctx); err != nil {
		r.removePendingChunkACK(msgID)
		return fmt.Errorf("acquire send slot: %w", err)
	}
	err = r.transport.SendToCoordinator(ctx, peerID, data)
	r.releaseSendSlot()
	if err != nil {
		r.removePendingChunkACK(msgID)
		return fmt.Errorf("send to coordinator: %w", err)
	}

	// Wait for ACK with timeout
	ack, err := r.waitForChunkAck(ctx, msgID)
	if err != nil {
		return fmt.Errorf("wait for chunk ack: %w", err)
	}

	if !ack.Success {
		return fmt.Errorf("chunk replication failed: %s", ack.Error)
	}

	return nil
}

// checkPeerCapacity performs a pre-flight capacity check before replication.
// Returns nil if the check passes or if no capacity checker is configured (fail-open).
func (r *Replicator) checkPeerCapacity(peerID, bucket, key string, chunks []string, meta *ObjectMeta) error {
	if r.capacityChecker == nil || len(chunks) == 0 {
		return nil
	}
	var estimatedBytes int64
	for _, chunkHash := range chunks {
		if cm := meta.ChunkMetadata[chunkHash]; cm != nil {
			estimatedBytes += cm.Size
		}
	}
	if estimatedBytes > 0 && !r.capacityChecker.HasCapacityFor(peerID, estimatedBytes) {
		r.logger.Warn().
			Str("peer", peerID).
			Str("bucket", bucket).
			Str("key", key).
			Int64("estimated_bytes", estimatedBytes).
			Msg("Peer near capacity, skipping replication")
		return fmt.Errorf("peer %s near capacity: need %d bytes", peerID, estimatedBytes)
	}
	return nil
}

// truncateHash returns first 8 chars of hash for logging, or full hash if shorter.
func truncateHashForLog(hash string) string {
	if len(hash) > 8 {
		return hash[:8]
	}
	return hash
}

// handleReplicateChunk processes an incoming chunk replication message.
func (r *Replicator) handleReplicateChunk(msg *Message) error {
	var payload ReplicateChunkPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal chunk payload: %w", err)
	}

	r.logger.Debug().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("chunk", truncateHashForLog(payload.ChunkHash)).
		Int("index", payload.ChunkIndex).
		Int("total", payload.TotalChunks).
		Str("from", msg.From).
		Msg("Received chunk replication")

	// Check local capacity before writing
	if r.capacityChecker != nil && !r.capacityChecker.HasCapacityFor(r.nodeID, int64(len(payload.ChunkData))) {
		r.logger.Warn().
			Str("chunk", truncateHashForLog(payload.ChunkHash)).
			Int("bytes", len(payload.ChunkData)).
			Msg("Storage capacity exceeded, rejecting chunk")
		return r.sendChunkAck(msg.ID, msg.From, payload.Bucket, payload.Key, payload.ChunkHash, payload.ChunkIndex, false, "storage capacity exceeded")
	}

	// Store chunk in local CAS.
	// RawTransfer=true means ChunkData is already encrypted+compressed on-disk bytes;
	// use WriteChunkDirectRaw to skip the re-encrypt cycle.
	var writeErr error
	if payload.RawTransfer {
		writeErr = r.s3.WriteChunkDirectRaw(r.ctx, payload.ChunkHash, payload.ChunkData)
	} else {
		writeErr = r.s3.WriteChunkDirect(r.ctx, payload.ChunkHash, payload.ChunkData)
	}
	if writeErr != nil {
		r.logger.Error().Err(writeErr).Str("chunk", truncateHashForLog(payload.ChunkHash)).Msg("Failed to write chunk")
		// Error ACK stays synchronous
		return r.sendChunkAck(msg.ID, msg.From, payload.Bucket, payload.Key, payload.ChunkHash, payload.ChunkIndex, false, writeErr.Error())
	}

	// Update chunk registry (mark us as owner)
	if r.chunkRegistry != nil {
		if err := r.chunkRegistry.AddOwner(payload.ChunkHash, r.nodeID); err != nil {
			// Non-fatal: log warning but still send success ACK
			r.logger.Warn().Err(err).Str("chunk", truncateHashForLog(payload.ChunkHash)).Msg("Failed to update chunk registry")
		}
	}

	// Send success ACK asynchronously to avoid blocking this handler
	r.asyncSendChunkAck(msg.ID, msg.From, payload.Bucket, payload.Key, payload.ChunkHash, payload.ChunkIndex, true, "")
	return nil
}

// sendChunkAck sends a chunk ACK message.
func (r *Replicator) sendChunkAck(replicateID, to, bucket, key, chunkHash string, chunkIndex int, success bool, errorMsg string) error {
	payload := ChunkAckPayload{
		ReplicateID: replicateID,
		Bucket:      bucket,
		Key:         key,
		ChunkHash:   chunkHash,
		ChunkIndex:  chunkIndex,
		Success:     success,
		Error:       errorMsg,
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal chunk ack: %w", err)
	}

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeChunkAck,
		ID:      uuid.New().String(),
		From:    r.nodeID,
		Payload: json.RawMessage(payloadJSON),
	}

	data, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("marshal ack message: %w", err)
	}

	ctx, cancel := context.WithTimeout(r.ctx, r.ackSendTimeout)
	defer cancel()

	if err := r.transport.SendToCoordinator(ctx, to, data); err != nil {
		r.logger.Error().Err(err).Str("to", to).Msg("Failed to send chunk ACK")
		return fmt.Errorf("send chunk ack: %w", err)
	}

	return nil
}

// handleChunkAck processes an incoming chunk ACK message.
func (r *Replicator) handleChunkAck(msg *Message) error {
	var payload ChunkAckPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal chunk ack: %w", err)
	}

	r.logger.Debug().
		Str("chunk", truncateHashForLog(payload.ChunkHash)).
		Bool("success", payload.Success).
		Msg("Received chunk ACK")

	// Find pending chunk operation using the replicate message ID
	r.pendingChunksMu.RLock()
	pending, exists := r.pendingChunks[payload.ReplicateID]
	r.pendingChunksMu.RUnlock()

	if !exists {
		// ACK for unknown message (probably timed out already)
		r.logger.Warn().Str("replicate_id", payload.ReplicateID).Msg("Received ACK for unknown chunk message")
		return nil
	}

	// Deliver ACK to waiting goroutine (non-blocking)
	select {
	case pending.ackChan <- &payload:
		// ACK delivered successfully
		// Don't remove pending entry here - waitForChunkAck will clean it up
	default:
		// Channel full or closed (timeout fired or waitForChunkAck not called yet)
		// This can happen if ACK arrives after timeout
	}

	return nil
}

// trackPendingChunkACK tracks a pending chunk ACK.
func (r *Replicator) trackPendingChunkACK(msgID, bucket, key, chunkHash string, chunkIndex int) bool {
	r.pendingChunksMu.Lock()
	defer r.pendingChunksMu.Unlock()

	// Check pending limit (reuse maxPendingOperations)
	if len(r.pendingChunks) >= r.maxPendingOperations {
		r.logger.Warn().
			Int("pending", len(r.pendingChunks)).
			Int("limit", r.maxPendingOperations).
			Str("chunk", truncateHashForLog(chunkHash)).
			Msg("Dropping chunk replication: pending limit reached")
		r.incrementDroppedCount()
		return false
	}

	r.pendingChunks[msgID] = &pendingChunkReplication{
		bucket:     bucket,
		key:        key,
		chunkHash:  chunkHash,
		chunkIndex: chunkIndex,
		sentAt:     time.Now(),
		ackChan:    make(chan *ChunkAckPayload, 1),
		timeout:    time.NewTimer(r.chunkAckTimeout),
	}

	return true
}

// removePendingChunkACK removes a pending chunk ACK entry.
func (r *Replicator) removePendingChunkACK(msgID string) {
	r.pendingChunksMu.Lock()
	defer r.pendingChunksMu.Unlock()

	if pending, exists := r.pendingChunks[msgID]; exists {
		pending.timeout.Stop()
		close(pending.ackChan)
		delete(r.pendingChunks, msgID)
	}
}

// waitForChunkAck waits for a chunk ACK with timeout.
func (r *Replicator) waitForChunkAck(ctx context.Context, msgID string) (*ChunkAckPayload, error) {
	r.pendingChunksMu.RLock()
	pending, exists := r.pendingChunks[msgID]
	r.pendingChunksMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no pending chunk operation for message %s", msgID)
	}

	// Always clean up when we're done (defer to ensure cleanup even on early return)
	defer r.removePendingChunkACK(msgID)

	// Wait for ACK or timeout (use timer from pending struct to avoid double timeout)
	select {
	case ack := <-pending.ackChan:
		return ack, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("context canceled: %w", ctx.Err())
	case <-pending.timeout.C:
		return nil, fmt.Errorf("chunk ack timeout after %v", r.chunkAckTimeout)
	}
}

// FetchChunk requests a chunk from a remote peer (for distributed reads).
func (r *Replicator) FetchChunk(ctx context.Context, peerID, chunkHash string) ([]byte, error) {
	requestID := uuid.New().String()
	msgID := uuid.New().String()

	// Check request count limit (DoS prevention)
	r.pendingChunksMu.Lock()
	if len(r.pendingFetchRequests) >= maxPendingFetchRequests {
		r.pendingChunksMu.Unlock()
		return nil, fmt.Errorf("too many pending fetch requests (%d), try again later", maxPendingFetchRequests)
	}
	r.pendingChunksMu.Unlock()

	// Create fetch request payload
	payload := FetchChunkPayload{
		ChunkHash: chunkHash,
		RequestID: requestID,
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal fetch payload: %w", err)
	}

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeFetchChunk,
		ID:      msgID,
		From:    r.nodeID,
		Payload: json.RawMessage(payloadJSON),
	}

	data, err := msg.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}

	// Track pending fetch request with timestamp
	responseChan := make(chan *FetchChunkResponsePayload, 1)
	r.pendingChunksMu.Lock()
	r.pendingFetchRequests[requestID] = responseChan
	r.pendingFetchTimestamps[requestID] = time.Now()
	r.pendingChunksMu.Unlock()

	// Clean up on return (don't close channel to avoid race - will be GC'd)
	defer func() {
		r.pendingChunksMu.Lock()
		delete(r.pendingFetchRequests, requestID)
		delete(r.pendingFetchTimestamps, requestID)
		r.pendingChunksMu.Unlock()
	}()

	// Send via transport
	if err := r.transport.SendToCoordinator(ctx, peerID, data); err != nil {
		return nil, fmt.Errorf("send to coordinator: %w", err)
	}

	// Wait for response with timeout (use context for proper timeout handling)
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	select {
	case response := <-responseChan:
		if !response.Success {
			return nil, fmt.Errorf("fetch chunk failed: %s", response.Error)
		}
		return response.ChunkData, nil
	case <-fetchCtx.Done():
		return nil, fmt.Errorf("fetch chunk timeout or canceled: %w", fetchCtx.Err())
	}
}

// handleFetchChunk processes an incoming chunk fetch request.
func (r *Replicator) handleFetchChunk(msg *Message) error {
	var payload FetchChunkPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal fetch chunk: %w", err)
	}

	r.logger.Debug().
		Str("chunk", truncateHashForLog(payload.ChunkHash)).
		Str("request_id", payload.RequestID).
		Str("from", msg.From).
		Msg("Received chunk fetch request")

	// Read chunk from local storage
	chunkData, err := r.s3.ReadChunk(context.Background(), payload.ChunkHash)

	// Create response
	response := FetchChunkResponsePayload{
		ChunkHash: payload.ChunkHash,
		RequestID: payload.RequestID,
		Success:   err == nil,
	}

	if err != nil {
		response.Error = fmt.Sprintf("chunk not found: %v", err)
		r.logger.Warn().
			Str("chunk", truncateHashForLog(payload.ChunkHash)).
			Err(err).
			Msg("Failed to read chunk for fetch request")
	} else {
		// Validate chunk integrity before sending
		h := sha256.Sum256(chunkData)
		actualHash := hex.EncodeToString(h[:])
		if actualHash != payload.ChunkHash {
			response.Success = false
			response.Error = "chunk integrity check failed: hash mismatch"
			r.logger.Error().
				Str("expected", truncateHashForLog(payload.ChunkHash)).
				Str("actual", truncateHashForLog(actualHash)).
				Msg("Chunk hash mismatch when serving fetch request")
		} else {
			response.ChunkData = chunkData
		}
	}

	// Send response
	return r.sendFetchChunkResponse(msg.From, response)
}

// handleFetchChunkResponse processes an incoming chunk fetch response.
func (r *Replicator) handleFetchChunkResponse(msg *Message) error {
	var payload FetchChunkResponsePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal fetch chunk response: %w", err)
	}

	// Validate chunk size to prevent resource exhaustion attacks
	const maxChunkSize = 65536 // 64KB (matches s3.MaxChunkSize)
	if len(payload.ChunkData) > maxChunkSize {
		r.logger.Warn().
			Str("chunk", truncateHashForLog(payload.ChunkHash)).
			Int("size", len(payload.ChunkData)).
			Int("max", maxChunkSize).
			Msg("Received chunk larger than maximum allowed size")
		// Convert to error response
		payload.Success = false
		payload.Error = fmt.Sprintf("chunk size %d exceeds maximum %d", len(payload.ChunkData), maxChunkSize)
		payload.ChunkData = nil
	}

	r.logger.Debug().
		Str("chunk", truncateHashForLog(payload.ChunkHash)).
		Str("request_id", payload.RequestID).
		Bool("success", payload.Success).
		Msg("Received chunk fetch response")

	// Find pending fetch request
	r.pendingChunksMu.RLock()
	responseChan, exists := r.pendingFetchRequests[payload.RequestID]
	r.pendingChunksMu.RUnlock()

	if !exists {
		// Response for unknown request (probably timed out already)
		r.logger.Warn().
			Str("request_id", payload.RequestID).
			Msg("Received fetch response for unknown request")
		return nil
	}

	// Deliver response to waiting goroutine (non-blocking)
	select {
	case responseChan <- &payload:
		// Response delivered successfully
	default:
		// Channel full or closed (timeout fired or FetchChunk not waiting)
	}

	return nil
}

// sendFetchChunkResponse sends a chunk fetch response to a peer.
func (r *Replicator) sendFetchChunkResponse(peerID string, payload FetchChunkResponsePayload) error {
	msgID := uuid.New().String()

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal fetch response: %w", err)
	}

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeFetchChunkResponse,
		ID:      msgID,
		From:    r.nodeID,
		Payload: json.RawMessage(payloadJSON),
	}

	data, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Send via transport (use background context since this is a response)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := r.transport.SendToCoordinator(ctx, peerID, data); err != nil {
		return fmt.Errorf("send to coordinator: %w", err)
	}

	return nil
}

// ==== Object Metadata Replication ====

// sendReplicateObjectMeta sends object metadata to a peer so it can serve reads.
// This is fire-and-forget (no ACK needed) since metadata is idempotent.
// Also includes version history so any coordinator can serve version listings.
func (r *Replicator) sendReplicateObjectMeta(ctx context.Context, peerID, bucket, key string, metaJSON []byte) error {
	payload := ReplicateObjectMetaPayload{
		Bucket:      bucket,
		Key:         key,
		MetaJSON:    json.RawMessage(metaJSON),
		BucketOwner: r.nodeID, // Preserve ownership: sender is the original bucket owner
	}

	// Include version history so all coordinators can serve version listings
	versions, err := r.s3.GetVersionHistory(ctx, bucket, key)
	if err != nil {
		r.logger.Warn().Err(err).
			Str("bucket", bucket).Str("key", key).
			Msg("Failed to get version history for replication, sending without versions")
	} else if len(versions) > 0 {
		payload.Versions = versions
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal object meta payload: %w", err)
	}

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeReplicateObjectMeta,
		ID:      uuid.New().String(),
		From:    r.nodeID,
		Payload: json.RawMessage(payloadJSON),
	}

	data, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Acquire send slot to limit concurrent outbound HTTP requests
	if err := r.acquireSendSlot(ctx); err != nil {
		return fmt.Errorf("acquire send slot: %w", err)
	}
	err = r.transport.SendToCoordinator(ctx, peerID, data)
	r.releaseSendSlot()
	if err != nil {
		return fmt.Errorf("send object meta to %s: %w", peerID, err)
	}

	r.logger.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Str("peer", peerID).
		Msg("Sent object metadata to peer")

	return nil
}

// enqueueDeferredChunkCleanup adds chunk hashes to the deferred cleanup set
// and notifies the background worker. This avoids expensive buildChunkReferenceSet
// walks in the HTTP handler path.
func (r *Replicator) enqueueDeferredChunkCleanup(chunks []string) {
	r.deferredChunksMu.Lock()
	for _, h := range chunks {
		r.deferredChunks[h] = struct{}{}
	}
	r.deferredChunksMu.Unlock()

	// Non-blocking notify — if channel already has a pending signal, skip.
	select {
	case r.deferredNotify <- struct{}{}:
	default:
	}
}

// runDeferredChunkCleanupWorker processes deferred chunk cleanup in batches.
// It coalesces chunk hashes over a 30-second window, then performs a single
// DeleteUnreferencedChunks call (one buildChunkReferenceSet walk instead of N).
func (r *Replicator) runDeferredChunkCleanupWorker() {
	defer r.wg.Done()

	for {
		// Wait for notification or shutdown.
		select {
		case <-r.deferredNotify:
		case <-r.ctx.Done():
			return
		}

		// Coalesce: wait 30 seconds to collect more chunks.
		// Use time.NewTimer instead of time.After to avoid a timer leak on ctx cancellation.
		coalesceTimer := time.NewTimer(30 * time.Second)
		select {
		case <-coalesceTimer.C:
		case <-r.ctx.Done():
			coalesceTimer.Stop()
			return
		}
		coalesceTimer.Stop()

		// Swap out the pending set so we don't block enqueuers during the walk.
		r.deferredChunksMu.Lock()
		pending := r.deferredChunks
		r.deferredChunks = make(map[string]struct{})
		r.deferredChunksMu.Unlock()

		if len(pending) == 0 {
			continue
		}

		// Convert set to slice.
		chunks := make([]string, 0, len(pending))
		for h := range pending {
			chunks = append(chunks, h)
		}

		r.logger.Debug().
			Int("chunks", len(chunks)).
			Msg("Deferred chunk cleanup: starting batch")

		freed := r.s3.DeleteUnreferencedChunks(r.ctx, chunks)

		r.logger.Info().
			Int("chunks_checked", len(chunks)).
			Int64("bytes_freed", freed).
			Msg("Deferred chunk cleanup completed")
	}
}

// handleReplicateObjectMeta processes an incoming object metadata replication message.
func (r *Replicator) handleReplicateObjectMeta(msg *Message) error {
	var payload ReplicateObjectMetaPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal object meta payload: %w", err)
	}

	r.logger.Debug().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("from", msg.From).
		Msg("Received object metadata replication")

	ctx, cancel := context.WithTimeout(r.ctx, r.applyTimeout)
	defer cancel()

	// Limit concurrent metadata imports to prevent Store.mu writer queue buildup.
	// With lock hold reduced to ~5ms, 3 concurrent imports create ~15ms max queue time.
	select {
	case r.metaImportSem <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("metadata import semaphore: %w", ctx.Err())
	}

	chunksToCheck, err := r.s3.ImportObjectMeta(ctx, payload.Bucket, payload.Key, payload.MetaJSON, payload.BucketOwner)
	if err != nil {
		<-r.metaImportSem
		r.logger.Error().Err(err).
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Msg("Failed to import object metadata")
		r.incrementErrorCount()
		return fmt.Errorf("import object meta: %w", err)
	}

	// Import version history if present (still under semaphore for bounded concurrency).
	var vChunks []string
	if len(payload.Versions) > 0 {
		imported, vc, vErr := r.s3.ImportVersionHistory(ctx, payload.Bucket, payload.Key, payload.Versions)
		if vErr != nil {
			r.logger.Warn().Err(vErr).
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Msg("Failed to import version history")
		} else if imported > 0 {
			r.logger.Debug().
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Int("imported", imported).
				Int("total", len(payload.Versions)).
				Msg("Imported version history from replication")
			vChunks = vc
		}
	}

	// Release semaphore immediately after imports — expensive chunk cleanup
	// is deferred to a background worker to keep handler response times low.
	<-r.metaImportSem

	// Defer chunk cleanup to background worker instead of running expensive
	// buildChunkReferenceSet filesystem walks inline (100-800ms+ each).
	// The 5-minute GC cycle serves as a safety net for any missed chunks.
	if len(chunksToCheck) > 0 {
		r.enqueueDeferredChunkCleanup(chunksToCheck)
	}
	if len(vChunks) > 0 {
		r.enqueueDeferredChunkCleanup(vChunks)
	}

	r.logger.Info().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("from", msg.From).
		Msg("Successfully imported object metadata")

	r.incrementReceivedCount()
	return nil
}

// handleObjectManifest processes an incoming object manifest from a peer.
// It purges local objects not present in the manifest, recovering from lost deletes.
func (r *Replicator) handleObjectManifest(msg *Message) error {
	var payload ObjectManifestPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal object manifest payload: %w", err)
	}

	// Build set of (bucket, key) from the manifest for O(1) lookups
	manifestSet := make(map[string]map[string]struct{}, len(payload.BucketKeys))
	for bucket, keys := range payload.BucketKeys {
		keySet := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			keySet[k] = struct{}{}
		}
		manifestSet[bucket] = keySet
	}

	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Minute)
	defer cancel()

	localKeys, err := r.s3.GetAllObjectKeys(ctx)
	if err != nil {
		r.logger.Warn().Err(err).Msg("Manifest reconciliation: failed to get local object keys")
		return fmt.Errorf("get local keys: %w", err)
	}

	// Collect objects to purge: local objects not in the manifest.
	// Safety: skip system bucket, only purge from buckets the sender owns.
	//
	// Two cases handled:
	//   1. Bucket in manifest: purge objects locally present but absent from manifest
	//      (sender updated/deleted individual objects).
	//   2. Bucket NOT in manifest: sender owns the bucket locally but excluded it
	//      entirely → sender deleted the whole bucket → purge all local objects.
	//      This is the primary mechanism for cleaning up replicated buckets after
	//      a file share (or any bucket) is deleted on the primary coordinator.
	//
	// maxPurgePerManifest caps the object-level reconciliation case (case 1) to
	// limit damage from unexpected state mismatches. Case 2 (bucket deletion) has
	// no cap because we must purge the entire bucket to recover storage.
	const maxPurgePerManifest = 1000
	var purged int

	// Resolve the sender's coordinator name once (msg.From is a mesh IP).
	senderName := r.getPeerName(msg.From)

	for bucket, keys := range localKeys {
		// Never purge system bucket based on manifest
		if bucket == auth.SystemBucket {
			continue
		}

		// Only process buckets the sender actually owns. On replicas, the
		// bucket owner is set to the source coordinator's node ID during import.
		// This prevents coordinator A's manifest from purging objects in buckets
		// owned by coordinator C that were replicated to this node.
		if owner := r.s3.GetBucketOwner(ctx, bucket); owner == "" || owner != senderName {
			continue
		}

		senderKeys, senderHasBucket := manifestSet[bucket]
		if !senderHasBucket {
			// Sender owns this bucket but omitted it from the manifest entirely.
			// This means the sender deleted the bucket; purge all local objects.
			for _, key := range keys {
				if err := r.s3.PurgeObject(ctx, bucket, key); err != nil {
					r.logger.Warn().Err(err).
						Str("bucket", bucket).
						Str("key", key).
						Msg("Manifest reconciliation: failed to purge object from deleted bucket")
					continue
				}
				purged++
			}
			continue
		}

		// Bucket exists in both the manifest and locally: reconcile at object level.
		for _, key := range keys {
			if purged >= maxPurgePerManifest {
				r.logger.Info().
					Int("purged", purged).
					Msg("Manifest reconciliation: hit per-manifest purge cap")
				return nil
			}

			if _, exists := senderKeys[key]; !exists {
				if err := r.s3.PurgeObject(ctx, bucket, key); err != nil {
					r.logger.Warn().Err(err).
						Str("bucket", bucket).
						Str("key", key).
						Msg("Manifest reconciliation: failed to purge stale object")
					continue
				}
				purged++
			}
		}
	}

	if purged > 0 {
		r.logger.Info().
			Int("purged", purged).
			Str("from", msg.From).
			Str("sender_name", senderName).
			Msg("Manifest reconciliation: purged stale objects")
	}

	return nil
}

// ==== Chunk Cleanup After Replication ====
