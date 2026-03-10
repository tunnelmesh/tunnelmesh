// Package s3 provides an S3-compatible object storage service for the coordinator.
package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/rs/zerolog"
)

// GCGracePeriod is the minimum age of a chunk before it can be garbage collected.
// This prevents deleting chunks that are being replicated or uploaded.
//
// IMPORTANT: This must be longer than the maximum time for metadata to arrive
// after chunk replication. Auto-sync has a 2-min initial delay + 5-min interval,
// so metadata can take up to 7 minutes to arrive. 10 minutes provides margin.
//
// For large file uploads or high-latency networks: Consider 1 hour or more
const GCGracePeriod = 10 * time.Minute

// ecDataShards is the number of data shards for the universal RS(4,2) encoder.
const ecDataShards = 4

// ecParityShards is the number of parity shards for the universal RS(4,2) encoder.
const ecParityShards = 2

// ecStreamBlock is the RS stripe size: data read per iteration of the streaming
// write loop. Must equal ecDataShards * ecPieceSize.
const ecStreamBlock = 4 * 1024 * 1024 // 4 MB

// ecPieceSize is the size of each shard piece within one RS stripe.
// A piece is stored as a single fixed-size CAS chunk (no CDC).
const ecPieceSize = ecStreamBlock / ecDataShards // 1 MB

// Compile-time assertion: ecStreamBlock must equal ecDataShards * ecPieceSize.
var _ [1]struct{} = [ecStreamBlock / (ecDataShards * ecPieceSize)]struct{}{}

// contextCheckInterval is how often to check for context cancellation during
// chunk fetching loops (~400KB at average chunk size of 4KB).
const contextCheckInterval = 100

// windowsFileRetries is the number of retry attempts for file operations on Windows,
// where antivirus or search indexer may transiently hold file handles.
const windowsFileRetries = 5

// windowsFileRetryDelay is the delay between file operation retries on Windows.
const windowsFileRetryDelay = 50 * time.Millisecond

// removeWithRetry removes a file, retrying on Windows where antivirus or search
// indexer may transiently hold file handles. Returns nil if the file doesn't exist.
func removeWithRetry(path string) error {
	retries := 1
	if runtime.GOOS == "windows" {
		retries = windowsFileRetries
	}
	var err error
	for i := 0; i < retries; i++ {
		err = os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}
		if i < retries-1 {
			time.Sleep(windowsFileRetryDelay)
		}
	}
	return err
}

// BucketMeta contains bucket metadata.
type BucketMeta struct {
	Name              string    `json:"name"`
	CreatedAt         time.Time `json:"created_at"`
	Owner             string    `json:"owner"`                 // User ID who created the bucket
	SizeBytes         int64     `json:"size_bytes"`            // Total size of live objects (updated incrementally)
	ReplicationFactor int       `json:"replication_factor"`    // Number of replicas (1-3)
	QuotaBytes        int64     `json:"quota_bytes,omitempty"` // Per-bucket quota in bytes; 0 = unlimited
}

// BucketMetadataUpdate contains mutable bucket metadata fields (admin-only).
type BucketMetadataUpdate struct {
	ReplicationFactor *int   `json:"replication_factor,omitempty"` // Update replication factor (1-3)
	QuotaBytes        *int64 `json:"quota_bytes,omitempty"`        // Update per-bucket quota; nil = no change, 0 = remove limit
}

// ChunkMetadata contains per-chunk metadata for distributed replication.
type ChunkMetadata struct {
	Hash           string            `json:"hash"`                      // SHA-256 of chunk plaintext
	Size           int64             `json:"size"`                      // Uncompressed size
	CompressedSize int64             `json:"compressed_size,omitempty"` // Compressed size (if known)
	VersionVector  map[string]uint64 `json:"version_vector"`            // Causality tracking (coordinatorID -> counter)
	Owners         []string          `json:"owners,omitempty"`          // Coordinators that have this chunk
	FirstSeen      time.Time         `json:"first_seen"`                // When chunk was first created
	LastModified   time.Time         `json:"last_modified"`             // When chunk metadata was last updated
	ShardType      string            `json:"shard_type,omitempty"`      // "data" or "parity" (for erasure-coded files)
	ShardIndex     int               `json:"shard_index,omitempty"`     // Position in RS matrix (0-based)
	ChunkSequence  int               `json:"chunk_sequence,omitempty"`  // Order within a shard (0-based, for CDC chunk reassembly)
	ParentFileID   string            `json:"parent_file_id,omitempty"`  // Link to parent file VersionID
}

// ecShardRecord is a lightweight record accumulated during the EC streaming loop.
// Using this flat struct instead of *ChunkMetadata in the hot path reduces
// per-iteration allocation from ~450 bytes to ~64 bytes. The full ChunkMetadata
// map is built once after the loop from these records.
type ecShardRecord struct {
	hash      string
	onDisk    int64
	shardType string
	shardIdx  int
	blockIdx  int
}

// ErasureCodingInfo contains erasure coding metadata for a specific object version.
type ErasureCodingInfo struct {
	Enabled         bool     `json:"enabled"`                 // Whether this object uses erasure coding
	DataShards      int      `json:"data_shards"`             // k: number of data shards
	ParityShards    int      `json:"parity_shards"`           // m: number of parity shards
	StreamBlockSize int64    `json:"stream_block_size"`       // RS stripe size in bytes (DataShards * piece_size)
	DataHashes      []string `json:"data_hashes,omitempty"`   // Piece hashes for data shards, shard-major order
	ParityHashes    []string `json:"parity_hashes,omitempty"` // Piece hashes for parity shards, shard-major order
}

// ObjectMeta contains object metadata.
type ObjectMeta struct {
	Key           string                    `json:"key"`
	Size          int64                     `json:"size"`
	ContentType   string                    `json:"content_type"`
	ETag          string                    `json:"etag"` // MD5 hash of content
	LastModified  time.Time                 `json:"last_modified"`
	Expires       *time.Time                `json:"expires,omitempty"`        // Optional expiration date
	Metadata      map[string]string         `json:"metadata,omitempty"`       // User-defined metadata
	VersionID     string                    `json:"version_id,omitempty"`     // Version identifier
	Chunks        []string                  `json:"chunks,omitempty"`         // Ordered list of chunk hashes (CAS)
	ChunkMetadata map[string]*ChunkMetadata `json:"chunk_metadata,omitempty"` // Per-chunk metadata with version vectors
	VersionVector map[string]uint64         `json:"version_vector,omitempty"` // File-level version vector
	ErasureCoding *ErasureCodingInfo        `json:"erasure_coding,omitempty"` // Erasure coding info (if enabled)
}

// VersionInfo contains version information for listing.
type VersionInfo struct {
	VersionID    string    `json:"version_id"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
	IsCurrent    bool      `json:"is_current"`
}

// RecycledEntry represents a deleted object in the recycle bin.
type RecycledEntry struct {
	ID          string     `json:"id"`           // UUID filename (without .json)
	OriginalKey string     `json:"original_key"` // Object key before deletion
	DeletedAt   time.Time  `json:"deleted_at"`   // When the object was deleted
	Meta        ObjectMeta `json:"meta"`         // Full object metadata snapshot
}

// Store provides S3 storage with content-addressable chunks and versioning.
// Directory structure:
//
//	{dataDir}/
//	  chunks/
//	    {hash}                # content-addressed, compressed, encrypted blocks
//	  buckets/
//	    {bucket}/
//	      _meta.json          # bucket metadata
//	      meta/
//	        {key}.json        # object metadata (includes version ID and chunk list)
//	      versions/
//	        {key}/
//	          {versionID}.json # version metadata
//
// VersionRetentionPolicy configures smart tiered version retention.
type VersionRetentionPolicy struct {
	RecentDays    int // Keep all versions from last N days
	WeeklyWeeks   int // Then keep one version per week for N weeks
	MonthlyMonths int // Then keep one version per month for N months
}

// ChunkRegistryInterface defines operations for tracking chunk ownership across coordinators.
// This interface allows the Store to integrate with the distributed chunk registry
// without creating a circular dependency with the replication package.
type ChunkRegistryInterface interface {
	// RegisterChunk registers a chunk as owned by the local coordinator
	RegisterChunk(hash string, size int64) error

	// RegisterChunkWithReplication registers a chunk with a custom replication factor
	RegisterChunkWithReplication(hash string, size int64, replicationFactor int) error

	// RegisterShardChunk registers an erasure-coded shard chunk with shard metadata
	RegisterShardChunk(hash string, size int64, parentFileID, shardType string, shardIndex, replicationFactor int) error

	// UnregisterChunk removes the local coordinator as an owner of the chunk
	UnregisterChunk(hash string) error

	// GetOwners returns the list of coordinator IDs that own the chunk
	GetOwners(hash string) ([]string, error)

	// GetChunksOwnedBy returns all chunk hashes owned by the specified coordinator
	GetChunksOwnedBy(coordID string) ([]string, error)
}

// Store provides S3 storage with content-addressable chunks and versioning.
// ReplicatorInterface defines the interface for chunk replication (to avoid import cycle).
type ReplicatorInterface interface {
	FetchChunk(ctx context.Context, peerID, chunkHash string) ([]byte, error)
	GetPeers() []string
}

type Store struct {
	dataDir                  string
	cas                      *CAS // Content-addressable storage for chunks
	quota                    *QuotaManager
	chunkRegistry            ChunkRegistryInterface   // Optional distributed chunk ownership tracking
	replicator               ReplicatorInterface      // Optional replicator for fetching remote chunks (Phase 5)
	coordinatorID            string                   // Local coordinator ID for version vectors (optional)
	logger                   zerolog.Logger           // Structured logger
	onObjectRemovedCallback  func(bucket, key string) // Called when an object is permanently removed from disk
	onObjectRecycledCallback func(bucket, key string) // Called when an object is moved to the recycle bin
	onBucketRemovedCallback  func(bucket string)      // Called when an entire bucket is removed from disk
	defaultObjectExpiryDays  int                      // Days until objects expire (0 = never)
	defaultShareExpiryDays   int                      // Days until file shares expire (0 = never)
	recyclebinRetentionHours float64                  // Hours to retain recycled objects before purging (0 = never purge)
	versionRetentionDays     int                      // Days to retain object versions (0 = forever)
	maxVersionsPerObject     int                      // Max versions to keep per object (0 = unlimited)
	versionRetentionPolicy   VersionRetentionPolicy
	// uploadSem caps concurrent PutObject calls. Each streaming EC upload uses
	// ~10 MB peak heap (4 MB blockBuf + 2 MB parity bufs). 3 slots = ~30 MB,
	// well within GOMEMLIMIT. Bounds concurrent CAS write pressure too.
	uploadSem chan struct{}
	// ecEncoder is the shared RS(4,2) encoder. Initialized once in NewStore() with
	// WithAutoGoroutines so Encode() uses 2×GOMAXPROCS goroutines instead of ~96.
	// Safe for concurrent use: Encode() reads the immutable matrix and operates
	// only on caller-provided shard slices (no shared mutable state).
	ecEncoder          reedsolomon.Encoder
	bgWg               sync.WaitGroup // Tracks background goroutines (e.g., shard caching)
	gcThrottleDisabled bool           // Skip GC CPU throttle sleeps (set by tests to avoid long waits)
	mu                 sync.RWMutex

	// Incremental CAS stats — atomic for lock-free metrics reads.
	// Initialized from filesystem walk at startup, updated at each mutation point.
	statsChunkCount    atomic.Int64
	statsChunkBytes    atomic.Int64 // on-disk (compressed+encrypted)
	statsObjectCount   atomic.Int64
	statsVersionCount  atomic.Int64
	statsLogicalBytes  atomic.Int64 // live object sizes summed per-object
	statsVersionBytes  atomic.Int64 // logical bytes in version files
	statsRecycledBytes atomic.Int64 // logical bytes in recyclebin entries
}

// NewStore creates a new S3 store with the given data directory.
// If quota is nil, no quota enforcement is applied.
func NewStore(dataDir string, quota *QuotaManager) (*Store, error) {
	bucketsDir := filepath.Join(dataDir, "buckets")
	if err := os.MkdirAll(bucketsDir, 0755); err != nil {
		return nil, fmt.Errorf("create buckets dir: %w", err)
	}

	enc, err := reedsolomon.New(ecDataShards, ecParityShards,
		reedsolomon.WithAutoGoroutines(ecPieceSize),
	)
	if err != nil {
		return nil, fmt.Errorf("create RS encoder: %w", err)
	}

	store := &Store{
		dataDir:   dataDir,
		quota:     quota,
		logger:    zerolog.Nop(),          // Default to no-op logger
		uploadSem: make(chan struct{}, 3), // Allow 3 concurrent EC uploads (~10 MB each)
		ecEncoder: enc,
	}

	// Calculate initial quota usage from existing objects
	if quota != nil {
		if err := store.calculateQuotaUsage(); err != nil {
			return nil, fmt.Errorf("calculate quota usage: %w", err)
		}
	}

	return store, nil
}

// syncedWriteFile atomically writes data to a file using write-to-temp + rename.
// This prevents data loss from disk-full (ENOSPC) or crash during write:
// data is written to a temp file in the same directory, fsynced, then renamed
// over the target. If any step fails, the original file is untouched.
//
// During tests, fsync is skipped (detected via TUNNELMESH_TEST=1 env var) since:
// - Tests use temp directories that are discarded anyway
// - fsync is very slow on Windows (100-500ms per call)
// - 179 S3 tests × fsync = 7+ minutes on Windows CI
func syncedWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)

	// Write to a temp file in the same directory (same filesystem for atomic rename)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	// Clean up temp file on any failure
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(perm); err != nil {
		return err
	}

	if _, err := tmp.Write(data); err != nil {
		return err
	}

	// Skip fsync during tests to avoid 7+ minute test times on Windows
	// Production code always fsyncs for durability
	if os.Getenv("TUNNELMESH_TEST") == "" {
		if err := tmp.Sync(); err != nil {
			return err
		}
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	// Atomic rename — on POSIX, rename within same filesystem is atomic
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}

	success = true
	return nil
}

// atomicWriteFile writes data to a file using atomic rename but without fsync.
// This is faster than syncedWriteFile (~0.1ms vs ~10-50ms) because it skips
// the fsync syscall. The data may be lost on power failure, but the file will
// never be partially written (atomic rename guarantees).
//
// Used for metadata writes under the global lock (PutObject, ImportObjectMeta)
// to minimize lock hold time. Metadata is recoverable via replication auto-sync.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(perm); err != nil {
		return err
	}

	if _, err := tmp.Write(data); err != nil {
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, path); err != nil {
		return err
	}

	success = true
	return nil
}

// NewStoreWithCAS creates a new S3 store with CAS (Content-Addressable Storage) enabled.
// This enables CDC chunking, encryption, compression, and version history.
func NewStoreWithCAS(dataDir string, quota *QuotaManager, masterKey [32]byte) (*Store, error) {
	store, err := NewStore(dataDir, quota)
	if err != nil {
		return nil, err
	}

	// Initialize CAS
	chunksDir := filepath.Join(dataDir, "chunks")
	cas, err := NewCAS(chunksDir, masterKey)
	if err != nil {
		return nil, fmt.Errorf("create CAS: %w", err)
	}
	store.cas = cas

	// Register a self-heal callback: when ReadChunk detects a MAC failure and
	// deletes a corrupt chunk, update storage stats and unregister from the
	// chunk registry so GC and quota tracking remain consistent.
	cas.onSelfHeal = func(hash string, freed int64) {
		if freed > 0 {
			store.statsChunkCount.Add(-1)
			store.statsChunkBytes.Add(-freed)
		}
		if store.chunkRegistry != nil {
			_ = store.chunkRegistry.UnregisterChunk(hash)
		}
	}

	// Initialize chunk stats synchronously before returning the store.
	// Running asynchronously with Store() caused a race: concurrent WriteChunk
	// Add() increments were silently overwritten by Store(), and GC then
	// decremented past zero, producing negative storage metrics.
	store.initCASStats()

	return store, nil
}

// DataDir returns the data directory path.
func (s *Store) DataDir() string {
	return s.dataDir
}

// SetChunkRegistry sets the distributed chunk registry for tracking ownership.
// This is optional and only used when replication is enabled.
func (s *Store) SetChunkRegistry(registry ChunkRegistryInterface) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunkRegistry = registry
}

// SetReplicator sets the replicator for fetching remote chunks (Phase 5).
func (s *Store) SetReplicator(replicator ReplicatorInterface) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicator = replicator
}

// WaitBackground blocks until all background goroutines (e.g., shard caching) complete.
func (s *Store) WaitBackground() {
	s.bgWg.Wait()
}

// SetCoordinatorID sets the coordinator ID for version vector tracking.
// This is optional and only used when replication is enabled.
func (s *Store) SetCoordinatorID(coordID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.coordinatorID = coordID
}

// CoordinatorID returns the coordinator ID used for version vector tracking.
func (s *Store) CoordinatorID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.coordinatorID
}

// SetLogger sets the logger for the store.
func (s *Store) SetLogger(logger zerolog.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger = logger
}

// SetOnObjectRemovedCallback sets a callback invoked when an object is permanently removed from disk.
// This covers both PurgeObject (replication deletes, hard-delete path) and purgeRecycledEntries (GC).
// The callback receives the bucket name and object key that was removed.
func (s *Store) SetOnObjectRemovedCallback(cb func(bucket, key string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onObjectRemovedCallback = cb
}

// SetOnObjectRecycledCallback sets a callback invoked when an object is soft-deleted (moved to recycle bin).
// The callback receives the bucket name and object key that was recycled.
func (s *Store) SetOnObjectRecycledCallback(cb func(bucket, key string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onObjectRecycledCallback = cb
}

// SetOnBucketRemovedCallback sets a callback invoked when an entire bucket is removed from disk.
// This covers ForceDeleteBucket (file share deletion, orphaned bucket GC).
func (s *Store) SetOnBucketRemovedCallback(cb func(bucket string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onBucketRemovedCallback = cb
}

// SetDefaultObjectExpiryDays sets the default expiry for new objects in days.
// A value of 0 means objects don't expire.
func (s *Store) SetDefaultObjectExpiryDays(days int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultObjectExpiryDays = days
}

// SetDefaultShareExpiryDays sets the default expiry for new file shares in days.
// A value of 0 means shares don't expire.
func (s *Store) SetDefaultShareExpiryDays(days int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultShareExpiryDays = days
}

// DefaultShareExpiryDays returns the configured default share expiry in days.
func (s *Store) DefaultShareExpiryDays() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.defaultShareExpiryDays
}

// VolumeStats returns filesystem volume statistics for the store's data directory.
func (s *Store) VolumeStats() (total, used, available int64, err error) {
	return GetVolumeStats(s.dataDir)
}

// QuotaStats returns current quota statistics, or nil if no quota is configured.
func (s *Store) QuotaStats() *QuotaStats {
	if s.quota == nil {
		return nil
	}
	stats := s.quota.Stats()
	return &stats
}

// calculateQuotaUsage scans all object metadata and rebuilds per-bucket quota
// tracking from logical file sizes. This keeps quota consistent with runtime
// tracking (which also uses logical sizes) and avoids the double-counting that
// occurs when physical chunk sizes are mixed with logical bucket sizes.
//
// Called at startup (after CAS init) and after GC to reconcile quota.
// Uses s.dataDir (immutable after construction); QuotaManager.SetUsed has its
// own internal mutex so no Store lock is needed.
func (s *Store) calculateQuotaUsage() error {
	if s.quota == nil {
		return nil
	}

	bucketsDir := filepath.Join(s.dataDir, "buckets")
	bucketEntries, err := os.ReadDir(bucketsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read buckets dir: %w", err)
	}

	for _, bucketEntry := range bucketEntries {
		if !bucketEntry.IsDir() {
			continue
		}
		bucketName := bucketEntry.Name()
		// System bucket is exempt from quota (listing indexes, stats, etc. are
		// infrastructure overhead, not user-quota-tracked data).
		if bucketName == SystemBucket {
			continue
		}

		// Walk the "meta" subdirectory which holds active (non-recycled) objects.
		metaDir := filepath.Join(bucketsDir, bucketName, "meta")
		var bucketLogicalSize int64
		_ = filepath.Walk(metaDir, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil || info.IsDir() || !strings.HasSuffix(path, ".json") {
				return nil
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil
			}
			var meta ObjectMeta
			if jsonErr := json.Unmarshal(data, &meta); jsonErr != nil {
				return nil
			}
			bucketLogicalSize += meta.Size
			return nil
		})
		s.quota.SetUsed(bucketName, bucketLogicalSize)
	}
	return nil
}

// validateName validates a bucket or object key name to prevent path traversal.
// This is a defense-in-depth measure that runs at the storage layer.
// The admin.go also has validateS3Name which performs identical validation at
// the API layer. Both functions are intentionally duplicated to ensure path
// traversal protection even if one layer is bypassed or refactored.
func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	// Check for null bytes which could truncate paths on some filesystems
	if strings.ContainsRune(name, 0) {
		return fmt.Errorf("null bytes not allowed")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("invalid name")
	}
	// Check for path traversal: ".." as a component (not "..." which is valid)
	// Split on both forward and back slashes to handle all platforms
	for _, sep := range []string{"/", "\\"} {
		parts := strings.Split(name, sep)
		for _, part := range parts {
			if part == ".." {
				return fmt.Errorf("path traversal not allowed")
			}
		}
	}
	// Block absolute paths
	if filepath.IsAbs(name) || strings.HasPrefix(name, "/") || strings.HasPrefix(name, "\\") {
		return fmt.Errorf("absolute paths not allowed")
	}
	// Block relative paths starting with "./" or ".\"
	if strings.HasPrefix(name, "./") || strings.HasPrefix(name, ".\\") {
		return fmt.Errorf("relative paths not allowed")
	}
	return nil
}

// bucketPath returns the path to a bucket directory.
func (s *Store) bucketPath(bucket string) string {
	return filepath.Join(s.dataDir, "buckets", bucket)
}

// bucketMetaPath returns the path to bucket metadata file.
func (s *Store) bucketMetaPath(bucket string) string {
	return filepath.Join(s.bucketPath(bucket), "_meta.json")
}

// objectMetaPath returns the path to an object metadata file.
func (s *Store) objectMetaPath(bucket, key string) string {
	return filepath.Join(s.bucketPath(bucket), "meta", key+".json")
}

// CreateBucket creates a new bucket with specified replication factor.
func (s *Store) CreateBucket(ctx context.Context, bucket, owner string, replicationFactor int) error {
	// Validate bucket name (defense in depth)
	if err := validateName(bucket); err != nil {
		return fmt.Errorf("invalid bucket name: %w", err)
	}

	// Validate replication factor
	if replicationFactor < 1 || replicationFactor > 3 {
		return fmt.Errorf("replication factor must be 1-3, got %d", replicationFactor)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	bucketDir := s.bucketPath(bucket)

	// Check if bucket already exists
	if _, err := os.Stat(bucketDir); err == nil {
		return ErrBucketExists
	}

	// Create bucket directory structure
	if err := os.MkdirAll(filepath.Join(bucketDir, "meta"), 0755); err != nil {
		return fmt.Errorf("create bucket meta dir: %w", err)
	}

	// Write bucket metadata
	meta := BucketMeta{
		Name:              bucket,
		CreatedAt:         time.Now().UTC(),
		Owner:             owner,
		ReplicationFactor: replicationFactor,
	}

	metaPath := s.bucketMetaPath(bucket)
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal bucket meta: %w", err)
	}

	if err := syncedWriteFile(metaPath, data, 0644); err != nil {
		return fmt.Errorf("write bucket meta: %w", err)
	}

	return nil
}

// UpdateBucketMetadata updates mutable bucket metadata (admin-only operation).
// Can update replication factor and erasure coding policy.
func (s *Store) UpdateBucketMetadata(ctx context.Context, bucket string, updates BucketMetadataUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, err := s.getBucketMeta(bucket)
	if err != nil {
		return err
	}

	// Validate and apply replication factor update
	if updates.ReplicationFactor != nil {
		rf := *updates.ReplicationFactor
		if rf < 1 || rf > 3 {
			return fmt.Errorf("replication factor must be 1-3, got %d", rf)
		}
		meta.ReplicationFactor = rf
	}

	// Validate and apply per-bucket quota update
	if updates.QuotaBytes != nil {
		if *updates.QuotaBytes < 0 {
			return fmt.Errorf("quota_bytes cannot be negative")
		}
		meta.QuotaBytes = *updates.QuotaBytes
	}

	// Save updated metadata
	return s.writeBucketMeta(bucket, meta)
}

// DeleteBucket removes an empty bucket.
func (s *Store) DeleteBucket(ctx context.Context, bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucketDir := s.bucketPath(bucket)

	// Check if bucket exists
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		return ErrBucketNotFound
	}

	// Check if bucket has any live objects
	objects, _, _, err := s.listObjectsUnsafe(bucket, "", "", 0)
	if err != nil {
		return err
	}
	if len(objects) > 0 {
		return ErrBucketNotEmpty
	}

	// Check if bucket has recycle bin entries
	recyclebinDir := s.recyclebinPath(bucket)
	if entries, err := os.ReadDir(recyclebinDir); err == nil && len(entries) > 0 {
		return fmt.Errorf("bucket has %d recycled objects: %w", len(entries), ErrBucketNotEmpty)
	}

	// Remove bucket directory.
	// On Windows, file handles may be transiently held by OS processes
	// (antivirus, search indexer), causing RemoveAll to fail. Retry briefly.
	var removeErr error
	retries := 1
	if runtime.GOOS == "windows" {
		retries = windowsFileRetries
	}
	for i := 0; i < retries; i++ {
		removeErr = os.RemoveAll(bucketDir)
		if removeErr == nil {
			return nil
		}
		if i < retries-1 {
			time.Sleep(windowsFileRetryDelay)
		}
	}
	return fmt.Errorf("remove bucket: %w", removeErr)
}

// ForceDeleteBucket removes a bucket and all its contents without per-object purge.
// This is used for file share deletion where orphaned chunk cleanup can be deferred
// to the next GC cycle. Much faster than purging each object individually.
func (s *Store) ForceDeleteBucket(ctx context.Context, bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucketDir := s.bucketPath(bucket)

	// Check if bucket exists
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		return nil // Idempotent
	}

	// Decrement stats for all objects being removed.
	// Corrupted files that can't be read/unmarshalled will cause stats drift
	// until the next initCASStats on restart — log a warning so it's diagnosable.
	metaDir := filepath.Join(bucketDir, "meta")
	_ = filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			s.logger.Warn().Err(readErr).Str("path", path).Msg("ForceDeleteBucket: unreadable metadata, stats may drift")
			return nil
		}
		var meta ObjectMeta
		if json.Unmarshal(data, &meta) != nil {
			s.logger.Warn().Str("path", path).Msg("ForceDeleteBucket: corrupted metadata, stats may drift")
			return nil
		}
		s.statsObjectCount.Add(-1)
		if meta.Size > 0 {
			s.statsLogicalBytes.Add(-meta.Size)
		}
		if s.quota != nil {
			s.quota.Release(bucket, meta.Size)
		}
		return nil
	})

	// Decrement version bytes for all versions being removed
	versionsDir := filepath.Join(bucketDir, "versions")
	_ = filepath.Walk(versionsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			s.logger.Warn().Err(readErr).Str("path", path).Msg("ForceDeleteBucket: unreadable version metadata, stats may drift")
			return nil
		}
		var meta ObjectMeta
		if json.Unmarshal(data, &meta) != nil {
			s.logger.Warn().Str("path", path).Msg("ForceDeleteBucket: corrupted version metadata, stats may drift")
			return nil
		}
		s.statsVersionCount.Add(-1)
		s.statsVersionBytes.Add(-meta.Size)
		return nil
	})

	// Decrement recycled bytes for all recyclebin entries being removed
	rbDir := s.recyclebinPath(bucket)
	_ = filepath.Walk(rbDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		var entry RecycledEntry
		if json.Unmarshal(data, &entry) == nil {
			s.statsRecycledBytes.Add(-entry.Meta.Size)
		}
		return nil
	})

	// Remove the entire bucket directory
	var removeErr error
	retries := 1
	if runtime.GOOS == "windows" {
		retries = windowsFileRetries
	}

	// Capture callback before loop to ensure consistent access pattern
	callback := s.onBucketRemovedCallback

	for i := 0; i < retries; i++ {
		removeErr = os.RemoveAll(bucketDir)
		if removeErr == nil {
			// Notify listing index of bucket removal (no deadlock: callback uses atomic CAS, not s.mu)
			if callback != nil {
				callback(bucket)
			}
			return nil
		}
		if i < retries-1 {
			time.Sleep(windowsFileRetryDelay)
		}
	}
	return fmt.Errorf("remove bucket: %w", removeErr)
}

// writeBucketMeta writes bucket metadata (caller must hold lock).
func (s *Store) writeBucketMeta(bucket string, meta *BucketMeta) error {
	metaPath := s.bucketMetaPath(bucket)
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal bucket meta: %w", err)
	}
	if err := syncedWriteFile(metaPath, data, 0644); err != nil {
		return fmt.Errorf("write bucket meta: %w", err)
	}
	return nil
}

// GetBucketOwner returns the owner of a bucket (empty string if not found).
func (s *Store) GetBucketOwner(_ context.Context, bucket string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, err := s.getBucketMeta(bucket)
	if err != nil {
		return ""
	}
	return meta.Owner
}

// HeadBucket checks if a bucket exists.
func (s *Store) HeadBucket(ctx context.Context, bucket string) (*BucketMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getBucketMeta(bucket)
}

// getBucketMeta reads bucket metadata (caller must hold lock).
func (s *Store) getBucketMeta(bucket string) (*BucketMeta, error) {
	metaPath := s.bucketMetaPath(bucket)

	data, err := os.ReadFile(metaPath)
	if os.IsNotExist(err) {
		return nil, ErrBucketNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read bucket meta: %w", err)
	}

	// Handle corrupted metadata (e.g. from past disk-full truncation)
	if len(data) == 0 {
		s.logger.Warn().Str("bucket", bucket).Str("path", metaPath).Msg("bucket metadata file is empty (possible disk-full corruption)")
		return nil, ErrBucketNotFound
	}

	var meta BucketMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		s.logger.Warn().Err(err).Str("bucket", bucket).Str("path", metaPath).Msg("bucket metadata file is corrupted")
		return nil, ErrBucketNotFound
	}

	return &meta, nil
}

// ListBuckets returns all buckets.
func (s *Store) ListBuckets(ctx context.Context) ([]BucketMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bucketsDir := filepath.Join(s.dataDir, "buckets")
	entries, err := os.ReadDir(bucketsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read buckets dir: %w", err)
	}

	var buckets []BucketMeta
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		meta, err := s.getBucketMeta(entry.Name())
		if err != nil {
			continue // Skip buckets with invalid metadata
		}
		buckets = append(buckets, *meta)
	}

	return buckets, nil
}

// CalculateBucketSize calculates the total size of all live objects in a bucket.
// Returns the total size in bytes, or 0 if the bucket doesn't exist or is empty.
func (s *Store) CalculateBucketSize(ctx context.Context, bucketName string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Verify bucket exists
	bucketMeta, err := s.getBucketMeta(bucketName)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			return 0, nil // Bucket doesn't exist, size is 0
		}
		return 0, err
	}

	// Return cached size if available (updated incrementally)
	return bucketMeta.SizeBytes, nil
}

// CalculatePrefixSize calculates the total size of all objects under a prefix.
// This is used for displaying folder sizes in the S3 explorer.
// Returns the total size in bytes.
func (s *Store) CalculatePrefixSize(ctx context.Context, bucketName, prefix string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Verify bucket exists
	if _, err := s.getBucketMeta(bucketName); err != nil {
		return 0, err
	}

	var totalSize int64

	// Iterate through all objects with the given prefix
	marker := ""
	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return totalSize, fmt.Errorf("calculate prefix size for %s/%s: %w", bucketName, prefix, ctx.Err())
		default:
		}

		objects, isTruncated, nextMarker, err := s.listObjectsUnsafe(bucketName, prefix, marker, 1000)
		if err != nil {
			return 0, fmt.Errorf("list objects: %w", err)
		}

		// Sum sizes of all live objects
		for _, obj := range objects {
			totalSize += obj.Size
		}

		if !isTruncated {
			break
		}
		marker = nextMarker
	}

	return totalSize, nil
}

// updateBucketSize updates the cached bucket size by delta.
// Must be called with write lock held.
func (s *Store) updateBucketSize(bucketName string, delta int64) error {
	bucketMeta, err := s.getBucketMeta(bucketName)
	if err != nil {
		return err
	}

	bucketMeta.SizeBytes += delta
	if bucketMeta.SizeBytes < 0 {
		bucketMeta.SizeBytes = 0 // Safety: prevent negative sizes
	}

	return s.writeBucketMeta(bucketName, bucketMeta)
}

// truncHash safely truncates a hash string for logging.
func truncHash(hash string) string {
	if len(hash) >= 8 {
		return hash[:8]
	}
	return hash
}

// fetchChunkDistributed tries local CAS first, then fetches from remote
// coordinators via the chunk registry and replicator.
func (s *Store) fetchChunkDistributed(ctx context.Context, chunkHash string) ([]byte, error) {
	// 1. Try local CAS (fast path)
	data, err := s.cas.ReadChunk(ctx, chunkHash)
	if err == nil {
		return data, nil
	}

	// 2. No distributed fetch possible without registry + replicator
	if s.chunkRegistry == nil || s.replicator == nil {
		return nil, fmt.Errorf("chunk %s not found locally and distributed fetch unavailable", truncHash(chunkHash))
	}

	// 3. Query registry for owners
	owners, regErr := s.chunkRegistry.GetOwners(chunkHash)
	if regErr != nil || len(owners) == 0 {
		return nil, fmt.Errorf("chunk %s: no remote owners found", truncHash(chunkHash))
	}

	// 4. Try each owner
	var lastErr error
	for _, ownerID := range owners {
		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		data, lastErr = s.replicator.FetchChunk(fetchCtx, ownerID, chunkHash)
		cancel()
		if lastErr != nil {
			continue
		}

		// 5. Validate hash integrity
		if actualHash := ContentHash(data); actualHash != chunkHash {
			s.logger.Warn().
				Str("expected", truncHash(chunkHash)).
				Str("actual", truncHash(actualHash)).
				Str("owner", ownerID).
				Msg("remote chunk hash mismatch, skipping")
			lastErr = fmt.Errorf("hash mismatch from owner %s", ownerID)
			continue
		}

		// 6. Cache locally and register ownership (best-effort).
		// chunkHash is validated above; pass it to avoid a redundant SHA-256.
		if onDiskBytes, werr := s.cas.writeChunkKnown(ctx, data, chunkHash); werr != nil {
			s.logger.Warn().Err(werr).Str("hash", truncHash(chunkHash)).Msg("failed to cache remote chunk locally")
		} else {
			if onDiskBytes > 0 {
				s.statsChunkCount.Add(1)
				s.statsChunkBytes.Add(onDiskBytes)
			}
			if s.chunkRegistry != nil {
				if rerr := s.chunkRegistry.RegisterChunk(chunkHash, int64(len(data))); rerr != nil {
					s.logger.Warn().Err(rerr).Str("hash", truncHash(chunkHash)).Msg("failed to register chunk ownership")
				}
			}
		}

		return data, nil
	}

	return nil, fmt.Errorf("chunk %s: failed to fetch from %d remote owners: %w", truncHash(chunkHash), len(owners), lastErr)
}

// getObjectContentWithErasureCoding reads an erasure-coded object and reconstructs
// missing data shards if necessary. Returns a reader for the reconstructed file.
//
// The write path CDC-chunks data shards, so ec.DataHashes contains chunk hashes (not shard hashes).
// We must fetch all chunks, reassemble them into data shards using ChunkMetadata.ShardIndex,
// then potentially reconstruct using Reed-Solomon if data shards are missing.
//
//nolint:gocyclo // Complexity from shard reconstruction logic
func (s *Store) getObjectContentWithErasureCoding(ctx context.Context, bucket, key string, meta *ObjectMeta) (io.ReadCloser, *ObjectMeta, error) {
	ec := meta.ErasureCoding
	k := ec.DataShards
	m := ec.ParityShards

	// Phase 1: Fetch all chunks and reassemble into data shards.
	//
	// DataHashes is shard-major: [shard0_b0, shard0_b1, ..., shard1_b0, ...]
	// so position i in DataHashes unambiguously encodes:
	//   numBlocks = len(DataHashes) / k
	//   shardIdx  = i / numBlocks
	//   blockIdx  = i % numBlocks
	//
	// We use position-derived shard routing instead of ChunkMetadata.ShardIndex
	// to correctly handle the case where multiple shards have identical content
	// (and therefore identical hashes) — which is common when small files produce
	// mostly-zero-padded RS pieces.
	numDataBlocks := 1
	if k > 0 && len(ec.DataHashes) > 0 {
		if len(ec.DataHashes)%k != 0 {
			return nil, nil, fmt.Errorf("corrupt EC metadata: DataHashes length %d not divisible by k=%d", len(ec.DataHashes), k)
		}
		numDataBlocks = len(ec.DataHashes) / k
		if numDataBlocks < 1 {
			numDataBlocks = 1
		}
	}

	// shardData[i] holds assembled piece bytes for data shard i, in block order.
	dataShardPieces := make([][][]byte, k) // [shardIdx][blockIdx] = piece bytes
	for i := range dataShardPieces {
		dataShardPieces[i] = make([][]byte, numDataBlocks)
	}
	incompleteShard := make(map[int]bool) // shards with any missing chunk

	for i, chunkHash := range ec.DataHashes {
		// Check for cancellation periodically
		if i%contextCheckInterval == 0 {
			select {
			case <-ctx.Done():
				return nil, nil, fmt.Errorf("context canceled during data chunk fetch: %w", ctx.Err())
			default:
			}
		}

		// Derive shard and block from position in DataHashes (shard-major order).
		shardIdx := i / numDataBlocks
		blockIdx := i % numDataBlocks

		// Fetch chunk from CAS (or remote coordinator).
		chunk, err := s.fetchChunkDistributed(ctx, chunkHash)
		if err != nil {
			s.logger.Debug().Str("hash", truncHash(chunkHash)).Int("shard", shardIdx).Int("block", blockIdx).Msg("data chunk unavailable")
			incompleteShard[shardIdx] = true
			continue
		}
		dataShardPieces[shardIdx][blockIdx] = chunk
	}

	// Assemble each data shard by concatenating its pieces in block order.
	dataShards := make([][]byte, k)
	for shardIdx := 0; shardIdx < k; shardIdx++ {
		if incompleteShard[shardIdx] {
			dataShards[shardIdx] = nil
			continue
		}
		// Verify all blocks are present.
		allPresent := true
		totalSize := 0
		for _, piece := range dataShardPieces[shardIdx] {
			if piece == nil {
				allPresent = false
				break
			}
			totalSize += len(piece)
		}
		if !allPresent || totalSize == 0 {
			dataShards[shardIdx] = nil
			continue
		}
		shardData := make([]byte, 0, totalSize)
		for _, piece := range dataShardPieces[shardIdx] {
			shardData = append(shardData, piece...)
		}
		dataShards[shardIdx] = shardData
	}

	// Count available data shards
	availableDataShards := 0
	for i := 0; i < k; i++ {
		if dataShards[i] != nil {
			availableDataShards++
		}
	}

	// Phase 2: Fast path - all data shards available, skip parity fetch entirely
	if availableDataShards == k {
		// No reconstruction needed — reassemble original bytes from data shards.
		// The write path splits each RS stripe (ecStreamBlock) into k equal pieces
		// and appends block b's piece to shard[i]. To recover the original byte
		// stream we must interleave pieces in block order:
		//   for each block b: concat shard[0][b], shard[1][b], ..., shard[k-1][b]
		// Concatenating entire shards (shard-major) produces the wrong order for
		// files > ecPieceSize (1 MB).
		pieceSize := int64(0)
		if numDataBlocks > 0 && k > 0 && len(dataShards[0]) > 0 {
			pieceSize = int64(len(dataShards[0])) / int64(numDataBlocks)
		}
		var fileData []byte
		if pieceSize > 0 && numDataBlocks > 1 {
			// Multi-block: interleave pieces from each shard.
			// All data shards must have exactly numDataBlocks*pieceSize bytes;
			// CAS hash validation guards against corruption, but a length mismatch
			// would cause silent truncation without this explicit check.
			for i := 0; i < k; i++ {
				expected := int64(numDataBlocks) * pieceSize
				if int64(len(dataShards[i])) != expected {
					return nil, nil, fmt.Errorf("data shard %d length %d, expected %d (versionID=%s)",
						i, len(dataShards[i]), expected, meta.VersionID)
				}
			}
			fileData = make([]byte, 0, meta.Size)
			for b := 0; b < numDataBlocks; b++ {
				for i := 0; i < k; i++ {
					start := int64(b) * pieceSize
					fileData = append(fileData, dataShards[i][start:start+pieceSize]...)
				}
			}
		} else {
			// Single-block (common case for files <= ecStreamBlock = 4 MB):
			// k pieces are already in order, concatenate directly.
			totalShardSize := int64(0)
			for i := 0; i < k; i++ {
				totalShardSize += int64(len(dataShards[i]))
			}
			fileData = make([]byte, 0, totalShardSize)
			for i := 0; i < k; i++ {
				fileData = append(fileData, dataShards[i]...)
			}
		}

		// Trim to original size (remove RS zero-padding from last block).
		if int64(len(fileData)) < meta.Size {
			return nil, nil, fmt.Errorf("reassembled data too small: got %d bytes, expected %d (versionID=%s)", len(fileData), meta.Size, meta.VersionID)
		}
		fileData = fileData[:meta.Size]

		return io.NopCloser(bytes.NewReader(fileData)), meta, nil
	}

	// Phase 3: Fetch parity shards (only needed when data shards are missing)
	// Check for cancellation before fetching parity shards
	select {
	case <-ctx.Done():
		return nil, nil, fmt.Errorf("context canceled before parity shard fetch: %w", ctx.Err())
	default:
	}

	// Parity shards: ParityHashes is also shard-major:
	// [shard0_b0, shard0_b1, ..., shard1_b0, ...], so we derive shard/block
	// from the position index — same approach as Phase 1 for data shards.
	// This avoids the hash-collision bug where two parity shards with identical
	// content (e.g. zero pads) would map to the same ChunkMetadata entry.
	numParityBlocks := 1
	if m > 0 && len(ec.ParityHashes) > 0 {
		numParityBlocks = len(ec.ParityHashes) / m
		if numParityBlocks < 1 {
			numParityBlocks = 1
		}
	}

	parityShardPieces := make([][][]byte, m) // [shardIdx][blockIdx] = piece bytes
	for i := range parityShardPieces {
		parityShardPieces[i] = make([][]byte, numParityBlocks)
	}
	incompleteParityShard := make(map[int]bool)

	for j, parityHash := range ec.ParityHashes {
		if j%contextCheckInterval == 0 {
			select {
			case <-ctx.Done():
				return nil, nil, fmt.Errorf("context canceled during parity chunk fetch: %w", ctx.Err())
			default:
			}
		}

		// Derive shard and block from position in ParityHashes (shard-major order).
		shardIdx := j / numParityBlocks
		blockIdx := j % numParityBlocks

		chunk, err := s.fetchChunkDistributed(ctx, parityHash)
		if err != nil {
			s.logger.Debug().Str("hash", truncHash(parityHash)).Int("shard", shardIdx).Int("block", blockIdx).Msg("parity chunk unavailable")
			incompleteParityShard[shardIdx] = true
			continue
		}
		parityShardPieces[shardIdx][blockIdx] = chunk
	}

	parityShards := make([][]byte, m)
	for shardIdx := 0; shardIdx < m; shardIdx++ {
		if incompleteParityShard[shardIdx] {
			parityShards[shardIdx] = nil
			continue
		}
		allPresent := true
		totalSize := 0
		for _, piece := range parityShardPieces[shardIdx] {
			if piece == nil {
				allPresent = false
				break
			}
			totalSize += len(piece)
		}
		if !allPresent || totalSize == 0 {
			parityShards[shardIdx] = nil
			continue
		}
		shardData := make([]byte, 0, totalSize)
		for _, piece := range parityShardPieces[shardIdx] {
			shardData = append(shardData, piece...)
		}
		parityShards[shardIdx] = shardData
	}

	// Count available parity shards
	availableParityShards := 0
	for i := 0; i < m; i++ {
		if parityShards[i] != nil {
			availableParityShards++
		}
	}

	// Phase 4: Reconstruction path - some data shards missing
	totalAvailable := availableDataShards + availableParityShards
	if totalAvailable < k {
		return nil, nil, fmt.Errorf("insufficient shards: need %d, have %d data + %d parity = %d total (versionID=%s)",
			k, availableDataShards, availableParityShards, totalAvailable, meta.VersionID)
	}

	s.logger.Info().
		Int("available_data", availableDataShards).
		Int("available_parity", availableParityShards).
		Int("needed", k).
		Str("version", meta.VersionID).
		Msg("reconstructing erasure-coded object")

	// Combine data + parity shards into single array for DecodeFile
	allShards := make([][]byte, k+m)
	for i := 0; i < k; i++ {
		allShards[i] = dataShards[i]
	}
	for i := 0; i < m; i++ {
		allShards[k+i] = parityShards[i]
	}

	// Check for cancellation before expensive reconstruction
	select {
	case <-ctx.Done():
		return nil, nil, fmt.Errorf("context canceled before reconstruction: %w", ctx.Err())
	default:
	}

	// Reconstruct using Reed-Solomon (block-wise streaming decoder).
	// ReconstructBlockwise uses ec.StreamBlockSize to know how large each RS stripe is.
	reconstructedData, err := ReconstructBlockwise(allShards, k, m, ec.StreamBlockSize, meta.Size)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to reconstruct file (versionID=%s): %w", meta.VersionID, err)
	}

	// Phase 5: Cache reconstructed data shards (best-effort, async)
	// This improves future read performance by storing reconstructed shards locally.
	// Deep copy reconstructed shards since ReconstructBlockwise may modify allShards in-place
	// and the caller may still be reading reconstructedData.
	cachedK := k
	cachedShards := make([][]byte, k)
	missingShards := make([]bool, k)
	for i := 0; i < k; i++ {
		missingShards[i] = dataShards[i] == nil
		if missingShards[i] && allShards[i] != nil {
			cachedShards[i] = make([]byte, len(allShards[i]))
			copy(cachedShards[i], allShards[i])
		}
	}
	s.bgWg.Add(1)
	go func() {
		defer s.bgWg.Done()
		// Use background context since original request may have completed
		bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Cache any data shards that were reconstructed
		for i := 0; i < cachedK; i++ {
			if !missingShards[i] {
				// Already had this shard
				continue
			}

			// This shard was reconstructed - cache it by chunking and storing
			reconstructedShard := cachedShards[i]
			if reconstructedShard == nil {
				s.logger.Warn().Int("shard", i).Msg("reconstructed shard is nil, skipping cache")
				continue
			}

			shardChunker := NewStreamingChunker(bytes.NewReader(reconstructedShard))
			for {
				chunk, chunkHash, err := shardChunker.NextChunk()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					s.logger.Warn().Err(err).Int("shard", i).Msg("failed to chunk reconstructed shard")
					break
				}

				// Write chunk to local CAS (best-effort); pass chunkHash to skip redundant SHA-256.
				if _, err := s.cas.writeChunkKnown(bgCtx, chunk, chunkHash); err != nil {
					if len(chunkHash) >= 8 {
						s.logger.Warn().Err(err).Int("shard", i).Str("hash", chunkHash[:8]).Msg("failed to cache reconstructed chunk")
					} else {
						s.logger.Warn().Err(err).Int("shard", i).Str("hash", chunkHash).Msg("failed to cache reconstructed chunk")
					}
				}
			}
		}
	}()

	return io.NopCloser(bytes.NewReader(reconstructedData)), meta, nil
}

// putObjectWithErasureCoding stores an object using the streaming RS(4,2)
// encoder. Each 4 MB RS stripe is split into 4 data pieces of 1 MB and 2
// parity pieces of 1 MB, written directly to CAS as fixed-size chunks.
//
// Memory: ~10 MB constant (4 MB blockBuf + 2 × 1 MB parity + overhead)
// regardless of file size. This makes EC viable for arbitrarily large files.
//
// Dedup guarantee: identical files produce identical RS pieces → identical
// SHA-256 hashes → zero extra CAS storage on re-upload.
//
// Lock strategy: the global lock is NOT held during streaming/encode/CAS-write.
// It is acquired only for brief metadata operations at the end.
//
//nolint:gocyclo
func (s *Store) putObjectWithErasureCoding(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string, metadata map[string]string, bucketMeta *BucketMeta) (*ObjectMeta, error) {
	metaPath := s.objectMetaPath(bucket, key)

	// blockBuf is the 4 MB scratch buffer. Data pieces alias its sub-slices;
	// no copy is needed before encoding (only parity pieces are written by Encode).
	blockBuf := make([]byte, ecStreamBlock)

	// pieces holds the RS shard pieces for one block.
	// Data pieces [0..ecDataShards-1] alias blockBuf.
	// Parity pieces [ecDataShards..ecDataShards+ecParityShards-1] are
	// independent allocations (Encode writes into them).
	pieces := make([][]byte, ecDataShards+ecParityShards)
	for i := 0; i < ecDataShards; i++ {
		pieces[i] = blockBuf[i*ecPieceSize : (i+1)*ecPieceSize]
	}
	for i := ecDataShards; i < ecDataShards+ecParityShards; i++ {
		pieces[i] = make([]byte, ecPieceSize)
	}

	// hashes[i] accumulates CAS chunk hashes for shard i, in block order.
	hashes := make([][]string, ecDataShards+ecParityShards)
	var shardRecords []ecShardRecord
	var chunks []string
	var actualSize int64
	blockIdx := 0

	now := time.Now().UTC()
	coordID := s.coordinatorID
	replicationFactor := bucketMeta.ReplicationFactor
	versionID := generateVersionID()

	fileVersionVector := make(map[string]uint64)
	if coordID != "" {
		fileVersionVector[coordID] = 1
	}

	// No rollback on failure: EC chunks are content-addressed. Multiple concurrent
	// uploads of similar-content files produce identical CAS hashes (deduplication),
	// so deleting a chunk on rollback could corrupt another upload that shares it.
	// Orphaned chunks from failed uploads are cleaned by the next GC run.

	md5Hasher := md5.New()

	// Streaming write loop: read ecStreamBlock at a time.
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("upload canceled: %w", ctx.Err())
		default:
		}

		n, readErr := io.ReadFull(reader, blockBuf)
		if n == 0 && errors.Is(readErr, io.EOF) {
			// No data: either empty file (blockIdx==0) or exact-block-aligned end.
			break
		}
		actualSize += int64(n)

		// Zero-pad the last (partial) block so all pieces are ecPieceSize.
		for j := n; j < ecStreamBlock; j++ {
			blockBuf[j] = 0
		}

		// Data-piece aliases already point into blockBuf — no reassignment needed.
		// RS-encode: fills parity pieces in-place.
		if encErr := s.ecEncoder.Encode(pieces); encErr != nil {
			return nil, fmt.Errorf("rs encode block %d: %w", blockIdx, encErr)
		}

		// Write each piece to CAS.
		for i := 0; i < ecDataShards+ecParityShards; i++ {
			shardType := "data"
			shardIdx := i
			if i >= ecDataShards {
				shardType = "parity"
				shardIdx = i - ecDataShards
			}

			h := sha256.Sum256(pieces[i])
			chunkHash := hex.EncodeToString(h[:])

			onDiskBytes, writeErr := s.cas.writeChunkKnown(ctx, pieces[i], chunkHash)
			if writeErr != nil {
				return nil, fmt.Errorf("write %s shard %d block %d: %w", shardType, shardIdx, blockIdx, writeErr)
			}

			var chunkOnDiskSize int64
			if onDiskBytes > 0 {
				s.statsChunkCount.Add(1)
				s.statsChunkBytes.Add(onDiskBytes)
				chunkOnDiskSize = onDiskBytes
			} else {
				if sz, szErr := s.cas.ChunkSize(ctx, chunkHash); szErr == nil {
					chunkOnDiskSize = sz
				}
			}

			if s.chunkRegistry != nil {
				if regErr := s.chunkRegistry.RegisterShardChunk(chunkHash, ecPieceSize, versionID, shardType, shardIdx, replicationFactor); regErr != nil {
					if os.Getenv("DEBUG") != "" || os.Getenv("TUNNELMESH_DEBUG") != "" {
						fmt.Fprintf(os.Stderr, "chunk registry warning: failed to register %s shard chunk %s: %v\n", shardType, chunkHash[:8], regErr)
					}
				}
			}

			shardRecords = append(shardRecords, ecShardRecord{
				hash:      chunkHash,
				onDisk:    chunkOnDiskSize,
				shardType: shardType,
				shardIdx:  shardIdx,
				blockIdx:  blockIdx,
			})

			hashes[i] = append(hashes[i], chunkHash)
			chunks = append(chunks, chunkHash)
		}

		// ETag: hash data piece bytes including zero-padding on the last block.
		// This makes the ETag stable across re-uploads of identical files, but
		// it does not equal MD5(file bytes) for files that are not an exact
		// multiple of ecStreamBlock — a known semantic departure from S3's ETag
		// convention for non-multipart uploads.
		for i := 0; i < ecDataShards; i++ {
			md5Hasher.Write(pieces[i])
		}

		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read block %d: %w", blockIdx, readErr)
		}
		blockIdx++
	}

	// Verify size if the caller provided a known Content-Length.
	if size > 0 && actualSize != size {
		return nil, fmt.Errorf("size mismatch: expected %d bytes, got %d", size, actualSize)
	}

	// Build the chunkMetadata map once from the flat shardRecords slice.
	// Doing this after the loop (vs inside) keeps per-iteration allocations at
	// ~64 bytes (ecShardRecord) instead of ~450 bytes (*ChunkMetadata + map header).
	// For a 10 GB file this reduces peak heap growth during streaming by ~7 MB.
	// sharedOwners is intentionally aliased across all ChunkMetadata entries to
	// avoid one allocation per shard. This is safe as long as all consumers treat
	// Owners as read-only after construction. Callers that need a mutable copy
	// (e.g., replication merge) must copy before appending: append([]string(nil), m.Owners...).
	var sharedOwners []string
	if coordID != "" {
		sharedOwners = []string{coordID}
	}
	chunkMetadata := make(map[string]*ChunkMetadata, len(shardRecords))
	for _, rec := range shardRecords {
		vv := make(map[string]uint64, 1)
		if coordID != "" {
			vv[coordID] = 1
		}
		chunkMetadata[rec.hash] = &ChunkMetadata{
			Hash:           rec.hash,
			Size:           ecPieceSize,
			CompressedSize: rec.onDisk,
			VersionVector:  vv,
			Owners:         sharedOwners,
			FirstSeen:      now,
			LastModified:   now,
			ShardType:      rec.shardType,
			ShardIndex:     rec.shardIdx,
			ChunkSequence:  rec.blockIdx,
			ParentFileID:   versionID,
		}
	}

	// Flatten per-shard hashes into DataHashes and ParityHashes.
	var dataHashes []string
	for i := 0; i < ecDataShards; i++ {
		dataHashes = append(dataHashes, hashes[i]...)
	}
	var parityHashes []string
	for i := ecDataShards; i < ecDataShards+ecParityShards; i++ {
		parityHashes = append(parityHashes, hashes[i]...)
	}

	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(md5Hasher.Sum(nil)))

	// Pre-create directories outside the lock (MkdirAll is idempotent).
	if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
		return nil, fmt.Errorf("create meta dir: %w", err)
	}

	// Phase 2: Acquire global lock for brief metadata operations only.
	s.mu.Lock()

	// Re-check bucket (could have been deleted during streaming).
	ecBucketMeta, err := s.getBucketMeta(bucket)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}

	// Check if object already exists (for quota and versioning).
	var oldSize int64
	var oldLogicalBytes int64
	isNewObject := true
	if oldMeta, metaErr := s.getObjectMeta(bucket, key); metaErr == nil {
		oldSize = oldMeta.Size
		isNewObject = false
		oldLogicalBytes = oldMeta.Size
	}

	// Quota check (system bucket is exempt — listing indexes, stats, and other
	// internal files are infrastructure overhead, not user-quota-tracked data).
	if s.quota != nil && actualSize > oldSize && bucket != SystemBucket {
		delta := actualSize - oldSize
		if !s.quota.CanAllocate(delta) {
			s.mu.Unlock()
			return nil, ErrQuotaExceeded
		}
		if ecBucketMeta.QuotaBytes > 0 {
			projected := ecBucketMeta.SizeBytes + delta
			if projected > ecBucketMeta.QuotaBytes {
				s.mu.Unlock()
				return nil, fmt.Errorf("%w: using %d of %d bytes", ErrBucketQuotaExceeded, projected, ecBucketMeta.QuotaBytes)
			}
		}
	}

	// Clear stale version files from a previous object lifecycle at this key.
	if isNewObject {
		s.clearVersionDir(bucket, key)
	}

	// Archive current version.
	s.archiveCurrentVersionAtomic(bucket, key)

	var quotaUpdated bool
	if s.quota != nil {
		if oldSize > 0 {
			s.quota.Update(bucket, oldSize, actualSize)
		} else {
			s.quota.Allocate(bucket, actualSize)
		}
		quotaUpdated = true
	}

	objMeta := ObjectMeta{
		Key:           key,
		Size:          actualSize,
		ContentType:   contentType,
		ETag:          etag,
		LastModified:  now,
		Metadata:      metadata,
		VersionID:     versionID,
		Chunks:        chunks,
		ChunkMetadata: chunkMetadata,
		VersionVector: fileVersionVector,
		ErasureCoding: &ErasureCodingInfo{
			Enabled:         true,
			DataShards:      ecDataShards,
			ParityShards:    ecParityShards,
			StreamBlockSize: ecStreamBlock,
			DataHashes:      dataHashes,
			ParityHashes:    parityHashes,
		},
	}

	if s.defaultObjectExpiryDays > 0 && bucket != SystemBucket {
		expiry := now.AddDate(0, 0, s.defaultObjectExpiryDays)
		objMeta.Expires = &expiry
	}

	metaData, marshalErr := json.MarshalIndent(objMeta, "", "  ")
	if marshalErr != nil {
		if quotaUpdated {
			if oldSize > 0 {
				s.quota.Update(bucket, actualSize, oldSize)
			} else {
				s.quota.Release(bucket, actualSize)
			}
		}
		s.mu.Unlock()
		return nil, fmt.Errorf("marshal object meta: %w", marshalErr)
	}

	if writeErr := atomicWriteFile(metaPath, metaData, 0644); writeErr != nil {
		if quotaUpdated {
			if oldSize > 0 {
				s.quota.Update(bucket, actualSize, oldSize)
			} else {
				s.quota.Release(bucket, actualSize)
			}
		}
		s.mu.Unlock()
		return nil, fmt.Errorf("write object meta: %w", writeErr)
	}

	if isNewObject {
		s.statsObjectCount.Add(1)
	}
	s.statsLogicalBytes.Add(actualSize - oldLogicalBytes)

	// Update bucket size tracking.
	sizeDelta := actualSize - oldSize
	if sizeDelta != 0 {
		ecBucketMeta.SizeBytes += sizeDelta
		if ecBucketMeta.SizeBytes < 0 {
			ecBucketMeta.SizeBytes = 0
		}
		bmData, bmErr := json.Marshal(ecBucketMeta)
		if bmErr != nil {
			s.logger.Warn().Err(bmErr).Str("bucket", bucket).Msg("failed to marshal bucket meta")
		} else if bmWriteErr := atomicWriteFile(s.bucketMetaPath(bucket), bmData, 0644); bmWriteErr != nil {
			s.logger.Warn().Err(bmWriteErr).Str("bucket", bucket).Int64("delta", sizeDelta).Msg("failed to update bucket size")
		}
	}

	s.mu.Unlock()

	// Phase 3: After lock — pruning (filesystem walk, safe without lock).
	s.pruneExpiredVersions(ctx, bucket, key)

	return &objMeta, nil
}

// PutObject writes an object to a bucket using the universal streaming RS(4,2)
// erasure encoder. EC is always on — all objects use it regardless of size or
// bucket settings.
//
// Lock strategy: the global lock is held only for brief metadata reads/writes,
// NOT during streaming I/O. CAS writes are safe for concurrent access
// (content-addressed, atomic rename).
//
// Context cancellation: partial uploads leave orphaned chunks until next GC.
func (s *Store) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string, metadata map[string]string) (*ObjectMeta, error) {
	// Validate names (defense in depth).
	if err := validateName(bucket); err != nil {
		return nil, fmt.Errorf("invalid bucket name: %w", err)
	}
	if err := validateName(key); err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}
	if s.cas == nil {
		return nil, fmt.Errorf("CAS not initialized - call InitCAS first")
	}

	// Read bucket metadata.
	s.mu.RLock()
	bucketMeta, err := s.getBucketMeta(bucket)
	if err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	s.mu.RUnlock()

	// Bound concurrent uploads. Each upload uses ~10 MB peak heap.
	select {
	case s.uploadSem <- struct{}{}:
		defer func() { <-s.uploadSem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.putObjectWithErasureCoding(ctx, bucket, key, reader, size, contentType, metadata, bucketMeta)
}

// GetObject retrieves an object from a bucket.
func (s *Store) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectMeta, error) {
	// Validate names (defense in depth)
	if err := validateName(bucket); err != nil {
		return nil, nil, fmt.Errorf("invalid bucket name: %w", err)
	}
	if err := validateName(key); err != nil {
		return nil, nil, fmt.Errorf("invalid key: %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return nil, nil, err
	}

	// Read object metadata
	meta, err := s.getObjectMeta(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	return s.getObjectContent(ctx, bucket, key, meta)
}

// HeadObject returns object metadata without the body.
func (s *Store) HeadObject(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return nil, err
	}

	return s.getObjectMeta(bucket, key)
}

// getObjectMeta reads object metadata (caller must hold lock).
func (s *Store) getObjectMeta(bucket, key string) (*ObjectMeta, error) {
	metaPath := s.objectMetaPath(bucket, key)

	data, err := os.ReadFile(metaPath)
	if os.IsNotExist(err) {
		return nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read object meta: %w", err)
	}

	var meta ObjectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal object meta: %w", err)
	}

	return &meta, nil
}

// DeleteObject moves an object to the recycle bin.
// The object is removed from the live path but its metadata is preserved in
// recyclebin/{uuid}.json for recovery. Chunks and versions remain intact.
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	// Validate names (defense in depth)
	if err := validateName(bucket); err != nil {
		return fmt.Errorf("invalid bucket name: %w", err)
	}
	if err := validateName(key); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	err := s.RecycleObject(ctx, bucket, key)
	// S3 semantics: delete is idempotent — deleting a non-existent object succeeds
	if errors.Is(err, ErrObjectNotFound) {
		return nil
	}
	return err
}

// recyclebinPath returns the path to a bucket's recycle bin directory.
func (s *Store) recyclebinPath(bucket string) string {
	return filepath.Join(s.bucketPath(bucket), "recyclebin")
}

// RecycleObject moves an object from the live path into the recycle bin.
// The metadata is preserved in recyclebin/{uuid}.json for recovery.
// Chunks and versions remain intact. Bucket SizeBytes and quota are updated.
func (s *Store) RecycleObject(ctx context.Context, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return err
	}

	// Get object metadata
	meta, err := s.getObjectMeta(bucket, key)
	if err != nil {
		return err
	}

	// Create recycle bin directory
	rbDir := s.recyclebinPath(bucket)
	if err := os.MkdirAll(rbDir, 0755); err != nil {
		return fmt.Errorf("create recyclebin dir: %w", err)
	}

	// Generate UUID for recycle bin entry
	var uuidBytes [16]byte
	if _, err := cryptorand.Read(uuidBytes[:]); err != nil {
		return fmt.Errorf("generate recyclebin UUID: %w", err)
	}
	entryID := hex.EncodeToString(uuidBytes[:])

	// Create recycled entry
	entry := RecycledEntry{
		ID:          entryID,
		OriginalKey: key,
		DeletedAt:   time.Now().UTC(),
		Meta:        *meta,
	}

	// Write recycle bin entry first, then remove live meta. If the process crashes
	// between these two operations, both the live object and the recycle bin entry
	// will exist on disk. This is safe: the live object takes precedence for all
	// read operations, and the orphaned recycle bin entry will be purged by
	// retention or the next GC cycle. We write the entry first so that a crash
	// after removal doesn't lose the object metadata entirely.
	entryData, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal recycled entry: %w", err)
	}

	entryPath := filepath.Join(rbDir, entryID+".json")
	if err := syncedWriteFile(entryPath, entryData, 0644); err != nil {
		return fmt.Errorf("write recycled entry: %w", err)
	}

	// Remove object from live path
	metaPath := s.objectMetaPath(bucket, key)
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		// Best-effort: remove the recycle bin entry since we failed to remove the live object
		_ = os.Remove(entryPath)
		return fmt.Errorf("remove live object meta: %w", err)
	}

	// Update bucket size
	if meta.Size > 0 {
		_ = s.updateBucketSize(bucket, -meta.Size)
	}

	// Release quota
	if s.quota != nil && meta.Size > 0 {
		s.quota.Release(bucket, meta.Size)
	}

	// Update stats
	s.statsObjectCount.Add(-1)
	if meta.Size > 0 {
		s.statsLogicalBytes.Add(-meta.Size)
	}
	s.statsRecycledBytes.Add(meta.Size)

	// Notify listing index (no deadlock: callback uses atomic CAS, not s.mu)
	if s.onObjectRecycledCallback != nil {
		s.onObjectRecycledCallback(bucket, key)
	}

	return nil
}

// RestoreRecycledObject restores the most recent recycled entry for a key back to the live path.
// Returns an error if a live object with that key already exists.
func (s *Store) RestoreRecycledObject(ctx context.Context, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return err
	}

	// Check if a live object with this key already exists
	if _, err := s.getObjectMeta(bucket, key); err == nil {
		return fmt.Errorf("cannot restore: live object %q already exists in bucket %q", key, bucket)
	}

	// Find the most recent recycled entry for this key
	rbDir := s.recyclebinPath(bucket)
	entries, err := os.ReadDir(rbDir)
	if err != nil {
		return ErrObjectNotFound
	}

	var bestEntry *RecycledEntry
	var bestPath string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(rbDir, e.Name()))
		if err != nil {
			continue
		}

		var entry RecycledEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}

		if entry.OriginalKey != key {
			continue
		}

		if bestEntry == nil || entry.DeletedAt.After(bestEntry.DeletedAt) {
			entryCopy := entry
			bestEntry = &entryCopy
			bestPath = filepath.Join(rbDir, e.Name())
		}
	}

	if bestEntry == nil {
		return ErrObjectNotFound
	}

	// Restore metadata to live path
	metaPath := s.objectMetaPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
		return fmt.Errorf("create meta dir: %w", err)
	}

	metaData, err := json.MarshalIndent(bestEntry.Meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal restored meta: %w", err)
	}

	if err := syncedWriteFile(metaPath, metaData, 0644); err != nil {
		return fmt.Errorf("write restored meta: %w", err)
	}

	// Remove recycle bin entry. If this fails, the entry is orphaned but harmless —
	// the live object takes precedence, and the entry will be purged by retention.
	entryRemoved := true
	if err := removeWithRetry(bestPath); err != nil {
		entryRemoved = false
		s.logger.Warn().Err(err).Str("path", bestPath).
			Msg("failed to remove recyclebin entry after restore; entry will be purged by retention")
	}

	// Update bucket size
	if bestEntry.Meta.Size > 0 {
		_ = s.updateBucketSize(bucket, bestEntry.Meta.Size)
	}

	// Restore quota
	if s.quota != nil && bestEntry.Meta.Size > 0 {
		s.quota.Allocate(bucket, bestEntry.Meta.Size)
	}

	// Update stats
	s.statsObjectCount.Add(1)
	if bestEntry.Meta.Size > 0 {
		s.statsLogicalBytes.Add(bestEntry.Meta.Size)
	}
	// Only decrement recycled bytes if the entry was actually removed,
	// otherwise stats drift until the next initCASStats on restart.
	if entryRemoved {
		s.statsRecycledBytes.Add(-bestEntry.Meta.Size)
	}

	return nil
}

// GetRecycledObject retrieves the content of the most recently recycled entry for a key.
func (s *Store) GetRecycledObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return nil, nil, err
	}

	// Find the most recent recycled entry for this key
	rbDir := s.recyclebinPath(bucket)
	entries, err := os.ReadDir(rbDir)
	if err != nil {
		return nil, nil, ErrObjectNotFound
	}

	var bestEntry *RecycledEntry
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(rbDir, e.Name()))
		if err != nil {
			continue
		}

		var entry RecycledEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}

		if entry.OriginalKey != key {
			continue
		}

		if bestEntry == nil || entry.DeletedAt.After(bestEntry.DeletedAt) {
			entryCopy := entry
			bestEntry = &entryCopy
		}
	}

	if bestEntry == nil {
		return nil, nil, ErrObjectNotFound
	}

	return s.getObjectContent(ctx, bucket, key, &bestEntry.Meta)
}

// ListRecycledObjects returns all recycled entries for a bucket.
func (s *Store) ListRecycledObjects(ctx context.Context, bucket string) ([]RecycledEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return nil, err
	}

	rbDir := s.recyclebinPath(bucket)
	entries, err := os.ReadDir(rbDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read recyclebin dir: %w", err)
	}

	var result []RecycledEntry
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(rbDir, e.Name()))
		if err != nil {
			continue
		}

		var entry RecycledEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}

		result = append(result, entry)
	}

	return result, nil
}

// PurgeObject permanently removes an object and all its versions from a bucket.
// This is used by the cleanup process for recycled objects past retention.
// Uses a two-phase approach: collect metadata and remove files under lock,
// then check and delete chunks outside the lock to avoid holding the mutex
// during expensive global filesystem scans.
func (s *Store) PurgeObject(ctx context.Context, bucket, key string) error {
	// Phase 1: Hold lock briefly to collect chunk info and remove metadata
	s.mu.Lock()

	// Check bucket exists — if bucket is missing, the object can't exist either
	if _, err := s.getBucketMeta(bucket); err != nil {
		s.mu.Unlock()
		if errors.Is(err, ErrBucketNotFound) {
			return nil // Idempotent: bucket doesn't exist, nothing to purge
		}
		return err
	}

	// Get current object metadata for quota release and chunk cleanup
	meta, err := s.getObjectMeta(bucket, key)
	if err != nil {
		s.mu.Unlock()
		if errors.Is(err, ErrObjectNotFound) {
			return nil // Idempotent: object doesn't exist, nothing to purge
		}
		return err
	}

	// Collect version chunks and remove version files
	versionChunks := s.collectAndDeleteAllVersions(bucket, key)

	// Preallocate for current + version chunks
	chunksToCheck := make([]string, 0, len(meta.Chunks)+len(versionChunks))
	chunksToCheck = append(chunksToCheck, meta.Chunks...)
	chunksToCheck = append(chunksToCheck, versionChunks...)

	// Remove current metadata.
	// On Windows, file handles may be transiently held by OS processes
	// (antivirus, search indexer), causing Remove to fail. Retry briefly.
	metaPath := s.objectMetaPath(bucket, key)
	retries := 1
	if runtime.GOOS == "windows" {
		retries = windowsFileRetries
	}
	for i := 0; i < retries; i++ {
		if removeErr := os.Remove(metaPath); removeErr == nil || os.IsNotExist(removeErr) {
			break
		} else if i < retries-1 {
			s.logger.Warn().Err(removeErr).
				Str("path", metaPath).
				Int("attempt", i+1).
				Msg("Failed to remove object metadata, retrying")
			time.Sleep(windowsFileRetryDelay)
		}
	}

	// Release quota
	if s.quota != nil && meta.Size > 0 {
		s.quota.Release(bucket, meta.Size)
	}

	// Decrement bucket size (object is actually being removed now)
	if meta.Size > 0 {
		_ = s.updateBucketSize(bucket, -meta.Size)
	}

	// Decrement object count and logical bytes
	s.statsObjectCount.Add(-1)
	if meta.Size > 0 {
		s.statsLogicalBytes.Add(-meta.Size)
	}

	// Notify listing index before releasing the lock (no deadlock: callback uses atomic CAS, not s.mu)
	callback := s.onObjectRemovedCallback
	if callback != nil {
		callback(bucket, key)
	}

	s.mu.Unlock()

	// Phase 2: Delete unreferenced chunks WITHOUT holding the lock.
	// This is best-effort: the object is already purged (metadata removed in Phase 1).
	// If context is cancelled, orphaned chunks will be cleaned by the next GC cycle.
	s.DeleteUnreferencedChunks(ctx, chunksToCheck)

	return nil
}

// SetRecycleBinRetentionHours sets the number of hours to retain recycled objects before purging.
// Fractional values are supported (e.g. 0.167 ≈ 10 minutes). 0 disables purging.
func (s *Store) SetRecycleBinRetentionHours(hours float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recyclebinRetentionHours = hours
}

// RecycleBinRetentionHours returns the configured recycle bin retention period in hours.
func (s *Store) RecycleBinRetentionHours() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.recyclebinRetentionHours
}

// PurgeRecycleBin removes all recycled objects older than the retention period.
// For each: delete recyclebin entry, delete versions, delete unreferenced chunks.
// Returns the number of entries purged.
func (s *Store) PurgeRecycleBin(ctx context.Context) int {
	s.mu.RLock()
	retentionHours := s.recyclebinRetentionHours
	s.mu.RUnlock()

	if retentionHours <= 0 {
		return 0 // Disabled
	}

	cutoff := time.Now().UTC().Add(-time.Duration(retentionHours * float64(time.Hour)))
	return s.purgeRecycledEntries(ctx, &cutoff)
}

// PurgeAllRecycled removes all recycled objects regardless of retention period.
// Returns the number of entries purged.
func (s *Store) PurgeAllRecycled(ctx context.Context) int {
	return s.purgeRecycledEntries(ctx, nil)
}

// PurgeAllRecycledInBucket removes all recycle bin entries for a specific bucket.
// Used when hard-deleting a file share bucket or via the per-bucket purge API.
func (s *Store) PurgeAllRecycledInBucket(ctx context.Context, bucket string) error {
	// Phase 1: Under lock — remove entries and collect chunk hashes for deferred cleanup.
	var allChunksToCheck []string

	s.mu.Lock()
	callback := s.onObjectRemovedCallback

	rbDir := s.recyclebinPath(bucket)
	entries, err := os.ReadDir(rbDir)
	if err != nil {
		s.mu.Unlock()
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read recyclebin: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}

		entryPath := filepath.Join(rbDir, e.Name())
		data, err := os.ReadFile(entryPath)
		if err != nil {
			continue
		}

		var entry RecycledEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}

		// Collect chunks for batch cleanup after releasing lock
		allChunksToCheck = append(allChunksToCheck, entry.Meta.Chunks...)

		// Delete version files
		versionDir := filepath.Join(s.dataDir, "buckets", bucket, "versions", entry.OriginalKey)
		if err := os.RemoveAll(versionDir); err != nil && !os.IsNotExist(err) {
			s.logger.Warn().Err(err).Str("dir", versionDir).Msg("failed to remove version dir during recyclebin purge")
		}

		// Remove the entry (retry on Windows where file handles may be held)
		if err := removeWithRetry(entryPath); err != nil {
			continue
		}

		// Notify listing index (safe: already under s.mu lock)
		if callback != nil {
			callback(bucket, entry.OriginalKey)
		}

		s.statsRecycledBytes.Add(-entry.Meta.Size)
	}

	s.mu.Unlock()

	// Phase 2: Outside lock — single batch cleanup (one reference set build).
	if len(allChunksToCheck) > 0 {
		s.DeleteUnreferencedChunks(ctx, allChunksToCheck)
	}

	return nil
}

// purgeRecycledEntries scans all buckets' recyclebin dirs and purges entries.
// If cutoff is non-nil, only entries older than cutoff are purged.
func (s *Store) purgeRecycledEntries(ctx context.Context, cutoff *time.Time) int {
	purgedCount := 0
	var allChunksToCheck []string

	// Read callback under lock to avoid data race
	s.mu.RLock()
	callback := s.onObjectRemovedCallback
	s.mu.RUnlock()

	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		return 0
	}

	// Phase 1: Remove entries and collect chunk hashes for batch cleanup.
	for _, bucket := range buckets {
		if ctx.Err() != nil {
			break
		}

		rbDir := s.recyclebinPath(bucket.Name)
		entries, err := os.ReadDir(rbDir)
		if err != nil {
			continue
		}

		for _, e := range entries {
			if ctx.Err() != nil {
				break
			}

			if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
				continue
			}

			entryPath := filepath.Join(rbDir, e.Name())
			data, err := os.ReadFile(entryPath)
			if err != nil {
				continue
			}

			var entry RecycledEntry
			if err := json.Unmarshal(data, &entry); err != nil {
				continue
			}

			// Check retention cutoff
			if cutoff != nil && !entry.DeletedAt.Before(*cutoff) {
				continue
			}

			// Collect chunks for batch cleanup
			allChunksToCheck = append(allChunksToCheck, entry.Meta.Chunks...)

			// Delete version files for this key
			versionDir := filepath.Join(s.dataDir, "buckets", bucket.Name, "versions", entry.OriginalKey)
			if err := os.RemoveAll(versionDir); err != nil && !os.IsNotExist(err) {
				s.logger.Warn().Err(err).Str("dir", versionDir).Msg("failed to remove version dir during recyclebin purge")
			}

			// Remove the recyclebin entry (retry on Windows where file handles may be held)
			if err := removeWithRetry(entryPath); err != nil {
				continue
			}

			// Notify listing index of permanent removal
			if callback != nil {
				callback(bucket.Name, entry.OriginalKey)
			}

			purgedCount++
			s.statsRecycledBytes.Add(-entry.Meta.Size)
		}
	}

	// Phase 2: Single batch cleanup (one reference set build for all purged entries).
	if len(allChunksToCheck) > 0 {
		s.DeleteUnreferencedChunks(ctx, allChunksToCheck)
	}

	return purgedCount
}

// ==== Phase 4: Chunk-Level Replication Interface Methods ====

// GetObjectMeta retrieves object metadata without loading chunk data.
// This is used by the replicator to determine which chunks to send.
func (s *Store) GetObjectMeta(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	// Validate names
	if err := validateName(bucket); err != nil {
		return nil, fmt.Errorf("invalid bucket name: %w", err)
	}
	if err := validateName(key); err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return nil, err
	}

	// Get object metadata (includes chunk list and chunk metadata)
	meta, err := s.getObjectMeta(bucket, key)
	if err != nil {
		return nil, err
	}

	// Return copy to prevent external modification
	metaCopy := *meta
	return &metaCopy, nil
}

// ReadChunk reads a chunk from CAS by its hash.
// This is used by the replicator to fetch chunk data for sending to peers.
// ChunkExists checks if a chunk exists in CAS without reading its data.
func (s *Store) ChunkExists(hash string) bool {
	if s.cas == nil {
		return false
	}
	return s.cas.ChunkExists(hash)
}

func (s *Store) ReadChunk(ctx context.Context, hash string) ([]byte, error) {
	if s.cas == nil {
		return nil, fmt.Errorf("CAS not initialized")
	}

	return s.cas.ReadChunk(ctx, hash)
}

// ReadChunkRaw reads the raw encrypted+compressed bytes for a chunk without
// decrypting or decompressing. Used by the replication sender.
func (s *Store) ReadChunkRaw(ctx context.Context, hash string) ([]byte, error) {
	if s.cas == nil {
		return nil, fmt.Errorf("CAS not initialized")
	}

	return s.cas.ReadChunkRaw(ctx, hash)
}

// WriteChunkDirectRaw writes pre-encrypted+compressed chunk bytes directly to CAS.
// Used by the replication receiver — skips the decrypt+compress+encrypt cycle.
// Integrity is deferred to read-time: the MAC check in ReadChunk will reject any
// bytes that were encrypted with a different key, and self-heals by deleting the
// chunk so callers can re-fetch from a peer with the correct plaintext.
func (s *Store) WriteChunkDirectRaw(ctx context.Context, hash string, raw []byte) error {
	if s.cas == nil {
		return fmt.Errorf("CAS not initialized")
	}

	onDiskBytes, err := s.cas.WriteChunkRaw(ctx, hash, raw)
	if err != nil {
		return fmt.Errorf("write raw chunk to CAS: %w", err)
	}

	if onDiskBytes > 0 {
		s.statsChunkCount.Add(1)
		s.statsChunkBytes.Add(onDiskBytes)
	}

	// Register chunk in distributed registry so GC can track ownership.
	if s.chunkRegistry != nil {
		_ = s.chunkRegistry.RegisterChunk(hash, onDiskBytes)
	}

	return nil
}

// WriteChunkDirect writes chunk data directly to CAS.
// This is used by the replication receiver to store incoming chunks.
// Unlike WriteChunk via PutObject, this doesn't create object metadata.
func (s *Store) WriteChunkDirect(ctx context.Context, hash string, data []byte) error {
	if s.cas == nil {
		return fmt.Errorf("CAS not initialized")
	}

	// Validate hash for security - don't trust sender
	computedHash := ContentHash(data)
	if computedHash != hash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", hash, computedHash)
	}

	// CAS writeChunkKnown is idempotent - if chunk exists, it returns immediately.
	// hash is already validated above, so pass it to skip the redundant SHA-256.
	onDiskBytes, err := s.cas.writeChunkKnown(ctx, data, hash)
	if err != nil {
		return fmt.Errorf("write chunk to CAS: %w", err)
	}

	// Update incremental chunk stats for new chunks
	if onDiskBytes > 0 {
		s.statsChunkCount.Add(1)
		s.statsChunkBytes.Add(onDiskBytes)
	}

	// Register chunk in distributed registry so GC can track ownership symmetrically.
	// Without this, GC on replica coordinators can't confirm chunk ownership and
	// skips deletion, causing storage to grow unbounded on replicas.
	if s.chunkRegistry != nil {
		_ = s.chunkRegistry.RegisterChunk(hash, int64(len(data)))
	}

	return nil
}

// ImportObjectMeta writes object metadata directly without processing chunks.
// This is used by the replication receiver to create the metadata file so the
// remote coordinator can serve reads for objects whose chunks arrive separately.
// bucketOwner is used when auto-creating the bucket (empty string defaults to "system").
//
// Lock hold time is minimized: validation and directory creation happen before
// the lock, pruneExpiredVersions runs after the lock is released, and all writes
// use atomicWriteFile (no fsync) since replicated metadata is recoverable via
// auto-sync from source coordinators.
func (s *Store) ImportObjectMeta(ctx context.Context, bucket, key string, metaJSON []byte, bucketOwner string) ([]string, error) {
	// === Phase 1: Before lock — validation, unmarshal, directory creation ===

	if err := validateName(bucket); err != nil {
		return nil, fmt.Errorf("invalid bucket name: %w", err)
	}
	if err := validateName(key); err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	var meta ObjectMeta
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		return nil, fmt.Errorf("invalid object meta JSON: %w", err)
	}

	// Ensure LastModified is set — legacy or test metadata may omit it.
	// Re-marshal so the stored JSON always has a valid timestamp.
	if meta.LastModified.IsZero() {
		meta.LastModified = time.Now().UTC()
		var marshalErr error
		metaJSON, marshalErr = json.Marshal(meta)
		if marshalErr != nil {
			return nil, fmt.Errorf("re-marshal meta with timestamp: %w", marshalErr)
		}
	}

	// Pre-create directories outside the lock (MkdirAll is idempotent)
	bucketDir := s.bucketPath(bucket)
	metaDir := filepath.Join(bucketDir, "meta")
	if mkErr := os.MkdirAll(metaDir, 0755); mkErr != nil {
		return nil, fmt.Errorf("create bucket/meta directories: %w", mkErr)
	}
	metaPath := s.objectMetaPath(bucket, key)
	if mkErr := os.MkdirAll(filepath.Dir(metaPath), 0755); mkErr != nil {
		return nil, fmt.Errorf("create meta parent directory: %w", mkErr)
	}

	// === Phase 2: Under lock — reads, archive, writes (all atomicWriteFile) ===

	s.mu.Lock()

	// Ensure bucket exists — create bucket meta if needed
	bucketMeta, err := s.getBucketMeta(bucket)
	if err != nil {
		if bucketOwner == "" {
			bucketOwner = "system"
		}
		bucketMeta = &BucketMeta{
			Name:              bucket,
			CreatedAt:         time.Now(),
			Owner:             bucketOwner,
			ReplicationFactor: 2, // Auto-created during replication → multi-coordinator setup
		}
		bmData, marshalErr := json.Marshal(bucketMeta)
		if marshalErr != nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("marshal bucket meta: %w", marshalErr)
		}
		if writeErr := atomicWriteFile(s.bucketMetaPath(bucket), bmData, 0644); writeErr != nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("write bucket meta: %w", writeErr)
		}
	}

	// Check if object already exists (for idempotent retries)
	var oldLogicalBytes int64
	isNewObject := true
	if oldMeta, err := s.getObjectMeta(bucket, key); err == nil {
		bucketMeta.SizeBytes -= oldMeta.Size
		isNewObject = false
		oldLogicalBytes = oldMeta.Size

		// Skip re-import if content is identical (same ETag).
		// Auto-sync re-imports all objects every 5 min; without this,
		// each re-import archives the current version, creating useless
		// version files that GC prunes, deleting associated chunks.
		if meta.ETag != "" && oldMeta.ETag == meta.ETag {
			s.mu.Unlock()
			return nil, nil
		}
	}

	// Clear stale version files from a previous object lifecycle at this key.
	if isNewObject {
		s.clearVersionDir(bucket, key)
	}

	// Archive current version using atomicWriteFile (no fsync) to minimize lock hold time
	s.archiveCurrentVersionAtomic(bucket, key)

	// Write the object metadata
	if writeErr := atomicWriteFile(metaPath, metaJSON, 0644); writeErr != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("write object meta: %w", writeErr)
	}

	// Update incremental stats (atomics, but done under lock for consistency with bucket meta)
	newLogicalBytes := meta.Size
	if isNewObject {
		s.statsObjectCount.Add(1)
	}
	s.statsLogicalBytes.Add(newLogicalBytes - oldLogicalBytes)

	// Update bucket size tracking
	bucketMeta.SizeBytes += meta.Size
	bmData, marshalErr := json.Marshal(bucketMeta)
	if marshalErr != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("marshal bucket meta: %w", marshalErr)
	}
	if writeErr := atomicWriteFile(s.bucketMetaPath(bucket), bmData, 0644); writeErr != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("update bucket meta: %w", writeErr)
	}

	s.mu.Unlock()

	// === Phase 3: After lock — pruning (filesystem walk, safe without lock) ===
	// Version files have unique names, os.Remove is atomic/idempotent,
	// and stats use atomics, so this is safe outside the lock.
	_, chunksToCheck := s.pruneExpiredVersions(ctx, bucket, key)

	return chunksToCheck, nil
}

// DeleteUnreferencedChunks checks each chunk hash against all objects, versions,
// and recyclebin entries, deleting any that are no longer referenced.
// Returns total bytes freed.
//
// Uses buildChunkReferenceSet to build the reference set ONCE (O(N) filesystem
// walk where N = total metadata files), then checks each chunk in O(1).
// This replaces the old per-chunk isChunkReferencedGlobally which was O(N*K)
// where K = len(chunkHashes) — causing CPU/memory spikes with many chunks.
func (s *Store) DeleteUnreferencedChunks(ctx context.Context, chunkHashes []string) int64 {
	if s.cas == nil || len(chunkHashes) == 0 {
		return 0
	}

	// Build reference set once for all chunks (single filesystem walk).
	//
	// Chunks written during replication arrive before their metadata is committed.
	// Apply GCGracePeriod to skip any chunk younger than 10 minutes — the same
	// protection that RunGarbageCollection uses — so newly-replicated EC pieces
	// cannot be deleted before their object metadata is saved.
	referencedChunks := s.buildChunkReferenceSet(ctx)

	var totalFreed int64
	for _, hash := range chunkHashes {
		select {
		case <-ctx.Done():
			return totalFreed
		default:
		}
		if _, referenced := referencedChunks[hash]; !referenced {
			// Grace period: skip chunks that were written recently.
			// Newly-received replication chunks have no metadata yet; deleting
			// them before metadata arrives makes the object permanently unreadable.
			if mtime, err := s.cas.ChunkModTime(hash); err == nil {
				if time.Since(mtime) < GCGracePeriod {
					continue
				}
			}
			if freed, err := s.cas.DeleteChunk(ctx, hash); err == nil && freed > 0 {
				s.statsChunkCount.Add(-1)
				s.statsChunkBytes.Add(-freed)
				totalFreed += freed
				if s.chunkRegistry != nil {
					if err := s.chunkRegistry.UnregisterChunk(hash); err != nil {
						s.logger.Warn().Str("hash", hash[:8]).Err(err).
							Msg("failed to unregister chunk from registry after deletion")
					}
				}
			}
		}
	}
	return totalFreed
}

// DeleteChunk removes a chunk from CAS by its hash.
// This is used by the replicator to clean up non-assigned chunks after replication.
func (s *Store) DeleteChunk(ctx context.Context, hash string) error {
	if s.cas == nil {
		return fmt.Errorf("CAS not initialized")
	}

	freed, err := s.cas.DeleteChunk(ctx, hash)
	if err != nil {
		return err
	}
	if freed > 0 {
		s.statsChunkCount.Add(-1)
		s.statsChunkBytes.Add(-freed)
	}
	return nil
}

// ListObjects lists objects in a bucket with optional prefix filter and pagination.
// marker is the key to start after (exclusive) for pagination.
// Returns (objects, isTruncated, nextMarker, error).
func (s *Store) ListObjects(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]ObjectMeta, bool, string, error) {
	// Hold RLock for the entire walk so concurrent deletes cannot call os.Remove
	// on files that walkObjectMeta has open via os.ReadFile. On Windows,
	// os.ReadFile opens files without FILE_SHARE_DELETE, causing os.Remove to
	// fail with "being used by another process" if the file is concurrently open.
	// Multiple concurrent ListObjects calls still run in parallel (RLock is shared).
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, err := s.getBucketMeta(bucket); err != nil {
		return nil, false, "", err
	}
	metaDir := filepath.Join(s.bucketPath(bucket), "meta")
	return s.walkObjectMeta(metaDir, prefix, marker, maxKeys)
}

// listObjectsUnsafe lists objects without lock (caller must hold lock).
// Returns (objects, isTruncated, nextMarker, error).
func (s *Store) listObjectsUnsafe(bucket, prefix, marker string, maxKeys int) ([]ObjectMeta, bool, string, error) {
	metaDir := filepath.Join(s.bucketPath(bucket), "meta")
	return s.walkObjectMeta(metaDir, prefix, marker, maxKeys)
}

// ScanObjectsForReconcile walks all object metadata for a bucket without holding
// any mutex. Intended exclusively for reconcileLocalIndex, which tolerates
// concurrent-delete gaps (ENOENT from ReadFile is silently skipped; the CAS
// merge loop fills in any entries added by incremental updates during the scan).
// Must not be used for external S3 API responses where lock-safe consistency matters.
func (s *Store) ScanObjectsForReconcile(ctx context.Context, bucket string) ([]ObjectMeta, error) {
	metaDir := filepath.Join(s.bucketPath(bucket), "meta")
	objs, _, _, err := s.walkObjectMeta(metaDir, "", "", 0)
	return objs, err
}

// ScanRecycledForReconcile reads recycled entries for a bucket without holding
// any mutex. Intended exclusively for reconcileLocalIndex — see ScanObjectsForReconcile.
func (s *Store) ScanRecycledForReconcile(ctx context.Context, bucket string) ([]RecycledEntry, error) {
	rbDir := s.recyclebinPath(bucket)
	entries, err := os.ReadDir(rbDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read recyclebin dir: %w", err)
	}

	var result []RecycledEntry
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(rbDir, e.Name()))
		if err != nil {
			continue
		}
		var entry RecycledEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}
		result = append(result, entry)
	}
	return result, nil
}

// walkObjectMeta walks the metadata directory and returns objects matching
// the prefix/marker/maxKeys filters. Caller must hold at least s.mu.RLock —
// os.ReadFile opens files without FILE_SHARE_DELETE on Windows, so a concurrent
// DeleteObject holding the write lock would fail to os.Remove a file that this
// walk has open. Atomic temp+rename writes remain safe under RLock.
func (s *Store) walkObjectMeta(metaDir, prefix, marker string, maxKeys int) ([]ObjectMeta, bool, string, error) {
	var objects []ObjectMeta
	passedMarker := marker == "" // If no marker, we've already passed it

	err := filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Only process .json files
		if filepath.Ext(path) != ".json" {
			return nil
		}

		// Get key from path (remove .json suffix and metaDir prefix)
		relPath, err := filepath.Rel(metaDir, path)
		if err != nil {
			return nil
		}
		key := relPath[:len(relPath)-5] // Remove .json

		// Normalize path separators to forward slashes (S3 uses forward slashes)
		key = filepath.ToSlash(key)

		// Apply prefix filter
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		// Skip until we pass the marker
		if !passedMarker {
			if key == marker {
				passedMarker = true
			}
			return nil
		}

		// Read metadata
		data, err := os.ReadFile(path)
		if err != nil {
			// Silently skip ENOENT: expected when called without a lock (reconcile path)
			// if a concurrent DeleteObject removes the file between Walk and ReadFile.
			if !os.IsNotExist(err) {
				s.logger.Warn().Err(err).Str("path", path).Msg("ListObjects: failed to read metadata file")
			}
			return nil
		}

		var meta ObjectMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			s.logger.Warn().Err(err).Str("path", path).Msg("ListObjects: corrupted metadata file")
			return nil
		}

		objects = append(objects, meta)

		// Collect maxKeys + 1 to detect truncation
		if maxKeys > 0 && len(objects) > maxKeys {
			return filepath.SkipAll
		}

		return nil
	})

	if err != nil {
		return nil, false, "", fmt.Errorf("walk meta dir: %w", err)
	}

	// Check if results are truncated (only if maxKeys > 0)
	var isTruncated bool
	var nextMarker string
	if maxKeys > 0 && len(objects) > maxKeys {
		isTruncated = true
		objects = objects[:maxKeys] // Trim to maxKeys
		nextMarker = objects[maxKeys-1].Key
	}

	return objects, isTruncated, nextMarker, nil
}

// InitCAS initializes the content-addressable storage for the store.
// masterKey should be derived from the mesh PSK for consistent encryption across coordinators.
func (s *Store) InitCAS(ctx context.Context, masterKey [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	chunksDir := filepath.Join(s.dataDir, "chunks")
	cas, err := NewCAS(chunksDir, masterKey)
	if err != nil {
		return fmt.Errorf("init CAS: %w", err)
	}
	s.cas = cas

	// Initialize incremental stats from filesystem (one-time walk at startup)
	s.initCASStats()

	return nil
}

// SetVersionRetentionDays sets the number of days to retain object versions.
func (s *Store) SetVersionRetentionDays(days int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versionRetentionDays = days
}

// VersionRetentionDays returns the configured version retention period in days.
func (s *Store) VersionRetentionDays() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versionRetentionDays
}

// SetMaxVersionsPerObject sets the maximum number of versions to keep per object.
func (s *Store) SetMaxVersionsPerObject(max int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxVersionsPerObject = max
}

// SetVersionRetentionPolicy sets the smart tiered version retention policy.
func (s *Store) SetVersionRetentionPolicy(policy VersionRetentionPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versionRetentionPolicy = policy
}

// generateVersionID creates a unique, sortable version ID.
// Format: {unixNano}-{random6hex}
// Uses crypto/rand for unpredictable random component.
func generateVersionID() string {
	var randomBytes [4]byte
	_, _ = cryptorand.Read(randomBytes[:])
	randomInt := binary.BigEndian.Uint32(randomBytes[:]) & 0xFFFFFF
	return fmt.Sprintf("%d-%06x", time.Now().UnixNano(), randomInt)
}

// timestampFromVersionID extracts the creation time encoded in a version ID.
// Version IDs have the format "{unixNano}-{random6hex}"; the nanosecond
// prefix is extracted and converted to a UTC time. Returns zero time if
// the version ID is empty or does not start with a valid integer.
func timestampFromVersionID(versionID string) time.Time {
	s := versionID
	if idx := strings.IndexByte(versionID, '-'); idx >= 0 {
		s = versionID[:idx]
	}
	ns, err := strconv.ParseInt(s, 10, 64)
	if err != nil || ns <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

// versionDir returns the path to an object's version directory.
func (s *Store) versionDir(bucket, key string) string {
	return filepath.Join(s.bucketPath(bucket), "versions", key)
}

// versionMetaPath returns the path to a version's metadata file.
func (s *Store) versionMetaPath(bucket, key, versionID string) string {
	return filepath.Join(s.versionDir(bucket, key), versionID+".json")
}

// archiveCurrentVersion copies the current object metadata to the versions directory.
// This is called before overwriting an object to preserve its history.
func (s *Store) archiveCurrentVersion(bucket, key string) error {
	meta, err := s.getObjectMeta(bucket, key)
	if errors.Is(err, ErrObjectNotFound) {
		return nil // No current version to archive
	}
	if err != nil {
		return err
	}

	// Generate version ID if not present (defensive - all new objects have IDs)
	versionID := meta.VersionID
	if versionID == "" {
		versionID = generateVersionID()
		meta.VersionID = versionID
	}

	// Create version directory
	versionDir := s.versionDir(bucket, key)
	if err := os.MkdirAll(versionDir, 0755); err != nil {
		return fmt.Errorf("create version dir: %w", err)
	}

	// Write version metadata
	versionPath := s.versionMetaPath(bucket, key, versionID)
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal version meta: %w", err)
	}

	if err := syncedWriteFile(versionPath, data, 0644); err != nil {
		return fmt.Errorf("write version meta: %w", err)
	}

	s.statsVersionCount.Add(1)
	s.statsVersionBytes.Add(meta.Size)

	return nil
}

// clearVersionDir removes all archived version files for an object.
// Used when a new object is created at a previously-used key to prevent stale
// versions from a deleted predecessor appearing in the version history.
// Caller must hold s.mu.Lock().
func (s *Store) clearVersionDir(bucket, key string) {
	versionDir := s.versionDir(bucket, key)
	entries, err := os.ReadDir(versionDir)
	if err != nil {
		return // Directory doesn't exist — nothing to clear
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(versionDir, entry.Name())
		// Decrement stats before removing
		if data, readErr := os.ReadFile(path); readErr == nil {
			var meta ObjectMeta
			if json.Unmarshal(data, &meta) == nil {
				s.statsVersionCount.Add(-1)
				s.statsVersionBytes.Add(-meta.Size)
			}
		}
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			s.logger.Warn().Err(removeErr).
				Str("bucket", bucket).Str("key", key).
				Str("file", entry.Name()).
				Msg("Failed to clear stale version file on new object creation")
		}
	}
}

// archiveCurrentVersionAtomic archives the current version using atomicWriteFile
// (no fsync) instead of syncedWriteFile. This is used under the global lock in
// PutObject and ImportObjectMeta to minimize lock hold time. Failures are logged
// as warnings rather than returned as errors, since version archival is best-effort.
// Caller must hold s.mu.Lock().
func (s *Store) archiveCurrentVersionAtomic(bucket, key string) {
	currentMeta, err := s.getObjectMeta(bucket, key)
	if err != nil {
		return // No current version to archive
	}

	versionID := currentMeta.VersionID
	if versionID == "" {
		versionID = generateVersionID()
		currentMeta.VersionID = versionID
	}

	versionDir := s.versionDir(bucket, key)
	if mkErr := os.MkdirAll(versionDir, 0755); mkErr != nil {
		s.logger.Warn().Err(mkErr).
			Str("bucket", bucket).Str("key", key).
			Msg("Failed to create version dir")
		return
	}

	versionPath := s.versionMetaPath(bucket, key, versionID)
	data, marshalErr := json.MarshalIndent(currentMeta, "", "  ")
	if marshalErr != nil {
		return
	}

	if wErr := atomicWriteFile(versionPath, data, 0644); wErr != nil {
		s.logger.Warn().Err(wErr).
			Str("bucket", bucket).Str("key", key).
			Msg("Failed to archive version")
		return
	}

	s.statsVersionCount.Add(1)
	s.statsVersionBytes.Add(currentMeta.Size)
}

// pruneExpiredVersions removes versions according to the retention policy.
// Smart tiered pruning (when configured):
//   - Keep all versions from last RecentDays
//   - Keep one version per week for WeeklyWeeks
//   - Keep one version per month for MonthlyMonths
//   - Delete everything older
//   - Enforce MaxVersionsPerObject limit
//
// If no smart pruning is configured (all values 0), falls back to simple
// versionRetentionDays cutoff.
//
// Returns the number of versions pruned.
// nolint:gocyclo // Version pruning has complex retention logic
func (s *Store) pruneExpiredVersions(ctx context.Context, bucket, key string) (int, []string) {
	versionDir := s.versionDir(bucket, key)
	entries, err := os.ReadDir(versionDir)
	if err != nil {
		return 0, nil
	}

	// Load all versions with metadata
	type versionEntry struct {
		path     string
		meta     ObjectMeta
		modified time.Time
	}
	var versions []versionEntry

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		versionPath := filepath.Join(versionDir, entry.Name())
		data, err := os.ReadFile(versionPath)
		if err != nil {
			continue
		}

		var meta ObjectMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}

		versions = append(versions, versionEntry{
			path:     versionPath,
			meta:     meta,
			modified: meta.LastModified,
		})
	}

	if len(versions) == 0 {
		return 0, nil
	}

	// Sort by modified time (newest first)
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].modified.After(versions[j].modified)
	})

	now := time.Now().UTC()
	policy := s.versionRetentionPolicy
	maxVersions := s.maxVersionsPerObject

	// Check if smart pruning is configured
	smartPruningEnabled := policy.RecentDays > 0 || policy.WeeklyWeeks > 0 || policy.MonthlyMonths > 0

	// Track which versions to keep
	keep := make(map[int]bool)

	if smartPruningEnabled {
		// Smart tiered pruning
		weekKept := make(map[int]bool)  // year*100 + week
		monthKept := make(map[int]bool) // year*100 + month

		// Calculate time boundaries (only if that tier is enabled)
		var recentCutoff, weeklyCutoff, monthlyCutoff time.Time
		if policy.RecentDays > 0 {
			recentCutoff = now.AddDate(0, 0, -policy.RecentDays)
		}
		if policy.WeeklyWeeks > 0 {
			weeklyCutoff = now.AddDate(0, 0, -policy.RecentDays-(policy.WeeklyWeeks*7))
		}
		if policy.MonthlyMonths > 0 {
			monthlyCutoff = now.AddDate(0, -policy.MonthlyMonths, -policy.RecentDays-(policy.WeeklyWeeks*7))
		}

		for i, v := range versions {
			// Recent period: keep all (if configured)
			if policy.RecentDays > 0 && v.modified.After(recentCutoff) {
				keep[i] = true
				continue
			}

			// Weekly period: keep one per week (if configured)
			if policy.WeeklyWeeks > 0 && v.modified.After(weeklyCutoff) {
				year, week := v.modified.ISOWeek()
				weekKey := year*100 + week
				if !weekKept[weekKey] {
					keep[i] = true
					weekKept[weekKey] = true
				}
				continue
			}

			// Monthly period: keep one per month (if configured)
			if policy.MonthlyMonths > 0 && v.modified.After(monthlyCutoff) {
				monthKey := v.modified.Year()*100 + int(v.modified.Month())
				if !monthKept[monthKey] {
					keep[i] = true
					monthKept[monthKey] = true
				}
				continue
			}

			// Older than all configured periods: check max versions
			if maxVersions > 0 && i < maxVersions {
				keep[i] = true
			}
		}

		// Enforce maxVersionsPerObject as hard cap even within recent windows.
		// Without this, rapid overwrites within RecentDays accumulate unbounded.
		if maxVersions > 0 {
			kept := 0
			for i := range versions {
				if keep[i] {
					kept++
					if kept > maxVersions {
						delete(keep, i)
					}
				}
			}
		}
	} else {
		// No smart pruning - use simple retention days only
		if s.versionRetentionDays > 0 {
			simpleCutoff := now.AddDate(0, 0, -s.versionRetentionDays)
			for i, v := range versions {
				if v.modified.After(simpleCutoff) || v.modified.Equal(simpleCutoff) {
					keep[i] = true
				} else if maxVersions > 0 && i < maxVersions {
					keep[i] = true
				}
			}
		} else {
			// No retention configured - keep all (but respect max versions)
			for i := range versions {
				if maxVersions <= 0 || i < maxVersions {
					keep[i] = true
				}
			}
		}
	}

	// Delete versions not in keep set, collect chunks for deferred GC
	prunedCount := 0
	chunksToCheck := make([]string, 0, len(versions))
	seen := make(map[string]struct{})
	for i, v := range versions {
		if keep[i] {
			continue
		}

		// Delete version metadata
		_ = os.Remove(v.path)
		prunedCount++
		s.statsVersionCount.Add(-1)
		s.statsVersionBytes.Add(-v.meta.Size)

		// Collect chunks for deferred GC (done outside the lock by caller)
		for _, hash := range v.meta.Chunks {
			if _, ok := seen[hash]; !ok {
				seen[hash] = struct{}{}
				chunksToCheck = append(chunksToCheck, hash)
			}
		}
	}

	return prunedCount, chunksToCheck
}

// isChunkReferencedGlobally checks if a chunk is referenced by ANY object in ANY bucket.
// This is the safe way to check before deleting a chunk to prevent data loss.
func (s *Store) isChunkReferencedGlobally(ctx context.Context, hash string) bool {
	bucketsDir := filepath.Join(s.dataDir, "buckets")
	bucketEntries, err := os.ReadDir(bucketsDir)
	if err != nil {
		return true // Assume referenced if we can't check
	}

	for _, bucketEntry := range bucketEntries {
		if !bucketEntry.IsDir() {
			continue
		}
		bucket := bucketEntry.Name()
		bucketDir := filepath.Join(bucketsDir, bucket)

		// Check live objects, versions, and recycle bin entries
		if dirContainsChunkRef(filepath.Join(bucketDir, "meta"), hash, extractChunksFromObjectMeta) {
			return true
		}
		if dirContainsChunkRef(filepath.Join(bucketDir, "versions"), hash, extractChunksFromObjectMeta) {
			return true
		}
		if dirContainsChunkRef(s.recyclebinPath(bucket), hash, extractChunksFromRecycledEntry) {
			return true
		}
	}

	return false
}

// dirContainsChunkRef walks a directory and checks if any JSON file references the given chunk hash.
// extractChunks parses a JSON file and returns chunk hashes it references.
// Returns true if found or on parse error (fail-safe: assume referenced).
func dirContainsChunkRef(dir, hash string, extractChunks func([]byte) ([]string, error)) bool {
	found := false
	parseError := false
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			parseError = true
			return filepath.SkipAll
		}
		chunks, extractErr := extractChunks(data)
		if extractErr != nil {
			parseError = true
			return filepath.SkipAll
		}
		for _, h := range chunks {
			if h == hash {
				found = true
				return filepath.SkipAll
			}
		}
		return nil
	})
	return found || parseError
}

// extractChunksFromObjectMeta parses ObjectMeta JSON and returns its chunk hashes.
func extractChunksFromObjectMeta(data []byte) ([]string, error) {
	var meta ObjectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return meta.Chunks, nil
}

// extractChunksFromRecycledEntry parses RecycledEntry JSON and returns its chunk hashes.
func extractChunksFromRecycledEntry(data []byte) ([]string, error) {
	var entry RecycledEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return entry.Meta.Chunks, nil
}

// CASStats holds statistics about content-addressed storage.
type CASStats struct {
	ChunkCount     int   // Total number of chunks
	ChunkBytes     int64 // Total bytes in chunks (after dedup, including EC parity shards)
	DataChunkBytes int64 // ChunkBytes adjusted for EC parity overhead (data shards only)
	LogicalBytes   int64 // Logical bytes (sum of live object sizes, before dedup)
	VersionBytes   int64 // Logical bytes in version files
	RecycledBytes  int64 // Logical bytes in recyclebin entries
	VersionCount   int   // Total number of versions
	ObjectCount    int   // Total number of current objects
}

// initCASStats populates the atomic stat counters from a one-time filesystem walk.
// Called once at startup in NewStoreWithCAS. After this, stats are maintained
// incrementally at each mutation point (PutObject, DeleteChunk, etc.).
func (s *Store) initCASStats() {
	if s.cas == nil {
		return
	}

	// Count chunks and their on-disk sizes (compressed+encrypted).
	chunksDir := filepath.Join(s.dataDir, "chunks")
	var chunkCount int64
	var chunkBytes int64
	_ = filepath.Walk(chunksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		// Skip orphaned temp files from interrupted CAS writes
		if strings.HasSuffix(info.Name(), ".tmp") {
			return nil
		}
		chunkCount++
		chunkBytes += info.Size()
		return nil
	})
	s.statsChunkCount.Store(chunkCount)
	s.statsChunkBytes.Store(chunkBytes)

	// Scan all buckets for objects and versions
	bucketsDir := filepath.Join(s.dataDir, "buckets")
	bucketEntries, err := os.ReadDir(bucketsDir)
	if err != nil {
		return
	}

	var objectCount int64
	var versionCount int64
	var logicalBytes int64
	var versionBytes int64
	var recycledBytes int64

	for _, bucketEntry := range bucketEntries {
		if !bucketEntry.IsDir() {
			continue
		}
		bucket := bucketEntry.Name()

		// Count current objects and their logical sizes.
		metaDir := filepath.Join(bucketsDir, bucket, "meta")
		_ = filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			objectCount++
			data, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			var meta ObjectMeta
			if json.Unmarshal(data, &meta) == nil {
				logicalBytes += meta.Size
			}
			return nil
		})

		// Count version files and their logical sizes
		versionsDir := filepath.Join(bucketsDir, bucket, "versions")
		_ = filepath.Walk(versionsDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			versionCount++
			data, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			var meta ObjectMeta
			if json.Unmarshal(data, &meta) == nil {
				versionBytes += meta.Size
			}
			return nil
		})

		// Count recyclebin entries and their logical sizes
		rbDir := s.recyclebinPath(bucket)
		_ = filepath.Walk(rbDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			var entry RecycledEntry
			if json.Unmarshal(data, &entry) == nil {
				recycledBytes += entry.Meta.Size
			}
			return nil
		})
	}

	s.statsObjectCount.Store(objectCount)
	s.statsVersionCount.Store(versionCount)
	s.statsLogicalBytes.Store(logicalBytes)
	s.statsVersionBytes.Store(versionBytes)
	s.statsRecycledBytes.Store(recycledBytes)
}

// GetCASStats returns statistics about content-addressed storage.
// Reads from atomic counters maintained incrementally at each mutation point.
func (s *Store) GetCASStats() CASStats {
	if s.cas == nil {
		return CASStats{}
	}

	chunkBytes := s.statsChunkBytes.Load()
	return CASStats{
		ChunkCount: int(s.statsChunkCount.Load()),
		ChunkBytes: chunkBytes,
		// Integer division is intentional: truncation error < 1 byte per 6 bytes,
		// negligible for any real-world chunk corpus.
		DataChunkBytes: chunkBytes * ecDataShards / (ecDataShards + ecParityShards),
		ObjectCount:    int(s.statsObjectCount.Load()),
		VersionCount:   int(s.statsVersionCount.Load()),
		LogicalBytes:   s.statsLogicalBytes.Load(),
		VersionBytes:   s.statsVersionBytes.Load(),
		RecycledBytes:  s.statsRecycledBytes.Load(),
	}
}

// GCStats holds statistics from a garbage collection run.
type GCStats struct {
	VersionsPruned           int   // Number of version metadata files deleted
	ChunksDeleted            int   // Number of orphaned chunks deleted
	BytesReclaimed           int64 // Approximate bytes reclaimed from chunk deletion
	ObjectsScanned           int   // Number of objects scanned
	BucketsProcessed         int   // Number of buckets processed
	ChunksSkippedGracePeriod int   // Chunks skipped due to grace period (Phase 6)
}

// RunGarbageCollection performs a full garbage collection pass.
// This should be called periodically (e.g., hourly) to clean up:
//   - Expired versions according to retention policy
//   - Orphaned chunks not referenced by any object
//
// Uses a phased approach to minimize lock contention:
//   - Phase 1: Prune expired versions (Lock per object, brief)
//   - Phase 2: Rebuild reference set after pruning (RLock only)
//   - Phase 3: Delete orphaned chunks (no Store lock, CAS has its own)
func (s *Store) RunGarbageCollection(ctx context.Context) GCStats {
	return s.runGC(ctx, false)
}

// RunGarbageCollectionForce performs a full GC pass, skipping the grace period.
// Use this for explicitly triggered (manual) GC where the admin wants immediate
// cleanup. The grace period is only needed for periodic GC to protect in-flight
// uploads and replication.
func (s *Store) RunGarbageCollectionForce(ctx context.Context) GCStats {
	return s.runGC(ctx, true)
}

func (s *Store) runGC(ctx context.Context, skipGracePeriod bool) GCStats {
	stats := GCStats{}

	if s.cas == nil {
		return stats // No CAS, no GC needed
	}

	// Record start time to avoid deleting chunks created during GC
	gcStartTime := time.Now()

	// Phase 1: Prune expired versions across all buckets.
	// Returns chunks from pruned versions that may now be unreferenced.
	chunksToCheck := s.pruneAllExpiredVersionsSimple(ctx, &stats)

	// Phase 2: Build reference set ONCE after pruning (lock-free filesystem scan).
	// This single scan serves both pruned-version chunk cleanup and orphan detection,
	// avoiding the previous double-scan that held RLock for extended periods.
	referencedChunks := s.buildChunkReferenceSet(ctx)

	// Phase 2b: Delete chunks from pruned versions that are no longer referenced
	for _, hash := range chunksToCheck {
		select {
		case <-ctx.Done():
			return stats
		default:
		}
		if _, ok := referencedChunks[hash]; !ok {
			if freed, err := s.cas.DeleteChunk(ctx, hash); err == nil && freed > 0 {
				s.statsChunkCount.Add(-1)
				s.statsChunkBytes.Add(-freed)
			}
		}
	}

	// Phase 3: Delete orphaned chunks (not in reference set)
	// CAS has its own locking; we don't need Store lock here
	s.deleteOrphanedChunks(ctx, &stats, referencedChunks, gcStartTime, skipGracePeriod)

	// Update quota if tracking
	// Note: calculateQuotaUsage acquires s.mu.Lock() internally,
	// so we must NOT hold the lock here.
	if s.quota != nil {
		_ = s.calculateQuotaUsage()
	}

	return stats
}

// buildChunkReferenceSet scans all objects and versions to find referenced chunks.
// No Store lock is needed because:
//   - s.dataDir is immutable after construction
//   - All file writes use syncedWriteFile (atomic temp+rename), so reads always
//     see complete content
//   - Chunks written during the scan are newer than gcStartTime and will be
//     skipped by deleteOrphanedChunks
func (s *Store) buildChunkReferenceSet(ctx context.Context) map[string]struct{} {
	// Pre-allocate using the live chunk count to avoid O(log N) rehash steps
	// during the reference set build. statsChunkCount is maintained atomically
	// at every write/delete, so it's an accurate hint with no extra I/O.
	hint := int(s.statsChunkCount.Load())
	if hint < 64 {
		hint = 64
	}
	referencedChunks := make(map[string]struct{}, hint)

	bucketsDir := filepath.Join(s.dataDir, "buckets")
	bucketEntries, err := os.ReadDir(bucketsDir)
	if err != nil {
		return referencedChunks
	}

	for _, bucketEntry := range bucketEntries {
		select {
		case <-ctx.Done():
			return referencedChunks
		default:
		}
		if !bucketEntry.IsDir() {
			continue
		}

		bucketStart := time.Now()
		bucket := bucketEntry.Name()

		// Scan current objects (walk to include subdirectories like meta/auth/, meta/dns/)
		metaDir := filepath.Join(bucketsDir, bucket, "meta")
		_ = filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil
			}
			var meta ObjectMeta
			if json.Unmarshal(data, &meta) == nil {
				for _, h := range meta.Chunks {
					referencedChunks[h] = struct{}{}
				}
			}
			return nil
		})

		// Scan versions
		versionsDir := filepath.Join(bucketsDir, bucket, "versions")
		_ = filepath.Walk(versionsDir, func(path string, info os.FileInfo, err error) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			data, _ := os.ReadFile(path)
			var meta ObjectMeta
			if json.Unmarshal(data, &meta) == nil {
				for _, h := range meta.Chunks {
					referencedChunks[h] = struct{}{}
				}
			}
			return nil
		})

		// Scan recycle bin entries
		recyclebinDir := s.recyclebinPath(bucket)
		_ = filepath.Walk(recyclebinDir, func(path string, info os.FileInfo, err error) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			data, _ := os.ReadFile(path)
			var entry RecycledEntry
			if json.Unmarshal(data, &entry) == nil {
				for _, h := range entry.Meta.Chunks {
					referencedChunks[h] = struct{}{}
				}
			}
			return nil
		})

		// Throttle to ~10% CPU duty cycle: sleep 9× the per-bucket scan time.
		// Fast buckets pause briefly; slow buckets pause proportionally.
		// GC completes well within the 5-minute interval even with throttling.
		// Skip under go test: the proportional sleep turns into hundreds of
		// seconds on slow Ubuntu CI runners (slow I/O → long elapsed → long sleep).
		if elapsed := time.Since(bucketStart); elapsed > 0 && !s.gcThrottleDisabled {
			select {
			case <-ctx.Done():
				return referencedChunks
			case <-time.After(elapsed * 9):
			}
		}
	}

	return referencedChunks
}

// GetActiveVersionIDs returns the set of all active version IDs across all buckets.
// It mirrors buildChunkReferenceSet, walking meta/, versions/, and recyclebin/ for
// each bucket. The recyclebin scan is required: soft-deleted objects still have their
// chunks on disk, so their EC shard registry entries must not be evicted prematurely.
func (s *Store) GetActiveVersionIDs(ctx context.Context) (map[string]bool, error) {
	activeVersionIDs := make(map[string]bool)

	bucketsDir := filepath.Join(s.dataDir, "buckets")
	bucketEntries, err := os.ReadDir(bucketsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return activeVersionIDs, nil
		}
		return nil, fmt.Errorf("read buckets dir: %w", err)
	}

	for _, bucketEntry := range bucketEntries {
		select {
		case <-ctx.Done():
			return activeVersionIDs, ctx.Err()
		default:
		}
		if !bucketEntry.IsDir() {
			continue
		}
		bucket := bucketEntry.Name()

		// Scan current objects (live versions under meta/) and archived versions.
		for _, dir := range []string{
			filepath.Join(bucketsDir, bucket, "meta"),
			filepath.Join(bucketsDir, bucket, "versions"),
		} {
			if walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
					return nil
				}
				data, readErr := os.ReadFile(path)
				if readErr != nil {
					s.logger.Debug().Err(readErr).Str("path", path).Msg("GetActiveVersionIDs: skipping unreadable object meta file")
					return nil
				}
				var meta ObjectMeta
				if json.Unmarshal(data, &meta) == nil && meta.VersionID != "" {
					activeVersionIDs[meta.VersionID] = true
				}
				return nil
			}); walkErr != nil {
				s.logger.Debug().Err(walkErr).Str("dir", dir).Msg("GetActiveVersionIDs: object meta walk error")
			}
		}

		// Scan recycle bin entries. Soft-deleted objects retain their chunks on disk
		// until the retention period expires, so their version IDs must be kept active
		// to prevent premature EC shard registry eviction.
		recyclebinDir := s.recyclebinPath(bucket)
		if walkErr := filepath.Walk(recyclebinDir, func(path string, info os.FileInfo, err error) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				s.logger.Debug().Err(readErr).Str("path", path).Msg("GetActiveVersionIDs: skipping unreadable recyclebin file")
				return nil
			}
			var entry RecycledEntry
			if json.Unmarshal(data, &entry) == nil && entry.Meta.VersionID != "" {
				activeVersionIDs[entry.Meta.VersionID] = true
			}
			return nil
		}); walkErr != nil {
			s.logger.Debug().Err(walkErr).Str("bucket", bucket).Msg("GetActiveVersionIDs: recyclebin walk error")
		}
	}

	return activeVersionIDs, nil
}

// pruneAllExpiredVersionsSimple prunes expired versions across all buckets.
// Takes brief read locks per object to minimize contention.
// Returns chunks from pruned versions that may now be unreferenced;
// the caller is responsible for building a reference set and deleting
// unreferenced chunks (avoiding duplicate filesystem scans).
func (s *Store) pruneAllExpiredVersionsSimple(ctx context.Context, stats *GCStats) []string {
	bucketsDir := filepath.Join(s.dataDir, "buckets")
	bucketEntries, err := os.ReadDir(bucketsDir)
	if err != nil {
		return nil
	}

	// Collect all chunks from pruned versions for deferred cleanup
	var allChunksToCheck []string

	for _, bucketEntry := range bucketEntries {
		select {
		case <-ctx.Done():
			return allChunksToCheck
		default:
		}
		if !bucketEntry.IsDir() {
			continue
		}
		bucket := bucketEntry.Name()
		stats.BucketsProcessed++

		// Find all objects in this bucket (walk to include subdirectories)
		metaDir := filepath.Join(bucketsDir, bucket, "meta")
		var objectKeys []string
		_ = filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			// Get key from path relative to metaDir (remove .json suffix)
			relPath, err := filepath.Rel(metaDir, path)
			if err != nil || len(relPath) <= 5 {
				return nil
			}
			key := relPath[:len(relPath)-5] // Remove .json
			objectKeys = append(objectKeys, key)
			return nil
		})

		for _, key := range objectKeys {
			select {
			case <-ctx.Done():
				return allChunksToCheck
			default:
			}
			stats.ObjectsScanned++

			// Brief RLock: pruneExpiredVersions only reads config fields
			// (versionRetentionPolicy, maxVersionsPerObject, versionRetentionDays)
			// and does filesystem ops on unique version file paths.
			s.mu.RLock()
			pruned, chunksToCheck := s.pruneExpiredVersions(ctx, bucket, key)
			s.mu.RUnlock()

			stats.VersionsPruned += pruned
			allChunksToCheck = append(allChunksToCheck, chunksToCheck...)
		}
	}

	return allChunksToCheck
}

// deleteOrphanedChunks deletes chunks not in the reference set.
// Uses CAS's own locking; doesn't need Store lock.
// Skips chunks created after gcStartTime to avoid races.
// When skipGracePeriod is true, the GCGracePeriod check is bypassed (for manual GC).
func (s *Store) deleteOrphanedChunks(ctx context.Context, stats *GCStats, referencedChunks map[string]struct{}, gcStartTime time.Time, skipGracePeriod bool) {
	chunksDir := filepath.Join(s.dataDir, "chunks")
	_ = filepath.Walk(chunksDir, func(path string, info os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return fmt.Errorf("delete orphaned chunks cancelled: %w", ctx.Err())
		default:
		}
		if err != nil || info.IsDir() {
			return nil
		}

		// Skip chunks created after GC started (race protection)
		if info.ModTime().After(gcStartTime) {
			return nil
		}

		// Grace period: Only delete chunks older than GCGracePeriod (Phase 6)
		// This prevents deleting chunks that are being replicated or uploaded.
		// Skipped for manual/forced GC where the admin wants immediate cleanup.
		//
		// See GCGracePeriod constant documentation for tuning guidance.
		if !skipGracePeriod && time.Since(info.ModTime()) < GCGracePeriod {
			stats.ChunksSkippedGracePeriod++
			return nil
		}

		// Extract hash from path (chunks are stored as chunks/ab/abcdef...)
		hash := filepath.Base(path)
		if _, referenced := referencedChunks[hash]; !referenced {
			// Chunk is not referenced by any local metadata (live objects,
			// versions, or recycle bin). The grace period above already
			// protects against races with in-flight replication. Safe to
			// delete locally regardless of registry state — each coordinator
			// independently manages its own storage.
			stats.BytesReclaimed += info.Size()

			// Unregister from chunk registry first (so other coordinators
			// stop considering us an owner).
			s.mu.RLock()
			registry := s.chunkRegistry
			s.mu.RUnlock()

			if registry != nil {
				_ = registry.UnregisterChunk(hash)
			}

			// CAS.DeleteChunk has its own locking
			if freed, delErr := s.cas.DeleteChunk(ctx, hash); delErr == nil {
				stats.ChunksDeleted++
				if freed > 0 {
					s.statsChunkCount.Add(-1)
					s.statsChunkBytes.Add(-freed)
				}
			}
		}
		return nil
	})
}

// ListVersions returns all versions of an object.
func (s *Store) ListVersions(ctx context.Context, bucket, key string) ([]VersionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return nil, err
	}

	var versions []VersionInfo

	// Get current version
	if meta, err := s.getObjectMeta(bucket, key); err == nil {
		lm := meta.LastModified
		if lm.IsZero() {
			lm = timestampFromVersionID(meta.VersionID)
		}
		versions = append(versions, VersionInfo{
			VersionID:    meta.VersionID,
			Size:         meta.Size,
			ETag:         meta.ETag,
			LastModified: lm,
			IsCurrent:    true,
		})
	}

	// Get archived versions
	versionDir := s.versionDir(bucket, key)
	entries, err := os.ReadDir(versionDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read version dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		versionPath := filepath.Join(versionDir, entry.Name())
		data, err := os.ReadFile(versionPath)
		if err != nil {
			continue
		}

		var meta ObjectMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}

		lm := meta.LastModified
		if lm.IsZero() {
			lm = timestampFromVersionID(meta.VersionID)
		}
		versions = append(versions, VersionInfo{
			VersionID:    meta.VersionID,
			Size:         meta.Size,
			ETag:         meta.ETag,
			LastModified: lm,
			IsCurrent:    false,
		})
	}

	// Sort by timestamp descending (newest first)
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].LastModified.After(versions[j].LastModified)
	})

	return versions, nil
}

// GetVersionHistory returns archived version entries for an object (version ID + full meta JSON).
// The current (live) version is not included; only entries from the versions/ directory.
func (s *Store) GetVersionHistory(ctx context.Context, bucket, key string) ([]VersionHistoryEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entries []VersionHistoryEntry

	// Get archived versions from versions directory
	versionDir := s.versionDir(bucket, key)
	dirEntries, err := os.ReadDir(versionDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read version dir: %w", err)
	}

	for _, entry := range dirEntries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		versionID := strings.TrimSuffix(entry.Name(), ".json")
		versionPath := filepath.Join(versionDir, entry.Name())
		data, readErr := os.ReadFile(versionPath)
		if readErr != nil {
			continue
		}

		entries = append(entries, VersionHistoryEntry{
			VersionID: versionID,
			MetaJSON:  data,
		})
	}

	return entries, nil
}

// VersionHistoryEntry represents a version entry with its raw metadata JSON.
type VersionHistoryEntry struct {
	VersionID string
	MetaJSON  []byte
}

// ImportVersionHistory imports version entries from replication, skipping any that
// already exist (dedup by versionID). After importing, prunes expired versions to
// enforce local retention policy. Returns count of newly imported versions and
// chunk hashes from pruned versions that may need garbage collection.
//
// Lock hold time is minimized: directory creation before lock, atomicWriteFile
// instead of syncedWriteFile, and pruneExpiredVersions after lock release.
func (s *Store) ImportVersionHistory(ctx context.Context, bucket, key string, versions []VersionHistoryEntry) (int, []string, error) {
	if len(versions) == 0 {
		return 0, nil, nil
	}

	// Pre-create version directory outside the lock (MkdirAll is idempotent)
	versionDir := s.versionDir(bucket, key)
	if err := os.MkdirAll(versionDir, 0755); err != nil {
		return 0, nil, fmt.Errorf("create version directory: %w", err)
	}

	s.mu.Lock()

	imported := 0
	for _, v := range versions {
		versionPath := filepath.Join(versionDir, v.VersionID+".json")

		// Skip if already exists (dedup by versionID)
		if _, err := os.Stat(versionPath); err == nil {
			continue
		}

		// Validate JSON before writing
		if !json.Valid(v.MetaJSON) {
			continue
		}

		if err := atomicWriteFile(versionPath, v.MetaJSON, 0644); err != nil {
			s.mu.Unlock()
			return imported, nil, fmt.Errorf("write version %s: %w", v.VersionID, err)
		}
		imported++

		// Update stats to match what archiveCurrentVersion does.
		s.statsVersionCount.Add(1)
		var meta ObjectMeta
		if json.Unmarshal(v.MetaJSON, &meta) == nil {
			s.statsVersionBytes.Add(meta.Size)
		}
	}

	s.mu.Unlock()

	// Prune expired versions after lock release — filesystem walk + deletes
	// are safe without lock (unique version filenames, atomic os.Remove, atomic stats).
	var chunksToCheck []string
	if imported > 0 {
		_, chunksToCheck = s.pruneExpiredVersions(ctx, bucket, key)
	}

	return imported, chunksToCheck, nil
}

// GetAllObjectKeys returns all object keys grouped by bucket.
// Used by the rebalancer to iterate all objects for redistribution.
// The lock is released between buckets to avoid holding it during long I/O scans.
func (s *Store) GetAllObjectKeys(ctx context.Context) (map[string][]string, error) {
	// First pass: get bucket names under lock
	s.mu.RLock()
	bucketsDir := filepath.Join(s.dataDir, "buckets")
	bucketEntries, err := os.ReadDir(bucketsDir)
	s.mu.RUnlock()
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read buckets dir: %w", err)
	}

	var bucketNames []string
	for _, bEntry := range bucketEntries {
		if bEntry.IsDir() {
			bucketNames = append(bucketNames, bEntry.Name())
		}
	}

	// Second pass: list objects per bucket, re-acquiring lock for each
	result := make(map[string][]string, len(bucketNames))
	for _, bucket := range bucketNames {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		s.mu.RLock()
		metaDir := filepath.Join(s.bucketPath(bucket), "meta")
		entries, readErr := os.ReadDir(metaDir)
		s.mu.RUnlock()
		if readErr != nil {
			if os.IsNotExist(readErr) {
				continue
			}
			return nil, fmt.Errorf("read meta dir for %s: %w", bucket, readErr)
		}

		var keys []string
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			keys = append(keys, strings.TrimSuffix(entry.Name(), ".json"))
		}
		if len(keys) > 0 {
			result[bucket] = keys
		}
	}

	return result, nil
}

// GetObjectVersion retrieves a specific version of an object.
func (s *Store) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (io.ReadCloser, *ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return nil, nil, err
	}

	// Check if it's the current version
	currentMeta, err := s.getObjectMeta(bucket, key)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return nil, nil, err
	}

	if currentMeta != nil && currentMeta.VersionID == versionID {
		// Return current version
		return s.getObjectContent(ctx, bucket, key, currentMeta)
	}

	// Look for archived version
	versionPath := s.versionMetaPath(bucket, key, versionID)
	data, err := os.ReadFile(versionPath)
	if os.IsNotExist(err) {
		return nil, nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read version meta: %w", err)
	}

	var meta ObjectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, nil, fmt.Errorf("unmarshal version meta: %w", err)
	}

	return s.getObjectContent(ctx, bucket, key, &meta)
}

// chunkReader implements io.ReadCloser for streaming chunk reads.
// It reads chunks on-demand from CAS, avoiding loading entire files into memory.
type chunkReader struct {
	// Note: Storing context here is necessary because io.Reader.Read() doesn't accept context.
	// This is a short-lived struct (per-request lifetime) used for streaming object content.
	// The context is checked on each Read() call to respect cancellation.
	ctx       context.Context
	cas       *CAS
	chunks    []string // Ordered list of chunk hashes
	chunkIdx  int      // Current chunk index
	chunkData []byte   // Current chunk data
	chunkPos  int      // Position within current chunk
}

// newChunkReader creates a streaming reader for the given chunks.
func newChunkReader(ctx context.Context, cas *CAS, chunks []string) *chunkReader {
	return &chunkReader{
		ctx:    ctx,
		cas:    cas,
		chunks: chunks,
	}
}

// Read implements io.Reader, fetching chunks on demand.
func (r *chunkReader) Read(p []byte) (n int, err error) {
	// Check for context cancellation before reading
	if err := r.ctx.Err(); err != nil {
		return 0, fmt.Errorf("read cancelled: %w", err)
	}

	for n < len(p) {
		// If we've exhausted current chunk, load the next one
		if r.chunkPos >= len(r.chunkData) {
			if r.chunkIdx >= len(r.chunks) {
				// No more chunks
				if n > 0 {
					return n, nil
				}
				return 0, io.EOF
			}

			// Check for cancellation before expensive I/O
			if err := r.ctx.Err(); err != nil {
				return n, fmt.Errorf("read cancelled: %w", err)
			}

			// Load next chunk
			r.chunkData, err = r.cas.ReadChunk(r.ctx, r.chunks[r.chunkIdx])
			if err != nil {
				return n, fmt.Errorf("read chunk %s: %w", r.chunks[r.chunkIdx], err)
			}
			r.chunkIdx++
			r.chunkPos = 0
		}

		// Copy from current chunk to output buffer
		copied := copy(p[n:], r.chunkData[r.chunkPos:])
		r.chunkPos += copied
		n += copied
	}
	return n, nil
}

// Close implements io.Closer.
func (r *chunkReader) Close() error {
	// Release chunk data for GC
	r.chunkData = nil
	return nil
}

// getObjectContent returns the content of an object by reading its chunks from CAS.
// Uses streaming to avoid loading entire files into memory.
func (s *Store) getObjectContent(ctx context.Context, bucket, key string, meta *ObjectMeta) (io.ReadCloser, *ObjectMeta, error) {
	if s.cas == nil {
		return nil, nil, fmt.Errorf("CAS not initialized")
	}

	if len(meta.Chunks) == 0 {
		// Empty file
		return io.NopCloser(bytes.NewReader(nil)), meta, nil
	}

	// Check if object uses erasure coding
	if meta.ErasureCoding != nil && meta.ErasureCoding.Enabled {
		return s.getObjectContentWithErasureCoding(ctx, bucket, key, meta)
	}

	// Use distributed chunk reader if replicator is configured (Phase 5)
	// This enables fetching missing chunks from remote coordinators
	// Note: Reading these pointers doesn't require locking as they're only
	// set once during initialization and never modified after that
	if s.replicator != nil && s.chunkRegistry != nil {
		// Distributed reads: can fetch chunks from remote peers
		reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
			Chunks:          meta.Chunks,
			LocalCAS:        s.cas,
			Registry:        s.chunkRegistry,
			Replicator:      s.replicator,
			Logger:          s.logger,
			TotalSize:       meta.Size,
			Prefetch:        PrefetchConfig{WindowSize: 8, Parallelism: 4},
			StatsChunkCount: &s.statsChunkCount,
			StatsChunkBytes: &s.statsChunkBytes,
		})
		return reader, meta, nil
	}

	// Local-only reads: all chunks must be local
	return newChunkReader(ctx, s.cas, meta.Chunks), meta, nil
}

// RestoreVersion makes a previous version the current version.
// This creates a new version (with new version ID) containing the old version's content.
func (s *Store) RestoreVersion(ctx context.Context, bucket, key, versionID string) (*ObjectMeta, error) {
	// Validate names
	if err := validateName(bucket); err != nil {
		return nil, fmt.Errorf("invalid bucket name: %w", err)
	}
	if err := validateName(key); err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check bucket exists
	if _, err := s.getBucketMeta(bucket); err != nil {
		return nil, err
	}

	// Find the version to restore
	versionPath := s.versionMetaPath(bucket, key, versionID)
	data, err := os.ReadFile(versionPath)
	if os.IsNotExist(err) {
		return nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read version meta: %w", err)
	}

	var oldMeta ObjectMeta
	if err := json.Unmarshal(data, &oldMeta); err != nil {
		return nil, fmt.Errorf("unmarshal version meta: %w", err)
	}

	// Archive current version
	if err := s.archiveCurrentVersion(bucket, key); err != nil {
		return nil, fmt.Errorf("archive current version: %w", err)
	}

	// Create new metadata pointing to old content.
	// ErasureCoding, ChunkMetadata and VersionVector are preserved so that
	// GetObject can decode the restored object correctly.
	now := time.Now().UTC()
	newMeta := ObjectMeta{
		Key:           key,
		Size:          oldMeta.Size,
		ContentType:   oldMeta.ContentType,
		ETag:          oldMeta.ETag,
		LastModified:  now,
		Metadata:      oldMeta.Metadata,
		VersionID:     generateVersionID(),
		Chunks:        oldMeta.Chunks,        // Reuse same chunks (no duplication)
		ChunkMetadata: oldMeta.ChunkMetadata, // Required for EC decoding
		ErasureCoding: oldMeta.ErasureCoding, // Required for GetObject to use EC path
		VersionVector: oldMeta.VersionVector,
	}

	// Set expiry if configured
	if s.defaultObjectExpiryDays > 0 && bucket != SystemBucket {
		expiry := now.AddDate(0, 0, s.defaultObjectExpiryDays)
		newMeta.Expires = &expiry
	}

	// Write new metadata
	metaPath := s.objectMetaPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
		return nil, fmt.Errorf("create meta dir: %w", err)
	}

	metaData, err := json.MarshalIndent(newMeta, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal object meta: %w", err)
	}

	if err := syncedWriteFile(metaPath, metaData, 0644); err != nil {
		return nil, fmt.Errorf("write object meta: %w", err)
	}

	// Prune expired versions.
	// Chunk GC for pruned versions is deferred to the periodic GC pass.
	s.pruneExpiredVersions(ctx, bucket, key)

	return &newMeta, nil
}

// collectAndDeleteAllVersions removes all version files for an object and returns
// the chunk hashes that were referenced by those versions. The caller is responsible
// for checking global references and deleting unreferenced chunks.
// Must be called with s.mu held.
func (s *Store) collectAndDeleteAllVersions(bucket, key string) []string {
	versionDir := s.versionDir(bucket, key)

	// Collect all chunk hashes from versions
	var chunks []string
	var versionCount int64
	var totalVersionBytes int64
	seen := make(map[string]struct{})
	entries, err := os.ReadDir(versionDir)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				continue
			}

			versionPath := filepath.Join(versionDir, entry.Name())
			data, err := os.ReadFile(versionPath)
			if err != nil {
				continue
			}

			var meta ObjectMeta
			if err := json.Unmarshal(data, &meta); err != nil {
				continue
			}

			versionCount++
			totalVersionBytes += meta.Size
			for _, h := range meta.Chunks {
				if _, ok := seen[h]; !ok {
					seen[h] = struct{}{}
					chunks = append(chunks, h)
				}
			}
		}
	}

	// Remove version directory
	_ = os.RemoveAll(versionDir)

	// Decrement version count and version bytes
	if versionCount > 0 {
		s.statsVersionCount.Add(-versionCount)
		s.statsVersionBytes.Add(-totalVersionBytes)
	}

	return chunks
}

// Close flushes any pending operations and syncs the data directory.
// Since all writes use syncedWriteFile (which calls fsync), this just ensures
// directory metadata is flushed for durability.
func (s *Store) Close() (retErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Defer CAS close so the encoder's internal goroutines are always released,
	// even if future callers of this function add early-return error paths.
	if s.cas != nil {
		defer func() {
			if err := s.cas.Close(); err != nil && retErr == nil {
				retErr = fmt.Errorf("close CAS encoder: %w", err)
			}
		}()
	}

	// Sync all bucket directories and their subdirectories to ensure
	// all filesystem operations are complete before cleanup.
	// This is critical for tests with race detector where cleanup happens immediately.
	bucketsDir := filepath.Join(s.dataDir, "buckets")
	if buckets, err := os.ReadDir(bucketsDir); err == nil {
		for _, bucket := range buckets {
			if !bucket.IsDir() {
				continue
			}
			bucketPath := filepath.Join(bucketsDir, bucket.Name())
			metaDir := filepath.Join(bucketPath, "meta")

			// Sync all metadata files first
			if metaFiles, err := os.ReadDir(metaDir); err == nil {
				for _, metaFile := range metaFiles {
					if metaFile.IsDir() {
						continue
					}
					metaPath := filepath.Join(metaDir, metaFile.Name())
					if f, err := os.Open(metaPath); err == nil {
						_ = f.Sync()
						_ = f.Close()
					}
				}
			}

			// Sync meta directory itself
			if f, err := os.Open(metaDir); err == nil {
				_ = f.Sync()
				_ = f.Close()
			}

			// Sync objects directory if it exists
			objectsDir := filepath.Join(bucketPath, "objects")
			if f, err := os.Open(objectsDir); err == nil {
				_ = f.Sync()
				_ = f.Close()
			}

			// Sync bucket directory
			if f, err := os.Open(bucketPath); err == nil {
				_ = f.Sync()
				_ = f.Close()
			}
		}

		// Sync buckets directory
		if f, err := os.Open(bucketsDir); err == nil {
			_ = f.Sync()
			_ = f.Close()
		}
	}

	// Sync the data directory to ensure directory entries are durable
	if f, err := os.Open(s.dataDir); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}

	return nil
}
