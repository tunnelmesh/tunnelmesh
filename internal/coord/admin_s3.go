package coord

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

// S3ObjectInfo represents an S3 object for the explorer API.
type S3ObjectInfo struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	LastModified string    `json:"last_modified"`
	Owner        string    `json:"owner,omitempty"`      // Owner peer name (derived from bucket owner)
	Expires      string    `json:"expires,omitempty"`    // Optional expiration date
	DeletedAt    string    `json:"deleted_at,omitempty"` // When the object was deleted (recycled)
	ContentType  string    `json:"content_type,omitempty"`
	IsPrefix     bool      `json:"is_prefix,omitempty"` // True for "folder" prefixes
	Forwarded    bool      `json:"-"`                   // In-memory: survives loadPeerIndexes until remote persists
	ForwardedAt  time.Time `json:"-"`                   // In-memory: when the forwarded entry was created
	SourceIP     string    `json:"-"`                   // In-memory: coordinator IP that owns this object
}

// S3BucketInfo represents an S3 bucket for the explorer API.
type S3BucketInfo struct {
	Name              string `json:"name"`
	CreatedAt         string `json:"created_at"`
	Writable          bool   `json:"writable"`
	UsedBytes         int64  `json:"used_bytes"`
	QuotaBytes        int64  `json:"quota_bytes,omitempty"`        // Per-bucket quota (from file share, 0 = unlimited)
	ReplicationFactor int    `json:"replication_factor,omitempty"` // Number of replicas (1-3)
}

// S3QuotaInfo represents overall S3 storage quota for the explorer API.
type S3QuotaInfo struct {
	MaxBytes   int64 `json:"max_bytes"`
	UsedBytes  int64 `json:"used_bytes"`
	AvailBytes int64 `json:"avail_bytes"`
}

// S3VolumeInfo represents filesystem volume information for the S3 storage.
type S3VolumeInfo struct {
	TotalBytes     int64 `json:"total_bytes"`
	UsedBytes      int64 `json:"used_bytes"`
	AvailableBytes int64 `json:"available_bytes"`
}

// S3StorageInfo represents deduplication storage statistics.
type S3StorageInfo struct {
	PhysicalBytes int64   `json:"physical_bytes"` // Actual on-disk bytes (after dedup)
	LogicalBytes  int64   `json:"logical_bytes"`  // Logical bytes (before dedup)
	DedupRatio    float64 `json:"dedup_ratio"`    // logical/physical (>1 means savings)
}

// S3BucketsResponse is the response for the buckets list endpoint.
type S3BucketsResponse struct {
	Buckets []S3BucketInfo `json:"buckets"`
	Quota   S3QuotaInfo    `json:"quota"`
	Volume  *S3VolumeInfo  `json:"volume,omitempty"`
	Storage *S3StorageInfo `json:"storage,omitempty"`
}

// validateS3Name validates a bucket or object key name to prevent path traversal.
// This is the API-level validation that runs before any S3 store operations.
// The s3.Store also has its own validateName function as defense-in-depth,
// ensuring protection even if the API layer is bypassed or refactored.
// Both functions perform identical checks; duplication is intentional for security.
func validateS3Name(name string) error {
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
	if strings.Contains(name, "..") {
		return fmt.Errorf("path traversal not allowed")
	}
	// Check for absolute paths or parent directory references
	if strings.HasPrefix(name, "/") || strings.HasPrefix(name, "\\") {
		return fmt.Errorf("absolute paths not allowed")
	}
	return nil
}

// handleS3Proxy routes S3 explorer API requests.
//
// handleS3GC triggers on-demand S3 garbage collection.
func (s *Server) handleS3GC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.s3Store == nil {
		s.jsonError(w, "S3 storage not enabled", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		PurgeRecycleBin bool `json:"purge_recycle_bin"`
		// Internal: set when forwarding to peers to prevent infinite recursion
		NoForward bool `json:"no_forward"`
		// ForceDeleteBuckets bypasses orphanedBucketGracePeriod for explicitly named fs+ buckets.
		// Used by the share deletion handler to clean coord-2/3 without waiting for grace period.
		ForceDeleteBuckets []string `json:"force_delete_buckets,omitempty"`
		// BucketsOnly skips full GC; only processes ForceDeleteBuckets deletions.
		BucketsOnly bool `json:"buckets_only,omitempty"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	// BucketsOnly fast path: force-delete named fs+ buckets without acquiring gcMu.
	// ForceDeleteBucket uses its own internal store lock so this is safe to run
	// concurrently with a full GC cycle. Keeping it outside gcMu prevents forwarded
	// bucket deletions from returning 429 when a full GC happens to be running.
	if req.BucketsOnly {
		var deletedBuckets int
		for _, bucket := range req.ForceDeleteBuckets {
			if !strings.HasPrefix(bucket, s3.FileShareBucketPrefix) {
				continue // Safety: only allow fs+ buckets via this mechanism
			}
			if err := s.s3Store.ForceDeleteBucket(r.Context(), bucket); err != nil {
				log.Warn().Err(err).Str("bucket", bucket).Msg("force delete bucket during GC")
			} else {
				deletedBuckets++
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"deleted_buckets": deletedBuckets,
		})
		return
	}

	// Serialize full GC operations — only one can run at a time
	if !s.gcMu.TryLock() {
		s.jsonError(w, "garbage collection already in progress", http.StatusTooManyRequests)
		return
	}
	defer s.gcMu.Unlock()

	gcStart := time.Now()

	// Phase 0: Purge orphaned file share buckets before GC.
	// When a share is deleted on one coordinator, ForceDeleteBucket only runs locally.
	// Replica coordinators still have the bucket and objects. Reconcile by checking
	// which fs+* buckets have no corresponding share and deleting them.
	if s.fileShareMgr != nil {
		if purged := s.fileShareMgr.PurgeOrphanedFileShareBuckets(r.Context()); purged > 0 {
			log.Info().Int("count", purged).Msg("purged orphaned file share buckets during manual GC")
		}
	}

	// Phase 1: Purge recycled objects
	var recycledPurged int
	if req.PurgeRecycleBin {
		recycledPurged = s.s3Store.PurgeAllRecycled(r.Context())
	} else {
		recycledPurged = s.s3Store.PurgeRecycleBin(r.Context())
	}

	// Phase 2: Run full GC with no grace period (admin explicitly requested cleanup)
	gcStats := s.s3Store.RunGarbageCollectionForce(r.Context())
	gcDuration := time.Since(gcStart).Seconds()

	// Record GC metrics (same as periodic path in StartPeriodicCleanup)
	if metrics := s3.GetS3Metrics(); metrics != nil {
		metrics.RecordGCRun(gcStats.VersionsPruned, gcStats.ChunksDeleted, gcStats.BytesReclaimed, gcDuration)
	}

	// Refresh storage gauges after GC
	s.updateS3Metrics()

	log.Info().
		Int("recycled_purged", recycledPurged).
		Int("versions_pruned", gcStats.VersionsPruned).
		Int("chunks_deleted", gcStats.ChunksDeleted).
		Int64("bytes_reclaimed", gcStats.BytesReclaimed).
		Float64("duration_seconds", gcDuration).
		Msg("manual S3 garbage collection completed")

	// Phase 3: Forward GC to peer coordinators (unless this is already a forwarded request)
	if !req.NoForward {
		s.forwardGCToPeers(r.Context(), req.PurgeRecycleBin)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"recycled_purged":  recycledPurged,
		"versions_pruned":  gcStats.VersionsPruned,
		"chunks_deleted":   gcStats.ChunksDeleted,
		"bytes_reclaimed":  gcStats.BytesReclaimed,
		"duration_seconds": gcDuration,
	}); err != nil {
		log.Error().Err(err).Msg("failed to encode GC stats response")
	}
}

// forwardGCToPeers sends GC requests to all peer coordinators in parallel.
// Uses no_forward=true to prevent infinite recursion between peers.
func (s *Server) forwardGCToPeers(ctx context.Context, purgeRecycleBin bool) {
	if s.replicator == nil {
		return
	}

	peers := s.replicator.GetPeers()
	if len(peers) == 0 {
		return
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"purge_recycle_bin": purgeRecycleBin,
		"no_forward":        true,
	})

	// Share a single HTTP client across all peer requests for connection reuse
	client := &http.Client{
		Timeout: 10 * time.Minute,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // mesh-internal
		},
	}

	var wg sync.WaitGroup
	for _, peerIP := range peers {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()

			gcURL := fmt.Sprintf("https://%s:443/api/s3/gc", ip)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, gcURL, bytes.NewReader(payload))
			if err != nil {
				log.Warn().Err(err).Str("peer", ip).Msg("failed to create GC forward request")
				return
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				log.Warn().Err(err).Str("peer", ip).Msg("failed to forward GC to peer")
				return
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode == http.StatusOK {
				log.Info().Str("peer", ip).Msg("GC forwarded to peer coordinator")
			} else {
				body, _ := io.ReadAll(resp.Body)
				log.Warn().Str("peer", ip).Int("status", resp.StatusCode).Str("body", string(body)).Msg("peer GC returned non-OK")
			}
		}(peerIP)
	}
	wg.Wait()
}

// handlePurgeBucketRecycleBin purges all recycle bin entries for a specific bucket.
// This allows users to empty a bucket's recycle bin before deleting the bucket,
// since DeleteBucket blocks on non-empty recycle bins.
//
// DELETE /api/s3/buckets/{bucket}/recyclebin
func (s *Server) handlePurgeBucketRecycleBin(w http.ResponseWriter, r *http.Request, bucket string) {
	if s.s3Store == nil {
		s.jsonError(w, "S3 storage not enabled", http.StatusServiceUnavailable)
		return
	}

	if err := s.s3Store.PurgeAllRecycledInBucket(r.Context(), bucket); err != nil {
		s.jsonError(w, fmt.Sprintf("purge recycle bin: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"bucket": bucket,
		"purged": true,
	})
}

// Security: This endpoint is registered on adminMux which is only served over HTTPS
// on the coordinator's mesh IP (via Server.setupAdminRoutes). It is NOT accessible
// from the public internet. All requests are authenticated via mTLS - the client
// must present a valid mesh certificate. The caller's identity is extracted from
// the TLS client certificate for authorization decisions.
func (s *Server) handleS3Proxy(w http.ResponseWriter, r *http.Request) {
	if s.s3Store == nil {
		s.jsonError(w, "S3 storage not enabled", http.StatusServiceUnavailable)
		return
	}

	// Strip /api/s3 prefix
	path := strings.TrimPrefix(r.URL.Path, "/api/s3")
	path = strings.TrimPrefix(path, "/")

	// Route: /api/s3/buckets
	if path == "buckets" || path == "" {
		s.withS3AdminMetrics(w, "listBuckets", func(w http.ResponseWriter) {
			s.handleS3ListBuckets(w, r)
		})
		return
	}

	// Route: /api/s3/buckets/{bucket}/objects/...
	if strings.HasPrefix(path, "buckets/") {
		parts := strings.SplitN(strings.TrimPrefix(path, "buckets/"), "/", 2)
		bucket := parts[0]

		// URL decode bucket name
		if decoded, err := url.PathUnescape(bucket); err == nil {
			bucket = decoded
		}

		// Validate bucket name
		if err := validateS3Name(bucket); err != nil {
			s.jsonError(w, "invalid bucket name: "+err.Error(), http.StatusBadRequest)
			return
		}

		if len(parts) == 1 || parts[1] == "" || parts[1] == "objects" {
			// List objects in bucket
			s.withS3AdminMetrics(w, "listObjects", func(w http.ResponseWriter) {
				s.handleS3ListObjects(w, r, bucket)
			})
			return
		}
		if parts[1] == "recyclebin" || strings.HasPrefix(parts[1], "recyclebin/") {
			rbPath := strings.TrimPrefix(parts[1], "recyclebin")
			rbPath = strings.TrimPrefix(rbPath, "/")
			if rbPath == "" {
				s.withS3AdminMetrics(w, "listRecycledObjects", func(w http.ResponseWriter) {
					s.handleS3ListRecycledObjects(w, r, bucket)
				})
			} else {
				if decoded, err := url.PathUnescape(rbPath); err == nil {
					rbPath = decoded
				}
				if err := validateS3Name(rbPath); err != nil {
					s.jsonError(w, "invalid object key: "+err.Error(), http.StatusBadRequest)
					return
				}
				s.withS3AdminMetrics(w, "getRecycledObject", func(w http.ResponseWriter) {
					s.handleS3GetRecycledObject(w, r, bucket, rbPath)
				})
			}
			return
		}
		if strings.HasPrefix(parts[1], "objects/") {
			objPath := strings.TrimPrefix(parts[1], "objects/")

			// Check for version subresources (e.g., objects/{key}/versions, objects/{key}/restore, objects/{key}/undelete)
			// Split on /versions, /restore, or /undelete to extract key and subresource
			var key, subresource string
			if idx := strings.Index(objPath, "/versions"); idx >= 0 {
				key = objPath[:idx]
				subresource = "versions"
			} else if idx := strings.Index(objPath, "/restore"); idx >= 0 {
				key = objPath[:idx]
				subresource = "restore"
			} else if idx := strings.Index(objPath, "/undelete"); idx >= 0 {
				key = objPath[:idx]
				subresource = "undelete"
			} else {
				key = objPath
			}

			// URL decode object key
			if decoded, err := url.PathUnescape(key); err == nil {
				key = decoded
			}

			// Validate object key
			if err := validateS3Name(key); err != nil {
				s.jsonError(w, "invalid object key: "+err.Error(), http.StatusBadRequest)
				return
			}

			// Route based on subresource
			switch subresource {
			case "versions":
				s.withS3AdminMetrics(w, "listVersions", func(w http.ResponseWriter) {
					s.handleS3ListVersions(w, r, bucket, key)
				})
			case "restore":
				s.withS3AdminMetrics(w, "restoreVersion", func(w http.ResponseWriter) {
					s.handleS3RestoreVersion(w, r, bucket, key)
				})
			case "undelete":
				s.withS3AdminMetrics(w, "undeleteObject", func(w http.ResponseWriter) {
					s.handleS3UndeleteObject(w, r, bucket, key)
				})
			default:
				s.handleS3Object(w, r, bucket, key)
			}
			return
		}
	}

	s.jsonError(w, "not found", http.StatusNotFound)
}

// handleS3ListBuckets returns all buckets (admin view).
func (s *Server) handleS3ListBuckets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	buckets, err := s.s3Store.ListBuckets(r.Context())
	if err != nil {
		s.jsonError(w, "failed to list buckets", http.StatusInternalServerError)
		return
	}

	// Get quota stats for per-bucket usage
	quotaStats := s.s3Store.QuotaStats()

	bucketInfos := make([]S3BucketInfo, 0, len(buckets))
	for _, b := range buckets {
		// System bucket is read-only (could be extended to check RBAC)
		writable := b.Name != auth.SystemBucket
		usedBytes := int64(0)
		if quotaStats != nil {
			usedBytes = quotaStats.PerBucket[b.Name]
		}

		// Look up per-bucket quota: prefer file share metadata for fs+ buckets,
		// fall back to BucketMeta.QuotaBytes for regular buckets.
		var quotaBytes int64
		if s.fileShareMgr != nil && strings.HasPrefix(b.Name, s3.FileShareBucketPrefix) {
			shareName := strings.TrimPrefix(b.Name, s3.FileShareBucketPrefix)
			if share := s.fileShareMgr.Get(shareName); share != nil {
				quotaBytes = share.QuotaBytes
			}
		} else {
			quotaBytes = b.QuotaBytes
		}

		bucketInfos = append(bucketInfos, S3BucketInfo{
			Name:              b.Name,
			CreatedAt:         b.CreatedAt.Format(time.RFC3339),
			Writable:          writable,
			UsedBytes:         usedBytes,
			QuotaBytes:        quotaBytes,
			ReplicationFactor: b.ReplicationFactor,
		})
	}

	// Build response with quota info
	resp := S3BucketsResponse{
		Buckets: bucketInfos,
	}
	if quotaStats != nil {
		resp.Quota = S3QuotaInfo{
			MaxBytes:   quotaStats.MaxBytes,
			UsedBytes:  quotaStats.UsedBytes,
			AvailBytes: quotaStats.AvailableBytes,
		}
	}

	// Add filesystem volume info
	if total, used, avail, err := s.s3Store.VolumeStats(); err == nil {
		resp.Volume = &S3VolumeInfo{TotalBytes: total, UsedBytes: used, AvailableBytes: avail}
	}

	// Add dedup storage stats (total logical = live + versions + recyclebin)
	casStats := s.s3Store.GetCASStats()
	totalLogical := casStats.LogicalBytes + casStats.VersionBytes + casStats.RecycledBytes
	dedupRatio := 1.0
	if casStats.ChunkBytes > 0 {
		// Clamp to >=1.0: transient states during GC can briefly have logical < physical
		dedupRatio = max(1.0, float64(totalLogical)/float64(casStats.ChunkBytes))
	}
	resp.Storage = &S3StorageInfo{
		PhysicalBytes: casStats.ChunkBytes,
		LogicalBytes:  totalLogical,
		DedupRatio:    dedupRatio,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleListBuckets wraps handleS3ListBuckets for the new API route
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	s.handleS3ListBuckets(w, r)
}

// handleCreateBucket creates a new S3 bucket with specified replication factor
func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name              string `json:"name"`
		ReplicationFactor int    `json:"replication_factor"` // Optional, defaults to 2
		QuotaBytes        int64  `json:"quota_bytes"`        // Optional per-bucket quota in bytes; 0 = unlimited
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Validate bucket name
	if err := validateS3Name(req.Name); err != nil {
		s.jsonError(w, "invalid bucket name: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Default replication factor
	if req.ReplicationFactor == 0 {
		req.ReplicationFactor = 2
	}

	// Validate range
	if req.ReplicationFactor < 1 || req.ReplicationFactor > 3 {
		s.jsonError(w, "replication factor must be 1-3", http.StatusBadRequest)
		return
	}

	// Validate quota (if specified)
	if req.QuotaBytes < 0 {
		s.jsonError(w, "quota bytes cannot be negative", http.StatusBadRequest)
		return
	}

	// Check authorization
	userID := s.getRequestOwner(r)
	if userID == "" {
		s.jsonError(w, "authentication required", http.StatusUnauthorized)
		return
	}

	// Check if user has permission to create buckets
	if !s.s3Authorizer.Authorize(userID, "create", "buckets", req.Name, "") {
		s.jsonError(w, "permission denied", http.StatusForbidden)
		return
	}

	// Create bucket via S3 store
	if err := s.s3Store.CreateBucket(r.Context(), req.Name, userID, req.ReplicationFactor, nil); err != nil {
		if errors.Is(err, s3.ErrBucketExists) {
			s.jsonError(w, "bucket already exists", http.StatusConflict)
			return
		}
		s.jsonError(w, "failed to create bucket: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Apply per-bucket quota if specified.
	// Best-effort: UpdateBucketMetadata is a local file write on a directory that was
	// just created, so failure here is extremely unlikely (filesystem error in <1ms window).
	// If it fails the bucket still exists but is unlimited; the warning log captures this.
	// Use the PATCH endpoint to correct quota post-creation if needed.
	if req.QuotaBytes > 0 {
		q := req.QuotaBytes
		if err := s.s3Store.UpdateBucketMetadata(r.Context(), req.Name, s3.BucketMetadataUpdate{
			QuotaBytes: &q,
		}); err != nil {
			log.Warn().Err(err).Str("bucket", req.Name).Msg("failed to set quota on new bucket; bucket created but is unlimited")
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"name": req.Name})
}

// handleGetBucket returns metadata for a specific bucket
func (s *Server) handleGetBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	// Validate bucket name
	if err := validateS3Name(bucket); err != nil {
		s.jsonError(w, "invalid bucket name: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Check authorization
	userID := s.getRequestOwner(r)
	if userID == "" {
		s.jsonError(w, "authentication required", http.StatusUnauthorized)
		return
	}

	// User must have read access to the bucket
	if !s.s3Authorizer.Authorize(userID, "get", "buckets", bucket, "") {
		s.jsonError(w, "access denied", http.StatusForbidden)
		return
	}

	// Get bucket metadata from store
	buckets, err := s.s3Store.ListBuckets(r.Context())
	if err != nil {
		s.jsonError(w, "failed to get bucket", http.StatusInternalServerError)
		return
	}

	// Find the requested bucket
	var bucketMeta *s3.BucketMeta
	for i := range buckets {
		if buckets[i].Name == bucket {
			bucketMeta = &buckets[i]
			break
		}
	}

	if bucketMeta == nil {
		s.jsonError(w, "bucket not found", http.StatusNotFound)
		return
	}

	// Return bucket metadata
	resp := map[string]interface{}{
		"name":               bucketMeta.Name,
		"created_at":         bucketMeta.CreatedAt.Format(time.RFC3339),
		"owner":              bucketMeta.Owner,
		"replication_factor": bucketMeta.ReplicationFactor,
		"size_bytes":         bucketMeta.SizeBytes,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleUpdateBucket updates bucket metadata (admin-only)
func (s *Server) handleUpdateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	var req struct {
		ReplicationFactor *int   `json:"replication_factor,omitempty"`
		QuotaBytes        *int64 `json:"quota_bytes,omitempty"` // nil = no change, 0 = remove limit
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Validate bucket name
	if err := validateS3Name(bucket); err != nil {
		s.jsonError(w, "invalid bucket name: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Block system bucket modifications (check before auth to match S3 proxy pattern)
	if bucket == auth.SystemBucket {
		s.jsonError(w, "cannot modify system bucket", http.StatusForbidden)
		return
	}

	// Admin check (consistent with panel handlers: IsAdmin on empty user → 403, not 401)
	userID := s.getRequestOwner(r)
	if !s.s3Authorizer.IsAdmin(userID) {
		s.jsonError(w, "admin permission required", http.StatusForbidden)
		return
	}

	if req.QuotaBytes != nil && *req.QuotaBytes < 0 {
		s.jsonError(w, "quota_bytes cannot be negative", http.StatusBadRequest)
		return
	}

	// Update bucket metadata
	updates := s3.BucketMetadataUpdate{
		ReplicationFactor: req.ReplicationFactor,
		QuotaBytes:        req.QuotaBytes,
	}

	if err := s.s3Store.UpdateBucketMetadata(r.Context(), bucket, updates); err != nil {
		if errors.Is(err, s3.ErrBucketNotFound) {
			s.jsonError(w, "bucket not found", http.StatusNotFound)
			return
		}
		s.jsonError(w, "failed to update bucket: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{})
}
