package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
)

// orphanedBucketGracePeriod is the minimum age a bucket must reach before it
// can be purged as orphaned. This prevents deleting buckets that were replicated
// from another coordinator before the corresponding share config arrives.
// 2 minutes is sufficient: file_shares.json replicates within seconds.
const orphanedBucketGracePeriod = 2 * time.Minute

// FileShareManager manages file shares backed by S3 buckets.
type FileShareManager struct {
	store       *Store
	systemStore *SystemStore
	authorizer  *auth.Authorizer
	shares      []*FileShare
	mu          sync.RWMutex
}

// NewFileShareManager creates a new file share manager.
// It loads existing shares from the system store and ensures their underlying
// S3 buckets exist on disk. Buckets may be missing if share metadata was
// replicated from another coordinator but the bucket directories were not.
func NewFileShareManager(store *Store, systemStore *SystemStore, authorizer *auth.Authorizer) *FileShareManager {
	mgr := &FileShareManager{
		store:       store,
		systemStore: systemStore,
		authorizer:  authorizer,
	}

	// Load existing shares
	if systemStore != nil {
		shares, _ := systemStore.LoadFileShares(context.Background())
		mgr.shares = shares

		// Ensure buckets exist for all loaded shares.
		// Share metadata may have been replicated from another coordinator
		// (via system store replication) without the corresponding bucket
		// directories being created locally.
		for _, share := range shares {
			bucketName := FileShareBucketPrefix + share.Name
			if _, err := store.HeadBucket(context.Background(), bucketName); err != nil {
				// Bucket missing — recreate it with the share's stored replication factor
				rf := share.ReplicationFactor
				if rf < 1 || rf > 3 {
					rf = 2 // Default for shares persisted before RF was stored
				}
				if createErr := store.CreateBucket(context.Background(), bucketName, share.Owner, rf, nil); createErr == nil {
					log.Info().Str("share", share.Name).Str("bucket", bucketName).Int("rf", rf).Msg("recreated missing bucket for share")
				} else {
					log.Warn().Err(createErr).Str("share", share.Name).Str("bucket", bucketName).Msg("failed to recreate missing bucket for share")
				}
			}
		}
	}

	return mgr
}

// reloadShares refreshes the in-memory shares list from the system store.
// Must be called with m.mu held. This prevents stale overwrites when multiple
// coordinators create or delete shares concurrently — without it, SaveFileShares
// would overwrite shares created by other coordinators since the last load.
func (m *FileShareManager) reloadShares(ctx context.Context) {
	if m.systemStore == nil {
		return
	}
	shares, err := m.systemStore.LoadFileShares(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to reload shares from system store before save")
		return
	}
	if shares != nil {
		m.shares = shares
	}
}

// FileShareOptions contains optional settings for creating a file share.
type FileShareOptions struct {
	ExpiresAt         time.Time // When the share expires (zero = use default or never)
	GuestRead         bool      // Allow all mesh users to read (default: true if not specified)
	GuestReadSet      bool      // Whether GuestRead was explicitly set
	ReplicationFactor int       // Number of replicas (1-3, default: 2 if 0)
}

// Create creates a new file share.
// It creates the underlying bucket, sets up default permissions, and persists the share metadata.
// quotaBytes of 0 means unlimited (within global quota).
func (m *FileShareManager) Create(ctx context.Context, name, description, ownerID string, quotaBytes int64, opts *FileShareOptions) (*FileShare, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Reload from system store to pick up shares created by other coordinators,
	// preventing stale overwrites when we SaveFileShares below.
	m.reloadShares(ctx)

	// Check if share already exists
	for _, s := range m.shares {
		if s.Name == name {
			return nil, fmt.Errorf("file share %q already exists", name)
		}
	}

	bucketName := FileShareBucketPrefix + name

	// Determine effective replication factor
	replicationFactor := 2
	if opts != nil && opts.ReplicationFactor > 0 {
		replicationFactor = opts.ReplicationFactor
	}

	// Check if bucket already exists (from a previously deleted share)
	bucketExists := false
	if _, err := m.store.HeadBucket(ctx, bucketName); err == nil {
		bucketExists = true
	} else {
		// Create new bucket
		if err := m.store.CreateBucket(ctx, bucketName, ownerID, replicationFactor, nil); err != nil {
			return nil, fmt.Errorf("create bucket: %w", err)
		}
	}

	// Determine if guest read should be enabled
	// Default is true unless explicitly disabled
	guestRead := true
	if opts != nil && opts.GuestReadSet {
		guestRead = opts.GuestRead
	}

	// Create group binding: everyone -> bucket-read (only if guest read is enabled)
	if guestRead {
		everyoneBinding := &auth.GroupBinding{
			Name:        fmt.Sprintf("everyone-%s-read", name),
			GroupName:   auth.GroupEveryone,
			RoleName:    auth.RoleBucketRead,
			BucketScope: bucketName,
		}
		m.authorizer.GroupBindings.Add(everyoneBinding)
	}

	// Create role binding: owner -> bucket-admin
	ownerBinding := auth.NewRoleBinding(ownerID, auth.RoleBucketAdmin, bucketName)
	m.authorizer.Bindings.Add(ownerBinding)

	// Create share record
	now := time.Now().UTC()
	share := &FileShare{
		Name:              name,
		Description:       description,
		Owner:             ownerID,
		CreatedAt:         now,
		QuotaBytes:        quotaBytes,
		GuestRead:         guestRead,
		ReplicationFactor: replicationFactor,
	}

	// Set expiry from opts or use default
	if opts != nil && !opts.ExpiresAt.IsZero() {
		share.ExpiresAt = opts.ExpiresAt
	} else if expiryDays := m.store.DefaultShareExpiryDays(); expiryDays > 0 {
		share.ExpiresAt = now.AddDate(0, 0, expiryDays)
	}
	m.shares = append(m.shares, share)

	// Persist share metadata.
	// Note: If persistence fails, the share is still functional in memory. The bucket
	// and bindings have been created, but the share won't survive a restart.
	// This is acceptable because:
	// 1. The bucket data persists on disk via the Store
	// 2. On restart, if the bucket exists but share metadata doesn't, an admin can
	//    recreate the share with the same name - the existing bucket will be restored
	// 3. Bindings are persisted separately by the caller (handleFileShareCreate)
	// A full rollback would require deleting the bucket and bindings, which could
	// leave the system in an inconsistent state if those operations also fail.
	if m.systemStore != nil {
		if err := m.systemStore.SaveFileShares(ctx, m.shares); err != nil {
			log.Warn().Err(err).Str("share", name).Msg("failed to persist file share metadata")
			return share, fmt.Errorf("persist share (share created but not persisted): %w", err)
		}
	}

	// Log if we restored a previous share
	if bucketExists {
		log.Info().Str("share", name).Msg("restored file share with previous content")
	}

	return share, nil
}

// Delete removes a file share.
// It deletes the underlying bucket, removes permissions, and updates metadata.
func (m *FileShareManager) Delete(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Reload from system store to pick up shares created by other coordinators,
	// preventing stale overwrites when we SaveFileShares below.
	m.reloadShares(ctx)

	// Find the share
	var shareIdx = -1
	for i, s := range m.shares {
		if s.Name == name {
			shareIdx = i
			break
		}
	}
	if shareIdx == -1 {
		return fmt.Errorf("file share %q not found", name)
	}

	bucketName := FileShareBucketPrefix + name

	// Hard delete: remove the entire bucket directory.
	// File shares are admin-managed, so we skip per-object purge (which does
	// expensive global chunk reference scans) and just delete the directory.
	// Orphaned chunks will be cleaned up by the next periodic GC cycle.
	_ = m.store.ForceDeleteBucket(ctx, bucketName)

	// Remove group bindings for this bucket
	for _, b := range m.authorizer.GroupBindings.List() {
		if b.BucketScope == bucketName {
			m.authorizer.GroupBindings.Remove(b.Name)
		}
	}

	// Remove all role bindings scoped to this bucket
	for _, b := range m.authorizer.Bindings.List() {
		if b.BucketScope == bucketName {
			m.authorizer.Bindings.Remove(b.Name)
		}
	}

	// Remove from shares list
	m.shares = append(m.shares[:shareIdx], m.shares[shareIdx+1:]...)

	// Persist
	if m.systemStore != nil {
		if err := m.systemStore.SaveFileShares(ctx, m.shares); err != nil {
			return fmt.Errorf("persist shares: %w", err)
		}
	}

	return nil
}

// Get returns a file share by name, or nil if not found.
func (m *FileShareManager) Get(name string) *FileShare {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.shares {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// EnsureBucketForShare recreates a missing bucket for an existing share.
// This handles the case where _meta.json was corrupted (e.g. disk-full truncation)
// but the share metadata still exists in the system store.
// Returns nil if the bucket is not a share bucket, no share exists, or the bucket already exists.
func (m *FileShareManager) EnsureBucketForShare(ctx context.Context, bucketName string) error {
	if !strings.HasPrefix(bucketName, FileShareBucketPrefix) {
		return nil
	}

	shareName := strings.TrimPrefix(bucketName, FileShareBucketPrefix)
	share := m.Get(shareName)
	if share == nil {
		// Share not in memory — it may have been created on another coordinator
		// and replicated to our system store. Reload from system store.
		share = m.refreshAndGet(ctx, shareName)
		if share == nil {
			return nil
		}
	}

	if _, err := m.store.HeadBucket(ctx, bucketName); err == nil {
		return nil // bucket exists
	}

	rf := share.ReplicationFactor
	if rf < 1 || rf > 3 {
		rf = 2
	}

	err := m.store.CreateBucket(ctx, bucketName, share.Owner, rf, nil)
	if err != nil && errors.Is(err, ErrBucketExists) {
		return nil // concurrent creation, fine
	}
	if err == nil {
		log.Info().Str("share", shareName).Str("bucket", bucketName).Msg("recreated missing bucket for share on demand")
	}
	return err
}

// refreshAndGet reloads shares from the system store and returns the named share if found.
// This catches shares that were created on another coordinator and replicated via the system bucket.
func (m *FileShareManager) refreshAndGet(ctx context.Context, name string) *FileShare {
	if m.systemStore == nil {
		return nil
	}
	shares, err := m.systemStore.LoadFileShares(ctx)
	if err != nil || len(shares) == 0 {
		return nil
	}

	m.mu.Lock()
	m.shares = shares
	m.mu.Unlock()

	for _, s := range shares {
		if s.Name == name {
			log.Info().Str("share", name).Msg("discovered replicated share from system store")
			return s
		}
	}
	return nil
}

// List returns all file shares.
func (m *FileShareManager) List() []*FileShare {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*FileShare, len(m.shares))
	copy(result, m.shares)
	return result
}

// BucketName returns the bucket name for a file share.
func (m *FileShareManager) BucketName(shareName string) string {
	return FileShareBucketPrefix + shareName
}

// IsProtectedBinding returns true if the binding is a file share owner's admin binding.
// These bindings should not be deleted manually - they are managed by the file share lifecycle.
func (m *FileShareManager) IsProtectedBinding(binding *auth.RoleBinding) bool {
	// Only bucket-admin bindings on file share buckets can be protected
	if binding.RoleName != auth.RoleBucketAdmin {
		return false
	}
	if !strings.HasPrefix(binding.BucketScope, FileShareBucketPrefix) {
		return false
	}

	// Check if the user is the owner of this file share
	shareName := strings.TrimPrefix(binding.BucketScope, FileShareBucketPrefix)
	share := m.Get(shareName)
	if share == nil {
		return false
	}

	return share.Owner == binding.PeerID
}

// IsProtectedGroupBinding checks if a group binding is a file share's "everyone" binding.
func (m *FileShareManager) IsProtectedGroupBinding(binding *auth.GroupBinding) bool {
	// Only bindings on file share buckets can be protected
	if !strings.HasPrefix(binding.BucketScope, FileShareBucketPrefix) {
		return false
	}

	// Check if this file share exists
	shareName := strings.TrimPrefix(binding.BucketScope, FileShareBucketPrefix)
	share := m.Get(shareName)
	if share == nil {
		return false
	}

	// The "everyone" group binding is protected
	return binding.GroupName == auth.GroupEveryone
}

// PurgeOrphanedFileShareBuckets deletes file share buckets (fs+*) that no longer
// have a corresponding share. This handles the case where a share is deleted on
// one coordinator but the bucket directory was only removed locally — replicas
// still have the bucket and its objects, preventing GC from cleaning up chunks.
// Reloads share config from the system store first to pick up cross-coordinator deletions.
func (m *FileShareManager) PurgeOrphanedFileShareBuckets(ctx context.Context) int {
	// Reload from system store to pick up share deletions from other coordinators.
	// Only overwrite if we got a non-nil result — LoadFileShares returns (nil, nil)
	// when the object doesn't exist (e.g., system bucket hasn't been replicated yet).
	// Without this nil check, m.shares gets wiped and ALL fs+ buckets past the
	// grace period are purged, causing file share data loss.
	if m.systemStore != nil {
		if shares, err := m.systemStore.LoadFileShares(ctx); err == nil && shares != nil {
			m.mu.Lock()
			m.shares = shares
			m.mu.Unlock()
		}
	}

	// Build set of active share bucket names
	m.mu.RLock()
	activeShareBuckets := make(map[string]struct{}, len(m.shares))
	for _, s := range m.shares {
		activeShareBuckets[FileShareBucketPrefix+s.Name] = struct{}{}
	}
	m.mu.RUnlock()

	// List all buckets and find orphaned file share buckets
	buckets, err := m.store.ListBuckets(ctx)
	if err != nil {
		return 0
	}

	purged := 0
	for _, bucket := range buckets {
		if !strings.HasPrefix(bucket.Name, FileShareBucketPrefix) {
			continue
		}
		if _, active := activeShareBuckets[bucket.Name]; active {
			continue
		}
		// Bucket has fs+ prefix but no corresponding share — orphaned.
		// Skip young buckets: in a multi-coordinator setup, object replication
		// can create the bucket before the share config is replicated.
		if time.Since(bucket.CreatedAt) < orphanedBucketGracePeriod {
			log.Debug().Str("bucket", bucket.Name).
				Dur("age", time.Since(bucket.CreatedAt)).
				Msg("skipping young orphaned file share bucket (grace period)")
			continue
		}
		log.Info().Str("bucket", bucket.Name).Msg("purging orphaned file share bucket")
		if err := m.store.ForceDeleteBucket(ctx, bucket.Name); err != nil {
			log.Warn().Err(err).Str("bucket", bucket.Name).Msg("failed to purge orphaned file share bucket")
		} else {
			purged++
		}
	}
	return purged
}

// PurgeExpiredShareContents purges all objects in expired file shares.
// Expired share content doesn't need a recycle bin — the share itself has expired.
// Returns the number of objects purged.
func (m *FileShareManager) PurgeExpiredShareContents(ctx context.Context) int {
	m.mu.RLock()
	expiredShares := make([]*FileShare, 0)
	for _, s := range m.shares {
		if s.IsExpired() {
			expiredShares = append(expiredShares, s)
		}
	}
	m.mu.RUnlock()

	purgedCount := 0
	for _, share := range expiredShares {
		bucketName := FileShareBucketPrefix + share.Name

		// Paginate through all objects in the bucket
		marker := ""
		for {
			objects, isTruncated, nextMarker, err := m.store.ListObjects(ctx, bucketName, "", marker, 1000)
			if err != nil {
				break
			}

			for _, obj := range objects {
				if err := m.store.PurgeObject(ctx, bucketName, obj.Key); err == nil {
					purgedCount++
				}
			}

			if !isTruncated {
				break
			}
			marker = nextMarker
		}
	}

	return purgedCount
}
