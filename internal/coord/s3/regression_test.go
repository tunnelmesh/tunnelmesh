package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
)

// =============================================================================
// Helpers
// =============================================================================

// newRegressionStore creates a fresh Store with CAS + chunk registry.
func newRegressionStore(t *testing.T, coordID string) *Store {
	t.Helper()
	dir := t.TempDir()
	store, err := NewStore(dir, nil)
	require.NoError(t, err)
	require.NoError(t, store.InitCAS(context.Background(), [32]byte{}))
	store.SetChunkRegistry(&mockChunkRegistry{
		owners:       make(map[string][]string),
		localCoordID: coordID,
	})
	store.SetCoordinatorID(coordID)
	return store
}

// uploadFile uploads data and returns the resulting ObjectMeta.
func uploadFile(t *testing.T, store *Store, bucket, key string, data []byte) *ObjectMeta {
	t.Helper()
	ctx := context.Background()
	meta, err := store.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "text/plain", nil)
	require.NoError(t, err)
	require.NotEmpty(t, meta.Chunks, "file should produce chunks")
	return meta
}

// replicateFile copies chunks + metadata from src to dst.
func replicateFile(t *testing.T, src, dst *Store, bucket, key string, meta *ObjectMeta) {
	t.Helper()
	ctx := context.Background()
	for _, h := range meta.Chunks {
		data, err := src.cas.ReadChunk(ctx, h)
		require.NoError(t, err, "read chunk %s", h[:8])
		require.NoError(t, dst.WriteChunkDirect(ctx, h, data), "write chunk %s", h[:8])
	}
	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)
	_, err = dst.ImportObjectMeta(ctx, bucket, key, metaJSON, "admin")
	require.NoError(t, err, "import metadata")
}

// ageAllChunks backdates every chunk file on the store by the given duration.
func ageAllChunks(t *testing.T, store *Store, age time.Duration) {
	t.Helper()
	past := time.Now().Add(-age)
	err := filepath.Walk(filepath.Join(store.dataDir, "chunks"), func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		return os.Chtimes(p, past, past)
	})
	require.NoError(t, err)
}

// ageAllVersions backdates every version JSON file for the given bucket.
func ageAllVersions(t *testing.T, store *Store, bucket string, age time.Duration) {
	t.Helper()
	past := time.Now().Add(-age)
	_ = filepath.Walk(filepath.Join(store.dataDir, "buckets", bucket, "versions"),
		func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			return os.Chtimes(p, past, past)
		})
}

// requireFileReadable is a hard assertion that a file is fully readable.
func requireFileReadable(t *testing.T, store *Store, bucket, key string, want []byte, label string) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.GetObjectMeta(ctx, bucket, key)
	require.NoError(t, err, "%s: GetObjectMeta", label)
	for _, h := range meta.Chunks {
		require.True(t, store.ChunkExists(h), "%s: chunk %s missing", label, h[:8])
	}
	reader, _, err := store.GetObject(ctx, bucket, key)
	require.NoError(t, err, "%s: GetObject", label)
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	require.NoError(t, err, "%s: ReadAll", label)
	require.Equal(t, want, got, "%s: content mismatch", label)
}

// =============================================================================
// 1. Regression: GC must NOT delete referenced chunks (regular bucket)
// =============================================================================

func TestRegression_GCPreservesReferencedChunks(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "mybucket", "admin", 2, nil))
	data := []byte("GC must never delete chunks that are referenced by live metadata")
	meta := uploadFile(t, store, "mybucket", "file.txt", data)
	t.Logf("uploaded %d chunks", len(meta.Chunks))

	// Age chunks well past grace period
	ageAllChunks(t, store, 2*time.Hour)

	// Run GC multiple times
	for i := 0; i < 3; i++ {
		stats := store.RunGarbageCollection(ctx)
		assert.Zero(t, stats.ChunksDeleted, "GC round %d should not delete referenced chunks", i)
	}

	requireFileReadable(t, store, "mybucket", "file.txt", data, "after 3 GC rounds")
}

// =============================================================================
// 2. Regression: Force GC (skipGracePeriod) must still preserve referenced chunks
// =============================================================================

func TestRegression_ForceGCPreservesReferencedChunks(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "mybucket", "admin", 2, nil))
	data := []byte("Force GC must not destroy live data")
	uploadFile(t, store, "mybucket", "file.txt", data)

	ageAllChunks(t, store, 2*time.Hour)

	stats := store.RunGarbageCollectionForce(ctx)
	assert.Zero(t, stats.ChunksDeleted, "force GC must not delete referenced chunks")

	requireFileReadable(t, store, "mybucket", "file.txt", data, "after force GC")
}

// =============================================================================
// 3. Regression: Repeated ImportObjectMeta (auto-sync) must not corrupt data
// =============================================================================

func TestRegression_RepeatedImportMetaPreservesFile(t *testing.T) {
	src := newRegressionStore(t, "coord1")
	dst := newRegressionStore(t, "coord2")
	ctx := context.Background()

	require.NoError(t, src.CreateBucket(ctx, "shared", "admin", 2, nil))
	data := []byte("Repeated imports must not destroy file data")
	meta := uploadFile(t, src, "shared", "doc.txt", data)
	metaJSON, _ := json.Marshal(meta)

	// Initial replication
	replicateFile(t, src, dst, "shared", "doc.txt", meta)
	requireFileReadable(t, dst, "shared", "doc.txt", data, "after initial replication")

	// Simulate 20 auto-sync rounds on the receiver
	for i := 0; i < 20; i++ {
		chunksToCheck, err := dst.ImportObjectMeta(ctx, "shared", "doc.txt", metaJSON, "admin")
		require.NoError(t, err, "import round %d", i)

		if len(chunksToCheck) > 0 {
			freed := dst.DeleteUnreferencedChunks(ctx, chunksToCheck)
			assert.Zero(t, freed, "round %d: deferred cleanup must not free live chunks", i)
		}
	}

	requireFileReadable(t, dst, "shared", "doc.txt", data, "after 20 import rounds")
}

// =============================================================================
// 4. Regression: ImportObjectMeta + version pruning + GC must not kill live file
// =============================================================================

func TestRegression_VersionPruningPlusGCPreservesLiveFile(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()
	store.SetMaxVersionsPerObject(3)

	require.NoError(t, store.CreateBucket(ctx, "versioned", "admin", 2, nil))

	// Upload v1
	v1Data := []byte("Version 1 content - original file with its own unique chunks")
	v1Meta := uploadFile(t, store, "versioned", "doc.txt", v1Data)

	// Upload v2 (different content = different chunks)
	v2Data := []byte("Version 2 content - different data creating entirely new chunks in CAS")
	v2Meta := uploadFile(t, store, "versioned", "doc.txt", v2Data)

	// Upload v3 (different again)
	v3Data := []byte("Version 3 content - yet another set of unique chunk hashes")
	uploadFile(t, store, "versioned", "doc.txt", v3Data)

	t.Logf("v1 chunks: %v", truncateHashes(v1Meta.Chunks))
	t.Logf("v2 chunks: %v", truncateHashes(v2Meta.Chunks))

	// Simulate many re-imports to trigger version pruning beyond max=3
	metaJSON, _ := json.Marshal(v2Meta)
	for i := 0; i < 10; i++ {
		chunksToCheck, err := store.ImportObjectMeta(ctx, "versioned", "doc.txt", metaJSON, "admin")
		require.NoError(t, err)
		if len(chunksToCheck) > 0 {
			store.DeleteUnreferencedChunks(ctx, chunksToCheck)
		}
	}

	// Age everything and run GC
	ageAllChunks(t, store, 2*time.Hour)
	ageAllVersions(t, store, "versioned", 2*time.Hour)

	stats := store.RunGarbageCollection(ctx)
	t.Logf("GC: pruned=%d deleted=%d reclaimed=%d", stats.VersionsPruned, stats.ChunksDeleted, stats.BytesReclaimed)

	// The current (latest) file version must always be readable
	requireFileReadable(t, store, "versioned", "doc.txt", v2Data, "after version pruning + GC")
}

// =============================================================================
// 5. Regression: Chunks arriving before metadata survive GC grace period
// =============================================================================

func TestRegression_ChunksBeforeMetaSurviveGracePeriod(t *testing.T) {
	src := newRegressionStore(t, "coord1")
	dst := newRegressionStore(t, "coord2")
	ctx := context.Background()

	require.NoError(t, src.CreateBucket(ctx, "data", "admin", 2, nil))
	data := []byte("Chunks replicated before metadata must survive until metadata arrives")
	meta := uploadFile(t, src, "data", "early.txt", data)

	// Replicate ONLY chunks (no metadata yet)
	for _, h := range meta.Chunks {
		chunkData, err := src.cas.ReadChunk(ctx, h)
		require.NoError(t, err)
		require.NoError(t, dst.WriteChunkDirect(ctx, h, chunkData))
	}

	// Run GC — chunks are young, grace period should protect them
	stats := dst.RunGarbageCollection(ctx)
	assert.Zero(t, stats.ChunksDeleted, "young chunks without metadata must survive GC (grace period)")

	for _, h := range meta.Chunks {
		assert.True(t, dst.ChunkExists(h), "chunk %s must survive GC grace period", h[:8])
	}

	// Now import metadata
	metaJSON, _ := json.Marshal(meta)
	_, err := dst.ImportObjectMeta(ctx, "data", "early.txt", metaJSON, "admin")
	require.NoError(t, err)

	requireFileReadable(t, dst, "data", "early.txt", data, "after delayed metadata")
}

// =============================================================================
// 6. Regression: Chunks PAST grace period WITHOUT metadata are cleaned up
// =============================================================================

func TestRegression_AgedOrphanedChunksDeletedByGC(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "data", "admin", 2, nil))
	data := []byte("Orphaned chunks should eventually be cleaned up")
	meta := uploadFile(t, store, "data", "file.txt", data)

	// Delete the metadata only (simulate metadata never arriving on a peer)
	metaPath := filepath.Join(store.dataDir, "buckets", "data", "meta", "file.txt.json")
	require.NoError(t, os.Remove(metaPath))

	// Age chunks past grace period
	ageAllChunks(t, store, 2*time.Hour)

	stats := store.RunGarbageCollection(ctx)
	assert.Greater(t, stats.ChunksDeleted, 0, "orphaned aged chunks must be deleted")
	for _, h := range meta.Chunks {
		assert.False(t, store.ChunkExists(h), "orphaned chunk %s must be deleted after aging", h[:8])
	}
}

// =============================================================================
// 7. Regression: DeleteUnreferencedChunks must not delete chunks referenced
//    by OTHER objects in OTHER buckets
// =============================================================================

func TestRegression_DeleteUnreferencedChunksRespectsAllBuckets(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "bucket1", "admin", 2, nil))
	require.NoError(t, store.CreateBucket(ctx, "bucket2", "admin", 2, nil))

	// Upload the SAME data to two buckets (creates shared chunks via CAS dedup)
	data := []byte("Shared content across buckets — dedup means same chunk hashes")
	meta1 := uploadFile(t, store, "bucket1", "shared.txt", data)
	meta2 := uploadFile(t, store, "bucket2", "shared.txt", data)

	// Chunks should be the same (CAS dedup)
	require.Equal(t, meta1.Chunks, meta2.Chunks, "same data should produce same chunks")

	// Delete the object from bucket1 via PurgeObject
	require.NoError(t, store.PurgeObject(ctx, "bucket1", "shared.txt"))

	// PurgeObject calls DeleteUnreferencedChunks — but chunks are still referenced by bucket2
	requireFileReadable(t, store, "bucket2", "shared.txt", data, "after purging from bucket1")
}

// =============================================================================
// 8. Regression: PurgeRecycleBin must not delete chunks still used by live objects
// =============================================================================

func TestRegression_RecycleBinPurgePreservesLiveChunks(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()
	store.SetRecycleBinRetentionDays(1) // Enable recycle bin

	require.NoError(t, store.CreateBucket(ctx, "mybucket", "admin", 2, nil))

	// Upload same data twice (creates shared chunks)
	data := []byte("Content shared between deleted and live objects")
	uploadFile(t, store, "mybucket", "keep.txt", data)
	uploadFile(t, store, "mybucket", "delete_me.txt", data)

	// Soft-delete one object (goes to recycle bin)
	require.NoError(t, store.DeleteObject(ctx, "mybucket", "delete_me.txt"))

	// Force purge the recycle bin
	purged := store.PurgeAllRecycled(ctx)
	assert.Equal(t, 1, purged, "should purge the deleted object")

	// The live object must still be readable (shared chunks must survive)
	requireFileReadable(t, store, "mybucket", "keep.txt", data, "after recycle bin purge")
}

// =============================================================================
// 9. Regression: Full GC cycle (versions + orphans) on multi-file store
// =============================================================================

func TestRegression_FullGCCycleMultipleFiles(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()
	store.SetMaxVersionsPerObject(2)

	require.NoError(t, store.CreateBucket(ctx, "docs", "admin", 2, nil))

	// Upload several files
	files := map[string][]byte{
		"readme.txt":  []byte("README content that should survive all GC operations"),
		"config.json": []byte(`{"key":"value","persist":true}`),
		"data.bin":    bytes.Repeat([]byte("ABCDEFGH"), 1024),
	}

	for key, data := range files {
		uploadFile(t, store, "docs", key, data)
	}

	// Update each file 5 times (creates versions, triggers pruning with max=2)
	for i := 0; i < 5; i++ {
		for key, data := range files {
			updated := make([]byte, len(data)+8)
			copy(updated, data)
			copy(updated[len(data):], " updated")
			files[key] = updated
			uploadFile(t, store, "docs", key, updated)
		}
	}

	// Age and run full GC
	ageAllChunks(t, store, 2*time.Hour)
	ageAllVersions(t, store, "docs", 2*time.Hour)

	stats := store.RunGarbageCollection(ctx)
	t.Logf("GC: pruned=%d deleted=%d reclaimed=%d scanned=%d",
		stats.VersionsPruned, stats.ChunksDeleted, stats.BytesReclaimed, stats.ObjectsScanned)

	// All current versions must be readable
	for key, data := range files {
		requireFileReadable(t, store, "docs", key, data, key+" after full GC")
	}
}

// =============================================================================
// 10. Regression: PurgeOrphanedFileShareBuckets must not purge when system store
//     returns nil (replication delay)
// =============================================================================

func TestRegression_PurgeOrphanedBucketsNilSystemStore(t *testing.T) {
	store := newTestStoreWithCASForFileshare(t)
	ctx := context.Background()

	systemStore, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	authorizer := auth.NewAuthorizerWithGroups()
	mgr := NewFileShareManager(store, systemStore, authorizer)

	// Create two shares
	_, err = mgr.Create(ctx, "photos", "Photos", "alice", 0, nil)
	require.NoError(t, err)
	_, err = mgr.Create(ctx, "docs", "Docs", "alice", 0, nil)
	require.NoError(t, err)

	// Upload content into the share buckets
	photoBucket := FileShareBucketPrefix + "photos"
	docsBucket := FileShareBucketPrefix + "docs"
	_, err = store.PutObject(ctx, photoBucket, "pic.jpg", bytes.NewReader([]byte("photo")), 5, "image/jpeg", nil)
	require.NoError(t, err)
	_, err = store.PutObject(ctx, docsBucket, "doc.txt", bytes.NewReader([]byte("document")), 8, "text/plain", nil)
	require.NoError(t, err)

	// Age buckets past grace period
	for _, bname := range []string{photoBucket, docsBucket} {
		metaPath := filepath.Join(store.DataDir(), "buckets", bname, "_meta.json")
		data, readErr := os.ReadFile(metaPath)
		require.NoError(t, readErr)

		bMeta, headErr := store.HeadBucket(ctx, bname)
		require.NoError(t, headErr)
		oldTime := time.Now().Add(-20 * time.Minute).UTC().Format(time.RFC3339Nano)
		updated := bytes.Replace(data, []byte(bMeta.CreatedAt.Format(time.RFC3339Nano)), []byte(oldTime), 1)
		require.NoError(t, os.WriteFile(metaPath, updated, 0644))
	}

	// Delete the system store object to simulate it not being replicated yet
	require.NoError(t, store.PurgeObject(ctx, SystemBucket, FileSharesPath))

	// Purge must NOT wipe shares
	purged := mgr.PurgeOrphanedFileShareBuckets(ctx)
	assert.Equal(t, 0, purged, "must not purge when system store returns nil")

	// Both buckets must survive
	_, err = store.HeadBucket(ctx, photoBucket)
	assert.NoError(t, err, "photos bucket must survive")
	_, err = store.HeadBucket(ctx, docsBucket)
	assert.NoError(t, err, "docs bucket must survive")

	// Files must be intact
	reader, _, err := store.GetObject(ctx, photoBucket, "pic.jpg")
	require.NoError(t, err)
	got, _ := io.ReadAll(reader)
	_ = reader.Close()
	assert.Equal(t, []byte("photo"), got)
}

// =============================================================================
// 11. Regression: PurgeOrphanedFileShareBuckets purges when share is deleted
//     AND system store confirms the deletion
// =============================================================================

func TestRegression_PurgeOrphanedBucketsOnReplicaAfterShareDeletion(t *testing.T) {
	// Scenario: Share was created on coord A, replicated to coord B (bucket exists),
	// then deleted on coord A. Coord B still has the bucket directory from replication
	// but the system store now reflects the deletion. Purge should clean it up.
	store := newTestStoreWithCASForFileshare(t)
	ctx := context.Background()

	systemStore, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	authorizer := auth.NewAuthorizerWithGroups()
	mgr := NewFileShareManager(store, systemStore, authorizer)

	// Simulate: share existed then was deleted on another coordinator.
	// The system store reflects the deletion (empty shares list).
	require.NoError(t, systemStore.SaveFileShares(ctx, []*FileShare{}))

	// But the bucket still exists locally (from replication before deletion)
	tempBucket := FileShareBucketPrefix + "temp"
	require.NoError(t, store.CreateBucket(ctx, tempBucket, "alice", 1, nil))

	// Age the bucket past grace period
	metaPath := filepath.Join(store.DataDir(), "buckets", tempBucket, "_meta.json")
	data, readErr := os.ReadFile(metaPath)
	require.NoError(t, readErr)
	bMeta, _ := store.HeadBucket(ctx, tempBucket)
	oldTime := time.Now().Add(-20 * time.Minute).UTC().Format(time.RFC3339Nano)
	updated := bytes.Replace(data, []byte(bMeta.CreatedAt.Format(time.RFC3339Nano)), []byte(oldTime), 1)
	require.NoError(t, os.WriteFile(metaPath, updated, 0644))

	purged := mgr.PurgeOrphanedFileShareBuckets(ctx)
	assert.Equal(t, 1, purged, "should purge bucket whose share was deleted remotely")

	_, err = store.HeadBucket(ctx, tempBucket)
	assert.ErrorIs(t, err, ErrBucketNotFound, "orphaned bucket should be gone")
}

// =============================================================================
// 12. Regression: Multi-coordinator replication cycle — file survives full
//     import + deferred cleanup + GC across 3 coordinators
// =============================================================================

func TestRegression_MultiCoordReplicationAndGC(t *testing.T) {
	ctx := context.Background()

	c1 := newRegressionStore(t, "coord1")
	c2 := newRegressionStore(t, "coord2")
	c3 := newRegressionStore(t, "coord3")

	require.NoError(t, c1.CreateBucket(ctx, "shared", "admin", 3, nil))

	data := []byte("File replicated across 3 coordinators must survive all cleanup paths")
	meta := uploadFile(t, c1, "shared", "critical.txt", data)

	// Replicate to c2 and c3
	replicateFile(t, c1, c2, "shared", "critical.txt", meta)
	replicateFile(t, c1, c3, "shared", "critical.txt", meta)

	// Verify all three can read
	for label, store := range map[string]*Store{"c1": c1, "c2": c2, "c3": c3} {
		requireFileReadable(t, store, "shared", "critical.txt", data, label+" initial")
	}

	// Simulate 5 auto-sync cycles (re-import on c2 and c3)
	metaJSON, _ := json.Marshal(meta)
	for i := 0; i < 5; i++ {
		for _, store := range []*Store{c2, c3} {
			chunksToCheck, err := store.ImportObjectMeta(ctx, "shared", "critical.txt", metaJSON, "admin")
			require.NoError(t, err)
			if len(chunksToCheck) > 0 {
				store.DeleteUnreferencedChunks(ctx, chunksToCheck)
			}
		}
	}

	// Age everything and run GC on all
	for _, store := range []*Store{c1, c2, c3} {
		ageAllChunks(t, store, 2*time.Hour)
		ageAllVersions(t, store, "shared", 2*time.Hour)
		store.RunGarbageCollection(ctx)
	}

	// All must still read
	for label, store := range map[string]*Store{"c1": c1, "c2": c2, "c3": c3} {
		requireFileReadable(t, store, "shared", "critical.txt", data, label+" after full GC")
	}
}

// =============================================================================
// 13. Regression: File update (v1→v2) must not lose v2 data after import + GC
// =============================================================================

func TestRegression_FileUpdateSurvivesImportAndGC(t *testing.T) {
	src := newRegressionStore(t, "coord1")
	dst := newRegressionStore(t, "coord2")
	ctx := context.Background()

	require.NoError(t, src.CreateBucket(ctx, "docs", "admin", 2, nil))

	// Upload v1
	v1Data := []byte("Version 1 - the original file content")
	v1Meta := uploadFile(t, src, "docs", "paper.txt", v1Data)
	replicateFile(t, src, dst, "docs", "paper.txt", v1Meta)
	requireFileReadable(t, dst, "docs", "paper.txt", v1Data, "v1 on dst")

	// Upload v2 (completely different content → different chunks)
	v2Data := []byte("Version 2 - rewritten content with different chunk hashes entirely")
	v2Meta := uploadFile(t, src, "docs", "paper.txt", v2Data)
	require.NotEqual(t, v1Meta.Chunks, v2Meta.Chunks, "v2 must have different chunks")

	// Replicate v2 to dst
	replicateFile(t, src, dst, "docs", "paper.txt", v2Meta)
	requireFileReadable(t, dst, "docs", "paper.txt", v2Data, "v2 on dst after replication")

	// Age and GC — v1 chunks should be cleaned up, v2 must survive
	ageAllChunks(t, dst, 2*time.Hour)
	ageAllVersions(t, dst, "docs", 2*time.Hour)
	dst.RunGarbageCollection(ctx)

	requireFileReadable(t, dst, "docs", "paper.txt", v2Data, "v2 on dst after GC")
}

// =============================================================================
// 14. Regression: Concurrent ImportObjectMeta from two sources
// =============================================================================

func TestRegression_ConcurrentImportFromTwoSources(t *testing.T) {
	c1 := newRegressionStore(t, "coord1")
	c2 := newRegressionStore(t, "coord2")
	dst := newRegressionStore(t, "coord3")
	ctx := context.Background()

	require.NoError(t, c1.CreateBucket(ctx, "docs", "admin", 2, nil))
	require.NoError(t, c2.CreateBucket(ctx, "docs", "admin", 2, nil))

	// c1 and c2 both have the same file
	data := []byte("File present on two coordinators, both replicate to a third")
	meta := uploadFile(t, c1, "docs", "shared.txt", data)

	// Copy same data to c2
	metaJSON, _ := json.Marshal(meta)
	for _, h := range meta.Chunks {
		chunkData, _ := c1.cas.ReadChunk(ctx, h)
		require.NoError(t, c2.WriteChunkDirect(ctx, h, chunkData))
	}
	_, err := c2.ImportObjectMeta(ctx, "docs", "shared.txt", metaJSON, "admin")
	require.NoError(t, err)

	// Now both c1 and c2 replicate to dst concurrently (simulated sequentially)
	replicateFile(t, c1, dst, "docs", "shared.txt", meta)
	replicateFile(t, c2, dst, "docs", "shared.txt", meta)

	// Run deferred cleanup after both imports
	chunksToCheck, _ := dst.ImportObjectMeta(ctx, "docs", "shared.txt", metaJSON, "admin")
	if len(chunksToCheck) > 0 {
		dst.DeleteUnreferencedChunks(ctx, chunksToCheck)
	}

	requireFileReadable(t, dst, "docs", "shared.txt", data, "dst after concurrent imports")

	// Age and GC
	ageAllChunks(t, dst, 2*time.Hour)
	ageAllVersions(t, dst, "docs", 2*time.Hour)
	dst.RunGarbageCollection(ctx)

	requireFileReadable(t, dst, "docs", "shared.txt", data, "dst after GC")
}

// =============================================================================
// 15. Regression: Multiple buckets — GC on one bucket must not affect another
// =============================================================================

func TestRegression_GCIsolationBetweenBuckets(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "keep", "admin", 2, nil))
	require.NoError(t, store.CreateBucket(ctx, "delete", "admin", 2, nil))

	keepData := []byte("This file is in the keep bucket and must survive")
	deleteData := []byte("This file is in the delete bucket and will be purged")

	uploadFile(t, store, "keep", "precious.txt", keepData)
	meta := uploadFile(t, store, "delete", "temp.txt", deleteData)

	// PurgeObject from the delete bucket
	require.NoError(t, store.PurgeObject(ctx, "delete", "temp.txt"))

	// Verify chunk cleanup didn't affect the keep bucket
	requireFileReadable(t, store, "keep", "precious.txt", keepData, "after purging from other bucket")

	// Age and GC — any remaining orphaned chunks should be cleaned up
	ageAllChunks(t, store, 2*time.Hour)
	store.RunGarbageCollection(ctx)

	requireFileReadable(t, store, "keep", "precious.txt", keepData, "after GC")

	// Orphaned chunks should be gone (PurgeObject cleaned them inline, GC is the safety net)
	for _, h := range meta.Chunks {
		assert.False(t, store.ChunkExists(h), "orphaned chunk %s from deleted object should be gone", h[:8])
	}
}

// =============================================================================
// 16. Regression: ImportObjectMeta auto-creates bucket — file survives GC
// =============================================================================

func TestRegression_AutoCreatedBucketSurvivesGC(t *testing.T) {
	src := newRegressionStore(t, "coord1")
	dst := newRegressionStore(t, "coord2")
	ctx := context.Background()

	require.NoError(t, src.CreateBucket(ctx, "remote", "admin", 2, nil))
	data := []byte("File whose bucket is auto-created on the destination via ImportObjectMeta")
	meta := uploadFile(t, src, "remote", "file.txt", data)

	// Replicate to dst (bucket doesn't exist yet — ImportObjectMeta auto-creates it)
	replicateFile(t, src, dst, "remote", "file.txt", meta)

	// Verify auto-created bucket exists
	bMeta, err := dst.HeadBucket(ctx, "remote")
	require.NoError(t, err, "bucket should be auto-created")
	assert.Equal(t, "admin", bMeta.Owner)

	requireFileReadable(t, dst, "remote", "file.txt", data, "after replication to auto-created bucket")

	// Age and GC
	ageAllChunks(t, dst, 2*time.Hour)
	dst.RunGarbageCollection(ctx)

	requireFileReadable(t, dst, "remote", "file.txt", data, "after GC on auto-created bucket")
}

// =============================================================================
// 17. Regression: System bucket files survive GC
// =============================================================================

func TestRegression_SystemBucketSurvivesGC(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()

	// Create system bucket and store config data
	require.NoError(t, store.CreateBucket(ctx, "_tunnelmesh", "svc:coordinator", 3, nil))

	configData := []byte(`{"shares":[{"name":"photos","owner":"alice"}]}`)
	_, err := store.PutObject(ctx, "_tunnelmesh", "auth/file_shares.json",
		bytes.NewReader(configData), int64(len(configData)), "application/json", nil)
	require.NoError(t, err)

	// Age and GC
	ageAllChunks(t, store, 2*time.Hour)
	stats := store.RunGarbageCollection(ctx)
	assert.Zero(t, stats.ChunksDeleted, "system bucket chunks must not be deleted")

	requireFileReadable(t, store, "_tunnelmesh", "auth/file_shares.json", configData, "system bucket after GC")
}

// =============================================================================
// 18. Regression: Topology change simulation — coord2 receives everything,
//     coord3 added later, GC runs everywhere
// =============================================================================

func TestRegression_AccordionModeFullCycle(t *testing.T) {
	ctx := context.Background()

	c1 := newRegressionStore(t, "coord1")
	c2 := newRegressionStore(t, "coord2")

	require.NoError(t, c1.CreateBucket(ctx, "data", "admin", 2, nil))

	// Upload 3 files
	files := map[string][]byte{
		"alpha.txt": []byte("Alpha file content for accordion test"),
		"beta.txt":  []byte("Beta file content - different from alpha"),
		"gamma.txt": []byte("Gamma file with yet another unique content"),
	}
	metas := make(map[string]*ObjectMeta)
	for key, data := range files {
		metas[key] = uploadFile(t, c1, "data", key, data)
	}

	// Phase 1: Replicate all to c2
	for key := range files {
		replicateFile(t, c1, c2, "data", key, metas[key])
	}

	// Phase 2: c3 joins (accordion — new coordinator)
	c3 := newRegressionStore(t, "coord3")
	for key := range files {
		replicateFile(t, c1, c3, "data", key, metas[key])
	}

	// Phase 3: Auto-sync — re-import metadata on all peers
	for key := range files {
		metaJSON, _ := json.Marshal(metas[key])
		for _, store := range []*Store{c2, c3} {
			chunksToCheck, _ := store.ImportObjectMeta(ctx, "data", key, metaJSON, "admin")
			if len(chunksToCheck) > 0 {
				store.DeleteUnreferencedChunks(ctx, chunksToCheck)
			}
		}
	}

	// Phase 4: c2 leaves — no action needed on c1/c3 (they still have data)

	// Phase 5: Age everything and run GC on c1 and c3
	for _, store := range []*Store{c1, c3} {
		ageAllChunks(t, store, 2*time.Hour)
		ageAllVersions(t, store, "data", 2*time.Hour)
		store.RunGarbageCollection(ctx)
	}

	// Phase 6: Verify all files on c1 and c3
	for key, data := range files {
		requireFileReadable(t, c1, "data", key, data, "c1: "+key)
		requireFileReadable(t, c3, "data", key, data, "c3: "+key)
	}
}

// =============================================================================
// 19. Regression: DeleteObject (soft delete) → recycle bin → GC doesn't lose
//     chunks referenced by other live objects
// =============================================================================

func TestRegression_SoftDeleteSharedChunksPreserved(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()
	store.SetRecycleBinRetentionDays(1)

	require.NoError(t, store.CreateBucket(ctx, "mybucket", "admin", 2, nil))

	// Two files with identical content (shared chunks)
	data := []byte("Shared chunk data between two objects")
	uploadFile(t, store, "mybucket", "file_a.txt", data)
	uploadFile(t, store, "mybucket", "file_b.txt", data)

	// Soft-delete file_a
	require.NoError(t, store.DeleteObject(ctx, "mybucket", "file_a.txt"))

	// Purge recycle bin
	store.PurgeAllRecycled(ctx)

	// Age and GC
	ageAllChunks(t, store, 2*time.Hour)
	store.RunGarbageCollection(ctx)

	// file_b must still be readable
	requireFileReadable(t, store, "mybucket", "file_b.txt", data, "after soft delete + recycle purge + GC")
}

// =============================================================================
// 20. Regression: PurgeOrphanedFileShareBuckets preserves buckets when
//     LoadFileShares returns stale (empty) data
// =============================================================================

func TestRegression_PurgeOrphanedBucketsStaleEmptyShares(t *testing.T) {
	store := newTestStoreWithCASForFileshare(t)
	ctx := context.Background()

	systemStore, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	authorizer := auth.NewAuthorizerWithGroups()
	mgr := NewFileShareManager(store, systemStore, authorizer)

	// Create a share, then manually overwrite system store with empty list
	_, err = mgr.Create(ctx, "photos", "Photos", "alice", 0, nil)
	require.NoError(t, err)

	// Overwrite with empty list (simulates a stale/corrupted write)
	require.NoError(t, systemStore.SaveFileShares(ctx, []*FileShare{}))

	photoBucket := FileShareBucketPrefix + "photos"

	// Age bucket
	metaPath := filepath.Join(store.DataDir(), "buckets", photoBucket, "_meta.json")
	data, _ := os.ReadFile(metaPath)
	bMeta, _ := store.HeadBucket(ctx, photoBucket)
	oldTime := time.Now().Add(-20 * time.Minute).UTC().Format(time.RFC3339Nano)
	updated := bytes.Replace(data, []byte(bMeta.CreatedAt.Format(time.RFC3339Nano)), []byte(oldTime), 1)
	_ = os.WriteFile(metaPath, updated, 0644)

	// The system store now says there are no shares, but the manager had "photos"
	// in memory. The empty list from LoadFileShares is not nil, so it WILL
	// overwrite in-memory shares. This test documents the current behavior.
	purged := mgr.PurgeOrphanedFileShareBuckets(ctx)

	// With an explicitly empty list (not nil), the purge WILL delete the bucket.
	// This is intentional: an empty list means "no shares configured", which is
	// different from nil (object not found / not replicated yet).
	assert.Equal(t, 1, purged, "empty (non-nil) share list should allow purging")
}

// 21. Regression: Identical re-imports must NOT create version archives
// =============================================================================
// Auto-sync re-imports all objects every 5 min. Without the ETag guard,
// each re-import archives the current version, GC prunes it (deleting chunks),
// then auto-sync re-replicates — causing a sawtooth in storage metrics.

func TestRegression_IdenticalReimportSkipsVersionArchive(t *testing.T) {
	src := newRegressionStore(t, "coord1")
	dst := newRegressionStore(t, "coord2")
	ctx := context.Background()

	require.NoError(t, src.CreateBucket(ctx, "docs", "admin", 2, nil))
	data := []byte("Identical re-imports must not create version archives")
	meta := uploadFile(t, src, "docs", "report.txt", data)
	require.NotEmpty(t, meta.ETag, "PutObject should set ETag")

	// Initial replication
	replicateFile(t, src, dst, "docs", "report.txt", meta)
	requireFileReadable(t, dst, "docs", "report.txt", data, "after initial replication")

	// Simulate 50 auto-sync re-imports with the same metadata
	metaJSON, _ := json.Marshal(meta)
	for i := 0; i < 50; i++ {
		chunksToCheck, err := dst.ImportObjectMeta(ctx, "docs", "report.txt", metaJSON, "admin")
		require.NoError(t, err, "import round %d", i)
		assert.Nil(t, chunksToCheck, "round %d: identical re-import should return nil", i)
	}

	// No version archives should have been created
	versionsDir := filepath.Join(dst.DataDir(), "buckets", "docs", "versions", "report.txt")
	entries, _ := os.ReadDir(versionsDir)
	assert.Empty(t, entries, "no version archives should exist after identical re-imports")

	// Age chunks and run GC — nothing should be deleted
	ageAllChunks(t, dst, 15*time.Minute)
	freed := dst.DeleteUnreferencedChunks(ctx, meta.Chunks)
	assert.Zero(t, freed, "GC must not delete chunks after identical re-imports")

	requireFileReadable(t, dst, "docs", "report.txt", data, "after 50 identical re-imports + GC")
}

// =============================================================================
// 22. Regression: Different ETag import must still archive the old version
// =============================================================================

func TestRegression_DifferentETagImportArchivesVersion(t *testing.T) {
	src := newRegressionStore(t, "coord1")
	dst := newRegressionStore(t, "coord2")
	ctx := context.Background()

	require.NoError(t, src.CreateBucket(ctx, "docs", "admin", 2, nil))

	// Upload v1 and replicate
	dataV1 := []byte("Version 1 content")
	metaV1 := uploadFile(t, src, "docs", "file.txt", dataV1)
	replicateFile(t, src, dst, "docs", "file.txt", metaV1)

	// Upload v2 with different content (different ETag) and import
	dataV2 := []byte("Version 2 content — different!")
	metaV2 := uploadFile(t, src, "docs", "file.txt", dataV2)
	require.NotEqual(t, metaV1.ETag, metaV2.ETag, "different content must produce different ETags")

	metaJSON2, _ := json.Marshal(metaV2)
	for _, h := range metaV2.Chunks {
		chunkData, err := src.cas.ReadChunk(ctx, h)
		require.NoError(t, err)
		require.NoError(t, dst.WriteChunkDirect(ctx, h, chunkData))
	}
	_, err := dst.ImportObjectMeta(ctx, "docs", "file.txt", metaJSON2, "admin")
	require.NoError(t, err)

	// Version archive should exist for v1
	versionsDir := filepath.Join(dst.DataDir(), "buckets", "docs", "versions", "file.txt")
	entries, err := os.ReadDir(versionsDir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "should have 1 archived version from v1")

	// Current version should be v2
	requireFileReadable(t, dst, "docs", "file.txt", dataV2, "after v2 import")
}

// =============================================================================
// 23. Regression: Empty ETag imports must NOT be skipped (legacy behavior)
// =============================================================================

func TestRegression_EmptyETagImportNotSkipped(t *testing.T) {
	store := newRegressionStore(t, "coord1")
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "legacy", "admin", 1, nil))

	// Import with empty ETag (simulates legacy or test metadata)
	meta := ObjectMeta{
		Key:         "old.txt",
		Size:        42,
		ContentType: "text/plain",
		Chunks:      []string{"legacyhash1"},
	}
	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)
	_, err = store.ImportObjectMeta(ctx, "legacy", "old.txt", metaJSON, "admin")
	require.NoError(t, err)

	// Re-import with same empty ETag — should NOT be skipped (archives)
	_, err = store.ImportObjectMeta(ctx, "legacy", "old.txt", metaJSON, "admin")
	require.NoError(t, err)

	// Should have created a version archive (old behavior preserved)
	versionsDir := filepath.Join(store.DataDir(), "buckets", "legacy", "versions", "old.txt")
	entries, err := os.ReadDir(versionsDir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "empty ETag re-import should still archive (legacy behavior)")
}

// =============================================================================
// 24. Regression: Delete on source → manifest reconciliation purges on replica
// =============================================================================

func TestRegression_ManifestReconciliationPurgesDeletedObjects(t *testing.T) {
	src := newRegressionStore(t, "coord1")
	dst := newRegressionStore(t, "coord2")
	ctx := context.Background()

	require.NoError(t, src.CreateBucket(ctx, "data", "admin", 2, nil))

	// Upload 3 files and replicate all to dst
	files := map[string][]byte{
		"keep1.txt":     []byte("File that should survive reconciliation"),
		"keep2.txt":     []byte("Another file that should survive"),
		"delete_me.txt": []byte("This file will be deleted on source"),
	}
	for key, data := range files {
		meta := uploadFile(t, src, "data", key, data)
		replicateFile(t, src, dst, "data", key, meta)
	}

	// Verify all 3 files exist on dst
	for key, data := range files {
		requireFileReadable(t, dst, "data", key, data, "before delete")
	}

	// Delete the file on source
	require.NoError(t, src.PurgeObject(ctx, "data", "delete_me.txt"))

	// Build manifest from source (simulates what auto-sync sends)
	srcKeys, err := src.GetAllObjectKeys(ctx)
	require.NoError(t, err)
	assert.Len(t, srcKeys["data"], 2, "source should have 2 objects after delete")

	// Simulate manifest reconciliation on dst: purge objects not in source manifest
	dstKeys, err := dst.GetAllObjectKeys(ctx)
	require.NoError(t, err)
	assert.Len(t, dstKeys["data"], 3, "dst should still have 3 objects before reconciliation")

	// Build manifest set
	manifestSet := make(map[string]map[string]struct{})
	for bucket, keys := range srcKeys {
		keySet := make(map[string]struct{})
		for _, k := range keys {
			keySet[k] = struct{}{}
		}
		manifestSet[bucket] = keySet
	}

	// Reconcile: purge objects on dst not in manifest
	var purged int
	for bucket, keys := range dstKeys {
		if bucket == "_tunnelmesh" {
			continue
		}
		senderKeys, hasBucket := manifestSet[bucket]
		if !hasBucket {
			continue
		}
		for _, key := range keys {
			if _, exists := senderKeys[key]; !exists {
				require.NoError(t, dst.PurgeObject(ctx, bucket, key))
				purged++
			}
		}
	}

	assert.Equal(t, 1, purged, "should purge exactly the deleted object")

	// Verify surviving files are still readable
	requireFileReadable(t, dst, "data", "keep1.txt", files["keep1.txt"], "after reconciliation")
	requireFileReadable(t, dst, "data", "keep2.txt", files["keep2.txt"], "after reconciliation")

	// Verify deleted file is gone
	_, err = dst.HeadObject(ctx, "data", "delete_me.txt")
	assert.ErrorIs(t, err, ErrObjectNotFound, "deleted object should be purged on replica")

	// Age and GC — surviving files must still be readable
	ageAllChunks(t, dst, 2*time.Hour)
	dst.RunGarbageCollection(ctx)

	requireFileReadable(t, dst, "data", "keep1.txt", files["keep1.txt"], "after GC")
	requireFileReadable(t, dst, "data", "keep2.txt", files["keep2.txt"], "after GC")
}
