package s3

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGC_DeletesUnreferencedChunksRegardlessOfRegistry tests that GC deletes
// locally unreferenced chunks even if other coordinators own them in the registry.
// Each coordinator independently manages its own storage; the reference set +
// grace period is sufficient for safety.
func TestGC_DeletesUnreferencedChunksRegardlessOfRegistry(t *testing.T) {
	dir := t.TempDir()

	// Create store with chunk registry
	store, err := NewStore(dir, nil)
	require.NoError(t, err)

	ctx := context.Background()

	var encryptionKey [32]byte
	err = store.InitCAS(ctx, encryptionKey)
	require.NoError(t, err)

	// Create mock registry
	mockReg := &mockChunkRegistry{
		owners:       make(map[string][]string),
		localCoordID: "coord1",
	}
	store.SetChunkRegistry(mockReg)
	store.SetCoordinatorID("coord1")

	// Create an orphaned chunk directly (not referenced by any object)
	chunkData := []byte("Orphaned chunk data for testing")
	chunkHash, _, err := store.cas.WriteChunk(ctx, chunkData)
	require.NoError(t, err)

	// Simulate: another coordinator (coord2) also owns this chunk
	mockReg.owners[chunkHash] = []string{"coord1", "coord2"}

	// Age the chunk (set mod time to 2 hours ago to pass grace period)
	chunkPath := filepath.Join(store.dataDir, "chunks", chunkHash[:2], chunkHash)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	err = os.Chtimes(chunkPath, twoHoursAgo, twoHoursAgo)
	require.NoError(t, err)

	// Run GC
	stats := store.RunGarbageCollection(ctx)

	// Chunk should be deleted locally — not referenced by any local metadata.
	// Other coordinators manage their own copies independently.
	assert.Equal(t, 1, stats.ChunksDeleted, "Should delete locally unreferenced chunk")

	// Verify chunk was deleted from local CAS
	_, err = store.cas.ReadChunk(ctx, chunkHash)
	assert.Error(t, err, "Chunk should be deleted locally")

	// Verify we unregistered ourselves from the registry
	assert.NotContains(t, mockReg.owners[chunkHash], "coord1", "Should unregister self from registry")
}

// TestGC_DeletesWhenSoleOwner tests that GC deletes chunks when we're the only owner.
func TestGC_DeletesWhenSoleOwner(t *testing.T) {
	dir := t.TempDir()

	// Create store with chunk registry
	store, err := NewStore(dir, nil)
	require.NoError(t, err)

	ctx := context.Background()

	var encryptionKey [32]byte
	err = store.InitCAS(ctx, encryptionKey)
	require.NoError(t, err)

	// Create mock registry
	mockReg := &mockChunkRegistry{
		owners:       make(map[string][]string),
		localCoordID: "coord1",
	}
	store.SetChunkRegistry(mockReg)
	store.SetCoordinatorID("coord1")

	// Create an orphaned chunk directly (not referenced by any object)
	chunkData := []byte("Test data for sole owner GC")
	chunkHash, _, err := store.cas.WriteChunk(ctx, chunkData)
	require.NoError(t, err)

	// We're the sole owner
	mockReg.owners[chunkHash] = []string{"coord1"}

	// Age the chunk (set mod time to 2 hours ago to pass grace period)
	chunkPath := filepath.Join(store.dataDir, "chunks", chunkHash[:2], chunkHash)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	err = os.Chtimes(chunkPath, twoHoursAgo, twoHoursAgo)
	require.NoError(t, err)

	// Run GC
	stats := store.RunGarbageCollection(ctx)

	// Chunk should be deleted (unreferenced locally)
	assert.Equal(t, 1, stats.ChunksDeleted, "Should delete unreferenced chunk")

	// Verify chunk was deleted
	_, err = store.cas.ReadChunk(ctx, chunkHash)
	assert.Error(t, err, "Chunk should be deleted")
}

// TestGC_GracePeriod tests that GC doesn't delete recently created chunks.
func TestGC_GracePeriod(t *testing.T) {
	dir := t.TempDir()

	// Create store
	store, err := NewStore(dir, nil)
	require.NoError(t, err)

	ctx := context.Background()

	var encryptionKey [32]byte
	err = store.InitCAS(ctx, encryptionKey)
	require.NoError(t, err)

	// Create bucket
	err = store.CreateBucket(ctx, "test-bucket", "test-user", 2)
	require.NoError(t, err)

	// Upload file
	data := []byte("Test data for grace period")
	reader := bytes.NewReader(data)

	meta, err := store.PutObject(ctx, "test-bucket", "test-file", reader, int64(len(data)), "text/plain", nil)
	require.NoError(t, err)
	require.NotEmpty(t, meta.Chunks)

	chunkHash := meta.Chunks[0]

	// Delete the object (chunk becomes orphaned)
	err = store.DeleteObject(ctx, "test-bucket", "test-file")
	require.NoError(t, err)

	// Chunk is recent (within grace period) - don't age it

	// Run GC
	stats := store.RunGarbageCollection(ctx)

	// Chunk should NOT be deleted (within grace period)
	assert.Equal(t, 0, stats.ChunksDeleted, "Should not delete recent chunks")
	// With universal EC (k=4, m=2), each object produces k+m chunks; all are within grace period.
	assert.GreaterOrEqual(t, stats.ChunksSkippedGracePeriod, 1, "Should skip chunks in grace period")

	// Verify chunk still exists
	_, err = store.cas.ReadChunk(ctx, chunkHash)
	assert.NoError(t, err, "Chunk should still exist during grace period")
}

// TestGC_NoRegistry tests that GC works without a registry (local-only mode).
func TestGC_NoRegistry(t *testing.T) {
	dir := t.TempDir()

	// Create store WITHOUT chunk registry
	store, err := NewStore(dir, nil)
	require.NoError(t, err)

	ctx := context.Background()

	var encryptionKey [32]byte
	err = store.InitCAS(ctx, encryptionKey)
	require.NoError(t, err)

	// Create an orphaned chunk directly (not referenced by any object)
	chunkData := []byte("Test data for no registry GC")
	chunkHash, _, err := store.cas.WriteChunk(ctx, chunkData)
	require.NoError(t, err)

	// Age the chunk (set mod time to 2 hours ago to pass grace period)
	chunkPath := filepath.Join(store.dataDir, "chunks", chunkHash[:2], chunkHash)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	err = os.Chtimes(chunkPath, twoHoursAgo, twoHoursAgo)
	require.NoError(t, err)

	// Run GC
	stats := store.RunGarbageCollection(ctx)

	// Chunk should be deleted (unreferenced locally)
	assert.Equal(t, 1, stats.ChunksDeleted, "Should delete orphaned chunks without registry")

	// Verify chunk was deleted
	_, err = store.cas.ReadChunk(ctx, chunkHash)
	assert.Error(t, err, "Chunk should be deleted")
}

// TestGC_MultipleOwners tests that GC deletes all locally unreferenced chunks
// regardless of registry ownership, and unregisters self from each.
func TestGC_MultipleOwners(t *testing.T) {
	dir := t.TempDir()

	// Create store with chunk registry
	store, err := NewStore(dir, nil)
	require.NoError(t, err)

	ctx := context.Background()

	var encryptionKey [32]byte
	err = store.InitCAS(ctx, encryptionKey)
	require.NoError(t, err)

	// Create mock registry
	mockReg := &mockChunkRegistry{
		owners:       make(map[string][]string),
		localCoordID: "coord1",
	}
	store.SetChunkRegistry(mockReg)
	store.SetCoordinatorID("coord1")

	// Create 3 orphaned chunks directly (not referenced by any object)
	chunk1Data := []byte("Chunk 1: sole owner")
	chunk1Hash, _, err := store.cas.WriteChunk(ctx, chunk1Data)
	require.NoError(t, err)

	chunk2Data := []byte("Chunk 2: shared ownership")
	chunk2Hash, _, err := store.cas.WriteChunk(ctx, chunk2Data)
	require.NoError(t, err)

	chunk3Data := []byte("Chunk 3: other coordinator only")
	chunk3Hash, _, err := store.cas.WriteChunk(ctx, chunk3Data)
	require.NoError(t, err)

	// Different registry states — but all are locally unreferenced, so all should be deleted
	mockReg.owners[chunk1Hash] = []string{"coord1"}
	mockReg.owners[chunk2Hash] = []string{"coord1", "coord2"}
	mockReg.owners[chunk3Hash] = []string{"coord2"}

	// Age all chunks
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	for _, chunkHash := range []string{chunk1Hash, chunk2Hash, chunk3Hash} {
		chunkPath := filepath.Join(store.dataDir, "chunks", chunkHash[:2], chunkHash)
		err = os.Chtimes(chunkPath, twoHoursAgo, twoHoursAgo)
		require.NoError(t, err)
	}

	// Run GC
	stats := store.RunGarbageCollection(ctx)

	// All 3 chunks should be deleted (all locally unreferenced + past grace period)
	assert.Equal(t, 3, stats.ChunksDeleted, "Should delete all locally unreferenced chunks")

	// Verify all chunks were deleted from local CAS
	for _, chunkHash := range []string{chunk1Hash, chunk2Hash, chunk3Hash} {
		_, err := store.cas.ReadChunk(ctx, chunkHash)
		assert.Error(t, err, "Chunk %s should be deleted", chunkHash[:8])
	}

	// Verify we unregistered from all chunks in the registry
	assert.NotContains(t, mockReg.owners[chunk1Hash], "coord1")
	assert.NotContains(t, mockReg.owners[chunk2Hash], "coord1")
}

// TestGC_RegistryUnavailable tests GC behavior when registry query fails.
func TestGC_RegistryUnavailable(t *testing.T) {
	dir := t.TempDir()

	// Create store with failing registry
	store, err := NewStore(dir, nil)
	require.NoError(t, err)

	ctx := context.Background()

	var encryptionKey [32]byte
	err = store.InitCAS(ctx, encryptionKey)
	require.NoError(t, err)

	// Create mock registry that returns errors
	mockReg := &failingChunkRegistry{}
	store.SetChunkRegistry(mockReg)
	store.SetCoordinatorID("coord1")

	// Create an orphaned chunk directly (not referenced by any object)
	chunkData := []byte("Test data for registry failure")
	chunkHash, _, err := store.cas.WriteChunk(ctx, chunkData)
	require.NoError(t, err)

	// Age the chunk (set mod time to 2 hours ago to pass grace period)
	chunkPath := filepath.Join(store.dataDir, "chunks", chunkHash[:2], chunkHash)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	err = os.Chtimes(chunkPath, twoHoursAgo, twoHoursAgo)
	require.NoError(t, err)

	// Run GC
	stats := store.RunGarbageCollection(ctx)

	// Fail-safe: When registry is unavailable, proceed with deletion
	// (better to delete locally than leak storage)
	assert.Equal(t, 1, stats.ChunksDeleted, "Should delete when registry unavailable (fail-safe)")
}

// failingChunkRegistry is a mock that always returns errors.
type failingChunkRegistry struct{}

func (f *failingChunkRegistry) RegisterChunk(hash string, size int64) error {
	return assert.AnError
}

func (f *failingChunkRegistry) RegisterChunkWithReplication(hash string, size int64, replicationFactor int) error {
	return assert.AnError
}

func (f *failingChunkRegistry) RegisterShardChunk(hash string, size int64, parentFileID string, shardType string, shardIndex int, replicationFactor int) error {
	return assert.AnError
}

func (f *failingChunkRegistry) UnregisterChunk(hash string) error {
	return assert.AnError
}

func (f *failingChunkRegistry) GetOwners(hash string) ([]string, error) {
	return nil, assert.AnError
}

func (f *failingChunkRegistry) GetChunksOwnedBy(coordinatorID string) ([]string, error) {
	return nil, assert.AnError
}

// TestGC_ErasureCodingParityShards tests that GC cleans up both data chunks and parity shards.
func TestGC_ErasureCodingParityShards(t *testing.T) {
	dir := t.TempDir()

	// Create store with CAS
	store, err := NewStore(dir, nil)
	require.NoError(t, err)

	ctx := context.Background()

	var encryptionKey [32]byte
	err = store.InitCAS(ctx, encryptionKey)
	require.NoError(t, err)

	// Manually create orphaned chunks simulating erasure-coded data
	// Data chunks (simulating CDC-chunked data shards)
	dataChunk1 := []byte("Data shard 1 chunk 1")
	dataHash1, _, err := store.cas.WriteChunk(ctx, dataChunk1)
	require.NoError(t, err)

	dataChunk2 := []byte("Data shard 2 chunk 1")
	dataHash2, _, err := store.cas.WriteChunk(ctx, dataChunk2)
	require.NoError(t, err)

	// Parity shards (stored whole)
	parityShard1 := []byte("Parity shard 1 data")
	parityHash1, _, err := store.cas.WriteChunk(ctx, parityShard1)
	require.NoError(t, err)

	parityShard2 := []byte("Parity shard 2 data")
	parityHash2, _, err := store.cas.WriteChunk(ctx, parityShard2)
	require.NoError(t, err)

	// Age all chunks (set mod time to 2 hours ago to pass grace period)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	for _, chunkHash := range []string{dataHash1, dataHash2, parityHash1, parityHash2} {
		chunkPath := filepath.Join(store.dataDir, "chunks", chunkHash[:2], chunkHash)
		err = os.Chtimes(chunkPath, twoHoursAgo, twoHoursAgo)
		require.NoError(t, err)
	}

	// Run GC (should delete all orphaned chunks including parity shards)
	stats := store.RunGarbageCollection(ctx)

	// Verify all chunks (data + parity) were deleted
	assert.Equal(t, 4, stats.ChunksDeleted, "should delete all orphaned data chunks and parity shards")

	// Verify chunks no longer exist in CAS
	for _, chunkHash := range []string{dataHash1, dataHash2, parityHash1, parityHash2} {
		_, err := store.cas.ReadChunk(ctx, chunkHash)
		assert.Error(t, err, "chunk %s should be deleted", chunkHash[:8])
	}
}

// TestGC_ErasureCodingKeepsReferencedShards tests that GC doesn't delete parity shards for live objects.
func TestGC_ErasureCodingKeepsReferencedShards(t *testing.T) {
	dir := t.TempDir()

	// Create store with erasure coding enabled
	store, err := NewStore(dir, nil)
	require.NoError(t, err)

	ctx := context.Background()

	var encryptionKey [32]byte
	err = store.InitCAS(ctx, encryptionKey)
	require.NoError(t, err)

	err = store.CreateBucket(ctx, "test-bucket", "test-user", 2)
	require.NoError(t, err)

	// Upload a file (will be erasure coded)
	testData := []byte("Test data for erasure coding GC protection. This file should not be deleted by GC.")
	meta, err := store.PutObject(ctx, "test-bucket", "protected-file.txt", bytes.NewReader(testData), int64(len(testData)), "text/plain", nil)
	require.NoError(t, err)
	require.NotNil(t, meta.ErasureCoding)
	require.True(t, meta.ErasureCoding.Enabled)

	// Verify we have both data chunk hashes and parity shard hashes
	require.NotEmpty(t, meta.ErasureCoding.DataHashes, "should have data chunk hashes")
	require.NotEmpty(t, meta.ErasureCoding.ParityHashes, "should have parity shard hashes")
	require.Equal(t, ecParityShards, len(meta.ErasureCoding.ParityHashes), "should have ecParityShards parity hashes")

	// Age all chunks (set mod time to 2 hours ago - normally would be GC'd)
	allChunks := append([]string{}, meta.ErasureCoding.DataHashes...)
	allChunks = append(allChunks, meta.ErasureCoding.ParityHashes...)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	for _, chunkHash := range allChunks {
		chunkPath := filepath.Join(store.dataDir, "chunks", chunkHash[:2], chunkHash)
		err = os.Chtimes(chunkPath, twoHoursAgo, twoHoursAgo)
		require.NoError(t, err)
	}

	// Run GC (should NOT delete any chunks - file still exists)
	stats := store.RunGarbageCollection(ctx)

	// No chunks should be deleted (file is still referenced)
	assert.Equal(t, 0, stats.ChunksDeleted, "should not delete chunks for live objects")

	// Verify all chunks still exist in CAS
	for _, chunkHash := range allChunks {
		_, err := store.cas.ReadChunk(ctx, chunkHash)
		assert.NoError(t, err, "chunk %s should still exist", chunkHash[:8])
	}
}
