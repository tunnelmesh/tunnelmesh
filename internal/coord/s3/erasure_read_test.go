package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStoreWithErasureCoding creates a test store and a bucket.
// The k and m parameters are stored as the bucket policy for API compatibility,
// but the store always encodes with the universal RS(ecDataShards, ecParityShards).
func newTestStoreWithErasureCoding(t *testing.T, k, m int) *Store {
	t.Helper()
	store := newTestStoreWithCAS(t)
	err := store.CreateBucket(context.Background(), "ec-bucket", "test-user", 2, &ErasureCodingPolicy{
		Enabled:      true,
		DataShards:   k,
		ParityShards: m,
	})
	require.NoError(t, err)
	return store
}

// putAndGet is a helper that puts an object and reads it back.
func putAndGet(t *testing.T, store *Store, bucket, key string, data []byte) []byte {
	t.Helper()
	ctx := context.Background()

	_, err := store.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
	require.NoError(t, err)

	reader, _, err := store.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	return got
}

// deleteDataShard removes all CAS chunks belonging to data shard shardIdx.
// Uses position-based routing (DataHashes is shard-major) to correctly handle
// the case where multiple shards have identical content.
func deleteDataShard(t *testing.T, store *Store, meta *ObjectMeta, shardIdx int) {
	t.Helper()
	k := meta.ErasureCoding.DataShards
	numBlocks := len(meta.ErasureCoding.DataHashes) / k
	if numBlocks == 0 {
		numBlocks = 1
	}
	start := shardIdx * numBlocks
	end := start + numBlocks
	ctx := context.Background()
	for i := start; i < end && i < len(meta.ErasureCoding.DataHashes); i++ {
		_, _ = store.cas.DeleteChunk(ctx, meta.ErasureCoding.DataHashes[i])
	}
}

// TestErasureCodingReadPath_FastPath verifies the fast path (all data shards available).
func TestErasureCodingReadPath_FastPath(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		k    int
		m    int
	}{
		{"small_3+2", []byte("Hello, erasure coding!"), 3, 2},
		{"medium_6+3", bytes.Repeat([]byte("ABCDEFGH"), 512), 6, 3}, // 4KB
		{"uneven_size_5+2", []byte("This data doesn't divide evenly into shards"), 5, 2},
		{"minimal_2+1", []byte("Minimum viable erasure coding test data"), 2, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStoreWithErasureCoding(t, tt.k, tt.m)
			got := putAndGet(t, store, "ec-bucket", "test.bin", tt.data)
			assert.Equal(t, tt.data, got, "data should round-trip through erasure coding")
		})
	}
}

// TestErasureCodingReadPath_LargeFile tests the fast path with a larger file
// that produces multiple RS blocks.
func TestErasureCodingReadPath_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file erasure coding test in short mode")
	}

	// 9 MB — produces 3 RS blocks (> 2 × ecStreamBlock/2)
	data := make([]byte, 9*1024*1024)
	_, err := rand.Read(data)
	require.NoError(t, err)

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	got := putAndGet(t, store, "ec-bucket", "large.bin", data)
	assert.Equal(t, data, got, "large file should round-trip through erasure coding")
}

// TestErasureCodingReadPath_Reconstruction tests reading when some data shards are missing.
// Uses random data to ensure all RS pieces have distinct hashes.
func TestErasureCodingReadPath_Reconstruction(t *testing.T) {
	// Use random data large enough to fill ecStreamBlock so all k pieces are distinct.
	// A file that fills the entire 4 MB block guarantees no zero-pad pieces.
	data := make([]byte, ecStreamBlock) // exactly 4 MB → 4 × 1 MB non-zero pieces
	_, err := rand.Read(data)
	require.NoError(t, err)

	// With k=4, m=2 we can tolerate up to m=2 missing data shards.
	for missing := 1; missing <= ecParityShards; missing++ {
		t.Run(fmt.Sprintf("missing_%d_data_shards", missing), func(t *testing.T) {
			store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
			ctx := context.Background()

			meta, err := store.PutObject(ctx, "ec-bucket", "test.bin", bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
			require.NoError(t, err)
			require.NotNil(t, meta.ErasureCoding)
			assert.Equal(t, ecDataShards, meta.ErasureCoding.DataShards)

			// Delete chunks for 'missing' data shards using position-based routing.
			for s := 0; s < missing; s++ {
				deleteDataShard(t, store, meta, s)
			}

			// Read should succeed via RS reconstruction.
			reader, _, err := store.GetObject(ctx, "ec-bucket", "test.bin")
			require.NoError(t, err)
			defer func() { _ = reader.Close() }()

			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, data, got, "reconstructed data should match original with %d missing shards", missing)

			store.WaitBackground()
		})
	}
}

// TestErasureCodingReadPath_InsufficientShards tests that reading fails when too many shards are missing.
func TestErasureCodingReadPath_InsufficientShards(t *testing.T) {
	data := make([]byte, ecStreamBlock)
	_, err := rand.Read(data)
	require.NoError(t, err)

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	meta, err := store.PutObject(ctx, "ec-bucket", "test.bin", bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
	require.NoError(t, err)

	// Delete ALL data chunks.
	for _, chunkHash := range meta.ErasureCoding.DataHashes {
		_, _ = store.cas.DeleteChunk(ctx, chunkHash)
	}
	// Delete one parity shard too so we have fewer than k shards available.
	k := meta.ErasureCoding.DataShards
	m := meta.ErasureCoding.ParityShards
	numParBlocks := len(meta.ErasureCoding.ParityHashes) / m
	if numParBlocks == 0 {
		numParBlocks = 1
	}
	for i := 0; i < numParBlocks && i < len(meta.ErasureCoding.ParityHashes); i++ {
		_, _ = store.cas.DeleteChunk(ctx, meta.ErasureCoding.ParityHashes[i])
	}
	_ = k

	_, _, err = store.GetObject(ctx, "ec-bucket", "test.bin")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient shards")
}

// TestErasureCodingReadPath_ContextCancellation tests that reads respect context cancellation.
func TestErasureCodingReadPath_ContextCancellation(t *testing.T) {
	data := []byte("Test data for context cancellation")

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)

	_, err := store.PutObject(context.Background(), "ec-bucket", "test.bin", bytes.NewReader(data), int64(len(data)), "text/plain", nil)
	require.NoError(t, err)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = store.GetObject(canceledCtx, "ec-bucket", "test.bin")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

// TestErasureCodingReadPath_ChunkOrdering verifies that chunks are reassembled
// in the correct order for multi-block files.
func TestErasureCodingReadPath_ChunkOrdering(t *testing.T) {
	// Use a 10 MB file → 3 RS blocks; data is random so all pieces are distinct.
	if testing.Short() {
		t.Skip("skipping chunk ordering test in short mode")
	}
	data := make([]byte, 10*1024*1024)
	_, err := rand.Read(data)
	require.NoError(t, err)

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	meta, err := store.PutObject(ctx, "ec-bucket", "ordered.bin", bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
	require.NoError(t, err)
	require.NotNil(t, meta.ErasureCoding)

	// DataHashes is shard-major: numBlocks × k entries total.
	expectedHashes := ecDataShards * 3 // 3 blocks × 4 shards
	assert.Equal(t, expectedHashes, len(meta.ErasureCoding.DataHashes),
		"10 MB file should produce 3 blocks × k=%d data hashes", ecDataShards)

	reader, _, err := store.GetObject(ctx, "ec-bucket", "ordered.bin")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got, "data should match byte-for-byte (ordering preserved)")
}

// TestErasureCodingReadPath_ParityOnlyReconstruction tests reconstruction using
// parity shards when some data shards are missing.
// With k=4, m=2 we can tolerate up to m=2 missing shards. This test
// removes exactly m data shards (maximum tolerance) and verifies reconstruction.
func TestErasureCodingReadPath_ParityOnlyReconstruction(t *testing.T) {
	// Random data filling a full block ensures distinct piece hashes.
	data := make([]byte, ecStreamBlock)
	_, err := rand.Read(data)
	require.NoError(t, err)

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	meta, err := store.PutObject(ctx, "ec-bucket", "test.bin", bytes.NewReader(data), int64(len(data)), "text/plain", nil)
	require.NoError(t, err)

	// Delete the maximum tolerated number of data shards (= ecParityShards).
	for s := 0; s < ecParityShards; s++ {
		deleteDataShard(t, store, meta, s)
	}

	reader, _, err := store.GetObject(ctx, "ec-bucket", "test.bin")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got, "should reconstruct with max-tolerance missing shards")

	store.WaitBackground()
}

// TestErasureCodingReadPath_MixedShardReconstruction tests reconstruction with
// a mix of data and parity shards missing (but within tolerance).
func TestErasureCodingReadPath_MixedShardReconstruction(t *testing.T) {
	data := make([]byte, ecStreamBlock)
	_, err := rand.Read(data)
	require.NoError(t, err)

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	meta, err := store.PutObject(ctx, "ec-bucket", "test.bin", bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
	require.NoError(t, err)

	// Delete 1 data shard and 1 parity chunk — total 2 shards missing = m.
	deleteDataShard(t, store, meta, 0)
	_, _ = store.cas.DeleteChunk(ctx, meta.ErasureCoding.ParityHashes[0])

	reader, _, err := store.GetObject(ctx, "ec-bucket", "test.bin")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got, "should reconstruct with mixed available shards")

	store.WaitBackground()
}

// TestErasureCodingReadPath_MetadataIntegrity verifies that erasure coding metadata
// is correctly stored and retrieved with the universal RS(4,2) encoder.
func TestErasureCodingReadPath_MetadataIntegrity(t *testing.T) {
	// Use data that produces exactly 1 RS block.
	data := bytes.Repeat([]byte("metadata test"), 1000) // ~13 KB
	k := ecDataShards
	m := ecParityShards

	store := newTestStoreWithErasureCoding(t, k, m)
	ctx := context.Background()

	meta, err := store.PutObject(ctx, "ec-bucket", "meta-test.bin", bytes.NewReader(data), int64(len(data)), "text/plain", nil)
	require.NoError(t, err)

	ec := meta.ErasureCoding
	require.NotNil(t, ec)
	assert.True(t, ec.Enabled)
	assert.Equal(t, ecDataShards, ec.DataShards)
	assert.Equal(t, ecParityShards, ec.ParityShards)
	// 1 block: k data hashes + m parity hashes.
	assert.Equal(t, k, len(ec.DataHashes), "1-block file should have k=%d data hashes", k)
	assert.Equal(t, m, len(ec.ParityHashes), "1-block file should have m=%d parity hashes", m)
	assert.Greater(t, ec.ShardSize, int64(0), "shard size should be positive")
	assert.Equal(t, int64(ecPieceSize), ec.ShardSize)
}

// TestErasureCodingReadPath_MultipleObjects tests reading multiple erasure-coded
// objects from the same bucket.
func TestErasureCodingReadPath_MultipleObjects(t *testing.T) {
	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	objects := map[string][]byte{
		"file1.txt": []byte("First file content"),
		"file2.txt": bytes.Repeat([]byte("Second file "), 100),
		"file3.bin": make([]byte, 8192),
	}
	_, _ = rand.Read(objects["file3.bin"])

	for key, data := range objects {
		_, err := store.PutObject(ctx, "ec-bucket", key, bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
		require.NoError(t, err)
	}

	for key, expected := range objects {
		reader, _, err := store.GetObject(ctx, "ec-bucket", key)
		require.NoError(t, err)

		got, err := io.ReadAll(reader)
		_ = reader.Close()
		require.NoError(t, err)
		assert.Equal(t, expected, got, "object %s should match", key)
	}
}

// TestErasureCodingReadPath_NonErasureBucketUnaffected tests that all buckets
// (even those created without an explicit EC policy) use EC and read back correctly.
// Since EC is now universal, all objects are stored with RS(4,2) regardless of
// bucket policy.
func TestErasureCodingReadPath_NonErasureBucketUnaffected(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Create a bucket without an explicit EC policy.
	err := store.CreateBucket(ctx, "normal-bucket", "test-user", 2, nil)
	require.NoError(t, err)

	data := []byte("Normal bucket data — EC is universal so this uses RS(4,2) too")
	_, err = store.PutObject(ctx, "normal-bucket", "test.txt", bytes.NewReader(data), int64(len(data)), "text/plain", nil)
	require.NoError(t, err)

	reader, meta, err := store.GetObject(ctx, "normal-bucket", "test.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// EC is universal — all objects have ErasureCoding set.
	require.NotNil(t, meta.ErasureCoding, "universal EC: all objects should have ErasureCoding metadata")
	assert.True(t, meta.ErasureCoding.Enabled)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// TestErasureCodingReadPath_VersionedObject tests erasure coding with versioning.
func TestErasureCodingReadPath_VersionedObject(t *testing.T) {
	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	v1Data := []byte("Version 1 of the file")
	_, err := store.PutObject(ctx, "ec-bucket", "versioned.txt", bytes.NewReader(v1Data), int64(len(v1Data)), "text/plain", nil)
	require.NoError(t, err)

	v2Data := []byte("Version 2 of the file - with more content to be different")
	_, err = store.PutObject(ctx, "ec-bucket", "versioned.txt", bytes.NewReader(v2Data), int64(len(v2Data)), "text/plain", nil)
	require.NoError(t, err)

	// Read current version (should be v2).
	reader, _, err := store.GetObject(ctx, "ec-bucket", "versioned.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, v2Data, got, "should read latest version")
}

// TestErasureCodingReadPath_RepeatedContent tests erasure coding with data that
// may produce duplicate chunk hashes (deduplication scenario).
func TestErasureCodingReadPath_RepeatedContent(t *testing.T) {
	data := []byte(strings.Repeat("ABCDEFGHIJKLMNOP", 256)) // 4KB of repeated pattern
	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	got := putAndGet(t, store, "ec-bucket", "repeated.bin", data)
	assert.Equal(t, data, got, "repeated content should round-trip correctly")
}

// TestErasureCodingReadPath_DistributedFetch tests that missing local chunks are
// fetched from remote coordinators via the chunk registry and replicator.
func TestErasureCodingReadPath_DistributedFetch(t *testing.T) {
	// Use a full-block file so all k data pieces have distinct hashes.
	data := make([]byte, ecStreamBlock)
	_, err := rand.Read(data)
	require.NoError(t, err)

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	meta, err := store.PutObject(ctx, "ec-bucket", "test.bin", bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
	require.NoError(t, err)
	require.NotNil(t, meta.ErasureCoding)

	// Move 2 data shards to a simulated remote coordinator.
	shardsToMove := 2
	k := meta.ErasureCoding.DataShards
	numBlocks := len(meta.ErasureCoding.DataHashes) / k
	if numBlocks == 0 {
		numBlocks = 1
	}

	remoteChunks := make(map[string][]byte)
	registryOwners := make(map[string][]string)

	for s := 0; s < shardsToMove; s++ {
		start := s * numBlocks
		end := start + numBlocks
		for i := start; i < end && i < len(meta.ErasureCoding.DataHashes); i++ {
			chunkHash := meta.ErasureCoding.DataHashes[i]
			chunkData, readErr := store.cas.ReadChunk(ctx, chunkHash)
			if readErr != nil {
				continue
			}
			remoteChunks[chunkHash] = chunkData
			registryOwners[chunkHash] = []string{"remote-coord-1"}
			_, _ = store.cas.DeleteChunk(ctx, chunkHash)
		}
	}

	mockReg := &mockChunkRegistry{owners: registryOwners}
	mockRepl := &mockReplicator{chunks: remoteChunks}
	store.SetChunkRegistry(mockReg)
	store.SetReplicator(mockRepl)

	reader, _, err := store.GetObject(ctx, "ec-bucket", "test.bin")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got, "data should match after distributed fetch of missing chunks")
}

// TestErasureCodingReadPath_DistributedFetchFallbackToReconstruction tests that when
// remote fetch fails for some chunks, the EC path falls back to RS reconstruction.
func TestErasureCodingReadPath_DistributedFetchFallbackToReconstruction(t *testing.T) {
	data := make([]byte, ecStreamBlock)
	_, err := rand.Read(data)
	require.NoError(t, err)

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	meta, err := store.PutObject(ctx, "ec-bucket", "test.bin", bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
	require.NoError(t, err)

	// Delete 2 data shards but do NOT populate the replicator with them.
	for s := 0; s < ecParityShards; s++ {
		deleteDataShard(t, store, meta, s)
	}

	mockReg := &mockChunkRegistry{owners: make(map[string][]string)}
	mockRepl := &mockReplicator{chunks: make(map[string][]byte)}
	store.SetChunkRegistry(mockReg)
	store.SetReplicator(mockRepl)

	reader, _, err := store.GetObject(ctx, "ec-bucket", "test.bin")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got, "should reconstruct via RS after failed distributed fetch")

	store.WaitBackground()
}

// TestErasureCodingReadPath_NoDistributedWithoutReplicator tests that without
// a replicator, the EC read path falls back to RS reconstruction.
func TestErasureCodingReadPath_NoDistributedWithoutReplicator(t *testing.T) {
	data := make([]byte, ecStreamBlock)
	_, err := rand.Read(data)
	require.NoError(t, err)

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	meta, err := store.PutObject(ctx, "ec-bucket", "test.bin", bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
	require.NoError(t, err)

	// Delete 2 data shards (within parity tolerance).
	for s := 0; s < ecParityShards; s++ {
		deleteDataShard(t, store, meta, s)
	}

	assert.Nil(t, store.chunkRegistry)
	assert.Nil(t, store.replicator)

	reader, _, err := store.GetObject(ctx, "ec-bucket", "test.bin")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got, "should reconstruct via RS without distributed fetch capability")

	store.WaitBackground()
}

// TestErasureCodingReadPath_DistributedFetchHashMismatch tests that corrupt chunks
// from remote coordinators are rejected and the read falls back to RS reconstruction.
func TestErasureCodingReadPath_DistributedFetchHashMismatch(t *testing.T) {
	data := make([]byte, ecStreamBlock)
	_, err := rand.Read(data)
	require.NoError(t, err)

	store := newTestStoreWithErasureCoding(t, ecDataShards, ecParityShards)
	ctx := context.Background()

	meta, err := store.PutObject(ctx, "ec-bucket", "test.bin", bytes.NewReader(data), int64(len(data)), "application/octet-stream", nil)
	require.NoError(t, err)

	// Delete 2 data shards and populate replicator with corrupted data.
	k := meta.ErasureCoding.DataShards
	numBlocks := len(meta.ErasureCoding.DataHashes) / k
	if numBlocks == 0 {
		numBlocks = 1
	}

	corruptChunks := make(map[string][]byte)
	registryOwners := make(map[string][]string)

	for s := 0; s < ecParityShards; s++ {
		start := s * numBlocks
		end := start + numBlocks
		for i := start; i < end && i < len(meta.ErasureCoding.DataHashes); i++ {
			chunkHash := meta.ErasureCoding.DataHashes[i]
			corruptChunks[chunkHash] = []byte("corrupted data that won't match hash")
			registryOwners[chunkHash] = []string{"bad-coord-1"}
			_, _ = store.cas.DeleteChunk(ctx, chunkHash)
		}
	}

	mockReg := &mockChunkRegistry{owners: registryOwners}
	mockRepl := &mockReplicator{chunks: corruptChunks}
	store.SetChunkRegistry(mockReg)
	store.SetReplicator(mockRepl)

	reader, _, err := store.GetObject(ctx, "ec-bucket", "test.bin")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got, "should reconstruct via RS after rejecting corrupt remote chunks")

	store.WaitBackground()
}
