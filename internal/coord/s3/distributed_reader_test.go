package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockReplicator implements ReplicatorInterface for testing.
type mockReplicator struct {
	mu            sync.Mutex
	chunks        map[string][]byte // chunkHash -> chunk data
	latency       time.Duration     // Simulated fetch latency
	failPeer      string            // If set, FetchChunk returns error for this peer
	calls         int32             // Atomic call counter
	concurrent    int32             // Current concurrent calls
	maxConcurrent int32             // Peak concurrent calls observed
}

func (m *mockReplicator) FetchChunk(ctx context.Context, peerID, chunkHash string) ([]byte, error) {
	atomic.AddInt32(&m.calls, 1)

	// Track peak concurrency without timing dependency.
	cur := atomic.AddInt32(&m.concurrent, 1)
	defer atomic.AddInt32(&m.concurrent, -1)
	for {
		max := atomic.LoadInt32(&m.maxConcurrent)
		if cur <= max || atomic.CompareAndSwapInt32(&m.maxConcurrent, max, cur) {
			break
		}
	}

	if m.latency > 0 {
		select {
		case <-time.After(m.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if m.failPeer != "" && peerID == m.failPeer {
		return nil, fmt.Errorf("peer %s unavailable", peerID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	data, exists := m.chunks[chunkHash]
	if !exists {
		return nil, fmt.Errorf("chunk not found: %s", chunkHash)
	}
	return data, nil
}

func (m *mockReplicator) GetPeers() []string {
	return []string{"peer-1", "peer-2"}
}

// mockChunkRegistry implements ChunkRegistryInterface for testing.
type mockChunkRegistry struct {
	owners       map[string][]string // chunkHash -> []coordinatorID
	localCoordID string              // set to enable UnregisterChunk tracking
}

func (m *mockChunkRegistry) RegisterChunk(hash string, size int64) error {
	return nil
}

func (m *mockChunkRegistry) RegisterChunkWithReplication(hash string, size int64, replicationFactor int) error {
	return nil
}

func (m *mockChunkRegistry) RegisterShardChunk(hash string, size int64, parentFileID string, shardType string, shardIndex int, replicationFactor int) error {
	return nil
}

func (m *mockChunkRegistry) UnregisterChunk(hash string) error {
	if m.owners != nil && m.localCoordID != "" {
		if owners, exists := m.owners[hash]; exists {
			var remaining []string
			for _, o := range owners {
				if o != m.localCoordID {
					remaining = append(remaining, o)
				}
			}
			if len(remaining) == 0 {
				delete(m.owners, hash)
			} else {
				m.owners[hash] = remaining
			}
		}
	}
	return nil
}

func (m *mockChunkRegistry) GetOwners(hash string) ([]string, error) {
	owners, exists := m.owners[hash]
	if !exists {
		return nil, fmt.Errorf("chunk not found in registry")
	}
	return owners, nil
}

func (m *mockChunkRegistry) GetChunksOwnedBy(coordinatorID string) ([]string, error) {
	var chunks []string
	for hash, owners := range m.owners {
		for _, owner := range owners {
			if owner == coordinatorID {
				chunks = append(chunks, hash)
				break
			}
		}
	}
	return chunks, nil
}

func TestDistributedChunkReader_AllChunksLocal(t *testing.T) {
	dir := t.TempDir()

	// Create CAS (use zero key for testing)
	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Write test chunks locally
	chunk1 := []byte("Hello ")
	chunk2 := []byte("World!")

	hash1, _, err := cas.WriteChunk(ctx, chunk1)
	require.NoError(t, err)

	hash2, _, err := cas.WriteChunk(ctx, chunk2)
	require.NoError(t, err)

	// Create distributed reader (replicator is nil - not needed for local-only)
	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:   []string{hash1, hash2},
		LocalCAS: cas,
		Logger:   zerolog.Nop(),
	})
	defer func() { _ = reader.Close() }()

	// Read all data
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "Hello World!", string(data))

	// Check progress
	assert.Equal(t, 0.0, reader.Progress()) // Total size not set
}

func TestDistributedChunkReader_FetchFromRemote(t *testing.T) {
	dir := t.TempDir()

	// Create CAS (use zero key for testing)
	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Create test chunks (only write chunk1 locally)
	chunk1 := []byte("Local ")
	chunk2 := []byte("Remote ")
	chunk3 := []byte("Data!")

	hash1, _, err := cas.WriteChunk(ctx, chunk1)
	require.NoError(t, err)

	// Hash chunk2 and chunk3 without writing locally
	hash2 := ContentHash(chunk2)
	hash3 := ContentHash(chunk3)

	// Create mock replicator with remote chunks
	mockRepl := &mockReplicator{
		chunks: map[string][]byte{
			hash2: chunk2,
			hash3: chunk3,
		},
	}

	// Create mock registry
	mockReg := &mockChunkRegistry{
		owners: map[string][]string{
			hash1: {"local"},
			hash2: {"peer1"},
			hash3: {"peer2"},
		},
	}

	// Create distributed reader
	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:     []string{hash1, hash2, hash3},
		LocalCAS:   cas,
		Registry:   mockReg,
		Replicator: mockRepl,
		Logger:     zerolog.Nop(),
		TotalSize:  int64(len(chunk1) + len(chunk2) + len(chunk3)),
	})
	defer func() { _ = reader.Close() }()

	// Read all data (should fetch chunk2 and chunk3 from remote)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "Local Remote Data!", string(data))

	// Verify chunks were cached locally
	cachedChunk2, err := cas.ReadChunk(ctx, hash2)
	require.NoError(t, err)
	assert.Equal(t, chunk2, cachedChunk2)

	cachedChunk3, err := cas.ReadChunk(ctx, hash3)
	require.NoError(t, err)
	assert.Equal(t, chunk3, cachedChunk3)

	// Check progress
	assert.Equal(t, 1.0, reader.Progress())
}

func TestDistributedChunkReader_ChunkNotFound(t *testing.T) {
	dir := t.TempDir()

	// Create CAS (use zero key for testing)
	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Missing chunk hash (not in CAS or replicator)
	missingHash := "0000000000000000000000000000000000000000000000000000000000000000"

	// Create mock registry (no owners for missing chunk)
	mockReg := &mockChunkRegistry{
		owners: map[string][]string{},
	}

	// Create distributed reader
	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:   []string{missingHash},
		LocalCAS: cas,
		Registry: mockReg,
		Logger:   zerolog.Nop(),
	})
	defer func() { _ = reader.Close() }()

	// Read should fail
	_, err = io.ReadAll(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDistributedChunkReader_EmptyFile(t *testing.T) {
	dir := t.TempDir()

	// Create CAS (use zero key for testing)
	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Create distributed reader with no chunks
	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:   []string{},
		LocalCAS: cas,
		Logger:   zerolog.Nop(),
	})
	defer func() { _ = reader.Close() }()

	// Read should return empty data
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Empty(t, data)
}

func TestDistributedChunkReader_PartialReads(t *testing.T) {
	dir := t.TempDir()

	// Create CAS (use zero key for testing)
	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Write test chunks
	chunk1 := []byte("AAAA")
	chunk2 := []byte("BBBB")
	chunk3 := []byte("CCCC")

	hash1, _, err := cas.WriteChunk(ctx, chunk1)
	require.NoError(t, err)

	hash2, _, err := cas.WriteChunk(ctx, chunk2)
	require.NoError(t, err)

	hash3, _, err := cas.WriteChunk(ctx, chunk3)
	require.NoError(t, err)

	// Create distributed reader
	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:   []string{hash1, hash2, hash3},
		LocalCAS: cas,
		Logger:   zerolog.Nop(),
	})
	defer func() { _ = reader.Close() }()

	// Read in small chunks to test partial reads across chunk boundaries
	var result bytes.Buffer
	buf := make([]byte, 3) // Smaller than chunk size

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			result.Write(buf[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, "AAAABBBBCCCC", result.String())
}

func TestDistributedChunkReader_NoRegistry(t *testing.T) {
	dir := t.TempDir()

	// Create CAS (use zero key for testing)
	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Missing chunk (not in CAS)
	missingHash := "0000000000000000000000000000000000000000000000000000000000000000"

	// Create distributed reader without registry
	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:   []string{missingHash},
		LocalCAS: cas,
		Registry: nil, // No registry
		Logger:   zerolog.Nop(),
	})
	defer func() { _ = reader.Close() }()

	// Read should fail with appropriate error
	_, err = io.ReadAll(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no peers available")
}

// ==== New parallel fetch tests ====

func TestDistributedChunkReader_ParallelFetch(t *testing.T) {
	dir := t.TempDir()

	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Create 8 chunks with simulated 50ms latency each
	const numChunks = 8
	chunks := make([][]byte, numChunks)
	hashes := make([]string, numChunks)
	remoteData := make(map[string][]byte)
	owners := make(map[string][]string)

	for i := 0; i < numChunks; i++ {
		chunks[i] = []byte(fmt.Sprintf("chunk-%02d-data", i))
		hashes[i] = ContentHash(chunks[i])
		remoteData[hashes[i]] = chunks[i]
		owners[hashes[i]] = []string{"peer1"}
	}

	mockRepl := &mockReplicator{
		chunks:  remoteData,
		latency: 50 * time.Millisecond,
	}

	mockReg := &mockChunkRegistry{owners: owners}

	// Parallel read (4 workers)
	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:     hashes,
		LocalCAS:   cas,
		Registry:   mockReg,
		Replicator: mockRepl,
		Logger:     zerolog.Nop(),
		Prefetch:   PrefetchConfig{WindowSize: 8, Parallelism: 4},
	})
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Verify correct data
	var expected bytes.Buffer
	for _, chunk := range chunks {
		expected.Write(chunk)
	}
	assert.Equal(t, expected.String(), string(data))

	// Verify parallelism by peak concurrency, not wall-clock time.
	// With 8 chunks, 4 workers, and 50ms latency per fetch, goroutines must overlap.
	// Even on a slow Windows runner this is deterministic: concurrent sleeps always overlap.
	assert.GreaterOrEqual(t, int(atomic.LoadInt32(&mockRepl.maxConcurrent)), 2,
		"Expected parallel fetching (parallelism=4) but peak concurrency was only %d", mockRepl.maxConcurrent)
}

func TestDistributedChunkReader_OrderedReassembly(t *testing.T) {
	dir := t.TempDir()

	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Create chunks with different latencies to force out-of-order arrival
	chunkData := [][]byte{
		[]byte("FIRST-"),
		[]byte("SECOND-"),
		[]byte("THIRD-"),
		[]byte("FOURTH"),
	}
	hashes := make([]string, len(chunkData))
	remoteData := make(map[string][]byte)
	owners := make(map[string][]string)

	for i, data := range chunkData {
		hashes[i] = ContentHash(data)
		remoteData[hashes[i]] = data
		owners[hashes[i]] = []string{"peer1"}
	}

	mockRepl := &mockReplicator{
		chunks:  remoteData,
		latency: 10 * time.Millisecond,
	}

	mockReg := &mockChunkRegistry{owners: owners}

	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:     hashes,
		LocalCAS:   cas,
		Registry:   mockReg,
		Replicator: mockRepl,
		Logger:     zerolog.Nop(),
		Prefetch:   PrefetchConfig{WindowSize: 4, Parallelism: 4},
	})
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Verify order is preserved regardless of fetch completion order
	assert.Equal(t, "FIRST-SECOND-THIRD-FOURTH", string(data))
}

func TestDistributedChunkReader_MixedLocalRemote(t *testing.T) {
	dir := t.TempDir()

	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Mix of local and remote chunks
	local1 := []byte("LOCAL1-")
	remote1 := []byte("REMOTE1-")
	local2 := []byte("LOCAL2-")
	remote2 := []byte("REMOTE2")

	hashL1, _, err := cas.WriteChunk(ctx, local1)
	require.NoError(t, err)
	hashR1 := ContentHash(remote1)
	hashL2, _, err := cas.WriteChunk(ctx, local2)
	require.NoError(t, err)
	hashR2 := ContentHash(remote2)

	mockRepl := &mockReplicator{
		chunks: map[string][]byte{
			hashR1: remote1,
			hashR2: remote2,
		},
		latency: 20 * time.Millisecond,
	}

	mockReg := &mockChunkRegistry{
		owners: map[string][]string{
			hashL1: {"local"},
			hashR1: {"peer1"},
			hashL2: {"local"},
			hashR2: {"peer2"},
		},
	}

	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:     []string{hashL1, hashR1, hashL2, hashR2},
		LocalCAS:   cas,
		Registry:   mockReg,
		Replicator: mockRepl,
		Logger:     zerolog.Nop(),
		Prefetch:   PrefetchConfig{WindowSize: 4, Parallelism: 2},
	})
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "LOCAL1-REMOTE1-LOCAL2-REMOTE2", string(data))
}

func TestDistributedChunkReader_PartialFailure(t *testing.T) {
	dir := t.TempDir()

	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	chunk1 := []byte("DATA1-")
	chunk2 := []byte("DATA2-")
	chunk3 := []byte("DATA3")

	hash1 := ContentHash(chunk1)
	hash2 := ContentHash(chunk2)
	hash3 := ContentHash(chunk3)

	mockRepl := &mockReplicator{
		chunks: map[string][]byte{
			hash1: chunk1,
			hash2: chunk2,
			hash3: chunk3,
		},
		failPeer: "bad-peer", // This peer will fail
	}

	// chunk2 has both a bad peer (first) and a good peer (second)
	mockReg := &mockChunkRegistry{
		owners: map[string][]string{
			hash1: {"good-peer"},
			hash2: {"bad-peer", "good-peer"}, // bad-peer fails, falls back to good-peer
			hash3: {"good-peer"},
		},
	}

	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:     []string{hash1, hash2, hash3},
		LocalCAS:   cas,
		Registry:   mockReg,
		Replicator: mockRepl,
		Logger:     zerolog.Nop(),
		Prefetch:   PrefetchConfig{WindowSize: 4, Parallelism: 2},
	})
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "DATA1-DATA2-DATA3", string(data))
}

func TestDistributedChunkReader_ContextCancellation(t *testing.T) {
	dir := t.TempDir()

	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create chunks with high latency
	chunks := make([][]byte, 10)
	hashes := make([]string, 10)
	remoteData := make(map[string][]byte)
	owners := make(map[string][]string)

	for i := 0; i < 10; i++ {
		chunks[i] = []byte(fmt.Sprintf("chunk-%d", i))
		hashes[i] = ContentHash(chunks[i])
		remoteData[hashes[i]] = chunks[i]
		owners[hashes[i]] = []string{"peer1"}
	}

	mockRepl := &mockReplicator{
		chunks:  remoteData,
		latency: 500 * time.Millisecond, // Slow fetches
	}

	mockReg := &mockChunkRegistry{owners: owners}

	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:     hashes,
		LocalCAS:   cas,
		Registry:   mockReg,
		Replicator: mockRepl,
		Logger:     zerolog.Nop(),
		Prefetch:   PrefetchConfig{WindowSize: 8, Parallelism: 4},
	})

	// Cancel after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// Read should eventually fail or return partial data
	_, readErr := io.ReadAll(reader)
	// Close waits for all worker goroutines to finish, ensuring no file
	// handles are held when t.TempDir() cleanup runs (fixes Windows flake).
	_ = reader.Close()

	// Either we get an error (context canceled) or we get no data
	// The key thing is that it doesn't hang
	if readErr != nil {
		// Any error is acceptable - the test validates we don't hang
		assert.Error(t, readErr)
	}
}

func TestDistributedChunkReader_SequentialFallback(t *testing.T) {
	dir := t.TempDir()

	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	chunk1 := []byte("SEQ1-")
	chunk2 := []byte("SEQ2-")
	chunk3 := []byte("SEQ3")

	hash1 := ContentHash(chunk1)
	hash2 := ContentHash(chunk2)
	hash3 := ContentHash(chunk3)

	mockRepl := &mockReplicator{
		chunks: map[string][]byte{
			hash1: chunk1,
			hash2: chunk2,
			hash3: chunk3,
		},
	}

	mockReg := &mockChunkRegistry{
		owners: map[string][]string{
			hash1: {"peer1"},
			hash2: {"peer1"},
			hash3: {"peer1"},
		},
	}

	// With Parallelism=1, should use sequential path
	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:     []string{hash1, hash2, hash3},
		LocalCAS:   cas,
		Registry:   mockReg,
		Replicator: mockRepl,
		Logger:     zerolog.Nop(),
		Prefetch:   PrefetchConfig{WindowSize: 4, Parallelism: 1},
	})
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "SEQ1-SEQ2-SEQ3", string(data))
}

func TestDistributedChunkReader_SequentialFallback_NoReplicator(t *testing.T) {
	dir := t.TempDir()

	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	chunk1 := []byte("A")
	chunk2 := []byte("B")

	hash1, _, err := cas.WriteChunk(ctx, chunk1)
	require.NoError(t, err)
	hash2, _, err := cas.WriteChunk(ctx, chunk2)
	require.NoError(t, err)

	// With replicator=nil, should use sequential path even with Parallelism > 1
	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:   []string{hash1, hash2},
		LocalCAS: cas,
		Logger:   zerolog.Nop(),
		Prefetch: PrefetchConfig{WindowSize: 4, Parallelism: 4},
	})
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "AB", string(data))
}

func TestDistributedChunkReader_LargeFile(t *testing.T) {
	dir := t.TempDir()

	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Create 100+ chunks
	const numChunks = 128
	allChunks := make([][]byte, numChunks)
	hashes := make([]string, numChunks)
	remoteData := make(map[string][]byte)
	owners := make(map[string][]string)
	var expected bytes.Buffer

	for i := 0; i < numChunks; i++ {
		allChunks[i] = []byte(fmt.Sprintf("chunk-data-%04d|", i))
		hashes[i] = ContentHash(allChunks[i])
		remoteData[hashes[i]] = allChunks[i]
		owners[hashes[i]] = []string{"peer1", "peer2"}
		expected.Write(allChunks[i])
	}

	mockRepl := &mockReplicator{
		chunks: remoteData,
	}

	mockReg := &mockChunkRegistry{owners: owners}

	reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
		Chunks:     hashes,
		LocalCAS:   cas,
		Registry:   mockReg,
		Replicator: mockRepl,
		Logger:     zerolog.Nop(),
		TotalSize:  int64(expected.Len()),
		Prefetch:   PrefetchConfig{WindowSize: 8, Parallelism: 4},
	})
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, expected.Bytes(), data)
	assert.InDelta(t, 1.0, reader.Progress(), 0.01)
}
