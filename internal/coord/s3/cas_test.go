package s3

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCAS(t *testing.T) *CAS {
	t.Helper()
	masterKey := [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	cas, err := NewCAS(t.TempDir(), masterKey)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cas.Close() })
	return cas
}

func TestCAS_ConcurrentWriteSameHash(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()
	data := []byte("identical content for all goroutines")

	const goroutines = 20
	var wg sync.WaitGroup
	hashes := make([]string, goroutines)
	errs := make([]error, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			h, _, err := cas.WriteChunk(ctx, data)
			hashes[idx] = h
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	// All should succeed with the same hash
	for i := 0; i < goroutines; i++ {
		require.NoError(t, errs[i], "goroutine %d failed", i)
		assert.Equal(t, hashes[0], hashes[i], "all hashes should be identical")
	}

	// Verify the chunk is readable and correct
	readBack, err := cas.ReadChunk(ctx, hashes[0])
	require.NoError(t, err)
	assert.Equal(t, data, readBack)
}

func TestCAS_ConcurrentWriteDifferentHashes(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	const goroutines = 20
	var wg sync.WaitGroup
	hashes := make([]string, goroutines)
	errs := make([]error, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			// Each goroutine writes unique data
			data := []byte("unique content " + string(rune('A'+idx)))
			h, _, err := cas.WriteChunk(ctx, data)
			hashes[idx] = h
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	// All should succeed
	for i := 0; i < goroutines; i++ {
		require.NoError(t, errs[i], "goroutine %d failed", i)
	}

	// All hashes should be unique (different content)
	seen := make(map[string]bool)
	for _, h := range hashes {
		assert.False(t, seen[h], "duplicate hash found: %s", h)
		seen[h] = true
	}

	// All should be readable
	for i := 0; i < goroutines; i++ {
		data := []byte("unique content " + string(rune('A'+i)))
		readBack, err := cas.ReadChunk(ctx, hashes[i])
		require.NoError(t, err, "failed to read chunk %d", i)
		assert.Equal(t, data, readBack, "chunk %d content mismatch", i)
	}
}

func TestCAS_WriteReadDeleteRoundTrip(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	data := []byte("test chunk data")
	hash, _, err := cas.WriteChunk(ctx, data)
	require.NoError(t, err)

	// Read back
	readBack, err := cas.ReadChunk(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, data, readBack)

	// Exists
	assert.True(t, cas.ChunkExists(hash))

	// Size
	size, err := cas.ChunkSize(ctx, hash)
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))

	// Delete
	_, err = cas.DeleteChunk(ctx, hash)
	require.NoError(t, err)

	// Should not exist anymore
	assert.False(t, cas.ChunkExists(hash))

	// Read should fail
	_, err = cas.ReadChunk(ctx, hash)
	assert.Error(t, err)
}

func TestCAS_WriteChunkDedup(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	data := []byte("dedup test data")

	// First write creates the chunk
	hash1, onDisk1, err := cas.WriteChunk(ctx, data)
	require.NoError(t, err)
	assert.Greater(t, onDisk1, int64(0), "first write should report on-disk bytes")

	// Second write should hit the dedup fast path (file already exists)
	hash2, onDisk2, err := cas.WriteChunk(ctx, data)
	require.NoError(t, err)
	assert.Equal(t, hash1, hash2)
	assert.Equal(t, int64(0), onDisk2, "dedup hit should report 0 on-disk bytes")
}

func TestCAS_WriteChunkBadDir(t *testing.T) {
	// Point CAS at a non-existent path to exercise the CreateTemp error path.
	// We create the CAS normally, then remove the chunksDir out from under it.
	masterKey := [32]byte{1, 2, 3}
	tmpDir := t.TempDir()

	cas, err := NewCAS(tmpDir, masterKey)
	require.NoError(t, err)

	// Remove the entire chunks directory and replace with a regular file
	// so MkdirAll and CreateTemp both fail
	require.NoError(t, os.RemoveAll(cas.chunksDir))
	require.NoError(t, os.WriteFile(cas.chunksDir, []byte("not a dir"), 0644))

	_, _, err = cas.WriteChunk(context.Background(), []byte("data"))
	assert.Error(t, err)
}

func TestCAS_TotalSize(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	// Empty store
	size, err := cas.TotalSize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	// Write some chunks
	_, _, err = cas.WriteChunk(ctx, []byte("chunk one"))
	require.NoError(t, err)
	_, _, err = cas.WriteChunk(ctx, []byte("chunk two"))
	require.NoError(t, err)

	size, err = cas.TotalSize(ctx)
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))
}

func TestCAS_DeleteChunkNonExistent(t *testing.T) {
	cas := newTestCAS(t)
	// Deleting a non-existent chunk should succeed (no-op)
	freed, err := cas.DeleteChunk(context.Background(), "nonexistent-hash")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), freed)
}

func TestCAS_ChunkSizeNotFound(t *testing.T) {
	cas := newTestCAS(t)
	_, err := cas.ChunkSize(context.Background(), "nonexistent-hash")
	assert.Error(t, err)
}

func TestCAS_ReadChunkNotFound(t *testing.T) {
	cas := newTestCAS(t)
	_, err := cas.ReadChunk(context.Background(), "nonexistent-hash")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "chunk not found")
}

func TestCAS_NoOrphanedTempFiles(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	// Write several chunks
	for i := 0; i < 10; i++ {
		data := []byte("chunk data " + string(rune('0'+i)))
		_, _, err := cas.WriteChunk(ctx, data)
		require.NoError(t, err)
	}

	// Walk the chunks dir and ensure no .tmp files remain
	err := filepath.Walk(cas.chunksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			assert.NotContains(t, info.Name(), ".tmp", "orphaned temp file found: %s", path)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestCAS_ReadChunkRaw_ReturnsEncryptedBytes(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	plaintext := []byte("hello raw chunk test")
	hash, _, err := cas.WriteChunk(ctx, plaintext)
	require.NoError(t, err)

	// Raw read should return encrypted+compressed bytes, not plaintext
	raw, err := cas.ReadChunkRaw(ctx, hash)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, raw, "ReadChunkRaw must not return plaintext")

	// Normal read must still return the original plaintext
	got, err := cas.ReadChunk(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestCAS_WriteChunkRaw_Roundtrip(t *testing.T) {
	casA := newTestCAS(t)
	casB := newTestCAS(t)
	ctx := context.Background()

	plaintext := []byte("roundtrip raw chunk data")

	// Write on A, read raw bytes
	hash, _, err := casA.WriteChunk(ctx, plaintext)
	require.NoError(t, err)

	raw, err := casA.ReadChunkRaw(ctx, hash)
	require.NoError(t, err)

	// Write raw bytes to B (simulating replication receiver)
	onDiskBytes, err := casB.WriteChunkRaw(ctx, hash, raw)
	require.NoError(t, err)
	assert.Greater(t, onDiskBytes, int64(0))

	// B must be able to read back the original plaintext
	got, err := casB.ReadChunk(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestCAS_WriteChunkRaw_Dedup(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	plaintext := []byte("dedup raw chunk")
	hash, _, err := cas.WriteChunk(ctx, plaintext)
	require.NoError(t, err)

	raw, err := cas.ReadChunkRaw(ctx, hash)
	require.NoError(t, err)

	// First WriteChunkRaw on empty store should write bytes
	cas2 := newTestCAS(t)
	onDisk1, err := cas2.WriteChunkRaw(ctx, hash, raw)
	require.NoError(t, err)
	assert.Greater(t, onDisk1, int64(0), "first write should return on-disk bytes")

	// Second WriteChunkRaw should hit dedup
	onDisk2, err := cas2.WriteChunkRaw(ctx, hash, raw)
	require.NoError(t, err)
	assert.Equal(t, int64(0), onDisk2, "dedup hit should return 0")
}

func TestCAS_WriteChunkRaw_ConcurrentSameHash(t *testing.T) {
	casA := newTestCAS(t)
	casB := newTestCAS(t)
	ctx := context.Background()

	plaintext := []byte("concurrent raw write")
	hash, _, err := casA.WriteChunk(ctx, plaintext)
	require.NoError(t, err)

	raw, err := casA.ReadChunkRaw(ctx, hash)
	require.NoError(t, err)

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = casB.WriteChunkRaw(ctx, hash, raw)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}

	// Chunk must be readable after concurrent writes
	got, err := casB.ReadChunk(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestDeriveMasterKey_DeterministicAndShared(t *testing.T) {
	token := "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"

	// Same token produces same key every time
	key1, err := DeriveMasterKey(token)
	require.NoError(t, err)
	key2, err := DeriveMasterKey(token)
	require.NoError(t, err)
	assert.Equal(t, key1, key2, "same token must produce same key")

	// Different token produces different key
	otherKey, err := DeriveMasterKey("b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3")
	require.NoError(t, err)
	assert.NotEqual(t, key1, otherKey, "different tokens must produce different keys")
}

func TestCAS_SelfHealCallbackInvoked(t *testing.T) {
	keyA := [32]byte{1}
	keyB := [32]byte{2}
	ctx := context.Background()

	casA, err := NewCAS(t.TempDir(), keyA)
	require.NoError(t, err)
	defer func() { _ = casA.Close() }()

	casB, err := NewCAS(t.TempDir(), keyB)
	require.NoError(t, err)
	defer func() { _ = casB.Close() }()

	var callbackHash string
	var callbackFreed int64
	casA.onSelfHeal = func(hash string, freed int64) {
		callbackHash = hash
		callbackFreed = freed
	}

	plaintext := []byte("callback test")
	hash, _, err := casA.WriteChunk(ctx, plaintext)
	require.NoError(t, err)

	// Write same plaintext with keyB, get raw bytes
	_, _, err = casB.WriteChunk(ctx, plaintext)
	require.NoError(t, err)
	rawB, err := casB.ReadChunkRaw(ctx, hash)
	require.NoError(t, err)

	// Corrupt A's chunk with B's raw bytes
	require.NoError(t, os.WriteFile(casA.chunkPath(hash), rawB, 0644))

	// ReadChunk triggers self-heal → callback must be called with correct args
	_, err = casA.ReadChunk(ctx, hash)
	require.Error(t, err)
	assert.Equal(t, hash, callbackHash)
	assert.Equal(t, int64(len(rawB)), callbackFreed)
}

func TestCAS_ReadChunk_SelfHealsCorruptedChunk(t *testing.T) {
	// Write a chunk with CAS A (one key), then overwrite the file with bytes
	// from CAS B (different key). Reading with A should detect the MAC failure,
	// delete the file, and return "chunk not found" so callers can re-fetch.
	keyA := [32]byte{1}
	keyB := [32]byte{2}
	ctx := context.Background()

	// Separate dirs so each CAS owns its own chunk files.
	casA, err := NewCAS(t.TempDir(), keyA)
	require.NoError(t, err)
	defer func() { _ = casA.Close() }()

	casB, err := NewCAS(t.TempDir(), keyB)
	require.NoError(t, err)
	defer func() { _ = casB.Close() }()

	plaintext := []byte("self-heal test data")

	// Write the same plaintext with both keys to get the raw bytes from key B.
	hash, _, err := casA.WriteChunk(ctx, plaintext)
	require.NoError(t, err)

	_, _, err = casB.WriteChunk(ctx, plaintext)
	require.NoError(t, err)

	// Get raw (key-B-encrypted) bytes
	rawB, err := casB.ReadChunkRaw(ctx, hash)
	require.NoError(t, err)

	// Overwrite A's chunk file with B's raw bytes (simulates wrong-key replication)
	chunkPath := casA.chunkPath(hash)
	require.NoError(t, os.WriteFile(chunkPath, rawB, 0644))

	// Reading with CAS A should detect MAC failure, delete the file, return "chunk not found"
	_, err = casA.ReadChunk(ctx, hash)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk not found", "corrupted chunk must look like not-found after self-heal")

	// File must be deleted
	assert.False(t, casA.ChunkExists(hash), "corrupted chunk file must be removed")
}

// TestCAS_EncryptIntoParity verifies that encryptInto produces identical output
// to encrypt, confirming the buffer-pooling refactor didn't change cipher semantics.
func TestCAS_EncryptIntoParity(t *testing.T) {
	cas := newTestCAS(t)
	plaintext := []byte("parity check plaintext")
	hash := ContentHash(plaintext)

	got, err := cas.encrypt(plaintext, hash)
	require.NoError(t, err)

	// encryptInto with nil dst must produce the same bytes
	gotInto, err := cas.encryptInto(nil, plaintext, hash)
	require.NoError(t, err)
	require.Equal(t, got, gotInto, "encryptInto(nil,...) must match encrypt()")

	// encryptInto with a pre-allocated dst must also produce the same bytes
	preDst := make([]byte, 0, len(got)+16)
	gotWithDst, err := cas.encryptInto(preDst, plaintext, hash)
	require.NoError(t, err)
	require.Equal(t, got, gotWithDst, "encryptInto(preDst,...) must match encrypt()")
}

// TestCAS_WriteChunkKnown_BufferReuse exercises the compressBufPool and
// encryptBufPool under dedup and concurrent scenarios with the race detector.
func TestCAS_WriteChunkKnown_BufferReuse(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	data := []byte("buffer reuse test data")

	// First write must succeed and return on-disk bytes.
	_, onDisk1, err := cas.WriteChunk(ctx, data)
	require.NoError(t, err)
	require.Greater(t, onDisk1, int64(0))

	// Second write of the same data must be a dedup hit.
	_, onDisk2, err := cas.WriteChunk(ctx, data)
	require.NoError(t, err)
	require.Equal(t, int64(0), onDisk2, "dedup hit must return 0 on-disk bytes")

	// Concurrent writes of two different chunks must both succeed and be readable.
	data1 := []byte("concurrent chunk alpha")
	data2 := []byte("concurrent chunk beta")
	var wg sync.WaitGroup
	var hash1, hash2 string
	var err1, err2 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		hash1, _, err1 = cas.WriteChunk(ctx, data1)
	}()
	go func() {
		defer wg.Done()
		hash2, _, err2 = cas.WriteChunk(ctx, data2)
	}()
	wg.Wait()
	require.NoError(t, err1)
	require.NoError(t, err2)

	got1, err := cas.ReadChunk(ctx, hash1)
	require.NoError(t, err)
	require.Equal(t, data1, got1)

	got2, err := cas.ReadChunk(ctx, hash2)
	require.NoError(t, err)
	require.Equal(t, data2, got2)
}

// TestCAS_WriteChunkKnown_LargeChunk verifies that the pooled buffers grow to
// fit a larger-than-typical piece (2× ecPieceSize) and the pool entry is
// correctly reused on a subsequent write.
func TestCAS_WriteChunkKnown_LargeChunk(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	// First large chunk — forces pool buffer to grow to high-water mark.
	large := make([]byte, 2*ecPieceSize)
	for i := range large {
		large[i] = byte(i % 251)
	}
	hash1, onDisk1, err := cas.WriteChunk(ctx, large)
	require.NoError(t, err)
	require.Greater(t, onDisk1, int64(0))

	// Second large chunk (different content) — reuses the grown pool buffer.
	large2 := make([]byte, 2*ecPieceSize)
	for i := range large2 {
		large2[i] = byte((i + 1) % 251)
	}
	hash2, onDisk2, err := cas.WriteChunk(ctx, large2)
	require.NoError(t, err)
	require.Greater(t, onDisk2, int64(0))
	require.NotEqual(t, hash1, hash2)

	// Both chunks must be readable with correct content.
	got1, err := cas.ReadChunk(ctx, hash1)
	require.NoError(t, err)
	require.Equal(t, large, got1)

	got2, err := cas.ReadChunk(ctx, hash2)
	require.NoError(t, err)
	require.Equal(t, large2, got2)
}

// TestCASEncoderMemoryBudget verifies that the CAS encoder stays within a
// bounded memory budget after upload spikes. casEncoderConcurrency=2 means
// two zstd sub-encoders (~13 MB each with lowMem) rather than GOMAXPROCS
// sub-encoders (~13 MB each). The test also validates that idle heap is
// returned to the OS when the runtime is given the opportunity — simulating
// what happens in production with GOMEMLIMIT=400MiB set in systemd/docker.
//
// Skipped under -short because it depends on GC timing and OS page
// reclaim behaviour which can be slow on memory-constrained CI runners.
func TestCASEncoderMemoryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory budget test in -short mode")
	}
	// Tighten GOMEMLIMIT so the background scavenger runs aggressively
	// during this test. In production, GOMEMLIMIT=400MiB in the systemd unit
	// and docker-compose files provides the same pressure automatically.
	const testLimit = 200 * 1024 * 1024 // 200 MiB
	prev := debug.SetMemoryLimit(int64(testLimit))
	defer debug.SetMemoryLimit(prev)

	cas := newTestCAS(t)
	ctx := context.Background()

	// Spike: write a 50 MB chunk — this exercises the zstd encoder and the
	// CAS pipeline, allocating ~50–80 MB of transient heap.
	data := make([]byte, 50*1024*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}
	_, _, err := cas.WriteChunk(ctx, data)
	require.NoError(t, err)
	data = nil //nolint:ineffassign // intentional: releases the 50 MiB before FreeOSMemory

	// FreeOSMemory runs GC then synchronously returns all idle heap spans to
	// the OS. In production, GOMEMLIMIT causes the background scavenger to do
	// this within ~60 s of a spike; here we force it for a fast deterministic test.
	debug.FreeOSMemory()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// After FreeOSMemory the idle heap should be well below the spike size.
	// The threshold accounts for the live CAS encoder (~13 MiB), Go runtime
	// internals, and heap span granularity. Without scavenging, HeapIdle
	// would be ~80–150 MiB (the spike size stays idle indefinitely).
	const maxIdle = 100 * 1024 * 1024 // 100 MiB
	assert.Less(t, ms.HeapIdle, uint64(maxIdle),
		"HeapIdle=%d MiB after FreeOSMemory; want < 100 MiB — idle pages should be returned to OS promptly under GOMEMLIMIT pressure",
		ms.HeapIdle/1024/1024)
}

// TestCAS_ChunkModTime verifies that ChunkModTime returns a recent time after
// writing a chunk and an error for a non-existent chunk.
func TestCAS_ChunkModTime(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()

	// Non-existent chunk must return error
	_, err := cas.ChunkModTime("deadbeef")
	assert.Error(t, err, "ChunkModTime on missing chunk should return error")

	// Write a chunk and check mod time
	before := time.Now().Add(-time.Second)
	data := []byte("mod time test data")
	hash, _, err := cas.WriteChunk(ctx, data)
	require.NoError(t, err)
	after := time.Now().Add(time.Second)

	mtime, err := cas.ChunkModTime(hash)
	require.NoError(t, err, "ChunkModTime on existing chunk should succeed")
	assert.True(t, mtime.After(before), "mtime should be after test start")
	assert.True(t, mtime.Before(after), "mtime should be before test end")
}
