package s3

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

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

func TestCAS_ReadWriteChunkRaw(t *testing.T) {
	cas := newTestCAS(t)
	ctx := context.Background()
	data := []byte("hello raw chunk transfer test data")

	// Write via normal WriteChunk (compresses+encrypts), then read raw bytes.
	hash, _, err := cas.WriteChunk(ctx, data)
	require.NoError(t, err)

	raw, err := cas.ReadChunkRaw(hash)
	require.NoError(t, err)

	// Raw bytes must not equal plaintext (they're encrypted).
	assert.NotEqual(t, data, raw, "ReadChunkRaw should return encrypted bytes, not plaintext")
	assert.Greater(t, len(raw), 0)

	// Write the raw bytes to a second CAS (same master key = convergent encryption).
	cas2, err := NewCAS(t.TempDir(), cas.masterKey)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cas2.Close() })

	created, err := cas2.WriteChunkRaw(hash, raw)
	require.NoError(t, err)
	assert.True(t, created, "chunk should be newly created")

	// ReadChunk on the second CAS should return original plaintext.
	got, err := cas2.ReadChunk(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, data, got, "ReadChunk after WriteChunkRaw should return original plaintext")

	// Writing same raw again is a dedup hit (created=false).
	created2, err := cas2.WriteChunkRaw(hash, raw)
	require.NoError(t, err)
	assert.False(t, created2, "second WriteChunkRaw should be a dedup hit")
}

func TestCAS_ReadChunkRaw_NotFound(t *testing.T) {
	cas := newTestCAS(t)

	_, err := cas.ReadChunkRaw("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk not found")
}
