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

// newTestStore creates a store with CAS initialized for testing.
func newTestStore(t *testing.T, dir string) *Store {
	t.Helper()

	quota := NewQuotaManager(100 * 1024 * 1024) // 100 MB
	masterKey := [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	store, err := NewStoreWithCAS(dir, quota, masterKey)
	require.NoError(t, err)

	return store
}

// TestConcurrent_MultipleUploads tests concurrent PutObject operations to same bucket.
func TestConcurrent_MultipleUploads(t *testing.T) {
	dir := t.TempDir()

	store := newTestStore(t, dir)

	ctx := context.Background()

	// Create bucket
	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "test-owner", 2))

	// Upload multiple files concurrently
	const numUploads = 50
	var wg sync.WaitGroup
	errors := make(chan error, numUploads)

	for i := 0; i < numUploads; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			key := fmt.Sprintf("file-%d.txt", n)
			data := []byte(fmt.Sprintf("Content for file %d", n))

			_, err := store.PutObject(ctx, "test-bucket", key, bytes.NewReader(data), int64(len(data)), "text/plain", nil)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Upload failed: %v", err)
	}

	// Verify all files were created
	objects, _, _, err := store.ListObjects(ctx, "test-bucket", "", "", 1000)
	require.NoError(t, err)
	assert.Len(t, objects, numUploads)
}

// TestConcurrent_ReadsAndWrites tests concurrent reads and writes (skip GC to avoid lock contention).
func TestConcurrent_ReadsAndWrites(t *testing.T) {
	dir := t.TempDir()

	store := newTestStore(t, dir)

	ctx := context.Background()

	// Create bucket and upload some initial files
	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "test-owner", 2))

	const numFiles = 20
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("file-%d.txt", i)
		data := []byte(fmt.Sprintf("Content %d", i))
		_, err := store.PutObject(ctx, "test-bucket", key, bytes.NewReader(data), int64(len(data)), "text/plain", nil)
		require.NoError(t, err)
	}

	// Concurrent readers and writers
	var wg sync.WaitGroup
	readCount := atomic.Int32{}
	writeCount := atomic.Int32{}

	// Launch readers (each does 10 reads)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("file-%d.txt", (workerID+j)%numFiles)
				reader, _, err := store.GetObject(ctx, "test-bucket", key)
				if err == nil {
					_, _ = io.Copy(io.Discard, reader)
					_ = reader.Close()
					readCount.Add(1)
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	// Launch writers (each writes 5 files)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < 5; j++ {
				key := fmt.Sprintf("worker-%d-file-%d.txt", workerID, j)
				data := []byte(fmt.Sprintf("Worker %d content %d", workerID, j))
				_, err := store.PutObject(ctx, "test-bucket", key, bytes.NewReader(data), int64(len(data)), "text/plain", nil)
				if err == nil {
					writeCount.Add(1)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Completed %d reads and %d writes concurrently", readCount.Load(), writeCount.Load())

	// Verify store is still functional
	objects, _, _, err := store.ListObjects(ctx, "test-bucket", "", "", 10000)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(objects), numFiles) // At least initial files
}

// TestConcurrent_DistributedReads tests concurrent distributed chunk reads.
func TestConcurrent_DistributedReads(t *testing.T) {
	dir := t.TempDir()

	// Create CAS
	var encryptionKey [32]byte
	cas, err := NewCAS(filepath.Join(dir, "cas"), encryptionKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Create test chunks
	const numChunks = 10
	chunks := make([]string, numChunks)
	expectedData := make([]byte, 0)

	for i := 0; i < numChunks; i++ {
		chunkData := []byte(fmt.Sprintf("Chunk %d ", i))
		hash, _, err := cas.WriteChunk(ctx, chunkData)
		require.NoError(t, err)
		chunks[i] = hash
		expectedData = append(expectedData, chunkData...)
	}

	// Create multiple readers concurrently
	const numReaders = 20
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			reader := NewDistributedChunkReader(ctx, DistributedChunkReaderConfig{
				Chunks:   chunks,
				LocalCAS: cas,
				Logger:   zerolog.Nop(),
			})

			data, err := io.ReadAll(reader)
			if err != nil {
				errors <- err
				return
			}

			if !bytes.Equal(data, expectedData) {
				errors <- fmt.Errorf("data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(data))
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Reader failed: %v", err)
	}
}

// TestConcurrent_RegistryUpdates tests concurrent registry operations.
func TestConcurrent_RegistryUpdates(t *testing.T) {
	dir := t.TempDir()

	store := newTestStore(t, dir)

	// Create thread-safe mock registry
	registry := &threadSafeRegistry{
		owners: make(map[string][]string),
	}

	store.SetChunkRegistry(registry)

	ctx := context.Background()

	// Create bucket
	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "test-owner", 2))

	// Concurrent uploads will trigger concurrent registry updates
	const numUploads = 30
	var wg sync.WaitGroup
	errors := make(chan error, numUploads)

	for i := 0; i < numUploads; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			key := fmt.Sprintf("file-%d.txt", n)
			data := bytes.Repeat([]byte(fmt.Sprintf("Data %d ", n)), 1000) // ~10KB file

			_, err := store.PutObject(ctx, "test-bucket", key, bytes.NewReader(data), int64(len(data)), "text/plain", nil)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Upload with registry update failed: %v", err)
	}
}

// threadSafeRegistry is a thread-safe mock registry for concurrent testing.
type threadSafeRegistry struct {
	owners map[string][]string
	mu     sync.RWMutex
}

func (r *threadSafeRegistry) RegisterChunk(hash string, size int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.owners[hash] == nil {
		r.owners[hash] = []string{"local"}
	}
	return nil
}

func (r *threadSafeRegistry) RegisterChunkWithReplication(hash string, size int64, replicationFactor int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.owners[hash] == nil {
		r.owners[hash] = []string{"local"}
	}
	return nil
}

func (r *threadSafeRegistry) RegisterShardChunk(hash string, size int64, parentFileID string, shardType string, shardIndex int, replicationFactor int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.owners[hash] == nil {
		r.owners[hash] = []string{"local"}
	}
	return nil
}

func (r *threadSafeRegistry) UnregisterChunk(hash string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.owners, hash)
	return nil
}

func (r *threadSafeRegistry) GetOwners(hash string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	owners, exists := r.owners[hash]
	if !exists {
		return nil, fmt.Errorf("chunk not found in registry")
	}
	return owners, nil
}

func (r *threadSafeRegistry) GetChunksOwnedBy(coordinatorID string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var chunks []string
	for hash, owners := range r.owners {
		for _, owner := range owners {
			if owner == coordinatorID {
				chunks = append(chunks, hash)
				break
			}
		}
	}
	return chunks, nil
}

// TestConcurrent_DeleteAndRead tests concurrent deletes and reads.
func TestConcurrent_DeleteAndRead(t *testing.T) {
	dir := t.TempDir()

	store := newTestStore(t, dir)

	ctx := context.Background()

	// Create bucket and upload files
	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "test-owner", 2))

	const numFiles = 50
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("file-%d.txt", i)
		data := []byte(fmt.Sprintf("Content %d", i))
		_, err := store.PutObject(ctx, "test-bucket", key, bytes.NewReader(data), int64(len(data)), "text/plain", nil)
		require.NoError(t, err)
	}

	// Start concurrent readers and deleters
	var wg sync.WaitGroup
	stopAll := make(chan struct{})
	deleted := atomic.Int32{}
	successfulReads := atomic.Int32{}
	expectedMissing := atomic.Int32{}

	// Launch readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				select {
				case <-stopAll:
					return
				default:
					key := fmt.Sprintf("file-%d.txt", workerID%numFiles)
					reader, _, err := store.GetObject(ctx, "test-bucket", key)
					if err == nil {
						_, _ = io.Copy(io.Discard, reader)
						_ = reader.Close()
						successfulReads.Add(1)
					} else if errors.Is(err, ErrObjectNotFound) {
						expectedMissing.Add(1)
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// Launch deleters
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			time.Sleep(5 * time.Millisecond) // Let readers get started

			for j := 0; j < 10; j++ {
				select {
				case <-stopAll:
					return
				default:
					key := fmt.Sprintf("file-%d.txt", workerID*10+j)
					err := store.DeleteObject(ctx, "test-bucket", key)
					if err == nil {
						deleted.Add(1)
					}
				}
			}
		}(i)
	}

	// Run for a bit
	time.Sleep(50 * time.Millisecond)
	close(stopAll)
	wg.Wait()

	t.Logf("Deleted: %d, Successful reads: %d, Expected missing: %d",
		deleted.Load(), successfulReads.Load(), expectedMissing.Load())

	// Verify store is still functional (don't check exact counts due to race timing)
	_, _, _, err := store.ListObjects(ctx, "test-bucket", "", "", 10000)
	require.NoError(t, err)

	// Verify we had concurrent activity
	assert.Greater(t, deleted.Load(), int32(0))
	assert.Greater(t, successfulReads.Load(), int32(0))
}

// TestConcurrent_BucketOperations tests concurrent bucket creates/deletes.
func TestConcurrent_BucketOperations(t *testing.T) {
	dir := t.TempDir()

	store := newTestStore(t, dir)

	ctx := context.Background()

	var wg sync.WaitGroup
	const numWorkers = 10
	errs := make(chan error, numWorkers*10)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			bucketName := fmt.Sprintf("bucket-%d", workerID)
			owner := fmt.Sprintf("owner-%d", workerID)

			// Create bucket
			err := store.CreateBucket(ctx, bucketName, owner, 2)
			if err != nil {
				errs <- fmt.Errorf("create bucket: %w", err)
				return
			}

			// Upload a file
			key := "test.txt"
			data := []byte(fmt.Sprintf("Worker %d", workerID))
			_, err = store.PutObject(ctx, bucketName, key, bytes.NewReader(data), int64(len(data)), "text/plain", nil)
			if err != nil {
				errs <- fmt.Errorf("put object: %w", err)
				return
			}

			// Read it back
			reader, _, err := store.GetObject(ctx, bucketName, key)
			if err != nil {
				errs <- fmt.Errorf("get object: %w", err)
				return
			}
			_, _ = io.Copy(io.Discard, reader)
			_ = reader.Close()

			// Delete object (moves to recycle bin)
			err = store.DeleteObject(ctx, bucketName, key)
			if err != nil {
				errs <- fmt.Errorf("delete object: %w", err)
				return
			}

			// Purge recycle bin so bucket can be deleted
			_ = store.PurgeAllRecycledInBucket(ctx, bucketName)

			// Delete bucket
			err = store.DeleteBucket(ctx, bucketName)
			if err != nil {
				errs <- fmt.Errorf("delete bucket: %w", err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	// Check for errors
	for err := range errs {
		t.Errorf("Bucket operation failed: %v", err)
	}

	// Verify all buckets cleaned up
	buckets, err := store.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Empty(t, buckets)
}

// TestConcurrent_VersioningAndReads tests concurrent versioning operations.
func TestConcurrent_VersioningAndReads(t *testing.T) {
	dir := t.TempDir()

	store := newTestStore(t, dir)

	ctx := context.Background()

	// Create bucket
	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "test-owner", 2))

	const key = "versioned-file.txt"

	// Upload initial version
	_, err := store.PutObject(ctx, "test-bucket", key, bytes.NewReader([]byte("v0")), 2, "text/plain", nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	stopAll := make(chan struct{})

	// Concurrent readers
	readCount := atomic.Int32{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-stopAll:
					return
				default:
					reader, _, err := store.GetObject(ctx, "test-bucket", key)
					if err == nil {
						_, _ = io.Copy(io.Discard, reader)
						_ = reader.Close()
						readCount.Add(1)
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// Concurrent writers (creating versions)
	writeCount := atomic.Int32{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				select {
				case <-stopAll:
					return
				default:
					content := fmt.Sprintf("v%d-%d", workerID, j)
					_, err := store.PutObject(ctx, "test-bucket", key, bytes.NewReader([]byte(content)), int64(len(content)), "text/plain", nil)
					if err == nil {
						writeCount.Add(1)
					}
					time.Sleep(2 * time.Millisecond)
				}
			}
		}(i)
	}

	// Run for a bit
	time.Sleep(50 * time.Millisecond)
	close(stopAll)
	wg.Wait()

	t.Logf("Reads: %d, Writes: %d", readCount.Load(), writeCount.Load())

	// Verify versions were created
	versions, err := store.ListVersions(ctx, "test-bucket", key)
	require.NoError(t, err)
	assert.Greater(t, len(versions), 1) // Should have multiple versions
}
