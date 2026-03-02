package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestReplicatorWithQueue(t *testing.T) (*Replicator, *mockTransport, *mockS3Store) {
	t.Helper()
	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	r := NewReplicator(Config{
		NodeID:              "coord-test",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0, // Disable auto-sync for unit tests
	})
	t.Cleanup(func() { _ = r.Stop() })
	return r, transport, s3Store
}

func TestEnqueueReplication_Dedup(t *testing.T) {
	r, _, _ := createTestReplicatorWithQueue(t)

	// Enqueue same key multiple times
	r.EnqueueReplication("bucket1", "key1", "put")
	r.EnqueueReplication("bucket1", "key1", "put")
	r.EnqueueReplication("bucket1", "key1", "put")

	// Count entries in the pending map
	count := 0
	r.replPending.Range(func(_, _ any) bool {
		count++
		return true
	})

	assert.Equal(t, 1, count, "Duplicate keys should be deduped to 1 entry")
}

func TestEnqueueReplication_DeleteSupersedesPut(t *testing.T) {
	r, _, _ := createTestReplicatorWithQueue(t)

	// Enqueue a PUT then a DELETE for the same key
	r.EnqueueReplication("bucket1", "key1", "put")
	r.EnqueueReplication("bucket1", "key1", "delete")

	// The DELETE should supersede the PUT
	compositeKey := "bucket1\x00key1"
	val, ok := r.replPending.Load(compositeKey)
	require.True(t, ok, "Entry should exist")

	entry := val.(*replQueueEntry)
	assert.Equal(t, "delete", entry.op, "DELETE should supersede PUT")
}

func TestEnqueueReplication_DifferentKeys(t *testing.T) {
	r, _, _ := createTestReplicatorWithQueue(t)

	r.EnqueueReplication("bucket1", "key1", "put")
	r.EnqueueReplication("bucket1", "key2", "put")
	r.EnqueueReplication("bucket2", "key1", "put")

	count := 0
	r.replPending.Range(func(_, _ any) bool {
		count++
		return true
	})

	assert.Equal(t, 3, count, "Different keys should all be queued")
}

func TestDrainReplicationQueue_NoPeers(t *testing.T) {
	r, _, _ := createTestReplicatorWithQueue(t)

	// Enqueue without adding peers — drain should be a no-op
	r.EnqueueReplication("bucket1", "key1", "put")

	r.drainReplicationQueue(context.Background())

	// Entry should be consumed (removed from pending map)
	count := 0
	r.replPending.Range(func(_, _ any) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "Pending map should be empty after drain")
}

func TestDrainReplicationQueue_Put(t *testing.T) {
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	registryA := newMockChunkRegistry()
	registryB := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           broker.newTransportFor("coord-a"),
		S3Store:             s3a,
		ChunkRegistry:       registryA,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	require.NoError(t, rA.Start())
	defer func() { _ = rA.Stop() }()

	rB := NewReplicator(Config{
		NodeID:        "coord-b",
		Transport:     broker.newTransportFor("coord-b"),
		S3Store:       s3b,
		ChunkRegistry: registryB,
		Logger:        zerolog.Nop(),
	})
	require.NoError(t, rB.Start())
	defer func() { _ = rB.Stop() }()

	rA.AddPeer("coord-b", "coord-b")

	// Add object on coord-a
	chunks := []string{"hash1", "hash2"}
	chunkData := map[string][]byte{
		"hash1": []byte("data1"),
		"hash2": []byte("data2"),
	}
	s3a.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	// Enqueue and drain
	rA.EnqueueReplication("bucket1", "file.txt", "put")
	rA.drainReplicationQueue(context.Background())

	// Wait for async processing
	time.Sleep(100 * time.Millisecond)

	// Verify chunks were replicated to coord-b
	for _, hash := range chunks {
		exists := s3b.ChunkExists(context.Background(), hash)
		assert.True(t, exists, "Chunk %s should be replicated to coord-b", hash)
	}
}

func TestDrainReplicationQueue_Delete(t *testing.T) {
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()

	rA := NewReplicator(Config{
		NodeID:           "coord-a",
		Transport:        broker.newTransportFor("coord-a"),
		S3Store:          s3a,
		Logger:           zerolog.Nop(),
		AutoSyncInterval: 0,
	})
	require.NoError(t, rA.Start())
	defer func() { _ = rA.Stop() }()

	rB := NewReplicator(Config{
		NodeID:    "coord-b",
		Transport: broker.newTransportFor("coord-b"),
		S3Store:   s3b,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, rB.Start())
	defer func() { _ = rB.Stop() }()

	rA.AddPeer("coord-b", "coord-b")

	// Add object on coord-b to be deleted
	_ = s3b.Put(context.Background(), "bucket1", "file.txt", []byte("data"), "text/plain", nil)

	// Enqueue delete and drain
	rA.EnqueueReplication("bucket1", "file.txt", "delete")
	rA.drainReplicationQueue(context.Background())

	// Wait for async processing
	time.Sleep(100 * time.Millisecond)

	// Verify object was deleted on coord-b
	_, _, err := s3b.Get(context.Background(), "bucket1", "file.txt")
	assert.Error(t, err, "Object should be deleted on coord-b")
}

func TestReEnqueueOnFailure_MaxRetries(t *testing.T) {
	r, _, _ := createTestReplicatorWithQueue(t)

	// Create an entry that has already been retried 3 times
	entry := &replQueueEntry{
		bucket:  "bucket1",
		key:     "key1",
		op:      "put",
		retries: 3,
	}

	r.reEnqueueOnFailure(entry)

	// Should NOT be re-enqueued (max retries exceeded)
	compositeKey := "bucket1\x00key1"
	_, loaded := r.replPending.Load(compositeKey)
	assert.False(t, loaded, "Entry should not be re-enqueued after max retries")
}

func TestReEnqueueOnFailure_IncrementRetry(t *testing.T) {
	r, _, _ := createTestReplicatorWithQueue(t)

	entry := &replQueueEntry{
		bucket:  "bucket1",
		key:     "key1",
		op:      "put",
		retries: 1,
	}

	r.reEnqueueOnFailure(entry)

	compositeKey := "bucket1\x00key1"
	val, loaded := r.replPending.Load(compositeKey)
	require.True(t, loaded, "Entry should be re-enqueued")

	reEntry := val.(*replQueueEntry)
	assert.Equal(t, 2, reEntry.retries, "Retry count should be incremented")
}

func TestReEnqueueOnFailure_NewerEntryPreserved(t *testing.T) {
	r, _, _ := createTestReplicatorWithQueue(t)

	// Add a newer entry first
	r.EnqueueReplication("bucket1", "key1", "delete")

	// Try to re-enqueue an older failed PUT
	entry := &replQueueEntry{
		bucket:  "bucket1",
		key:     "key1",
		op:      "put",
		retries: 0,
	}

	r.reEnqueueOnFailure(entry)

	// The newer DELETE should still be there
	compositeKey := "bucket1\x00key1"
	val, loaded := r.replPending.Load(compositeKey)
	require.True(t, loaded)

	reEntry := val.(*replQueueEntry)
	assert.Equal(t, "delete", reEntry.op, "Newer entry should be preserved")
}

func TestDrainReplicationQueue_BoundedConcurrency(t *testing.T) {
	// Verify that drain processes entries with bounded concurrency (5 concurrent).
	// We enqueue many delete operations (which don't need chunk ACKs)
	// and verify they all get processed.
	transport := newMockTransport()
	s3Store := newMockS3Store()

	r := NewReplicator(Config{
		NodeID:           "coord-test",
		Transport:        transport,
		S3Store:          s3Store,
		Logger:           zerolog.Nop(),
		AutoSyncInterval: 0,
	})
	t.Cleanup(func() { _ = r.Stop() })

	r.AddPeer("coord-peer", "coord-peer")

	// Enqueue 20 delete operations
	for i := 0; i < 20; i++ {
		key := "key" + string(rune('a'+i))
		r.EnqueueReplication("bucket", key, "delete")
	}

	// The drain function uses a semaphore of size 5, so we verify that
	// all 20 entries get processed (drain completes without hanging)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.drainReplicationQueue(ctx)

	// Verify all entries were consumed
	count := 0
	r.replPending.Range(func(_, _ any) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "All entries should be consumed by drain")
}

func TestShutdownDrain(t *testing.T) {
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	registryA := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           broker.newTransportFor("coord-a"),
		S3Store:             s3a,
		ChunkRegistry:       registryA,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	require.NoError(t, rA.Start())

	rB := NewReplicator(Config{
		NodeID:    "coord-b",
		Transport: broker.newTransportFor("coord-b"),
		S3Store:   s3b,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, rB.Start())
	defer func() { _ = rB.Stop() }()

	rA.AddPeer("coord-b", "coord-b")

	// Add object and enqueue
	chunks := []string{"hash1"}
	chunkData := map[string][]byte{"hash1": []byte("data1")}
	s3a.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)
	rA.EnqueueReplication("bucket1", "file.txt", "put")

	// Stop should drain the queue
	err := rA.Stop()
	require.NoError(t, err)

	// Wait for rB to receive and store the chunk. Stop() drains the local send queue
	// but delivery to rB is asynchronous — use Eventually so Windows isn't flaky.
	assert.Eventually(t, func() bool {
		return s3b.ChunkExists(context.Background(), "hash1")
	}, 5*time.Second, 50*time.Millisecond, "Chunk should be replicated during shutdown drain")
}

func TestRunReplicationQueueWorker_SignalWakeup(t *testing.T) {
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	registryA := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           broker.newTransportFor("coord-a"),
		S3Store:             s3a,
		ChunkRegistry:       registryA,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	require.NoError(t, rA.Start())
	defer func() { _ = rA.Stop() }()

	rB := NewReplicator(Config{
		NodeID:    "coord-b",
		Transport: broker.newTransportFor("coord-b"),
		S3Store:   s3b,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, rB.Start())
	defer func() { _ = rB.Stop() }()

	rA.AddPeer("coord-b", "coord-b")

	// Add object
	chunks := []string{"hash1"}
	chunkData := map[string][]byte{"hash1": []byte("data1")}
	s3a.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	// Enqueue — the worker should wake up via the signal channel
	rA.EnqueueReplication("bucket1", "file.txt", "put")

	// Wait for the worker to process (should be fast since it's signal-driven)
	assert.Eventually(t, func() bool {
		return s3b.ChunkExists(context.Background(), "hash1")
	}, 5*time.Second, 50*time.Millisecond, "Worker should process enqueued replication via signal")
}

func TestAutoSyncCycle(t *testing.T) {
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	registryA := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           broker.newTransportFor("coord-a"),
		S3Store:             s3a,
		ChunkRegistry:       registryA,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0, // We'll call runAutoSyncCycle manually
	})
	require.NoError(t, rA.Start())
	defer func() { _ = rA.Stop() }()

	rB := NewReplicator(Config{
		NodeID:    "coord-b",
		Transport: broker.newTransportFor("coord-b"),
		S3Store:   s3b,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, rB.Start())
	defer func() { _ = rB.Stop() }()

	rA.AddPeer("coord-b", "coord-b")

	// Add multiple objects
	for _, key := range []string{"file1.txt", "file2.txt"} {
		chunks := []string{"hash-" + key}
		chunkData := map[string][]byte{
			"hash-" + key: []byte("data-" + key),
		}
		s3a.addObjectWithChunks("bucket1", key, chunks, chunkData)
	}

	// Run auto-sync cycle manually
	rA.runAutoSyncCycle()

	// Drain the queue (since worker might not process immediately in test)
	rA.drainReplicationQueue(context.Background())

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)

	// Verify all objects were replicated
	for _, key := range []string{"file1.txt", "file2.txt"} {
		hash := "hash-" + key
		exists := s3b.ChunkExists(context.Background(), hash)
		assert.True(t, exists, "Chunk %s should be replicated via auto-sync", hash)
	}
}

func TestAutoSyncCycle_NoPeers(t *testing.T) {
	r, _, s3Store := createTestReplicatorWithQueue(t)

	// Add objects but no peers
	s3Store.addObjectWithChunks("bucket1", "file.txt", []string{"hash1"}, map[string][]byte{"hash1": []byte("data")})

	// Should be a no-op
	r.runAutoSyncCycle()

	// Verify nothing was enqueued
	count := 0
	r.replPending.Range(func(_, _ any) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "Nothing should be enqueued when no peers")
}

func TestChunkPipelining_MultipleChunks(t *testing.T) {
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	registryA := newMockChunkRegistry()
	registryB := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           broker.newTransportFor("coord-a"),
		S3Store:             s3a,
		ChunkRegistry:       registryA,
		ChunkPipelineWindow: 3, // Pipeline window of 3
		Logger:              zerolog.Nop(),
	})
	require.NoError(t, rA.Start())
	defer func() { _ = rA.Stop() }()

	rB := NewReplicator(Config{
		NodeID:        "coord-b",
		Transport:     broker.newTransportFor("coord-b"),
		S3Store:       s3b,
		ChunkRegistry: registryB,
		Logger:        zerolog.Nop(),
	})
	require.NoError(t, rB.Start())
	defer func() { _ = rB.Stop() }()

	rA.AddPeer("coord-b", "coord-b")

	// Create object with 10 chunks
	chunks := make([]string, 10)
	chunkData := make(map[string][]byte)
	for i := 0; i < 10; i++ {
		hash := "chunkhash" + string(rune('0'+i))
		chunks[i] = hash
		chunkData[hash] = []byte("data" + string(rune('0'+i)))
	}
	s3a.addObjectWithChunks("bucket1", "bigfile.bin", chunks, chunkData)

	// Replicate
	err := rA.ReplicateObject(context.Background(), "bucket1", "bigfile.bin", "coord-b")
	require.NoError(t, err)

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)

	// Verify all chunks were replicated
	for _, hash := range chunks {
		exists := s3b.ChunkExists(context.Background(), hash)
		assert.True(t, exists, "Chunk %s should be replicated", hash)
	}
}

func TestChunkPipelining_WindowOne(t *testing.T) {
	// Window of 1 should behave like sequential
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	registryA := newMockChunkRegistry()
	registryB := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           broker.newTransportFor("coord-a"),
		S3Store:             s3a,
		ChunkRegistry:       registryA,
		ChunkPipelineWindow: 1, // Sequential
		Logger:              zerolog.Nop(),
	})
	require.NoError(t, rA.Start())
	defer func() { _ = rA.Stop() }()

	rB := NewReplicator(Config{
		NodeID:        "coord-b",
		Transport:     broker.newTransportFor("coord-b"),
		S3Store:       s3b,
		ChunkRegistry: registryB,
		Logger:        zerolog.Nop(),
	})
	require.NoError(t, rB.Start())
	defer func() { _ = rB.Stop() }()

	rA.AddPeer("coord-b", "coord-b")

	chunks := []string{"hash-a", "hash-b", "hash-c"}
	chunkData := map[string][]byte{
		"hash-a": []byte("data-a"),
		"hash-b": []byte("data-b"),
		"hash-c": []byte("data-c"),
	}
	s3a.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	err := rA.ReplicateObject(context.Background(), "bucket1", "file.txt", "coord-b")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	for _, hash := range chunks {
		exists := s3b.ChunkExists(context.Background(), hash)
		assert.True(t, exists, "Chunk %s should be replicated with window=1", hash)
	}
}

func TestProcessQueuePut_ParallelPeerReplication(t *testing.T) {
	// Verify that processQueuePut replicates to multiple peers in parallel.
	// With 3 coordinators and RF=2, each chunk is assigned to 2 coordinators.
	// We use enough chunks that both peers get some assigned chunks.
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	s3c := newMockS3Store()
	registryA := newMockChunkRegistry()
	registryB := newMockChunkRegistry()
	registryC := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           broker.newTransportFor("coord-a"),
		S3Store:             s3a,
		ChunkRegistry:       registryA,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	require.NoError(t, rA.Start())
	defer func() { _ = rA.Stop() }()

	rB := NewReplicator(Config{
		NodeID:              "coord-b",
		Transport:           broker.newTransportFor("coord-b"),
		S3Store:             s3b,
		ChunkRegistry:       registryB,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
	})
	require.NoError(t, rB.Start())
	defer func() { _ = rB.Stop() }()

	rC := NewReplicator(Config{
		NodeID:              "coord-c",
		Transport:           broker.newTransportFor("coord-c"),
		S3Store:             s3c,
		ChunkRegistry:       registryC,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
	})
	require.NoError(t, rC.Start())
	defer func() { _ = rC.Stop() }()

	rA.AddPeer("coord-b", "coord-b")
	rA.AddPeer("coord-c", "coord-c")

	// Create object with 6 chunks so striping distributes across all coordinators
	chunks := make([]string, 6)
	chunkData := make(map[string][]byte)
	for i := 0; i < 6; i++ {
		hash := fmt.Sprintf("hash%d", i)
		chunks[i] = hash
		chunkData[hash] = []byte(fmt.Sprintf("data%d", i))
	}
	s3a.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	// Enqueue and drain
	rA.EnqueueReplication("bucket1", "file.txt", "put")
	rA.drainReplicationQueue(context.Background())

	// Wait for async chunk processing
	time.Sleep(500 * time.Millisecond)

	// With RF=2 and 3 coordinators, each peer should get ~4 of 6 chunks.
	// Verify both peers received at least some chunks (proves parallel execution).
	bCount := 0
	cCount := 0
	for _, hash := range chunks {
		if s3b.ChunkExists(context.Background(), hash) {
			bCount++
		}
		if s3c.ChunkExists(context.Background(), hash) {
			cCount++
		}
	}
	assert.Greater(t, bCount, 0, "coord-b should have received some chunks")
	assert.Greater(t, cCount, 0, "coord-c should have received some chunks")
}

func TestProcessQueuePut_ReEnqueuesOnPartialFailure(t *testing.T) {
	// Verify that when one peer fails, the entry is re-enqueued for retry.
	// coord-c has no replicator registered, so sending to it fails immediately.
	// Note: rA is NOT started (no background worker) so re-enqueued entries stay in replPending.
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	registryA := newMockChunkRegistry()
	registryB := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           broker.newTransportFor("coord-a"),
		S3Store:             s3a,
		ChunkRegistry:       registryA,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	// Don't call rA.Start() — we drain manually to avoid the worker consuming re-enqueued entries
	t.Cleanup(func() { _ = rA.Stop() })

	rB := NewReplicator(Config{
		NodeID:              "coord-b",
		Transport:           broker.newTransportFor("coord-b"),
		S3Store:             s3b,
		ChunkRegistry:       registryB,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
	})
	require.NoError(t, rB.Start())
	defer func() { _ = rB.Stop() }()

	// Add coord-b (reachable) and coord-c (no replicator = unreachable)
	rA.AddPeer("coord-b", "coord-b")
	rA.AddPeer("coord-c", "coord-c")

	// Create object with 6 chunks for realistic striping
	chunks := make([]string, 6)
	chunkData := make(map[string][]byte)
	for i := 0; i < 6; i++ {
		hash := fmt.Sprintf("hash%d", i)
		chunks[i] = hash
		chunkData[hash] = []byte(fmt.Sprintf("data%d", i))
	}
	s3a.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	// Enqueue and drain manually
	rA.EnqueueReplication("bucket1", "file.txt", "put")
	rA.drainReplicationQueue(context.Background())

	// Wait for async chunk processing on receiver side
	time.Sleep(500 * time.Millisecond)

	// Entry should be re-enqueued because coord-c is unreachable
	count := 0
	rA.replPending.Range(func(_, _ any) bool {
		count++
		return true
	})
	assert.Equal(t, 1, count, "Entry should be re-enqueued due to partial failure (coord-c unreachable)")

	// coord-b should have received its assigned chunks
	bCount := 0
	for _, hash := range chunks {
		if s3b.ChunkExists(context.Background(), hash) {
			bCount++
		}
	}
	assert.Greater(t, bCount, 0, "coord-b should have received some chunks despite coord-c failure")
}

func TestAutoSyncCycle_SendsManifest(t *testing.T) {
	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	r := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	t.Cleanup(func() { _ = r.Stop() })

	r.AddPeer("coord-b", "coord-b")

	// Add some objects
	s3Store.addObjectWithChunks("bucket1", "file1.txt", []string{"h1"}, map[string][]byte{"h1": []byte("d1")})
	s3Store.addObjectWithChunks("bucket1", "file2.txt", []string{"h2"}, map[string][]byte{"h2": []byte("d2")})
	s3Store.addObjectWithChunks("bucket2", "doc.txt", []string{"h3"}, map[string][]byte{"h3": []byte("d3")})

	// Run auto-sync cycle — this enqueues puts AND sends the manifest
	r.runAutoSyncCycle()

	// Check that the transport received a manifest message
	transport.mu.Lock()
	messages := transport.sentMessages
	transport.mu.Unlock()

	var foundManifest bool
	for _, sent := range messages {
		msg, err := UnmarshalMessage(sent.data)
		if err != nil {
			continue
		}
		if msg.Type == MessageTypeObjectManifest {
			foundManifest = true
			var payload ObjectManifestPayload
			require.NoError(t, json.Unmarshal(msg.Payload, &payload))
			assert.Contains(t, payload.BucketKeys, "bucket1")
			assert.Contains(t, payload.BucketKeys, "bucket2")
			assert.ElementsMatch(t, []string{"file1.txt", "file2.txt"}, payload.BucketKeys["bucket1"])
			assert.ElementsMatch(t, []string{"doc.txt"}, payload.BucketKeys["bucket2"])
			break
		}
	}
	assert.True(t, foundManifest, "Auto-sync should send an object manifest to peers")
}

func TestAutoSyncCycle_EmptyStoreSkipsManifest(t *testing.T) {
	// An empty object store (e.g. during startup before S3 loads) must NOT
	// send a manifest — an empty manifest would cause replicas to purge everything.
	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	r := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	t.Cleanup(func() { _ = r.Stop() })

	r.AddPeer("coord-b", "coord-b")

	// No objects in the store — run auto-sync
	r.runAutoSyncCycle()

	// No manifest should be sent
	transport.mu.Lock()
	messages := transport.sentMessages
	transport.mu.Unlock()

	for _, sent := range messages {
		msg, err := UnmarshalMessage(sent.data)
		if err != nil {
			continue
		}
		assert.NotEqual(t, MessageTypeObjectManifest, msg.Type,
			"Empty store must not send manifest (could cause mass purge on replicas)")
	}
}

func TestManifestReconciliation_SkipsBucketsNotOwnedBySender(t *testing.T) {
	// In a multi-coordinator setup, coordinator A should not be able to
	// purge objects from buckets owned by coordinator C on this node.
	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	r := NewReplicator(Config{
		NodeID:              "coord-b",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	t.Cleanup(func() { _ = r.Stop() })

	// Register coord-a and coord-c as peers (mesh IP → name mapping)
	r.AddPeer("10.0.0.1", "coord-a")
	r.AddPeer("10.0.0.3", "coord-c")

	// coord-b has objects from two different owners:
	// - bucket-a owned by coord-a (replicated from coord-a)
	// - bucket-c owned by coord-c (replicated from coord-c)
	s3Store.addObjectWithChunks("bucket-a", "file1.txt", []string{"h1"}, map[string][]byte{"h1": []byte("d1")})
	s3Store.addObjectWithChunks("bucket-c", "file1.txt", []string{"h2"}, map[string][]byte{"h2": []byte("d2")})
	s3Store.bucketOwners = map[string]string{
		"bucket-a": "coord-a",
		"bucket-c": "coord-c",
	}

	// coord-a sends a manifest listing bucket-a (empty) and bucket-c (empty).
	// Without ownership check, this would purge both. With the fix, only bucket-a
	// objects should be purged because coord-a doesn't own bucket-c.
	// Note: msg.From is a mesh IP (overridden by handleIncomingMessage in production)
	payload := ObjectManifestPayload{
		BucketKeys: map[string][]string{
			"bucket-a": {}, // coord-a removed all objects from its bucket
			"bucket-c": {}, // coord-a claims bucket-c is empty too (wrong!)
		},
	}
	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeObjectManifest,
		ID:      "test-manifest",
		From:    "10.0.0.1", // Sender's mesh IP (resolved to "coord-a" via peerNames)
		Payload: json.RawMessage(payloadJSON),
	}

	err = r.handleObjectManifest(msg)
	require.NoError(t, err)

	// bucket-a/file1.txt should be purged (coord-a owns bucket-a, and file is not in manifest)
	_, _, err = s3Store.Get(context.Background(), "bucket-a", "file1.txt")
	assert.Error(t, err, "bucket-a/file1.txt should be purged (sender owns this bucket)")

	// bucket-c/file1.txt should NOT be purged (coord-a doesn't own bucket-c)
	_, _, err = s3Store.Get(context.Background(), "bucket-c", "file1.txt")
	assert.NoError(t, err, "bucket-c/file1.txt should survive (sender doesn't own this bucket)")
}

func TestAutoSyncCycle_SkipsReplicatedBuckets(t *testing.T) {
	tests := []struct {
		name        string
		nodeID      string
		peerMeshIP  string
		peerName    string
		bucketOwner string // owner of the replicated bucket
	}{
		{
			name:        "same_identifier",
			nodeID:      "coord-b",
			peerMeshIP:  "coord-a",
			peerName:    "coord-a",
			bucketOwner: "coord-a",
		},
		{
			name:        "mesh_ip_vs_coordinator_name",
			nodeID:      "coordinator-2",
			peerMeshIP:  "10.0.0.1",
			peerName:    "coordinator-1",
			bucketOwner: "coordinator-1", // Owner is coordinator name, NOT mesh IP
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := newMockTransport()
			s3Store := newMockS3Store()
			registry := newMockChunkRegistry()

			r := NewReplicator(Config{
				NodeID:              tc.nodeID,
				Transport:           transport,
				S3Store:             s3Store,
				ChunkRegistry:       registry,
				Logger:              zerolog.Nop(),
				ChunkPipelineWindow: 5,
				AutoSyncInterval:    0,
			})
			t.Cleanup(func() { _ = r.Stop() })

			r.AddPeer(tc.peerMeshIP, tc.peerName)

			s3Store.addObjectWithChunks("local-bucket", "local.txt", []string{"h1"}, map[string][]byte{"h1": []byte("d1")})
			s3Store.addObjectWithChunks("replicated-bucket", "remote.txt", []string{"h2"}, map[string][]byte{"h2": []byte("d2")})
			s3Store.bucketOwners = map[string]string{
				"local-bucket":      "alice",
				"replicated-bucket": tc.bucketOwner,
			}

			r.runAutoSyncCycle()

			transport.mu.Lock()
			messages := transport.sentMessages
			transport.mu.Unlock()

			var manifestPayload ObjectManifestPayload
			var foundManifest bool
			for _, sent := range messages {
				msg, err := UnmarshalMessage(sent.data)
				if err != nil {
					continue
				}
				if msg.Type == MessageTypeObjectManifest {
					foundManifest = true
					require.NoError(t, json.Unmarshal(msg.Payload, &manifestPayload))
					break
				}
			}

			require.True(t, foundManifest, "Should send manifest for locally-owned buckets")
			assert.Contains(t, manifestPayload.BucketKeys, "local-bucket",
				"Manifest should include locally-owned bucket")
			assert.NotContains(t, manifestPayload.BucketKeys, "replicated-bucket",
				"Manifest should NOT include replicated bucket (owned by peer)")
		})
	}
}

func TestManifestReconciliation_PurgesDeletedBuckets(t *testing.T) {
	// After a file share (or any bucket) is deleted on the primary coordinator,
	// the next manifest won't include that bucket at all. Replica coordinators
	// must purge all local objects in buckets that are:
	//   a) owned by the sender, AND
	//   b) absent from the sender's manifest (= sender has deleted the bucket)
	// This is the primary fix for inter-run object accumulation in s3bench.
	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	r := NewReplicator(Config{
		NodeID:              "coord-b",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	t.Cleanup(func() { _ = r.Stop() })

	r.AddPeer("10.0.0.1", "coord-a")

	// coord-b has two file-share buckets replicated from coord-a.
	// coord-a then deletes "fs+deleted-share" but keeps "fs+active-share".
	s3Store.addObjectWithChunks("fs+active-share", "doc1.txt", []string{"h1"}, map[string][]byte{"h1": []byte("d1")})
	s3Store.addObjectWithChunks("fs+active-share", "doc2.txt", []string{"h2"}, map[string][]byte{"h2": []byte("d2")})
	s3Store.addObjectWithChunks("fs+deleted-share", "doc1.txt", []string{"h3"}, map[string][]byte{"h3": []byte("d3")})
	s3Store.addObjectWithChunks("fs+deleted-share", "doc2.txt", []string{"h4"}, map[string][]byte{"h4": []byte("d4")})
	s3Store.bucketOwners = map[string]string{
		"fs+active-share":  "coord-a",
		"fs+deleted-share": "coord-a",
	}

	// coord-a sends a manifest that includes only fs+active-share.
	// fs+deleted-share is absent because coord-a called ForceDeleteBucket on it.
	payload := ObjectManifestPayload{
		BucketKeys: map[string][]string{
			"fs+active-share": {"doc1.txt", "doc2.txt"}, // still alive
			// fs+deleted-share intentionally absent — coord-a deleted it
		},
	}
	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeObjectManifest,
		ID:      "test-manifest-deleted",
		From:    "10.0.0.1",
		Payload: json.RawMessage(payloadJSON),
	}

	err = r.handleObjectManifest(msg)
	require.NoError(t, err)

	// fs+active-share objects must survive (they're in the manifest)
	_, _, err = s3Store.Get(context.Background(), "fs+active-share", "doc1.txt")
	assert.NoError(t, err, "active share doc1.txt should survive")
	_, _, err = s3Store.Get(context.Background(), "fs+active-share", "doc2.txt")
	assert.NoError(t, err, "active share doc2.txt should survive")

	// fs+deleted-share objects must be purged (sender deleted the whole bucket)
	_, _, err = s3Store.Get(context.Background(), "fs+deleted-share", "doc1.txt")
	assert.Error(t, err, "deleted share doc1.txt must be purged")
	_, _, err = s3Store.Get(context.Background(), "fs+deleted-share", "doc2.txt")
	assert.Error(t, err, "deleted share doc2.txt must be purged")
}

func TestManifestReconciliation_UnknownSenderSkipsPurge(t *testing.T) {
	// If the sender's mesh IP is not in our peerNames map (e.g. peer not yet
	// registered), senderName is empty and GetBucketOwner returns a non-empty
	// name, so the ownership check prevents any purge.
	transport := newMockTransport()
	s3Store := newMockS3Store()

	r := NewReplicator(Config{
		NodeID:    "coord-b",
		Transport: transport,
		S3Store:   s3Store,
		Logger:    zerolog.Nop(),
	})
	t.Cleanup(func() { _ = r.Stop() })

	// No peers registered → getPeerName returns ""
	s3Store.addObjectWithChunks("bucket-a", "file.txt", []string{"h1"}, map[string][]byte{"h1": []byte("d1")})
	s3Store.bucketOwners = map[string]string{"bucket-a": "coord-a"}

	payload := ObjectManifestPayload{BucketKeys: map[string][]string{}} // empty manifest
	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeObjectManifest,
		ID:      "test-unknown-sender",
		From:    "10.0.0.99", // unknown IP
		Payload: json.RawMessage(payloadJSON),
	}

	err = r.handleObjectManifest(msg)
	require.NoError(t, err)

	// Object must NOT be purged (sender is unknown, no ownership match)
	_, _, err = s3Store.Get(context.Background(), "bucket-a", "file.txt")
	assert.NoError(t, err, "object must survive when sender is unknown")
}

// TestTriggerManifestSync_NonBlocking verifies that TriggerManifestSync is
// non-blocking and idempotent: multiple rapid calls produce at most one pending
// signal in the buffered channel.
func TestTriggerManifestSync_NonBlocking(t *testing.T) {
	r := NewReplicator(Config{
		NodeID:           "coord-a",
		Transport:        newMockTransport(),
		S3Store:          newMockS3Store(),
		Logger:           zerolog.Nop(),
		AutoSyncInterval: 0,
	})
	t.Cleanup(func() { _ = r.Stop() })

	// Channel starts empty.
	assert.Equal(t, 0, len(r.manifestSyncCh), "channel should start empty")

	// First call queues a signal.
	r.TriggerManifestSync()
	assert.Equal(t, 1, len(r.manifestSyncCh), "one signal should be queued after first call")

	// Second call must be a no-op (channel already full) and must not block.
	done := make(chan struct{})
	go func() {
		r.TriggerManifestSync()
		close(done)
	}()
	select {
	case <-done:
		// Good: returned without blocking.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("TriggerManifestSync blocked unexpectedly")
	}

	// Still only one pending signal.
	assert.Equal(t, 1, len(r.manifestSyncCh), "channel should still hold exactly one signal")
}

// TestTriggerManifestSync_WorkerDrains verifies that the auto-sync worker picks
// up the on-demand trigger and calls runAutoSyncCycle (evidenced by a manifest
// message being sent to peers).
func TestTriggerManifestSync_WorkerDrains(t *testing.T) {
	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	// Use a very long ticker interval so only the on-demand path fires.
	r := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    24 * time.Hour, // effectively never ticks during test
	})
	require.NoError(t, r.Start())
	t.Cleanup(func() { _ = r.Stop() })

	r.AddPeer("coord-b", "coord-b")

	// Put an object so the manifest is non-empty (empty manifests are suppressed).
	s3Store.addObjectWithChunks("bucket1", "file.txt", []string{"h1"}, map[string][]byte{"h1": []byte("data")})

	// Signal the on-demand sync — worker must wake and send manifest.
	r.TriggerManifestSync()

	// Wait for the manifest message to arrive (worker runs asynchronously).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		transport.mu.Lock()
		msgs := transport.sentMessages
		transport.mu.Unlock()

		for _, sent := range msgs {
			msg, err := UnmarshalMessage(sent.data)
			if err != nil {
				continue
			}
			if msg.Type == MessageTypeObjectManifest {
				// Channel should be drained by the worker now.
				assert.Equal(t, 0, len(r.manifestSyncCh), "worker should drain manifestSyncCh")
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("worker did not send manifest within 2 seconds of TriggerManifestSync")
}

// TestTriggerManifestSync_Deduplication verifies that two calls that both arrive
// while the worker is busy processing the first result in exactly one cycle, not two.
// It uses preSyncHook to pause the worker at a deterministic point, ensuring the
// second TriggerManifestSync call races with an in-progress cycle (not a future one).
// TestAutoSyncCycle_PrunesReplicatedStateVectors verifies that runAutoSyncCycle
// prunes state vectors for replicated (non-locally-owned) objects after they
// are deleted on the primary coordinator.  Previously, activeSet was built from
// localKeys only, so replicated objects' state vectors were never eligible for
// pruning even after the objects were removed from disk.
func TestAutoSyncCycle_PrunesReplicatedStateVectors(t *testing.T) {
	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	r := NewReplicator(Config{
		NodeID:              "coord-b",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	t.Cleanup(func() { _ = r.Stop() })

	r.AddPeer("10.0.0.1", "coord-a")

	// Simulate coord-b receiving a replicated object from coord-a.
	// The object exists on disk and its state vector is tracked.
	s3Store.addObjectWithChunks("replicated-bucket", "doc.txt", []string{"h1"}, map[string][]byte{"h1": []byte("d1")})
	s3Store.bucketOwners = map[string]string{
		"replicated-bucket": "coord-a",
	}
	r.state.Merge("replicated-bucket", "doc.txt", VersionVector{"coord-a": 1})
	assert.Equal(t, 1, r.state.Count(), "state should track the replicated object")

	// Run auto-sync: the object is still on disk, so its state vector must NOT be pruned.
	r.runAutoSyncCycle()
	assert.Equal(t, 1, r.state.Count(), "live replicated object state vector must be preserved")

	// Now simulate the object being deleted on the primary and removed from disk.
	s3Store.mu.Lock()
	delete(s3Store.objects, "replicated-bucket/doc.txt")
	s3Store.mu.Unlock()

	// Back-date the state vector so it's past GCGracePeriod.
	r.state.mu.Lock()
	r.state.lastUpdated["replicated-bucket/doc.txt"] = time.Now().Add(-20 * time.Minute)
	r.state.mu.Unlock()

	// Run auto-sync again: the object is now absent from disk (not in allKeys),
	// so its state vector is eligible for pruning.
	r.runAutoSyncCycle()
	assert.Equal(t, 0, r.state.Count(),
		"deleted replicated object state vector must be pruned after GCGracePeriod")
}

func TestTriggerManifestSync_Deduplication(t *testing.T) {
	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	r := NewReplicator(Config{
		NodeID:              "coord-a",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    24 * time.Hour,
	})

	// Hook: block the worker at the start of its first cycle until we release it.
	// This guarantees the second TriggerManifestSync call arrives while the cycle
	// is in progress and the channel is empty (worker already consumed the first signal).
	workerStarted := make(chan struct{})
	releaseCh := make(chan struct{})
	r.preSyncHook = func() {
		close(workerStarted) // signal: worker is inside the cycle, channel now empty
		<-releaseCh          // block until test releases
		r.preSyncHook = nil  // run hook only for the first cycle
	}

	require.NoError(t, r.Start())
	t.Cleanup(func() { _ = r.Stop() })

	r.AddPeer("coord-b", "coord-b")
	s3Store.addObjectWithChunks("bucket1", "file.txt", []string{"h1"}, map[string][]byte{"h1": []byte("data")})

	// First trigger: worker wakes from manifestSyncCh, enters the hook, and blocks.
	r.TriggerManifestSync()
	<-workerStarted // worker has read the channel; channel is now empty

	// Second trigger: channel is empty, so this queues a real signal.
	r.TriggerManifestSync()
	assert.Equal(t, 1, len(r.manifestSyncCh), "second trigger queued while worker is busy")

	// Release: worker finishes cycle 1 then drains the second signal (no cycle 2).
	close(releaseCh)

	// Wait for the manifest from cycle 1.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		transport.mu.Lock()
		manifestCount := 0
		for _, sent := range transport.sentMessages {
			msg, err := UnmarshalMessage(sent.data)
			if err != nil {
				continue
			}
			if msg.Type == MessageTypeObjectManifest {
				manifestCount++
			}
		}
		transport.mu.Unlock()

		if manifestCount >= 1 {
			// Give the worker a moment to potentially fire a second time.
			time.Sleep(100 * time.Millisecond)

			transport.mu.Lock()
			finalCount := 0
			for _, sent := range transport.sentMessages {
				msg, err := UnmarshalMessage(sent.data)
				if err != nil {
					continue
				}
				if msg.Type == MessageTypeObjectManifest {
					finalCount++
				}
			}
			transport.mu.Unlock()

			assert.Equal(t, 1, finalCount,
				"trigger queued during an in-progress cycle should be drained, not cause a second cycle")
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("worker did not send manifest within 2 seconds")
}
