package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sentMessage records a single sent message for test assertions.
type sentMessage struct {
	to   string
	data []byte
}

// mockTransport implements Transport for testing.
type mockTransport struct {
	mu           sync.Mutex
	sent         map[string][]byte // map[coordMeshIP]lastMessage
	sentMessages []sentMessage     // all sent messages in order
	handler      func(from string, data []byte) error
	sendErr      error // If set, SendToCoordinator will return this error
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		sent: make(map[string][]byte),
	}
}

func (m *mockTransport) SendToCoordinator(ctx context.Context, coordMeshIP string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sendErr != nil {
		return m.sendErr
	}

	copied := append([]byte(nil), data...)
	m.sent[coordMeshIP] = copied
	m.sentMessages = append(m.sentMessages, sentMessage{to: coordMeshIP, data: copied})
	return nil
}

func (m *mockTransport) RegisterHandler(handler func(from string, data []byte) error) {
	m.handler = handler
}

func (m *mockTransport) simulateReceive(from string, data []byte) error {
	if m.handler == nil {
		return fmt.Errorf("no handler registered")
	}
	return m.handler(from, data)
}

func (m *mockTransport) getLastSent(coordMeshIP string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sent[coordMeshIP]
}

// mockS3Store implements S3Store for testing.
type mockS3Store struct {
	mu             sync.Mutex
	objects        map[string]mockS3Object   // map["bucket/key"]object
	chunks         map[string][]byte         // map[hash]data (for chunk-level operations)
	versionHistory map[string][]VersionEntry // map["bucket/key:versions"]entries
	bucketOwners   map[string]string         // map[bucket]ownerNodeID
	putErr         error                     // If set, Put will return this error
	metaErr        error                     // If set, GetObjectMeta will return this error
	chunkErr       error                     // If set, chunk operations will return this error
	chunkRequests  int                       // Count of ReadChunk calls
}

type mockS3Object struct {
	data        []byte
	contentType string
	metadata    map[string]string
	chunks      []string                  // Chunk hashes (for chunk-level replication)
	chunkMeta   map[string]*ChunkMetadata // Per-chunk metadata
}

func newMockS3Store() *mockS3Store {
	return &mockS3Store{
		objects: make(map[string]mockS3Object),
		chunks:  make(map[string][]byte),
	}
}

func (m *mockS3Store) makeKey(bucket, key string) string {
	return bucket + "/" + key
}

func (m *mockS3Store) Get(ctx context.Context, bucket, key string) ([]byte, map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	obj, exists := m.objects[m.makeKey(bucket, key)]
	if !exists {
		return nil, nil, fmt.Errorf("object not found")
	}

	// Return empty map if metadata is nil
	metadata := obj.metadata
	if metadata == nil {
		metadata = make(map[string]string)
	}

	// Add content-type to metadata (mimics real S3 behavior where content-type is in metadata)
	if obj.contentType != "" {
		metadata["content-type"] = obj.contentType
	}

	return obj.data, metadata, nil
}

func (m *mockS3Store) Put(ctx context.Context, bucket, key string, data []byte, contentType string, metadata map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.putErr != nil {
		return m.putErr
	}

	m.objects[m.makeKey(bucket, key)] = mockS3Object{
		data:        append([]byte(nil), data...),
		contentType: contentType,
		metadata:    metadata,
	}

	return nil
}

func (m *mockS3Store) Delete(ctx context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, m.makeKey(bucket, key))
	return nil
}

func (m *mockS3Store) List(ctx context.Context, bucket string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var keys []string
	prefix := bucket + "/"
	for k := range m.objects {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k[len(prefix):])
		}
	}
	return keys, nil
}

func (m *mockS3Store) ListBuckets(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	buckets := make(map[string]bool)
	for k := range m.objects {
		// Extract bucket name
		for i, c := range k {
			if c == '/' {
				buckets[k[:i]] = true
				break
			}
		}
	}

	result := make([]string, 0, len(buckets))
	for b := range buckets {
		result = append(result, b)
	}
	return result, nil
}

// GetObjectMeta returns object metadata (for chunk-level replication).
func (m *mockS3Store) GetObjectMeta(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.metaErr != nil {
		return nil, m.metaErr
	}

	obj, exists := m.objects[m.makeKey(bucket, key)]
	if !exists {
		return nil, fmt.Errorf("object not found")
	}

	return &ObjectMeta{
		Key:           key,
		Size:          int64(len(obj.data)),
		ContentType:   obj.contentType,
		Metadata:      obj.metadata,
		Chunks:        obj.chunks,
		ChunkMetadata: obj.chunkMeta,
	}, nil
}

// ReadChunk reads a chunk from CAS.
func (m *mockS3Store) ChunkExists(_ context.Context, hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.chunks[hash]
	return exists
}

func (m *mockS3Store) ReadChunk(ctx context.Context, hash string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.chunkRequests++

	if m.chunkErr != nil {
		return nil, m.chunkErr
	}

	data, exists := m.chunks[hash]
	if !exists {
		return nil, fmt.Errorf("chunk not found: %s", hash)
	}

	return data, nil
}

// WriteChunkDirect writes a chunk to CAS.
func (m *mockS3Store) WriteChunkDirect(ctx context.Context, hash string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.chunkErr != nil {
		return m.chunkErr
	}

	m.chunks[hash] = append([]byte(nil), data...)
	return nil
}

// ImportObjectMeta writes object metadata directly.
func (m *mockS3Store) ImportObjectMeta(ctx context.Context, bucket, key string, metaJSON []byte, bucketOwner string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.putErr != nil {
		return nil, m.putErr
	}

	// Decode meta to get basic info
	var meta struct {
		Key         string            `json:"key"`
		Size        int64             `json:"size"`
		ContentType string            `json:"content_type"`
		Metadata    map[string]string `json:"metadata,omitempty"`
		Chunks      []string          `json:"chunks,omitempty"`
	}
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal meta: %w", err)
	}

	m.objects[m.makeKey(bucket, key)] = mockS3Object{
		data:        nil, // No data — chunks arrive separately
		contentType: meta.ContentType,
		metadata:    meta.Metadata,
		chunks:      meta.Chunks,
	}
	return nil, nil
}

// DeleteUnreferencedChunks is a no-op in the mock.
func (m *mockS3Store) DeleteUnreferencedChunks(_ context.Context, _ []string) int64 {
	return 0
}

// GetBucketReplicationFactor returns the replication factor for a bucket.
func (m *mockS3Store) GetBucketReplicationFactor(_ context.Context, _ string) int {
	return 2 // Default for tests
}

// DeleteChunk removes a chunk from CAS.
func (m *mockS3Store) DeleteChunk(ctx context.Context, hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.chunkErr != nil {
		return m.chunkErr
	}

	delete(m.chunks, hash)
	return nil
}

// PurgeObject permanently removes an object and its chunks (no tombstone).
func (m *mockS3Store) PurgeObject(ctx context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, m.makeKey(bucket, key))
	return nil
}

// GetVersionHistory returns version history entries for an object.
func (m *mockS3Store) GetVersionHistory(_ context.Context, bucket, key string) ([]VersionEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.makeKey(bucket, key) + ":versions"
	versions, exists := m.versionHistory[k]
	if !exists {
		return nil, nil
	}
	// Return a copy
	result := make([]VersionEntry, len(versions))
	copy(result, versions)
	return result, nil
}

// ImportVersionHistory imports version entries, deduplicating by versionID.
func (m *mockS3Store) ImportVersionHistory(_ context.Context, bucket, key string, versions []VersionEntry) (int, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.makeKey(bucket, key) + ":versions"
	if m.versionHistory == nil {
		m.versionHistory = make(map[string][]VersionEntry)
	}

	existing := make(map[string]bool)
	for _, v := range m.versionHistory[k] {
		existing[v.VersionID] = true
	}

	imported := 0
	for _, v := range versions {
		if existing[v.VersionID] {
			continue
		}
		m.versionHistory[k] = append(m.versionHistory[k], v)
		existing[v.VersionID] = true
		imported++
	}

	return imported, nil, nil
}

// GetAllObjectKeys returns all object keys grouped by bucket.
func (m *mockS3Store) GetAllObjectKeys(_ context.Context) (map[string][]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[string][]string)
	for k := range m.objects {
		parts := splitFirst(k, '/')
		if len(parts) == 2 {
			result[parts[0]] = append(result[parts[0]], parts[1])
		}
	}
	return result, nil
}

// GetBucketOwner returns the owner of a bucket.
func (m *mockS3Store) GetBucketOwner(_ context.Context, bucket string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.bucketOwners != nil {
		return m.bucketOwners[bucket]
	}
	return ""
}

// splitFirst splits a string at the first occurrence of sep.
func splitFirst(s string, sep byte) []string {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return []string{s[:i], s[i+1:]}
		}
	}
	return []string{s}
}

// addObjectWithChunks adds an object with chunk-level metadata (helper for tests).
func (m *mockS3Store) addObjectWithChunks(bucket, key string, chunks []string, chunkData map[string][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Add chunks to CAS
	chunkMetadata := make(map[string]*ChunkMetadata)
	var totalSize int64
	for _, hash := range chunks {
		if data, ok := chunkData[hash]; ok {
			m.chunks[hash] = append([]byte(nil), data...)
			chunkMetadata[hash] = &ChunkMetadata{
				Hash: hash,
				Size: int64(len(data)),
			}
			totalSize += int64(len(data))
		}
	}

	// Reconstruct full data (calculate size for preallocation)
	var size int
	for _, hash := range chunks {
		size += len(m.chunks[hash])
	}
	data := make([]byte, 0, size)
	for _, hash := range chunks {
		data = append(data, m.chunks[hash]...)
	}

	// Add object
	m.objects[m.makeKey(bucket, key)] = mockS3Object{
		data:        data,
		contentType: "application/octet-stream",
		metadata:    make(map[string]string),
		chunks:      chunks,
		chunkMeta:   chunkMetadata,
	}
}

// testTransportBroker routes messages between multiple coordinators in tests.
type testTransportBroker struct {
	mu       sync.Mutex
	handlers map[string]func(from string, data []byte) error // map[coordID]handler
}

func newTestTransportBroker() *testTransportBroker {
	return &testTransportBroker{
		handlers: make(map[string]func(from string, data []byte) error),
	}
}

// newTransportFor creates a transport for a specific coordinator.
func (b *testTransportBroker) newTransportFor(coordID string) *brokerTransport {
	return &brokerTransport{
		broker:  b,
		coordID: coordID,
	}
}

// brokerTransport is a per-coordinator transport that routes via the broker.
type brokerTransport struct {
	broker  *testTransportBroker
	coordID string // This coordinator's ID
}

func (t *brokerTransport) SendToCoordinator(ctx context.Context, destCoordID string, data []byte) error {
	t.broker.mu.Lock()
	handler := t.broker.handlers[destCoordID]
	t.broker.mu.Unlock()

	if handler == nil {
		return fmt.Errorf("no handler registered for %s", destCoordID)
	}

	// Deliver synchronously for deterministic tests
	return handler(t.coordID, data)
}

func (t *brokerTransport) RegisterHandler(handler func(from string, data []byte) error) {
	t.broker.mu.Lock()
	defer t.broker.mu.Unlock()
	t.broker.handlers[t.coordID] = handler
}

// mockChunkRegistry implements ChunkRegistryInterface for testing.
type mockChunkRegistry struct {
	mu        sync.Mutex
	ownership map[string]map[string]bool // map[chunkHash]map[coordID]bool
}

func newMockChunkRegistry() *mockChunkRegistry {
	return &mockChunkRegistry{
		ownership: make(map[string]map[string]bool),
	}
}

func (m *mockChunkRegistry) RegisterChunk(hash string, size int64) error {
	return nil // No-op for tests
}

func (m *mockChunkRegistry) UnregisterChunk(hash string) error {
	return nil // No-op for tests
}

func (m *mockChunkRegistry) GetOwners(hash string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var owners []string
	if coords, ok := m.ownership[hash]; ok {
		for coordID := range coords {
			owners = append(owners, coordID)
		}
	}
	return owners, nil
}

func (m *mockChunkRegistry) GetChunksOwnedBy(coordID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var chunks []string
	for hash, coords := range m.ownership {
		if coords[coordID] {
			chunks = append(chunks, hash)
		}
	}
	return chunks, nil
}

func (m *mockChunkRegistry) AddOwner(hash string, coordID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ownership[hash] == nil {
		m.ownership[hash] = make(map[string]bool)
	}
	m.ownership[hash][coordID] = true
	return nil
}

// setOwnership sets chunk ownership for testing.
func (m *mockChunkRegistry) setOwnership(hash string, coordIDs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ownership[hash] = make(map[string]bool)
	for _, coordID := range coordIDs {
		m.ownership[hash][coordID] = true
	}
}

func TestNewReplicator(t *testing.T) {
	transport := newMockTransport()
	s3 := newMockS3Store()
	logger := zerolog.Nop()

	config := Config{
		NodeID:    "coord1.tunnelmesh:8443",
		Transport: transport,
		S3Store:   s3,
		Logger:    logger,
	}

	r := NewReplicator(config)
	require.NotNil(t, r)
	assert.Equal(t, "coord1.tunnelmesh:8443", r.nodeID)
	assert.NotNil(t, transport.handler) // Handler should be registered
}

func TestReplicator_AddRemovePeer(t *testing.T) {
	r := createTestReplicator(t)

	// Initially no peers
	assert.Empty(t, r.GetPeers())

	// Add peers
	r.AddPeer("coord2.mesh", "coord2.mesh")
	r.AddPeer("coord3.mesh", "coord3.mesh")

	peers := r.GetPeers()
	assert.Len(t, peers, 2)
	assert.Contains(t, peers, "coord2.mesh")
	assert.Contains(t, peers, "coord3.mesh")

	// Add duplicate (should be idempotent)
	r.AddPeer("coord2.mesh", "coord2.mesh")
	assert.Len(t, r.GetPeers(), 2)

	// Remove peer
	r.RemovePeer("coord2.mesh")
	peers = r.GetPeers()
	assert.Len(t, peers, 1)
	assert.Contains(t, peers, "coord3.mesh")

	// Remove non-existent peer (should not error)
	r.RemovePeer("nonexistent.mesh")
	assert.Len(t, r.GetPeers(), 1)
}

func TestReplicator_HandleReplicate(t *testing.T) {
	transport := newMockTransport()
	s3 := newMockS3Store()
	r := createTestReplicatorWithMocks(t, transport, s3)

	// Create a replicate message
	payload := ReplicatePayload{
		Bucket:        "system",
		Key:           "test.json",
		Data:          []byte(`{"test":"data"}`),
		VersionVector: VersionVector{"coord2.mesh": 1},
		ContentType:   "application/json",
	}

	msg, err := NewReplicateMessage("msg-001", "coord2.mesh", payload)
	require.NoError(t, err)

	data, err := msg.Marshal()
	require.NoError(t, err)

	// Simulate receiving the message
	err = transport.simulateReceive("coord2.mesh", data)
	assert.NoError(t, err)

	// Verify data was written to S3
	storedData, storedMeta, err := s3.Get(context.Background(), "system", "test.json")
	require.NoError(t, err)
	assert.Equal(t, payload.Data, storedData)
	assert.NotNil(t, storedMeta)

	// Verify version vector was merged
	vv := r.state.Get("system", "test.json")
	assert.Equal(t, uint64(1), vv.Get("coord2.mesh"))
}

func TestReplicator_HandleReplicate_Conflict(t *testing.T) {
	transport := newMockTransport()
	s3 := newMockS3Store()
	r := createTestReplicatorWithMocks(t, transport, s3)

	// Create local version (simulating concurrent update)
	r.state.Update("system", "config.json")
	r.state.Update("system", "config.json")

	// Store local data
	localData := []byte(`{"local":"data"}`)
	err := s3.Put(context.Background(), "system", "config.json", localData, "application/json", nil)
	require.NoError(t, err)

	// Receive concurrent update from coord2
	payload := ReplicatePayload{
		Bucket:        "system",
		Key:           "config.json",
		Data:          []byte(`{"remote":"data"}`),
		VersionVector: VersionVector{"coord1.tunnelmesh:8443": 1, "coord2.mesh": 1},
		ContentType:   "application/json",
	}

	msg, err := NewReplicateMessage("msg-002", "coord2.mesh", payload)
	require.NoError(t, err)

	data, err := msg.Marshal()
	require.NoError(t, err)

	// Simulate receiving the message
	err = transport.simulateReceive("coord2.mesh", data)
	assert.NoError(t, err)

	// Conflict should have been detected
	stats := r.GetStats()
	assert.Greater(t, stats.ConflictCount, uint64(0))

	// Version vectors should be merged
	vv := r.state.Get("system", "config.json")
	assert.Equal(t, uint64(2), vv.Get("coord1.tunnelmesh:8443"))
	assert.Equal(t, uint64(1), vv.Get("coord2.mesh"))
}

func TestReplicator_HandleAck(t *testing.T) {
	transport := newMockTransport()
	s3 := newMockS3Store()
	r := createTestReplicatorWithMocks(t, transport, s3)

	// Track a pending ACK
	msgID := "test-replicate-001"
	r.trackPendingACK(msgID, "system", "test.json")

	// Create ACK message
	ackPayload := AckPayload{
		ReplicateID:   msgID,
		Success:       true,
		VersionVector: VersionVector{"coord1.tunnelmesh:8443": 1, "coord2.mesh": 1},
	}

	ackMsg, err := NewAckMessage("ack-001", "coord2.mesh", ackPayload)
	require.NoError(t, err)

	ackData, err := ackMsg.Marshal()
	require.NoError(t, err)

	// Get pending entry before sending ACK (it will be removed after ACK is delivered)
	r.pendingMu.RLock()
	pending, exists := r.pending[msgID]
	ackChan := pending.ackChan // Keep reference to channel before it's removed
	r.pendingMu.RUnlock()

	assert.True(t, exists)

	// Simulate receiving ACK
	err = transport.simulateReceive("coord2.mesh", ackData)
	assert.NoError(t, err)

	// Try to receive from ackChan (should be available)
	// Note: The pending entry is removed immediately after ACK delivery (security fix #6)
	select {
	case ack := <-ackChan:
		assert.True(t, ack.Success)
		assert.Equal(t, msgID, ack.ReplicateID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ACK was not delivered to pending channel")
	}

	// Verify the pending entry was cleaned up (security fix #6)
	r.pendingMu.RLock()
	_, stillExists := r.pending[msgID]
	r.pendingMu.RUnlock()
	assert.False(t, stillExists, "Pending entry should be removed after ACK delivery")
}

func TestReplicator_GetStats(t *testing.T) {
	r := createTestReplicator(t)

	// Initial stats
	stats := r.GetStats()
	assert.Equal(t, uint64(0), stats.SentCount)
	assert.Equal(t, uint64(0), stats.ReceivedCount)
	assert.Equal(t, uint64(0), stats.ConflictCount)
	assert.Equal(t, uint64(0), stats.ErrorCount)
	assert.Equal(t, 0, stats.PeerCount)
	assert.Equal(t, 0, stats.PendingACKs)

	// Add peers
	r.AddPeer("coord2.mesh", "coord2.mesh")
	r.AddPeer("coord3.mesh", "coord3.mesh")

	stats = r.GetStats()
	assert.Equal(t, 2, stats.PeerCount)

	// Simulate some operations
	r.incrementSentCount()
	r.incrementSentCount()
	r.incrementReceivedCount()
	r.incrementConflictCount()
	r.incrementErrorCount()

	stats = r.GetStats()
	assert.Equal(t, uint64(2), stats.SentCount)
	assert.Equal(t, uint64(1), stats.ReceivedCount)
	assert.Equal(t, uint64(1), stats.ConflictCount)
	assert.Equal(t, uint64(1), stats.ErrorCount)
}

func TestReplicator_StartStop(t *testing.T) {
	r := createTestReplicator(t)

	// Start
	err := r.Start()
	assert.NoError(t, err)

	// Give background workers a moment to start
	time.Sleep(50 * time.Millisecond)

	// Stop
	err = r.Stop()
	assert.NoError(t, err)
}

func TestReplicator_TransportError(t *testing.T) {
	transport := newMockTransport()
	transport.sendErr = fmt.Errorf("network error")

	s3 := newMockS3Store()
	r := createTestReplicatorWithMocks(t, transport, s3)

	r.AddPeer("coord2.mesh", "coord2.mesh")

	// ReplicateDelete should return error when transport fails
	err := r.ReplicateDelete(context.Background(), "system", "test.json")
	assert.Error(t, err)

	// Error count should be incremented
	stats := r.GetStats()
	assert.Greater(t, stats.ErrorCount, uint64(0))
}

func TestReplicator_S3Error(t *testing.T) {
	transport := newMockTransport()
	s3 := newMockS3Store()
	s3.putErr = fmt.Errorf("s3 error")

	r := createTestReplicatorWithMocks(t, transport, s3)

	// Receive a replicate message
	payload := ReplicatePayload{
		Bucket:        "system",
		Key:           "test.json",
		Data:          []byte("data"),
		VersionVector: VersionVector{"coord2.mesh": 1},
	}

	msg, err := NewReplicateMessage("msg-001", "coord2.mesh", payload)
	require.NoError(t, err)

	data, err := msg.Marshal()
	require.NoError(t, err)

	// Should handle error gracefully
	err = transport.simulateReceive("coord2.mesh", data)
	assert.NoError(t, err) // Handler doesn't propagate S3 errors

	// Error count should be incremented
	stats := r.GetStats()
	assert.Greater(t, stats.ErrorCount, uint64(0))
}

// Helper functions

func createTestReplicator(t *testing.T) *Replicator {
	transport := newMockTransport()
	s3 := newMockS3Store()
	return createTestReplicatorWithMocks(t, transport, s3)
}

func createTestReplicatorWithMocks(t *testing.T, transport *mockTransport, s3 *mockS3Store) *Replicator {
	logger := zerolog.Nop()

	config := Config{
		NodeID:    "coord1.tunnelmesh:8443",
		Transport: transport,
		S3Store:   s3,
		Logger:    logger,
	}

	r := NewReplicator(config)
	// Ensure async ACK goroutines complete before goleak checks
	t.Cleanup(func() { _ = r.Stop() })
	return r
}

// ==== Phase 4: Chunk-Level Replication Tests ====

func TestReplicateObject_AllChunksAlreadyReplicated(t *testing.T) {
	ctx := context.Background()
	s3 := newMockS3Store()
	registry := newMockChunkRegistry()
	transport := newMockTransport()

	// Create object with 3 chunks
	chunks := []string{"chunk1", "chunk2", "chunk3"}
	chunkData := map[string][]byte{
		"chunk1": []byte("data1"),
		"chunk2": []byte("data2"),
		"chunk3": []byte("data3"),
	}
	s3.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	// Mark all chunks as already owned by peer
	for _, hash := range chunks {
		registry.setOwnership(hash, []string{"coord-local", "coord-peer"})
	}

	replicator := NewReplicator(Config{
		NodeID:        "coord-local",
		Transport:     transport,
		S3Store:       s3,
		ChunkRegistry: registry,
		Logger:        zerolog.Nop(),
	})
	t.Cleanup(func() { _ = replicator.Stop() })

	// Replicate to peer (should skip all chunks)
	err := replicator.ReplicateObject(ctx, "bucket1", "file.txt", "coord-peer")
	require.NoError(t, err)

	// Verify no chunk read requests were made
	assert.Equal(t, 0, s3.chunkRequests, "Expected 0 chunk requests when all chunks already replicated")
}

func TestReplicateObject_SomeMissingChunks(t *testing.T) {
	ctx := context.Background()
	senderS3 := newMockS3Store()
	receiverS3 := newMockS3Store()
	registry := newMockChunkRegistry()

	// Create transport broker for message routing
	broker := newTestTransportBroker()

	// Create replicator pair with separate transports
	sender := NewReplicator(Config{
		NodeID:          "coord-sender",
		Transport:       broker.newTransportFor("coord-sender"),
		S3Store:         senderS3,
		ChunkRegistry:   registry,
		ChunkAckTimeout: 1 * time.Second, // Reasonable timeout
		Logger:          zerolog.Nop(),
	})
	t.Cleanup(func() { _ = sender.Stop() })

	receiver := NewReplicator(Config{
		NodeID:        "coord-receiver",
		Transport:     broker.newTransportFor("coord-receiver"),
		S3Store:       receiverS3,
		ChunkRegistry: registry,
		Logger:        zerolog.Nop(),
	})
	t.Cleanup(func() { _ = receiver.Stop() })

	// Receiver is used indirectly via the broker routing messages to its handler
	_ = receiver

	// Create object with 5 chunks on sender
	chunks := []string{"chunk1", "chunk2", "chunk3", "chunk4", "chunk5"}
	chunkData := map[string][]byte{
		"chunk1": []byte("data1"),
		"chunk2": []byte("data2"),
		"chunk3": []byte("data3"),
		"chunk4": []byte("data4"),
		"chunk5": []byte("data5"),
	}
	senderS3.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	// Receiver already has chunks 1, 3, 5
	registry.setOwnership("chunk1", []string{"coord-sender", "coord-receiver"})
	registry.setOwnership("chunk2", []string{"coord-sender"})
	registry.setOwnership("chunk3", []string{"coord-sender", "coord-receiver"})
	registry.setOwnership("chunk4", []string{"coord-sender"})
	registry.setOwnership("chunk5", []string{"coord-sender", "coord-receiver"})

	// Replicate (should only send chunks 2 and 4)
	err := sender.ReplicateObject(ctx, "bucket1", "file.txt", "coord-receiver")
	require.NoError(t, err)

	// Wait for async handlers
	time.Sleep(100 * time.Millisecond)

	// Verify receiver got exactly 2 chunks (chunk2 and chunk4)
	assert.Equal(t, 2, len(receiverS3.chunks), "Expected 2 chunks to be replicated")

	// Verify correct chunks received
	assert.Contains(t, receiverS3.chunks, "chunk2", "chunk2 should be replicated")
	assert.Contains(t, receiverS3.chunks, "chunk4", "chunk4 should be replicated")

	// Verify chunk registry updated with receiver as owner
	owners2, _ := registry.GetOwners("chunk2")
	assert.Contains(t, owners2, "coord-receiver", "Receiver should own chunk2 after replication")

	owners4, _ := registry.GetOwners("chunk4")
	assert.Contains(t, owners4, "coord-receiver", "Receiver should own chunk4 after replication")
}

func TestReplicateObject_ChunkReadError(t *testing.T) {
	ctx := context.Background()
	s3 := newMockS3Store()
	registry := newMockChunkRegistry()
	transport := newMockTransport()

	// Create object
	chunks := []string{"chunk1"}
	chunkData := map[string][]byte{"chunk1": []byte("data1")}
	s3.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	// Mark chunk as needing replication
	registry.setOwnership("chunk1", []string{"coord-local"})

	// Inject read error
	s3.chunkErr = fmt.Errorf("disk error")

	replicator := NewReplicator(Config{
		NodeID:        "coord-local",
		Transport:     transport,
		S3Store:       s3,
		ChunkRegistry: registry,
		Logger:        zerolog.Nop(),
	})
	t.Cleanup(func() { _ = replicator.Stop() })

	// Replicate should fail
	err := replicator.ReplicateObject(ctx, "bucket1", "file.txt", "coord-peer")
	require.Error(t, err, "Expected error when chunk read fails")
	assert.Contains(t, err.Error(), "disk error", "Error should mention chunk error")
}

func TestReplicateObject_ChunkReadError_FallbackSuccess(t *testing.T) {
	ctx := context.Background()
	senderS3 := newMockS3Store()
	peerS3 := newMockS3Store()
	receiverS3 := newMockS3Store()
	registry := newMockChunkRegistry()

	broker := newTestTransportBroker()

	// Use real sha256 hash so FetchChunk integrity check passes
	data := []byte("test chunk data for fallback")
	h := sha256.Sum256(data)
	chunkHash := hex.EncodeToString(h[:])

	// Create object on sender with chunk metadata but inject read error
	chunks := []string{chunkHash}
	chunkData := map[string][]byte{chunkHash: data}
	senderS3.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	// Peer has the chunk (can serve FetchChunk requests)
	peerS3.addObjectWithChunks("bucket1", "file.txt", chunks, chunkData)

	// Chunk needs replication (only sender owns it)
	registry.setOwnership(chunkHash, []string{"coord-sender"})

	// Inject read error on sender (simulates chunk being unavailable locally)
	senderS3.chunkErr = fmt.Errorf("chunk cleaned up")

	sender := NewReplicator(Config{
		NodeID:          "coord-sender",
		Transport:       broker.newTransportFor("coord-sender"),
		S3Store:         senderS3,
		ChunkRegistry:   registry,
		ChunkAckTimeout: 1 * time.Second,
		Logger:          zerolog.Nop(),
	})
	require.NoError(t, sender.Start())
	defer func() { _ = sender.Stop() }()

	// Peer replicator can serve FetchChunk requests
	peer := NewReplicator(Config{
		NodeID:    "coord-peer",
		Transport: broker.newTransportFor("coord-peer"),
		S3Store:   peerS3,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, peer.Start())
	defer func() { _ = peer.Stop() }()

	// Receiver handles incoming replicated chunks
	receiver := NewReplicator(Config{
		NodeID:        "coord-receiver",
		Transport:     broker.newTransportFor("coord-receiver"),
		S3Store:       receiverS3,
		ChunkRegistry: registry,
		Logger:        zerolog.Nop(),
	})
	_ = receiver

	// Sender knows about both peers: coord-peer for fallback fetching,
	// coord-receiver as the replication target (both needed for striping)
	sender.AddPeer("coord-peer", "coord-peer")
	sender.AddPeer("coord-receiver", "coord-receiver")

	// Replicate should succeed via peer fallback
	err := sender.ReplicateObject(ctx, "bucket1", "file.txt", "coord-receiver")
	require.NoError(t, err, "Replication should succeed via peer fallback")

	// Wait for async handlers
	time.Sleep(100 * time.Millisecond)

	// Verify receiver got the chunk
	assert.Contains(t, receiverS3.chunks, chunkHash, "Receiver should have chunk via peer fallback")
}

func TestReplicateObject_ObjectNotFound(t *testing.T) {
	ctx := context.Background()
	s3 := newMockS3Store()
	registry := newMockChunkRegistry()
	transport := newMockTransport()

	replicator := NewReplicator(Config{
		NodeID:        "coord-local",
		Transport:     transport,
		S3Store:       s3,
		ChunkRegistry: registry,
		Logger:        zerolog.Nop(),
	})
	t.Cleanup(func() { _ = replicator.Stop() })

	// Try to replicate non-existent object
	err := replicator.ReplicateObject(ctx, "bucket1", "nonexistent.txt", "coord-peer")
	require.Error(t, err, "Expected error for non-existent object")
	assert.Contains(t, err.Error(), "object not found", "Error should indicate object not found")
}

func TestReplicateObject_NoChunkRegistry(t *testing.T) {
	ctx := context.Background()
	s3 := newMockS3Store()
	transport := newMockTransport()

	// No chunk registry — should return error
	replicator := NewReplicator(Config{
		NodeID:    "coord-local",
		Transport: transport,
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	t.Cleanup(func() { _ = replicator.Stop() })

	err := replicator.ReplicateObject(ctx, "bucket1", "file.txt", "coord-peer")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "chunk registry not configured")
}

// ==== Striping Policy Tests ====

func TestStripingPolicy_PrimaryOwner(t *testing.T) {
	tests := []struct {
		name       string
		peers      []string
		chunkIndex int
		wantOwner  string
	}{
		{
			name:       "single peer",
			peers:      []string{"coord-a"},
			chunkIndex: 0,
			wantOwner:  "coord-a",
		},
		{
			name:       "single peer, any index",
			peers:      []string{"coord-a"},
			chunkIndex: 5,
			wantOwner:  "coord-a",
		},
		{
			name:       "two peers, index 0",
			peers:      []string{"coord-b", "coord-a"}, // will be sorted
			chunkIndex: 0,
			wantOwner:  "coord-a", // sorted: [coord-a, coord-b]
		},
		{
			name:       "two peers, index 1",
			peers:      []string{"coord-b", "coord-a"},
			chunkIndex: 1,
			wantOwner:  "coord-b",
		},
		{
			name:       "three peers, round-robin",
			peers:      []string{"coord-c", "coord-a", "coord-b"},
			chunkIndex: 2,
			wantOwner:  "coord-c", // sorted: [a, b, c], index 2 % 3 = 2 -> c
		},
		{
			name:       "three peers, wraps around",
			peers:      []string{"coord-a", "coord-b", "coord-c"},
			chunkIndex: 3,
			wantOwner:  "coord-a", // 3 % 3 = 0 -> a
		},
		{
			name:       "empty peers",
			peers:      []string{},
			chunkIndex: 0,
			wantOwner:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := NewStripingPolicy(tt.peers)
			got := sp.PrimaryOwner(tt.chunkIndex)
			assert.Equal(t, tt.wantOwner, got)
		})
	}
}

func TestStripingPolicy_AssignedChunks(t *testing.T) {
	// 10 chunks across 3 peers (sorted: [a, b, c])
	sp := NewStripingPolicy([]string{"coord-c", "coord-a", "coord-b"})

	chunksA := sp.AssignedChunks("coord-a", 10)
	chunksB := sp.AssignedChunks("coord-b", 10)
	chunksC := sp.AssignedChunks("coord-c", 10)

	// Round-robin: a gets 0,3,6,9; b gets 1,4,7; c gets 2,5,8
	assert.Equal(t, []int{0, 3, 6, 9}, chunksA)
	assert.Equal(t, []int{1, 4, 7}, chunksB)
	assert.Equal(t, []int{2, 5, 8}, chunksC)

	// All chunks are covered exactly once
	all := make(map[int]bool)
	for _, idx := range chunksA {
		all[idx] = true
	}
	for _, idx := range chunksB {
		all[idx] = true
	}
	for _, idx := range chunksC {
		all[idx] = true
	}
	assert.Len(t, all, 10)

	// Unknown peer gets nothing
	chunksX := sp.AssignedChunks("coord-x", 10)
	assert.Nil(t, chunksX)
}

func TestStripingPolicy_ChunksForPeer_WithReplicationFactor(t *testing.T) {
	// 6 chunks, 3 peers, RF=2
	sp := NewStripingPolicy([]string{"coord-a", "coord-b", "coord-c"})

	chunksA := sp.ChunksForPeer("coord-a", 6, 2)
	chunksB := sp.ChunksForPeer("coord-b", 6, 2)
	chunksC := sp.ChunksForPeer("coord-c", 6, 2)

	// Primary assignments: a=0,3; b=1,4; c=2,5
	// With RF=2, each chunk also goes to the next peer:
	// chunk 0: primary=a(0), replica=b(1) -> a,b
	// chunk 1: primary=b(1), replica=c(2) -> b,c
	// chunk 2: primary=c(2), replica=a(0) -> c,a
	// chunk 3: primary=a(0), replica=b(1) -> a,b
	// chunk 4: primary=b(1), replica=c(2) -> b,c
	// chunk 5: primary=c(2), replica=a(0) -> c,a
	assert.Equal(t, []int{0, 2, 3, 5}, chunksA) // primary 0,3 + replica from c: 2,5
	assert.Equal(t, []int{0, 1, 3, 4}, chunksB) // primary 1,4 + replica from a: 0,3
	assert.Equal(t, []int{1, 2, 4, 5}, chunksC) // primary 2,5 + replica from b: 1,4

	// Each chunk appears exactly RF times across all peers
	chunkCount := make(map[int]int)
	for _, idx := range chunksA {
		chunkCount[idx]++
	}
	for _, idx := range chunksB {
		chunkCount[idx]++
	}
	for _, idx := range chunksC {
		chunkCount[idx]++
	}
	for i := 0; i < 6; i++ {
		assert.Equal(t, 2, chunkCount[i], "chunk %d should appear exactly RF=2 times", i)
	}
}

func TestStripedReplicateObject_Distribution(t *testing.T) {
	// Test that ReplicateObject with striping only sends assigned chunks to each peer
	ctx := context.Background()
	broker := newTestTransportBroker()

	// Create sender coordinator
	senderTransport := broker.newTransportFor("coord-sender")
	senderS3 := newMockS3Store()

	// Create chunk data
	chunkHashes := []string{"hash-0", "hash-1", "hash-2", "hash-3", "hash-4", "hash-5"}
	chunkData := map[string][]byte{
		"hash-0": []byte("data-0"),
		"hash-1": []byte("data-1"),
		"hash-2": []byte("data-2"),
		"hash-3": []byte("data-3"),
		"hash-4": []byte("data-4"),
		"hash-5": []byte("data-5"),
	}

	senderS3.addObjectWithChunks("bucket1", "file.txt", chunkHashes, chunkData)

	// Create chunk registry - sender owns all chunks, peers own none
	registry := NewChunkRegistry("coord-sender", nil)
	for _, hash := range chunkHashes {
		_ = registry.RegisterChunk(hash, int64(len(chunkData[hash])))
	}

	sender := NewReplicator(Config{
		NodeID:        "coord-sender",
		Transport:     senderTransport,
		S3Store:       senderS3,
		ChunkRegistry: registry,
		Logger:        zerolog.Nop(),
	})
	t.Cleanup(func() { _ = sender.Stop() })

	// Add two peers
	sender.AddPeer("coord-peer1", "coord-peer1")
	sender.AddPeer("coord-peer2", "coord-peer2")

	// Track chunks received by each peer via intercepting broker
	var peer1Chunks []string
	var peer2Chunks []string
	var mu sync.Mutex

	for _, peerID := range []string{"coord-peer1", "coord-peer2"} {
		peerTransport := broker.newTransportFor(peerID)
		peerS3 := newMockS3Store()
		peerRegistry := NewChunkRegistry(peerID, nil)

		capturedPeerID := peerID
		receiver := NewReplicator(Config{
			NodeID:        peerID,
			Transport:     peerTransport,
			S3Store:       peerS3,
			ChunkRegistry: peerRegistry,
			Logger:        zerolog.Nop(),
		})
		t.Cleanup(func() { _ = receiver.Stop() })
		_ = receiver

		// Wrap the handler in the broker to intercept messages
		broker.mu.Lock()
		originalHandler := broker.handlers[peerID]
		broker.handlers[peerID] = func(from string, data []byte) error {
			msg, err := UnmarshalMessage(data)
			if err == nil && msg.Type == MessageTypeReplicateChunk {
				var payload ReplicateChunkPayload
				if jsonErr := json.Unmarshal(msg.Payload, &payload); jsonErr == nil {
					mu.Lock()
					if capturedPeerID == "coord-peer1" {
						peer1Chunks = append(peer1Chunks, payload.ChunkHash)
					} else {
						peer2Chunks = append(peer2Chunks, payload.ChunkHash)
					}
					mu.Unlock()
				}
			}
			return originalHandler(from, data)
		}
		broker.mu.Unlock()
	}

	// Replicate to peer1
	err := sender.ReplicateObject(ctx, "bucket1", "file.txt", "coord-peer1")
	require.NoError(t, err)

	// Replicate to peer2
	err = sender.ReplicateObject(ctx, "bucket1", "file.txt", "coord-peer2")
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	// With striping (3 coordinators, RF=2), each peer should get 4 of 6 chunks
	// Total sent should be 8, not 12 (without striping both would get all 6)
	totalSent := len(peer1Chunks) + len(peer2Chunks)
	assert.Less(t, totalSent, 12, "Striping should reduce total chunks sent (got %d)", totalSent)
	assert.Greater(t, len(peer1Chunks), 0, "peer1 should receive some chunks")
	assert.Greater(t, len(peer2Chunks), 0, "peer2 should receive some chunks")
}

func TestStripedReplicateObject_SinglePeer(t *testing.T) {
	// With only one peer (besides self), all chunks should be replicated
	ctx := context.Background()
	broker := newTestTransportBroker()

	senderTransport := broker.newTransportFor("coord-sender")
	senderS3 := newMockS3Store()

	chunkHashes := []string{"hash-0", "hash-1", "hash-2"}
	chunkData := map[string][]byte{
		"hash-0": []byte("data-0"),
		"hash-1": []byte("data-1"),
		"hash-2": []byte("data-2"),
	}

	senderS3.addObjectWithChunks("bucket1", "file.txt", chunkHashes, chunkData)

	registry := NewChunkRegistry("coord-sender", nil)
	for _, hash := range chunkHashes {
		_ = registry.RegisterChunk(hash, int64(len(chunkData[hash])))
	}

	sender := NewReplicator(Config{
		NodeID:        "coord-sender",
		Transport:     senderTransport,
		S3Store:       senderS3,
		ChunkRegistry: registry,
		Logger:        zerolog.Nop(),
	})
	t.Cleanup(func() { _ = sender.Stop() })

	// Only one peer
	sender.AddPeer("coord-peer1", "coord-peer1")

	// Set up receiver
	peerTransport := broker.newTransportFor("coord-peer1")
	peerS3 := newMockS3Store()
	peerReplicator := NewReplicator(Config{
		NodeID:        "coord-peer1",
		Transport:     peerTransport,
		S3Store:       peerS3,
		ChunkRegistry: NewChunkRegistry("coord-peer1", nil),
		Logger:        zerolog.Nop(),
	})
	t.Cleanup(func() { _ = peerReplicator.Stop() })
	_ = peerReplicator

	var receivedChunks []string
	var mu sync.Mutex

	// Wrap the handler in the broker to intercept messages
	broker.mu.Lock()
	originalHandler := broker.handlers["coord-peer1"]
	broker.handlers["coord-peer1"] = func(from string, data []byte) error {
		msg, err := UnmarshalMessage(data)
		if err == nil && msg.Type == MessageTypeReplicateChunk {
			var payload ReplicateChunkPayload
			if jsonErr := json.Unmarshal(msg.Payload, &payload); jsonErr == nil {
				mu.Lock()
				receivedChunks = append(receivedChunks, payload.ChunkHash)
				mu.Unlock()
			}
		}
		return originalHandler(from, data)
	}
	broker.mu.Unlock()

	err := sender.ReplicateObject(ctx, "bucket1", "file.txt", "coord-peer1")
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	// With RF=2 and only 2 coordinators total, all chunks go to the single peer
	assert.Len(t, receivedChunks, 3, "Single peer should receive all chunks with RF=2")
}

// Erasure coding shard distribution tests

func TestShardDistributionForPeer(t *testing.T) {
	// 3 coordinators, 10 data + 3 parity shards
	peers := []string{"coord-a", "coord-b", "coord-c"}
	sp := NewStripingPolicy(peers)

	// Get distribution for each coordinator
	dataA, parityA := sp.ShardDistributionForPeer("coord-a", 10, 3)
	dataB, parityB := sp.ShardDistributionForPeer("coord-b", 10, 3)
	dataC, parityC := sp.ShardDistributionForPeer("coord-c", 10, 3)

	// Verify all data shards are distributed
	totalDataShards := len(dataA) + len(dataB) + len(dataC)
	assert.Equal(t, 10, totalDataShards, "All data shards should be distributed")

	// Verify all parity shards are distributed
	totalParityShards := len(parityA) + len(parityB) + len(parityC)
	assert.Equal(t, 3, totalParityShards, "All parity shards should be distributed")

	// Verify round-robin distribution for data shards
	// coord-a should have indices 0, 3, 6, 9 (i % 3 == 0)
	assert.Equal(t, []int{0, 3, 6, 9}, dataA, "coord-a data shards")
	assert.Equal(t, []int{1, 4, 7}, dataB, "coord-b data shards")
	assert.Equal(t, []int{2, 5, 8}, dataC, "coord-c data shards")

	// Verify parity shards are distributed (with offset to spread load)
	// With 10 data shards, parity starts at index (10+i) % 3
	// parity[0] -> (10+0) % 3 = 1 -> coord-b
	// parity[1] -> (10+1) % 3 = 2 -> coord-c
	// parity[2] -> (10+2) % 3 = 0 -> coord-a
	assert.Equal(t, []int{2}, parityA, "coord-a parity shards")
	assert.Equal(t, []int{0}, parityB, "coord-b parity shards")
	assert.Equal(t, []int{1}, parityC, "coord-c parity shards")
}

func TestShardDistributionForPeerTwoCoordinators(t *testing.T) {
	// 2 coordinators, 6 data + 2 parity shards
	peers := []string{"coord-a", "coord-b"}
	sp := NewStripingPolicy(peers)

	dataA, parityA := sp.ShardDistributionForPeer("coord-a", 6, 2)
	dataB, parityB := sp.ShardDistributionForPeer("coord-b", 6, 2)

	// Verify data distribution
	assert.Equal(t, []int{0, 2, 4}, dataA, "coord-a data shards")
	assert.Equal(t, []int{1, 3, 5}, dataB, "coord-b data shards")

	// Verify parity distribution
	// parity[0] -> (6+0) % 2 = 0 -> coord-a
	// parity[1] -> (6+1) % 2 = 1 -> coord-b
	assert.Equal(t, []int{0}, parityA, "coord-a parity shards")
	assert.Equal(t, []int{1}, parityB, "coord-b parity shards")
}

func TestShardDistributionForPeerSingleCoordinator(t *testing.T) {
	// 1 coordinator, 10 data + 3 parity shards (all on same coordinator)
	peers := []string{"coord-a"}
	sp := NewStripingPolicy(peers)

	dataA, parityA := sp.ShardDistributionForPeer("coord-a", 10, 3)

	// Single coordinator gets all shards
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, dataA, "coord-a should get all data shards")
	assert.Equal(t, []int{0, 1, 2}, parityA, "coord-a should get all parity shards")
}

func TestShardDistributionForPeerUnknownPeer(t *testing.T) {
	peers := []string{"coord-a", "coord-b"}
	sp := NewStripingPolicy(peers)

	data, parity := sp.ShardDistributionForPeer("coord-unknown", 10, 3)

	assert.Nil(t, data, "Unknown peer should get no data shards")
	assert.Nil(t, parity, "Unknown peer should get no parity shards")
}

func TestShardDistributionForPeerEmptyPeers(t *testing.T) {
	sp := NewStripingPolicy([]string{})

	data, parity := sp.ShardDistributionForPeer("coord-a", 10, 3)

	assert.Nil(t, data, "Empty peer list should return nil data shards")
	assert.Nil(t, parity, "Empty peer list should return nil parity shards")
}

func TestGetShardOwner(t *testing.T) {
	peers := []string{"coord-a", "coord-b", "coord-c"}
	sp := NewStripingPolicy(peers)

	// Test data shard ownership (round-robin)
	assert.Equal(t, "coord-a", sp.GetShardOwner(0, false, 10))
	assert.Equal(t, "coord-b", sp.GetShardOwner(1, false, 10))
	assert.Equal(t, "coord-c", sp.GetShardOwner(2, false, 10))
	assert.Equal(t, "coord-a", sp.GetShardOwner(3, false, 10))

	// Test parity shard ownership (offset round-robin)
	// parity[0] with 10 data shards -> (10+0) % 3 = 1 -> coord-b
	// parity[1] with 10 data shards -> (10+1) % 3 = 2 -> coord-c
	// parity[2] with 10 data shards -> (10+2) % 3 = 0 -> coord-a
	assert.Equal(t, "coord-b", sp.GetShardOwner(0, true, 10))
	assert.Equal(t, "coord-c", sp.GetShardOwner(1, true, 10))
	assert.Equal(t, "coord-a", sp.GetShardOwner(2, true, 10))
}

func TestGetShardOwnerEmptyPeers(t *testing.T) {
	sp := NewStripingPolicy([]string{})

	owner := sp.GetShardOwner(0, false, 10)
	assert.Equal(t, "", owner, "Empty peer list should return empty owner")
}

func TestShardDistributionBalanced(t *testing.T) {
	// Test that shard distribution is reasonably balanced across coordinators
	peers := []string{"coord-a", "coord-b", "coord-c", "coord-d"}
	sp := NewStripingPolicy(peers)

	// 20 data + 5 parity shards across 4 coordinators
	distributions := make(map[string]int)

	for _, peer := range peers {
		data, parity := sp.ShardDistributionForPeer(peer, 20, 5)
		distributions[peer] = len(data) + len(parity)
	}

	// Each coordinator should get roughly 25/4 = 6-7 shards
	for peer, count := range distributions {
		assert.GreaterOrEqual(t, count, 5, "peer %s should get at least 5 shards", peer)
		assert.LessOrEqual(t, count, 7, "peer %s should get at most 7 shards", peer)
	}

	// Total should be 25 shards
	total := 0
	for _, count := range distributions {
		total += count
	}
	assert.Equal(t, 25, total, "Total shard count should be 25")
}

func TestShardDistributionSeparation(t *testing.T) {
	// Verify that no single coordinator has all data or all parity shards
	peers := []string{"coord-a", "coord-b", "coord-c"}
	sp := NewStripingPolicy(peers)

	for _, peer := range peers {
		data, parity := sp.ShardDistributionForPeer(peer, 10, 3)

		// No single coordinator should have all 10 data shards
		assert.Less(t, len(data), 10, "peer %s should not have all data shards", peer)

		// No single coordinator should have all 3 parity shards
		assert.Less(t, len(parity), 3, "peer %s should not have all parity shards", peer)

		// Most importantly, no coordinator should have both all data AND all parity
		if len(data) == 10 {
			assert.Less(t, len(parity), 3, "peer %s has all data shards, so should not have all parity shards", peer)
		}
	}
}

// ==== Object Metadata Replication Tests ====

func TestReplicateObjectMeta_SendReceive(t *testing.T) {
	// Setup two coordinators connected via broker transport
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	registryA := newMockChunkRegistry()
	registryB := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:        "coord-a",
		Transport:     broker.newTransportFor("coord-a"),
		S3Store:       s3a,
		ChunkRegistry: registryA,
		Logger:        zerolog.Nop(),
	})
	_ = rA.Start()
	defer func() { _ = rA.Stop() }()

	rB := NewReplicator(Config{
		NodeID:        "coord-b",
		Transport:     broker.newTransportFor("coord-b"),
		S3Store:       s3b,
		ChunkRegistry: registryB,
		Logger:        zerolog.Nop(),
	})
	_ = rB.Start()
	defer func() { _ = rB.Stop() }()

	// Send metadata from A to B
	metaJSON := []byte(`{"key":"test.txt","size":100,"content_type":"text/plain","chunks":["hash1","hash2"]}`)
	err := rA.sendReplicateObjectMeta(context.Background(), "coord-b", "mybucket", "test.txt", metaJSON)
	require.NoError(t, err)

	// Wait briefly for async processing
	time.Sleep(50 * time.Millisecond)

	// Verify B received the metadata
	obj, exists := s3b.objects[s3b.makeKey("mybucket", "test.txt")]
	assert.True(t, exists, "Object metadata should exist on coord-b")
	assert.Equal(t, "text/plain", obj.contentType)
	assert.Equal(t, []string{"hash1", "hash2"}, obj.chunks)
}

func TestHandleReplicateObjectMeta_InvalidPayload(t *testing.T) {
	transport := newMockTransport()
	s3 := newMockS3Store()

	r := NewReplicator(Config{
		NodeID:    "coord-a",
		Transport: transport,
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	t.Cleanup(func() { _ = r.Stop() })

	// Send message with invalid JSON payload
	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeReplicateObjectMeta,
		ID:      "test-1",
		From:    "coord-b",
		Payload: json.RawMessage(`invalid json`),
	}

	err := r.handleReplicateObjectMeta(msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestReplicateObject_SendsMetadata(t *testing.T) {
	// Test that ReplicateObject sends object metadata to the peer
	broker := newTestTransportBroker()
	s3a := newMockS3Store()
	s3b := newMockS3Store()
	registryA := newMockChunkRegistry()
	registryB := newMockChunkRegistry()

	rA := NewReplicator(Config{
		NodeID:        "coord-a",
		Transport:     broker.newTransportFor("coord-a"),
		S3Store:       s3a,
		ChunkRegistry: registryA,
		Logger:        zerolog.Nop(),
	})
	_ = rA.Start()
	defer func() { _ = rA.Stop() }()

	rB := NewReplicator(Config{
		NodeID:        "coord-b",
		Transport:     broker.newTransportFor("coord-b"),
		S3Store:       s3b,
		ChunkRegistry: registryB,
		Logger:        zerolog.Nop(),
	})
	_ = rB.Start()
	defer func() { _ = rB.Stop() }()

	rA.AddPeer("coord-b", "coord-b")

	// Create object on coordinator A with chunks
	chunks := []string{"hash-a", "hash-b"}
	chunkData := map[string][]byte{
		"hash-a": []byte("data-a"),
		"hash-b": []byte("data-b"),
	}
	s3a.addObjectWithChunks("mybucket", "doc.txt", chunks, chunkData)

	// Replicate to coord-b
	err := rA.ReplicateObject(context.Background(), "mybucket", "doc.txt", "coord-b")
	require.NoError(t, err)

	// Wait for async processing
	time.Sleep(50 * time.Millisecond)

	// Verify metadata was received by coord-b
	_, exists := s3b.objects[s3b.makeKey("mybucket", "doc.txt")]
	assert.True(t, exists, "Object metadata should have been replicated to coord-b")

	// Verify chunks were replicated
	for _, hash := range chunks {
		_, exists := s3b.chunks[hash]
		assert.True(t, exists, "Chunk %s should have been replicated to coord-b", hash)
	}
}

func TestHandleReplicateChunk_AsyncACK(t *testing.T) {
	// Verify that handleReplicateChunk returns before the ACK is sent.
	// We use a slow transport that blocks on send to prove the handler
	// doesn't wait for the ACK delivery.
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	ackStarted := make(chan struct{}, 1)
	ackBlocked := make(chan struct{})

	slowTransport := &mockTransport{
		sent: make(map[string][]byte),
	}
	// Override SendToCoordinator to block until we signal
	slowTransport.sendErr = nil

	r := NewReplicator(Config{
		NodeID:        "coord-receiver",
		Transport:     slowTransport,
		S3Store:       s3Store,
		ChunkRegistry: registry,
		Logger:        zerolog.Nop(),
	})
	t.Cleanup(func() {
		close(ackBlocked) // unblock any pending goroutines
		_ = r.Stop()
	})

	// Replace transport's SendToCoordinator to block on ACK send
	origHandler := slowTransport.handler
	blockingTransport := &blockingSendTransport{
		handler:    origHandler,
		ackStarted: ackStarted,
		ackBlocked: ackBlocked,
	}
	// Re-register with the blocking transport
	r.transport = blockingTransport
	blockingTransport.RegisterHandler(r.handleIncomingMessage)

	// Create a chunk replication message
	payload := ReplicateChunkPayload{
		Bucket:      "bucket",
		Key:         "file.txt",
		ChunkHash:   "testhash123",
		ChunkData:   []byte("chunk data"),
		ChunkIndex:  0,
		TotalChunks: 1,
	}

	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeReplicateChunk,
		ID:      "test-chunk-msg",
		From:    "coord-sender",
		Payload: json.RawMessage(payloadJSON),
	}

	data, err := msg.Marshal()
	require.NoError(t, err)

	// Simulate receiving the chunk — handler should return immediately
	err = blockingTransport.simulateReceive("coord-sender", data)
	assert.NoError(t, err)

	// Verify chunk was written (handler completed)
	_, exists := s3Store.chunks["testhash123"]
	assert.True(t, exists, "Chunk should be written before ACK is sent")

	// Wait for the async ACK goroutine to start (proves it's async)
	select {
	case <-ackStarted:
		// Good — the ACK send started in a separate goroutine
	case <-time.After(2 * time.Second):
		t.Fatal("Async ACK goroutine did not start")
	}
}

// blockingSendTransport blocks on SendToCoordinator until signaled.
type blockingSendTransport struct {
	handler    func(from string, data []byte) error
	ackStarted chan struct{}
	ackBlocked chan struct{}
}

func (t *blockingSendTransport) SendToCoordinator(_ context.Context, _ string, _ []byte) error {
	// Signal that the ACK send started
	select {
	case t.ackStarted <- struct{}{}:
	default:
	}
	// Block until test unblocks us
	<-t.ackBlocked
	return nil
}

func (t *blockingSendTransport) RegisterHandler(handler func(from string, data []byte) error) {
	t.handler = handler
}

func (t *blockingSendTransport) simulateReceive(from string, data []byte) error {
	if t.handler == nil {
		return fmt.Errorf("no handler registered")
	}
	return t.handler(from, data)
}

func TestAsyncSendAck_SemaphoreFallback(t *testing.T) {
	// Fill the semaphore to capacity and verify that asyncSendAck falls
	// back to synchronous send without dropping the ACK.
	transport := newMockTransport()
	s3Store := newMockS3Store()

	r := NewReplicator(Config{
		NodeID:    "coord-test",
		Transport: transport,
		S3Store:   s3Store,
		Logger:    zerolog.Nop(),
	})
	t.Cleanup(func() { _ = r.Stop() })

	// Fill the semaphore completely
	for i := 0; i < cap(r.ackSendSem); i++ {
		r.ackSendSem <- struct{}{}
	}

	// asyncSendAck should fall back to synchronous send (default branch)
	vv := VersionVector{"coord-test": 1}
	r.asyncSendAck("msg-fallback", "coord-peer", true, "", vv)

	// Verify the ACK was still sent (sync fallback)
	sentData := transport.getLastSent("coord-peer")
	assert.NotNil(t, sentData, "ACK should be sent synchronously when semaphore is full")

	// Drain the semaphore
	for i := 0; i < cap(r.ackSendSem); i++ {
		<-r.ackSendSem
	}
}

// ==== Capacity Checker Tests ====

// mockCapacityChecker implements CapacityChecker for testing.
type mockCapacityChecker struct {
	results map[string]bool // coordinatorName -> hasCapacity
}

func newMockCapacityChecker() *mockCapacityChecker {
	return &mockCapacityChecker{results: make(map[string]bool)}
}

func (m *mockCapacityChecker) HasCapacityFor(coordinatorName string, _ int64) bool {
	result, ok := m.results[coordinatorName]
	if !ok {
		return true // fail-open like the real implementation
	}
	return result
}

func TestNewReplicator_CapacityCheckerWiring(t *testing.T) {
	checker := newMockCapacityChecker()
	transport := newMockTransport()
	s3Store := newMockS3Store()
	logger := zerolog.Nop()

	r := NewReplicator(Config{
		NodeID:          "coord1",
		Transport:       transport,
		S3Store:         s3Store,
		CapacityChecker: checker,
		Logger:          logger,
	})
	t.Cleanup(func() { _ = r.Stop() })

	assert.NotNil(t, r.capacityChecker, "capacityChecker should be set")
}

func TestNewReplicator_NilCapacityChecker(t *testing.T) {
	transport := newMockTransport()
	s3Store := newMockS3Store()
	logger := zerolog.Nop()

	r := NewReplicator(Config{
		NodeID:    "coord1",
		Transport: transport,
		S3Store:   s3Store,
		Logger:    logger,
	})
	t.Cleanup(func() { _ = r.Stop() })

	assert.Nil(t, r.capacityChecker, "capacityChecker should be nil when not configured")
}

func TestCheckPeerCapacity_NilChecker(t *testing.T) {
	r := createTestReplicator(t)
	// With nil checker, should pass (fail-open)
	err := r.checkPeerCapacity("peer1", "bucket", "key", []string{"chunk1"}, &ObjectMeta{
		ChunkMetadata: map[string]*ChunkMetadata{
			"chunk1": {Hash: "chunk1", Size: 1000},
		},
	})
	assert.NoError(t, err)
}

func TestCheckPeerCapacity_EmptyChunks(t *testing.T) {
	checker := newMockCapacityChecker()
	checker.results["peer1"] = false // would fail if checked

	transport := newMockTransport()
	s3Store := newMockS3Store()
	r := NewReplicator(Config{
		NodeID:          "coord1",
		Transport:       transport,
		S3Store:         s3Store,
		CapacityChecker: checker,
		Logger:          zerolog.Nop(),
	})
	t.Cleanup(func() { _ = r.Stop() })

	// Empty chunks list should pass without checking
	err := r.checkPeerCapacity("peer1", "bucket", "key", []string{}, &ObjectMeta{})
	assert.NoError(t, err)
}

func TestCheckPeerCapacity_SufficientCapacity(t *testing.T) {
	checker := newMockCapacityChecker()
	checker.results["peer1"] = true

	transport := newMockTransport()
	s3Store := newMockS3Store()
	r := NewReplicator(Config{
		NodeID:          "coord1",
		Transport:       transport,
		S3Store:         s3Store,
		CapacityChecker: checker,
		Logger:          zerolog.Nop(),
	})
	t.Cleanup(func() { _ = r.Stop() })

	err := r.checkPeerCapacity("peer1", "bucket", "key", []string{"chunk1", "chunk2"}, &ObjectMeta{
		ChunkMetadata: map[string]*ChunkMetadata{
			"chunk1": {Hash: "chunk1", Size: 500},
			"chunk2": {Hash: "chunk2", Size: 500},
		},
	})
	assert.NoError(t, err)
}

func TestCheckPeerCapacity_InsufficientCapacity(t *testing.T) {
	checker := newMockCapacityChecker()
	checker.results["peer1"] = false

	transport := newMockTransport()
	s3Store := newMockS3Store()
	r := NewReplicator(Config{
		NodeID:          "coord1",
		Transport:       transport,
		S3Store:         s3Store,
		CapacityChecker: checker,
		Logger:          zerolog.Nop(),
	})
	t.Cleanup(func() { _ = r.Stop() })

	err := r.checkPeerCapacity("peer1", "bucket", "key", []string{"chunk1"}, &ObjectMeta{
		ChunkMetadata: map[string]*ChunkMetadata{
			"chunk1": {Hash: "chunk1", Size: 1000},
		},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "near capacity")
}

func TestCheckPeerCapacity_NoMetadataForChunks(t *testing.T) {
	checker := newMockCapacityChecker()
	checker.results["peer1"] = false // would fail if bytes > 0

	transport := newMockTransport()
	s3Store := newMockS3Store()
	r := NewReplicator(Config{
		NodeID:          "coord1",
		Transport:       transport,
		S3Store:         s3Store,
		CapacityChecker: checker,
		Logger:          zerolog.Nop(),
	})
	t.Cleanup(func() { _ = r.Stop() })

	// Chunks with no metadata — estimatedBytes stays 0, so check is skipped
	err := r.checkPeerCapacity("peer1", "bucket", "key", []string{"chunk1"}, &ObjectMeta{
		ChunkMetadata: map[string]*ChunkMetadata{},
	})
	assert.NoError(t, err)
}

func TestHandleReplicateChunk_CapacityExceeded(t *testing.T) {
	checker := newMockCapacityChecker()
	s3Store := newMockS3Store()
	transport := newMockTransport()

	r := NewReplicator(Config{
		NodeID:          "coord-local",
		Transport:       transport,
		S3Store:         s3Store,
		CapacityChecker: checker,
		Logger:          zerolog.Nop(),
	})
	t.Cleanup(func() { _ = r.Stop() })
	require.NoError(t, r.Start())

	// Mark local coordinator as having no capacity
	checker.results["coord-local"] = false

	// Build a chunk replication message
	payload := ReplicateChunkPayload{
		Bucket:      "test-bucket",
		Key:         "test-key",
		ChunkHash:   "abc123",
		ChunkData:   []byte("some chunk data"),
		ChunkIndex:  0,
		TotalChunks: 1,
		ChunkSize:   15,
	}
	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &Message{
		ID:      "msg-1",
		Type:    MessageTypeReplicateChunk,
		From:    "coord-peer",
		Payload: payloadJSON,
	}

	err = r.handleReplicateChunk(msg)
	assert.NoError(t, err) // Returns nil (sends error ACK instead)

	// Verify chunk was NOT written to store
	_, chunkErr := s3Store.ReadChunk(context.Background(), "abc123")
	assert.Error(t, chunkErr, "chunk should not be stored when capacity exceeded")

	// Verify error ACK was sent
	sentData := transport.getLastSent("coord-peer")
	assert.NotNil(t, sentData, "error ACK should be sent")
	if sentData != nil {
		var ackMsg Message
		require.NoError(t, json.Unmarshal(sentData, &ackMsg))
		assert.Equal(t, MessageTypeChunkAck, ackMsg.Type)

		var ackPayload ChunkAckPayload
		require.NoError(t, json.Unmarshal(ackMsg.Payload, &ackPayload))
		assert.False(t, ackPayload.Success)
		assert.Contains(t, ackPayload.Error, "storage capacity exceeded")
	}
}

func TestHandleReplicateChunk_CapacityOK(t *testing.T) {
	checker := newMockCapacityChecker()
	s3Store := newMockS3Store()
	transport := newMockTransport()

	r := NewReplicator(Config{
		NodeID:          "coord-local",
		Transport:       transport,
		S3Store:         s3Store,
		CapacityChecker: checker,
		Logger:          zerolog.Nop(),
	})
	t.Cleanup(func() { _ = r.Stop() })
	require.NoError(t, r.Start())

	// Mark local coordinator as having capacity
	checker.results["coord-local"] = true

	// Build a chunk replication message
	payload := ReplicateChunkPayload{
		Bucket:      "test-bucket",
		Key:         "test-key",
		ChunkHash:   "abc123",
		ChunkData:   []byte("some chunk data"),
		ChunkIndex:  0,
		TotalChunks: 1,
		ChunkSize:   15,
	}
	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &Message{
		ID:      "msg-2",
		Type:    MessageTypeReplicateChunk,
		From:    "coord-peer",
		Payload: payloadJSON,
	}

	err = r.handleReplicateChunk(msg)
	assert.NoError(t, err)

	// Verify chunk WAS written to store
	data, readErr := s3Store.ReadChunk(context.Background(), "abc123")
	assert.NoError(t, readErr, "chunk should be stored when capacity is OK")
	assert.Equal(t, []byte("some chunk data"), data)
}

// blockingTransport is a mock transport that blocks sends until released,
// allowing tests to verify concurrency limits.
type blockingTransport struct {
	mu         sync.Mutex
	handler    func(from string, data []byte) error
	blockCh    chan struct{} // Sends block until this channel is closed
	concurrent atomic.Int32  // Current number of concurrent sends
	maxSeen    atomic.Int32  // Peak concurrent sends observed
}

func newBlockingTransport() *blockingTransport {
	return &blockingTransport{
		blockCh: make(chan struct{}),
	}
}

func (bt *blockingTransport) SendToCoordinator(ctx context.Context, coordMeshIP string, data []byte) error {
	cur := bt.concurrent.Add(1)
	defer bt.concurrent.Add(-1)

	// Track peak concurrency
	for {
		old := bt.maxSeen.Load()
		if cur <= old || bt.maxSeen.CompareAndSwap(old, cur) {
			break
		}
	}

	// Block until released or context cancelled
	select {
	case <-bt.blockCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (bt *blockingTransport) RegisterHandler(handler func(from string, data []byte) error) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.handler = handler
}

func TestSendSemaphoreLimitsConcurrency(t *testing.T) {
	const maxSends = 3
	transport := newBlockingTransport()
	s3Store := newMockS3Store()

	r := NewReplicator(Config{
		NodeID:             "coord-test",
		Transport:          transport,
		S3Store:            s3Store,
		Logger:             zerolog.Nop(),
		MaxConcurrentSends: maxSends,
	})
	t.Cleanup(func() { _ = r.Stop() })

	r.AddPeer("peer-1", "peer-1")

	// Store a test object
	err := s3Store.Put(context.Background(), "bucket", "key", []byte("data"), "text/plain", nil)
	require.NoError(t, err)

	// Launch more goroutines than the semaphore allows
	const totalSends = 10
	var wg sync.WaitGroup
	started := make(chan struct{})

	for i := 0; i < totalSends; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-started // Wait for all goroutines to be ready
			ctx := context.Background()
			// Use sendReplicateMessage directly (old-style full-object replication)
			_ = r.sendReplicateMessage(ctx, "peer-1", ReplicatePayload{
				Bucket:      "bucket",
				Key:         "key",
				Data:        []byte("data"),
				ContentType: "text/plain",
			})
		}()
	}

	// Release all goroutines at once
	close(started)

	// Give goroutines time to hit the semaphore
	time.Sleep(100 * time.Millisecond)

	// The peak concurrent sends should be capped at maxSends
	peak := transport.maxSeen.Load()
	assert.LessOrEqual(t, peak, int32(maxSends),
		"peak concurrent sends (%d) should not exceed MaxConcurrentSends (%d)", peak, maxSends)
	assert.Greater(t, peak, int32(0), "at least one send should have started")

	// Unblock all sends so goroutines can complete
	close(transport.blockCh)
	wg.Wait()
}

func TestSendSemaphoreRespectsContextCancellation(t *testing.T) {
	transport := newBlockingTransport()
	s3Store := newMockS3Store()

	// Semaphore of size 1
	r := NewReplicator(Config{
		NodeID:             "coord-test",
		Transport:          transport,
		S3Store:            s3Store,
		Logger:             zerolog.Nop(),
		MaxConcurrentSends: 1,
	})
	t.Cleanup(func() { _ = r.Stop() })

	// Fill the semaphore slot
	err := r.acquireSendSlot(context.Background())
	require.NoError(t, err)

	// Try to acquire with an already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = r.acquireSendSlot(ctx)
	assert.ErrorIs(t, err, context.Canceled)

	// Release the slot
	r.releaseSendSlot()
}
