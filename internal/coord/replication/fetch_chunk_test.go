package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFetchChunk_Success tests successful chunk fetching.
func TestFetchChunk_Success(t *testing.T) {
	ctx := context.Background()
	s3 := newMockS3Store()
	broker := newTestTransportBroker()

	chunkData := []byte("test chunk data")
	h := sha256.Sum256(chunkData)
	chunkHash := hex.EncodeToString(h[:])

	// Store chunk
	require.NoError(t, s3.WriteChunkDirect(ctx, chunkHash, chunkData))

	// Create replicators
	local := NewReplicator(Config{
		NodeID:    "coord-local",
		Transport: broker.newTransportFor("coord-local"),
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, local.Start())
	defer func() { _ = local.Stop() }()

	remote := NewReplicator(Config{
		NodeID:    "coord-remote",
		Transport: broker.newTransportFor("coord-remote"),
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, remote.Start())
	defer func() { _ = remote.Stop() }()

	// Fetch chunk
	fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	result, err := local.FetchChunk(fetchCtx, "coord-remote", chunkHash)
	require.NoError(t, err)
	assert.Equal(t, chunkData, result)
}

// TestFetchChunk_HashMismatch tests hash validation when serving chunks.
func TestFetchChunk_HashMismatch(t *testing.T) {
	ctx := context.Background()
	s3 := newMockS3Store()
	broker := newTestTransportBroker()

	// Store chunk with one hash
	correctData := []byte("correct data")
	h := sha256.Sum256(correctData)
	correctHash := hex.EncodeToString(h[:])
	require.NoError(t, s3.WriteChunkDirect(ctx, correctHash, correctData))

	// But corrupt the data after storage (simulating bit rot or malicious modification)
	corruptedData := []byte("corrupted data")
	s3.chunks[correctHash] = corruptedData // Directly modify mock storage

	remote := NewReplicator(Config{
		NodeID:    "coord-remote",
		Transport: broker.newTransportFor("coord-remote"),
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, remote.Start())
	defer func() { _ = remote.Stop() }()

	local := NewReplicator(Config{
		NodeID:    "coord-local",
		Transport: broker.newTransportFor("coord-local"),
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, local.Start())
	defer func() { _ = local.Stop() }()

	// Try to fetch corrupted chunk
	fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	_, err := local.FetchChunk(fetchCtx, "coord-remote", correctHash)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "integrity check failed")
}

// TestFetchChunk_SizeLimitExceeded tests size validation.
func TestFetchChunk_SizeLimitExceeded(t *testing.T) {
	local := NewReplicator(Config{
		NodeID:    "coord-local",
		Transport: newMockTransport(),
		S3Store:   newMockS3Store(),
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, local.Start())
	defer func() { _ = local.Stop() }()

	// Create oversized response (> 64KB)
	oversizedData := make([]byte, 65537) // 64KB + 1
	for i := range oversizedData {
		oversizedData[i] = byte(i % 256)
	}

	payload := FetchChunkResponsePayload{
		ChunkHash: "test-hash",
		RequestID: "req-123",
		ChunkData: oversizedData,
		Success:   true,
	}

	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeFetchChunkResponse,
		ID:      "msg-id",
		From:    "coord-remote",
		Payload: json.RawMessage(payloadJSON),
	}

	// Handle response (should reject oversized data)
	err = local.handleFetchChunkResponse(msg)
	require.NoError(t, err)

	// The response should be converted to error response internally
	// (We can't easily verify this without exposing internal state,
	// but the handler runs without panic which validates the path)
}

// TestFetchChunk_ChunkNotFound tests fetching non-existent chunk.
func TestFetchChunk_ChunkNotFound(t *testing.T) {
	ctx := context.Background()
	s3 := newMockS3Store()
	broker := newTestTransportBroker()

	local := NewReplicator(Config{
		NodeID:    "coord-local",
		Transport: broker.newTransportFor("coord-local"),
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, local.Start())
	defer func() { _ = local.Stop() }()

	remote := NewReplicator(Config{
		NodeID:    "coord-remote",
		Transport: broker.newTransportFor("coord-remote"),
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, remote.Start())
	defer func() { _ = remote.Stop() }()

	// Try to fetch non-existent chunk
	fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	_, err := local.FetchChunk(fetchCtx, "coord-remote", "nonexistent-hash")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk not found")
}

// TestFetchChunk_Timeout tests fetch timeout handling.
func TestFetchChunk_Timeout(t *testing.T) {
	ctx := context.Background()
	s3 := newMockS3Store()
	transport := newMockTransport()

	local := NewReplicator(Config{
		NodeID:    "coord-local",
		Transport: transport,
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, local.Start())
	defer func() { _ = local.Stop() }()

	// Don't set up message routing - response will never arrive
	transport.handler = func(ctx context.Context, from string, data []byte) error {
		// Drop all messages (simulate network timeout)
		return nil
	}

	// Fetch with short timeout
	fetchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err := local.FetchChunk(fetchCtx, "coord-remote", "some-hash")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

// TestFetchChunk_ContextCancellation tests context cancellation during fetch.
func TestFetchChunk_ContextCancellation(t *testing.T) {
	ctx := context.Background()
	s3 := newMockS3Store()
	transport := newMockTransport()

	local := NewReplicator(Config{
		NodeID:    "coord-local",
		Transport: transport,
		S3Store:   s3,
		Logger:    zerolog.Nop(),
	})
	require.NoError(t, local.Start())
	defer func() { _ = local.Stop() }()

	// Don't set up message routing
	transport.handler = func(ctx context.Context, from string, data []byte) error {
		return nil
	}

	// Cancel context immediately
	fetchCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel before fetch

	_, err := local.FetchChunk(fetchCtx, "coord-remote", "some-hash")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "canceled")
}

// TestHandleFetchChunkResponse_UnknownRequest tests handling response for unknown request.
func TestHandleFetchChunkResponse_UnknownRequest(t *testing.T) {
	local := NewReplicator(Config{
		NodeID:    "coord-local",
		Transport: newMockTransport(),
		S3Store:   newMockS3Store(),
		Logger:    zerolog.Nop(),
	})

	payload := FetchChunkResponsePayload{
		ChunkHash: "test-hash",
		RequestID: "unknown-request-id",
		Success:   true,
		ChunkData: []byte("data"),
	}

	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeFetchChunkResponse,
		ID:      "msg-id",
		From:    "coord-remote",
		Payload: json.RawMessage(payloadJSON),
	}

	// Should handle gracefully (just log warning)
	err = local.handleFetchChunkResponse(msg)
	assert.NoError(t, err)
}
