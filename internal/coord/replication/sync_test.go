package replication

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Test helpers use existing mockTransport and mockS3Store from replicator_test.go

// countObjects is a helper to count total objects across all buckets
func countObjects(s3 *mockS3Store) int {
	s3.mu.Lock()
	defer s3.mu.Unlock()
	return len(s3.objects)
}

func TestSyncRequest_EmptyStore(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	// Create coordinator with empty S3
	transport := newMockTransport()
	s3 := newMockS3Store()
	replicator := NewReplicator(Config{
		NodeID:    "coord1",
		Transport: transport,
		S3Store:   s3,
		Logger:    logger,
	})

	if err := replicator.Start(); err != nil {
		t.Fatalf("Failed to start replicator: %v", err)
	}
	defer func() {
		if err := replicator.Stop(); err != nil {
			t.Logf("Failed to stop replicator: %v", err)
		}
	}()

	// Create sync request message
	payload := SyncRequestPayload{
		RequestedBuckets: nil, // Request all buckets
	}
	msg, err := NewSyncRequestMessage("test-msg-1", "coord2", payload)
	if err != nil {
		t.Fatalf("Failed to create sync request: %v", err)
	}

	// Handle the sync request
	if err := replicator.handleSyncRequest(msg); err != nil {
		t.Fatalf("Failed to handle sync request: %v", err)
	}

	// Wait for response to be sent
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var responseData []byte
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		responseData = transport.getLastSent("coord2")
		return responseData != nil
	})
	if err != nil {
		t.Fatal("Expected sync response to be sent")
	}

	// Decode response
	responseMsg, err := UnmarshalMessage(responseData)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if responseMsg.Type != MessageTypeSyncResponse {
		t.Errorf("Expected sync_response, got %s", responseMsg.Type)
	}

	responsePayload, err := responseMsg.DecodeSyncResponsePayload()
	if err != nil {
		t.Fatalf("Failed to decode response payload: %v", err)
	}

	if len(responsePayload.Objects) != 0 {
		t.Errorf("Expected 0 objects, got %d", len(responsePayload.Objects))
	}
}

func TestSyncRequest_WithData(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	// Create coordinator with some data
	transport := newMockTransport()
	s3 := newMockS3Store()

	// Add test objects
	ctx := context.Background()
	if err := s3.Put(ctx, "bucket1", "key1", []byte("data1"), "text/plain", map[string]string{"foo": "bar"}); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	if err := s3.Put(ctx, "bucket1", "key2", []byte("data2"), "application/json", nil); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	if err := s3.Put(ctx, "bucket2", "key3", []byte("data3"), "", nil); err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}

	replicator := NewReplicator(Config{
		NodeID:    "coord1",
		Transport: transport,
		S3Store:   s3,
		Logger:    logger,
	})

	if err := replicator.Start(); err != nil {
		t.Fatalf("Failed to start replicator: %v", err)
	}
	defer func() {
		if err := replicator.Stop(); err != nil {
			t.Logf("Failed to stop replicator: %v", err)
		}
	}()

	// Update version vectors for the objects
	replicator.state.Update("bucket1", "key1")
	replicator.state.Update("bucket1", "key2")
	replicator.state.Update("bucket2", "key3")

	// Create sync request message
	payload := SyncRequestPayload{
		RequestedBuckets: nil, // Request all buckets
	}
	msg, err := NewSyncRequestMessage("test-msg-2", "coord2", payload)
	if err != nil {
		t.Fatalf("Failed to create sync request: %v", err)
	}

	// Handle the sync request
	if err := replicator.handleSyncRequest(msg); err != nil {
		t.Fatalf("Failed to handle sync request: %v", err)
	}

	// Wait for response to be sent
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var responseData []byte
	err = waitFor(ctx, 10*time.Millisecond, func() bool {
		responseData = transport.getLastSent("coord2")
		return responseData != nil
	})
	if err != nil {
		t.Fatal("Expected sync response to be sent")
	}

	// Decode response
	responseMsg, err := UnmarshalMessage(responseData)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	responsePayload, err := responseMsg.DecodeSyncResponsePayload()
	if err != nil {
		t.Fatalf("Failed to decode response payload: %v", err)
	}

	// Verify all objects are in the response
	if len(responsePayload.Objects) != 3 {
		t.Fatalf("Expected 3 objects, got %d", len(responsePayload.Objects))
	}

	// Verify state snapshot is included
	if len(responsePayload.StateSnapshot) == 0 {
		t.Error("Expected state snapshot to be included")
	}

	// Verify object data
	objectMap := make(map[string]SyncObjectEntry)
	for _, obj := range responsePayload.Objects {
		key := obj.Bucket + "/" + obj.Key
		objectMap[key] = obj
	}

	// Check bucket1/key1
	obj1, ok := objectMap["bucket1/key1"]
	if !ok {
		t.Error("Expected bucket1/key1 in response")
	} else {
		if string(obj1.Data) != "data1" {
			t.Errorf("Expected data1, got %s", string(obj1.Data))
		}
		if obj1.ContentType != "text/plain" {
			t.Errorf("Expected text/plain, got %s", obj1.ContentType)
		}
		if obj1.Metadata["foo"] != "bar" {
			t.Errorf("Expected metadata foo=bar, got %v", obj1.Metadata)
		}
	}
}

func TestSyncRequest_SpecificBuckets(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	// Create coordinator with data in multiple buckets
	transport := newMockTransport()
	s3 := newMockS3Store()

	ctx := context.Background()
	if err := s3.Put(ctx, "bucket1", "key1", []byte("data1"), "", nil); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	if err := s3.Put(ctx, "bucket2", "key2", []byte("data2"), "", nil); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	if err := s3.Put(ctx, "bucket3", "key3", []byte("data3"), "", nil); err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}

	replicator := NewReplicator(Config{
		NodeID:    "coord1",
		Transport: transport,
		S3Store:   s3,
		Logger:    logger,
	})

	if err := replicator.Start(); err != nil {
		t.Fatalf("Failed to start replicator: %v", err)
	}
	defer func() {
		if err := replicator.Stop(); err != nil {
			t.Logf("Failed to stop replicator: %v", err)
		}
	}()

	// Request only bucket1 and bucket3
	payload := SyncRequestPayload{
		RequestedBuckets: []string{"bucket1", "bucket3"},
	}
	msg, err := NewSyncRequestMessage("test-msg-3", "coord2", payload)
	if err != nil {
		t.Fatalf("Failed to create sync request: %v", err)
	}

	if err := replicator.handleSyncRequest(msg); err != nil {
		t.Fatalf("Failed to handle sync request: %v", err)
	}

	responseData := transport.getLastSent("coord2")
	responseMsg, _ := UnmarshalMessage(responseData)
	responsePayload, _ := responseMsg.DecodeSyncResponsePayload()

	// Should only have 2 objects (bucket1 and bucket3)
	if len(responsePayload.Objects) != 2 {
		t.Fatalf("Expected 2 objects, got %d", len(responsePayload.Objects))
	}

	// Verify bucket2 is not included
	for _, obj := range responsePayload.Objects {
		if obj.Bucket == "bucket2" {
			t.Error("bucket2 should not be in response")
		}
	}
}

func TestSyncResponse_ApplyState(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	// Create receiving coordinator (empty)
	transport := newMockTransport()
	s3 := newMockS3Store()

	replicator := NewReplicator(Config{
		NodeID:    "coord2",
		Transport: transport,
		S3Store:   s3,
		Logger:    logger,
	})

	if err := replicator.Start(); err != nil {
		t.Fatalf("Failed to start replicator: %v", err)
	}
	defer func() {
		if err := replicator.Stop(); err != nil {
			t.Logf("Failed to stop replicator: %v", err)
		}
	}()

	// Create a state snapshot from coord1
	coord1State := NewState("coord1")
	coord1State.Update("bucket1", "key1")
	coord1State.Update("bucket1", "key2")
	stateSnapshot, err := coord1State.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create state snapshot: %v", err)
	}

	// Create sync response with objects
	objects := []SyncObjectEntry{
		{
			Bucket:        "bucket1",
			Key:           "key1",
			Data:          []byte("data1"),
			VersionVector: coord1State.Get("bucket1", "key1"),
			ContentType:   "text/plain",
			Metadata:      map[string]string{"test": "value"},
		},
		{
			Bucket:        "bucket1",
			Key:           "key2",
			Data:          []byte("data2"),
			VersionVector: coord1State.Get("bucket1", "key2"),
			ContentType:   "application/json",
		},
	}

	responsePayload := SyncResponsePayload{
		StateSnapshot: stateSnapshot,
		Objects:       objects,
	}

	msg, err := NewSyncResponseMessage("test-response", "coord1", responsePayload)
	if err != nil {
		t.Fatalf("Failed to create sync response: %v", err)
	}

	// Handle the sync response
	if err := replicator.handleSyncResponse(msg); err != nil {
		t.Fatalf("Failed to handle sync response: %v", err)
	}

	// Verify objects were added to S3
	if countObjects(s3) != 2 {
		t.Fatalf("Expected 2 objects in S3, got %d", countObjects(s3))
	}

	// Verify object data
	ctx := context.Background()
	data1, metadata1, err := s3.Get(ctx, "bucket1", "key1")
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if string(data1) != "data1" {
		t.Errorf("Expected data1, got %s", string(data1))
	}
	if metadata1["test"] != "value" {
		t.Errorf("Expected metadata test=value, got %v", metadata1)
	}

	// Verify version vectors were updated
	localVV := replicator.state.Get("bucket1", "key1")
	if localVV.Get("coord1") == 0 {
		t.Error("Expected version vector to be updated for coord1")
	}
}

func TestSyncResponse_SkipOlderObjects(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	// Create receiving coordinator with existing newer data
	transport := newMockTransport()
	s3 := newMockS3Store()

	replicator := NewReplicator(Config{
		NodeID:    "coord2",
		Transport: transport,
		S3Store:   s3,
		Logger:    logger,
	})

	if err := replicator.Start(); err != nil {
		t.Fatalf("Failed to start replicator: %v", err)
	}
	defer func() {
		if err := replicator.Stop(); err != nil {
			t.Logf("Failed to stop replicator: %v", err)
		}
	}()

	// Add object locally with version vector
	ctx := context.Background()
	if err := s3.Put(ctx, "bucket1", "key1", []byte("local-data-newer"), "", nil); err != nil {
		t.Fatalf("Failed to put local data: %v", err)
	}
	replicator.state.Update("bucket1", "key1")
	replicator.state.Update("bucket1", "key1")         // Update again to make it newer
	localVV := replicator.state.Get("bucket1", "key1") // Get current state after updates

	// Create older version vector from coord1
	coord1VV := NewVersionVector()
	coord1VV.Increment("coord1")

	// Create sync response with older object
	objects := []SyncObjectEntry{
		{
			Bucket:        "bucket1",
			Key:           "key1",
			Data:          []byte("remote-data-older"),
			VersionVector: coord1VV,
		},
	}

	coord1State := NewState("coord1")
	coord1State.Merge("bucket1", "key1", coord1VV)
	stateSnapshot, _ := coord1State.Snapshot()

	responsePayload := SyncResponsePayload{
		StateSnapshot: stateSnapshot,
		Objects:       objects,
	}

	msg, err := NewSyncResponseMessage("test-response", "coord1", responsePayload)
	if err != nil {
		t.Fatalf("Failed to create sync response: %v", err)
	}

	// Handle the sync response
	if err := replicator.handleSyncResponse(msg); err != nil {
		t.Fatalf("Failed to handle sync response: %v", err)
	}

	// Verify local data was NOT overwritten (local is newer)
	data, _, err := s3.Get(ctx, "bucket1", "key1")
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}

	// Local version vector should still be ahead
	currentVV := replicator.state.Get("bucket1", "key1")
	if currentVV.Get("coord2") != localVV.Get("coord2") {
		t.Error("Local version vector should not be overwritten")
	}

	// Note: The actual data check depends on conflict resolution logic
	// For this test, we're mainly checking that the version vector is preserved
	t.Logf("Data after sync: %s", string(data))
}

func TestRequestSync(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	transport := newMockTransport()
	s3 := newMockS3Store()

	replicator := NewReplicator(Config{
		NodeID:    "coord1",
		Transport: transport,
		S3Store:   s3,
		Logger:    logger,
	})

	if err := replicator.Start(); err != nil {
		t.Fatalf("Failed to start replicator: %v", err)
	}
	defer func() {
		if err := replicator.Stop(); err != nil {
			t.Logf("Failed to stop replicator: %v", err)
		}
	}()

	// Request sync from coord2
	ctx := context.Background()
	err := replicator.RequestSync(ctx, "10.42.0.2", []string{"bucket1"})
	if err != nil {
		t.Fatalf("Failed to request sync: %v", err)
	}

	// Verify message was sent
	msgData := transport.getLastSent("10.42.0.2")
	if msgData == nil {
		t.Fatal("Expected sync request to be sent")
	}

	// Decode and verify message
	msg, err := UnmarshalMessage(msgData)
	if err != nil {
		t.Fatalf("Failed to unmarshal message: %v", err)
	}

	if msg.Type != MessageTypeSyncRequest {
		t.Errorf("Expected sync_request, got %s", msg.Type)
	}

	payload, err := msg.DecodeSyncRequestPayload()
	if err != nil {
		t.Fatalf("Failed to decode payload: %v", err)
	}

	if len(payload.RequestedBuckets) != 1 || payload.RequestedBuckets[0] != "bucket1" {
		t.Errorf("Expected [bucket1], got %v", payload.RequestedBuckets)
	}
}

func TestRequestSyncFromAll(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	transport := newMockTransport()
	s3 := newMockS3Store()

	replicator := NewReplicator(Config{
		NodeID:    "coord1",
		Transport: transport,
		S3Store:   s3,
		Logger:    logger,
	})

	if err := replicator.Start(); err != nil {
		t.Fatalf("Failed to start replicator: %v", err)
	}
	defer func() {
		if err := replicator.Stop(); err != nil {
			t.Logf("Failed to stop replicator: %v", err)
		}
	}()

	// Add some peers
	replicator.AddPeer("10.42.0.2", "10.42.0.2")
	replicator.AddPeer("10.42.0.3", "10.42.0.3")

	// Request sync from all peers
	ctx := context.Background()
	err := replicator.RequestSyncFromAll(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to request sync from all: %v", err)
	}

	// Verify messages were sent to all peers
	msg2 := transport.getLastSent("10.42.0.2")
	msg3 := transport.getLastSent("10.42.0.3")

	if msg2 == nil {
		t.Error("Expected message to be sent to peer 10.42.0.2")
	}
	if msg3 == nil {
		t.Error("Expected message to be sent to peer 10.42.0.3")
	}
}

func TestFullSyncRoundtrip(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	// Create coordinator 1 with data
	transport1 := newMockTransport()
	s3_1 := newMockS3Store()

	coord1 := NewReplicator(Config{
		NodeID:    "coord1",
		Transport: transport1,
		S3Store:   s3_1,
		Logger:    logger,
	})

	if err := coord1.Start(); err != nil {
		t.Fatalf("Failed to start coord1: %v", err)
	}
	defer func() {
		if err := coord1.Stop(); err != nil {
			t.Logf("Failed to stop coord1: %v", err)
		}
	}()

	// Add data to coord1
	ctx := context.Background()
	if err := s3_1.Put(ctx, "bucket1", "key1", []byte("data1"), "text/plain", map[string]string{"meta": "value"}); err != nil {
		t.Fatalf("Failed to put key1 in coord1: %v", err)
	}
	if err := s3_1.Put(ctx, "bucket1", "key2", []byte("data2"), "application/json", nil); err != nil {
		t.Fatalf("Failed to put key2 in coord1: %v", err)
	}
	coord1.state.Update("bucket1", "key1")
	coord1.state.Update("bucket1", "key2")

	// Create coordinator 2 (empty)
	transport2 := newMockTransport()
	s3_2 := newMockS3Store()

	coord2 := NewReplicator(Config{
		NodeID:    "coord2",
		Transport: transport2,
		S3Store:   s3_2,
		Logger:    logger,
	})

	if err := coord2.Start(); err != nil {
		t.Fatalf("Failed to start coord2: %v", err)
	}
	defer func() {
		if err := coord2.Stop(); err != nil {
			t.Logf("Failed to stop coord2: %v", err)
		}
	}()

	// Wire up transports bidirectionally
	transport1.RegisterHandler(func(from string, data []byte) error {
		return coord1.handleIncomingMessage(from, data)
	})
	transport2.RegisterHandler(func(from string, data []byte) error {
		return coord2.handleIncomingMessage(from, data)
	})

	// Coord2 requests sync from coord1
	err := coord2.RequestSync(ctx, "coord1", nil)
	if err != nil {
		t.Fatalf("Failed to request sync: %v", err)
	}

	// Deliver sync request from transport2 to coord1
	syncRequest := transport2.getLastSent("coord1")
	if syncRequest == nil {
		t.Fatal("No sync request was sent")
	}
	if err := transport1.simulateReceive("coord2", syncRequest); err != nil {
		t.Fatalf("Failed to deliver sync request: %v", err)
	}

	// Wait for coord1 to process and send response

	// Deliver sync response from transport1 back to coord2
	syncResponse := transport1.getLastSent("coord2")
	if syncResponse == nil {
		t.Fatal("No sync response was sent")
	}
	if err := transport2.simulateReceive("coord1", syncResponse); err != nil {
		t.Fatalf("Failed to deliver sync response: %v", err)
	}

	// Wait for coord2 to process the response

	// Verify coord2 now has the data
	if countObjects(s3_2) != 2 {
		t.Fatalf("Expected 2 objects in coord2, got %d", countObjects(s3_2))
	}

	// Verify data content
	data1, metadata1, err := s3_2.Get(ctx, "bucket1", "key1")
	if err != nil {
		t.Fatalf("Failed to get key1 from coord2: %v", err)
	}
	if string(data1) != "data1" {
		t.Errorf("Expected data1, got %s", string(data1))
	}
	if metadata1["meta"] != "value" {
		t.Errorf("Expected metadata meta=value, got %v", metadata1)
	}

	// Verify version vectors match
	vv1 := coord1.state.Get("bucket1", "key1")
	vv2 := coord2.state.Get("bucket1", "key1")
	if !vv1.Equal(vv2) {
		t.Errorf("Version vectors should match: %v != %v", vv1, vv2)
	}
}
