package replication

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewReplicateMessage(t *testing.T) {
	payload := ReplicatePayload{
		Bucket:        "system",
		Key:           "test.json",
		Data:          []byte(`{"key":"value"}`),
		VersionVector: VersionVector{"coord1": 1},
		ContentType:   "application/json",
		Metadata:      map[string]string{"author": "test"},
	}

	msg, err := NewReplicateMessage("msg-001", "coord1.tunnelmesh:8443", payload)
	require.NoError(t, err)

	assert.Equal(t, MessageTypeReplicate, msg.Type)
	assert.Equal(t, "msg-001", msg.ID)
	assert.Equal(t, "coord1.tunnelmesh:8443", msg.From)
	assert.NotEmpty(t, msg.Payload)

	// Verify we can decode it back
	decoded, err := msg.DecodeReplicatePayload()
	require.NoError(t, err)
	assert.Equal(t, payload.Bucket, decoded.Bucket)
	assert.Equal(t, payload.Key, decoded.Key)
	assert.Equal(t, payload.Data, decoded.Data)
	assert.Equal(t, payload.ContentType, decoded.ContentType)
	assert.Equal(t, payload.Metadata, decoded.Metadata)
	assert.True(t, payload.VersionVector.Equal(decoded.VersionVector))
}

func TestNewAckMessage(t *testing.T) {
	payload := AckPayload{
		ReplicateID:   "msg-001",
		Success:       true,
		VersionVector: VersionVector{"coord1": 1, "coord2": 1},
	}

	msg, err := NewAckMessage("ack-001", "coord2.tunnelmesh:8443", payload)
	require.NoError(t, err)

	assert.Equal(t, MessageTypeAck, msg.Type)
	assert.Equal(t, "ack-001", msg.ID)
	assert.Equal(t, "coord2.tunnelmesh:8443", msg.From)

	// Verify we can decode it back
	decoded, err := msg.DecodeAckPayload()
	require.NoError(t, err)
	assert.Equal(t, payload.ReplicateID, decoded.ReplicateID)
	assert.Equal(t, payload.Success, decoded.Success)
	assert.Equal(t, payload.ErrorMessage, decoded.ErrorMessage)
	assert.True(t, payload.VersionVector.Equal(decoded.VersionVector))
}

func TestNewAckMessage_WithError(t *testing.T) {
	payload := AckPayload{
		ReplicateID:   "msg-001",
		Success:       false,
		ErrorMessage:  "conflict detected",
		VersionVector: VersionVector{"coord1": 1, "coord2": 2},
	}

	msg, err := NewAckMessage("ack-002", "coord2.tunnelmesh:8443", payload)
	require.NoError(t, err)

	decoded, err := msg.DecodeAckPayload()
	require.NoError(t, err)
	assert.False(t, decoded.Success)
	assert.Equal(t, "conflict detected", decoded.ErrorMessage)
}

func TestMessage_Marshal_Unmarshal(t *testing.T) {
	original := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeReplicate,
		ID:      "test-123",
		From:    "coord1.tunnelmesh:8443",
		Payload: []byte(`{
			"bucket": "system",
			"key": "test.json",
			"data": "dGVzdA==",
			"version_vector": {"coord1": 5}
		}`),
	}

	// Marshal to JSON
	data, err := original.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Unmarshal back
	unmarshaled, err := UnmarshalMessage(data)
	require.NoError(t, err)

	assert.Equal(t, original.Type, unmarshaled.Type)
	assert.Equal(t, original.ID, unmarshaled.ID)
	assert.Equal(t, original.From, unmarshaled.From)
}

func TestMessage_DecodeWrongType(t *testing.T) {
	msg := &Message{
		Type:    MessageTypeReplicate,
		ID:      "test",
		From:    "coord1",
		Payload: []byte(`{}`),
	}

	// Try to decode as wrong type
	_, err := msg.DecodeAckPayload()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not ack")
}

func TestMessage_DecodeInvalidJSON(t *testing.T) {
	msg := &Message{
		Type:    MessageTypeReplicate,
		ID:      "test",
		From:    "coord1",
		Payload: []byte(`invalid json`),
	}

	_, err := msg.DecodeReplicatePayload()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestReplicatePayload_WithMetadata(t *testing.T) {
	payload := ReplicatePayload{
		Bucket: "system",
		Key:    "config.json",
		Data:   []byte(`{"config": "value"}`),
		VersionVector: VersionVector{
			"coord1.tunnelmesh:8443": 1,
			"coord2.tunnelmesh:8443": 2,
		},
		ContentType: "application/json",
		Metadata: map[string]string{
			"author":    "admin",
			"timestamp": "2024-01-01T00:00:00Z",
			"checksum":  "abc123",
		},
	}

	msg, err := NewReplicateMessage("msg-123", "coord1.tunnelmesh:8443", payload)
	require.NoError(t, err)

	decoded, err := msg.DecodeReplicatePayload()
	require.NoError(t, err)

	assert.Equal(t, "application/json", decoded.ContentType)
	assert.Len(t, decoded.Metadata, 3)
	assert.Equal(t, "admin", decoded.Metadata["author"])
	assert.Equal(t, "2024-01-01T00:00:00Z", decoded.Metadata["timestamp"])
	assert.Equal(t, "abc123", decoded.Metadata["checksum"])
}

func TestMessageType_Constants(t *testing.T) {
	// Verify message type constants are distinct
	types := map[MessageType]bool{
		MessageTypeReplicate:           true,
		MessageTypeAck:                 true,
		MessageTypeReplicateObjectMeta: true,
	}

	assert.Len(t, types, 3, "All message types should be distinct")

	// Verify string values
	assert.Equal(t, MessageType("replicate"), MessageTypeReplicate)
	assert.Equal(t, MessageType("ack"), MessageTypeAck)
	assert.Equal(t, MessageType("replicate_object_meta"), MessageTypeReplicateObjectMeta)
}

func TestReplicateObjectMetaPayload_MarshalUnmarshal(t *testing.T) {
	payload := ReplicateObjectMetaPayload{
		Bucket:   "mybucket",
		Key:      "documents/report.pdf",
		MetaJSON: []byte(`{"key":"report.pdf","size":12345,"content_type":"application/pdf","chunks":["abc","def"]}`),
	}

	// Marshal to JSON
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	// Unmarshal back
	var decoded ReplicateObjectMetaPayload
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, payload.Bucket, decoded.Bucket)
	assert.Equal(t, payload.Key, decoded.Key)
	assert.JSONEq(t, string(payload.MetaJSON), string(decoded.MetaJSON))
}

func TestReplicateObjectMetaPayload_InMessage(t *testing.T) {
	payload := ReplicateObjectMetaPayload{
		Bucket:   "system",
		Key:      "config.json",
		MetaJSON: []byte(`{"key":"config.json","size":42}`),
	}

	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeReplicateObjectMeta,
		ID:      "meta-001",
		From:    "coord1.tunnelmesh:8443",
		Payload: payloadJSON,
	}

	// Round-trip via marshal/unmarshal
	data, err := msg.Marshal()
	require.NoError(t, err)

	parsed, err := UnmarshalMessage(data)
	require.NoError(t, err)
	assert.Equal(t, MessageTypeReplicateObjectMeta, parsed.Type)
	assert.Equal(t, "meta-001", parsed.ID)

	// Decode payload
	var decodedPayload ReplicateObjectMetaPayload
	err = json.Unmarshal(parsed.Payload, &decodedPayload)
	require.NoError(t, err)
	assert.Equal(t, "system", decodedPayload.Bucket)
	assert.Equal(t, "config.json", decodedPayload.Key)
}
