package replication

import (
	"encoding/json"
	"fmt"
)

// ProtocolVersion is the current replication protocol version.
// Version history:
//   - v1: Initial implementation with version vectors and conflict resolution
const ProtocolVersion = 1

// MessageType identifies the type of replication message.
type MessageType string

const (
	// MessageTypeReplicate is sent when a coordinator wants to replicate data to another coordinator
	MessageTypeReplicate MessageType = "replicate"

	// MessageTypeAck is sent in response to a successful replication
	MessageTypeAck MessageType = "ack"

	// Chunk-level replication message types (added in Phase 2)

	// MessageTypeChunkOwnershipUpdate is broadcast when a coordinator adds/removes chunk ownership
	MessageTypeChunkOwnershipUpdate MessageType = "chunk_ownership_update"

	// MessageTypeChunkRegistrySync is sent to synchronize chunk registry state
	MessageTypeChunkRegistrySync MessageType = "chunk_registry_sync"

	// MessageTypeQueryChunkLocation requests the list of coordinators that have a specific chunk
	MessageTypeQueryChunkLocation MessageType = "query_chunk_location"

	// MessageTypeChunkLocationResponse responds with the list of chunk owners
	MessageTypeChunkLocationResponse MessageType = "chunk_location_response"

	// MessageTypeReplicateChunk is sent to replicate an individual chunk (not a full object)
	MessageTypeReplicateChunk MessageType = "replicate_chunk"

	// MessageTypeChunkAck acknowledges successful chunk replication
	MessageTypeChunkAck MessageType = "chunk_ack"

	// MessageTypeFetchChunk requests chunk data from a peer (for distributed reads)
	MessageTypeFetchChunk MessageType = "fetch_chunk"

	// MessageTypeFetchChunkResponse returns chunk data in response to a fetch request
	MessageTypeFetchChunkResponse MessageType = "fetch_chunk_response"

	// MessageTypeReplicateObjectMeta replicates object metadata to a peer
	MessageTypeReplicateObjectMeta MessageType = "replicate_object_meta"

	// MessageTypeObjectManifest sends a manifest of all live objects for reconciliation.
	// Replicas purge local objects not present in the manifest to recover from lost deletes.
	MessageTypeObjectManifest MessageType = "object_manifest"
)

// Message is the envelope for all replication protocol messages.
type Message struct {
	Version int             `json:"version"` // Protocol version for compatibility checking
	Type    MessageType     `json:"type"`
	ID      string          `json:"id"`      // Unique message ID for tracking ACKs
	From    string          `json:"from"`    // Sender's coordinator address
	Payload json.RawMessage `json:"payload"` // Type-specific payload
}

// ReplicatePayload contains the data for a replication operation.
type ReplicatePayload struct {
	Bucket        string            `json:"bucket"`
	Key           string            `json:"key"`
	Data          []byte            `json:"data"`           // S3 object data
	VersionVector VersionVector     `json:"version_vector"` // Version vector for conflict detection
	ContentType   string            `json:"content_type,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// AckPayload contains the response to a replication operation.
type AckPayload struct {
	ReplicateID   string        `json:"replicate_id"` // ID of the replicate message being acked
	Success       bool          `json:"success"`
	ErrorMessage  string        `json:"error_message,omitempty"`
	VersionVector VersionVector `json:"version_vector"` // Final version vector after merge
}

// Chunk-level replication payloads (added in Phase 2)

// ChunkOwnershipUpdatePayload is broadcast when chunk ownership changes.
type ChunkOwnershipUpdatePayload struct {
	ChunkHash     string        `json:"chunk_hash"`
	CoordinatorID string        `json:"coordinator_id"`
	Action        string        `json:"action"`    // "add" or "remove"
	Timestamp     int64         `json:"timestamp"` // Unix timestamp
	VersionVector VersionVector `json:"version_vector"`
	Size          int64         `json:"size,omitempty"` // Chunk size (for "add" action)
}

// ChunkRegistrySyncPayload contains the full chunk registry state.
type ChunkRegistrySyncPayload struct {
	RegistryState []byte `json:"registry_state"` // Serialized ChunkOwnership records
	Checksum      string `json:"checksum"`       // SHA-256 of registry_state for integrity
}

// QueryChunkLocationPayload requests the location of a specific chunk.
type QueryChunkLocationPayload struct {
	ChunkHash string `json:"chunk_hash"`
	RequestID string `json:"request_id"` // For matching response
}

// ChunkLocationResponsePayload responds with chunk owner information.
type ChunkLocationResponsePayload struct {
	ChunkHash string   `json:"chunk_hash"`
	Owners    []string `json:"owners"`     // List of coordinator IDs that have this chunk
	RequestID string   `json:"request_id"` // Matches QueryChunkLocationPayload.RequestID
}

// ReplicateChunkPayload contains data for chunk-level replication.
type ReplicateChunkPayload struct {
	Bucket        string        `json:"bucket"`         // File this chunk belongs to
	Key           string        `json:"key"`            // File key
	ChunkHash     string        `json:"chunk_hash"`     // SHA-256 of chunk plaintext
	ChunkData     []byte        `json:"chunk_data"`     // Compressed + encrypted chunk
	ChunkIndex    int           `json:"chunk_index"`    // Position in file (for ordering)
	TotalChunks   int           `json:"total_chunks"`   // Total number of chunks in file
	ChunkSize     int64         `json:"chunk_size"`     // Uncompressed chunk size
	VersionVector VersionVector `json:"version_vector"` // Per-chunk version vector
}

// ChunkAckPayload acknowledges chunk replication.
type ChunkAckPayload struct {
	ReplicateID string `json:"replicate_id"` // ID of the chunk replicate message being acked
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	ChunkHash   string `json:"chunk_hash"`
	ChunkIndex  int    `json:"chunk_index"`
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
}

// FetchChunkPayload requests chunk data from a peer.
type FetchChunkPayload struct {
	ChunkHash string `json:"chunk_hash"` // SHA-256 of chunk to fetch
	RequestID string `json:"request_id"` // For matching response
}

// FetchChunkResponsePayload returns chunk data.
type FetchChunkResponsePayload struct {
	ChunkHash string `json:"chunk_hash"` // SHA-256 of chunk
	ChunkData []byte `json:"chunk_data"` // Chunk data (compressed + encrypted)
	RequestID string `json:"request_id"` // Matches FetchChunkPayload.RequestID
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`
}

// VersionEntry represents a single version's metadata for replication.
// Used to replicate version history alongside object metadata.
type VersionEntry struct {
	VersionID string          `json:"version_id"`
	MetaJSON  json.RawMessage `json:"meta_json"`
}

// ReplicateObjectMetaPayload contains object metadata for replication to a peer.
// The receiving coordinator uses this to create the metadata file so it can
// serve reads for objects whose chunks arrive via chunk-level replication.
type ReplicateObjectMetaPayload struct {
	Bucket      string          `json:"bucket"`
	Key         string          `json:"key"`
	MetaJSON    json.RawMessage `json:"meta_json"`
	BucketOwner string          `json:"bucket_owner,omitempty"` // Original bucket owner for auto-created buckets
	Versions    []VersionEntry  `json:"versions,omitempty"`     // Version history entries (replicated to all coordinators)
}

// ObjectManifestPayload contains the set of live objects on the sender.
// Replicas should purge any local objects not in this manifest (except system bucket).
type ObjectManifestPayload struct {
	// BucketKeys maps bucket name → set of live object keys.
	BucketKeys map[string][]string `json:"bucket_keys"`
}

// NewReplicateMessage creates a new replication message.
func NewReplicateMessage(id, from string, payload ReplicatePayload) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal replicate payload: %w", err)
	}

	return &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeReplicate,
		ID:      id,
		From:    from,
		Payload: data,
	}, nil
}

// NewAckMessage creates a new acknowledgment message.
func NewAckMessage(id, from string, payload AckPayload) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal ack payload: %w", err)
	}

	return &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeAck,
		ID:      id,
		From:    from,
		Payload: data,
	}, nil
}

// DecodeReplicatePayload decodes a replicate payload from a message.
func (m *Message) DecodeReplicatePayload() (*ReplicatePayload, error) {
	if m.Type != MessageTypeReplicate {
		return nil, fmt.Errorf("message type is %s, not replicate", m.Type)
	}

	var payload ReplicatePayload
	if err := json.Unmarshal(m.Payload, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal replicate payload: %w", err)
	}

	return &payload, nil
}

// DecodeAckPayload decodes an ack payload from a message.
func (m *Message) DecodeAckPayload() (*AckPayload, error) {
	if m.Type != MessageTypeAck {
		return nil, fmt.Errorf("message type is %s, not ack", m.Type)
	}

	var payload AckPayload
	if err := json.Unmarshal(m.Payload, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal ack payload: %w", err)
	}

	return &payload, nil
}

// Marshal serializes the message to JSON.
func (m *Message) Marshal() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	return data, nil
}

// UnmarshalMessage deserializes a message from JSON.
func UnmarshalMessage(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}

	// Check protocol version compatibility
	if msg.Version != ProtocolVersion {
		return nil, fmt.Errorf("incompatible protocol version: got %d, expected %d", msg.Version, ProtocolVersion)
	}

	return &msg, nil
}
