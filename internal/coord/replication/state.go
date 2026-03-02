package replication

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// State tracks version vectors for each replicated S3 key.
// This provides conflict detection for the replication system.
// Version vectors are separate from S3's object versioning.
type State struct {
	mu          sync.RWMutex
	vectors     map[string]VersionVector // key: "bucket/key", value: version vector
	lastUpdated map[string]time.Time     // key: "bucket/key", value: last mutation time
	nodeID      string                   // This coordinator's ID
}

// NewState creates a new replication state tracker.
func NewState(nodeID string) *State {
	return &State{
		vectors:     make(map[string]VersionVector),
		lastUpdated: make(map[string]time.Time),
		nodeID:      nodeID,
	}
}

// makeKey creates a consistent key for the version vector map.
func makeKey(bucket, key string) string {
	return bucket + "/" + key
}

// Get retrieves the version vector for a given S3 object.
// Returns a copy to prevent external modification.
func (s *State) Get(bucket, key string) VersionVector {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k := makeKey(bucket, key)
	if vv, exists := s.vectors[k]; exists {
		return vv.Copy()
	}
	return NewVersionVector()
}

// Set sets the version vector for a given S3 object.
// This should be called when adopting a remote version vector after conflict resolution.
func (s *State) Set(bucket, key string, vv VersionVector) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := makeKey(bucket, key)
	s.vectors[k] = vv.Copy()
	s.lastUpdated[k] = time.Now()
}

// Update updates the version vector for a given S3 object.
// This should be called when an object is modified locally.
// It increments the version for this coordinator.
func (s *State) Update(bucket, key string) VersionVector {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := makeKey(bucket, key)
	vv, exists := s.vectors[k]
	if !exists {
		vv = NewVersionVector()
	}

	vv.Increment(s.nodeID)
	s.vectors[k] = vv
	s.lastUpdated[k] = time.Now()

	return vv.Copy()
}

// Merge merges a received version vector for a given S3 object.
// This should be called when receiving a replicated update from another coordinator.
// Returns the merged version vector and whether this is a new object (didn't exist before).
func (s *State) Merge(bucket, key string, remoteVV VersionVector) (merged VersionVector, isNew bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := makeKey(bucket, key)
	localVV, exists := s.vectors[k]
	if !exists {
		localVV = NewVersionVector()
		isNew = true
	}

	// Merge the remote version vector
	localVV.Merge(remoteVV)
	s.vectors[k] = localVV
	s.lastUpdated[k] = time.Now()

	return localVV.Copy(), isNew
}

// CheckConflict checks if a remote version vector conflicts with the local one.
// Returns the relationship and whether a conflict needs resolution.
func (s *State) CheckConflict(bucket, key string, remoteVV VersionVector) (VectorRelationship, bool) {
	localVV := s.Get(bucket, key)
	relationship := localVV.Compare(remoteVV)

	needsResolution := relationship == VectorConcurrent
	return relationship, needsResolution
}

// Delete removes the version vector for a given S3 object.
// This should be called when an object is deleted.
func (s *State) Delete(bucket, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := makeKey(bucket, key)
	delete(s.vectors, k)
	delete(s.lastUpdated, k)
}

// DeleteBucket removes all version vectors for every key in the given bucket.
// This should be called when an entire bucket is force-deleted (e.g. file share cleanup).
func (s *State) DeleteBucket(bucket string) {
	prefix := bucket + "/"
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.vectors {
		if strings.HasPrefix(k, prefix) {
			delete(s.vectors, k)
			delete(s.lastUpdated, k)
		}
	}
}

// Count returns the number of tracked objects.
func (s *State) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.vectors)
}

// Clear removes all version vectors. Used for testing.
func (s *State) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vectors = make(map[string]VersionVector)
	s.lastUpdated = make(map[string]time.Time)
}

// stateSnapshot represents a serializable snapshot of the replication state.
type stateSnapshot struct {
	NodeID   string                   `json:"node_id"`
	Vectors  map[string]VersionVector `json:"vectors"`
	Checksum string                   `json:"checksum"` // SHA256 of vectors JSON
}

// Snapshot creates a serializable snapshot of the current state.
func (s *State) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create snapshot
	snapshot := stateSnapshot{
		NodeID:  s.nodeID,
		Vectors: make(map[string]VersionVector, len(s.vectors)),
	}

	// Copy vectors
	for k, vv := range s.vectors {
		snapshot.Vectors[k] = vv.Copy()
	}

	// Marshal vectors for checksum
	vectorsJSON, err := json.Marshal(snapshot.Vectors)
	if err != nil {
		return nil, fmt.Errorf("marshal vectors for checksum: %w", err)
	}

	// Calculate checksum
	hash := sha256.Sum256(vectorsJSON)
	snapshot.Checksum = hex.EncodeToString(hash[:])

	// Marshal full snapshot
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}

	return data, nil
}

// LoadSnapshot loads a previously saved snapshot.
func (s *State) LoadSnapshot(data []byte) error {
	var snapshot stateSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	// Verify checksum
	vectorsJSON, err := json.Marshal(snapshot.Vectors)
	if err != nil {
		return fmt.Errorf("marshal vectors for verification: %w", err)
	}

	hash := sha256.Sum256(vectorsJSON)
	expectedChecksum := hex.EncodeToString(hash[:])
	if expectedChecksum != snapshot.Checksum {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedChecksum, snapshot.Checksum)
	}

	// Load data
	s.mu.Lock()
	defer s.mu.Unlock()

	// SECURITY FIX #8: Don't overwrite our own nodeID with the sender's nodeID
	// The snapshot.NodeID is the sender's identity, not ours
	// We only want to load the version vectors to understand remote state
	// s.nodeID = snapshot.NodeID  // REMOVED: This would corrupt our identity
	s.vectors = snapshot.Vectors

	return nil
}

// StateStats returns statistics about the replication state.
type StateStats struct {
	NodeID       string `json:"node_id"`
	TrackedKeys  int    `json:"tracked_keys"`
	TotalUpdates uint64 `json:"total_updates"` // Sum of all version vector counts
}

// GetStats returns current replication state statistics.
func (s *State) GetStats() StateStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := StateStats{
		NodeID:      s.nodeID,
		TrackedKeys: len(s.vectors),
	}

	// Calculate total updates across all version vectors
	for _, vv := range s.vectors {
		for _, count := range vv {
			stats.TotalUpdates += count
		}
	}

	return stats
}

// ListKeys returns all tracked bucket/key combinations.
// Useful for debugging and administration.
func (s *State) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.vectors))
	for k := range s.vectors {
		keys = append(keys, k)
	}
	return keys
}

// PruneDeletedKeys removes version vector entries for keys no longer in the
// active set whose last update is older than maxAge. This prevents unbounded
// growth when accordion-style workloads delete and recreate many unique keys.
// The maxAge grace period ensures in-flight delayed PUT replicas can still be
// rejected via conflict detection before the tombstone is discarded.
// Returns the number of entries removed.
func (s *State) PruneDeletedKeys(activeKeys map[string]bool, maxAge time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	pruned := 0
	for k, ts := range s.lastUpdated {
		if !activeKeys[k] && time.Since(ts) > maxAge {
			delete(s.vectors, k)
			delete(s.lastUpdated, k)
			pruned++
		}
	}
	return pruned
}
