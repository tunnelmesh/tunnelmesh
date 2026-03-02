package replication

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewState(t *testing.T) {
	state := NewState("coord1.tunnelmesh:8443")
	assert.NotNil(t, state)
	assert.Equal(t, 0, state.Count())
	assert.Equal(t, "coord1.tunnelmesh:8443", state.nodeID)
}

func TestState_Get(t *testing.T) {
	state := NewState("coord1")

	// Get non-existent key returns empty version vector
	vv := state.Get("system", "test.json")
	assert.NotNil(t, vv)
	assert.Equal(t, 0, len(vv))

	// Update a key
	state.Update("system", "test.json")

	// Get returns a copy
	vv = state.Get("system", "test.json")
	assert.Equal(t, uint64(1), vv.Get("coord1"))

	// Modifying the returned vector doesn't affect state
	vv.Increment("coord2")
	vv2 := state.Get("system", "test.json")
	assert.Equal(t, uint64(1), vv2.Get("coord1"))
	assert.Equal(t, uint64(0), vv2.Get("coord2")) // coord2 not in original
}

func TestState_Update(t *testing.T) {
	state := NewState("coord1")

	// First update
	vv1 := state.Update("system", "test.json")
	assert.Equal(t, uint64(1), vv1.Get("coord1"))
	assert.Equal(t, 1, state.Count())

	// Second update on same key
	vv2 := state.Update("system", "test.json")
	assert.Equal(t, uint64(2), vv2.Get("coord1"))
	assert.Equal(t, 1, state.Count()) // Still one key

	// Update different key
	vv3 := state.Update("system", "other.json")
	assert.Equal(t, uint64(1), vv3.Get("coord1"))
	assert.Equal(t, 2, state.Count()) // Now two keys

	// Verify independence
	finalVV := state.Get("system", "test.json")
	assert.Equal(t, uint64(2), finalVV.Get("coord1"))
}

func TestState_Merge(t *testing.T) {
	state := NewState("coord1")

	t.Run("merge new key", func(t *testing.T) {
		remoteVV := VersionVector{"coord2": 3}
		merged, isNew := state.Merge("system", "new.json", remoteVV)

		assert.True(t, isNew)
		assert.Equal(t, uint64(3), merged.Get("coord2"))
		assert.Equal(t, 1, state.Count())
	})

	t.Run("merge existing key", func(t *testing.T) {
		// Create local version
		state.Update("system", "existing.json")
		state.Update("system", "existing.json")

		// Merge remote version
		remoteVV := VersionVector{"coord1": 1, "coord2": 5}
		merged, isNew := state.Merge("system", "existing.json", remoteVV)

		assert.False(t, isNew)
		assert.Equal(t, uint64(2), merged.Get("coord1")) // Local higher
		assert.Equal(t, uint64(5), merged.Get("coord2")) // Remote added
	})

	t.Run("merge doesn't modify input", func(t *testing.T) {
		remoteVV := VersionVector{"coord3": 1}
		originalCount := uint64(1)

		state.Merge("system", "test.json", remoteVV)

		// Remote VV should be unchanged
		assert.Equal(t, originalCount, remoteVV.Get("coord3"))
	})
}

func TestState_CheckConflict(t *testing.T) {
	state := NewState("coord1")

	tests := []struct {
		name            string
		setup           func()
		bucket          string
		key             string
		remoteVV        VersionVector
		expectedRel     VectorRelationship
		needsResolution bool
	}{
		{
			name:            "no local version - remote wins",
			setup:           func() {},
			bucket:          "system",
			key:             "new.json",
			remoteVV:        VersionVector{"coord2": 1},
			expectedRel:     VectorBefore, // Empty happened before remote
			needsResolution: false,
		},
		{
			name: "local happened before remote",
			setup: func() {
				state.Clear()
				state.Update("system", "test.json")
			},
			bucket:          "system",
			key:             "test.json",
			remoteVV:        VersionVector{"coord1": 2},
			expectedRel:     VectorBefore,
			needsResolution: false,
		},
		{
			name: "local happened after remote",
			setup: func() {
				state.Clear()
				state.Update("system", "test.json")
				state.Update("system", "test.json")
			},
			bucket:          "system",
			key:             "test.json",
			remoteVV:        VersionVector{"coord1": 1},
			expectedRel:     VectorAfter,
			needsResolution: false,
		},
		{
			name: "concurrent - conflict",
			setup: func() {
				state.Clear()
				state.Update("system", "test.json")
				state.Update("system", "test.json")
			},
			bucket:          "system",
			key:             "test.json",
			remoteVV:        VersionVector{"coord1": 1, "coord2": 5},
			expectedRel:     VectorConcurrent,
			needsResolution: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			rel, needsRes := state.CheckConflict(tt.bucket, tt.key, tt.remoteVV)
			assert.Equal(t, tt.expectedRel, rel)
			assert.Equal(t, tt.needsResolution, needsRes)
		})
	}
}

func TestState_Delete(t *testing.T) {
	state := NewState("coord1")

	// Create some keys
	state.Update("system", "test1.json")
	state.Update("system", "test2.json")
	assert.Equal(t, 2, state.Count())

	// Delete one key
	state.Delete("system", "test1.json")
	assert.Equal(t, 1, state.Count())

	// Verify it's gone
	vv := state.Get("system", "test1.json")
	assert.Equal(t, 0, len(vv))

	// Other key still exists
	vv = state.Get("system", "test2.json")
	assert.Equal(t, uint64(1), vv.Get("coord1"))

	// Delete non-existent key doesn't error
	state.Delete("system", "nonexistent.json")
	assert.Equal(t, 1, state.Count())
}

func TestState_DeleteBucket(t *testing.T) {
	state := NewState("coord1")

	// Populate two buckets
	state.Update("bucket1", "obj1.json")
	state.Update("bucket1", "obj2.json")
	state.Update("bucket1", "sub/obj3.json")
	state.Update("bucket2", "other.json")
	state.Update("bucket2", "another.json")
	assert.Equal(t, 5, state.Count())

	// Delete entire bucket1
	state.DeleteBucket("bucket1")

	// All bucket1 keys should be gone
	assert.Equal(t, 2, state.Count())
	assert.Equal(t, 0, len(state.Get("bucket1", "obj1.json")))
	assert.Equal(t, 0, len(state.Get("bucket1", "obj2.json")))
	assert.Equal(t, 0, len(state.Get("bucket1", "sub/obj3.json")))

	// bucket2 keys must remain intact
	assert.Equal(t, uint64(1), state.Get("bucket2", "other.json").Get("coord1"))
	assert.Equal(t, uint64(1), state.Get("bucket2", "another.json").Get("coord1"))

	// Deleting non-existent bucket is a no-op
	state.DeleteBucket("bucket1")
	assert.Equal(t, 2, state.Count())

	// Deleting the remaining bucket leaves state empty
	state.DeleteBucket("bucket2")
	assert.Equal(t, 0, state.Count())

	// Bucket name that is a prefix of another bucket name must not collide.
	// "data/" prefix must not match "data-archive/file.txt".
	state.Update("data", "file.txt")
	state.Update("data-archive", "file.txt")
	state.DeleteBucket("data")
	assert.Equal(t, 1, state.Count())
	assert.Equal(t, uint64(1), state.Get("data-archive", "file.txt").Get("coord1"))
}

func TestState_Clear(t *testing.T) {
	state := NewState("coord1")

	// Add some data
	state.Update("system", "test1.json")
	state.Update("system", "test2.json")
	state.Update("user", "data.json")
	assert.Equal(t, 3, state.Count())

	// Clear
	state.Clear()
	assert.Equal(t, 0, state.Count())

	// Verify all keys are gone
	vv := state.Get("system", "test1.json")
	assert.Equal(t, 0, len(vv))
}

func TestState_Snapshot(t *testing.T) {
	state := NewState("coord1")

	// Create some state
	state.Update("system", "test1.json")
	state.Update("system", "test1.json")
	state.Update("system", "test2.json")
	state.Merge("user", "data.json", VersionVector{"coord2": 5})

	// Create snapshot
	data, err := state.Snapshot()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify it's valid JSON
	var snapshot stateSnapshot
	err = json.Unmarshal(data, &snapshot)
	require.NoError(t, err)
	assert.Equal(t, "coord1", snapshot.NodeID)
	assert.Equal(t, 3, len(snapshot.Vectors))
	assert.NotEmpty(t, snapshot.Checksum)

	// Verify checksumincluded
	assert.Len(t, snapshot.Checksum, 64) // SHA256 hex string
}

func TestState_LoadSnapshot(t *testing.T) {
	// Create original state
	original := NewState("coord1")
	original.Update("system", "test1.json")
	original.Update("system", "test1.json")
	original.Update("system", "test2.json")
	original.Merge("user", "data.json", VersionVector{"coord2": 5})

	// Create snapshot
	data, err := original.Snapshot()
	require.NoError(t, err)

	// Load into new state
	restored := NewState("coord2") // Different nodeID initially
	err = restored.LoadSnapshot(data)
	require.NoError(t, err)

	// Verify restoration
	// SECURITY FIX #8: NodeID should NOT change when loading snapshot (prevents identity corruption)
	assert.Equal(t, "coord2", restored.nodeID) // NodeID should remain unchanged
	assert.Equal(t, original.Count(), restored.Count())

	// Verify individual keys
	vv1 := restored.Get("system", "test1.json")
	assert.Equal(t, uint64(2), vv1.Get("coord1"))

	vv2 := restored.Get("system", "test2.json")
	assert.Equal(t, uint64(1), vv2.Get("coord1"))

	vv3 := restored.Get("user", "data.json")
	assert.Equal(t, uint64(5), vv3.Get("coord2"))
}

func TestState_LoadSnapshot_ChecksumValidation(t *testing.T) {
	state := NewState("coord1")
	state.Update("system", "test.json")

	// Create snapshot
	data, err := state.Snapshot()
	require.NoError(t, err)

	// Tamper with the snapshot
	var snapshot stateSnapshot
	err = json.Unmarshal(data, &snapshot)
	require.NoError(t, err)

	// Modify data without updating checksum
	snapshot.Vectors["system/test.json"] = VersionVector{"coord1": 999}

	tamperedData, err := json.Marshal(snapshot)
	require.NoError(t, err)

	// Loading tampered snapshot should fail
	newState := NewState("coord2")
	err = newState.LoadSnapshot(tamperedData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checksum mismatch")
}

func TestState_GetStats(t *testing.T) {
	state := NewState("coord1")

	// Empty state
	stats := state.GetStats()
	assert.Equal(t, "coord1", stats.NodeID)
	assert.Equal(t, 0, stats.TrackedKeys)
	assert.Equal(t, uint64(0), stats.TotalUpdates)

	// Add some data
	state.Update("system", "test1.json")                         // coord1: 1
	state.Update("system", "test1.json")                         // coord1: 2
	state.Update("system", "test2.json")                         // coord1: 1
	state.Merge("user", "data.json", VersionVector{"coord2": 5}) // coord2: 5

	stats = state.GetStats()
	assert.Equal(t, "coord1", stats.NodeID)
	assert.Equal(t, 3, stats.TrackedKeys)
	assert.Equal(t, uint64(8), stats.TotalUpdates) // 2 + 1 + 5
}

func TestState_ListKeys(t *testing.T) {
	state := NewState("coord1")

	// Empty state
	keys := state.ListKeys()
	assert.Empty(t, keys)

	// Add some keys
	state.Update("system", "test1.json")
	state.Update("system", "test2.json")
	state.Update("user", "data.json")

	keys = state.ListKeys()
	assert.Len(t, keys, 3)
	assert.Contains(t, keys, "system/test1.json")
	assert.Contains(t, keys, "system/test2.json")
	assert.Contains(t, keys, "user/data.json")
}

func TestState_ConcurrentAccess(t *testing.T) {
	// This test verifies thread-safety by running concurrent operations
	// Similar to the SRV registry concurrency test

	state := NewState("coord1")
	const goroutines = 20
	const opsPerGoroutine = 50

	// Run concurrent updates
	done := make(chan bool)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			for j := 0; j < opsPerGoroutine; j++ {
				// Mix different operations
				switch j % 4 {
				case 0:
					state.Update("system", "test.json")
				case 1:
					_ = state.Get("system", "test.json")
				case 2:
					state.Merge("system", "test.json", VersionVector{"coord2": uint64(j)})
				case 3:
					_, _ = state.CheckConflict("system", "test.json", VersionVector{"coord2": uint64(j)})
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// If we get here without panicking, thread safety works
	assert.Greater(t, state.Count(), 0)
}

func TestState_ReplicationScenario(t *testing.T) {
	t.Run("two coordinators sequential sync", func(t *testing.T) {
		coord1 := NewState("coord1")
		coord2 := NewState("coord2")

		// Coord1 creates a file
		vv1 := coord1.Update("system", "config.json")

		// Coord1 replicates to Coord2
		merged, isNew := coord2.Merge("system", "config.json", vv1)
		assert.True(t, isNew)
		assert.Equal(t, uint64(1), merged.Get("coord1"))

		// Coord2 modifies the file
		vv2 := coord2.Update("system", "config.json")
		assert.Equal(t, uint64(1), vv2.Get("coord1"))
		assert.Equal(t, uint64(1), vv2.Get("coord2"))

		// Coord2 replicates back to Coord1
		merged, isNew = coord1.Merge("system", "config.json", vv2)
		assert.False(t, isNew)
		assert.Equal(t, uint64(1), merged.Get("coord1"))
		assert.Equal(t, uint64(1), merged.Get("coord2"))

		// No conflict - sequential updates
		rel, needsRes := coord1.CheckConflict("system", "config.json", vv2)
		assert.Equal(t, VectorEqual, rel) // Already merged
		assert.False(t, needsRes)
	})

	t.Run("two coordinators concurrent conflict", func(t *testing.T) {
		coord1 := NewState("coord1")
		coord2 := NewState("coord2")

		// Both start from same base
		base := VersionVector{"coord1": 1}
		coord1.Merge("system", "config.json", base)
		coord2.Merge("system", "config.json", base)

		// Both modify concurrently
		vv1 := coord1.Update("system", "config.json")
		vv2 := coord2.Update("system", "config.json")

		// Coord1 receives Coord2's update
		rel, needsRes := coord1.CheckConflict("system", "config.json", vv2)
		assert.Equal(t, VectorConcurrent, rel)
		assert.True(t, needsRes)

		// Resolve conflict - choose winner based on version vectors
		winner := ResolveConflict(vv1, vv2)
		assert.NotNil(t, winner)

		// Create merged version vector that includes both updates
		mergedVV := vv1.Copy()
		mergedVV.Merge(vv2)

		// Both coordinators should adopt the merged version vector
		// (In practice, winner determines which DATA to use, but version vector includes both updates)
		coord1.Merge("system", "config.json", mergedVV)
		coord2.Merge("system", "config.json", mergedVV)

		// Both should now have same version
		final1 := coord1.Get("system", "config.json")
		final2 := coord2.Get("system", "config.json")
		assert.True(t, final1.Equal(final2))

		// Both should have seen updates from both coordinators
		assert.Equal(t, uint64(2), final1.Get("coord1"))
		assert.Equal(t, uint64(1), final1.Get("coord2"))
	})

	t.Run("three-way concurrent conflict", func(t *testing.T) {
		coord1 := NewState("coord1")
		coord2 := NewState("coord2")
		coord3 := NewState("coord3")

		// All start from empty
		vv1 := coord1.Update("system", "config.json")
		vv2 := coord2.Update("system", "config.json")
		vv3 := coord3.Update("system", "config.json")

		// All are concurrent
		assert.True(t, vv1.ConcurrentWith(vv2))
		assert.True(t, vv1.ConcurrentWith(vv3))
		assert.True(t, vv2.ConcurrentWith(vv3))

		// Create merged version vector that includes all three updates
		mergedVV := vv1.Copy()
		mergedVV.Merge(vv2)
		mergedVV.Merge(vv3)

		// All coordinators adopt the merged version vector
		// (Winner determines which DATA to use, but VV includes all updates)
		coord1.Merge("system", "config.json", mergedVV)
		coord2.Merge("system", "config.json", mergedVV)
		coord3.Merge("system", "config.json", mergedVV)

		// Verify convergence
		final1 := coord1.Get("system", "config.json")
		final2 := coord2.Get("system", "config.json")
		final3 := coord3.Get("system", "config.json")

		assert.True(t, final1.Equal(final2))
		assert.True(t, final1.Equal(final3))

		// All should have seen all three updates
		assert.Equal(t, uint64(1), final1.Get("coord1"))
		assert.Equal(t, uint64(1), final1.Get("coord2"))
		assert.Equal(t, uint64(1), final1.Get("coord3"))
	})
}

func TestState_PruneDeletedKeys(t *testing.T) {
	t.Run("prunes stale inactive keys", func(t *testing.T) {
		state := NewState("coord1")

		// Add keys
		state.Update("bucket", "active.json")
		state.Update("bucket", "deleted.json")
		assert.Equal(t, 2, state.Count())

		// Back-date the lastUpdated for the deleted key so it looks old
		state.mu.Lock()
		state.lastUpdated["bucket/deleted.json"] = time.Now().Add(-20 * time.Minute)
		state.mu.Unlock()

		// active.json is in the active set; deleted.json is not
		activeKeys := map[string]bool{"bucket/active.json": true}
		pruned := state.PruneDeletedKeys(activeKeys, 10*time.Minute)

		assert.Equal(t, 1, pruned)
		assert.Equal(t, 1, state.Count())
		assert.NotEmpty(t, state.Get("bucket", "active.json"))
		assert.Empty(t, state.Get("bucket", "deleted.json"))
	})

	t.Run("does not prune keys within grace period", func(t *testing.T) {
		state := NewState("coord1")

		state.Update("bucket", "recent.json")
		// recent.json is not in active set, but was just updated — must be kept
		activeKeys := map[string]bool{}
		pruned := state.PruneDeletedKeys(activeKeys, 10*time.Minute)

		assert.Equal(t, 0, pruned)
		assert.Equal(t, 1, state.Count())
	})

	t.Run("does not prune keys still in active set", func(t *testing.T) {
		state := NewState("coord1")

		state.Update("bucket", "live.json")
		// Back-date so it looks old
		state.mu.Lock()
		state.lastUpdated["bucket/live.json"] = time.Now().Add(-1 * time.Hour)
		state.mu.Unlock()

		// Key is still active — must not be pruned even though it is old
		activeKeys := map[string]bool{"bucket/live.json": true}
		pruned := state.PruneDeletedKeys(activeKeys, 10*time.Minute)

		assert.Equal(t, 0, pruned)
		assert.Equal(t, 1, state.Count())
	})

	t.Run("empty state returns zero", func(t *testing.T) {
		state := NewState("coord1")
		pruned := state.PruneDeletedKeys(map[string]bool{}, time.Minute)
		assert.Equal(t, 0, pruned)
	})
}

func TestState_PruneDeletedKeys_Timestamps(t *testing.T) {
	state := NewState("coord1")

	// lastUpdated should be set by Update
	before := time.Now()
	state.Update("bucket", "obj.json")
	after := time.Now()

	state.mu.RLock()
	ts, ok := state.lastUpdated["bucket/obj.json"]
	state.mu.RUnlock()

	assert.True(t, ok, "lastUpdated should be set after Update")
	assert.True(t, !ts.Before(before) && !ts.After(after), "timestamp should be within test window")

	// lastUpdated should be set by Merge
	state.Merge("bucket", "merged.json", VersionVector{"coord2": 1})
	state.mu.RLock()
	_, ok = state.lastUpdated["bucket/merged.json"]
	state.mu.RUnlock()
	assert.True(t, ok, "lastUpdated should be set after Merge")

	// lastUpdated should be set by Set
	state.Set("bucket", "set.json", VersionVector{"coord1": 5})
	state.mu.RLock()
	_, ok = state.lastUpdated["bucket/set.json"]
	state.mu.RUnlock()
	assert.True(t, ok, "lastUpdated should be set after Set")

	// lastUpdated should be cleared by Delete
	state.Delete("bucket", "obj.json")
	state.mu.RLock()
	_, ok = state.lastUpdated["bucket/obj.json"]
	state.mu.RUnlock()
	assert.False(t, ok, "lastUpdated should be cleared after Delete")
}
