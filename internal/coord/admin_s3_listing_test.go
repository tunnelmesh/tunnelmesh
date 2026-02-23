package coord

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpsertObjectList_NewKey(t *testing.T) {
	objs := []S3ObjectInfo{
		{Key: "a.txt", Size: 10},
	}

	info := S3ObjectInfo{Key: "b.txt", Size: 20}
	result := upsertObjectList(objs, "b.txt", info)

	assert.Len(t, result, 2)
	assert.Equal(t, "a.txt", result[0].Key)
	assert.Equal(t, "b.txt", result[1].Key)
}

func TestRemoveFromObjectList_NotFound(t *testing.T) {
	objs := []S3ObjectInfo{
		{Key: "a.txt", Size: 10},
		{Key: "b.txt", Size: 20},
	}

	result, removed := removeFromObjectList(objs, "nonexistent.txt")

	assert.Len(t, result, 2)
	assert.Nil(t, removed)
}

func TestListingIndexEqual_BothNil(t *testing.T) {
	assert.True(t, listingIndexEqual(nil, nil))
}

func TestListingIndexEqual_OneNil(t *testing.T) {
	idx := &listingIndex{Buckets: make(map[string]*bucketListing)}
	assert.False(t, listingIndexEqual(nil, idx))
	assert.False(t, listingIndexEqual(idx, nil))
}

func TestListingIndexEqual_IgnoresSeq(t *testing.T) {
	a := &listingIndex{
		Buckets: map[string]*bucketListing{
			"b": {Objects: []S3ObjectInfo{{Key: "k", Size: 1, LastModified: "t1"}}},
		},
		Seq: 5,
	}
	b := &listingIndex{
		Buckets: map[string]*bucketListing{
			"b": {Objects: []S3ObjectInfo{{Key: "k", Size: 1, LastModified: "t1"}}},
		},
		Seq: 99,
	}
	assert.True(t, listingIndexEqual(a, b), "Seq should be ignored in equality comparison")
}

func TestUpdateListingIndex_IncrementsSeq(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	info := S3ObjectInfo{Key: "a.txt", Size: 10, LastModified: "2024-01-01T00:00:00Z"}
	srv.updateListingIndex("bkt", "a.txt", &info, "put")

	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
	assert.Equal(t, uint64(1), idx.Seq)

	// Second update should increment again
	info2 := S3ObjectInfo{Key: "b.txt", Size: 20, LastModified: "2024-01-02T00:00:00Z"}
	srv.updateListingIndex("bkt", "b.txt", &info2, "put")

	idx2 := srv.localListingIndex.Load()
	assert.Equal(t, uint64(2), idx2.Seq)
}

func TestReconcileLocalIndex_MergesWithConcurrentUpdates(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Pre-populate listing index (Seq=1)
	info := S3ObjectInfo{Key: "file.txt", Size: 100, LastModified: "2024-01-01T00:00:00Z"}
	srv.updateListingIndex("test-bucket", "file.txt", &info, "put")

	before := srv.localListingIndex.Load()
	require.NotNil(t, before)
	require.Equal(t, uint64(1), before.Seq)

	// Run reconcile in a goroutine so we can do an incremental update while
	// it scans the filesystem (ListBuckets → ListObjects → ListRecycledObjects).
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.reconcileLocalIndex(t.Context())
	}()

	// Give reconcile time to capture preSeq and start the filesystem scan.
	time.Sleep(20 * time.Millisecond)

	// Incremental update bumps Seq from 1 → 2 while reconcile is mid-scan.
	info2 := S3ObjectInfo{Key: "concurrent.txt", Size: 50, LastModified: "2024-01-02T00:00:00Z"}
	srv.updateListingIndex("test-bucket", "concurrent.txt", &info2, "put")

	<-done

	after := srv.localListingIndex.Load()
	require.NotNil(t, after)

	// The concurrent update must be preserved — reconcile merges filesystem
	// scan (ground truth) with incremental updates that arrived during the scan.
	assert.Equal(t, uint64(2), after.Seq)
	bl := after.Buckets["test-bucket"]
	require.NotNil(t, bl)
	found := false
	for _, obj := range bl.Objects {
		if obj.Key == "concurrent.txt" {
			found = true
		}
	}
	assert.True(t, found, "concurrent incremental update should be preserved via merge")
}

func TestReconcileLocalIndex_RemovesStaleEntries(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Add a stale entry that does NOT exist on the filesystem
	stale := S3ObjectInfo{Key: "stale-ghost.txt", Size: 999, LastModified: "2024-01-01T00:00:00Z"}
	srv.updateListingIndex("test-bucket", "stale-ghost.txt", &stale, "put")

	// Also add one more to bump Seq (simulating sustained writes)
	info := S3ObjectInfo{Key: "another.txt", Size: 10, LastModified: "2024-01-02T00:00:00Z"}
	srv.updateListingIndex("test-bucket", "another.txt", &info, "put")

	before := srv.localListingIndex.Load()
	require.NotNil(t, before)
	require.Equal(t, uint64(2), before.Seq)

	// Reconcile scans the actual filesystem — neither stale-ghost.txt nor
	// another.txt exist on disk, so both should be removed.
	srv.reconcileLocalIndex(t.Context())

	after := srv.localListingIndex.Load()
	require.NotNil(t, after)

	// The stale entry should be gone (filesystem is ground truth)
	bl := after.Buckets["test-bucket"]
	if bl != nil {
		for _, obj := range bl.Objects {
			assert.NotEqual(t, "stale-ghost.txt", obj.Key, "stale entry should be removed by reconcile")
		}
	}
}

func TestForwardedEntryTTL(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Need multiple coord IPs for loadPeerIndexes to process forwarded entries
	srv.storeCoordIPs([]string{"10.0.0.1", "10.0.0.2"})

	// Seed peerListings with a forwarded entry that has already expired
	expired := &peerListings{
		Objects: map[string][]S3ObjectInfo{
			"bkt": {
				{
					Key:         "old.txt",
					Size:        10,
					Forwarded:   true,
					ForwardedAt: time.Now().Add(-15 * time.Minute), // 15min ago, well past 10min TTL
					SourceIP:    "10.0.0.2",
				},
				{
					Key:         "fresh.txt",
					Size:        20,
					Forwarded:   true,
					ForwardedAt: time.Now(), // just now
					SourceIP:    "10.0.0.2",
				},
			},
		},
		Recycled: make(map[string][]S3ObjectInfo),
	}
	srv.peerListings.Store(expired)

	// loadPeerIndexes should drop "old.txt" (expired) and keep "fresh.txt"
	srv.loadPeerIndexes(t.Context())

	pl := srv.peerListings.Load()
	require.NotNil(t, pl)

	var foundOld, foundFresh bool
	for _, obj := range pl.Objects["bkt"] {
		if obj.Key == "old.txt" {
			foundOld = true
		}
		if obj.Key == "fresh.txt" {
			foundFresh = true
		}
	}
	assert.False(t, foundOld, "expired forwarded entry should be removed")
	assert.True(t, foundFresh, "fresh forwarded entry should be preserved")
}

func TestUpdateListingIndex_DeleteDeduplicatesRecycled(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Put → Delete → Put → Delete the same key
	info := S3ObjectInfo{Key: "file.txt", Size: 100, LastModified: "2024-01-01T00:00:00Z"}
	srv.updateListingIndex("bkt", "file.txt", &info, "put")
	srv.updateListingIndex("bkt", "file.txt", nil, "delete")

	info2 := S3ObjectInfo{Key: "file.txt", Size: 200, LastModified: "2024-02-01T00:00:00Z"}
	srv.updateListingIndex("bkt", "file.txt", &info2, "put")
	srv.updateListingIndex("bkt", "file.txt", nil, "delete")

	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
	bl := idx.Buckets["bkt"]
	require.NotNil(t, bl)

	// Should have exactly one recycled entry (dedup by key)
	assert.Len(t, bl.Recycled, 1, "recycled list should be deduplicated")
	assert.Equal(t, "file.txt", bl.Recycled[0].Key)
}

// --- Regression tests for listing index sync ---

// TestUpdateListingIndex_RemoveOp verifies that the "remove" op clears entries
// from both Objects and Recycled in one atomic update.
func TestUpdateListingIndex_RemoveOp(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Sub-test 1: put → remove clears Objects
	info := S3ObjectInfo{Key: "a.txt", Size: 10, LastModified: "2024-01-01T00:00:00Z"}
	srv.updateListingIndex("bkt", "a.txt", &info, "put")
	srv.updateListingIndex("bkt", "a.txt", nil, "remove")

	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
	bl := idx.Buckets["bkt"]
	if bl != nil {
		assert.Empty(t, bl.Objects, "remove should clear from Objects")
		assert.Empty(t, bl.Recycled, "remove should leave Recycled empty")
	}

	// Sub-test 2: put → delete → remove clears Recycled too
	srv2 := newTestServerWithListingIndex(t)
	info2 := S3ObjectInfo{Key: "b.txt", Size: 20, LastModified: "2024-01-01T00:00:00Z"}
	srv2.updateListingIndex("bkt", "b.txt", &info2, "put")
	srv2.updateListingIndex("bkt", "b.txt", nil, "delete") // moves to Recycled
	srv2.updateListingIndex("bkt", "b.txt", nil, "remove") // clears from Recycled

	idx2 := srv2.localListingIndex.Load()
	require.NotNil(t, idx2)
	bl2 := idx2.Buckets["bkt"]
	if bl2 != nil {
		assert.Empty(t, bl2.Objects, "remove should leave Objects empty")
		assert.Empty(t, bl2.Recycled, "remove should clear from Recycled")
	}
}

// TestPurgeObject_UpdatesListingIndex verifies that calling store.PurgeObject
// immediately removes the entry from the listing index via onObjectRemovedCallback.
func TestPurgeObject_UpdatesListingIndex(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Wire the callback (mirrors server.go initialization)
	srv.s3Store.SetOnObjectRemovedCallback(func(bucket, key string) {
		srv.updateListingIndex(bucket, key, nil, "remove")
	})

	// Put via the store to create the object on disk
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "purge-me.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)

	// Add to listing index to simulate what the HTTP handler does
	info := S3ObjectInfo{Key: "purge-me.txt", Size: 4, LastModified: time.Now().Format(time.RFC3339)}
	srv.updateListingIndex("test-bucket", "purge-me.txt", &info, "put")

	// Verify it appears in the listing
	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
	bl := idx.Buckets["test-bucket"]
	require.NotNil(t, bl)
	found := false
	for _, obj := range bl.Objects {
		if obj.Key == "purge-me.txt" {
			found = true
		}
	}
	require.True(t, found, "object should be in listing before purge")

	// PurgeObject simulates what replication delete does
	err = srv.s3Store.PurgeObject(context.Background(), "test-bucket", "purge-me.txt")
	require.NoError(t, err)

	// Entry should be gone from the listing immediately
	idx2 := srv.localListingIndex.Load()
	require.NotNil(t, idx2)
	bl2 := idx2.Buckets["test-bucket"]
	if bl2 != nil {
		for _, obj := range bl2.Objects {
			assert.NotEqual(t, "purge-me.txt", obj.Key, "purged object should be removed from listing")
		}
		for _, obj := range bl2.Recycled {
			assert.NotEqual(t, "purge-me.txt", obj.Key, "purged object should not be in recycled list")
		}
	}
}

// TestForceDeleteBucket_UpdatesListingIndex verifies that ForceDeleteBucket
// immediately removes the entire bucket from the listing index.
func TestForceDeleteBucket_UpdatesListingIndex(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Wire the callback
	srv.s3Store.SetOnBucketRemovedCallback(func(bucket string) {
		srv.removeListingBucket(bucket)
	})

	// Put 3 objects and add them to the listing
	for i, key := range []string{"a.txt", "b.txt", "c.txt"} {
		_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader([]byte("x")), 1, "text/plain", nil)
		require.NoError(t, err)
		info := S3ObjectInfo{Key: key, Size: int64(i + 1), LastModified: time.Now().Format(time.RFC3339)}
		srv.updateListingIndex("test-bucket", key, &info, "put")
	}

	// Verify all 3 appear in the listing
	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
	require.NotNil(t, idx.Buckets["test-bucket"])
	assert.Len(t, idx.Buckets["test-bucket"].Objects, 3, "all 3 objects should be in listing before delete")

	// ForceDeleteBucket wipes the entire bucket
	err := srv.s3Store.ForceDeleteBucket(context.Background(), "test-bucket")
	require.NoError(t, err)

	// Entire bucket should be absent from the listing
	idx2 := srv.localListingIndex.Load()
	require.NotNil(t, idx2)
	_, exists := idx2.Buckets["test-bucket"]
	assert.False(t, exists, "bucket should be removed from listing after ForceDeleteBucket")
}

// TestReplicationDelete_UpdatesListingIndex simulates the replication path:
// an object is added to both the store and the listing index, then PurgeObject
// is called (as applyReplication does for delete ops) — the listing must clear immediately.
func TestReplicationDelete_UpdatesListingIndex(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Wire the callback (same as server.go initialization)
	srv.s3Store.SetOnObjectRemovedCallback(func(bucket, key string) {
		srv.updateListingIndex(bucket, key, nil, "remove")
	})

	// Simulate replication arrival: object appears on disk
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "replicated.txt", bytes.NewReader([]byte("hello")), 5, "text/plain", nil)
	require.NoError(t, err)

	// Listing index updated (as reconcileLocalIndex or updateListingIndex would do)
	info := S3ObjectInfo{Key: "replicated.txt", Size: 5, LastModified: time.Now().Format(time.RFC3339)}
	srv.updateListingIndex("test-bucket", "replicated.txt", &info, "put")

	// Simulate replication delete: remote peer deleted the object
	err = srv.s3Store.PurgeObject(context.Background(), "test-bucket", "replicated.txt")
	require.NoError(t, err)

	// Listing must be clean — no phantom entry
	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
	if bl := idx.Buckets["test-bucket"]; bl != nil {
		for _, obj := range bl.Objects {
			assert.NotEqual(t, "replicated.txt", obj.Key, "deleted object should not remain in listing")
		}
	}
}

// TestRecycledPurge_UpdatesListingIndex covers the recycle-bin GC path:
// put → delete (moves to Recycled) → PurgeAllRecycled → both Objects and Recycled empty.
func TestRecycledPurge_UpdatesListingIndex(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Wire the callback
	srv.s3Store.SetOnObjectRemovedCallback(func(bucket, key string) {
		srv.updateListingIndex(bucket, key, nil, "remove")
	})

	// Put and add to listing
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "will-purge.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)
	info := S3ObjectInfo{Key: "will-purge.txt", Size: 4, LastModified: time.Now().Format(time.RFC3339)}
	srv.updateListingIndex("test-bucket", "will-purge.txt", &info, "put")

	// Delete (moves to recycled)
	err = srv.s3Store.DeleteObject(context.Background(), "test-bucket", "will-purge.txt")
	require.NoError(t, err)
	srv.updateListingIndex("test-bucket", "will-purge.txt", nil, "delete")

	// Verify in Recycled
	idx := srv.localListingIndex.Load()
	bl := idx.Buckets["test-bucket"]
	require.NotNil(t, bl)
	assert.Empty(t, bl.Objects, "object should have moved out of Objects")
	assert.Len(t, bl.Recycled, 1, "object should be in Recycled")

	// GC purge removes from recycle bin
	srv.s3Store.PurgeAllRecycled(context.Background())

	// Both Objects and Recycled should be empty
	idx2 := srv.localListingIndex.Load()
	require.NotNil(t, idx2)
	if bl2 := idx2.Buckets["test-bucket"]; bl2 != nil {
		assert.Empty(t, bl2.Objects, "Objects should be empty after purge")
		assert.Empty(t, bl2.Recycled, "Recycled should be empty after purge")
	}
}

// TestReconcileLocalIndex_StaleEntriesMetric verifies that countStaleListingEntries
// correctly detects phantom entries (in current but absent from filesystem scan).
func TestReconcileLocalIndex_StaleEntriesMetric(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Inject a stale entry that does NOT exist on disk
	stale := S3ObjectInfo{Key: "phantom.txt", Size: 999, LastModified: "2024-01-01T00:00:00Z"}
	srv.updateListingIndex("test-bucket", "phantom.txt", &stale, "put")

	// countStaleListingEntries should detect the phantom entry
	current := srv.localListingIndex.Load()
	emptyFilesystem := &listingIndex{Buckets: make(map[string]*bucketListing)}
	count := countStaleListingEntries(current, emptyFilesystem)
	assert.Greater(t, count, 0, "should detect stale entry before reconcile")

	// countStaleListingEntries returns 0 for nil current index
	assert.Equal(t, 0, countStaleListingEntries(nil, emptyFilesystem), "nil current should return 0")

	// Reconcile scans the actual filesystem — phantom.txt doesn't exist on disk
	srv.reconcileLocalIndex(context.Background())

	// After reconcile, phantom entry should be removed
	after := srv.localListingIndex.Load()
	require.NotNil(t, after)
	if bl := after.Buckets["test-bucket"]; bl != nil {
		for _, obj := range bl.Objects {
			assert.NotEqual(t, "phantom.txt", obj.Key, "stale entry should be removed by reconcile")
		}
	}

	// After reconcile with a filesystem that matches, count should be 0
	cleanFilesystem := &listingIndex{Buckets: make(map[string]*bucketListing)}
	afterCount := countStaleListingEntries(after, cleanFilesystem)
	assert.Equal(t, 0, afterCount, "no stale entries after reconcile clears phantom")
}
