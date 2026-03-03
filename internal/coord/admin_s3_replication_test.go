package coord

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdatePeerListingsAfterForward_Put(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Create a PUT request to simulate a forwarded write
	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/forwarded.txt", nil)
	req.Header.Set("Content-Type", "text/plain")
	req.ContentLength = 42

	srv.updatePeerListingsAfterForward("test-bucket", "forwarded.txt", "10.0.0.2", req)

	pl := srv.peerListings.Load()
	require.NotNil(t, pl)
	objs := pl.Objects["test-bucket"]
	require.Len(t, objs, 1)
	assert.Equal(t, "forwarded.txt", objs[0].Key)
	assert.Equal(t, int64(42), objs[0].Size)
	assert.Equal(t, "text/plain", objs[0].ContentType)
	assert.True(t, objs[0].Forwarded, "PUT entry should be marked as Forwarded")
	assert.Equal(t, "10.0.0.2", objs[0].SourceIP, "PUT entry should have SourceIP set")
}

func TestUpdatePeerListingsAfterForward_Delete(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// First add an entry via PUT
	putReq := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/del.txt", nil)
	putReq.ContentLength = 10
	srv.updatePeerListingsAfterForward("test-bucket", "del.txt", "10.0.0.2", putReq)

	// Then delete it
	delReq := httptest.NewRequest(http.MethodDelete, "/api/s3/buckets/test-bucket/objects/del.txt", nil)
	srv.updatePeerListingsAfterForward("test-bucket", "del.txt", "10.0.0.2", delReq)

	pl := srv.peerListings.Load()
	require.NotNil(t, pl)
	assert.Empty(t, pl.Objects["test-bucket"])

	// Verify the deleted object appears in the recycled list with a DeletedAt timestamp
	recycled := pl.Recycled["test-bucket"]
	require.Len(t, recycled, 1)
	assert.Equal(t, "del.txt", recycled[0].Key)
	assert.Equal(t, int64(10), recycled[0].Size)
	assert.NotEmpty(t, recycled[0].DeletedAt)
	assert.True(t, recycled[0].Forwarded, "DELETE recycled entry should be marked as Forwarded")
	assert.Equal(t, "10.0.0.2", recycled[0].SourceIP, "DELETE recycled entry should have SourceIP set")
}

func TestUpdatePeerListingsAfterForward_DeleteNonExistent(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Delete a key that was never added — recycled list should stay empty
	delReq := httptest.NewRequest(http.MethodDelete, "/api/s3/buckets/test-bucket/objects/ghost.txt", nil)
	srv.updatePeerListingsAfterForward("test-bucket", "ghost.txt", "10.0.0.2", delReq)

	pl := srv.peerListings.Load()
	require.NotNil(t, pl)
	assert.Empty(t, pl.Objects["test-bucket"])
	assert.Empty(t, pl.Recycled["test-bucket"])
}

func TestDiscardResponseWriter_CapturesStatus(t *testing.T) {
	d := &discardResponseWriter{header: make(http.Header)}

	d.WriteHeader(http.StatusNotFound)
	assert.Equal(t, http.StatusNotFound, d.status)

	n, err := d.Write([]byte("discarded"))
	assert.NoError(t, err)
	assert.Equal(t, 9, n) // len("discarded")
}

func TestLoadPeerIndexes_PreservesForwardedEntries(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Inject a forwarded entry via updatePeerListingsAfterForward
	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/fwd.txt", nil)
	req.Header.Set("Content-Type", "text/plain")
	req.ContentLength = 99
	srv.updatePeerListingsAfterForward("test-bucket", "fwd.txt", "10.0.0.3", req)

	// Simulate having two coordinators so loadPeerIndexes doesn't short-circuit
	srv.storeCoordIPs([]string{"10.0.0.1", "10.0.0.2"})

	// Run loadPeerIndexes — system store has no peer data, so without
	// forwarded-entry preservation the entry would be lost.
	srv.loadPeerIndexes(context.Background())

	pl := srv.peerListings.Load()
	require.NotNil(t, pl)
	objs := pl.Objects["test-bucket"]
	require.Len(t, objs, 1, "forwarded entry should survive loadPeerIndexes")
	assert.Equal(t, "fwd.txt", objs[0].Key)
	assert.True(t, objs[0].Forwarded)
	assert.Equal(t, "10.0.0.3", objs[0].SourceIP, "SourceIP should survive loadPeerIndexes")
}

func TestLoadPeerIndexes_SupersedesForwardedEntry(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Inject a forwarded entry
	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/fwd.txt", nil)
	req.Header.Set("Content-Type", "text/plain")
	req.ContentLength = 99
	srv.updatePeerListingsAfterForward("test-bucket", "fwd.txt", "10.0.0.3", req)

	// Simulate two coordinators
	srv.storeCoordIPs([]string{"10.0.0.1", "10.0.0.2"})

	// Persist a peer index for 10.0.0.2 that contains the same key with a newer timestamp
	peerIdx := &listingIndex{
		Buckets: map[string]*bucketListing{
			"test-bucket": {
				Objects: []S3ObjectInfo{
					{
						Key:          "fwd.txt",
						Size:         99,
						LastModified: "2099-01-01T00:00:00Z",
						ContentType:  "text/plain",
						Owner:        "remote-peer",
					},
				},
			},
		},
	}
	err := srv.s3SystemStore.SaveJSON(context.Background(), "listings/10.0.0.2.json", peerIdx)
	require.NoError(t, err)

	// Run loadPeerIndexes — system store entry should supersede the forwarded one
	srv.loadPeerIndexes(context.Background())

	pl := srv.peerListings.Load()
	require.NotNil(t, pl)
	objs := pl.Objects["test-bucket"]
	require.Len(t, objs, 1)
	assert.Equal(t, "fwd.txt", objs[0].Key)
	assert.Equal(t, "remote-peer", objs[0].Owner, "system store entry should win")
	assert.False(t, objs[0].Forwarded, "persisted entry should not be marked Forwarded")
	assert.Equal(t, "10.0.0.2", objs[0].SourceIP, "persisted entry should have SourceIP from peer IP")
}

func TestLoadPeerIndexes_SetsSourceIPFromPeerIP(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Simulate three coordinators
	srv.storeCoordIPs([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})

	// Persist peer indexes with objects
	for _, peer := range []struct {
		ip, key string
	}{
		{"10.0.0.2", "from-peer2.txt"},
		{"10.0.0.3", "from-peer3.txt"},
	} {
		idx := &listingIndex{
			Buckets: map[string]*bucketListing{
				"mybucket": {
					Objects: []S3ObjectInfo{
						{Key: peer.key, Size: 10, LastModified: "2024-01-01T00:00:00Z"},
					},
					Recycled: []S3ObjectInfo{
						{Key: "recycled-" + peer.key, Size: 5, DeletedAt: "2024-01-02T00:00:00Z"},
					},
				},
			},
		}
		err := srv.s3SystemStore.SaveJSON(context.Background(), "listings/"+peer.ip+".json", idx)
		require.NoError(t, err)
	}

	srv.loadPeerIndexes(context.Background())

	pl := srv.peerListings.Load()
	require.NotNil(t, pl)

	objs := pl.Objects["mybucket"]
	require.Len(t, objs, 2)
	sourceIPs := map[string]string{}
	for _, obj := range objs {
		sourceIPs[obj.Key] = obj.SourceIP
	}
	assert.Equal(t, "10.0.0.2", sourceIPs["from-peer2.txt"])
	assert.Equal(t, "10.0.0.3", sourceIPs["from-peer3.txt"])

	recycled := pl.Recycled["mybucket"]
	require.Len(t, recycled, 2)
	recycledIPs := map[string]string{}
	for _, obj := range recycled {
		recycledIPs[obj.Key] = obj.SourceIP
	}
	assert.Equal(t, "10.0.0.2", recycledIPs["recycled-from-peer2.txt"])
	assert.Equal(t, "10.0.0.3", recycledIPs["recycled-from-peer3.txt"])
}

func TestFindObjectSourceIP(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// No peer listings — should return empty
	assert.Equal(t, "", srv.findObjectSourceIP("bucket", "key"))

	// Inject entries via peer listings
	srv.peerListings.Store(&peerListings{
		Objects: map[string][]S3ObjectInfo{
			"mybucket": {
				{Key: "a.txt", SourceIP: "10.0.0.2"},
				{Key: "b.txt", SourceIP: "10.0.0.3"},
				{Key: "c.txt", SourceIP: ""}, // no SourceIP
			},
		},
	})

	assert.Equal(t, "10.0.0.2", srv.findObjectSourceIP("mybucket", "a.txt"))
	assert.Equal(t, "10.0.0.3", srv.findObjectSourceIP("mybucket", "b.txt"))
	assert.Equal(t, "", srv.findObjectSourceIP("mybucket", "c.txt"), "empty SourceIP should return empty")
	assert.Equal(t, "", srv.findObjectSourceIP("mybucket", "nonexistent.txt"))
	assert.Equal(t, "", srv.findObjectSourceIP("otherbucket", "a.txt"))
}

func TestFindRecycledObjectSourceIP(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// No peer listings — should return empty
	assert.Equal(t, "", srv.findRecycledObjectSourceIP("bucket", "key"))

	// Inject entries via peer listings
	srv.peerListings.Store(&peerListings{
		Objects: map[string][]S3ObjectInfo{},
		Recycled: map[string][]S3ObjectInfo{
			"mybucket": {
				{Key: "deleted-a.txt", SourceIP: "10.0.0.2"},
				{Key: "deleted-b.txt", SourceIP: "10.0.0.3"},
				{Key: "deleted-c.txt", SourceIP: ""}, // no SourceIP
			},
		},
	})

	assert.Equal(t, "10.0.0.2", srv.findRecycledObjectSourceIP("mybucket", "deleted-a.txt"))
	assert.Equal(t, "10.0.0.3", srv.findRecycledObjectSourceIP("mybucket", "deleted-b.txt"))
	assert.Equal(t, "", srv.findRecycledObjectSourceIP("mybucket", "deleted-c.txt"), "empty SourceIP should return empty")
	assert.Equal(t, "", srv.findRecycledObjectSourceIP("mybucket", "nonexistent.txt"))
	assert.Equal(t, "", srv.findRecycledObjectSourceIP("otherbucket", "deleted-a.txt"))
}

func TestCircuitBreakerCleanup(t *testing.T) {
	cb := &coordForwardBreaker{}

	// Inject an expired entry (cooldown already past)
	cb.state = map[string]*coordBreakerState{
		"10.0.0.2": {consecutiveFails: 3, cooldownUntil: time.Now().Add(-1 * time.Minute)},
		// Active entry (cooldown still in future)
		"10.0.0.3": {consecutiveFails: 1, cooldownUntil: time.Now().Add(5 * time.Minute)},
	}

	cb.Cleanup()

	cb.mu.Lock()
	defer cb.mu.Unlock()
	assert.NotContains(t, cb.state, "10.0.0.2", "expired entry should be removed")
	assert.Contains(t, cb.state, "10.0.0.3", "active entry should remain")
}

func TestCircuitBreakerCleanup_NilState(t *testing.T) {
	// Should not panic when state is nil
	cb := &coordForwardBreaker{}
	cb.Cleanup() // no-op, no panic
	assert.Nil(t, cb.state)
}

func TestObjectPrimaryCoordinator_NilIPs(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// No stored IPs - should return empty
	assert.Equal(t, "", srv.objectPrimaryCoordinator("bucket", "key"))
}
