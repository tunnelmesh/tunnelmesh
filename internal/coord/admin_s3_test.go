package coord

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/replication"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

func TestValidateS3Name(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid name", "my-bucket", false},
		{"empty", "", true},
		{"dot", ".", true},
		{"dotdot", "..", true},
		{"contains dotdot", "foo/../bar", true},
		{"absolute path", "/etc/passwd", true},
		{"backslash prefix", "\\etc\\passwd", true},
		{"valid nested", "folder/file.txt", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateS3Name(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestUpdateBucket_QuotaBytes verifies end-to-end wiring of quota_bytes through
// the PATCH /api/s3/buckets/{bucket} HTTP handler to the store.
func TestUpdateBucket_QuotaBytes(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Set a 1 MB quota on the test bucket via the PATCH handler.
	const quotaBytes = int64(1 << 20)
	body, _ := json.Marshal(map[string]int64{"quota_bytes": quotaBytes})
	req := httptest.NewRequest(http.MethodPatch, "/api/s3/buckets/test-bucket", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	// Verify the quota was persisted in the store.
	meta, err := srv.s3Store.HeadBucket(t.Context(), "test-bucket")
	require.NoError(t, err)
	assert.Equal(t, quotaBytes, meta.QuotaBytes)
}

// TestUpdateBucket_QuotaBytes_RemoveLimit verifies that quota_bytes=0 removes an existing quota.
func TestUpdateBucket_QuotaBytes_RemoveLimit(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Set a quota via the store directly (tested independently in store tests).
	q := int64(512 * 1024)
	require.NoError(t, srv.s3Store.UpdateBucketMetadata(t.Context(), "test-bucket",
		s3.BucketMetadataUpdate{QuotaBytes: &q},
	))

	// Remove the quota via the PATCH handler (quota_bytes=0 means unlimited).
	body, _ := json.Marshal(map[string]int64{"quota_bytes": 0})
	req := httptest.NewRequest(http.MethodPatch, "/api/s3/buckets/test-bucket", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	meta, err := srv.s3Store.HeadBucket(t.Context(), "test-bucket")
	require.NoError(t, err)
	assert.Equal(t, int64(0), meta.QuotaBytes)
}

// TestUpdateBucket_QuotaBytes_NegativeRejected verifies the validation path.
func TestUpdateBucket_QuotaBytes_NegativeRejected(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	body, _ := json.Marshal(map[string]int64{"quota_bytes": -1})
	req := httptest.NewRequest(http.MethodPatch, "/api/s3/buckets/test-bucket", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestUpdateBucket_QuotaBytes_NonAdminForbidden verifies unauthenticated requests
// are rejected (consistent with panel handler pattern: 403 for non-admin including guest).
func TestUpdateBucket_QuotaBytes_NonAdminForbidden(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	// No makeTestAdmin call — guest user is not admin.

	body, _ := json.Marshal(map[string]int64{"quota_bytes": 1000000})
	req := httptest.NewRequest(http.MethodPatch, "/api/s3/buckets/test-bucket", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

// TestForceDeleteBucketsViaGC verifies that the force_delete_buckets+buckets_only GC path
// deletes explicitly named fs+ buckets immediately, bypassing orphanedBucketGracePeriod.
func TestForceDeleteBucketsViaGC(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Create a fresh fs+ bucket (simulates a young replicated share bucket on coord-2/3)
	bucketName := s3.FileShareBucketPrefix + "accordion-share"
	err := srv.s3Store.CreateBucket(t.Context(), bucketName, "owner", 2)
	require.NoError(t, err)

	// Verify it exists
	meta, err := srv.s3Store.HeadBucket(t.Context(), bucketName)
	require.NoError(t, err)
	require.NotNil(t, meta, "bucket should exist before GC")

	// POST to /api/s3/gc with force_delete_buckets + buckets_only
	body, _ := json.Marshal(map[string]interface{}{
		"force_delete_buckets": []string{bucketName},
		"buckets_only":         true,
		"no_forward":           true,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/s3/gc", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, float64(1), resp["deleted_buckets"], "should report 1 deleted bucket")

	// Verify the bucket is gone despite being just created (< 10 min old)
	_, err = srv.s3Store.HeadBucket(t.Context(), bucketName)
	assert.Error(t, err, "bucket should be deleted after force_delete_buckets GC")
}

// TestForceDeleteBuckets_NonFSPrefixIgnored verifies the safety check that only fs+ buckets
// can be force-deleted via the GC endpoint (prevents accidental deletion of regular buckets).
func TestForceDeleteBuckets_NonFSPrefixIgnored(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// POST with a non-fs+ bucket name — should be silently ignored
	body, _ := json.Marshal(map[string]interface{}{
		"force_delete_buckets": []string{"test-bucket"},
		"buckets_only":         true,
		"no_forward":           true,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/s3/gc", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	// test-bucket should still exist — non-fs+ buckets are ignored
	meta, err := srv.s3Store.HeadBucket(t.Context(), "test-bucket")
	require.NoError(t, err)
	assert.NotNil(t, meta, "non-fs+ bucket should not be deleted by force_delete_buckets")
}

// TestBucketsOnly_ForwardsToPeers verifies that BucketsOnly=true with NoForward=false (default)
// forwards the force_delete_buckets request to peer coordinators.
// This ensures that manually calling POST /api/s3/gc with explicit bucket names produces
// cluster-wide deletion rather than local-only deletion.
func TestBucketsOnly_ForwardsToPeers(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Create an fs+ bucket to delete
	bucketName := s3.FileShareBucketPrefix + "peer-forward-test"
	require.NoError(t, srv.s3Store.CreateBucket(t.Context(), bucketName, "owner", 1))

	// Track whether the mock peer receives a forwarded deletion request
	peerReceived := make(chan struct{}, 1)
	mockPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case peerReceived <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"deleted_buckets":1}`))
	}))
	defer mockPeer.Close()

	// Override shareGCClient to route mesh HTTPS calls to the mock HTTP server.
	srv.shareGCClient = &http.Client{
		Transport: &mockGCRoundTripper{targetBase: mockPeer.URL},
	}

	// Inject a minimal replicator with one fake peer.
	fakeReplicator := replication.NewReplicator(replication.Config{
		Transport: &noopReplicationTransport{},
	})
	fakeReplicator.AddPeer("10.99.99.99", "mock-coord")
	srv.replicator = fakeReplicator

	// POST /api/s3/gc with buckets_only=true — no_forward is omitted (false), so
	// the handler should forward the deletion to peer coordinators.
	body, _ := json.Marshal(map[string]interface{}{
		"force_delete_buckets": []string{bucketName},
		"buckets_only":         true,
		// no_forward intentionally omitted to test the forwarding path
	})
	req := httptest.NewRequest(http.MethodPost, "/api/s3/gc", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["deleted_buckets"])

	// Forwarding is synchronous: peer must have received the request before handler returned.
	select {
	case <-peerReceived:
		// Good: peer coordinator was notified
	default:
		t.Error("peer coordinator was not notified (BucketsOnly=true should forward to peers when NoForward=false)")
	}
}
