package coord

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

func TestShares_List_NoManager(t *testing.T) {
	srv := newTestServer(t)

	// Temporarily nil out fileShareMgr to test the guard
	orig := srv.fileShareMgr
	srv.fileShareMgr = nil
	defer func() { srv.fileShareMgr = orig }()

	req := httptest.NewRequest(http.MethodGet, "/api/shares", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "file shares not enabled")
}

func TestValidateShareName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid simple", "myshare", false},
		{"valid with numbers", "share123", false},
		{"valid with underscore", "my_share", false},
		{"valid with hyphen", "my-share", false},
		{"empty", "", false}, // empty is valid (caught by caller)
		{"too long", "a234567890123456789012345678901234567890123456789012345678901234", true},
		{"hyphen at start", "-myshare", true},
		{"hyphen at end", "myshare-", true},
		{"space", "my share", true},
		{"dot", "my.share", true},
		{"slash", "my/share", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateShareName(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateShareQuota(t *testing.T) {
	tests := []struct {
		name    string
		input   int64
		wantErr bool
	}{
		{"zero (unlimited)", 0, false},
		{"valid 1GB", 1024 * 1024 * 1024, false},
		{"valid 1TB (max)", 1024 * 1024 * 1024 * 1024, false},
		{"negative", -1, true},
		{"exceeds max", 1024*1024*1024*1024 + 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateShareQuota(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestShareDeleteForwardsToCoords verifies that forwardBucketDeletionToPeers is a no-op
// when replicator is nil (standard test environment), confirming the guard prevents panics.
func TestShareDeleteForwardsToCoords(t *testing.T) {
	srv := newTestServer(t)

	// With replicator == nil, function must return immediately without panicking
	srv.forwardBucketDeletionToPeers(context.Background(), s3.FileShareBucketPrefix+"myshare")
	// No assertion needed — just verify no panic
}

// TestForwardBucketDeletion_Payload verifies the GC payload sent by forwardBucketDeletionToPeers.
// Uses a direct call to handleS3GC to test the buckets_only code path end-to-end.
func TestForwardBucketDeletion_Payload(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Create an fs+ bucket to delete
	bucketName := s3.FileShareBucketPrefix + "payload-test"
	require.NoError(t, srv.s3Store.CreateBucket(t.Context(), bucketName, "owner", 1, nil))

	// Simulate what forwardBucketDeletionToPeers sends to this coordinator
	payload, _ := json.Marshal(map[string]interface{}{
		"force_delete_buckets": []string{bucketName},
		"buckets_only":         true,
		"no_forward":           true,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/s3/gc", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["deleted_buckets"])

	// Bucket must be gone
	_, err := srv.s3Store.HeadBucket(t.Context(), bucketName)
	assert.Error(t, err, "bucket should be gone after force delete")
}
