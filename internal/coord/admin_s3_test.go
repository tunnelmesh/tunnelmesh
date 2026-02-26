package coord

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
