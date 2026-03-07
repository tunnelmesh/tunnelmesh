package coord

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- handleS3ListObjects ---

func TestHandleS3ListObjects_EmptyBucket(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var result []S3ObjectInfo
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&result))
	assert.Empty(t, result)
}

func TestHandleS3ListObjects_WithObjects(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Upload two objects directly via admin route
	for _, key := range []string{"file1.txt", "file2.txt"} {
		body := strings.NewReader("hello")
		req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/"+key, body)
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()
		srv.adminMux.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "upload %s", key)
	}

	// List bucket
	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var result []S3ObjectInfo
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&result))
	assert.Len(t, result, 2)
}

func TestHandleS3ListObjects_PrefixFilter(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Upload objects with different prefixes
	for _, key := range []string{"a/file1.txt", "a/file2.txt", "b/file3.txt"} {
		body := strings.NewReader("data")
		req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/"+key, body)
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()
		srv.adminMux.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List with prefix=a/
	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects?prefix=a/", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var result []S3ObjectInfo
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&result))
	for _, obj := range result {
		if !obj.IsPrefix {
			assert.True(t, strings.HasPrefix(obj.Key, "a/"), "key %q should have prefix a/", obj.Key)
		}
	}
}

func TestHandleS3ListObjects_BucketNotFound(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/nonexistent-bucket/objects", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- handleS3Object (PUT) ---

func TestHandleS3Object_PUT_Basic(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	body := strings.NewReader("hello world")
	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/myfile.txt", body)
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("ETag"))
}

func TestHandleS3Object_PUT_ToReadOnlySystemBucket(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/_tunnelmesh/objects/key", strings.NewReader("data"))
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

// --- handleS3Object (GET) ---

func TestHandleS3Object_GET_ExistingObject(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Upload
	content := "test content"
	putReq := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/doc.txt", strings.NewReader(content))
	putReq.Header.Set("Content-Type", "text/plain")
	putRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code)

	// Retrieve
	getReq := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects/doc.txt", nil)
	getRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)

	body, _ := io.ReadAll(getRec.Body)
	assert.Equal(t, content, string(body))
}

func TestHandleS3Object_GET_Missing(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects/does-not-exist.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- handleS3Object (DELETE) ---

func TestHandleS3Object_DELETE_ExistingObject(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Upload first
	putReq := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/todelete.txt", strings.NewReader("bye"))
	putReq.Header.Set("Content-Type", "text/plain")
	putRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code)

	// Delete
	delReq := httptest.NewRequest(http.MethodDelete, "/api/s3/buckets/test-bucket/objects/todelete.txt", nil)
	delRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(delRec, delReq)
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Verify gone from live listing
	listReq := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects", nil)
	listRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(listRec, listReq)
	require.Equal(t, http.StatusOK, listRec.Code)

	var result []S3ObjectInfo
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&result))
	for _, obj := range result {
		assert.NotEqual(t, "todelete.txt", obj.Key)
	}
}

// --- handleS3HeadObject ---

func TestHandleS3Object_HEAD_Headers(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	content := "header test"
	putReq := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/hdr.txt", strings.NewReader(content))
	putReq.Header.Set("Content-Type", "text/plain")
	putRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code)

	headReq := httptest.NewRequest(http.MethodHead, "/api/s3/buckets/test-bucket/objects/hdr.txt", nil)
	headRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(headRec, headReq)

	assert.Equal(t, http.StatusOK, headRec.Code)
	assert.NotEmpty(t, headRec.Header().Get("ETag"), "ETag header should be present")
	assert.NotEmpty(t, headRec.Header().Get("Content-Length"), "Content-Length header should be present")
}

// --- handleS3RestoreVersion ---

func TestHandleRestoreVersion_MissingVersionID(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// POST without version_id
	body, _ := json.Marshal(map[string]string{})
	req := httptest.NewRequest(http.MethodPost, "/api/s3/buckets/test-bucket/objects/key.txt/restore", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TunnelMesh-Forwarded", "true") // prevent forwarding in test
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleRestoreVersion_NonExistentObject(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	body, _ := json.Marshal(map[string]string{"version_id": "doesnotexist"})
	req := httptest.NewRequest(http.MethodPost, "/api/s3/buckets/test-bucket/objects/nokey.txt/restore", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TunnelMesh-Forwarded", "true") // prevent forwarding in test
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- handleS3UndeleteObject ---

func TestHandleS3Undelete_DeleteThenRestore(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Upload
	putReq := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/restore-me.txt", strings.NewReader("data"))
	putReq.Header.Set("Content-Type", "text/plain")
	putRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code)

	// Delete (moves to recycle bin)
	delReq := httptest.NewRequest(http.MethodDelete, "/api/s3/buckets/test-bucket/objects/restore-me.txt", nil)
	delRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(delRec, delReq)
	require.Equal(t, http.StatusNoContent, delRec.Code)

	// Undelete
	undoReq := httptest.NewRequest(http.MethodPost, "/api/s3/buckets/test-bucket/objects/restore-me.txt/undelete", nil)
	undoReq.Header.Set("X-TunnelMesh-Forwarded", "true") // prevent forwarding in test
	undoRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(undoRec, undoReq)
	require.Equal(t, http.StatusOK, undoRec.Code)

	// Verify back in live listing
	listReq := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects", nil)
	listRec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(listRec, listReq)
	require.Equal(t, http.StatusOK, listRec.Code)

	var result []S3ObjectInfo
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&result))
	found := false
	for _, obj := range result {
		if obj.Key == "restore-me.txt" {
			found = true
			break
		}
	}
	assert.True(t, found, "undeleted object should appear in live listing")
}

// --- Body size limit enforcement ---

func TestHandleS3Object_PUT_BodySizeLimit(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Attempt an upload exceeding maxObjectSize (default 1 GiB). We can't actually
	// send 1 GiB in a test, so instead we override maxObjectSize to a tiny value.
	srv.maxObjectSize = 10 // 10 bytes

	body := strings.NewReader("this is more than ten bytes")
	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/big.txt", body)
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}
