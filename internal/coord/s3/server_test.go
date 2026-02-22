package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAuthorizer is a test authorizer that allows all requests.
type mockAuthorizer struct {
	userID          string
	allowAll        bool
	denyAll         bool
	allowVerb       map[string]bool
	allowedPrefixes map[string][]string // bucket -> prefixes (nil = unrestricted)
}

func (m *mockAuthorizer) AuthorizeRequest(r *http.Request, verb, resource, bucket, objectKey string) (string, error) {
	if m.denyAll {
		return "", ErrAccessDenied
	}
	if m.allowAll {
		return m.userID, nil
	}
	if m.allowVerb != nil && m.allowVerb[verb] {
		return m.userID, nil
	}
	return "", ErrAccessDenied
}

func (m *mockAuthorizer) GetAllowedPrefixes(userID, bucket string) []string {
	if m.allowedPrefixes == nil {
		return nil // unrestricted
	}
	prefixes, ok := m.allowedPrefixes[bucket]
	if !ok {
		return nil // unrestricted if bucket not configured
	}
	return prefixes
}

func newTestServer(t *testing.T) (*Server, *Store) {
	t.Helper()
	// Use CAS-enabled store for server tests (required for PutObject)
	masterKey := [32]byte{1, 2, 3, 4, 5, 6, 7, 8}
	store, err := NewStoreWithCAS(t.TempDir(), nil, masterKey)
	require.NoError(t, err)
	auth := &mockAuthorizer{userID: "alice", allowAll: true}
	server := NewServer(store, auth, nil) // nil metrics for tests
	return server, store
}

// newTestStoreWithCASForServer creates a CAS-enabled store for server tests
// that need to use a custom authorizer.
func newTestStoreWithCASForServer(t *testing.T) *Store {
	t.Helper()
	masterKey := [32]byte{1, 2, 3, 4, 5, 6, 7, 8}
	store, err := NewStoreWithCAS(t.TempDir(), nil, masterKey)
	require.NoError(t, err)
	return store
}

func TestListBuckets(t *testing.T) {
	server, store := newTestServer(t)

	// Create some buckets
	require.NoError(t, store.CreateBucket(context.Background(), "bucket-a", "alice", 2, nil))
	require.NoError(t, store.CreateBucket(context.Background(), "bucket-b", "bob", 2, nil))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

	var resp ListAllMyBucketsResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))

	assert.Len(t, resp.Buckets.Bucket, 2)
}

func TestListBucketsEmpty(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ListAllMyBucketsResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Empty(t, resp.Buckets.Bucket)
}

func TestCreateBucket(t *testing.T) {
	server, store := newTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/my-bucket", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify bucket exists
	meta, err := store.HeadBucket(context.Background(), "my-bucket")
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", meta.Name)
}

func TestCreateBucketAlreadyExists(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	req := httptest.NewRequest(http.MethodPut, "/my-bucket", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)

	var resp ErrorResponse
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "BucketAlreadyExists", resp.Code)
}

func TestDeleteBucket(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	req := httptest.NewRequest(http.MethodDelete, "/my-bucket", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)

	// Verify bucket is gone
	_, err := store.HeadBucket(context.Background(), "my-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestDeleteBucketNotFound(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/nonexistent", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp ErrorResponse
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "NoSuchBucket", resp.Code)
}

func TestDeleteBucketNotEmpty(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))
	_, err := store.PutObject(context.Background(), "my-bucket", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodDelete, "/my-bucket", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)

	var resp ErrorResponse
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "BucketNotEmpty", resp.Code)
}

func TestHeadBucket(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	req := httptest.NewRequest(http.MethodHead, "/my-bucket", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHeadBucketNotFound(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodHead, "/nonexistent", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestPutObject(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	content := []byte("hello world")
	req := httptest.NewRequest(http.MethodPut, "/my-bucket/greeting.txt", bytes.NewReader(content))
	req.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, w.Header().Get("ETag"))

	// Verify object exists
	meta, err := store.HeadObject(context.Background(), "my-bucket", "greeting.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), meta.Size)
}

func TestPutObjectBucketNotFound(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/nonexistent/file.txt", bytes.NewReader([]byte("data")))
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp ErrorResponse
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "NoSuchBucket", resp.Code)
}

func TestPutObjectWithMetadata(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	req := httptest.NewRequest(http.MethodPut, "/my-bucket/doc.txt", bytes.NewReader([]byte("data")))
	req.Header.Set("X-Amz-Meta-Author", "alice")
	req.Header.Set("X-Amz-Meta-Version", "1.0")
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	meta, err := store.HeadObject(context.Background(), "my-bucket", "doc.txt")
	require.NoError(t, err)
	assert.Equal(t, "alice", meta.Metadata["X-Amz-Meta-Author"])
}

func TestGetObject(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))
	content := []byte("hello world")
	_, err := store.PutObject(context.Background(), "my-bucket", "greeting.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/my-bucket/greeting.txt", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/plain", w.Header().Get("Content-Type"))
	assert.NotEmpty(t, w.Header().Get("ETag"))
	assert.NotEmpty(t, w.Header().Get("Last-Modified"))
	assert.Equal(t, content, w.Body.Bytes())
}

func TestGetObjectNotFound(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	req := httptest.NewRequest(http.MethodGet, "/my-bucket/nonexistent.txt", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp ErrorResponse
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "NoSuchKey", resp.Code)
}

func TestDeleteObject(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))
	_, err := store.PutObject(context.Background(), "my-bucket", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodDelete, "/my-bucket/file.txt", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)

	// Verify object is gone from live path (moved to recycle bin)
	_, err = store.HeadObject(context.Background(), "my-bucket", "file.txt")
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestDeleteObjectNotFound(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	// S3 returns 204 even for non-existent objects
	req := httptest.NewRequest(http.MethodDelete, "/my-bucket/nonexistent.txt", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestHeadObject(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))
	content := []byte("hello world")
	_, err := store.PutObject(context.Background(), "my-bucket", "greeting.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodHead, "/my-bucket/greeting.txt", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/plain", w.Header().Get("Content-Type"))
	assert.Equal(t, "11", w.Header().Get("Content-Length"))
	assert.NotEmpty(t, w.Header().Get("ETag"))
	assert.Empty(t, w.Body.Bytes()) // HEAD has no body
}

func TestHeadObjectNotFound(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	req := httptest.NewRequest(http.MethodHead, "/my-bucket/nonexistent.txt", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// listObjectsTestCase represents a test case for list objects operations
type listObjectsTestCase struct {
	name           string
	objectKeys     []string
	requestURL     string
	expectedCount  int
	expectedPrefix string
	expectedName   string
}

// runListObjectsTest executes a list objects test case
func runListObjectsTest(t *testing.T, tc listObjectsTestCase) {
	t.Helper()
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	// Add test objects
	for _, key := range tc.objectKeys {
		_, err := store.PutObject(context.Background(), "my-bucket", key, bytes.NewReader([]byte("data")), 4, "text/plain", nil)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodGet, tc.requestURL, nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ListBucketResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, tc.expectedName, resp.Name)
	assert.Equal(t, tc.expectedPrefix, resp.Prefix)
	assert.Len(t, resp.Contents, tc.expectedCount)
}

func TestListObjects(t *testing.T) {
	runListObjectsTest(t, listObjectsTestCase{
		name:          "list all objects",
		objectKeys:    []string{"a.txt", "b.txt", "c.txt"},
		requestURL:    "/my-bucket",
		expectedCount: 3,
		expectedName:  "my-bucket",
	})
}

func TestListObjectsWithPrefix(t *testing.T) {
	runListObjectsTest(t, listObjectsTestCase{
		name:           "list objects with prefix",
		objectKeys:     []string{"docs/a.txt", "docs/b.txt", "images/c.png"},
		requestURL:     "/my-bucket?prefix=docs/",
		expectedCount:  2,
		expectedPrefix: "docs/",
		expectedName:   "my-bucket",
	})
}

func TestListObjectsV2(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	// Add some objects
	for _, key := range []string{"a.txt", "b.txt"} {
		_, err := store.PutObject(context.Background(), "my-bucket", key, bytes.NewReader([]byte("data")), 4, "text/plain", nil)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodGet, "/my-bucket?list-type=2", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ListBucketResultV2
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "my-bucket", resp.Name)
	assert.Equal(t, 2, resp.KeyCount)
	assert.Len(t, resp.Contents, 2)
}

func TestListObjectsBucketNotFound(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp ErrorResponse
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "NoSuchBucket", resp.Code)
}

func TestAccessDenied(t *testing.T) {
	store := newTestStoreWithCASForServer(t)
	auth := &mockAuthorizer{denyAll: true}
	server := NewServer(store, auth, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)

	var resp ErrorResponse
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "AccessDenied", resp.Code)
}

func TestNestedObjectKey(t *testing.T) {
	server, store := newTestServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	content := []byte("nested content")
	req := httptest.NewRequest(http.MethodPut, "/my-bucket/path/to/deep/file.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Retrieve it
	req = httptest.NewRequest(http.MethodGet, "/my-bucket/path/to/deep/file.txt", nil)
	w = httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	data, _ := io.ReadAll(w.Body)
	assert.Equal(t, content, data)
}

func TestListObjects_PrefixFiltered(t *testing.T) {
	store := newTestStoreWithCASForServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	// Create objects in different prefixes
	_, _ = store.PutObject(context.Background(), "my-bucket", "teamA/doc1.txt", bytes.NewReader([]byte("a1")), 2, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "my-bucket", "teamA/doc2.txt", bytes.NewReader([]byte("a2")), 2, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "my-bucket", "teamB/doc1.txt", bytes.NewReader([]byte("b1")), 2, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "my-bucket", "teamC/doc1.txt", bytes.NewReader([]byte("c1")), 2, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "my-bucket", "root.txt", bytes.NewReader([]byte("root")), 4, "text/plain", nil)

	// Create authorizer that only allows teamA/ prefix
	auth := &mockAuthorizer{
		userID:   "alice",
		allowAll: true,
		allowedPrefixes: map[string][]string{
			"my-bucket": {"teamA/"},
		},
	}
	server := NewServer(store, auth, nil)

	req := httptest.NewRequest(http.MethodGet, "/my-bucket", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ListBucketResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))

	// Should only see teamA/* objects
	assert.Len(t, resp.Contents, 2)
	keys := []string{resp.Contents[0].Key, resp.Contents[1].Key}
	assert.Contains(t, keys, "teamA/doc1.txt")
	assert.Contains(t, keys, "teamA/doc2.txt")
}

func TestListObjects_MultiplePrefixesFiltered(t *testing.T) {
	store := newTestStoreWithCASForServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	_, _ = store.PutObject(context.Background(), "my-bucket", "teamA/doc.txt", bytes.NewReader([]byte("a")), 1, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "my-bucket", "teamB/doc.txt", bytes.NewReader([]byte("b")), 1, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "my-bucket", "teamC/doc.txt", bytes.NewReader([]byte("c")), 1, "text/plain", nil)

	// User can access both teamA/ and teamB/
	auth := &mockAuthorizer{
		userID:   "alice",
		allowAll: true,
		allowedPrefixes: map[string][]string{
			"my-bucket": {"teamA/", "teamB/"},
		},
	}
	server := NewServer(store, auth, nil)

	req := httptest.NewRequest(http.MethodGet, "/my-bucket", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ListBucketResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))

	assert.Len(t, resp.Contents, 2)
	keys := []string{resp.Contents[0].Key, resp.Contents[1].Key}
	assert.Contains(t, keys, "teamA/doc.txt")
	assert.Contains(t, keys, "teamB/doc.txt")
}

func TestListObjects_UnrestrictedAccess(t *testing.T) {
	store := newTestStoreWithCASForServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	_, _ = store.PutObject(context.Background(), "my-bucket", "teamA/doc.txt", bytes.NewReader([]byte("a")), 1, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "my-bucket", "teamB/doc.txt", bytes.NewReader([]byte("b")), 1, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "my-bucket", "root.txt", bytes.NewReader([]byte("r")), 1, "text/plain", nil)

	// nil prefixes = unrestricted
	auth := &mockAuthorizer{
		userID:          "alice",
		allowAll:        true,
		allowedPrefixes: nil,
	}
	server := NewServer(store, auth, nil)

	req := httptest.NewRequest(http.MethodGet, "/my-bucket", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ListBucketResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))

	// Should see all objects
	assert.Len(t, resp.Contents, 3)
}

func TestListObjectsV2_PrefixFiltered(t *testing.T) {
	store := newTestStoreWithCASForServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	_, _ = store.PutObject(context.Background(), "my-bucket", "projects/teamA/doc.txt", bytes.NewReader([]byte("a")), 1, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "my-bucket", "projects/teamB/doc.txt", bytes.NewReader([]byte("b")), 1, "text/plain", nil)

	auth := &mockAuthorizer{
		userID:   "alice",
		allowAll: true,
		allowedPrefixes: map[string][]string{
			"my-bucket": {"projects/teamA/"},
		},
	}
	server := NewServer(store, auth, nil)

	req := httptest.NewRequest(http.MethodGet, "/my-bucket?list-type=2", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ListBucketResultV2
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))

	assert.Len(t, resp.Contents, 1)
	assert.Equal(t, "projects/teamA/doc.txt", resp.Contents[0].Key)
	assert.Equal(t, 1, resp.KeyCount)
}

func TestListObjects_EmptyPrefixesNoAccess(t *testing.T) {
	store := newTestStoreWithCASForServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))

	_, _ = store.PutObject(context.Background(), "my-bucket", "doc.txt", bytes.NewReader([]byte("a")), 1, "text/plain", nil)

	// Empty slice = no access
	auth := &mockAuthorizer{
		userID:   "alice",
		allowAll: true,
		allowedPrefixes: map[string][]string{
			"my-bucket": {},
		},
	}
	server := NewServer(store, auth, nil)

	req := httptest.NewRequest(http.MethodGet, "/my-bucket", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ListBucketResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &resp))

	// No objects visible
	assert.Len(t, resp.Contents, 0)
}

// newTestMetrics creates S3Metrics backed by a fresh Prometheus registry for testing.
func newTestMetrics(t *testing.T) (*S3Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := &S3Metrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tunnelmesh_s3_requests_total",
			Help: "Test",
		}, []string{"operation", "status"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "tunnelmesh_s3_request_duration_seconds",
			Help: "Test",
		}, []string{"operation"}),
		BytesUploaded:   prometheus.NewCounter(prometheus.CounterOpts{Name: "tunnelmesh_s3_bytes_uploaded_total", Help: "Test"}),
		BytesDownloaded: prometheus.NewCounter(prometheus.CounterOpts{Name: "tunnelmesh_s3_bytes_downloaded_total", Help: "Test"}),
	}
	reg.MustRegister(m.RequestsTotal, m.RequestDuration, m.BytesUploaded, m.BytesDownloaded)
	return m, reg
}

// getMetricCount finds a specific counter value from gathered metrics.
func getMetricCount(families []*dto.MetricFamily, name, operation, status string) float64 {
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			labels := make(map[string]string)
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["operation"] == operation && labels["status"] == status {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestServerMetrics(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*Store)
		method     string
		path       string
		body       []byte
		wantOp     string
		wantStatus string
	}{
		{
			name:       "ListBuckets success",
			method:     http.MethodGet,
			path:       "/",
			wantOp:     "ListBuckets",
			wantStatus: "success",
		},
		{
			name:       "CreateBucket success",
			method:     http.MethodPut,
			path:       "/new-bucket",
			wantOp:     "CreateBucket",
			wantStatus: "success",
		},
		{
			name: "CreateBucket conflict",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "exists", "alice", 2, nil)
			},
			method:     http.MethodPut,
			path:       "/exists",
			wantOp:     "CreateBucket",
			wantStatus: "error",
		},
		{
			name: "HeadBucket success",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "b", "alice", 2, nil)
			},
			method:     http.MethodHead,
			path:       "/b",
			wantOp:     "HeadBucket",
			wantStatus: "success",
		},
		{
			name:       "HeadBucket not found",
			method:     http.MethodHead,
			path:       "/nonexistent",
			wantOp:     "HeadBucket",
			wantStatus: "not_found",
		},
		{
			name: "DeleteBucket success",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "del", "alice", 2, nil)
			},
			method:     http.MethodDelete,
			path:       "/del",
			wantOp:     "DeleteBucket",
			wantStatus: "success",
		},
		{
			name:       "DeleteBucket not found",
			method:     http.MethodDelete,
			path:       "/nonexistent",
			wantOp:     "DeleteBucket",
			wantStatus: "not_found",
		},
		{
			name: "ListObjects success",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "lb", "alice", 2, nil)
			},
			method:     http.MethodGet,
			path:       "/lb",
			wantOp:     "ListObjects",
			wantStatus: "success",
		},
		{
			name:       "ListObjects bucket not found",
			method:     http.MethodGet,
			path:       "/nonexistent",
			wantOp:     "ListObjects",
			wantStatus: "not_found",
		},
		{
			name: "ListObjectsV2 success",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "lv2", "alice", 2, nil)
			},
			method:     http.MethodGet,
			path:       "/lv2?list-type=2",
			wantOp:     "ListObjectsV2",
			wantStatus: "success",
		},
		{
			name: "PutObject success",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "put", "alice", 2, nil)
			},
			method:     http.MethodPut,
			path:       "/put/file.txt",
			body:       []byte("hello"),
			wantOp:     "PutObject",
			wantStatus: "success",
		},
		{
			name:       "PutObject bucket not found",
			method:     http.MethodPut,
			path:       "/nonexistent/file.txt",
			body:       []byte("hello"),
			wantOp:     "PutObject",
			wantStatus: "not_found",
		},
		{
			name: "GetObject success",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "get", "alice", 2, nil)
				_, _ = s.PutObject(context.Background(), "get", "f.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
			},
			method:     http.MethodGet,
			path:       "/get/f.txt",
			wantOp:     "GetObject",
			wantStatus: "success",
		},
		{
			name: "GetObject not found",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "get2", "alice", 2, nil)
			},
			method:     http.MethodGet,
			path:       "/get2/missing.txt",
			wantOp:     "GetObject",
			wantStatus: "not_found",
		},
		{
			name: "DeleteObject success",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "do", "alice", 2, nil)
				_, _ = s.PutObject(context.Background(), "do", "f.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
			},
			method:     http.MethodDelete,
			path:       "/do/f.txt",
			wantOp:     "DeleteObject",
			wantStatus: "success",
		},
		{
			name: "HeadObject success",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "ho", "alice", 2, nil)
				_, _ = s.PutObject(context.Background(), "ho", "f.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
			},
			method:     http.MethodHead,
			path:       "/ho/f.txt",
			wantOp:     "HeadObject",
			wantStatus: "success",
		},
		{
			name: "HeadObject not found",
			setup: func(s *Store) {
				_ = s.CreateBucket(context.Background(), "ho2", "alice", 2, nil)
			},
			method:     http.MethodHead,
			path:       "/ho2/missing.txt",
			wantOp:     "HeadObject",
			wantStatus: "not_found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics, reg := newTestMetrics(t)
			store := newTestStoreWithCASForServer(t)
			if tt.setup != nil {
				tt.setup(store)
			}
			auth := &mockAuthorizer{userID: "alice", allowAll: true}
			server := NewServer(store, auth, metrics)

			var body io.Reader
			if tt.body != nil {
				body = bytes.NewReader(tt.body)
			}
			req := httptest.NewRequest(tt.method, tt.path, body)
			w := httptest.NewRecorder()

			server.Handler().ServeHTTP(w, req)

			families, err := reg.Gather()
			require.NoError(t, err)

			count := getMetricCount(families, "tunnelmesh_s3_requests_total", tt.wantOp, tt.wantStatus)
			assert.Equal(t, 1.0, count,
				"expected tunnelmesh_s3_requests_total{operation=%q, status=%q} = 1", tt.wantOp, tt.wantStatus)
		})
	}
}

func TestMetricsOnCorrectRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics, _ := newTestMetrics(t)
	reg.MustRegister(metrics.RequestsTotal)

	metrics.RecordRequest("GetObject", "success", 0.1)

	families, err := reg.Gather()
	require.NoError(t, err)

	found := false
	for _, f := range families {
		if f.GetName() == "tunnelmesh_s3_requests_total" {
			found = true
		}
	}
	assert.True(t, found, "S3 request metrics should be on the custom registry")
}

// mockDeleteNotifier records calls to NotifyObjectDeleted.
type mockDeleteNotifier struct {
	calls []struct{ bucket, key string }
}

func (m *mockDeleteNotifier) NotifyObjectDeleted(bucket, key string) {
	m.calls = append(m.calls, struct{ bucket, key string }{bucket, key})
}

func TestDeleteObject_NotifiesDeleteNotifier(t *testing.T) {
	server, store := newTestServer(t)
	notifier := &mockDeleteNotifier{}
	server.SetDeleteNotifier(notifier)

	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))
	_, err := store.PutObject(context.Background(), "my-bucket", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodDelete, "/my-bucket/file.txt", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	require.Len(t, notifier.calls, 1)
	assert.Equal(t, "my-bucket", notifier.calls[0].bucket)
	assert.Equal(t, "file.txt", notifier.calls[0].key)
}

func TestDeleteObject_WorksWithoutNotifier(t *testing.T) {
	server, store := newTestServer(t)
	// No notifier set — nil safety check

	require.NoError(t, store.CreateBucket(context.Background(), "my-bucket", "alice", 2, nil))
	_, err := store.PutObject(context.Background(), "my-bucket", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodDelete, "/my-bucket/file.txt", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestDeleteObject_NoNotifyOnError(t *testing.T) {
	server, _ := newTestServer(t)
	notifier := &mockDeleteNotifier{}
	server.SetDeleteNotifier(notifier)

	// Delete from non-existent bucket
	req := httptest.NewRequest(http.MethodDelete, "/nonexistent/file.txt", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Empty(t, notifier.calls, "should not notify on error")
}

func TestSetMetricsWiresIntoServer(t *testing.T) {
	store := newTestStoreWithCASForServer(t)
	require.NoError(t, store.CreateBucket(context.Background(), "b", "alice", 2, nil))
	auth := &mockAuthorizer{userID: "alice", allowAll: true}

	// Create server without metrics
	server := NewServer(store, auth, nil)

	// Make a request — should work without metrics (no panic)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Now wire in metrics via SetMetrics
	metrics, reg := newTestMetrics(t)
	server.SetMetrics(metrics)

	// Make another request — should now record metrics
	req = httptest.NewRequest(http.MethodHead, "/b", nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	families, err := reg.Gather()
	require.NoError(t, err)

	count := getMetricCount(families, "tunnelmesh_s3_requests_total", "HeadBucket", "success")
	assert.Equal(t, 1.0, count, "HeadBucket metric should be recorded after SetMetrics")
}
