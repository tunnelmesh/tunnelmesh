package coord

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
	"github.com/tunnelmesh/tunnelmesh/pkg/bytesize"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
)

func TestAdminOverview_IncludesLocation(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.Locations = true // Enable location tracking for this test

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })
	require.NotNil(t, srv.adminMux, "adminMux should be created when JoinMesh is configured")

	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Register a peer with location
	client := NewClient(ts.URL, "test-token")
	location := &proto.GeoLocation{
		Latitude:  51.5074,
		Longitude: -0.1278,
		Accuracy:  100,
		Source:    "manual",
		City:      "London",
		Country:   "United Kingdom",
	}
	_, err = client.Register("geonode", "SHA256:abc123", []string{"1.2.3.4"}, nil, 2222, 0, false, "v1.0.0", location, "", false, nil, false, false)
	require.NoError(t, err)

	// Fetch admin overview from adminMux (internal mesh only)
	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var overview AdminOverview
	err = json.NewDecoder(rec.Body).Decode(&overview)
	require.NoError(t, err)

	// Verify location is included in peer info
	require.Len(t, overview.Peers, 1)
	require.NotNil(t, overview.Peers[0].Location)
	assert.Equal(t, 51.5074, overview.Peers[0].Location.Latitude)
	assert.Equal(t, -0.1278, overview.Peers[0].Location.Longitude)
	assert.Equal(t, "manual", overview.Peers[0].Location.Source)
	assert.Equal(t, "London", overview.Peers[0].Location.City)
	assert.Equal(t, "United Kingdom", overview.Peers[0].Location.Country)
}

func TestAdminOverview_ExitPeerInfo(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })
	require.NotNil(t, srv.adminMux, "adminMux should be created for coordinators")

	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Register an exit node
	client := NewClient(ts.URL, "test-token")
	_, err = client.Register("exit-node", "SHA256:exitkey", []string{"1.2.3.4"}, nil, 2222, 0, false, "v1.0.0", nil, "", true, nil, false, false)
	require.NoError(t, err)

	// Register a client that uses the exit node
	_, err = client.Register("client1", "SHA256:client1key", []string{"5.6.7.8"}, nil, 2223, 0, false, "v1.0.0", nil, "exit-node", false, nil, false, false)
	require.NoError(t, err)

	// Fetch admin overview from adminMux (internal mesh only)
	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var overview AdminOverview
	err = json.NewDecoder(rec.Body).Decode(&overview)
	require.NoError(t, err)

	// Find peers by name
	var exitNodeInfo, clientInfo *AdminPeerInfo
	for i := range overview.Peers {
		if overview.Peers[i].Name == "exit-node" {
			exitNodeInfo = &overview.Peers[i]
		}
		if overview.Peers[i].Name == "client1" {
			clientInfo = &overview.Peers[i]
		}
	}

	require.NotNil(t, exitNodeInfo, "exit-node should be in peers")
	require.NotNil(t, clientInfo, "client1 should be in peers")

	// Verify exit node info
	assert.True(t, exitNodeInfo.AllowsExitTraffic, "exit-node should allow exit traffic")
	assert.Equal(t, "", exitNodeInfo.ExitPeer, "exit-node should not have an exit node")
	assert.Contains(t, exitNodeInfo.ExitClients, "client1", "exit-node should have client1 as exit client")

	// Verify client info
	assert.False(t, clientInfo.AllowsExitTraffic, "client1 should not allow exit traffic")
	assert.Equal(t, "exit-node", clientInfo.ExitPeer, "client1 should use exit-node")
	assert.Empty(t, clientInfo.ExitClients, "client1 should not have exit clients")
}

func TestAdminOverview_ConnectionTypes(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })
	require.NotNil(t, srv.adminMux, "adminMux should be created when JoinMesh is configured")

	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Register a peer
	client := NewClient(ts.URL, "test-token")
	_, err = client.Register("peer1", "SHA256:peer1key", []string{"1.2.3.4"}, nil, 2222, 0, false, "v1.0.0", nil, "", false, nil, false, false)
	require.NoError(t, err)

	// Directly set stats on the server (simulating heartbeat)
	// This is necessary because heartbeats are sent via WebSocket in production
	srv.peersMu.Lock()
	if peerInfo, ok := srv.peers["peer1"]; ok {
		peerInfo.stats = &proto.PeerStats{
			PacketsSent: 100,
			Connections: map[string]string{
				"peer2": "ssh",
				"peer3": "udp",
			},
		}
	}
	srv.peersMu.Unlock()

	// Fetch admin overview from adminMux (internal mesh only)
	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var overview AdminOverview
	err = json.NewDecoder(rec.Body).Decode(&overview)
	require.NoError(t, err)

	// Find peer1
	var peer1Info *AdminPeerInfo
	for i := range overview.Peers {
		if overview.Peers[i].Name == "peer1" {
			peer1Info = &overview.Peers[i]
			break
		}
	}

	require.NotNil(t, peer1Info, "peer1 should be in peers")
	require.NotNil(t, peer1Info.Connections, "peer1 should have connections")
	assert.Equal(t, "ssh", peer1Info.Connections["peer2"])
	assert.Equal(t, "udp", peer1Info.Connections["peer3"])
}

func TestSetupMonitoringProxies_AdminMux(t *testing.T) {
	// Start mock Prometheus and Grafana servers before creating the server,
	// since SetupMonitoringProxies is called unconditionally in setupRoutes()
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("prometheus"))
	}))
	defer promServer.Close()

	grafanaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("grafana"))
	}))
	defer grafanaServer.Close()

	// Create a server with monitoring URLs pre-configured so setupRoutes() registers them
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.Monitoring.PrometheusURL = promServer.URL
	cfg.Coordinator.Monitoring.GrafanaURL = grafanaServer.URL

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })
	require.NotNil(t, srv.adminMux, "adminMux should be created for coordinators")

	// Test adminMux has the prometheus route (admin is mesh-internal only)
	req := httptest.NewRequest(http.MethodGet, "/prometheus/", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "prometheus", rec.Body.String())

	// Test adminMux has the grafana route
	req = httptest.NewRequest(http.MethodGet, "/grafana/", nil)
	rec = httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "grafana", rec.Body.String())

	// Verify main mux does NOT have the monitoring routes (admin is mesh-internal only)
	req = httptest.NewRequest(http.MethodGet, "/prometheus/", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/grafana/", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- S3 Explorer API Tests ---

func newTestServerWithS3AndBucket(t *testing.T) *Server {
	t.Helper()
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.DataDir = t.TempDir()

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Create a test bucket
	err = srv.s3Store.CreateBucket(context.Background(), "test-bucket", "admin", 2, nil)
	require.NoError(t, err)

	return srv
}

func TestS3Proxy_ListBuckets(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp S3BucketsResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	// Server creates system bucket automatically, plus our test bucket
	require.Len(t, resp.Buckets, 2)

	// Verify quota info is present
	assert.Greater(t, resp.Quota.MaxBytes, int64(0))

	// Find test bucket and verify it's writable
	var testBucket *S3BucketInfo
	for i := range resp.Buckets {
		if resp.Buckets[i].Name == "test-bucket" {
			testBucket = &resp.Buckets[i]
			break
		}
	}
	require.NotNil(t, testBucket)
	assert.True(t, testBucket.Writable)

	// Verify dedup storage info is present
	require.NotNil(t, resp.Storage)
	assert.GreaterOrEqual(t, resp.Storage.PhysicalBytes, int64(0))
	assert.GreaterOrEqual(t, resp.Storage.LogicalBytes, int64(0))
	assert.GreaterOrEqual(t, resp.Storage.DedupRatio, 1.0)
}

func TestS3Proxy_ListBuckets_SystemBucketReadOnly(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// System bucket is created automatically by the server
	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp S3BucketsResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)

	// Find system bucket and verify it's read-only
	var systemBucket *S3BucketInfo
	for i := range resp.Buckets {
		if resp.Buckets[i].Name == auth.SystemBucket {
			systemBucket = &resp.Buckets[i]
			break
		}
	}
	require.NotNil(t, systemBucket)
	assert.False(t, systemBucket.Writable)
}

func TestS3Proxy_ListObjects(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Add some objects
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "file1.txt", bytes.NewReader([]byte("hello")), 5, "text/plain", nil)
	require.NoError(t, err)
	_, err = srv.s3Store.PutObject(context.Background(), "test-bucket", "folder/file2.txt", bytes.NewReader([]byte("world")), 5, "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var objects []S3ObjectInfo
	err = json.NewDecoder(rec.Body).Decode(&objects)
	require.NoError(t, err)
	assert.Len(t, objects, 2)
}

func TestS3Proxy_ListObjects_WithDelimiter(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Add objects in a folder structure
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "file1.txt", bytes.NewReader([]byte("hello")), 5, "text/plain", nil)
	require.NoError(t, err)
	_, err = srv.s3Store.PutObject(context.Background(), "test-bucket", "folder/file2.txt", bytes.NewReader([]byte("world")), 5, "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects?delimiter=/", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var objects []S3ObjectInfo
	err = json.NewDecoder(rec.Body).Decode(&objects)
	require.NoError(t, err)

	// Should have file1.txt and folder/ prefix
	assert.Len(t, objects, 2)

	// Check for prefix
	var hasPrefix bool
	for _, obj := range objects {
		if obj.IsPrefix && obj.Key == "folder/" {
			hasPrefix = true
		}
	}
	assert.True(t, hasPrefix, "should have folder/ prefix")
}

func TestMergeObjectListings(t *testing.T) {
	local := []S3ObjectInfo{
		{Key: "a.txt", Size: 10, LastModified: "2024-01-01T00:00:00Z"},
		{Key: "b.txt", Size: 20, LastModified: "2024-01-01T00:00:00Z"},
		{Key: "folder/", Size: 100, IsPrefix: true},
	}
	remote := []S3ObjectInfo{
		{Key: "b.txt", Size: 25, LastModified: "2024-02-01T00:00:00Z"}, // newer duplicate
		{Key: "c.txt", Size: 30, LastModified: "2024-01-01T00:00:00Z"}, // new
		{Key: "folder/", Size: 50, IsPrefix: true},                     // folder duplicate
	}

	merged := mergeObjectListings(local, remote)

	// Should have 4 entries: a.txt, b.txt (updated), folder/ (summed), c.txt
	assert.Len(t, merged, 4)

	byKey := make(map[string]S3ObjectInfo)
	for _, obj := range merged {
		byKey[obj.Key] = obj
	}

	// a.txt unchanged
	assert.Equal(t, int64(10), byKey["a.txt"].Size)

	// b.txt should have remote's newer version
	assert.Equal(t, int64(25), byKey["b.txt"].Size)
	assert.Equal(t, "2024-02-01T00:00:00Z", byKey["b.txt"].LastModified)

	// c.txt from remote
	assert.Equal(t, int64(30), byKey["c.txt"].Size)

	// folder/ sizes summed
	assert.Equal(t, int64(150), byKey["folder/"].Size)
	assert.True(t, byKey["folder/"].IsPrefix)
}

func TestMergeObjectListings_EmptyRemote(t *testing.T) {
	local := []S3ObjectInfo{{Key: "a.txt", Size: 10}}
	merged := mergeObjectListings(local, nil)
	assert.Equal(t, local, merged)
}

func TestS3Proxy_GetObject(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Add an object
	content := []byte("hello world")
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "test.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects/test.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))

	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, content, body)
}

func TestS3Proxy_GetObject_NotFound(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Proxy_PutObject(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	content := []byte("new content")
	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/new.txt", bytes.NewReader(content))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("ETag"))

	// Verify object was created
	reader, _, err := srv.s3Store.GetObject(context.Background(), "test-bucket", "new.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	data, _ := io.ReadAll(reader)
	assert.Equal(t, content, data)
}

func TestS3Proxy_PutObject_Streaming(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Use an io.Pipe to provide a non-seekable, non-bufferable streaming body.
	// This verifies the handler works with a true streaming reader (like a real HTTP body),
	// not just bytes.Reader which can be consumed all at once.
	content := make([]byte, 128*1024) // 128KB — larger than streaming chunk size (~64KB)
	for i := range content {
		content[i] = byte(i % 256)
	}

	pr, pw := io.Pipe()
	go func() {
		// Write in small chunks to simulate a streaming upload
		for offset := 0; offset < len(content); {
			end := offset + 4096
			if end > len(content) {
				end = len(content)
			}
			_, err := pw.Write(content[offset:end])
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			offset = end
		}
		_ = pw.Close()
	}()

	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/streamed.bin", pr)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(content))
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("ETag"))

	// Verify the object was stored correctly
	reader, _, err := srv.s3Store.GetObject(context.Background(), "test-bucket", "streamed.bin")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	data, _ := io.ReadAll(reader)
	assert.Equal(t, content, data)
}

// zeroReader is an infinite source of zero bytes used to test size limits
// without allocating large buffers.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func TestS3Proxy_PutObject_TooLarge(t *testing.T) {
	const testLimit = 1 * 1024 * 1024 // 1MB for test speed

	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.DataDir = t.TempDir()
	cfg.Coordinator.S3.MaxObjectSize = bytesize.Size(testLimit)

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })
	require.NoError(t, srv.s3Store.CreateBucket(context.Background(), "test-bucket", "admin", 2, nil))

	// Stream zeros just over testLimit to trigger MaxBytesReader without
	// allocating the full buffer in memory.
	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/test-bucket/objects/huge.bin",
		io.LimitReader(zeroReader{}, testLimit+1))
	req.Header.Set("Content-Type", "application/octet-stream")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func TestS3Proxy_PutObject_SystemBucketForbidden(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// System bucket is created automatically by the server
	content := []byte("malicious content")
	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/"+auth.SystemBucket+"/objects/hack.txt", bytes.NewReader(content))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestS3Proxy_DeleteObject(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Add an object
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "to-delete.txt", bytes.NewReader([]byte("bye")), 3, "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodDelete, "/api/s3/buckets/test-bucket/objects/to-delete.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify object is gone from live path (moved to recycle bin)
	_, err = srv.s3Store.HeadObject(context.Background(), "test-bucket", "to-delete.txt")
	assert.ErrorIs(t, err, s3.ErrObjectNotFound)
}

func TestS3Proxy_DeleteObject_SystemBucketForbidden(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// System bucket is created automatically, add an object to it
	_, err := srv.s3Store.PutObject(context.Background(), auth.SystemBucket, "protected.txt", bytes.NewReader([]byte("secret")), 6, "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodDelete, "/api/s3/buckets/"+auth.SystemBucket+"/objects/protected.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestUpdateBucket_SystemBucketForbidden(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Attempt to update system bucket replication factor
	reqBody := `{"replication_factor": 1}`
	req := httptest.NewRequest(http.MethodPatch, "/api/s3/buckets/"+auth.SystemBucket, bytes.NewReader([]byte(reqBody)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)

	// Verify error message
	var resp proto.ErrorResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Contains(t, resp.Message, "cannot modify system bucket")
}

func TestS3Proxy_HeadObject(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Add an object
	content := []byte("hello world")
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "test.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodHead, "/api/s3/buckets/test-bucket/objects/test.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
	assert.Equal(t, "11", rec.Header().Get("Content-Length"))
	assert.NotEmpty(t, rec.Header().Get("ETag"))
}

func TestS3Proxy_HeadObject_NotFound(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodHead, "/api/s3/buckets/test-bucket/objects/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Proxy_NotFound(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/invalid/path", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Proxy_PathTraversal(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	tests := []struct {
		name         string
		path         string
		expectedCode int
	}{
		// URL-encoded path traversal attempts (bypass HTTP path cleaning)
		{"bucket url-encoded ..", "/api/s3/buckets/%2e%2e/objects/passwd", http.StatusBadRequest},
		{"key url-encoded ..", "/api/s3/buckets/test-bucket/objects/%2e%2e%2fetc%2fpasswd", http.StatusBadRequest},
		// Backslash attempts
		{"bucket with backslash", "/api/s3/buckets/..\\..\\etc/objects/passwd", http.StatusBadRequest},
		// URL-encoded dotdot
		{"bucket is dotdot", "/api/s3/buckets/%2e%2e/objects/test", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			srv.adminMux.ServeHTTP(rec, req)

			// Should be rejected with BadRequest, not return file contents
			assert.Equal(t, tt.expectedCode, rec.Code)
		})
	}
}

func TestS3Proxy_URLEncodedKey(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Create object with special chars in name
	content := []byte("hello world")
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "file with spaces.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Access with URL-encoded key
	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects/file%20with%20spaces.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "hello world", rec.Body.String())
}

// --- Panel API Tests ---

func TestPanels_List(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/api/panels", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Panels []auth.PanelDefinition `json:"panels"`
	}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)

	// Should have built-in panels
	assert.GreaterOrEqual(t, len(resp.Panels), 10)

	// Check that visualizer panel exists
	var found bool
	for _, p := range resp.Panels {
		if p.ID == auth.PanelVisualizer {
			found = true
			assert.Equal(t, "Network Topology", p.Name)
			assert.True(t, p.Builtin)
			break
		}
	}
	assert.True(t, found, "visualizer panel should be present")
}

func TestPanels_ListExternal(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Register an external panel first
	panel := auth.PanelDefinition{
		ID:        "ext-test",
		Name:      "External Test",
		Tab:       auth.PanelTabData,
		External:  true,
		PluginURL: "https://example.com/plugin.js",
	}
	err := srv.s3Authorizer.PanelRegistry.Register(panel)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/panels?external=true", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Panels []auth.PanelDefinition `json:"panels"`
	}
	err = json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)

	// Should only have external panels
	require.Len(t, resp.Panels, 1)
	assert.Equal(t, "ext-test", resp.Panels[0].ID)
	assert.True(t, resp.Panels[0].External)
}

func TestPanels_GetBuiltin(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/api/panels/visualizer", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var panel auth.PanelDefinition
	err := json.NewDecoder(rec.Body).Decode(&panel)
	require.NoError(t, err)

	assert.Equal(t, auth.PanelVisualizer, panel.ID)
	assert.True(t, panel.Builtin)
}

func TestPanels_GetNotFound(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/api/panels/nonexistent", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// makeTestAdmin grants admin access for tests (guest user since no TLS)
func makeTestAdmin(srv *Server) {
	// getRequestOwner returns "" for test requests without TLS
	// Some handlers use "" directly, handleUserPermissions converts to "guest"
	// Bind to both for consistent behavior
	srv.s3Authorizer.Bindings.Add(&auth.RoleBinding{
		Name:     "test-admin-binding-empty",
		PeerID:   "",
		RoleName: auth.RoleAdmin,
	})
	srv.s3Authorizer.Bindings.Add(&auth.RoleBinding{
		Name:     "test-admin-binding-guest",
		PeerID:   "guest",
		RoleName: auth.RoleAdmin,
	})
}

func TestPanels_RegisterExternal(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	panel := auth.PanelDefinition{
		ID:         "test-plugin",
		Name:       "Test Plugin",
		Tab:        auth.PanelTabMesh,
		External:   true,
		PluginURL:  "https://example.com/plugin.js",
		PluginType: "script",
	}
	body, _ := json.Marshal(panel)

	req := httptest.NewRequest(http.MethodPost, "/api/panels", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)

	// Verify it was registered
	registered := srv.s3Authorizer.PanelRegistry.Get("test-plugin")
	require.NotNil(t, registered)
	assert.Equal(t, "Test Plugin", registered.Name)
	assert.True(t, registered.External)
}

func TestPanels_RegisterExternal_InvalidURL(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	panel := auth.PanelDefinition{
		ID:        "bad-plugin",
		Name:      "Bad Plugin",
		Tab:       auth.PanelTabMesh,
		External:  true,
		PluginURL: "javascript:alert(1)", // XSS attempt
	}
	body, _ := json.Marshal(panel)

	req := httptest.NewRequest(http.MethodPost, "/api/panels", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid plugin URL")
}

func TestPanels_RegisterExternal_NoURL(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// External panels without PluginURL are allowed (for script-injected panels)
	panel := auth.PanelDefinition{
		ID:   "no-url-plugin",
		Name: "No URL Plugin",
		Tab:  auth.PanelTabMesh,
		// External flag is set by API automatically
	}
	body, _ := json.Marshal(panel)

	req := httptest.NewRequest(http.MethodPost, "/api/panels", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)

	// Verify it was registered with External flag set
	registered := srv.s3Authorizer.PanelRegistry.Get("no-url-plugin")
	require.NotNil(t, registered)
	assert.True(t, registered.External)
}

func TestPanels_UpdatePublic(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// Set visualizer panel to public
	update := map[string]interface{}{
		"public": true,
	}
	body, _ := json.Marshal(update)

	req := httptest.NewRequest(http.MethodPatch, "/api/panels/visualizer", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	panel := srv.s3Authorizer.PanelRegistry.Get(auth.PanelVisualizer)
	require.NotNil(t, panel)
	assert.True(t, panel.Public)
}

func TestPanels_UpdateExternal(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// First register an external panel
	panel := auth.PanelDefinition{
		ID:        "update-test",
		Name:      "Update Test",
		Tab:       auth.PanelTabMesh,
		External:  true,
		PluginURL: "https://example.com/plugin.js",
	}
	err := srv.s3Authorizer.PanelRegistry.Register(panel)
	require.NoError(t, err)

	// Update it
	update := map[string]interface{}{
		"name":        "Updated Name",
		"description": "New description",
	}
	body, _ := json.Marshal(update)

	req := httptest.NewRequest(http.MethodPatch, "/api/panels/update-test", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	updated := srv.s3Authorizer.PanelRegistry.Get("update-test")
	require.NotNil(t, updated)
	assert.Equal(t, "Updated Name", updated.Name)
	assert.Equal(t, "New description", updated.Description)
}

func TestPanels_UpdateNotFound(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	update := map[string]interface{}{
		"public": true,
	}
	body, _ := json.Marshal(update)

	req := httptest.NewRequest(http.MethodPatch, "/api/panels/nonexistent", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestPanels_DeleteExternal(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	// First register an external panel
	panel := auth.PanelDefinition{
		ID:        "delete-test",
		Name:      "Delete Test",
		Tab:       auth.PanelTabMesh,
		External:  true,
		PluginURL: "https://example.com/plugin.js",
	}
	err := srv.s3Authorizer.PanelRegistry.Register(panel)
	require.NoError(t, err)

	// Delete it
	req := httptest.NewRequest(http.MethodDelete, "/api/panels/delete-test", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deletion
	deleted := srv.s3Authorizer.PanelRegistry.Get("delete-test")
	assert.Nil(t, deleted)
}

func TestPanels_DeleteBuiltin_Forbidden(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	req := httptest.NewRequest(http.MethodDelete, "/api/panels/visualizer", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	// Returns BadRequest because Unregister returns error for built-in panels
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cannot unregister built-in panel")
}

func TestPanels_DeleteNotFound(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	req := httptest.NewRequest(http.MethodDelete, "/api/panels/nonexistent", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUserPermissions(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Request without TLS will have empty user, which becomes "guest"
	req := httptest.NewRequest(http.MethodGet, "/api/user/permissions", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		PeerID  string   `json:"peer_id"`
		IsAdmin bool     `json:"is_admin"`
		Panels  []string `json:"panels"`
	}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)

	// Guest user should not be admin
	assert.Equal(t, "guest", resp.PeerID)
	assert.False(t, resp.IsAdmin)
}

func TestUserPermissions_Admin(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	makeTestAdmin(srv)

	req := httptest.NewRequest(http.MethodGet, "/api/user/permissions", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		PeerID  string   `json:"peer_id"`
		IsAdmin bool     `json:"is_admin"`
		Panels  []string `json:"panels"`
	}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)

	// makeTestAdmin grants admin to empty user, which becomes "guest"
	assert.Equal(t, "guest", resp.PeerID)
	assert.True(t, resp.IsAdmin)
	assert.GreaterOrEqual(t, len(resp.Panels), 10) // Admin gets all panels
}

// --- Filter API Tests ---

func TestFilterAdd_TTLValidation(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)
	require.NotNil(t, srv.filter)

	tests := []struct {
		name         string
		ttl          int64
		expectedCode int
		errorMsg     string
	}{
		{
			name:         "permanent rule (ttl=0)",
			ttl:          0,
			expectedCode: http.StatusOK,
		},
		{
			name:         "1 hour ttl",
			ttl:          3600,
			expectedCode: http.StatusOK,
		},
		{
			name:         "1 day ttl",
			ttl:          86400,
			expectedCode: http.StatusOK,
		},
		{
			name:         "1 year ttl (max)",
			ttl:          365 * 24 * 3600,
			expectedCode: http.StatusOK,
		},
		{
			name:         "negative ttl",
			ttl:          -1,
			expectedCode: http.StatusBadRequest,
			errorMsg:     "ttl cannot be negative",
		},
		{
			name:         "ttl exceeds maximum",
			ttl:          365*24*3600 + 1,
			expectedCode: http.StatusBadRequest,
			errorMsg:     "ttl exceeds maximum",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqBody := FilterRulesRequest{
				PeerName: "test-peer",
				Port:     22,
				Protocol: "tcp",
				Action:   "allow",
				TTL:      tt.ttl,
			}
			body, _ := json.Marshal(reqBody)

			req := httptest.NewRequest(http.MethodPost, "/api/filter/rules", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			srv.adminMux.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedCode, rec.Code)
			if tt.errorMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.errorMsg)
			}
		})
	}
}

func TestFilterAdd_WithTTL(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)
	require.NotNil(t, srv.filter)

	// Add a rule with TTL
	reqBody := FilterRulesRequest{
		PeerName: "test-peer",
		Port:     22,
		Protocol: "tcp",
		Action:   "allow",
		TTL:      3600, // 1 hour
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/filter/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify rule was added with correct expiry
	rules := srv.filter.ListRules()
	var found bool
	for _, r := range rules {
		if r.Source == routing.SourceTemporary && r.Rule.Port == 22 {
			found = true
			assert.Greater(t, r.Rule.Expires, time.Now().Unix(), "expires should be in the future")
			assert.LessOrEqual(t, r.Rule.Expires, time.Now().Unix()+3601, "expires should be approximately 1 hour from now")
			break
		}
	}
	assert.True(t, found, "rule should have been added")
}

func TestFilterAdd_WithoutTTL(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)
	require.NotNil(t, srv.filter)

	// Add a permanent rule (TTL=0)
	reqBody := FilterRulesRequest{
		PeerName: "test-peer",
		Port:     80,
		Protocol: "tcp",
		Action:   "allow",
		TTL:      0,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/filter/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify rule was added with no expiry
	rules := srv.filter.ListRules()
	var found bool
	for _, r := range rules {
		if r.Source == routing.SourceTemporary && r.Rule.Port == 80 {
			found = true
			assert.Equal(t, int64(0), r.Rule.Expires, "permanent rule should have expires=0")
			break
		}
	}
	assert.True(t, found, "rule should have been added")
}

func TestFilterAdd_WithSourcePeer(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)
	require.NotNil(t, srv.filter)

	// Add a peer-specific rule
	reqBody := FilterRulesRequest{
		PeerName:   "test-peer",
		Port:       22,
		Protocol:   "tcp",
		Action:     "allow",
		SourcePeer: "trusted-peer",
		TTL:        0,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/filter/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify rule was added with source peer
	rules := srv.filter.ListRules()
	var found bool
	for _, r := range rules {
		if r.Source == routing.SourceTemporary && r.Rule.Port == 22 && r.Rule.SourcePeer == "trusted-peer" {
			found = true
			break
		}
	}
	assert.True(t, found, "peer-specific rule should have been added")
}

// --- Landing Page & Admin Route Tests ---

func TestLandingPage_Default(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "secured mesh network")
	assert.Contains(t, body, `content="5;url=/admin/"`)
}

func TestLandingPage_RedirectsToAdmin(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `href="/admin/"`)
}

func TestAdminDashboard_ServesAtAdmin(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<html")
	assert.Contains(t, body, "tunnelmesh")
}

func TestAdminDashboard_StaticAssets(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/admin/css/style.css", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, strings.Contains(rec.Header().Get("Content-Type"), "text/css"))
}

func TestAdminDashboard_APIStillAtRoot(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestLandingPage_404ForUnknownPaths(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestLandingPage_Custom(t *testing.T) {
	tmpDir := t.TempDir()
	customPage := filepath.Join(tmpDir, "landing.html")
	err := os.WriteFile(customPage, []byte("<html><body>Custom Landing</body></html>"), 0o644)
	require.NoError(t, err)

	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.LandingPage = customPage

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Custom Landing")
}

func TestForwardToMonitoringCoordinator_LoopDetection(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Request without forwarding header — should return 503 (no monitoring coordinator)
	req := httptest.NewRequest(http.MethodGet, "/prometheus/", nil)
	rec := httptest.NewRecorder()
	srv.forwardToMonitoringCoordinator(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	// Request with forwarding header — should return 508 (loop detected)
	req = httptest.NewRequest(http.MethodGet, "/prometheus/", nil)
	req.Header.Set("X-TunnelMesh-Forwarded", "true")
	rec = httptest.NewRecorder()
	srv.forwardToMonitoringCoordinator(rec, req)
	assert.Equal(t, http.StatusLoopDetected, rec.Code)
	assert.Contains(t, rec.Body.String(), "loop detected")
}

func TestObjectPrimaryCoordinator(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Simulate 3 coordinators: self=10.0.0.1, others=10.0.0.2, 10.0.0.3
	srv.storeCoordIPs([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})

	// Same bucket+key always maps to the same coordinator (deterministic)
	first := srv.objectPrimaryCoordinator("bucket-a", "file.txt")
	for i := 0; i < 100; i++ {
		assert.Equal(t, first, srv.objectPrimaryCoordinator("bucket-a", "file.txt"))
	}

	// Different keys should distribute across coordinators
	// With 3 coordinators and many keys, we should hit at least 2 different targets
	targets := make(map[string]bool)
	for i := 0; i < 100; i++ {
		target := srv.objectPrimaryCoordinator("bucket", "key-"+strings.Repeat("x", i))
		if target != "" {
			targets[target] = true
		} else {
			targets["10.0.0.1"] = true // "" means self
		}
	}
	assert.GreaterOrEqual(t, len(targets), 2, "keys should distribute across coordinators")
}

func TestObjectPrimaryCoordinator_SingleCoord(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Single coordinator — no forwarding
	srv.storeCoordIPs([]string{"10.0.0.1"})
	assert.Equal(t, "", srv.objectPrimaryCoordinator("bucket", "key"))

	// Empty list — no forwarding
	srv.storeCoordIPs([]string{})
	assert.Equal(t, "", srv.objectPrimaryCoordinator("bucket", "key"))
}

func TestObjectPrimaryCoordinator_Self(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// With enough coordinators, self should be primary for some keys
	srv.storeCoordIPs([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})

	selfCount := 0
	for i := 0; i < 100; i++ {
		if srv.objectPrimaryCoordinator("b", strings.Repeat("k", i)) == "" {
			selfCount++
		}
	}
	assert.Greater(t, selfCount, 0, "self should be primary for some keys")
	assert.Less(t, selfCount, 100, "self should not be primary for all keys")
}

func TestForwardS3Request_LoopPrevention(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	srv.storeCoordIPs([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})

	// Request with X-TunnelMesh-Forwarded header should not be forwarded
	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/b/objects/k", nil)
	req.Header.Set("X-TunnelMesh-Forwarded", "true")
	rec := httptest.NewRecorder()

	forwarded := srv.ForwardS3Request(rec, req, "b", "k", "")
	assert.False(t, forwarded, "forwarded requests should be handled locally")
}

func TestForwardS3Request_ProxyHeaders(t *testing.T) {
	// Start a test server to receive the proxied request
	var receivedReq *http.Request
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedReq = r
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	// Extract host:port from backend URL
	backendAddr := strings.TrimPrefix(backend.URL, "https://")

	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Override transport to trust the test server's certificate
	srv.s3ForwardTransport = backend.Client().Transport.(*http.Transport)

	req := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/b/objects/k", bytes.NewReader([]byte("hello")))
	rec := httptest.NewRecorder()

	srv.forwardS3Request(rec, req, backendAddr, "b")

	require.NotNil(t, receivedReq, "backend should have received the request")
	assert.Equal(t, "true", receivedReq.Header.Get("X-TunnelMesh-Forwarded"))
	assert.Equal(t, http.MethodPut, receivedReq.Method)
}

func TestS3GC_MethodNotAllowed(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/gc", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestS3GC_PurgeAllRecycled(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Create objects and move them to recycle bin
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "a.txt", bytes.NewReader([]byte("aaa")), 3, "text/plain", nil)
	require.NoError(t, err)
	_, err = srv.s3Store.PutObject(context.Background(), "test-bucket", "b.txt", bytes.NewReader([]byte("bbb")), 3, "text/plain", nil)
	require.NoError(t, err)

	err = srv.s3Store.DeleteObject(context.Background(), "test-bucket", "a.txt")
	require.NoError(t, err)
	err = srv.s3Store.DeleteObject(context.Background(), "test-bucket", "b.txt")
	require.NoError(t, err)

	// Trigger GC with purge_recycle_bin
	body := bytes.NewReader([]byte(`{"purge_recycle_bin": true}`))
	req := httptest.NewRequest(http.MethodPost, "/api/s3/gc", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var result map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&result)
	require.NoError(t, err)

	// Should have purged both recycled objects
	assert.Equal(t, float64(2), result["recycled_purged"])
	assert.Contains(t, result, "versions_pruned")
	assert.Contains(t, result, "chunks_deleted")
	assert.Contains(t, result, "bytes_reclaimed")
	assert.Contains(t, result, "duration_seconds")
}

func TestS3GC_WithoutPurgeAll(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Create and delete an object (recently, so retention won't expire it)
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "recent.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)
	err = srv.s3Store.DeleteObject(context.Background(), "test-bucket", "recent.txt")
	require.NoError(t, err)

	// Set retention to 90 days — recent recycled entry should NOT be purged
	srv.s3Store.SetRecycleBinRetentionDays(90)

	body := bytes.NewReader([]byte(`{}`))
	req := httptest.NewRequest(http.MethodPost, "/api/s3/gc", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var result map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&result)
	require.NoError(t, err)

	// Recent recycled entry should not have been purged
	assert.Equal(t, float64(0), result["recycled_purged"])
}

func TestS3GC_EmptyBody(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// POST with no body should work (defaults to no purge_all)
	req := httptest.NewRequest(http.MethodPost, "/api/s3/gc", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var result map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&result)
	require.NoError(t, err)
	assert.Contains(t, result, "recycled_purged")
}

func TestS3GC_ConcurrentReturns429(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Hold the GC lock to simulate an in-progress GC
	srv.gcMu.Lock()

	req := httptest.NewRequest(http.MethodPost, "/api/s3/gc", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	srv.gcMu.Unlock()

	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
}

// --- Recycle Bin HTTP Handler Tests ---

func TestS3Proxy_GetRecycledObject(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Upload and delete an object so it lands in the recycle bin
	content := []byte("recoverable content")
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "deleted.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	err = srv.s3Store.DeleteObject(context.Background(), "test-bucket", "deleted.txt")
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/recyclebin/deleted.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))

	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, content, body)
}

func TestS3Proxy_GetRecycledObject_NotFound(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/recyclebin/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Proxy_GetRecycledObject_MethodNotAllowed(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	req := httptest.NewRequest(http.MethodPost, "/api/s3/buckets/test-bucket/recyclebin/file.txt", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestS3Proxy_ListRecycledObjects_PrefixFilter(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Create and delete objects in different prefixes
	for _, key := range []string{"docs/a.txt", "docs/b.txt", "src/main.go"} {
		_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader([]byte("x")), 1, "text/plain", nil)
		require.NoError(t, err)
		err = srv.s3Store.DeleteObject(context.Background(), "test-bucket", key)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/recyclebin?prefix=docs/", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var objects []S3ObjectInfo
	err := json.NewDecoder(rec.Body).Decode(&objects)
	require.NoError(t, err)

	assert.Len(t, objects, 2)
	for _, obj := range objects {
		assert.True(t, strings.HasPrefix(obj.Key, "docs/"), "expected docs/ prefix, got: %s", obj.Key)
	}
}

// --- Listing Index Tests ---

func newTestServerWithListingIndex(t *testing.T) *Server {
	t.Helper()
	srv := newTestServerWithS3AndBucket(t)
	srv.listingIndexNotify = make(chan struct{}, 1)
	return srv
}

func TestUpdateListingIndex_Put(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	info := S3ObjectInfo{
		Key:          "file.txt",
		Size:         100,
		LastModified: "2024-01-01T00:00:00Z",
		ContentType:  "text/plain",
	}
	srv.updateListingIndex("test-bucket", "file.txt", &info, "put")

	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
	bl := idx.Buckets["test-bucket"]
	require.NotNil(t, bl)
	require.Len(t, bl.Objects, 1)
	assert.Equal(t, "file.txt", bl.Objects[0].Key)
	assert.Equal(t, int64(100), bl.Objects[0].Size)
	assert.Equal(t, "text/plain", bl.Objects[0].ContentType)
}

func TestUpdateListingIndex_PutOverwrite(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	info1 := S3ObjectInfo{Key: "file.txt", Size: 100, LastModified: "2024-01-01T00:00:00Z"}
	srv.updateListingIndex("test-bucket", "file.txt", &info1, "put")

	info2 := S3ObjectInfo{Key: "file.txt", Size: 200, LastModified: "2024-02-01T00:00:00Z"}
	srv.updateListingIndex("test-bucket", "file.txt", &info2, "put")

	idx := srv.localListingIndex.Load()
	bl := idx.Buckets["test-bucket"]
	require.Len(t, bl.Objects, 1)
	assert.Equal(t, int64(200), bl.Objects[0].Size)
	assert.Equal(t, "2024-02-01T00:00:00Z", bl.Objects[0].LastModified)
}

func TestUpdateListingIndex_Delete(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	info := S3ObjectInfo{Key: "file.txt", Size: 100, LastModified: "2024-01-01T00:00:00Z"}
	srv.updateListingIndex("test-bucket", "file.txt", &info, "put")

	srv.updateListingIndex("test-bucket", "file.txt", nil, "delete")

	idx := srv.localListingIndex.Load()
	bl := idx.Buckets["test-bucket"]
	assert.Empty(t, bl.Objects)
	require.Len(t, bl.Recycled, 1)
	assert.Equal(t, "file.txt", bl.Recycled[0].Key)
	assert.NotEmpty(t, bl.Recycled[0].DeletedAt)
}

func TestUpdateListingIndex_DeleteNonexistent(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Should not panic when deleting a key that doesn't exist
	srv.updateListingIndex("test-bucket", "nonexistent.txt", nil, "delete")

	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
}

func TestUpdateListingIndex_Undelete(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	info := S3ObjectInfo{Key: "file.txt", Size: 100, LastModified: "2024-01-01T00:00:00Z"}
	srv.updateListingIndex("test-bucket", "file.txt", &info, "put")
	srv.updateListingIndex("test-bucket", "file.txt", nil, "delete")
	srv.updateListingIndex("test-bucket", "file.txt", nil, "undelete")

	idx := srv.localListingIndex.Load()
	bl := idx.Buckets["test-bucket"]
	require.Len(t, bl.Objects, 1)
	assert.Equal(t, "file.txt", bl.Objects[0].Key)
	assert.Empty(t, bl.Objects[0].DeletedAt, "DeletedAt should be cleared after undelete")
	assert.Empty(t, bl.Recycled)
}

func TestUpdateListingIndex_SetsDirtyFlag(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	assert.False(t, srv.listingIndexDirty.Load())

	info := S3ObjectInfo{Key: "file.txt", Size: 100}
	srv.updateListingIndex("test-bucket", "file.txt", &info, "put")

	assert.True(t, srv.listingIndexDirty.Load())
}

func TestUpdateListingIndex_CopyOnWrite(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	info1 := S3ObjectInfo{Key: "file1.txt", Size: 100}
	srv.updateListingIndex("test-bucket", "file1.txt", &info1, "put")

	// Capture the pointer before the concurrent update
	before := srv.localListingIndex.Load()

	// Concurrent updates should not corrupt the snapshot
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			info := S3ObjectInfo{Key: "concurrent.txt", Size: int64(n)}
			srv.updateListingIndex("test-bucket", "concurrent.txt", &info, "put")
		}(i)
	}
	wg.Wait()

	// The old snapshot should still have exactly 1 object
	require.Len(t, before.Buckets["test-bucket"].Objects, 1)
	assert.Equal(t, "file1.txt", before.Buckets["test-bucket"].Objects[0].Key)
}

// --- filterByPrefixDelimiter Tests ---

func TestFilterByPrefixDelimiter_NoFilter(t *testing.T) {
	objs := []S3ObjectInfo{
		{Key: "a.txt", Size: 10},
		{Key: "b.txt", Size: 20},
	}
	result := filterByPrefixDelimiter(objs, "", "")
	assert.Len(t, result, 2)
}

func TestFilterByPrefixDelimiter_PrefixOnly(t *testing.T) {
	objs := []S3ObjectInfo{
		{Key: "docs/readme.md", Size: 10},
		{Key: "docs/guide.md", Size: 20},
		{Key: "src/main.go", Size: 30},
	}
	result := filterByPrefixDelimiter(objs, "docs/", "")
	assert.Len(t, result, 2)
	for _, obj := range result {
		assert.True(t, strings.HasPrefix(obj.Key, "docs/"))
	}
}

func TestFilterByPrefixDelimiter_PrefixAndDelimiter(t *testing.T) {
	objs := []S3ObjectInfo{
		{Key: "file1.txt", Size: 10},
		{Key: "folder/file2.txt", Size: 20},
		{Key: "folder/file3.txt", Size: 30},
	}
	result := filterByPrefixDelimiter(objs, "", "/")

	// Should have: file1.txt and folder/ prefix
	assert.Len(t, result, 2)

	byKey := make(map[string]S3ObjectInfo)
	for _, obj := range result {
		byKey[obj.Key] = obj
	}

	assert.Equal(t, int64(10), byKey["file1.txt"].Size)
	assert.True(t, byKey["folder/"].IsPrefix)
	assert.Equal(t, int64(50), byKey["folder/"].Size) // 20 + 30
}

func TestFilterByPrefixDelimiter_NestedFolders(t *testing.T) {
	objs := []S3ObjectInfo{
		{Key: "a/b/c/file.txt", Size: 10},
		{Key: "a/b/d/file.txt", Size: 20},
		{Key: "a/file.txt", Size: 30},
	}
	result := filterByPrefixDelimiter(objs, "a/", "/")

	// Should have: a/file.txt and a/b/ prefix (grouping both c/ and d/)
	assert.Len(t, result, 2)

	byKey := make(map[string]S3ObjectInfo)
	for _, obj := range result {
		byKey[obj.Key] = obj
	}

	assert.Equal(t, int64(30), byKey["a/file.txt"].Size)
	assert.True(t, byKey["a/b/"].IsPrefix)
}

func TestFilterByPrefixDelimiter_EmptyResult(t *testing.T) {
	objs := []S3ObjectInfo{
		{Key: "a.txt", Size: 10},
	}
	result := filterByPrefixDelimiter(objs, "nonexistent/", "")
	assert.Empty(t, result)
}

// --- reconcileLocalIndex Tests ---

func TestReconcileLocalIndex_PopulatesFromDisk(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Add objects directly to S3 store
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("hello")), 5, "text/plain", nil)
	require.NoError(t, err)

	srv.reconcileLocalIndex(context.Background())

	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
	bl := idx.Buckets["test-bucket"]
	require.NotNil(t, bl)
	require.Len(t, bl.Objects, 1)
	assert.Equal(t, "file.txt", bl.Objects[0].Key)
}

func TestReconcileLocalIndex_IncludesRecycled(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("hello")), 5, "text/plain", nil)
	require.NoError(t, err)

	err = srv.s3Store.DeleteObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)

	srv.reconcileLocalIndex(context.Background())

	idx := srv.localListingIndex.Load()
	bl := idx.Buckets["test-bucket"]
	require.NotNil(t, bl)
	assert.Empty(t, bl.Objects)
	require.Len(t, bl.Recycled, 1)
	assert.Equal(t, "file.txt", bl.Recycled[0].Key)
	assert.NotEmpty(t, bl.Recycled[0].DeletedAt)
}

func TestReconcileLocalIndex_SkipsSystemBucket(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	srv.reconcileLocalIndex(context.Background())

	idx := srv.localListingIndex.Load()
	require.NotNil(t, idx)
	_, exists := idx.Buckets[auth.SystemBucket]
	assert.False(t, exists, "system bucket should be excluded from listing index")
}

func TestReconcileLocalIndex_DetectsReplicationArrivals(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// First reconcile — empty
	srv.reconcileLocalIndex(context.Background())
	idx := srv.localListingIndex.Load()
	_, exists := idx.Buckets["test-bucket"]
	assert.False(t, exists, "bucket should not be in index when empty")

	// Simulate a replication arrival by adding an object directly
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "replicated.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)

	// Clear dirty flag to detect the change
	srv.listingIndexDirty.Store(false)

	srv.reconcileLocalIndex(context.Background())

	idx = srv.localListingIndex.Load()
	bl := idx.Buckets["test-bucket"]
	require.NotNil(t, bl)
	require.Len(t, bl.Objects, 1)
	assert.Equal(t, "replicated.txt", bl.Objects[0].Key)
}

func TestReconcileLocalIndex_SetsDirtyOnChange(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// First reconcile with no objects — sets to something from nil
	srv.reconcileLocalIndex(context.Background())
	// Drain the notify channel
	select {
	case <-srv.listingIndexNotify:
	default:
	}
	srv.listingIndexDirty.Store(false)

	// Second reconcile with no changes — should NOT set dirty
	srv.reconcileLocalIndex(context.Background())
	assert.False(t, srv.listingIndexDirty.Load(), "dirty flag should not be set when nothing changed")

	// Add an object and reconcile — SHOULD set dirty
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "new.txt", bytes.NewReader([]byte("x")), 1, "text/plain", nil)
	require.NoError(t, err)

	srv.reconcileLocalIndex(context.Background())
	assert.True(t, srv.listingIndexDirty.Load(), "dirty flag should be set when index changes")
}

// --- Integration tests for listing with peer index ---

func TestListObjects_MergesPeerListings(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Add a local object
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "local.txt", bytes.NewReader([]byte("local")), 5, "text/plain", nil)
	require.NoError(t, err)

	// Populate peer listings via atomic pointer
	pl := &peerListings{
		Objects: map[string][]S3ObjectInfo{
			"test-bucket": {
				{Key: "peer.txt", Size: 42, LastModified: "2024-01-01T00:00:00Z", ContentType: "text/plain"},
			},
		},
		Recycled: make(map[string][]S3ObjectInfo),
	}
	srv.peerListings.Store(pl)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var objects []S3ObjectInfo
	err = json.NewDecoder(rec.Body).Decode(&objects)
	require.NoError(t, err)

	// Should have both local and peer objects
	keys := make(map[string]bool)
	for _, obj := range objects {
		keys[obj.Key] = true
	}
	assert.True(t, keys["local.txt"], "should include local object")
	assert.True(t, keys["peer.txt"], "should include peer object")
}

func TestListObjects_PeerListingsDedup(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Add a local object
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "shared.txt", bytes.NewReader([]byte("local")), 5, "text/plain", nil)
	require.NoError(t, err)

	// Peer has the same key with a newer timestamp
	pl := &peerListings{
		Objects: map[string][]S3ObjectInfo{
			"test-bucket": {
				{Key: "shared.txt", Size: 99, LastModified: "2099-01-01T00:00:00Z"},
			},
		},
		Recycled: make(map[string][]S3ObjectInfo),
	}
	srv.peerListings.Store(pl)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	var objects []S3ObjectInfo
	err = json.NewDecoder(rec.Body).Decode(&objects)
	require.NoError(t, err)

	// Find the shared.txt entry — newer peer version should win
	for _, obj := range objects {
		if obj.Key == "shared.txt" {
			assert.Equal(t, int64(99), obj.Size, "peer's newer entry should win dedup")
			break
		}
	}
}

func TestListRecycledObjects_MergesPeerListings(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Add and delete a local object to create a recycled entry
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "deleted-local.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)
	err = srv.s3Store.DeleteObject(context.Background(), "test-bucket", "deleted-local.txt")
	require.NoError(t, err)

	// Populate peer recycled listings
	pl := &peerListings{
		Objects: make(map[string][]S3ObjectInfo),
		Recycled: map[string][]S3ObjectInfo{
			"test-bucket": {
				{Key: "deleted-peer.txt", Size: 10, DeletedAt: "2024-06-01T00:00:00Z"},
			},
		},
	}
	srv.peerListings.Store(pl)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/recyclebin", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var objects []S3ObjectInfo
	err = json.NewDecoder(rec.Body).Decode(&objects)
	require.NoError(t, err)

	keys := make(map[string]bool)
	for _, obj := range objects {
		keys[obj.Key] = true
	}
	assert.True(t, keys["deleted-local.txt"], "should include local recycled object")
	assert.True(t, keys["deleted-peer.txt"], "should include peer recycled object")
}

func TestListObjects_NoPeers(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)

	// peerListings is nil (no peers)
	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var objects []S3ObjectInfo
	err = json.NewDecoder(rec.Body).Decode(&objects)
	require.NoError(t, err)
	assert.Len(t, objects, 1)
	assert.Equal(t, "file.txt", objects[0].Key)
}

func TestListObjects_PrefixFilterOnPeerResults(t *testing.T) {
	srv := newTestServerWithListingIndex(t)

	// Peer has objects with various prefixes
	pl := &peerListings{
		Objects: map[string][]S3ObjectInfo{
			"test-bucket": {
				{Key: "docs/readme.md", Size: 10, LastModified: "2024-01-01T00:00:00Z"},
				{Key: "docs/guide.md", Size: 20, LastModified: "2024-01-01T00:00:00Z"},
				{Key: "src/main.go", Size: 30, LastModified: "2024-01-01T00:00:00Z"},
			},
		},
		Recycled: make(map[string][]S3ObjectInfo),
	}
	srv.peerListings.Store(pl)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects?prefix=docs/", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	var objects []S3ObjectInfo
	err := json.NewDecoder(rec.Body).Decode(&objects)
	require.NoError(t, err)

	// Only docs/ entries should be included
	for _, obj := range objects {
		assert.True(t, strings.HasPrefix(obj.Key, "docs/"), "all results should have docs/ prefix, got: %s", obj.Key)
	}
	assert.Len(t, objects, 2)
}

func TestS3ListVersions_ForwardsToPrimary(t *testing.T) {
	// Backend simulates the primary coordinator that holds version history
	var receivedReq *http.Request
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedReq = r
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]S3VersionInfo{
			{VersionID: "v1", Size: 100, IsCurrent: true},
		})
	}))
	defer backend.Close()
	backendAddr := strings.TrimPrefix(backend.URL, "https://")

	srv := newTestServerWithS3AndBucket(t)
	srv.s3ForwardTransport = backend.Client().Transport.(*http.Transport)

	// Configure multi-coordinator: self=10.0.0.1, primary for test-bucket/doc.txt = backendAddr
	srv.storeCoordIPs([]string{"10.0.0.1", backendAddr})

	// Ensure "test-bucket/doc.txt" primary is the backend (not self)
	// Find a key that hashes to the backend
	key := findKeyWithPrimary(t, srv, "test-bucket", backendAddr)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects/"+key+"/versions", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.NotNil(t, receivedReq, "request should have been forwarded to primary")
	assert.Equal(t, "true", receivedReq.Header.Get("X-TunnelMesh-Forwarded"))
	assert.Equal(t, http.StatusOK, rec.Code)

	var versions []S3VersionInfo
	err := json.NewDecoder(rec.Body).Decode(&versions)
	require.NoError(t, err)
	assert.Len(t, versions, 1)
	assert.Equal(t, "v1", versions[0].VersionID)
}

func TestS3ListVersions_NoForwardWhenAlreadyForwarded(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	srv.storeCoordIPs([]string{"10.0.0.1", "10.0.0.2"})

	// Put an object so the bucket/key exist locally (no forwarding on not-found)
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "doc.txt",
		bytes.NewReader([]byte("hello")), 5, "text/plain", nil)
	require.NoError(t, err)

	// Request with forwarded header should NOT forward again (loop prevention)
	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects/doc.txt/versions", nil)
	req.Header.Set("X-TunnelMesh-Forwarded", "true")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	// Should return local result (empty or with versions), not forward
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestS3GetObjectVersion_ForwardsToPrimary(t *testing.T) {
	// Backend simulates the primary coordinator that holds the version
	var receivedReq *http.Request
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedReq = r
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Version-Id", "v1")
		_, _ = w.Write([]byte("version content"))
	}))
	defer backend.Close()
	backendAddr := strings.TrimPrefix(backend.URL, "https://")

	srv := newTestServerWithS3AndBucket(t)
	srv.s3ForwardTransport = backend.Client().Transport.(*http.Transport)
	srv.storeCoordIPs([]string{"10.0.0.1", backendAddr})

	key := findKeyWithPrimary(t, srv, "test-bucket", backendAddr)

	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets/test-bucket/objects/"+key+"?versionId=v1", nil)
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.NotNil(t, receivedReq, "request should have been forwarded to primary")
	assert.Equal(t, "true", receivedReq.Header.Get("X-TunnelMesh-Forwarded"))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "version content", rec.Body.String())
}

func TestS3RestoreVersion_ForwardsToPrimary(t *testing.T) {
	// Backend simulates the primary coordinator
	var receivedReq *http.Request
	var receivedBody []byte
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedReq = r
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":     "restored",
			"version_id": "v1",
		})
	}))
	defer backend.Close()
	backendAddr := strings.TrimPrefix(backend.URL, "https://")

	srv := newTestServerWithS3AndBucket(t)
	srv.s3ForwardTransport = backend.Client().Transport.(*http.Transport)
	srv.storeCoordIPs([]string{"10.0.0.1", backendAddr})

	key := findKeyWithPrimary(t, srv, "test-bucket", backendAddr)

	body := `{"version_id":"v1"}`
	req := httptest.NewRequest(http.MethodPost, "/api/s3/buckets/test-bucket/objects/"+key+"/restore",
		bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	require.NotNil(t, receivedReq, "request should have been forwarded to primary")
	assert.Equal(t, "true", receivedReq.Header.Get("X-TunnelMesh-Forwarded"))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, string(receivedBody), "v1")
}

func TestS3RestoreVersion_NoForwardWhenAlreadyForwarded(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)
	srv.storeCoordIPs([]string{"10.0.0.1", "10.0.0.2"})

	// Put an object with a version to restore
	_, err := srv.s3Store.PutObject(context.Background(), "test-bucket", "doc.txt",
		bytes.NewReader([]byte("v1")), 2, "text/plain", nil)
	require.NoError(t, err)
	_, err = srv.s3Store.PutObject(context.Background(), "test-bucket", "doc.txt",
		bytes.NewReader([]byte("v2")), 2, "text/plain", nil)
	require.NoError(t, err)

	versions, err := srv.s3Store.ListVersions(context.Background(), "test-bucket", "doc.txt")
	require.NoError(t, err)
	require.True(t, len(versions) >= 2, "should have at least 2 versions")

	// Find the non-current version
	var oldVersionID string
	for _, v := range versions {
		if !v.IsCurrent {
			oldVersionID = v.VersionID
			break
		}
	}
	require.NotEmpty(t, oldVersionID, "should have a non-current version")

	// Request with forwarded header should NOT forward again
	body := `{"version_id":"` + oldVersionID + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/s3/buckets/test-bucket/objects/doc.txt/restore",
		bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TunnelMesh-Forwarded", "true")
	rec := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(rec, req)

	// Should handle locally, not forward
	assert.Equal(t, http.StatusOK, rec.Code)
	var result map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, "restored", result["status"])
}

// findKeyWithPrimary finds a key that hashes to the given target coordinator.
func findKeyWithPrimary(t *testing.T, srv *Server, bucket, targetAddr string) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		key := "key-" + strconv.Itoa(i)
		if primary := srv.objectPrimaryCoordinator(bucket, key); primary == targetAddr {
			return key
		}
	}
	t.Fatal("could not find a key that hashes to target coordinator")
	return ""
}
