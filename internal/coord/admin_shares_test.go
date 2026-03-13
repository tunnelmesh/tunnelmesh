package coord

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/replication"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

// mockGCRoundTripper redirects all HTTP(S) requests to a target base URL.
// Used in tests to intercept mesh-internal GC calls without TLS.
type mockGCRoundTripper struct {
	targetBase string // e.g., "http://127.0.0.1:12345"
}

func (m *mockGCRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	newURL := m.targetBase + req.URL.Path
	newReq, err := http.NewRequestWithContext(req.Context(), req.Method, newURL, req.Body)
	if err != nil {
		return nil, err
	}
	for k, v := range req.Header {
		newReq.Header[k] = v
	}
	return http.DefaultTransport.RoundTrip(newReq)
}

// noopReplicationTransport satisfies replication.Transport for tests that only need
// a real *replication.Replicator to call GetPeers() — no actual message passing.
type noopReplicationTransport struct{}

func (n *noopReplicationTransport) SendToCoordinator(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (n *noopReplicationTransport) RegisterHandler(_ func(ctx context.Context, from string, data []byte) error) {
}

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
	require.NoError(t, srv.s3Store.CreateBucket(t.Context(), bucketName, "owner", 1))

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

// TestShareDelete_PeerForwardingIsSynchronous verifies that the DELETE /api/shares/{name}
// HTTP response is not returned until peer bucket deletion completes.
// If forwarding ran in a goroutine, the handler would close handlerDone before the
// mock peer receives the request, causing the test to fail.
func TestShareDelete_PeerForwardingIsSynchronous(t *testing.T) {
	srv := newTestServerWithS3AndBucket(t)

	// Create a file share directly (bypasses TLS-based owner auth in the HTTP handler)
	_, err := srv.fileShareMgr.Create(t.Context(), "sync-test", "Test share", "testowner", 0, nil)
	require.NoError(t, err)

	// Track when the mock peer GC endpoint receives the deletion request.
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

	// Inject a minimal replicator with one fake peer so forwardBucketDeletionToPeers
	// has a non-empty peer list and actually makes the HTTP call.
	fakeReplicator := replication.NewReplicator(replication.Config{
		Transport: &noopReplicationTransport{},
	})
	fakeReplicator.AddPeer("10.99.99.99", "mock-coord")
	srv.replicator = fakeReplicator

	// Call DELETE in a goroutine so we can observe ordering.
	handlerDone := make(chan struct{})
	go func() {
		req := httptest.NewRequest(http.MethodDelete, "/api/shares/sync-test", nil)
		rec := httptest.NewRecorder()
		srv.adminMux.ServeHTTP(rec, req)
		close(handlerDone)
	}()

	// Synchronous forwarding: peer must receive the request before the handler returns.
	// If forwarding is async (the old bug), handlerDone fires first.
	select {
	case <-peerReceived:
		// Good: peer was notified before the handler returned
	case <-handlerDone:
		t.Error("handler returned before peer received the deletion request (forwarding must be synchronous)")
		return
	case <-time.After(5 * time.Second):
		t.Error("timeout waiting for peer notification")
		return
	}

	// Wait for the handler to complete
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Error("timeout waiting for handler to complete")
	}
}
