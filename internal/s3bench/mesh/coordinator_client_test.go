package mesh

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCoordinatorClient_CreateBucket(t *testing.T) {
	tests := []struct {
		name           string
		bucketName     string
		ownerID        string
		quotaMB        int64
		serverStatus   int
		serverResponse string
		expectError    bool
		errorContains  string
	}{
		{
			name:         "successful creation",
			bucketName:   "test-bucket",
			ownerID:      "user123",
			quotaMB:      1000,
			serverStatus: http.StatusOK,
			expectError:  false,
		},
		{
			name:         "bucket already exists (conflict)",
			bucketName:   "existing-bucket",
			ownerID:      "user123",
			quotaMB:      1000,
			serverStatus: http.StatusConflict,
			expectError:  false, // Should handle gracefully
		},
		{
			name:           "server error",
			bucketName:     "test-bucket",
			ownerID:        "user123",
			quotaMB:        1000,
			serverStatus:   http.StatusInternalServerError,
			serverResponse: "internal error",
			expectError:    true,
			errorContains:  "500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("Expected POST, got %s", r.Method)
				}
				if !strings.Contains(r.URL.Path, "/api/s3/buckets") {
					t.Errorf("Expected /api/s3/buckets in path, got %s", r.URL.Path)
				}

				w.WriteHeader(tt.serverStatus)
				if tt.serverResponse != "" {
					_, _ = w.Write([]byte(tt.serverResponse))
				}
			}))
			defer server.Close()

			client := &CoordinatorClient{
				baseURL:    server.URL,
				httpClient: server.Client(),
				accessKey:  "test-access",
				secretKey:  "test-secret",
			}

			err := client.CreateBucket(context.Background(), tt.bucketName, tt.ownerID, tt.quotaMB, nil)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}

func TestCoordinatorClient_PutObject(t *testing.T) {
	tests := []struct {
		name          string
		bucket        string
		key           string
		body          []byte
		contentType   string
		metadata      map[string]string
		serverStatus  int
		expectError   bool
		errorContains string
	}{
		{
			name:         "successful upload",
			bucket:       "test-bucket",
			key:          "test-file.txt",
			body:         []byte("test content"),
			contentType:  "text/plain",
			metadata:     map[string]string{"author": "test"},
			serverStatus: http.StatusOK,
			expectError:  false,
		},
		{
			name:         "key with special characters",
			bucket:       "test-bucket",
			key:          "folder/sub folder/file name.txt",
			body:         []byte("test content"),
			contentType:  "text/plain",
			serverStatus: http.StatusOK,
			expectError:  false,
		},
		{
			name:          "server error",
			bucket:        "test-bucket",
			key:           "test-file.txt",
			body:          []byte("test content"),
			contentType:   "text/plain",
			serverStatus:  http.StatusInternalServerError,
			expectError:   true,
			errorContains: "500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut {
					t.Errorf("Expected PUT, got %s", r.Method)
				}

				// Verify headers
				if r.Header.Get("Content-Type") != tt.contentType {
					t.Errorf("Expected Content-Type %s, got %s", tt.contentType, r.Header.Get("Content-Type"))
				}

				// Verify metadata headers
				for k, v := range tt.metadata {
					headerKey := "x-amz-meta-" + k
					if r.Header.Get(headerKey) != v {
						t.Errorf("Expected header %s=%s, got %s", headerKey, v, r.Header.Get(headerKey))
					}
				}

				// Verify body
				body, _ := io.ReadAll(r.Body)
				if string(body) != string(tt.body) {
					t.Errorf("Expected body %q, got %q", string(tt.body), string(body))
				}

				w.WriteHeader(tt.serverStatus)
			}))
			defer server.Close()

			client := &CoordinatorClient{
				baseURL:    server.URL,
				httpClient: server.Client(),
				accessKey:  "test-access",
				secretKey:  "test-secret",
			}

			err := client.PutObject(context.Background(), tt.bucket, tt.key, tt.body, tt.contentType, tt.metadata)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}

func TestCoordinatorClient_GetObject(t *testing.T) {
	tests := []struct {
		name          string
		bucket        string
		key           string
		serverStatus  int
		serverBody    string
		expectError   bool
		errorContains string
	}{
		{
			name:         "successful download",
			bucket:       "test-bucket",
			key:          "test-file.txt",
			serverStatus: http.StatusOK,
			serverBody:   "file content here",
			expectError:  false,
		},
		{
			name:          "object not found",
			bucket:        "test-bucket",
			key:           "missing-file.txt",
			serverStatus:  http.StatusNotFound,
			expectError:   true,
			errorContains: "not found",
		},
		{
			name:         "key with special characters",
			bucket:       "test-bucket",
			key:          "folder/file with spaces.txt",
			serverStatus: http.StatusOK,
			serverBody:   "content",
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("Expected GET, got %s", r.Method)
				}

				w.WriteHeader(tt.serverStatus)
				if tt.serverBody != "" {
					_, _ = w.Write([]byte(tt.serverBody))
				}
			}))
			defer server.Close()

			client := &CoordinatorClient{
				baseURL:    server.URL,
				httpClient: server.Client(),
				accessKey:  "test-access",
				secretKey:  "test-secret",
			}

			data, err := client.GetObject(context.Background(), tt.bucket, tt.key)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if string(data) != tt.serverBody {
					t.Errorf("Expected body %q, got %q", tt.serverBody, string(data))
				}
			}
		})
	}
}

func TestCoordinatorClient_DeleteObject(t *testing.T) {
	tests := []struct {
		name          string
		bucket        string
		key           string
		serverStatus  int
		expectError   bool
		errorContains string
	}{
		{
			name:         "successful deletion",
			bucket:       "test-bucket",
			key:          "test-file.txt",
			serverStatus: http.StatusOK,
			expectError:  false,
		},
		{
			name:         "deletion with 204 No Content",
			bucket:       "test-bucket",
			key:          "test-file.txt",
			serverStatus: http.StatusNoContent,
			expectError:  false,
		},
		{
			name:          "object not found",
			bucket:        "test-bucket",
			key:           "missing-file.txt",
			serverStatus:  http.StatusNotFound,
			expectError:   true,
			errorContains: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					t.Errorf("Expected DELETE, got %s", r.Method)
				}

				w.WriteHeader(tt.serverStatus)
			}))
			defer server.Close()

			client := &CoordinatorClient{
				baseURL:    server.URL,
				httpClient: server.Client(),
				accessKey:  "test-access",
				secretKey:  "test-secret",
			}

			err := client.DeleteObject(context.Background(), tt.bucket, tt.key)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}

func TestCoordinatorClient_BucketExists(t *testing.T) {
	tests := []struct {
		name         string
		bucketName   string
		serverStatus int
		expectExists bool
		expectError  bool
	}{
		{
			name:         "bucket exists",
			bucketName:   "existing-bucket",
			serverStatus: http.StatusOK,
			expectExists: true,
			expectError:  false,
		},
		{
			name:         "bucket does not exist",
			bucketName:   "missing-bucket",
			serverStatus: http.StatusNotFound,
			expectExists: false,
			expectError:  false,
		},
		{
			name:         "server error",
			bucketName:   "test-bucket",
			serverStatus: http.StatusInternalServerError,
			expectExists: false,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead {
					t.Errorf("Expected HEAD, got %s", r.Method)
				}

				w.WriteHeader(tt.serverStatus)
			}))
			defer server.Close()

			client := &CoordinatorClient{
				baseURL:    server.URL,
				httpClient: server.Client(),
				accessKey:  "test-access",
				secretKey:  "test-secret",
			}

			exists, err := client.BucketExists(context.Background(), tt.bucketName)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if exists != tt.expectExists {
					t.Errorf("Expected exists=%v, got %v", tt.expectExists, exists)
				}
			}
		})
	}
}

func TestCoordinatorClient_DeleteShare(t *testing.T) {
	tests := []struct {
		name          string
		shareName     string
		serverStatus  int
		expectError   bool
		errorContains string
	}{
		{
			name:         "successful deletion",
			shareName:    "s3bench_alien-public",
			serverStatus: http.StatusOK,
			expectError:  false,
		},
		{
			name:         "not found is idempotent",
			shareName:    "nonexistent-share",
			serverStatus: http.StatusNotFound,
			expectError:  false,
		},
		{
			name:          "server error",
			shareName:     "test-share",
			serverStatus:  http.StatusInternalServerError,
			expectError:   true,
			errorContains: "500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					t.Errorf("Expected DELETE, got %s", r.Method)
				}
				if !strings.Contains(r.URL.Path, "/api/shares/") {
					t.Errorf("Expected /api/shares/ in path, got %s", r.URL.Path)
				}

				w.WriteHeader(tt.serverStatus)
			}))
			defer server.Close()

			client := &CoordinatorClient{
				baseURL:    server.URL,
				httpClient: server.Client(),
				accessKey:  "test-access",
				secretKey:  "test-secret",
			}

			err := client.DeleteShare(context.Background(), tt.shareName)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}

func TestCoordinatorClient_TriggerGC(t *testing.T) {
	newGCServer := func(t *testing.T, status int, response string) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("Expected POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/s3/gc" {
				t.Errorf("Expected /api/s3/gc, got %s", r.URL.Path)
			}
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), "purge_recycle_bin") {
				t.Errorf("Expected purge_recycle_bin in body, got %s", string(body))
			}
			w.WriteHeader(status)
			if response != "" {
				_, _ = w.Write([]byte(response))
			}
		}))
	}

	t.Run("successful GC", func(t *testing.T) {
		server := newGCServer(t, http.StatusOK, `{"recycled_purged":5,"versions_pruned":3,"chunks_deleted":10,"bytes_reclaimed":1048576}`)
		defer server.Close()

		client := &CoordinatorClient{baseURL: server.URL, httpClient: server.Client(), accessKey: "test", secretKey: "test"}
		stats, err := client.TriggerGC(context.Background(), true)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if stats.RecycledPurged != 5 || stats.VersionsPruned != 3 || stats.ChunksDeleted != 10 || stats.BytesReclaimed != 1048576 {
			t.Errorf("Unexpected stats: %+v", stats)
		}
	})

	t.Run("server error", func(t *testing.T) {
		server := newGCServer(t, http.StatusInternalServerError, "")
		defer server.Close()

		client := &CoordinatorClient{baseURL: server.URL, httpClient: server.Client(), accessKey: "test", secretKey: "test"}
		_, err := client.TriggerGC(context.Background(), true)
		if err == nil || !strings.Contains(err.Error(), "500") {
			t.Errorf("Expected error containing '500', got: %v", err)
		}
	})

	t.Run("retries on 429 then succeeds", func(t *testing.T) {
		// Simulate periodic GC holding the lock: first two requests return 429,
		// third succeeds. TriggerGC must retry transparently.
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := calls.Add(1)
			if n <= 2 {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"recycled_purged":1,"versions_pruned":0,"chunks_deleted":0,"bytes_reclaimed":0}`))
		}))
		defer server.Close()

		client := &CoordinatorClient{
			baseURL:      server.URL,
			httpClient:   server.Client(),
			accessKey:    "test",
			secretKey:    "test",
			gcRetryDelay: time.Millisecond, // no real delay in tests
		}
		stats, err := client.TriggerGC(context.Background(), true)
		if err != nil {
			t.Fatalf("Expected success after retries, got: %v", err)
		}
		if stats.RecycledPurged != 1 {
			t.Errorf("Expected RecycledPurged=1, got %d", stats.RecycledPurged)
		}
		if calls.Load() != 3 {
			t.Errorf("Expected 3 attempts (2×429 + 1×200), got %d", calls.Load())
		}
	})

	t.Run("exhausts retries and returns error", func(t *testing.T) {
		// Server always returns 429 — TriggerGC must give up after maxRetries.
		server := newGCServer(t, http.StatusTooManyRequests, "")
		defer server.Close()

		client := &CoordinatorClient{baseURL: server.URL, httpClient: server.Client(), accessKey: "test", secretKey: "test"}
		// Use a context that times out quickly so the retry delays don't block the test.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := client.TriggerGC(ctx, true)
		if err == nil {
			t.Fatal("Expected error when all retries exhausted")
		}
	})
}

func TestCoordinatorClient_TriggerGC_SlowServer(t *testing.T) {
	// Verify that TriggerGC tolerates responses slower than the default 60s client timeout.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // Simulate slow GC
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"recycled_purged":1,"versions_pruned":0,"chunks_deleted":0,"bytes_reclaimed":0}`))
	}))
	defer server.Close()

	client := &CoordinatorClient{
		baseURL:   server.URL,
		accessKey: "test",
		secretKey: "test",
		httpClient: &http.Client{
			Timeout:   1 * time.Second, // Shorter than the server delay
			Transport: server.Client().Transport,
		},
	}

	stats, err := client.TriggerGC(context.Background(), true)
	if err != nil {
		t.Fatalf("GC should not timeout with context-based timeout: %v", err)
	}
	if stats.RecycledPurged != 1 {
		t.Errorf("Expected RecycledPurged=1, got %d", stats.RecycledPurged)
	}
}

func TestCoordinatorClient_TriggerGC_ContextCancellation(t *testing.T) {
	// Verify that TriggerGC respects parent context cancellation.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &CoordinatorClient{
		baseURL:    server.URL,
		accessKey:  "test",
		secretKey:  "test",
		httpClient: server.Client(),
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := client.TriggerGC(ctx, true)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Expected error from cancelled context")
	}
	if elapsed > 1*time.Second {
		t.Errorf("Expected cancellation within ~200ms, took %v", elapsed)
	}
}

func TestCoordinatorClient_URLEscaping(t *testing.T) {
	// Test that special characters in bucket/key are properly escaped
	var capturedRawPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// RawPath contains the encoded form, Path contains decoded form
		// RawPath is only set if encoding was necessary
		if r.URL.RawPath != "" {
			capturedRawPath = r.URL.RawPath
		} else {
			capturedRawPath = r.URL.Path
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &CoordinatorClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		accessKey:  "test",
		secretKey:  "test",
	}

	// Test with spaces and special characters
	bucket := "my-bucket"
	key := "folder/file with spaces.txt"

	_ = client.PutObject(context.Background(), bucket, key, []byte("test"), "text/plain", nil)

	// URL should have slashes and spaces escaped
	expectedPath := fmt.Sprintf("/api/s3/buckets/%s/objects/%s", bucket, "folder%2Ffile%20with%20spaces.txt")
	if capturedRawPath != expectedPath {
		t.Errorf("Expected URL path %q, got %q", expectedPath, capturedRawPath)
	}
}

func TestDeregisterPeer(t *testing.T) {
	tests := []struct {
		name          string
		peerName      string
		serverStatus  int
		expectError   bool
		errorContains string
	}{
		{
			name:         "successful deregistration",
			peerName:     "s3bench",
			serverStatus: http.StatusOK,
			expectError:  false,
		},
		{
			name:         "204 No Content treated as success",
			peerName:     "s3bench",
			serverStatus: http.StatusNoContent,
			expectError:  false,
		},
		{
			name:         "404 Not Found is idempotent (success)",
			peerName:     "already-gone",
			serverStatus: http.StatusNotFound,
			expectError:  false,
		},
		{
			name:          "server error returns error",
			peerName:      "s3bench",
			serverStatus:  http.StatusInternalServerError,
			expectError:   true,
			errorContains: "500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedPath string
			var capturedMethod string
			var capturedAuth string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedPath = r.URL.Path
				capturedMethod = r.Method
				capturedAuth = r.Header.Get("Authorization")
				w.WriteHeader(tt.serverStatus)
			}))
			defer server.Close()

			// publicBaseURL points to the mock server; bearerToken used for auth.
			client := &CoordinatorClient{
				baseURL:       "https://10.0.0.1:443", // admin mux — NOT used by DeregisterPeer
				publicBaseURL: server.URL,
				bearerToken:   "test-bearer-token",
				httpClient:    server.Client(),
				accessKey:     "test-access",
				secretKey:     "test-secret",
			}

			err := client.DeregisterPeer(context.Background(), tt.peerName)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}

			// Verify request used correct path, method, and Bearer auth.
			if capturedMethod != "" {
				if capturedMethod != http.MethodDelete {
					t.Errorf("Expected DELETE method, got %s", capturedMethod)
				}
				expectedPath := "/api/v1/peers/" + tt.peerName
				if capturedPath != expectedPath {
					t.Errorf("Expected path %q, got %q", expectedPath, capturedPath)
				}
				if capturedAuth != "Bearer test-bearer-token" {
					t.Errorf("Expected Bearer token auth, got %q", capturedAuth)
				}
			}
		})
	}
}

func TestDeregisterPeer_NoPubicURL(t *testing.T) {
	// If publicBaseURL is not set, DeregisterPeer should return an error immediately.
	client := &CoordinatorClient{
		baseURL:    "https://10.0.0.1:443",
		httpClient: &http.Client{},
		accessKey:  "test-access",
		secretKey:  "test-secret",
	}
	err := client.DeregisterPeer(context.Background(), "s3bench")
	if err == nil || !strings.Contains(err.Error(), "public base URL") {
		t.Errorf("Expected error about missing public URL, got: %v", err)
	}
}
