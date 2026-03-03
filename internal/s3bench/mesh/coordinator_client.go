package mesh

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

// CoordinatorClient provides HTTP access to the coordinator's S3 API.
type CoordinatorClient struct {
	baseURL      string // Base URL for coordinator (https://10.42.0.1:443)
	accessKey    string
	secretKey    string
	httpClient   *http.Client
	gcRetryDelay time.Duration // delay between GC 429 retries; 0 means use default (15s)
}

// NewCoordinatorClient creates a new coordinator S3 API client.
// baseURL should be the coordinator's URL (e.g., "http://localhost:8081").
func NewCoordinatorClient(baseURL string, creds *Credentials, insecureSkipVerify bool) *CoordinatorClient {
	return &CoordinatorClient{
		baseURL:   strings.TrimRight(baseURL, "/"),
		accessKey: creds.AccessKey,
		secretKey: creds.SecretKey,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: insecureSkipVerify,
				},
				IdleConnTimeout:     90 * time.Second,
				MaxIdleConnsPerHost: 10,
			},
		},
	}
}

// CreateBucket creates a new S3 bucket on the coordinator.
func (c *CoordinatorClient) CreateBucket(ctx context.Context, bucketName, ownerID string, quotaMB int64) error {
	requestURL := fmt.Sprintf("%s/api/s3/buckets", c.baseURL)

	// Build request payload
	payload := map[string]interface{}{
		"name": bucketName,
	}
	if ownerID != "" {
		payload["owner_id"] = ownerID
	}
	if quotaMB > 0 {
		payload["quota_bytes"] = quotaMB * 1024 * 1024
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	c.setBasicAuth(req)

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP POST %s: %w", requestURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check response
	if resp.StatusCode == http.StatusConflict {
		// Bucket already exists - this is fine (idempotent)
		return nil
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create bucket failed: %d %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// ShareResponse is the response from creating a file share.
type ShareResponse struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Owner       string `json:"owner"`
	GuestRead   bool   `json:"guest_read"`
}

// CreateShare creates a file share on the coordinator via /api/shares.
// The coordinator auto-prefixes the share name with the peer name.
// Returns the actual share name (with prefix) so callers can compute the bucket name.
func (c *CoordinatorClient) CreateShare(ctx context.Context, name, description string, quotaMB int64) (string, error) {
	requestURL := fmt.Sprintf("%s/api/shares", c.baseURL)

	payload := map[string]interface{}{
		"name":        name,
		"description": description,
	}
	if quotaMB > 0 {
		payload["quota_bytes"] = quotaMB * 1024 * 1024
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.setBasicAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP POST %s: %w", requestURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusConflict {
		// Share already exists — return expected name (caller should handle)
		return "", nil
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("create share failed: %d %s", resp.StatusCode, string(bodyBytes))
	}

	var shareResp ShareResponse
	if err := json.NewDecoder(resp.Body).Decode(&shareResp); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}

	return shareResp.Name, nil
}

// BucketExists checks if a bucket exists on the coordinator.
func (c *CoordinatorClient) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	requestURL := fmt.Sprintf("%s/api/s3/buckets/%s", c.baseURL, url.PathEscape(bucketName))

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, requestURL, nil)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}
	c.setBasicAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("HTTP HEAD %s: %w", requestURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	return false, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

// PutObject uploads an object to the coordinator S3 API.
func (c *CoordinatorClient) PutObject(ctx context.Context, bucket, key string, body []byte, contentType string, metadata map[string]string) error {
	requestURL := fmt.Sprintf("%s/api/s3/buckets/%s/objects/%s", c.baseURL, url.PathEscape(bucket), url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	c.setBasicAuth(req)

	// Add metadata as x-amz-meta- headers
	for k, v := range metadata {
		req.Header.Set("x-amz-meta-"+k, v)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP PUT %s: %w", requestURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check response
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put object failed: %d %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// DeleteObject deletes an object from the coordinator S3 API.
func (c *CoordinatorClient) DeleteObject(ctx context.Context, bucket, key string) error {
	requestURL := fmt.Sprintf("%s/api/s3/buckets/%s/objects/%s", c.baseURL, url.PathEscape(bucket), url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, requestURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setBasicAuth(req)

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP DELETE %s: %w", requestURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check response
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("object not found")
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete object failed: %d %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// GetObject downloads an object from the coordinator S3 API.
func (c *CoordinatorClient) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	requestURL := fmt.Sprintf("%s/api/s3/buckets/%s/objects/%s", c.baseURL, url.PathEscape(bucket), url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.setBasicAuth(req)

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s: %w", requestURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check response
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("get object failed: %d %s", resp.StatusCode, string(bodyBytes))
	}

	// Read response body
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return data, nil
}

// DeleteShare deletes a file share on the coordinator via DELETE /api/shares/{name}.
// Treats 404 as success (idempotent).
func (c *CoordinatorClient) DeleteShare(ctx context.Context, name string) error {
	requestURL := fmt.Sprintf("%s/api/shares/%s", c.baseURL, url.PathEscape(name))

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, requestURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setBasicAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP DELETE %s: %w", requestURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil // Idempotent: already deleted
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete share failed: %d %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// GCStats represents the response from the S3 GC endpoint.
type GCStats struct {
	RecycledPurged int   `json:"recycled_purged"`
	VersionsPruned int   `json:"versions_pruned"`
	ChunksDeleted  int   `json:"chunks_deleted"`
	BytesReclaimed int64 `json:"bytes_reclaimed"`
}

// TriggerGC triggers on-demand garbage collection on the coordinator via POST /api/s3/gc.
// Uses a 10-minute context timeout since GC can be slow under sustained load.
// Retries on HTTP 429 (GC already in progress) since the periodic GC may briefly hold
// the server-side lock; accordion mode would otherwise fail hard on the race.
func (c *CoordinatorClient) TriggerGC(ctx context.Context, purgeRecycleBin bool) (*GCStats, error) {
	// GC can take several minutes under sustained load; use a longer timeout
	// than the default 60-second client. Context-based timeout respects parent
	// cancellation (e.g., user interrupt) unlike http.Client.Timeout.
	gcCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	requestURL := fmt.Sprintf("%s/api/s3/gc", c.baseURL)

	payload := map[string]interface{}{
		"purge_recycle_bin": purgeRecycleBin,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	// Use a client without its own Timeout so the context controls cancellation.
	gcClient := &http.Client{Transport: c.httpClient.Transport}

	const maxRetries = 6
	retryDelay := c.gcRetryDelay
	if retryDelay == 0 {
		retryDelay = 15 * time.Second
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-gcCtx.Done():
				return nil, fmt.Errorf("trigger GC: %w", gcCtx.Err())
			case <-time.After(retryDelay):
			}
		}

		req, err := http.NewRequestWithContext(gcCtx, http.MethodPost, requestURL, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		c.setBasicAuth(req)

		resp, err := gcClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("HTTP POST %s: %w", requestURL, err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			continue
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return nil, fmt.Errorf("trigger GC failed: %d %s", resp.StatusCode, string(bodyBytes))
		}

		var stats GCStats
		if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("decode response: %w", err)
		}
		_ = resp.Body.Close()
		return &stats, nil
	}

	return nil, fmt.Errorf("trigger GC: server busy after %d retries", maxRetries)
}

// setBasicAuth sets HTTP Basic Auth header using S3 credentials.
func (c *CoordinatorClient) setBasicAuth(req *http.Request) {
	auth := c.accessKey + ":" + c.secretKey
	encoded := base64.StdEncoding.EncodeToString([]byte(auth))
	req.Header.Set("Authorization", "Basic "+encoded)
}

// ListBuckets returns all buckets accessible to the authenticated user.
func (c *CoordinatorClient) ListBuckets(ctx context.Context) ([]s3.BucketMeta, error) {
	url := fmt.Sprintf("%s/api/s3/buckets", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.setBasicAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list buckets failed: %d %s", resp.StatusCode, string(bodyBytes))
	}

	var buckets []s3.BucketMeta
	if err := json.NewDecoder(resp.Body).Decode(&buckets); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return buckets, nil
}
