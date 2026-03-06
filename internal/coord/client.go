package coord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
)

// ErrPeerNotFound is returned when the peer is not registered with the server.
var ErrPeerNotFound = errors.New("peer not found")

// ErrUnauthorized is returned when the server rejects authentication (e.g., expired JWT).
var ErrUnauthorized = errors.New("peer unauthorized")

// Client is a client for the coordination server.
type Client struct {
	baseURL   string
	authToken string
	jwtToken  string // JWT token for relay authentication
	client    *http.Client
}

// NewClient creates a new coordination client.
func NewClient(baseURL, authToken string) *Client {
	return &Client{
		baseURL:   baseURL,
		authToken: authToken,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Register registers this peer with the coordination server.
// The location parameter is optional and can be nil.
func (c *Client) Register(name, publicKey string, publicIPs, privateIPs []string, sshPort, udpPort int, behindNAT bool, version string, location *proto.GeoLocation, exitNode string, allowsExitTraffic bool, aliases []string, isCoordinator bool, hasMonitoring bool) (*proto.RegisterResponse, error) {
	req := proto.RegisterRequest{
		Name:              name,
		PublicKey:         publicKey,
		PublicIPs:         publicIPs,
		PrivateIPs:        privateIPs,
		SSHPort:           sshPort,
		UDPPort:           udpPort,
		BehindNAT:         behindNAT,
		Version:           version,
		Location:          location,
		ExitPeer:          exitNode,
		AllowsExitTraffic: allowsExitTraffic,
		Aliases:           aliases,
		IsCoordinator:     isCoordinator,
		HasMonitoring:     hasMonitoring,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	resp, err := c.doRequestWithContext(context.Background(), http.MethodPost, "/api/v1/register", body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseError(resp)
	}

	var result proto.RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Store JWT token for relay authentication
	c.jwtToken = result.Token

	return &result, nil
}

// RetryConfig configures the retry behavior for registration.
type RetryConfig struct {
	MaxRetries     int           // Maximum number of retry attempts (default: 10)
	InitialBackoff time.Duration // Initial backoff duration (default: 2s)
	MaxBackoff     time.Duration // Maximum backoff duration (default: 60s)
}

// DefaultRetryConfig returns the default retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:     10,
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     60 * time.Second,
	}
}

// RegisterWithRetry registers this peer with exponential backoff retry.
// It will retry up to MaxRetries times if registration fails.
// The location parameter is optional and can be nil.
func (c *Client) RegisterWithRetry(ctx context.Context, name, publicKey string, publicIPs, privateIPs []string, sshPort, udpPort int, behindNAT bool, version string, location *proto.GeoLocation, exitNode string, allowsExitTraffic bool, aliases []string, isCoordinator bool, hasMonitoring bool, cfg RetryConfig) (*proto.RegisterResponse, error) {
	if cfg.MaxRetries == 0 {
		cfg = DefaultRetryConfig()
	}

	backoff := cfg.InitialBackoff
	var lastErr error

	for attempt := 1; attempt <= cfg.MaxRetries; attempt++ {
		resp, err := c.Register(name, publicKey, publicIPs, privateIPs, sshPort, udpPort, behindNAT, version, location, exitNode, allowsExitTraffic, aliases, isCoordinator, hasMonitoring)
		if err == nil {
			return resp, nil
		}

		lastErr = err

		if attempt == cfg.MaxRetries {
			break
		}

		log.Warn().
			Err(err).
			Int("attempt", attempt).
			Int("max_attempts", cfg.MaxRetries).
			Dur("retry_in", backoff).
			Msg("failed to register with server, retrying...")

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("registration cancelled after %d attempts: %w", attempt, ctx.Err())
		case <-time.After(backoff):
		}

		// Exponential backoff with cap
		backoff *= 2
		if backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}

	return nil, fmt.Errorf("register with server after %d attempts: %w", cfg.MaxRetries, lastErr)
}

// ListPeers returns a list of all registered peers.
func (c *Client) ListPeers() ([]proto.Peer, error) {
	resp, err := c.doRequestWithContext(context.Background(), http.MethodGet, "/api/v1/peers", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseError(resp)
	}

	var result proto.PeerListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return result.Peers, nil
}

// Deregister removes this peer from the mesh.
func (c *Client) Deregister(name string) error {
	resp, err := c.doRequestWithContext(context.Background(), http.MethodDelete, "/api/v1/peers/"+name, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.parseError(resp)
	}

	return nil
}

// GetDNSRecords returns the current DNS records.
func (c *Client) GetDNSRecords() ([]proto.DNSRecord, error) {
	resp, err := c.doRequestWithContext(context.Background(), http.MethodGet, "/api/v1/dns", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseError(resp)
	}

	var result proto.DNSUpdateNotification
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return result.Records, nil
}

func (c *Client) doRequestWithContext(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.authToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	tracing.InjectHeaders(ctx, req.Header)

	return c.client.Do(req)
}

func (c *Client) parseError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)

	var errResp proto.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Message != "" {
		return fmt.Errorf("%s: %s", errResp.Error, errResp.Message)
	}

	return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
}

// JWTToken returns the JWT token received during registration.
// This token is used for relay authentication.
func (c *Client) JWTToken() string {
	return c.jwtToken
}

// BaseURL returns the base URL of the coordination server.
func (c *Client) BaseURL() string {
	return c.baseURL
}

// CloseIdleConnections closes any idle connections in the HTTP client pool.
// This should be called after network changes to prevent stale connections.
func (c *Client) CloseIdleConnections() {
	c.client.CloseIdleConnections()
}

// CheckRelayRequests checks if any peers are waiting on relay for us.
// Returns nil if the server doesn't support this endpoint (404).
func (c *Client) CheckRelayRequests() ([]string, error) {
	if c.jwtToken == "" {
		return nil, nil // No JWT token yet, can't check relay status
	}

	resp, err := c.doRequestWithJWTContext(context.Background(), http.MethodGet, "/api/v1/relay-status", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // Server doesn't support this endpoint
	}
	if resp.StatusCode != http.StatusOK {
		return nil, c.parseError(resp)
	}

	var result proto.RelayStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return result.RelayRequests, nil
}

func (c *Client) doRequestWithJWTContext(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.jwtToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	tracing.InjectHeaders(ctx, req.Header)

	return c.client.Do(req)
}
