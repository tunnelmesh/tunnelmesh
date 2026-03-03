package mesh

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
)

// MeshInfo contains mesh connectivity information returned by registration.
type MeshInfo struct {
	MeshIP       string   // This peer's assigned mesh IP
	PeerID       string   // Peer ID for RBAC
	PeerName     string   // Assigned peer name (may differ if renamed)
	IsAdmin      bool     // Whether peer has admin access
	CoordMeshIPs []string // Coordinator mesh IPs for admin mux access
}

// RegisterWithCoordinator registers s3bench as a peer with the coordinator.
// Returns mesh connectivity information needed for S3 API access.
func RegisterWithCoordinator(ctx context.Context, coordinatorURL string, creds *Credentials, insecureSkipVerify bool, authToken string) (*MeshInfo, error) {
	// Get local IPs for registration
	publicIPs, privateIPs, behindNAT := proto.GetLocalIPs()

	// Build registration request
	req := proto.RegisterRequest{
		Name:          "s3bench",
		PublicKey:     creds.PublicKey,
		PublicIPs:     publicIPs,
		PrivateIPs:    privateIPs,
		SSHPort:       0, // Not listening
		UDPPort:       0, // Not listening
		BehindNAT:     behindNAT,
		Version:       "s3bench-dev",
		IsCoordinator: false,
	}

	// Marshal request
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal registration request: %w", err)
	}

	// Create HTTP client with TLS config
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecureSkipVerify,
			},
			IdleConnTimeout: 30 * time.Second,
		},
	}

	// Make registration request
	url := fmt.Sprintf("%s/api/v1/register", coordinatorURL)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create HTTP request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Add auth token if provided
	if authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+authToken)
	}

	// Execute request
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("coordinator unreachable at %s: %w", coordinatorURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		var errResp proto.ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return nil, fmt.Errorf("registration failed with status %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("registration failed: %s", errResp.Error)
	}

	// Parse response
	var regResp proto.RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return nil, fmt.Errorf("decode registration response: %w", err)
	}

	meshInfo := &MeshInfo{
		MeshIP:       regResp.MeshIP,
		PeerID:       regResp.PeerID,
		PeerName:     regResp.PeerName,
		IsAdmin:      regResp.IsAdmin,
		CoordMeshIPs: regResp.CoordMeshIPs,
	}

	return meshInfo, nil
}
