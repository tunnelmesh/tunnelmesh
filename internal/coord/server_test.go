package coord

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	s3 "github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
	"github.com/tunnelmesh/tunnelmesh/pkg/bytesize"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
	"golang.org/x/crypto/ssh"
)

// newTestConfig creates a test configuration with proper S3 setup
func newTestConfig(t *testing.T) *config.PeerConfig {
	tmpDir := t.TempDir()

	return &config.PeerConfig{
		Name:      "test-coord",
		Servers:   []string{"http://localhost:8080"},
		AuthToken: "test-token",
		TUN: config.TUNConfig{
			MTU: 1400,
		},
		Coordinator: config.CoordinatorConfig{
			Listen: ":0",
			S3: config.S3Config{
				DataDir: tmpDir,
				MaxSize: bytesize.Size(1 << 30), // 1Gi for tests
			},
		},
	}
}

// cleanupServer shuts down a server with a timeout to prevent hanging tests.
// The server's WaitGroup ensures all background goroutines complete before returning.
func cleanupServer(t *testing.T, srv *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

	// Force garbage collection and give Windows time to close file handles.
	// Windows can keep file handles open briefly even after Close() returns.
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
}

func newTestServer(t *testing.T) *Server {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	return srv
}

func TestServer_Health(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}

func TestServer_Register_Success(t *testing.T) {
	srv := newTestServer(t)

	regReq := proto.RegisterRequest{
		Name:       "testnode",
		PublicKey:  "SHA256:abc123",
		PublicIPs:  []string{"1.2.3.4"},
		PrivateIPs: []string{"192.168.1.100"},
		SSHPort:    2222,
	}
	body, _ := json.Marshal(regReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp proto.RegisterResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.NotEmpty(t, resp.MeshIP)
	assert.Equal(t, "10.42.0.0/16", resp.MeshCIDR)
	assert.Equal(t, ".tunnelmesh", resp.Domain)
	assert.NotEmpty(t, resp.Token, "should return JWT token for relay auth")

	// Verify token is valid
	claims, err := srv.ValidateToken(resp.Token)
	require.NoError(t, err)
	assert.Equal(t, "testnode", claims.PeerName)
	assert.Equal(t, resp.MeshIP, claims.MeshIP)
}

func TestServer_Register_Unauthorized(t *testing.T) {
	srv := newTestServer(t)

	regReq := proto.RegisterRequest{
		Name:      "testnode",
		PublicKey: "SHA256:abc123",
	}
	body, _ := json.Marshal(regReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	// No Authorization header
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestServer_Register_InvalidToken(t *testing.T) {
	srv := newTestServer(t)

	regReq := proto.RegisterRequest{
		Name:      "testnode",
		PublicKey: "SHA256:abc123",
	}
	body, _ := json.Marshal(regReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer wrong-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestServer_Register_DuplicateName(t *testing.T) {
	srv := newTestServer(t)

	regReq := proto.RegisterRequest{
		Name:      "testnode",
		PublicKey: "SHA256:abc123",
		SSHPort:   2222,
	}
	body, _ := json.Marshal(regReq)

	// First registration
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Second registration with same name - should update
	body, _ = json.Marshal(regReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestServer_Register_ReservedName(t *testing.T) {
	srv := newTestServer(t)

	reserved := []string{"admin", "administrator", "super", "supervisor", "peers", "api", "prometheus", "grafana", "health", "assets", "static", "Admin", "ADMINISTRATOR", "Peers", "Prometheus"}
	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			regReq := proto.RegisterRequest{
				Name:      name,
				PublicKey: "SHA256:abc123",
				SSHPort:   2222,
			}
			body, _ := json.Marshal(regReq)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer test-token")
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)

			assert.Equal(t, http.StatusForbidden, w.Code)
			assert.Contains(t, w.Body.String(), "reserved")
		})
	}
}

func TestServer_Peers(t *testing.T) {
	srv := newTestServer(t)

	// Register a peer first
	regReq := proto.RegisterRequest{
		Name:       "node1",
		PublicKey:  "SHA256:abc123",
		PublicIPs:  []string{"1.2.3.4"},
		PrivateIPs: []string{"192.168.1.100"},
		SSHPort:    2222,
	}
	body, _ := json.Marshal(regReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// List peers
	req = httptest.NewRequest(http.MethodGet, "/api/v1/peers", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp proto.PeerListResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Len(t, resp.Peers, 1)
	assert.Equal(t, "node1", resp.Peers[0].Name)
}

// Note: TestServer_Heartbeat removed - HTTP heartbeat endpoint replaced by WebSocket
// See relay_test.go for WebSocket heartbeat tests

func TestServer_Deregister(t *testing.T) {
	srv := newTestServer(t)

	// Register first
	regReq := proto.RegisterRequest{
		Name:      "testnode",
		PublicKey: "SHA256:abc123",
		SSHPort:   2222,
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Verify peer exists
	req = httptest.NewRequest(http.MethodGet, "/api/v1/peers", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	var listResp proto.PeerListResponse
	_ = json.Unmarshal(w.Body.Bytes(), &listResp)
	assert.Len(t, listResp.Peers, 1)

	// Deregister
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/peers/testnode", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Verify peer is gone
	req = httptest.NewRequest(http.MethodGet, "/api/v1/peers", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	_ = json.Unmarshal(w.Body.Bytes(), &listResp)
	assert.Len(t, listResp.Peers, 0)
}

func TestServer_IPAllocation(t *testing.T) {
	srv := newTestServer(t)

	// Register multiple peers and verify unique IPs
	ips := make(map[string]bool)

	for i := 0; i < 5; i++ {
		regReq := proto.RegisterRequest{
			Name:      "node" + string(rune('A'+i)),
			PublicKey: "SHA256:key" + string(rune('A'+i)),
			SSHPort:   2222,
		}
		body, _ := json.Marshal(regReq)

		req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var resp proto.RegisterResponse
		_ = json.Unmarshal(w.Body.Bytes(), &resp)

		// Verify IP is unique
		assert.False(t, ips[resp.MeshIP], "IP should be unique: %s", resp.MeshIP)
		ips[resp.MeshIP] = true

		// Verify IP is in mesh range
		assert.Contains(t, resp.MeshIP, "10.42.")
	}
}

func TestServer_AdminOverview_NoPeers(t *testing.T) {
	srv := newTestServer(t)
	require.NotNil(t, srv.adminMux, "adminMux should be created when JoinMesh is configured")

	// Admin is now mesh-internal only, test via adminMux
	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	w := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp AdminOverview
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, 0, resp.TotalPeers)
	assert.Equal(t, 0, resp.OnlinePeers)
	assert.Equal(t, "10.42.0.0/16", resp.MeshCIDR)
	assert.Empty(t, resp.Peers)
}

func TestServer_AdminOverview_WithPeers(t *testing.T) {
	srv := newTestServer(t)
	require.NotNil(t, srv.adminMux, "adminMux should be created when JoinMesh is configured")

	// Register a peer first
	regReq := proto.RegisterRequest{
		Name:       "node1",
		PublicKey:  "SHA256:abc123",
		PublicIPs:  []string{"1.2.3.4"},
		PrivateIPs: []string{"192.168.1.100"},
		SSHPort:    2222,
	}
	body, _ := json.Marshal(regReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Get admin overview (mesh-internal only via adminMux)
	req = httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	w = httptest.NewRecorder()
	srv.adminMux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp AdminOverview
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, 1, resp.TotalPeers)
	assert.Equal(t, 1, resp.OnlinePeers)
	assert.Len(t, resp.Peers, 1)
	assert.Equal(t, "node1", resp.Peers[0].Name)
	assert.True(t, resp.Peers[0].Online)
}

// Note: TestServer_HeartbeatWithStats removed - HTTP heartbeat endpoint replaced by WebSocket
// See relay_test.go for WebSocket heartbeat tests

func TestServer_AdminStaticFiles(t *testing.T) {
	srv := newTestServer(t)
	require.NotNil(t, srv.adminMux, "adminMux should be created when JoinMesh is configured")

	// Admin dashboard served at /admin/ prefix
	// Test index.html
	req := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	w := httptest.NewRecorder()
	srv.adminMux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "<title>TunnelMesh</title>")

	// Test CSS
	req = httptest.NewRequest(http.MethodGet, "/admin/css/style.css", nil)
	w = httptest.NewRecorder()
	srv.adminMux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Test JS
	req = httptest.NewRequest(http.MethodGet, "/admin/js/app.js", nil)
	w = httptest.NewRecorder()
	srv.adminMux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Test root serves landing page
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	w = httptest.NewRecorder()
	srv.adminMux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "secured mesh network")
}

func TestServer_HolePunchBidirectionalCoordination(t *testing.T) {
	srv := newTestServer(t)

	// Register two peers
	for _, name := range []string{"peerA", "peerB"} {
		regReq := proto.RegisterRequest{
			Name:       name,
			PublicKey:  "SHA256:" + name,
			PublicIPs:  []string{"1.2.3.4"},
			PrivateIPs: []string{"192.168.1.100"},
			SSHPort:    2222,
			UDPPort:    2223,
		}
		body, _ := json.Marshal(regReq)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
	}

	// Register UDP endpoints for both peers
	for _, peerData := range []struct {
		name         string
		externalAddr string
	}{
		{"peerA", "1.1.1.1:5000"},
		{"peerB", "2.2.2.2:5001"},
	} {
		udpReq := RegisterUDPRequest{
			PeerName:  peerData.name,
			LocalAddr: "0.0.0.0:51820",
			UDPPort:   51820,
		}
		body, _ := json.Marshal(udpReq)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/udp/register", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		colonIdx := strings.Index(peerData.externalAddr, ":")
		if colonIdx > 0 {
			req.Header.Set("X-Real-IP", peerData.externalAddr[:colonIdx])
		}
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
	}

	// PeerA initiates hole-punch to PeerB
	holePunchReq := HolePunchRequest{
		FromPeer:     "peerA",
		ToPeer:       "peerB",
		LocalAddr:    "0.0.0.0:51820",
		ExternalAddr: "1.1.1.1:5000",
	}
	body, _ := json.Marshal(holePunchReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/udp/holepunch", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var holePunchResp HolePunchResponse
	err := json.Unmarshal(w.Body.Bytes(), &holePunchResp)
	require.NoError(t, err)
	assert.True(t, holePunchResp.OK)
	assert.True(t, holePunchResp.Ready)

	// Note: Hole-punch notifications are now delivered via WebSocket push instead of HTTP heartbeat.
	// See relay_test.go TestRelayManager_NotifyHolePunch for WebSocket notification tests.
}

func TestServer_DualStackUDPEndpointRegistration(t *testing.T) {
	srv := newTestServer(t)

	// Register a peer
	regReq := proto.RegisterRequest{
		Name:       "dual-stack-peer",
		PublicKey:  "SHA256:dual-stack-peer",
		PublicIPs:  []string{"1.2.3.4"},
		PrivateIPs: []string{"192.168.1.100"},
		SSHPort:    2222,
		UDPPort:    2223,
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Register IPv4 UDP endpoint
	udpReq := RegisterUDPRequest{
		PeerName:  "dual-stack-peer",
		LocalAddr: "0.0.0.0:51820",
		UDPPort:   51820,
	}
	body, _ = json.Marshal(udpReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/udp/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Real-IP", "203.0.113.50") // IPv4 address
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Register IPv6 UDP endpoint (same peer, different address family)
	body, _ = json.Marshal(udpReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/udp/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Real-IP", "2001:db8::1") // IPv6 address
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Get the endpoint and verify both addresses are stored
	req = httptest.NewRequest(http.MethodGet, "/api/v1/udp/endpoint/dual-stack-peer", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var endpoint UDPEndpoint
	err := json.Unmarshal(w.Body.Bytes(), &endpoint)
	require.NoError(t, err)

	assert.Equal(t, "dual-stack-peer", endpoint.PeerName)
	assert.Equal(t, "203.0.113.50:51820", endpoint.ExternalAddr4, "IPv4 address should be stored in ExternalAddr4")
	assert.Equal(t, "[2001:db8::1]:51820", endpoint.ExternalAddr6, "IPv6 address should be stored in ExternalAddr6")
}

func TestServer_IPv4OnlyEndpointRegistration(t *testing.T) {
	srv := newTestServer(t)

	// Register a peer
	regReq := proto.RegisterRequest{
		Name:       "ipv4-only-peer",
		PublicKey:  "SHA256:ipv4-only-peer",
		PublicIPs:  []string{"1.2.3.4"},
		PrivateIPs: []string{"192.168.1.100"},
		SSHPort:    2222,
		UDPPort:    2223,
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Register only IPv4 UDP endpoint
	udpReq := RegisterUDPRequest{
		PeerName:  "ipv4-only-peer",
		LocalAddr: "0.0.0.0:51820",
		UDPPort:   51820,
	}
	body, _ = json.Marshal(udpReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/udp/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Real-IP", "198.51.100.25") // IPv4 only
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Get the endpoint and verify only IPv4 is stored
	req = httptest.NewRequest(http.MethodGet, "/api/v1/udp/endpoint/ipv4-only-peer", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var endpoint UDPEndpoint
	err := json.Unmarshal(w.Body.Bytes(), &endpoint)
	require.NoError(t, err)

	assert.Equal(t, "198.51.100.25:51820", endpoint.ExternalAddr4, "IPv4 address should be stored")
	assert.Empty(t, endpoint.ExternalAddr6, "IPv6 address should be empty")
}

// newTestServerWithWireGuard creates a test server with WireGuard enabled.
func newTestServerWithWireGuard(t *testing.T) *Server {
	cfg := newTestConfig(t)
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.WireGuardServer = config.WireGuardServerConfig{
		Endpoint: "wg.example.com:51820",
	}

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	return srv
}

func TestServer_WireGuardDNSIntegration(t *testing.T) {
	// Skip: WireGuard client management has moved to the concentrator peer.
	// The coordinator now proxies API requests to the concentrator via relay.
	// This test would need a mock concentrator to function.
	t.Skip("WireGuard client management now proxied to concentrator")

	srv := newTestServerWithWireGuard(t)

	// Create a WireGuard client
	createReq := map[string]string{"name": "iPhone"}
	body, _ := json.Marshal(createReq)
	req := httptest.NewRequest(http.MethodPost, "/admin/api/wireguard/clients", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)

	// Parse response to get client ID and DNS name
	var createResp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &createResp)
	require.NoError(t, err)

	client := createResp["client"].(map[string]interface{})
	clientID := client["id"].(string)
	dnsName := client["dns_name"].(string)
	meshIP := client["mesh_ip"].(string)

	// Verify client appears in DNS records
	req = httptest.NewRequest(http.MethodGet, "/api/v1/dns", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var dnsResp proto.DNSUpdateNotification
	err = json.Unmarshal(w.Body.Bytes(), &dnsResp)
	require.NoError(t, err)

	// Find the WG client in DNS records
	found := false
	for _, record := range dnsResp.Records {
		if record.Hostname == dnsName && record.MeshIP == meshIP {
			found = true
			break
		}
	}
	assert.True(t, found, "WireGuard client should appear in DNS records")

	// Delete the client
	req = httptest.NewRequest(http.MethodDelete, "/admin/api/wireguard/clients/"+clientID, nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Verify client is removed from DNS records
	req = httptest.NewRequest(http.MethodGet, "/api/v1/dns", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	err = json.Unmarshal(w.Body.Bytes(), &dnsResp)
	require.NoError(t, err)

	// Verify the WG client is no longer in DNS records
	found = false
	for _, record := range dnsResp.Records {
		if record.Hostname == dnsName {
			found = true
			break
		}
	}
	assert.False(t, found, "WireGuard client should be removed from DNS records after deletion")
}

// DNS Alias Tests

func TestServer_Register_WithAliases(t *testing.T) {
	srv := newTestServer(t)

	regReq := proto.RegisterRequest{
		Name:       "testnode",
		PublicKey:  "SHA256:abc123",
		PublicIPs:  []string{"1.2.3.4"},
		PrivateIPs: []string{"192.168.1.100"},
		SSHPort:    2222,
		Aliases:    []string{"webserver", "api"},
	}
	body, _ := json.Marshal(regReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp proto.RegisterResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	// Verify aliases are in DNS cache
	req = httptest.NewRequest(http.MethodGet, "/api/v1/dns", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var dnsResp proto.DNSUpdateNotification
	err = json.Unmarshal(w.Body.Bytes(), &dnsResp)
	require.NoError(t, err)

	// Should have testnode + 2 aliases = 3 records
	assert.Len(t, dnsResp.Records, 3)

	// Check all records point to same IP
	foundMain := false
	foundWebserver := false
	foundAPI := false
	for _, record := range dnsResp.Records {
		assert.Equal(t, resp.MeshIP, record.MeshIP)
		switch record.Hostname {
		case "testnode":
			foundMain = true
		case "webserver":
			foundWebserver = true
		case "api":
			foundAPI = true
		}
	}
	assert.True(t, foundMain, "main hostname should be in DNS")
	assert.True(t, foundWebserver, "webserver alias should be in DNS")
	assert.True(t, foundAPI, "api alias should be in DNS")
}

func TestServer_Register_AliasConflictWithPeerName(t *testing.T) {
	srv := newTestServer(t)

	// Register first peer
	regReq := proto.RegisterRequest{
		Name:      "existingpeer",
		PublicKey: "SHA256:abc123",
		SSHPort:   2222,
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Try to register second peer with alias that matches first peer's name
	regReq = proto.RegisterRequest{
		Name:      "newpeer",
		PublicKey: "SHA256:def456",
		SSHPort:   2222,
		Aliases:   []string{"existingpeer"}, // Conflicts with existing peer name
	}
	body, _ = json.Marshal(regReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "existingpeer")
}

func TestServer_Register_AliasConflictWithOtherAlias(t *testing.T) {
	srv := newTestServer(t)

	// Register first peer with an alias
	regReq := proto.RegisterRequest{
		Name:      "peer1",
		PublicKey: "SHA256:abc123",
		SSHPort:   2222,
		Aliases:   []string{"shared-name"},
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Try to register second peer with same alias
	regReq = proto.RegisterRequest{
		Name:      "peer2",
		PublicKey: "SHA256:def456",
		SSHPort:   2222,
		Aliases:   []string{"shared-name"}, // Already used by peer1
	}
	body, _ = json.Marshal(regReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "shared-name")
	assert.Contains(t, w.Body.String(), "peer1")
}

func TestServer_Register_ReregistrationUpdatesAliases(t *testing.T) {
	srv := newTestServer(t)

	// Initial registration with aliases
	regReq := proto.RegisterRequest{
		Name:      "testnode",
		PublicKey: "SHA256:abc123",
		SSHPort:   2222,
		Aliases:   []string{"old-alias1", "old-alias2"},
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Re-registration with new aliases
	regReq = proto.RegisterRequest{
		Name:      "testnode",
		PublicKey: "SHA256:abc123",
		SSHPort:   2222,
		Aliases:   []string{"new-alias"},
	}
	body, _ = json.Marshal(regReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Check DNS - should have testnode + new-alias, NOT old aliases
	req = httptest.NewRequest(http.MethodGet, "/api/v1/dns", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var dnsResp proto.DNSUpdateNotification
	err := json.Unmarshal(w.Body.Bytes(), &dnsResp)
	require.NoError(t, err)

	assert.Len(t, dnsResp.Records, 2) // testnode + new-alias

	hostnames := make(map[string]bool)
	for _, record := range dnsResp.Records {
		hostnames[record.Hostname] = true
	}
	assert.True(t, hostnames["testnode"])
	assert.True(t, hostnames["new-alias"])
	assert.False(t, hostnames["old-alias1"], "old aliases should be removed")
	assert.False(t, hostnames["old-alias2"], "old aliases should be removed")
}

func TestServer_Register_SamePeerCanReclaimOwnAliases(t *testing.T) {
	srv := newTestServer(t)

	// Initial registration
	regReq := proto.RegisterRequest{
		Name:      "testnode",
		PublicKey: "SHA256:abc123",
		SSHPort:   2222,
		Aliases:   []string{"myalias"},
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Re-registration with same alias - should succeed
	body, _ = json.Marshal(regReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code, "same peer should be able to reclaim own aliases")
}

func TestServer_Deregister_CleansUpAliases(t *testing.T) {
	srv := newTestServer(t)

	// Register peer with aliases
	regReq := proto.RegisterRequest{
		Name:      "testnode",
		PublicKey: "SHA256:abc123",
		SSHPort:   2222,
		Aliases:   []string{"alias1", "alias2"},
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Deregister
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/peers/testnode", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Verify DNS is empty
	req = httptest.NewRequest(http.MethodGet, "/api/v1/dns", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var dnsResp proto.DNSUpdateNotification
	err := json.Unmarshal(w.Body.Bytes(), &dnsResp)
	require.NoError(t, err)

	assert.Len(t, dnsResp.Records, 0, "all DNS records including aliases should be removed")

	// Verify aliases are free for other peers
	regReq = proto.RegisterRequest{
		Name:      "newpeer",
		PublicKey: "SHA256:def456",
		SSHPort:   2222,
		Aliases:   []string{"alias1"}, // Should be available now
	}
	body, _ = json.Marshal(regReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code, "aliases should be available after peer deregistration")
}

func TestServer_ValidateAliases(t *testing.T) {
	srv := newTestServer(t)

	// Register a peer
	regReq := proto.RegisterRequest{
		Name:      "existingpeer",
		PublicKey: "SHA256:abc123",
		SSHPort:   2222,
		Aliases:   []string{"taken-alias"},
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Test validateAliases directly
	tests := []struct {
		name           string
		aliases        []string
		requestingPeer string
		wantErr        bool
	}{
		{
			name:           "valid aliases",
			aliases:        []string{"new-alias"},
			requestingPeer: "newpeer",
			wantErr:        false,
		},
		{
			name:           "conflict with peer name",
			aliases:        []string{"existingpeer"},
			requestingPeer: "newpeer",
			wantErr:        true,
		},
		{
			name:           "conflict with other peer's alias",
			aliases:        []string{"taken-alias"},
			requestingPeer: "newpeer",
			wantErr:        true,
		},
		{
			name:           "same peer can use own alias",
			aliases:        []string{"taken-alias"},
			requestingPeer: "existingpeer",
			wantErr:        false,
		},
		{
			name:           "empty aliases",
			aliases:        []string{},
			requestingPeer: "newpeer",
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv.peersMu.Lock()
			err := srv.validateAliases(tt.aliases, tt.requestingPeer)
			srv.peersMu.Unlock()

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestServer_S3UserRecoveryOnRestart(t *testing.T) {
	// This test verifies that registered users are recovered after coordinator restart
	tempDir := t.TempDir()

	cfg := &config.PeerConfig{
		Name:      "test-coord",
		Servers:   []string{"http://localhost:8080"},
		AuthToken: "test-token",
		TUN: config.TUNConfig{
			MTU: 1400,
		},
		Coordinator: config.CoordinatorConfig{
			Listen: ":0",
			S3: config.S3Config{
				DataDir: tempDir + "/s3",
				MaxSize: 1 * 1024 * 1024 * 1024, // 1Gi - Required for quota enforcement
			},
		},
	}

	// Create first server instance
	srv1, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)

	// Simulate user registration by directly adding to stores
	testPeerID := "testuser123"
	testPubKey := "dGVzdHB1YmxpY2tleQ==" // base64 "testpublickey"

	// Register user credentials
	accessKey, secretKey, err := srv1.s3Credentials.RegisterUser(testPeerID, testPubKey)
	require.NoError(t, err)
	require.NotEmpty(t, accessKey)
	require.NotEmpty(t, secretKey)

	// Add role binding
	srv1.s3Authorizer.Bindings.Add(&auth.RoleBinding{
		Name:     "test-binding",
		PeerID:   testPeerID,
		RoleName: auth.RoleBucketRead,
	})

	// Persist to system store
	users := []*auth.Peer{{ID: testPeerID, PublicKey: testPubKey, Name: "Test User"}}
	err = srv1.s3SystemStore.SavePeers(context.Background(), users)
	require.NoError(t, err)

	bindings := srv1.s3Authorizer.Bindings.List()
	// Filter out service peer binding for persistence
	var peerBindings []*auth.RoleBinding
	for _, b := range bindings {
		if b.PeerID == testPeerID {
			peerBindings = append(peerBindings, b)
		}
	}
	err = srv1.s3SystemStore.SaveBindings(context.Background(), peerBindings)
	require.NoError(t, err)

	// Create second server instance (simulating restart)
	srv2, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)

	// Verify peer credentials were recovered
	recoveredPeerID, ok := srv2.s3Credentials.LookupUser(accessKey)
	assert.True(t, ok, "peer should be recovered after restart")
	assert.Equal(t, testPeerID, recoveredPeerID)

	recoveredSecret, ok := srv2.s3Credentials.GetSecret(testPeerID)
	assert.True(t, ok, "peer secret should be recovered after restart")
	assert.Equal(t, secretKey, recoveredSecret)

	// Verify role bindings were recovered
	canRead := srv2.s3Authorizer.Authorize(testPeerID, "get", "objects", "test-bucket", "")
	assert.True(t, canRead, "peer should have read permission after restart")
}

func newTestServerWithS3(t *testing.T) *Server {
	t.Helper()
	tempDir := t.TempDir()
	cfg := &config.PeerConfig{
		Name:      "test-coord",
		Servers:   []string{"http://localhost:8080"},
		AuthToken: "test-token",
		TUN: config.TUNConfig{
			MTU: 1400,
		},
		Coordinator: config.CoordinatorConfig{
			Enabled: true,
			Listen:  ":0",
			S3: config.S3Config{
				DataDir: tempDir + "/s3",
				MaxSize: 1 * 1024 * 1024 * 1024, // 1Gi - Required for quota enforcement
			},
			Filter: config.FilterConfig{}, // Initialize Filter field
		},
	}
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	return srv
}

func TestServer_BindingsAPI_ListEmpty(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)
	require.NotNil(t, srv.s3Authorizer)

	req := httptest.NewRequest(http.MethodGet, "/api/bindings", nil)
	w := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var bindings []*auth.RoleBinding
	err := json.Unmarshal(w.Body.Bytes(), &bindings)
	require.NoError(t, err)
	// Should have at least one binding (for the service user)
	assert.GreaterOrEqual(t, len(bindings), 0)
}

func TestServer_BindingsAPI_CreateBinding(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)
	require.NotNil(t, srv.s3Authorizer)

	// Create a new binding
	reqBody := RoleBindingRequest{
		PeerID:       "alice",
		RoleName:     auth.RoleBucketRead,
		BucketScope:  "test-bucket",
		ObjectPrefix: "data/",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/bindings", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)

	var created auth.RoleBinding
	err := json.Unmarshal(w.Body.Bytes(), &created)
	require.NoError(t, err)

	assert.Equal(t, "alice", created.PeerID)
	assert.Equal(t, auth.RoleBucketRead, created.RoleName)
	assert.Equal(t, "test-bucket", created.BucketScope)
	assert.Equal(t, "data/", created.ObjectPrefix)
	assert.NotEmpty(t, created.Name)
}

func TestServer_BindingsAPI_CreateBindingValidation(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)

	tests := []struct {
		name     string
		req      RoleBindingRequest
		wantCode int
	}{
		{
			name:     "missing user_id",
			req:      RoleBindingRequest{RoleName: auth.RoleBucketRead},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing role_name",
			req:      RoleBindingRequest{PeerID: "alice"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.req)
			req := httptest.NewRequest(http.MethodPost, "/api/bindings", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			srv.adminMux.ServeHTTP(w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

func TestServer_BindingsAPI_GetBinding(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)
	require.NotNil(t, srv.s3Authorizer)

	// Create a binding first
	binding := auth.NewRoleBindingWithPrefix("bob", auth.RoleBucketWrite, "mybucket", "prefix/")
	srv.s3Authorizer.Bindings.Add(binding)

	req := httptest.NewRequest(http.MethodGet, "/api/bindings/"+binding.Name, nil)
	w := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var got auth.RoleBinding
	err := json.Unmarshal(w.Body.Bytes(), &got)
	require.NoError(t, err)

	assert.Equal(t, binding.Name, got.Name)
	assert.Equal(t, "bob", got.PeerID)
	assert.Equal(t, auth.RoleBucketWrite, got.RoleName)
}

func TestServer_BindingsAPI_GetBindingNotFound(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)

	req := httptest.NewRequest(http.MethodGet, "/api/bindings/nonexistent", nil)
	w := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestServer_BindingsAPI_DeleteBinding(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)
	require.NotNil(t, srv.s3Authorizer)

	// Create a binding first
	binding := auth.NewRoleBindingWithPrefix("charlie", auth.RoleBucketAdmin, "", "")
	srv.s3Authorizer.Bindings.Add(binding)

	req := httptest.NewRequest(http.MethodDelete, "/api/bindings/"+binding.Name, nil)
	w := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify it's gone
	assert.Nil(t, srv.s3Authorizer.Bindings.Get(binding.Name))
}

func TestServer_BindingsAPI_DeleteBindingNotFound(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.adminMux)

	req := httptest.NewRequest(http.MethodDelete, "/api/bindings/nonexistent", nil)
	w := httptest.NewRecorder()

	srv.adminMux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestServer_FilterRulesPersistence(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.filter)
	require.NotNil(t, srv.s3SystemStore)

	// Add some temporary rules
	srv.filter.AddTemporaryRule(routing.FilterRule{
		Port:       22,
		Protocol:   routing.ProtoTCP,
		Action:     routing.ActionAllow,
		Expires:    0,
		SourcePeer: "",
	})
	srv.filter.AddTemporaryRule(routing.FilterRule{
		Port:       80,
		Protocol:   routing.ProtoTCP,
		Action:     routing.ActionAllow,
		Expires:    time.Now().Unix() + 3600,
		SourcePeer: "peer1",
	})

	// Save filter rules
	err := srv.SaveFilterRules(context.Background())
	require.NoError(t, err)

	// Verify rules were saved
	assert.True(t, srv.s3SystemStore.Exists(context.Background(), s3.FilterRulesPath))

	// Load and verify
	loaded, err := srv.s3SystemStore.LoadFilterRules(context.Background())
	require.NoError(t, err)
	assert.Len(t, loaded.Temporary, 2)
}

func TestServer_FilterRulesRecoveryFiltersExpired(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.PeerConfig{
		Name:      "test-coord",
		Servers:   []string{"http://localhost:8080"},
		AuthToken: "test-token",
		TUN: config.TUNConfig{
			MTU: 1400,
		},
		Coordinator: config.CoordinatorConfig{
			Listen: ":0",
			S3: config.S3Config{
				DataDir: tempDir + "/s3",
				MaxSize: 1 * 1024 * 1024 * 1024,
			},
		},
	}

	// Create first server
	srv1, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, srv1.filter)
	require.NotNil(t, srv1.s3SystemStore)

	// Add rules with different expiries
	pastTime := time.Now().Unix() - 1000   // Already expired
	futureTime := time.Now().Unix() + 3600 // Expires in 1 hour

	srv1.filter.AddTemporaryRule(routing.FilterRule{
		Port:       22,
		Protocol:   routing.ProtoTCP,
		Action:     routing.ActionAllow,
		Expires:    pastTime,
		SourcePeer: "",
	})
	srv1.filter.AddTemporaryRule(routing.FilterRule{
		Port:       80,
		Protocol:   routing.ProtoTCP,
		Action:     routing.ActionAllow,
		Expires:    futureTime,
		SourcePeer: "",
	})
	srv1.filter.AddTemporaryRule(routing.FilterRule{
		Port:       443,
		Protocol:   routing.ProtoTCP,
		Action:     routing.ActionAllow,
		Expires:    0, // Permanent
		SourcePeer: "",
	})

	// Save rules
	err = srv1.SaveFilterRules(context.Background())
	require.NoError(t, err)

	// Create second server (simulating restart)
	srv2, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, srv2.filter)

	// Verify only non-expired rules were recovered
	rules := srv2.filter.ListRules()
	temporaryRules := make([]routing.FilterRuleWithSource, 0)
	for _, r := range rules {
		if r.Source == routing.SourceTemporary {
			temporaryRules = append(temporaryRules, r)
		}
	}

	// Should have 2 rules (expired one was filtered out)
	assert.Len(t, temporaryRules, 2)

	// Verify port 22 (expired) is not present
	for _, r := range temporaryRules {
		assert.NotEqual(t, uint16(22), r.Rule.Port, "expired rule should not be recovered")
	}

	// Verify port 80 and 443 are present
	foundPort80 := false
	foundPort443 := false
	for _, r := range temporaryRules {
		if r.Rule.Port == 80 {
			foundPort80 = true
		}
		if r.Rule.Port == 443 {
			foundPort443 = true
		}
	}
	assert.True(t, foundPort80, "non-expired rule should be recovered")
	assert.True(t, foundPort443, "permanent rule should be recovered")
}

func TestServer_SaveFilterRulesFiltersExpired(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.filter)
	require.NotNil(t, srv.s3SystemStore)

	// Add expired rule
	pastTime := time.Now().Unix() - 1000
	srv.filter.AddTemporaryRule(routing.FilterRule{
		Port:       22,
		Protocol:   routing.ProtoTCP,
		Action:     routing.ActionAllow,
		Expires:    pastTime,
		SourcePeer: "",
	})

	// Add non-expired rule
	srv.filter.AddTemporaryRule(routing.FilterRule{
		Port:       80,
		Protocol:   routing.ProtoTCP,
		Action:     routing.ActionAllow,
		Expires:    0,
		SourcePeer: "",
	})

	// Save - should filter out expired rule
	err := srv.SaveFilterRules(context.Background())
	require.NoError(t, err)

	// Load and verify expired rule was not persisted
	loaded, err := srv.s3SystemStore.LoadFilterRules(context.Background())
	require.NoError(t, err)
	assert.Len(t, loaded.Temporary, 1, "expired rule should not be saved")
	assert.Equal(t, uint16(80), loaded.Temporary[0].Port)
}

func TestCoordMeshIPs_CallbackAndGetter(t *testing.T) {
	cfg := newTestConfig(t)
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = srv.Shutdown(context.Background()) }()

	// Initially empty
	assert.Empty(t, srv.GetCoordMeshIPs())

	// Set callback
	var received []string
	srv.OnCoordIPsChanged(func(ips []string) {
		received = ips
	})

	// SetCoordMeshIP triggers callback
	srv.SetCoordMeshIP("10.42.0.1")
	assert.Equal(t, []string{"10.42.0.1"}, srv.GetCoordMeshIPs())
	assert.Equal(t, []string{"10.42.0.1"}, received)
}

// generateTestSSHPubKey generates a valid ed25519 SSH public key in base64 wire format
// (as expected by config.DecodePublicKey / config.EncodePublicKey) and returns the raw ed25519 key.
func generateTestSSHPubKey(t *testing.T) (string, ed25519.PublicKey) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	sshPub, err := ssh.NewPublicKey(pub)
	require.NoError(t, err)
	return config.EncodePublicKey(sshPub), pub
}

func TestServer_AdminPeers_GlobPatterns(t *testing.T) {
	tests := []struct {
		name        string
		adminPeers  []string
		peerName    string
		usePeerID   bool // if true, also add the peer ID to adminPeers
		expectAdmin bool
	}{
		{
			name:        "exact name match",
			adminPeers:  []string{"mynode"},
			peerName:    "mynode",
			expectAdmin: true,
		},
		{
			name:        "wildcard suffix",
			adminPeers:  []string{"coordinator-*"},
			peerName:    "coordinator-prod",
			expectAdmin: true,
		},
		{
			name:        "wildcard prefix",
			adminPeers:  []string{"*-coordinator"},
			peerName:    "prod-coordinator",
			expectAdmin: true,
		},
		{
			name:        "question mark",
			adminPeers:  []string{"node-?"},
			peerName:    "node-a",
			expectAdmin: true,
		},
		{
			name:        "question mark no match (too long)",
			adminPeers:  []string{"node-?"},
			peerName:    "node-ab",
			expectAdmin: false,
		},
		{
			name:        "no match",
			adminPeers:  []string{"coordinator-*"},
			peerName:    "peer-xyz",
			expectAdmin: false,
		},
		{
			name:        "peer ID match preferred",
			adminPeers:  nil, // will be filled with peer ID
			peerName:    "wrong-name",
			usePeerID:   true,
			expectAdmin: true,
		},
		{
			name:        "invalid pattern does not crash",
			adminPeers:  []string{"coordinator-[*"},
			peerName:    "coordinator-1",
			expectAdmin: false,
		},
		{
			name:        "empty admin_peers",
			adminPeers:  []string{},
			peerName:    "anynode",
			expectAdmin: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newTestConfig(t)
			cfg.Coordinator.Enabled = true

			// Generate a valid ed25519 SSH key so peerID is derived
			sshPubKeyStr, edPub := generateTestSSHPubKey(t)

			if tt.usePeerID {
				peerID := auth.ComputePeerID(edPub)
				tt.adminPeers = []string{peerID}
			}
			cfg.Coordinator.AdminPeers = tt.adminPeers

			srv, err := NewServer(context.Background(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { cleanupServer(t, srv) })

			regReq := proto.RegisterRequest{
				Name:       tt.peerName,
				PublicKey:  sshPubKeyStr,
				PublicIPs:  []string{"1.2.3.4"},
				PrivateIPs: []string{"192.168.1.100"},
				SSHPort:    2222,
			}
			body, err := json.Marshal(regReq)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer test-token")
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			srv.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code)

			var resp proto.RegisterResponse
			err = json.Unmarshal(w.Body.Bytes(), &resp)
			require.NoError(t, err)

			assert.Equal(t, tt.expectAdmin, resp.IsAdmin,
				"expected IsAdmin=%v for peer %q with admin_peers=%v", tt.expectAdmin, tt.peerName, tt.adminPeers)
		})
	}
}

func TestGCStaggerDelay(t *testing.T) {
	// %1800 produces 0–1799 seconds, i.e. up to 29m59s (< 30min)
	maxStagger := 1799 * time.Second

	t.Run("within range", func(t *testing.T) {
		names := []string{"coord1", "coord2", "coord3", "us-east-1", "eu-west-2"}
		for _, name := range names {
			d := gcStaggerDelay(name)
			assert.GreaterOrEqual(t, d, time.Duration(0), "stagger for %q should be >= 0", name)
			assert.LessOrEqual(t, d, maxStagger, "stagger for %q should be <= 29m59s", name)
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		d1 := gcStaggerDelay("coord1")
		d2 := gcStaggerDelay("coord1")
		assert.Equal(t, d1, d2, "same name should produce same stagger")
	})

	t.Run("different names produce different staggers", func(t *testing.T) {
		d1 := gcStaggerDelay("coord1")
		d2 := gcStaggerDelay("coord2")
		d3 := gcStaggerDelay("coord3")
		// At least two of three should differ (hash collisions are theoretically possible but extremely unlikely)
		assert.False(t, d1 == d2 && d2 == d3, "different coordinator names should produce different staggers")
	})

	t.Run("empty name falls back", func(t *testing.T) {
		d := gcStaggerDelay("")
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.LessOrEqual(t, d, maxStagger)
		// Empty name should use "coordinator" fallback
		assert.Equal(t, gcStaggerDelay("coordinator"), d)
	})
}

func TestServer_SetCoordinatorID_OnInit(t *testing.T) {
	srv := newTestServerWithS3(t)
	require.NotNil(t, srv.s3Store)

	// Verify SetCoordinatorID was called during server init with the coordinator name
	assert.Equal(t, "test-coord", srv.s3Store.CoordinatorID(),
		"SetCoordinatorID should be called during server init with cfg.Name")
}

func TestHandleRegister_IncludesSelfInCoordinatorsList(t *testing.T) {
	srv := newTestServer(t)

	// Simulate coordinator having its own mesh IP (normally set after join_mesh completes)
	srv.SetCoordMeshIP("10.42.0.1")

	// Register a coordinator peer (different name from this coordinator)
	regReq := proto.RegisterRequest{
		Name:          "coordinator-2",
		PublicKey:     "SHA256:coord2key",
		PublicIPs:     []string{"2.3.4.5"},
		IsCoordinator: true,
	}
	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp proto.RegisterResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	// Self (10.42.0.1 / "test-coord") must be included so coordinator-2 can populate peerNames
	assert.Contains(t, resp.Coordinators, "10.42.0.1",
		"registrar must include itself in Coordinators so registrant can resolve sender name")
	assert.Equal(t, "test-coord", resp.CoordinatorNames["10.42.0.1"],
		"registrar must include its own name so registrant can populate peerNames correctly")

	// Self-registration guard: if same-name coordinator registers, self must NOT be included
	// (would cause replication to self)
	regSelf := proto.RegisterRequest{
		Name:          "test-coord",
		PublicKey:     "SHA256:selfkey",
		IsCoordinator: true,
	}
	bodySelf, _ := json.Marshal(regSelf)
	reqSelf := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(bodySelf))
	reqSelf.Header.Set("Authorization", "Bearer test-token")
	reqSelf.Header.Set("Content-Type", "application/json")
	wSelf := httptest.NewRecorder()
	srv.ServeHTTP(wSelf, reqSelf)

	var respSelf proto.RegisterResponse
	err = json.Unmarshal(wSelf.Body.Bytes(), &respSelf)
	require.NoError(t, err)
	assert.NotContains(t, respSelf.Coordinators, "10.42.0.1",
		"self-registration must not include self to avoid replication loops")
}

func TestServer_RegistrationResponseCoordinatorPeers(t *testing.T) {
	srv := newTestServerWithS3(t)

	replicator := srv.GetReplicator()
	require.NotNil(t, replicator, "server with S3 should have a replicator")

	// Before: no peers
	assert.Empty(t, replicator.GetPeers(), "replicator should start with no peers")

	// Simulate processing resp.Coordinators from registration response
	// (mirrors the code in main.go after join_mesh registration)
	respCoordinators := []string{"10.0.0.1", "10.0.0.2"}
	for _, coordIP := range respCoordinators {
		replicator.AddPeer(coordIP, coordIP)
	}

	// After: peers should be added
	peers := replicator.GetPeers()
	assert.Len(t, peers, 2, "replicator should have 2 peers after adding from registration response")
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, peers)

	// AddPeer is idempotent — adding same peer again should not duplicate
	replicator.AddPeer("10.0.0.1", "10.0.0.1")
	assert.Len(t, replicator.GetPeers(), 2, "duplicate AddPeer should be idempotent")
}
