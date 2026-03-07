package coord

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_GenerateAndValidateToken(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.AuthToken = "test-secret-key-12345"
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.Listen = ":8080"

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Generate token
	token, err := srv.GenerateToken("peer1", "10.42.0.1")
	require.NoError(t, err)
	assert.NotEmpty(t, token)

	// Validate token
	claims, err := srv.ValidateToken(token)
	require.NoError(t, err)
	assert.Equal(t, "peer1", claims.PeerName)
	assert.Equal(t, "10.42.0.1", claims.MeshIP)
	assert.Equal(t, "tunnelmesh", claims.Issuer)
}

func TestServer_ValidateToken_InvalidSignature(t *testing.T) {
	cfg1 := newTestConfig(t)
	cfg1.Name = "test-coord1"
	cfg1.AuthToken = "secret-key-1"
	cfg1.Coordinator.Enabled = true
	cfg1.Coordinator.Listen = ":8080"

	srv1, err := NewServer(context.Background(), cfg1)
	require.NoError(t, err)
	defer func() { _ = srv1.Shutdown(context.Background()) }()

	cfg2 := newTestConfig(t)
	cfg2.Name = "test-coord2"
	cfg2.AuthToken = "secret-key-2" // Different key
	cfg2.Coordinator.Enabled = true
	cfg2.Coordinator.Listen = ":8080"

	srv2, err := NewServer(context.Background(), cfg2)
	require.NoError(t, err)
	defer func() { _ = srv2.Shutdown(context.Background()) }()

	// Generate token with one server
	token, err := srv1.GenerateToken("peer1", "10.42.0.1")
	require.NoError(t, err)

	// Try to validate with different server (different signing key)
	_, err = srv2.ValidateToken(token)
	assert.Error(t, err)
}

func TestServer_ValidateToken_InvalidToken(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.AuthToken = "test-secret-key"
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.Listen = ":8080"

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// Test with invalid token
	_, err = srv.ValidateToken("invalid-token")
	assert.Error(t, err)

	// Test with empty token
	_, err = srv.ValidateToken("")
	assert.Error(t, err)
}

func TestTokenExpiry(t *testing.T) {
	// Verify the token expiry constant is reasonable
	assert.Equal(t, 24*time.Hour, TokenExpiry)
}

// makeExpiredToken crafts a JWT that expired ago seconds in the past,
// signed with the given secret. Used to test expired-token rejection.
func makeExpiredToken(t *testing.T, secret, peerName, meshIP string, ago time.Duration) string {
	t.Helper()
	claims := JWTClaims{
		PeerName: peerName,
		MeshIP:   meshIP,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-ago)),
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-TokenExpiry - ago)),
			Issuer:    "tunnelmesh",
		},
	}
	raw := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := raw.SignedString([]byte(secret))
	require.NoError(t, err)
	return tokenStr
}

func TestServer_ValidateToken_Expired(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.AuthToken = "test-secret-expired"
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.Listen = ":8080"

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	tokenStr := makeExpiredToken(t, cfg.AuthToken, "peer1", "10.42.0.1", time.Second)
	_, err = srv.ValidateToken(tokenStr)
	require.Error(t, err, "expired token must be rejected")
}

func TestServer_ValidateToken_WithRelay_Expired(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.AuthToken = "test-secret-relay-expired"
	cfg.Coordinator.Enabled = true
	cfg.Coordinator.Listen = ":8080"

	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupServer(t, srv) })

	// The relay handler calls ValidateToken; verify it rejects expired tokens
	// using the same code path as the relay authentication handler.
	tokenStr := makeExpiredToken(t, cfg.AuthToken, "relay-peer", "10.42.0.5", time.Minute)
	_, err = srv.ValidateToken(tokenStr)
	require.Error(t, err, "relay path must reject expired token")
}
