package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/mesh"
)

func TestNormalizeServerURL(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "hostname without scheme or port",
			input:    "coord.example.com",
			expected: "https://coord.example.com:8443",
		},
		{
			name:     "hostname with port",
			input:    "coord.example.com:9443",
			expected: "https://coord.example.com:9443",
		},
		{
			name:     "https URL without port",
			input:    "https://coord.example.com",
			expected: "https://coord.example.com:8443",
		},
		{
			name:     "https URL with port",
			input:    "https://coord.example.com:9443",
			expected: "https://coord.example.com:9443",
		},
		{
			name:     "IPv4 without port",
			input:    "192.168.1.1",
			expected: "https://192.168.1.1:8443",
		},
		{
			name:     "IPv4 with port",
			input:    "192.168.1.1:9443",
			expected: "https://192.168.1.1:9443",
		},
		{
			name:     "IPv6 without port",
			input:    "[2001:db8::1]",
			expected: "https://[2001:db8::1]:8443",
		},
		{
			name:     "IPv6 with port",
			input:    "[2001:db8::1]:9443",
			expected: "https://[2001:db8::1]:9443",
		},
		{
			name:     "localhost without port",
			input:    "localhost",
			expected: "https://localhost:8443",
		},
		{
			name:     "localhost with port",
			input:    "localhost:9443",
			expected: "https://localhost:9443",
		},
		{
			name:        "URL with path",
			input:       "coord.example.com/api",
			expectError: true,
			errorMsg:    "should not include path",
		},
		{
			name:        "URL with path and port",
			input:       "coord.example.com:8443/api",
			expectError: true,
			errorMsg:    "should not include path",
		},
		{
			name:        "URL with query parameters",
			input:       "coord.example.com?foo=bar",
			expectError: true,
			errorMsg:    "should not include query parameters",
		},
		{
			name:        "HTTP scheme for remote server (insecure)",
			input:       "http://coord.example.com",
			expectError: true,
			errorMsg:    "must use HTTPS for remote servers",
		},
		{
			name:        "HTTP with port for remote server",
			input:       "http://coord.example.com:8080",
			expectError: true,
			errorMsg:    "must use HTTPS for remote servers",
		},
		{
			name:     "HTTP localhost without port",
			input:    "http://localhost",
			expected: "http://localhost:8443",
		},
		{
			name:     "HTTP localhost with port",
			input:    "http://localhost:8081",
			expected: "http://localhost:8081",
		},
		{
			name:     "HTTP 127.0.0.1 without port",
			input:    "http://127.0.0.1",
			expected: "http://127.0.0.1:8443",
		},
		{
			name:     "HTTP 127.0.0.1 with port",
			input:    "http://127.0.0.1:8081",
			expected: "http://127.0.0.1:8081",
		},
		{
			name:     "HTTP IPv6 localhost without port",
			input:    "http://[::1]",
			expected: "http://[::1]:8443",
		},
		{
			name:     "HTTP IPv6 localhost with port",
			input:    "http://[::1]:8081",
			expected: "http://[::1]:8081",
		},
		{
			name:     "HTTP 127.0.0.50 (loopback range)",
			input:    "http://127.0.0.50:8081",
			expected: "http://127.0.0.50:8081",
		},
		{
			name:     "HTTP private IP 192.168.x.x (allowed for internal networks)",
			input:    "http://192.168.1.10:8081",
			expected: "http://192.168.1.10:8081",
		},
		{
			name:     "HTTP private IP 192.168.0.1",
			input:    "http://192.168.0.1:8081",
			expected: "http://192.168.0.1:8081",
		},
		{
			name:     "HTTP private IP 192.168.255.254",
			input:    "http://192.168.255.254:8081",
			expected: "http://192.168.255.254:8081",
		},
		{
			name:     "HTTP private IP 10.0.0.1 (Class A)",
			input:    "http://10.0.0.1:8081",
			expected: "http://10.0.0.1:8081",
		},
		{
			name:     "HTTP private IP 10.123.45.67",
			input:    "http://10.123.45.67:8081",
			expected: "http://10.123.45.67:8081",
		},
		{
			name:     "HTTP private IP 10.255.255.255",
			input:    "http://10.255.255.255:8081",
			expected: "http://10.255.255.255:8081",
		},
		{
			name:     "HTTP private IP 172.16.0.1 (start of 172.16.0.0/12)",
			input:    "http://172.16.0.1:8081",
			expected: "http://172.16.0.1:8081",
		},
		{
			name:     "HTTP private IP 172.20.10.5 (middle of 172.16.0.0/12)",
			input:    "http://172.20.10.5:8081",
			expected: "http://172.20.10.5:8081",
		},
		{
			name:     "HTTP private IP 172.31.255.255 (end of 172.16.0.0/12)",
			input:    "http://172.31.255.255:8081",
			expected: "http://172.31.255.255:8081",
		},
		{
			name:        "HTTP 172.15.255.255 (before private range - should fail)",
			input:       "http://172.15.255.255:8081",
			expectError: true,
			errorMsg:    "must use HTTPS for remote servers",
		},
		{
			name:        "HTTP 172.32.0.1 (after private range - should fail)",
			input:       "http://172.32.0.1:8081",
			expectError: true,
			errorMsg:    "must use HTTPS for remote servers",
		},
		{
			name:        "HTTP public IP (should fail)",
			input:       "http://8.8.8.8:8081",
			expectError: true,
			errorMsg:    "must use HTTPS for remote servers",
		},
		{
			name:        "HTTP with empty hostname (malformed URL)",
			input:       "http://:8443",
			expectError: true,
			errorMsg:    "must use HTTPS for remote servers",
		},
		{
			name:        "URL with path (gets HTTPS prefix added)",
			input:       "ht!tp://invalid",
			expectError: true,
			errorMsg:    "should not include path",
		},
		{
			name:     "URL with trailing slash",
			input:    "https://coord.example.com:8443/",
			expected: "https://coord.example.com:8443/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := normalizeServerURL(tt.input)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestIsValidAuthToken(t *testing.T) {
	tests := []struct {
		name     string
		token    string
		expected bool
	}{
		{
			name:     "valid 64 hex characters",
			token:    "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
			expected: true,
		},
		{
			name:     "valid 64 hex characters (uppercase)",
			token:    "A1B2C3D4E5F6A7B8C9D0E1F2A3B4C5D6E7F8A9B0C1D2E3F4A5B6C7D8E9F0A1B2",
			expected: true,
		},
		{
			name:     "valid 64 hex characters (mixed case)",
			token:    "a1B2c3D4e5F6a7B8c9D0e1F2a3B4c5D6e7F8a9B0c1D2e3F4a5B6c7D8e9F0a1B2",
			expected: true,
		},
		{
			name:     "valid all zeros",
			token:    "0000000000000000000000000000000000000000000000000000000000000000",
			expected: true,
		},
		{
			name:     "valid all fs",
			token:    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			expected: true,
		},
		{
			name:     "empty string",
			token:    "",
			expected: false,
		},
		{
			name:     "too short (63 chars)",
			token:    "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b",
			expected: false,
		},
		{
			name:     "too long (65 chars)",
			token:    "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c",
			expected: false,
		},
		{
			name:     "non-hex character (g)",
			token:    "g1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
			expected: false,
		},
		{
			name:     "non-hex character (space)",
			token:    "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1 2",
			expected: false,
		},
		{
			name:     "non-hex character (hyphen)",
			token:    "a1b2c3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0",
			expected: false,
		},
		{
			name:     "special characters",
			token:    "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1!@",
			expected: false,
		},
		{
			name:     "only numbers",
			token:    "1234567890123456789012345678901234567890123456789012345678901234",
			expected: true,
		},
		{
			name:     "only letters",
			token:    "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidAuthToken(tt.token)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnsureCoordinatorConfig(t *testing.T) {
	tests := []struct {
		name                    string
		inputServers            []string
		inputCoordinatorEnabled bool
		inputCoordinatorListen  string
		expectedEnabled         bool
		expectedListen          string
	}{
		{
			name:                    "no servers, coordinator not enabled - should enable with default listen",
			inputServers:            []string{},
			inputCoordinatorEnabled: false,
			inputCoordinatorListen:  "",
			expectedEnabled:         true,
			expectedListen:          ":8443",
		},
		{
			name:                    "no servers, coordinator not enabled, custom listen - should enable with custom listen",
			inputServers:            []string{},
			inputCoordinatorEnabled: false,
			inputCoordinatorListen:  ":9443",
			expectedEnabled:         true,
			expectedListen:          ":9443",
		},
		{
			name:                    "has servers - should not enable coordinator",
			inputServers:            []string{"https://coord.example.com:8443"},
			inputCoordinatorEnabled: false,
			inputCoordinatorListen:  "",
			expectedEnabled:         false,
			expectedListen:          "",
		},
		{
			name:                    "coordinator already enabled - should not change",
			inputServers:            []string{},
			inputCoordinatorEnabled: true,
			inputCoordinatorListen:  ":9443",
			expectedEnabled:         true,
			expectedListen:          ":9443",
		},
		{
			name:                    "has servers and coordinator enabled - should not change",
			inputServers:            []string{"https://coord.example.com:8443"},
			inputCoordinatorEnabled: true,
			inputCoordinatorListen:  ":9443",
			expectedEnabled:         true,
			expectedListen:          ":9443",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.PeerConfig{
				Servers: tt.inputServers,
				Coordinator: config.CoordinatorConfig{
					Enabled: tt.inputCoordinatorEnabled,
					Listen:  tt.inputCoordinatorListen,
				},
			}

			ensureCoordinatorConfig(cfg)

			assert.Equal(t, tt.expectedEnabled, cfg.Coordinator.Enabled)
			assert.Equal(t, tt.expectedListen, cfg.Coordinator.Listen)
		})
	}
}

// TestConfigureLinuxResolvers_InvalidAddr verifies that a malformed DNS address
// (missing port) returns an error before any system calls are made.
func TestConfigureLinuxResolvers_InvalidAddr(t *testing.T) {
	err := configureLinuxResolvers("127.0.0.1", "tun-mesh", mesh.AllSuffixes())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid DNS address")
}

// TestConfigureSystemdResolvedDropIn verifies that the drop-in config file is
// written with all three mesh suffixes in a single file using the ~suffix format.
func TestConfigureSystemdResolvedDropIn(t *testing.T) {
	dir := t.TempDir()

	err := configureSystemdResolvedDropIn(dir, "127.0.0.1", "5353", mesh.AllSuffixes())
	require.NoError(t, err)

	confFile := filepath.Join(dir, "tunnelmesh.conf")
	data, err := os.ReadFile(confFile)
	require.NoError(t, err)

	content := string(data)
	assert.Contains(t, content, "[Resolve]")
	assert.Contains(t, content, "DNS=127.0.0.1:5353")

	// All three suffixes must be in a single Domains= line (not separate files).
	// Each suffix must use ~suffix format (leading dot stripped, ~ prepended).
	domainsLine := ""
	for _, line := range strings.Split(content, "\n") {
		if strings.HasPrefix(line, "Domains=") {
			domainsLine = line
			break
		}
	}
	require.NotEmpty(t, domainsLine, "Domains= line missing from drop-in config")

	for _, suffix := range mesh.AllSuffixes() {
		expected := "~" + strings.TrimPrefix(suffix, ".")
		assert.Contains(t, domainsLine, expected, "expected domain %s in Domains= line", expected)
	}

	// Verify no leading dot (invalid resolvectl format) sneaked through.
	assert.NotContains(t, domainsLine, "~.")
}

// TestRemoveLinuxResolversAt_FileExists verifies that the drop-in file is removed
// when it exists.
func TestRemoveLinuxResolversAt_FileExists(t *testing.T) {
	dir := t.TempDir()
	confFile := filepath.Join(dir, "tunnelmesh.conf")
	require.NoError(t, os.WriteFile(confFile, []byte("[Resolve]\n"), 0o644))

	err := removeLinuxResolversAt(dir, "tun-mesh")
	require.NoError(t, err)

	_, statErr := os.Stat(confFile)
	assert.True(t, os.IsNotExist(statErr), "drop-in file should be removed")
}

// TestRemoveLinuxResolversAt_NoFile verifies that removeLinuxResolversAt is a
// no-op (no error) when no drop-in file exists.
func TestRemoveLinuxResolversAt_NoFile(t *testing.T) {
	dir := t.TempDir()
	err := removeLinuxResolversAt(dir, "tun-mesh")
	require.NoError(t, err)
}
