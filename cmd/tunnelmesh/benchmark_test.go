package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/benchmark"
)

func TestNewBenchmarkCmd(t *testing.T) {
	cmd := newBenchmarkCmd()

	assert.Equal(t, "benchmark <peer-name>", cmd.Use)
	assert.Equal(t, "Run a speed test to a peer", cmd.Short)

	// Verify flags exist with correct defaults
	sizeFlag := cmd.Flags().Lookup("size")
	require.NotNil(t, sizeFlag)
	assert.Equal(t, "10MB", sizeFlag.DefValue)

	dirFlag := cmd.Flags().Lookup("direction")
	require.NotNil(t, dirFlag)
	assert.Equal(t, "upload", dirFlag.DefValue)

	outputFlag := cmd.Flags().Lookup("output")
	require.NotNil(t, outputFlag)
	assert.Equal(t, "", outputFlag.DefValue)

	timeoutFlag := cmd.Flags().Lookup("timeout")
	require.NotNil(t, timeoutFlag)
	assert.Equal(t, "2m0s", timeoutFlag.DefValue)

	portFlag := cmd.Flags().Lookup("port")
	require.NotNil(t, portFlag)
	assert.Equal(t, "9443", portFlag.DefValue)

	// Chaos flags
	packetLossFlag := cmd.Flags().Lookup("packet-loss")
	require.NotNil(t, packetLossFlag)
	assert.Equal(t, "0", packetLossFlag.DefValue)

	latencyFlag := cmd.Flags().Lookup("latency")
	require.NotNil(t, latencyFlag)
	assert.Equal(t, "0s", latencyFlag.DefValue)

	jitterFlag := cmd.Flags().Lookup("jitter")
	require.NotNil(t, jitterFlag)
	assert.Equal(t, "0s", jitterFlag.DefValue)

	bandwidthFlag := cmd.Flags().Lookup("bandwidth")
	require.NotNil(t, bandwidthFlag)
	assert.Equal(t, "", bandwidthFlag.DefValue)
}

func TestNewBenchmarkCmd_RequiresArg(t *testing.T) {
	cmd := newBenchmarkCmd()

	// Should require exactly 1 argument
	err := cmd.Args(cmd, []string{})
	assert.Error(t, err)

	err = cmd.Args(cmd, []string{"peer1"})
	assert.NoError(t, err)

	err = cmd.Args(cmd, []string{"peer1", "peer2"})
	assert.Error(t, err)
}

func TestWriteBenchmarkJSON(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "result.json")

	result := &benchmark.Result{
		ID:              "test-123",
		LocalPeer:       "local",
		RemotePeer:      "remote",
		Direction:       "upload",
		Timestamp:       time.Now(),
		RequestedSize:   1024,
		TransferredSize: 1024,
		DurationMs:      100,
		ThroughputBps:   10240,
		LatencyAvgMs:    5.0,
		Success:         true,
	}

	err := writeBenchmarkJSON(result, outputPath)
	require.NoError(t, err)

	// Verify file exists and contains valid JSON
	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"id": "test-123"`)
	assert.Contains(t, string(data), `"local_peer": "local"`)
	assert.Contains(t, string(data), `"remote_peer": "remote"`)
	assert.Contains(t, string(data), `"success": true`)
}

func TestWriteBenchmarkJSON_InvalidPath(t *testing.T) {
	result := &benchmark.Result{Success: true}

	// Try to write to a path that doesn't exist
	err := writeBenchmarkJSON(result, "/nonexistent/dir/result.json")
	assert.Error(t, err)
}

func TestPrintBenchmarkResult_Success(_ *testing.T) {
	result := &benchmark.Result{
		Success:         true,
		TransferredSize: 10 * 1024 * 1024, // 10 MB
		RequestedSize:   10 * 1024 * 1024,
		DurationMs:      1000,
		ThroughputBps:   10 * 1024 * 1024,
		LatencyMinMs:    1.0,
		LatencyAvgMs:    5.0,
		LatencyMaxMs:    10.0,
	}

	// Just verify it doesn't panic - output goes to stdout
	printBenchmarkResult(result)
}

func TestPrintBenchmarkResult_Failure(_ *testing.T) {
	result := &benchmark.Result{
		Success: false,
		Error:   "connection refused",
	}

	// Just verify it doesn't panic
	printBenchmarkResult(result)
}

func TestGetPeerMeshIP_Success(t *testing.T) {
	// Save original and restore after test
	originalLookup := lookupHost
	defer func() { lookupHost = originalLookup }()

	// Mock the lookup function
	// nolint:revive // ctx required by benchmark signature but not used in this test
	lookupHost = func(ctx context.Context, host string) ([]string, error) {
		if host == "peer1.tunnelmesh" {
			return []string{"10.99.0.5"}, nil
		}
		return nil, &net.DNSError{Name: host, Err: "not found"}
	}

	ip, err := getPeerMeshIP("peer1")
	require.NoError(t, err)
	assert.Equal(t, "10.99.0.5", ip)
}

func TestGetPeerMeshIP_FallbackToDirectName(t *testing.T) {
	originalLookup := lookupHost
	defer func() { lookupHost = originalLookup }()

	// nolint:revive // ctx required by benchmark signature but not used in this test
	// First lookup fails, fallback succeeds
	lookupHost = func(ctx context.Context, host string) ([]string, error) {
		if host == "peer1" {
			return []string{"192.168.1.100"}, nil
		}
		return nil, &net.DNSError{Name: host, Err: "not found"}
	}

	ip, err := getPeerMeshIP("peer1")
	require.NoError(t, err)
	assert.Equal(t, "192.168.1.100", ip)
}

func TestGetPeerMeshIP_BothFail(t *testing.T) {
	originalLookup := lookupHost
	// nolint:revive // ctx required by benchmark signature but not used in this test
	defer func() { lookupHost = originalLookup }()

	lookupHost = func(ctx context.Context, host string) ([]string, error) {
		return nil, &net.DNSError{Name: host, Err: "not found"}
	}

	_, err := getPeerMeshIP("unknown-peer")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot resolve peer")
	assert.Contains(t, err.Error(), "unknown-peer")
}

func TestGetPeerMeshIP_EmptyResult(t *testing.T) {
	originalLookup := lookupHost
	defer func() { lookupHost = originalLookup }()

	lookupHost = func(ctx context.Context, host string) ([]string, error) {
		return []string{}, nil // Empty result
	}

	_, err := getPeerMeshIP("peer1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no address found")
}

func TestGetPeerMeshIP_MultipleAddresses(t *testing.T) {
	originalLookup := lookupHost
	defer func() { lookupHost = originalLookup }()

	lookupHost = func(ctx context.Context, host string) ([]string, error) {
		return []string{"10.99.0.1", "10.99.0.2", "10.99.0.3"}, nil
	}

	// Should return the first address
	ip, err := getPeerMeshIP("peer1")
	require.NoError(t, err)
	assert.Equal(t, "10.99.0.1", ip)
}
