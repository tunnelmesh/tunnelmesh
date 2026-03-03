package benchmark

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startTestServer starts a TLS test server with the benchmark HTTP handler and
// returns the port.  The server is automatically closed when the test ends.
func startTestServer(t *testing.T) int {
	t.Helper()
	ts := httptest.NewTLSServer(NewHTTPHandler())
	t.Cleanup(ts.Close)
	u, err := url.Parse(ts.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	return port
}

func TestClient_Run_Upload(t *testing.T) {
	port := startTestServer(t)

	cfg := Config{
		PeerName:  "test-peer",
		Size:      64 * 1024, // 64KB
		Direction: DirectionUpload,
		Timeout:   10 * time.Second,
		Port:      port,
	}

	client := NewClient("test-local", "127.0.0.1")
	result, err := client.Run(context.Background(), cfg)
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.Empty(t, result.Error)
	assert.Equal(t, "test-local", result.LocalPeer)
	assert.Equal(t, "test-peer", result.RemotePeer)
	assert.Equal(t, DirectionUpload, result.Direction)
	assert.Equal(t, int64(64*1024), result.RequestedSize)
	assert.Equal(t, int64(64*1024), result.TransferredSize)
	assert.GreaterOrEqual(t, result.DurationMs, int64(0))
	assert.GreaterOrEqual(t, result.ThroughputBps, float64(0))
}

func TestClient_Run_Download(t *testing.T) {
	port := startTestServer(t)

	cfg := Config{
		PeerName:  "test-peer",
		Size:      64 * 1024,
		Direction: DirectionDownload,
		Timeout:   10 * time.Second,
		Port:      port,
	}

	client := NewClient("test-local", "127.0.0.1")
	result, err := client.Run(context.Background(), cfg)
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.Equal(t, DirectionDownload, result.Direction)
	assert.Greater(t, result.TransferredSize, int64(0))
}

func TestClient_Run_WithChaos(t *testing.T) {
	port := startTestServer(t)

	cfg := Config{
		PeerName:  "test-peer",
		Size:      1024,
		Direction: DirectionUpload,
		Timeout:   10 * time.Second,
		Port:      port,
		Chaos: ChaosConfig{
			Latency: 10 * time.Millisecond,
		},
	}

	client := NewClient("test-local", "127.0.0.1")
	start := time.Now()
	result, err := client.Run(context.Background(), cfg)
	elapsed := time.Since(start)
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(10))
	assert.NotNil(t, result.Chaos)
	assert.Equal(t, 10*time.Millisecond, result.Chaos.Latency)
}

func TestClient_Run_Latency(t *testing.T) {
	port := startTestServer(t)

	cfg := Config{
		PeerName:  "test-peer",
		Size:      1024,
		Direction: DirectionUpload,
		Timeout:   10 * time.Second,
		Port:      port,
	}

	client := NewClient("test-local", "127.0.0.1")
	result, err := client.Run(context.Background(), cfg)
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.GreaterOrEqual(t, result.LatencyAvgMs, float64(0))
	assert.GreaterOrEqual(t, result.LatencyMaxMs, result.LatencyMinMs)
}

func TestClient_Run_ConnectionError(t *testing.T) {
	cfg := Config{
		PeerName:  "test-peer",
		Size:      1024,
		Direction: DirectionUpload,
		Timeout:   1 * time.Second,
		Port:      19999, // No server on this port
	}

	client := NewClient("test-local", "127.0.0.1")
	result, err := client.Run(context.Background(), cfg)

	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestClient_Run_ContextCancellation(t *testing.T) {
	port := startTestServer(t)

	cfg := Config{
		PeerName:  "test-peer",
		Size:      10 * ChunkSize, // 10 chunks
		Direction: DirectionUpload,
		Timeout:   60 * time.Second,
		Port:      port,
		Chaos: ChaosConfig{
			Latency: 100 * time.Millisecond,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	client := NewClient("test-local", "127.0.0.1")
	result, err := client.Run(ctx, cfg)

	if err == nil {
		assert.Less(t, result.TransferredSize, cfg.Size)
	} else {
		assert.Error(t, err)
	}
}

func TestClient_Run_LargeTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large transfer test in short mode")
	}

	port := startTestServer(t)

	cfg := Config{
		PeerName:  "test-peer",
		Size:      1024 * 1024, // 1MB
		Direction: DirectionUpload,
		Timeout:   30 * time.Second,
		Port:      port,
	}

	client := NewClient("test-local", "127.0.0.1")
	result, err := client.Run(context.Background(), cfg)
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.Equal(t, int64(1024*1024), result.TransferredSize)
}

func TestClient_MultiplePings(t *testing.T) {
	port := startTestServer(t)

	cfg := Config{
		PeerName:  "test-peer",
		Size:      ChunkSize * 3,
		Direction: DirectionUpload,
		Timeout:   10 * time.Second,
		Port:      port,
	}

	client := NewClient("test-local", "127.0.0.1")
	result, err := client.Run(context.Background(), cfg)
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.GreaterOrEqual(t, result.LatencyAvgMs, float64(0))
}

// BenchmarkClient_Upload measures the end-to-end throughput via the HTTP handler.
func BenchmarkClient_Upload(b *testing.B) {
	ts := httptest.NewTLSServer(NewHTTPHandler())
	defer ts.Close()
	u, err := url.Parse(ts.URL)
	if err != nil {
		b.Fatalf("failed to parse test server URL: %v", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		b.Fatalf("failed to parse port: %v", err)
	}

	cfg := Config{
		PeerName:  "bench-peer",
		Size:      64 * 1024,
		Direction: DirectionUpload,
		Timeout:   10 * time.Second,
		Port:      port,
	}

	client := NewClient("bench-local", "127.0.0.1")

	b.ResetTimer()
	b.SetBytes(cfg.Size)

	for i := 0; i < b.N; i++ {
		_, err := client.Run(context.Background(), cfg)
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}
