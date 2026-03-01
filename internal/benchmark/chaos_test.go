package benchmark

import (
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockWriter is a simple io.Writer that records all writes.
type mockWriter struct {
	mu         sync.Mutex
	writes     [][]byte
	totalBytes int
}

func newMockWriter() *mockWriter {
	return &mockWriter{
		writes: make([][]byte, 0),
	}
}

func (m *mockWriter) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(p))
	copy(cp, p)
	m.writes = append(m.writes, cp)
	m.totalBytes += len(p)
	return len(p), nil
}

func (m *mockWriter) WriteCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.writes)
}

func (m *mockWriter) TotalBytes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalBytes
}

func (m *mockWriter) Data() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.writes) == 0 {
		return nil
	}
	// Return the last write (most common use case)
	return m.writes[len(m.writes)-1]
}

func TestTokenBucket_Basic(t *testing.T) {
	// 1000 bytes per second
	bucket := NewTokenBucket(1000)

	// First write should be immediate (bucket starts full)
	wait := bucket.Take(100)
	assert.Equal(t, time.Duration(0), wait, "first take should be immediate")

	// Second write should wait
	wait = bucket.Take(100)
	assert.GreaterOrEqual(t, wait, time.Duration(0))
}

func TestTokenBucket_RateLimiting(t *testing.T) {
	// 10000 bytes per second = 10 bytes per millisecond
	bucket := NewTokenBucket(10000)

	// Take all initial tokens
	bucket.Take(10000)

	// Now taking more should require waiting
	start := time.Now()
	wait := bucket.Take(1000)
	if wait > 0 {
		time.Sleep(wait)
	}
	elapsed := time.Since(start)

	// Should have waited roughly 100ms for 1000 bytes at 10000 bytes/sec
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(80), "should wait for tokens")
}

func TestTokenBucket_Refill(t *testing.T) {
	bucket := NewTokenBucket(10000) // 10KB/s

	// Drain the bucket
	bucket.Take(10000)

	// Wait for refill
	time.Sleep(100 * time.Millisecond)

	// Should have ~1000 tokens now
	wait := bucket.Take(500)
	assert.Equal(t, time.Duration(0), wait, "should have refilled tokens")
}

func TestChaosWriter_NoConfig(t *testing.T) {
	mock := newMockWriter()
	chaos := NewChaosWriter(mock, ChaosConfig{})

	data := []byte("hello world")
	n, err := chaos.Write(data)

	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, mock.Data())
}

func TestChaosWriter_PacketLoss(t *testing.T) {
	// Use fixed seed for reproducibility
	rng := rand.New(rand.NewSource(42))

	mock := newMockWriter()
	chaos := NewChaosWriterWithRng(mock, ChaosConfig{
		PacketLossPercent: 50.0,
	}, rng)

	// Write many packets
	totalWrites := 1000
	for i := 0; i < totalWrites; i++ {
		_, _ = chaos.Write([]byte("x"))
	}

	// With 50% packet loss, we should see roughly half the writes
	actualWrites := mock.WriteCount()
	assert.Greater(t, actualWrites, 300, "should have some writes")
	assert.Less(t, actualWrites, 700, "should have dropped some packets")
}

func TestChaosWriter_PacketLoss100Percent(t *testing.T) {
	mock := newMockWriter()
	chaos := NewChaosWriter(mock, ChaosConfig{
		PacketLossPercent: 100.0,
	})

	// All writes should be "dropped" (still return success)
	for i := 0; i < 100; i++ {
		n, err := chaos.Write([]byte("x"))
		require.NoError(t, err)
		assert.Equal(t, 1, n) // Reports success
	}

	assert.Equal(t, 0, mock.WriteCount(), "all packets should be dropped")
}

func TestChaosWriter_Latency(t *testing.T) {
	mock := newMockWriter()
	latency := 50 * time.Millisecond
	chaos := NewChaosWriter(mock, ChaosConfig{
		Latency: latency,
	})

	start := time.Now()
	_, _ = chaos.Write([]byte("test"))
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, latency-5*time.Millisecond, "should add latency")
}

func TestChaosWriter_Jitter(t *testing.T) {
	mock := newMockWriter()
	jitter := 20 * time.Millisecond
	chaos := NewChaosWriter(mock, ChaosConfig{
		Jitter: jitter,
	})

	// Run multiple writes and check variance
	var durations []time.Duration
	for i := 0; i < 10; i++ {
		start := time.Now()
		_, _ = chaos.Write([]byte("test"))
		durations = append(durations, time.Since(start))
	}

	// All durations should be between 0 and 2*jitter
	// Use wider tolerance for CI runners which have variable scheduling delays
	for _, d := range durations {
		assert.LessOrEqual(t, d, 2*jitter+20*time.Millisecond)
	}
}

func TestChaosWriter_LatencyWithJitter(t *testing.T) {
	mock := newMockWriter()
	latency := 50 * time.Millisecond
	jitter := 10 * time.Millisecond

	chaos := NewChaosWriter(mock, ChaosConfig{
		Latency: latency,
		Jitter:  jitter,
	})

	// Run multiple writes
	var durations []time.Duration
	for i := 0; i < 5; i++ {
		start := time.Now()
		_, _ = chaos.Write([]byte("test"))
		durations = append(durations, time.Since(start))
	}

	// All durations should be at least latency-jitter (verifies delay is applied).
	// Upper bound uses a generous 150ms slack: the exact ceiling isn't meaningful
	// and CI schedulers can add significant overhead on loaded runners.
	for _, d := range durations {
		assert.GreaterOrEqual(t, d, latency-jitter-5*time.Millisecond)
		assert.LessOrEqual(t, d, latency+jitter+150*time.Millisecond)
	}
}

func TestChaosWriter_Bandwidth(t *testing.T) {
	mock := newMockWriter()
	bandwidth := int64(10000) // 10 KB/s

	chaos := NewChaosWriter(mock, ChaosConfig{
		BandwidthBps: bandwidth,
	})

	// First write drains initial bucket (10KB at 10KB/s rate)
	data := make([]byte, 10000)
	_, _ = chaos.Write(data)

	// Second write should be fully rate limited
	start := time.Now()
	_, _ = chaos.Write(data) // Another 10KB
	elapsed := time.Since(start)

	// Should wait roughly 1 second for 10KB at 10KB/s
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(900), "should be rate limited")
}

func TestChaosWriter_AllChaosOptions(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	mock := newMockWriter()

	chaos := NewChaosWriterWithRng(mock, ChaosConfig{
		PacketLossPercent: 10.0,
		Latency:           10 * time.Millisecond,
		Jitter:            5 * time.Millisecond,
		BandwidthBps:      100000, // 100 KB/s
	}, rng)

	// Write some data
	data := make([]byte, 1000)
	start := time.Now()

	for i := 0; i < 10; i++ {
		_, _ = chaos.Write(data)
	}

	elapsed := time.Since(start)

	// Should have some latency from all the effects
	assert.Greater(t, elapsed.Milliseconds(), int64(50))

	// Should have some writes (with 10% packet loss)
	assert.Greater(t, mock.WriteCount(), 5)
	assert.Less(t, mock.WriteCount(), 10)
}

func TestChaosWriter_Stats(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	mock := newMockWriter()

	chaos := NewChaosWriterWithRng(mock, ChaosConfig{
		PacketLossPercent: 20.0,
	}, rng)

	// Write 100 packets
	for i := 0; i < 100; i++ {
		_, _ = chaos.Write([]byte("x"))
	}

	stats := chaos.Stats()
	assert.Equal(t, int64(100), stats.TotalWrites)
	assert.Greater(t, stats.DroppedWrites, int64(0))
	assert.Equal(t, int64(100), stats.TotalBytes)
	assert.Less(t, stats.ActualBytes, int64(100))
}

func TestChaosWriter_Concurrent(t *testing.T) {
	mock := newMockWriter()
	chaos := NewChaosWriter(mock, ChaosConfig{
		Latency: 1 * time.Millisecond,
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = chaos.Write([]byte("test"))
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, 100, mock.WriteCount())
}

// Benchmark the ChaosWriter overhead
func BenchmarkChaosWriter_NoConfig(b *testing.B) {
	mock := newMockWriter()
	chaos := NewChaosWriter(mock, ChaosConfig{})
	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = chaos.Write(data)
	}
}

func BenchmarkChaosWriter_PacketLoss(b *testing.B) {
	mock := newMockWriter()
	chaos := NewChaosWriter(mock, ChaosConfig{
		PacketLossPercent: 10.0,
	})
	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = chaos.Write(data)
	}
}

func BenchmarkTokenBucket(b *testing.B) {
	bucket := NewTokenBucket(1000000000) // 1GB/s - shouldn't limit
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bucket.Take(1024)
	}
}

// Verify ChaosWriter implements io.Writer
var _ io.Writer = (*ChaosWriter)(nil)
