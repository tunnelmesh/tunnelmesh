package s3

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/klauspost/reedsolomon"
)

// encodeForTest is a test helper that encodes data into k data shards + m parity
// shards using the same block-streaming layout as putObjectWithErasureCoding.
// streamBlockSize must be a multiple of k.
func encodeForTest(data []byte, k, m int, streamBlockSize int) ([][]byte, error) {
	if k < 1 || m < 1 {
		return nil, fmt.Errorf("invalid k=%d or m=%d", k, m)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("data must not be empty")
	}
	if k+m > 256 {
		return nil, fmt.Errorf("total shards (k+m) must be <= 256, got %d", k+m)
	}
	if streamBlockSize%k != 0 {
		return nil, fmt.Errorf("streamBlockSize %d is not divisible by k=%d", streamBlockSize, k)
	}

	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, fmt.Errorf("create RS encoder: %w", err)
	}

	pieceSize := streamBlockSize / k

	// Allocate shard output buffers.
	shards := make([][]byte, k+m)
	for i := range shards {
		shards[i] = nil
	}

	blockBuf := make([]byte, streamBlockSize)
	pieces := make([][]byte, k+m)
	for i := 0; i < k; i++ {
		pieces[i] = blockBuf[i*pieceSize : (i+1)*pieceSize]
	}
	for i := k; i < k+m; i++ {
		pieces[i] = make([]byte, pieceSize)
	}

	offset := 0
	for offset < len(data) {
		n := copy(blockBuf, data[offset:])
		// zero-pad remainder
		for j := n; j < streamBlockSize; j++ {
			blockBuf[j] = 0
		}
		offset += n

		if err := enc.Encode(pieces); err != nil {
			return nil, fmt.Errorf("encode block: %w", err)
		}

		for i := 0; i < k+m; i++ {
			shards[i] = append(shards[i], pieces[i]...)
		}
	}
	return shards, nil
}

func TestReconstructBlockwise_RoundTrip(t *testing.T) {
	k, m := 4, 2
	streamBlockSize := k * 256 // 1 KB stripe

	tests := []struct {
		name string
		size int
	}{
		{"small", 35},
		{"one-block", k * 256},
		{"multi-block", k * 256 * 3},
		{"uneven", k*256*2 + 100},
		{"medium", 10 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := make([]byte, tt.size)
			_, _ = rand.Read(original)

			shards, err := encodeForTest(original, k, m, streamBlockSize)
			if err != nil {
				t.Fatalf("encodeForTest failed: %v", err)
			}

			if len(shards) != k+m {
				t.Fatalf("expected %d shards, got %d", k+m, len(shards))
			}

			decoded, err := ReconstructBlockwise(shards, k, m, int64(streamBlockSize), int64(tt.size))
			if err != nil {
				t.Fatalf("ReconstructBlockwise failed: %v", err)
			}

			if !bytes.Equal(decoded, original) {
				t.Errorf("decoded data doesn't match original (size=%d)", tt.size)
			}
		})
	}
}

func TestReconstructBlockwise_MissingDataShards(t *testing.T) {
	k, m := 4, 2
	streamBlockSize := k * 256

	data := make([]byte, 10*1024)
	_, _ = rand.Read(data)

	shards, err := encodeForTest(data, k, m, streamBlockSize)
	if err != nil {
		t.Fatalf("encodeForTest failed: %v", err)
	}

	for missing := 1; missing <= m; missing++ {
		t.Run(fmt.Sprintf("missing_%d_data_shards", missing), func(t *testing.T) {
			testShards := make([][]byte, k+m)
			copy(testShards, shards)
			for i := 0; i < missing; i++ {
				testShards[i] = nil
			}

			decoded, err := ReconstructBlockwise(testShards, k, m, int64(streamBlockSize), int64(len(data)))
			if err != nil {
				t.Fatalf("ReconstructBlockwise failed with %d missing shards: %v", missing, err)
			}

			if !bytes.Equal(decoded, data) {
				t.Errorf("decoded data doesn't match original with %d missing shards", missing)
			}
		})
	}
}

func TestReconstructBlockwise_MissingParityShards(t *testing.T) {
	k, m := 4, 2
	streamBlockSize := k * 256

	data := make([]byte, 5*1024)
	_, _ = rand.Read(data)

	shards, err := encodeForTest(data, k, m, streamBlockSize)
	if err != nil {
		t.Fatalf("encodeForTest failed: %v", err)
	}

	// Nil out parity shards — should reconstruct fine with all data shards.
	testShards := make([][]byte, k+m)
	copy(testShards[:k], shards[:k])
	// testShards[k:] remain nil

	decoded, err := ReconstructBlockwise(testShards, k, m, int64(streamBlockSize), int64(len(data)))
	if err != nil {
		t.Fatalf("ReconstructBlockwise failed with missing parity: %v", err)
	}

	if !bytes.Equal(decoded, data) {
		t.Errorf("decoded data doesn't match original with missing parity shards")
	}
}

func TestReconstructBlockwise_MixedShards(t *testing.T) {
	k, m := 4, 2
	streamBlockSize := k * 256

	data := make([]byte, 8*1024)
	_, _ = rand.Read(data)

	shards, err := encodeForTest(data, k, m, streamBlockSize)
	if err != nil {
		t.Fatalf("encodeForTest failed: %v", err)
	}

	// Remove 1 data shard and 1 parity shard — still within tolerance (m=2).
	testShards := make([][]byte, k+m)
	copy(testShards, shards)
	testShards[0] = nil // missing data shard 0
	testShards[k] = nil // missing parity shard 0

	decoded, err := ReconstructBlockwise(testShards, k, m, int64(streamBlockSize), int64(len(data)))
	if err != nil {
		t.Fatalf("ReconstructBlockwise failed with mixed missing: %v", err)
	}

	if !bytes.Equal(decoded, data) {
		t.Errorf("decoded data doesn't match original with mixed missing shards")
	}
}

func TestReconstructBlockwise_InsufficientShards(t *testing.T) {
	k, m := 4, 2
	streamBlockSize := k * 256

	data := make([]byte, 1024)
	_, _ = rand.Read(data)

	shards, err := encodeForTest(data, k, m, streamBlockSize)
	if err != nil {
		t.Fatalf("encodeForTest failed: %v", err)
	}

	// Nil out too many shards (more than m missing).
	testShards := make([][]byte, k+m)
	copy(testShards, shards)
	for i := 0; i < m+1; i++ {
		testShards[i] = nil
	}

	_, err = ReconstructBlockwise(testShards, k, m, int64(streamBlockSize), int64(len(data)))
	if err == nil {
		t.Errorf("expected error with insufficient shards, got nil")
	}
}

func TestReconstructBlockwise_InvalidInputs(t *testing.T) {
	k, m := 4, 2
	streamBlockSize := k * 256

	data := make([]byte, 1024)
	_, _ = rand.Read(data)

	shards, err := encodeForTest(data, k, m, streamBlockSize)
	if err != nil {
		t.Fatalf("encodeForTest failed: %v", err)
	}

	tests := []struct {
		name            string
		shards          [][]byte
		k               int
		m               int
		streamBlockSize int64
		originalSize    int64
	}{
		{"wrong_shard_count", shards[:k], k, m, int64(streamBlockSize), int64(len(data))},
		{"zero_original_size", shards, k, m, int64(streamBlockSize), 0},
		{"negative_original_size", shards, k, m, int64(streamBlockSize), -1},
		{"invalid_k_zero", shards, 0, m, int64(streamBlockSize), int64(len(data))},
		{"invalid_m_zero", shards, k, 0, int64(streamBlockSize), int64(len(data))},
		{"zero_stream_block_size", shards, k, m, 0, int64(len(data))},
		{"oversized", shards, k, m, int64(streamBlockSize), int64(len(data)) * 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ReconstructBlockwise(tt.shards, tt.k, tt.m, tt.streamBlockSize, tt.originalSize)
			if err == nil {
				t.Errorf("expected error for %s, got nil", tt.name)
			}
		})
	}
}

func TestEncodeForTest_InvalidParameters(t *testing.T) {
	data := []byte("test")

	tests := []struct {
		name            string
		k               int
		m               int
		streamBlockSize int
	}{
		{"k_zero", 0, 2, 256},
		{"m_zero", 3, 0, 3 * 256},
		{"k_negative", -1, 2, 256},
		{"m_negative", 3, -1, 3 * 256},
		{"total_too_large", 200, 100, 200 * 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := encodeForTest(data, tt.k, tt.m, tt.streamBlockSize)
			if err == nil {
				t.Errorf("expected error with k=%d, m=%d, got nil", tt.k, tt.m)
			}
		})
	}
}

func TestEncodeForTest_EmptyData(t *testing.T) {
	_, err := encodeForTest([]byte{}, 4, 2, 4*256)
	if err == nil {
		t.Errorf("expected error with empty data, got nil")
	}
}

func TestReconstructBlockwise_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file test in short mode")
	}

	k, m := 4, 2
	// Use the canonical 4MB block size
	streamBlockSize := ecStreamBlock

	data := make([]byte, 10*1024*1024) // 10 MB
	_, _ = rand.Read(data)

	shards, err := encodeForTest(data, k, m, streamBlockSize)
	if err != nil {
		t.Fatalf("encodeForTest failed: %v", err)
	}

	// Decode with 2 missing data shards (within tolerance of m=2).
	testShards := make([][]byte, k+m)
	copy(testShards, shards)
	testShards[0] = nil
	testShards[2] = nil

	decoded, err := ReconstructBlockwise(testShards, k, m, int64(streamBlockSize), int64(len(data)))
	if err != nil {
		t.Fatalf("ReconstructBlockwise failed: %v", err)
	}

	if !bytes.Equal(decoded, data) {
		t.Errorf("decoded large file doesn't match original")
	}
}

func TestReconstructBlockwiseWriter_RoundTrip(t *testing.T) {
	k, m := 4, 2
	streamBlockSize := k * 256

	tests := []struct {
		name    string
		size    int
		missing int
	}{
		{"small_no_missing", 35, 0},
		{"multi_block_no_missing", k * 256 * 3, 0},
		{"small_missing_1", 35, 1},
		{"large_missing_2", k * 256 * 5, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := make([]byte, tt.size)
			_, _ = rand.Read(original)

			shards, err := encodeForTest(original, k, m, streamBlockSize)
			if err != nil {
				t.Fatalf("encodeForTest failed: %v", err)
			}

			for i := 0; i < tt.missing; i++ {
				shards[i] = nil
			}

			var buf bytes.Buffer
			err = ReconstructBlockwiseWriter(&buf, shards, k, m, int64(streamBlockSize), int64(tt.size))
			if err != nil {
				t.Fatalf("ReconstructBlockwiseWriter failed: %v", err)
			}

			if !bytes.Equal(buf.Bytes(), original) {
				t.Errorf("decoded data doesn't match original (size=%d, missing=%d)", tt.size, tt.missing)
			}
		})
	}
}

func TestReconstructBlockwiseWriter_WriterError(t *testing.T) {
	k, m := 4, 2
	streamBlockSize := k * 256
	data := make([]byte, 1024)
	_, _ = rand.Read(data)

	shards, err := encodeForTest(data, k, m, streamBlockSize)
	if err != nil {
		t.Fatalf("encodeForTest failed: %v", err)
	}

	// errorWriter fails after the first write
	ew := &struct{ written bool }{}
	w := writerFunc(func(p []byte) (int, error) {
		if ew.written {
			return 0, fmt.Errorf("simulated write error")
		}
		ew.written = true
		return len(p), nil
	})

	err = ReconstructBlockwiseWriter(w, shards, k, m, int64(streamBlockSize), int64(len(data)))
	if err == nil {
		t.Error("expected error from writer, got nil")
	}
}

// writerFunc is an io.Writer backed by a function.
type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func BenchmarkReconstructBlockwise(b *testing.B) {
	k, m := 4, 2
	streamBlockSize := ecStreamBlock

	data := make([]byte, 1024*1024) // 1 MB
	_, _ = rand.Read(data)

	shards, err := encodeForTest(data, k, m, streamBlockSize)
	if err != nil {
		b.Fatalf("encodeForTest failed: %v", err)
	}

	tests := []struct {
		name    string
		missing int
	}{
		{"no_missing", 0},
		{"missing_1", 1},
		{"missing_2", 2},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				testShards := make([][]byte, k+m)
				for j := range shards {
					if shards[j] != nil {
						testShards[j] = make([]byte, len(shards[j]))
						copy(testShards[j], shards[j])
					}
				}
				for j := 0; j < tt.missing; j++ {
					testShards[j] = nil
				}

				_, err := ReconstructBlockwise(testShards, k, m, int64(streamBlockSize), int64(len(data)))
				if err != nil {
					b.Fatalf("ReconstructBlockwise failed: %v", err)
				}
			}
		})
	}
}
