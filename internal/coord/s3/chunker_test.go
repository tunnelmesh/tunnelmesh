package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
)

// TestStreamingChunkerMatchesBatch verifies that StreamingChunker produces
// identical chunk boundaries to the batch chunker ChunkData().
func TestStreamingChunkerMatchesBatch(t *testing.T) {
	tests := []struct {
		name     string
		dataSize int
	}{
		{"empty", 0},
		{"tiny", 100},
		{"single chunk", 2048},
		{"multiple chunks", 100000},
		{"large file", 1024 * 1024}, // 1MB
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Generate random test data
			data := make([]byte, tt.dataSize)
			if tt.dataSize > 0 {
				if _, err := rand.Read(data); err != nil {
					t.Fatalf("generate test data: %v", err)
				}
			}

			// Batch chunking (original method)
			batchChunks, err := ChunkData(data)
			if err != nil {
				t.Fatalf("ChunkData failed: %v", err)
			}

			// Streaming chunking (new method)
			streamChunker := NewStreamingChunker(bytes.NewReader(data))
			var streamChunks [][]byte
			var streamHashes []string

			for {
				chunk, hash, err := streamChunker.NextChunk()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("NextChunk failed: %v", err)
				}
				streamChunks = append(streamChunks, chunk)
				streamHashes = append(streamHashes, hash)
			}

			// Verify same number of chunks
			if len(streamChunks) != len(batchChunks) {
				t.Errorf("chunk count mismatch: streaming=%d, batch=%d", len(streamChunks), len(batchChunks))
			}

			// Verify chunk contents match exactly
			for i := 0; i < len(streamChunks) && i < len(batchChunks); i++ {
				if !bytes.Equal(streamChunks[i], batchChunks[i]) {
					t.Errorf("chunk %d content mismatch: streaming size=%d, batch size=%d",
						i, len(streamChunks[i]), len(batchChunks[i]))
				}

				// Verify hash is correct
				expectedHash := ContentHash(streamChunks[i])
				if streamHashes[i] != expectedHash {
					t.Errorf("chunk %d hash mismatch: got=%s, want=%s",
						i, streamHashes[i], expectedHash)
				}
			}

			// Verify total data reconstructed correctly
			streamReconstructed := make([]byte, 0, tt.dataSize)
			for _, chunk := range streamChunks {
				streamReconstructed = append(streamReconstructed, chunk...)
			}

			if !bytes.Equal(streamReconstructed, data) {
				t.Errorf("reconstructed data mismatch: got %d bytes, want %d bytes",
					len(streamReconstructed), len(data))
			}
		})
	}
}

// TestStreamingChunkerCancellation verifies that streaming can be interrupted.
func TestStreamingChunkerCancellation(t *testing.T) {
	// Create a reader that will error mid-stream
	slowReader := &cancelableReader{
		data: make([]byte, 500000), // Large enough for multiple chunks
	}

	if _, err := rand.Read(slowReader.data); err != nil {
		t.Fatalf("generate test data: %v", err)
	}

	streamChunker := NewStreamingChunker(slowReader)

	// Read first chunk successfully
	_, _, err := streamChunker.NextChunk()
	if err != nil {
		t.Fatalf("first chunk failed: %v", err)
	}

	// Cancel the reader (will error on next refill)
	slowReader.cancelled = true

	// Continue reading until we hit the error
	// The chunker buffers data, so we may get a few more chunks
	// before seeing the error
	var gotError bool
	for i := 0; i < 100; i++ { // Limit iterations to prevent infinite loop
		_, _, err := streamChunker.NextChunk()
		if err != nil && !errors.Is(err, io.EOF) {
			gotError = true
			break
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}

	if !gotError {
		t.Error("expected error from cancelled reader")
	}
}

// TestStreamingChunkerErrors verifies error handling during streaming.
func TestStreamingChunkerErrors(t *testing.T) {
	tests := []struct {
		name        string
		reader      io.Reader
		expectError bool
	}{
		{
			name:        "normal reader",
			reader:      bytes.NewReader([]byte("hello world")),
			expectError: false,
		},
		{
			name:        "error reader",
			reader:      &errorReader{err: io.ErrUnexpectedEOF},
			expectError: true,
		},
		{
			name:        "partial error reader",
			reader:      &partialErrorReader{data: make([]byte, 10000), errorAfter: 5000},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamChunker := NewStreamingChunker(tt.reader)

			for {
				_, _, err := streamChunker.NextChunk()
				if errors.Is(err, io.EOF) {
					if tt.expectError {
						t.Error("expected error, got EOF")
					}
					break
				}
				if err != nil {
					if !tt.expectError {
						t.Errorf("unexpected error: %v", err)
					}
					break
				}
			}
		})
	}
}

// TestStreamingChunkerChunkSizes verifies chunk size constraints.
func TestStreamingChunkerChunkSizes(t *testing.T) {
	// Generate large test data
	data := make([]byte, 1024*1024) // 1MB
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate test data: %v", err)
	}

	streamChunker := NewStreamingChunker(bytes.NewReader(data))

	for {
		chunk, _, err := streamChunker.NextChunk()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("NextChunk failed: %v", err)
		}

		// Verify chunk size constraints
		if len(chunk) > MaxChunkSize {
			t.Errorf("chunk too large: %d > %d", len(chunk), MaxChunkSize)
		}

		// Note: Minimum chunk size constraint can be violated at EOF
	}
}

// TestStreamingChunkerMemoryUsage verifies memory efficiency.
// This is more of a documentation test than a strict enforcement.
func TestStreamingChunkerMemoryUsage(t *testing.T) {
	// Create 10MB of data
	data := make([]byte, 10*1024*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate test data: %v", err)
	}

	streamChunker := NewStreamingChunker(bytes.NewReader(data))

	var chunkCount int
	var totalSize int64

	for {
		chunk, _, err := streamChunker.NextChunk()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("NextChunk failed: %v", err)
		}

		chunkCount++
		totalSize += int64(len(chunk))

		// Each chunk should be released before the next is allocated
		// Peak memory should be ~MaxChunkSize (64KB), not the full 10MB
	}

	if totalSize != int64(len(data)) {
		t.Errorf("total size mismatch: got %d, want %d", totalSize, len(data))
	}

	t.Logf("Processed %d bytes in %d chunks (avg %.1f KB/chunk)",
		totalSize, chunkCount, float64(totalSize)/float64(chunkCount)/1024)
}

// TestChunkConfigForSize verifies tier selection for boundary inputs.
func TestChunkConfigForSize(t *testing.T) {
	tests := []struct {
		size     int64
		wantMask uint32
	}{
		{0, 0xFFF},
		{512 * 1024, 0xFFF},
		{1 * 1024 * 1024, 0xFFF},
		{1*1024*1024 + 1, 0xFFFF},
		{32 * 1024 * 1024, 0xFFFF},
		{64 * 1024 * 1024, 0xFFFF},
		{64*1024*1024 + 1, 0x7FFFF},
		{1 * 1024 * 1024 * 1024, 0x7FFFF},
	}
	for _, tt := range tests {
		cfg := ChunkConfigForSize(tt.size)
		if cfg.Mask != tt.wantMask {
			t.Errorf("ChunkConfigForSize(%d): mask=%#x, want %#x", tt.size, cfg.Mask, tt.wantMask)
		}
		if cfg.MinSize <= 0 {
			t.Errorf("ChunkConfigForSize(%d): MinSize=%d must be positive", tt.size, cfg.MinSize)
		}
		if cfg.MaxSize <= cfg.MinSize {
			t.Errorf("ChunkConfigForSize(%d): MaxSize=%d must exceed MinSize=%d", tt.size, cfg.MaxSize, cfg.MinSize)
		}
	}
}

// TestStreamingChunkerWithConfig_ReducesChunkCount verifies that large config produces
// far fewer chunks than small config for the same data, and both reconstruct correctly.
func TestStreamingChunkerWithConfig_ReducesChunkCount(t *testing.T) {
	data := make([]byte, 10*1024*1024) // 10 MB
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate test data: %v", err)
	}

	countChunks := func(cfg ChunkerConfig) (int, []byte) {
		sc := NewStreamingChunkerWithConfig(bytes.NewReader(data), cfg)
		var reconstructed []byte
		var count int
		for {
			chunk, _, err := sc.NextChunk()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("NextChunk: %v", err)
			}
			reconstructed = append(reconstructed, chunk...)
			count++
		}
		return count, reconstructed
	}

	smallCfg := ChunkConfigForSize(512 * 1024)        // small tier
	largeCfg := ChunkConfigForSize(200 * 1024 * 1024) // large tier

	smallCount, smallData := countChunks(smallCfg)
	largeCount, largeData := countChunks(largeCfg)

	if !bytes.Equal(smallData, data) {
		t.Error("small config: reconstructed data mismatch")
	}
	if !bytes.Equal(largeData, data) {
		t.Error("large config: reconstructed data mismatch")
	}
	if largeCount*10 >= smallCount {
		t.Errorf("expected large config to produce ≥10× fewer chunks: small=%d large=%d", smallCount, largeCount)
	}
}

// TestStreamingChunkerWithConfig_SizeConstraints verifies all chunks respect MaxSize.
func TestStreamingChunkerWithConfig_SizeConstraints(t *testing.T) {
	data := make([]byte, 5*1024*1024) // 5 MB
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate test data: %v", err)
	}

	cfg := ChunkConfigForSize(32 * 1024 * 1024) // medium tier: MaxSize=256KB
	sc := NewStreamingChunkerWithConfig(bytes.NewReader(data), cfg)

	for {
		chunk, _, err := sc.NextChunk()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("NextChunk: %v", err)
		}
		if len(chunk) > cfg.MaxSize {
			t.Errorf("chunk size %d exceeds MaxSize %d", len(chunk), cfg.MaxSize)
		}
	}
}

// cancelableReader is a test reader that can be cancelled via a flag.
type cancelableReader struct {
	data      []byte
	pos       int
	cancelled bool
}

func (r *cancelableReader) Read(p []byte) (int, error) {
	if r.cancelled {
		return 0, context.Canceled
	}
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// errorReader always returns an error.
type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

// partialErrorReader returns data up to a point, then errors.
type partialErrorReader struct {
	data       []byte
	pos        int
	errorAfter int
}

func (r *partialErrorReader) Read(p []byte) (int, error) {
	if r.pos >= r.errorAfter {
		return 0, io.ErrUnexpectedEOF
	}
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	remaining := r.errorAfter - r.pos
	limit := len(p)
	if limit > remaining {
		limit = remaining
	}

	n := copy(p[:limit], r.data[r.pos:])
	r.pos += n

	if r.pos >= r.errorAfter {
		return n, io.ErrUnexpectedEOF
	}

	return n, nil
}
