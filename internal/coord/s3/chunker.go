// Package s3 provides an S3-compatible object storage service for the coordinator.
package s3

import (
	"io"
)

// Chunker configuration constants.
const (
	MinChunkSize    = 1024       // 1KB minimum chunk size
	TargetChunkSize = 4096       // 4KB target (average) chunk size
	MaxChunkSize    = 65536      // 64KB maximum chunk size
	ChunkMask       = 0xFFF      // Mask for boundary detection (1 in 4096 on average)
	BuzhashSeed     = 0x47b6137b // Arbitrary seed for buzhash
)

// buzhashTable is a precomputed table for the Buzhash rolling hash algorithm.
// Each byte maps to a pseudo-random 32-bit value.
var buzhashTable [256]uint32

func init() {
	// Initialize buzhash table using a simple PRNG seeded with BuzhashSeed
	state := uint32(BuzhashSeed)
	for i := range buzhashTable {
		// xorshift32 PRNG
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		buzhashTable[i] = state
	}
}

// ChunkerConfig holds size and boundary-detection parameters for the chunker.
// Use ChunkConfigForSize to get an appropriate config based on file size.
type ChunkerConfig struct {
	MinSize int    // Minimum chunk size in bytes
	MaxSize int    // Maximum chunk size in bytes
	Mask    uint32 // Boundary detection mask: hash & Mask == 0 triggers boundary
}

// ChunkConfigForSize returns a ChunkerConfig tuned for the given file size.
// Larger files use coarser chunking to reduce chunk count, metadata size, and
// replication overhead. Dedup quality is acceptable to trade off for large files
// since they are typically unique binaries or archives.
//
//   - <= 1 MB:  ~4 KB avg chunks (mask=0xFFF)
//   - <= 64 MB: ~64 KB avg chunks (mask=0xFFFF)
//   - > 64 MB:  ~512 KB avg chunks (mask=0x7FFFF)
//
// Negative or zero size (e.g. -1 from chunked HTTP transfer encoding where
// Content-Length is unknown) selects the smallest tier — a safe, conservative default.
//
// Dedup tradeoff: two uploads of identical content with different Content-Length
// values (e.g. one with a known size, one chunked) may land in different tiers,
// producing different chunk boundaries and therefore different CAS hashes. The
// content is stored twice on disk but deduplicated within each tier. This is an
// accepted tradeoff: large files (where cross-tier dedup would matter most) are
// overwhelmingly unique binaries or archives.
func ChunkConfigForSize(fileSize int64) ChunkerConfig {
	const (
		mb1  = 1 * 1024 * 1024
		mb64 = 64 * 1024 * 1024
	)
	switch {
	case fileSize <= mb1:
		return ChunkerConfig{MinSize: 1024, MaxSize: 65536, Mask: 0xFFF}
	case fileSize <= mb64:
		return ChunkerConfig{MinSize: 16384, MaxSize: 262144, Mask: 0xFFFF}
	default:
		return ChunkerConfig{MinSize: 131072, MaxSize: 2097152, Mask: 0x7FFFF}
	}
}

// Chunker splits data into variable-size chunks using content-defined chunking (CDC).
// It uses the Buzhash rolling hash algorithm to find chunk boundaries based on content,
// which provides stable boundaries even when data is inserted or deleted.
type Chunker struct {
	reader     io.Reader
	buf        []byte
	bufLen     int
	bufPos     int
	windowSize int
	hash       uint32
	done       bool
	minSize    int
	maxSize    int
	mask       uint32
}

// NewChunker creates a new CDC chunker for the given reader using default chunk sizes.
func NewChunker(r io.Reader) *Chunker {
	return NewChunkerWithConfig(r, ChunkerConfig{
		MinSize: MinChunkSize,
		MaxSize: MaxChunkSize,
		Mask:    ChunkMask,
	})
}

// NewChunkerWithConfig creates a new CDC chunker using the provided configuration.
func NewChunkerWithConfig(r io.Reader, cfg ChunkerConfig) *Chunker {
	return &Chunker{
		reader:     r,
		buf:        make([]byte, cfg.MaxSize*2), // Double buffer for efficiency
		windowSize: 64,                          // Rolling hash window size
		minSize:    cfg.MinSize,
		maxSize:    cfg.MaxSize,
		mask:       cfg.Mask,
	}
}

// Next returns the next chunk from the reader.
// Returns nil when EOF is reached.
func (c *Chunker) Next() ([]byte, error) {
	if c.done {
		return nil, nil
	}

	// Ensure we have data in the buffer
	if err := c.fillBuffer(); err != nil {
		return nil, err
	}

	if c.bufLen == 0 {
		c.done = true
		return nil, nil
	}

	// Find chunk boundary
	chunkEnd := c.findBoundary()

	// Extract chunk
	chunk := make([]byte, chunkEnd)
	copy(chunk, c.buf[:chunkEnd])

	// Shift buffer
	copy(c.buf, c.buf[chunkEnd:c.bufLen])
	c.bufLen -= chunkEnd
	c.bufPos = 0
	c.hash = 0

	return chunk, nil
}

// fillBuffer reads more data into the buffer if needed.
func (c *Chunker) fillBuffer() error {
	// If we have less than maxSize, try to read more
	if c.bufLen < c.maxSize {
		n, err := c.reader.Read(c.buf[c.bufLen:])
		c.bufLen += n
		if err == io.EOF {
			// EOF is not an error, we'll process remaining data
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// findBoundary finds the next chunk boundary using Buzhash rolling hash.
// Uses the standard Buzhash formula: H_new = rol(H_old, 1) ^ h(new) ^ rol(h(old), n)
func (c *Chunker) findBoundary() int {
	// Initialize rolling hash with first windowSize bytes (or less if not enough data)
	windowStart := 0
	if c.bufLen >= c.windowSize {
		windowStart = c.windowSize
		for i := 0; i < c.windowSize; i++ {
			c.hash = rol32(c.hash, 1) ^ buzhashTable[c.buf[i]]
		}
	}

	// Scan for boundary starting after minimum chunk size
	start := c.minSize
	if start < windowStart {
		start = windowStart
	}

	for i := start; i < c.bufLen; i++ {
		// Update rolling hash using correct Buzhash order:
		// 1. Rotate the hash left by 1
		// 2. XOR in the new byte
		// 3. XOR out the old byte (rotated by window size)
		c.hash = rol32(c.hash, 1)
		c.hash ^= buzhashTable[c.buf[i]]
		if i >= c.windowSize {
			outByte := c.buf[i-c.windowSize]
			c.hash ^= rol32(buzhashTable[outByte], uint32(c.windowSize))
		}

		// Check for boundary (after minimum size)
		if i >= c.minSize && (c.hash&c.mask) == 0 {
			return i + 1
		}

		// Force boundary at maximum size
		if i+1 >= c.maxSize {
			return c.maxSize
		}
	}

	// If we haven't found a boundary and have all the data (EOF), return what we have
	if c.bufLen < c.maxSize {
		return c.bufLen
	}

	// Shouldn't reach here, but force boundary at max size
	return c.maxSize
}

// rol32 performs a 32-bit left rotation.
func rol32(x uint32, n uint32) uint32 {
	return (x << n) | (x >> (32 - n))
}

// ChunkData splits data into chunks using CDC.
// This is a convenience function for chunking in-memory data.
func ChunkData(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	chunker := NewChunker(&byteReader{data: data})
	var chunks [][]byte

	for {
		chunk, err := chunker.Next()
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			break
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// byteReader wraps a byte slice as an io.Reader.
type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// StreamingChunker wraps the existing Chunker and adds hash computation.
// It processes data in a streaming fashion, never buffering entire files in memory.
// Peak buffer memory is MaxSize*2 per instance: ~128 KB for the default config,
// up to ~4 MB when using the large-file config via NewStreamingChunkerWithConfig.
type StreamingChunker struct {
	chunker *Chunker
}

// NewStreamingChunker creates a new streaming chunker that computes hashes.
func NewStreamingChunker(r io.Reader) *StreamingChunker {
	return &StreamingChunker{
		chunker: NewChunker(r),
	}
}

// NewStreamingChunkerWithConfig creates a streaming chunker using the provided configuration.
func NewStreamingChunkerWithConfig(r io.Reader, cfg ChunkerConfig) *StreamingChunker {
	return &StreamingChunker{
		chunker: NewChunkerWithConfig(r, cfg),
	}
}

// NextChunk reads and returns the next chunk along with its SHA-256 hash.
// Returns (nil, "", io.EOF) when all data has been processed.
// Returns (nil, "", err) on read errors.
func (sc *StreamingChunker) NextChunk() ([]byte, string, error) {
	chunk, err := sc.chunker.Next()
	if err != nil {
		return nil, "", err
	}
	if chunk == nil {
		return nil, "", io.EOF
	}

	// Compute SHA-256 hash of chunk
	hash := ContentHash(chunk)

	return chunk, hash, nil
}
