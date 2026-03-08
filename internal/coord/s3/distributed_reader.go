package s3

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// PrefetchConfig controls the parallel chunk prefetch pipeline.
type PrefetchConfig struct {
	WindowSize  int // Max chunks to prefetch ahead (default 8)
	Parallelism int // Number of parallel fetch workers (default 4)
}

// DefaultPrefetchConfig returns the default prefetch configuration.
func DefaultPrefetchConfig() PrefetchConfig {
	return PrefetchConfig{
		WindowSize:  8,
		Parallelism: 4,
	}
}

// prefetchResult holds the result of fetching a single chunk.
type prefetchResult struct {
	index int
	data  []byte
	err   error
}

// DistributedChunkReader reads chunks from local storage or remote coordinators.
// It implements io.ReadCloser and transparently fetches missing chunks from peers.
// When Parallelism > 1 and a replicator is configured, chunks are prefetched in
// parallel and reassembled in order.
type DistributedChunkReader struct {
	ctx             context.Context
	cancel          context.CancelFunc // cancels prefetch goroutines
	chunks          []string           // Ordered list of chunk hashes
	localCAS        *CAS               // Local chunk storage
	registry        ChunkRegistryInterface
	replicator      ReplicatorInterface
	logger          zerolog.Logger
	timeout         time.Duration
	totalSize       int64
	bytesRead       int64
	statsChunkCount *atomic.Int64 // Incremental chunk count stats (optional)
	statsChunkBytes *atomic.Int64 // Incremental chunk bytes stats (optional)

	// Prefetch pipeline state
	prefetchCfg PrefetchConfig
	resultCh    chan prefetchResult     // results from workers
	readyBuf    map[int]*prefetchResult // out-of-order buffer
	nextRead    int                     // next chunk index Read() expects
	started     bool                    // lazy-start flag
	workerWg    sync.WaitGroup          // tracks prefetch worker goroutines

	// Sequential fallback state (used when parallelism <= 1 or no replicator)
	currentChunk []byte
	chunkOffset  int
	chunkIndex   int
}

// DistributedChunkReaderConfig contains configuration for the distributed chunk reader.
type DistributedChunkReaderConfig struct {
	Chunks          []string               // Ordered list of chunk hashes
	LocalCAS        *CAS                   // Local chunk storage
	Registry        ChunkRegistryInterface // Chunk ownership registry
	Replicator      ReplicatorInterface    // For remote chunk fetching
	Logger          zerolog.Logger         // Structured logger (optional)
	Timeout         time.Duration          // Timeout for remote chunk fetches
	TotalSize       int64                  // Total file size
	Prefetch        PrefetchConfig         // Parallel prefetch settings
	StatsChunkCount *atomic.Int64          // Incremental chunk count stats (optional)
	StatsChunkBytes *atomic.Int64          // Incremental chunk bytes stats (optional)
}

// NewDistributedChunkReader creates a new distributed chunk reader.
func NewDistributedChunkReader(ctx context.Context, config DistributedChunkReaderConfig) *DistributedChunkReader {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	prefetch := config.Prefetch
	if prefetch.WindowSize == 0 {
		prefetch.WindowSize = DefaultPrefetchConfig().WindowSize
	}
	if prefetch.Parallelism == 0 {
		prefetch.Parallelism = DefaultPrefetchConfig().Parallelism
	}

	childCtx, cancel := context.WithCancel(ctx)

	return &DistributedChunkReader{
		ctx:             childCtx,
		cancel:          cancel,
		chunks:          config.Chunks,
		localCAS:        config.LocalCAS,
		registry:        config.Registry,
		replicator:      config.Replicator,
		logger:          config.Logger,
		timeout:         timeout,
		totalSize:       config.TotalSize,
		prefetchCfg:     prefetch,
		readyBuf:        make(map[int]*prefetchResult),
		statsChunkCount: config.StatsChunkCount,
		statsChunkBytes: config.StatsChunkBytes,
	}
}

// useParallel returns true if parallel prefetch should be used.
func (r *DistributedChunkReader) useParallel() bool {
	return r.prefetchCfg.Parallelism > 1 && r.replicator != nil
}

// Read implements io.Reader, reading data from chunks (local or remote).
func (r *DistributedChunkReader) Read(p []byte) (int, error) {
	if r.useParallel() {
		return r.readParallel(p)
	}
	return r.readSequential(p)
}

// readSequential is the original sequential read path.
func (r *DistributedChunkReader) readSequential(p []byte) (int, error) {
	if r.currentChunk == nil || r.chunkOffset >= len(r.currentChunk) {
		if r.chunkIndex >= len(r.chunks) {
			return 0, io.EOF
		}
		if err := r.loadNextChunk(); err != nil {
			return 0, err
		}
	}

	n := copy(p, r.currentChunk[r.chunkOffset:])
	r.chunkOffset += n
	r.bytesRead += int64(n)

	return n, nil
}

// readParallel uses the prefetch pipeline for parallel chunk reads.
func (r *DistributedChunkReader) readParallel(p []byte) (int, error) {
	if !r.started {
		r.startPrefetch()
		r.started = true
	}

	if r.currentChunk == nil || r.chunkOffset >= len(r.currentChunk) {
		if r.nextRead >= len(r.chunks) {
			return 0, io.EOF
		}
		if err := r.loadNextPrefetched(); err != nil {
			return 0, err
		}
	}

	n := copy(p, r.currentChunk[r.chunkOffset:])
	r.chunkOffset += n
	r.bytesRead += int64(n)

	return n, nil
}

// startPrefetch launches the producer and worker goroutines.
func (r *DistributedChunkReader) startPrefetch() {
	// Buffer up to WindowSize results to allow workers to run ahead
	r.resultCh = make(chan prefetchResult, r.prefetchCfg.WindowSize)

	// Work channel: producer sends chunk indices, workers consume
	workCh := make(chan int, r.prefetchCfg.WindowSize)

	// Launch producer
	go func() {
		defer close(workCh)
		for i := range r.chunks {
			select {
			case workCh <- i:
			case <-r.ctx.Done():
				return
			}
		}
	}()

	// Launch worker pool
	parallelism := r.prefetchCfg.Parallelism
	if parallelism > len(r.chunks) {
		parallelism = len(r.chunks)
	}

	for w := 0; w < parallelism; w++ {
		r.workerWg.Add(1)
		go func() {
			defer r.workerWg.Done()
			for idx := range workCh {
				data, err := r.fetchChunk(idx)
				select {
				case r.resultCh <- prefetchResult{index: idx, data: data, err: err}:
				case <-r.ctx.Done():
					return
				}
			}
		}()
	}

	// Close resultCh when all workers are done
	go func() {
		r.workerWg.Wait()
		close(r.resultCh)
	}()
}

// loadNextPrefetched waits for the next chunk in order from the prefetch pipeline.
func (r *DistributedChunkReader) loadNextPrefetched() error {
	for {
		// Check if we already have the next chunk buffered
		if res, ok := r.readyBuf[r.nextRead]; ok {
			delete(r.readyBuf, r.nextRead)
			if res.err != nil {
				return res.err
			}
			r.currentChunk = res.data
			r.chunkOffset = 0
			r.nextRead++
			return nil
		}

		// Wait for more results
		res, ok := <-r.resultCh
		if !ok {
			// Channel closed, all workers done
			if r.nextRead >= len(r.chunks) {
				return io.EOF
			}
			// Check buffer one more time
			if res, ok := r.readyBuf[r.nextRead]; ok {
				delete(r.readyBuf, r.nextRead)
				if res.err != nil {
					return res.err
				}
				r.currentChunk = res.data
				r.chunkOffset = 0
				r.nextRead++
				return nil
			}
			return fmt.Errorf("prefetch pipeline closed before chunk %d was delivered", r.nextRead)
		}

		if res.index == r.nextRead {
			// This is the chunk we need
			if res.err != nil {
				return res.err
			}
			r.currentChunk = res.data
			r.chunkOffset = 0
			r.nextRead++
			return nil
		}

		// Out of order - buffer it
		r.readyBuf[res.index] = &res
	}
}

// fetchChunk fetches a single chunk by index (used by parallel workers).
// Tries local CAS first, then remote via registry/replicator.
func (r *DistributedChunkReader) fetchChunk(idx int) ([]byte, error) {
	chunkHash := r.chunks[idx]

	// Try local first (fast path)
	chunkData, err := r.localCAS.ReadChunk(r.ctx, chunkHash)
	if err == nil {
		return chunkData, nil
	}

	// Not local - try to find who has it
	var owners []string

	// Query registry for owners
	if r.registry != nil {
		owners, _ = r.registry.GetOwners(chunkHash)
	}

	// Fall back to all known peers when registry has no ownership info
	// (ownership gossip may not be implemented yet)
	if len(owners) == 0 && r.replicator != nil {
		r.logger.Debug().Str("chunk", chunkHash).Msg("no chunk owners found in registry, trying all peers")
		owners = r.replicator.GetPeers()
	}

	if len(owners) == 0 {
		return nil, fmt.Errorf("chunk %s not found locally and no peers available", chunkHash)
	}

	// Try each owner in order
	var lastErr error
	for _, ownerID := range owners {
		chunkData, err = r.fetchChunkFromPeer(ownerID, chunkHash)
		if err != nil {
			lastErr = err
			continue
		}

		// Cache locally for future reads; pass chunkHash to skip redundant SHA-256.
		if onDiskBytes, werr := r.localCAS.writeChunkKnown(r.ctx, chunkData, chunkHash); werr != nil {
			r.logger.Warn().
				Err(werr).
				Str("chunk_hash", chunkHash[:8]+"...").
				Int("chunk_size", len(chunkData)).
				Msg("Failed to cache chunk locally after remote fetch")
		} else if onDiskBytes > 0 && r.statsChunkCount != nil {
			r.statsChunkCount.Add(1)
			r.statsChunkBytes.Add(onDiskBytes)
		}

		// Register ownership locally
		if r.registry != nil {
			if rerr := r.registry.RegisterChunk(chunkHash, int64(len(chunkData))); rerr != nil {
				r.logger.Warn().
					Err(rerr).
					Str("chunk_hash", chunkHash[:8]+"...").
					Msg("Failed to register chunk ownership in registry")
			}
		}

		return chunkData, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to fetch chunk %s from any owner: %w", chunkHash, lastErr)
	}

	return nil, fmt.Errorf("failed to fetch chunk %s from %d owners", chunkHash, len(owners))
}

// loadNextChunk fetches the next chunk sequentially (used in sequential fallback path).
func (r *DistributedChunkReader) loadNextChunk() error {
	if r.chunkIndex >= len(r.chunks) {
		return io.EOF
	}

	data, err := r.fetchChunk(r.chunkIndex)
	if err != nil {
		return err
	}

	r.currentChunk = data
	r.chunkOffset = 0
	r.chunkIndex++
	return nil
}

// fetchChunkFromPeer requests a chunk from a remote coordinator.
func (r *DistributedChunkReader) fetchChunkFromPeer(peerID, chunkHash string) ([]byte, error) {
	if r.replicator == nil {
		return nil, fmt.Errorf("no replicator configured for remote chunk fetching")
	}

	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()

	chunkData, err := r.replicator.FetchChunk(ctx, peerID, chunkHash)
	if err != nil {
		return nil, fmt.Errorf("fetch chunk from %s: %w", peerID, err)
	}

	// Validate chunk integrity after receiving from remote peer
	actualHash := ContentHash(chunkData)
	if actualHash != chunkHash {
		return nil, fmt.Errorf("chunk integrity check failed: expected hash %s, got %s (possible corruption or malicious peer)",
			chunkHash, actualHash)
	}

	return chunkData, nil
}

// Close cancels prefetch goroutines, waits for them to finish, and releases resources.
// Waiting ensures all file handles are closed before the caller cleans up temp directories.
func (r *DistributedChunkReader) Close() error {
	r.cancel()
	r.workerWg.Wait()
	return nil
}

// Progress returns the current read progress (bytes read / total size).
func (r *DistributedChunkReader) Progress() float64 {
	if r.totalSize == 0 {
		return 0
	}
	return float64(r.bytesRead) / float64(r.totalSize)
}
