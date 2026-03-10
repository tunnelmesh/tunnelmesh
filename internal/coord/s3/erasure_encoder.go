package s3

import (
	"fmt"
	"io"

	"github.com/klauspost/reedsolomon"
)

// ReconstructBlockwiseWriter reconstructs the original file from partial shards
// and streams it block-by-block to w. Memory: O(streamBlockSize) constant
// regardless of file size (one RS stripe processed at a time).
//
// shards[i] is nil if the shard is unavailable. Each non-nil shard contains
// concatenated fixed-size pieces: piece for block 0, piece for block 1, …, block N-1.
//
// streamBlockSize is the RS stripe size (e.g. 4 MB for RS(4,2));
// pieceSize = streamBlockSize / k. originalSize trims RS zero-padding from the last block.
//
//nolint:gocyclo // Complexity from shard validation + block loop + write trimming
func ReconstructBlockwiseWriter(w io.Writer, shards [][]byte, k, m int, streamBlockSize, originalSize int64) error {
	if k < 1 || m < 1 {
		return fmt.Errorf("invalid k=%d or m=%d", k, m)
	}
	if len(shards) != k+m {
		return fmt.Errorf("expected %d shards (k+m), got %d", k+m, len(shards))
	}
	if originalSize <= 0 {
		return fmt.Errorf("originalSize must be > 0, got %d", originalSize)
	}
	if streamBlockSize <= 0 {
		return fmt.Errorf("streamBlockSize must be > 0, got %d", streamBlockSize)
	}
	if int64(k) > streamBlockSize {
		return fmt.Errorf("k=%d exceeds streamBlockSize=%d", k, streamBlockSize)
	}

	// Count available shards
	available := 0
	for _, s := range shards {
		if s != nil {
			available++
		}
	}
	if available < k {
		return fmt.Errorf("insufficient shards: need %d, have %d", k, available)
	}

	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return fmt.Errorf("create RS decoder: %w", err)
	}

	pieceSize := streamBlockSize / int64(k)

	// Determine number of blocks from the longest available shard.
	var maxShardLen int64
	for _, s := range shards {
		if s != nil && int64(len(s)) > maxShardLen {
			maxShardLen = int64(len(s))
		}
	}
	if maxShardLen == 0 {
		return fmt.Errorf("all shards are empty")
	}
	numBlocks := (maxShardLen + pieceSize - 1) / pieceSize

	written := int64(0)
	for blockIdx := int64(0); blockIdx < numBlocks && written < originalSize; blockIdx++ {
		pieceStart := blockIdx * pieceSize
		pieceEnd := pieceStart + pieceSize

		// Extract and copy pieces for this block from each shard.
		// ReconstructData modifies slices in-place, so each must be independently allocated.
		blockPieces := make([][]byte, k+m)
		for i, shard := range shards {
			if shard == nil {
				blockPieces[i] = nil
				continue
			}
			if pieceStart >= int64(len(shard)) {
				blockPieces[i] = nil
				continue
			}
			end := pieceEnd
			if end > int64(len(shard)) {
				end = int64(len(shard))
			}
			piece := make([]byte, pieceSize)
			copy(piece, shard[pieceStart:end])
			blockPieces[i] = piece
		}

		// Only reconstruct if some data shard piece is missing.
		needsReconstruction := false
		for i := 0; i < k; i++ {
			if blockPieces[i] == nil {
				needsReconstruction = true
				break
			}
		}
		if needsReconstruction {
			if err := enc.ReconstructData(blockPieces); err != nil {
				return fmt.Errorf("reconstruct block %d: %w", blockIdx, err)
			}
		}

		// Write data pieces to w, trimming RS zero-padding at end of file.
		for i := 0; i < k && written < originalSize; i++ {
			if blockPieces[i] == nil {
				return fmt.Errorf("data shard %d nil after reconstruction (block %d)", i, blockIdx)
			}
			toWrite := blockPieces[i]
			if remaining := originalSize - written; int64(len(toWrite)) > remaining {
				toWrite = toWrite[:remaining]
			}
			n, werr := w.Write(toWrite)
			written += int64(n)
			if werr != nil {
				return fmt.Errorf("write block %d shard %d: %w", blockIdx, i, werr)
			}
		}
	}

	if written < originalSize {
		return fmt.Errorf("reconstructed %d bytes, expected %d", written, originalSize)
	}
	return nil
}

// ReconstructBlockwise reconstructs the original file from partial shards into
// a byte slice. For large files, prefer ReconstructBlockwiseWriter to avoid an
// O(fileSize) output buffer.
func ReconstructBlockwise(shards [][]byte, k, m int, streamBlockSize, originalSize int64) ([]byte, error) {
	var buf []byte
	if originalSize > 0 {
		buf = make([]byte, 0, originalSize)
	}
	w := &appendWriter{buf: &buf}
	if err := ReconstructBlockwiseWriter(w, shards, k, m, streamBlockSize, originalSize); err != nil {
		return nil, err
	}
	return buf, nil
}

// appendWriter is an io.Writer that appends to a *[]byte.
type appendWriter struct{ buf *[]byte }

func (w *appendWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}
