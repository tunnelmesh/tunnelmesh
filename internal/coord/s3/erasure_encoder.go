package s3

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// ReconstructBlockwise reconstructs the original file from partial shards
// encoded by the streaming block method. shards[i] is nil if the shard is
// unavailable. Each non-nil shard contains concatenated fixed-size pieces:
// piece for block 0, piece for block 1, …, piece for block N-1.
//
// streamBlockSize is the RS stripe size (e.g. 4 MB for RS(4,2));
// pieceSize = streamBlockSize / k. originalSize is the true file byte count
// used to trim RS zero-padding from the last block.
//
// Memory: O(fileSize) — assembles all shard buffers before block-wise RS
// decode. Streaming block-by-block reconstruction is future work.
func ReconstructBlockwise(shards [][]byte, k, m int, streamBlockSize, originalSize int64) ([]byte, error) {
	if k < 1 || m < 1 {
		return nil, fmt.Errorf("invalid k=%d or m=%d", k, m)
	}
	if len(shards) != k+m {
		return nil, fmt.Errorf("expected %d shards (k+m), got %d", k+m, len(shards))
	}
	if originalSize <= 0 {
		return nil, fmt.Errorf("originalSize must be > 0, got %d", originalSize)
	}
	if streamBlockSize <= 0 {
		return nil, fmt.Errorf("streamBlockSize must be > 0, got %d", streamBlockSize)
	}
	if int64(k) > streamBlockSize {
		return nil, fmt.Errorf("k=%d exceeds streamBlockSize=%d", k, streamBlockSize)
	}

	// Count available shards
	available := 0
	for _, s := range shards {
		if s != nil {
			available++
		}
	}
	if available < k {
		return nil, fmt.Errorf("insufficient shards: need %d, have %d", k, available)
	}

	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, fmt.Errorf("create RS decoder: %w", err)
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
		return nil, fmt.Errorf("all shards are empty")
	}
	numBlocks := (maxShardLen + pieceSize - 1) / pieceSize

	result := make([]byte, 0, originalSize)

	for blockIdx := int64(0); blockIdx < numBlocks; blockIdx++ {
		pieceStart := blockIdx * pieceSize
		pieceEnd := pieceStart + pieceSize

		// Extract and copy pieces for this block from each shard.
		// Reconstruct modifies slices in-place, so each must be an
		// independently allocated buffer.
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
				return nil, fmt.Errorf("reconstruct block %d: %w", blockIdx, err)
			}
		}

		// Append data pieces to result.
		for i := 0; i < k; i++ {
			if blockPieces[i] == nil {
				return nil, fmt.Errorf("data shard %d nil after reconstruction (block %d)", i, blockIdx)
			}
			result = append(result, blockPieces[i]...)
		}
	}

	// Trim RS zero-padding to the original file size.
	if int64(len(result)) < originalSize {
		return nil, fmt.Errorf("reconstructed %d bytes, expected %d", len(result), originalSize)
	}
	return result[:originalSize], nil
}
