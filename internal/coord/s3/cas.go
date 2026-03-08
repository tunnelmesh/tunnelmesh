// Package s3 provides an S3-compatible object storage service for the coordinator.
package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// macFailureMsg is the error string returned by chacha20poly1305.Open() when the
// message authentication code check fails (wrong key or corrupt ciphertext).
// Extracted as a constant to make it grep-able and document the upstream dependency.
// Source: golang.org/x/crypto/chacha20poly1305 — Open() → errOpen.
const macFailureMsg = "message authentication failed"

// casEncoderConcurrency caps the zstd encoder's internal sub-encoder pool.
// At SpeedDefault with lowMem, each sub-encoder uses ~12–13 MB, so
// 2 × 13 MB ≈ 26 MB. This allows two concurrent WriteChunk callers to
// compress in parallel (relevant when large-tier chunks of ~2 MB arrive
// from multiple peers simultaneously) while staying well within the 400 MiB
// GOMEMLIMIT budget set in systemd and docker-compose.
const casEncoderConcurrency = 2

// CAS (Content-Addressable Storage) manages encrypted, compressed chunks.
// Chunks are identified by their SHA-256 hash (computed on plaintext).
// Storage format: plaintext -> zstd compress -> XChaCha20-Poly1305 encrypt -> store
//
// SECURITY NOTE - Convergent Encryption:
// This implementation uses convergent encryption where identical plaintext produces
// identical ciphertext. This enables deduplication across the storage system but has
// security implications:
//
//   - Attackers who know the plaintext can verify if that exact content exists by
//     computing the expected ciphertext and comparing.
//   - All users share the same encryption keys (derived from mesh PSK), so content
//     uploaded by one user can be deduplicated against content from another.
//
// Mitigations in place:
//   - Nonces are derived from masterKey + content hash (not just content), preventing
//     attackers without the master key from predicting ciphertexts.
//   - Authenticated encryption (XChaCha20-Poly1305) prevents tampering.
//   - Hash verification on read detects corruption.
//
// This tradeoff is acceptable for single-tenant mesh deployments. For multi-tenant
// scenarios with strict data isolation requirements, consider adding per-user salt
// to the key derivation (at the cost of losing cross-user deduplication).
type CAS struct {
	chunksDir string
	masterKey [32]byte // Derived from mesh PSK for convergent encryption

	// encoder is a single shared zstd encoder. EncodeAll is concurrency-safe via
	// its own internal sub-encoder channel pool. A sync.Pool of Encoders is an
	// anti-pattern here: each pooled Encoder allocates GOMAXPROCS sub-encoders on
	// first use (~12.7 MB of hash tables each), causing ~158 MB of permanent heap
	// after any upload burst that fully saturates the pool.
	encoder     *zstd.Encoder
	decoderPool sync.Pool

	// onSelfHeal is called when ReadChunk deletes a chunk due to a MAC failure
	// (the chunk was encrypted with a different key). Callers can use this to
	// update storage stats and unregister the chunk from the chunk registry.
	// May be nil. Called with the chunk hash and the freed on-disk bytes.
	onSelfHeal func(hash string, freedBytes int64)

	// knownDirs caches the set of two-hex-char subdirectories that have been
	// created under chunksDir. There are only 256 possible subdirectories (00–ff);
	// after warmup all are cached and chunkPath() skips os.MkdirAll entirely.
	knownDirs sync.Map
}

// DeriveMasterKey derives a stable CAS master key from the mesh auth token using HKDF.
// All coordinators in the same mesh share the same token, so they derive the same key,
// which is required for raw-bytes chunk replication to work correctly.
// token must be a 64-character hex string (32 raw bytes); any other format is hashed directly.
func DeriveMasterKey(token string) ([32]byte, error) {
	var masterKey [32]byte
	// Trim whitespace before hex-decoding so that tokens with a trailing newline
	// (e.g., copy-pasted from a config file) don't silently fall through to the
	// raw-bytes path and produce a different key.
	token = strings.TrimSpace(token)
	tokenBytes, err := hex.DecodeString(token)
	if err != nil {
		// Non-hex token (unusual): use raw UTF-8 bytes
		tokenBytes = []byte(token)
	}
	h := hkdf.New(sha256.New, tokenBytes, nil, []byte("tunnelmesh-cas-v1"))
	if _, err := io.ReadFull(h, masterKey[:]); err != nil {
		return masterKey, fmt.Errorf("derive CAS key from token: %w", err)
	}
	return masterKey, nil
}

// NewCAS creates a new content-addressable storage.
// masterKey should be derived from the mesh PSK for consistent encryption across coordinators.
func NewCAS(chunksDir string, masterKey [32]byte) (*CAS, error) {
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return nil, fmt.Errorf("create chunks dir: %w", err)
	}

	cas := &CAS{
		chunksDir: chunksDir,
		masterKey: masterKey,
	}

	// Initialize single shared encoder with bounded concurrency.
	// See casEncoderConcurrency for the memory/throughput tradeoff rationale.
	// WithLowerEncoderMem reduces the per-sub-encoder history buffer from
	// ~32 MB (SpeedDefault without lowMem) to ~12–13 MB; with concurrency=2
	// total zstd encoder memory stays at ~26 MB with no change in compression ratio.
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(casEncoderConcurrency),
		zstd.WithLowerEncoderMem(true),
	)
	if err != nil {
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}
	cas.encoder = enc

	// Initialize decoder pool
	cas.decoderPool = sync.Pool{
		New: func() interface{} {
			dec, err := zstd.NewReader(nil)
			if err != nil {
				// Unreachable: NewReader only fails on invalid options, and we
				// pass none. Panic rather than silently returning a nil decoder.
				panic(fmt.Sprintf("zstd.NewReader: %v", err))
			}
			return dec
		},
	}

	return cas, nil
}

// Close releases the resources held by the shared encoder (its internal
// sub-encoder pool). Should be called when the CAS is no longer needed.
func (c *CAS) Close() error {
	return c.encoder.Close()
}

// WriteChunk stores a chunk and returns its content hash and on-disk size.
// If the chunk already exists (same content), it returns immediately with onDiskBytes=0 (dedup hit).
// Pipeline: plaintext -> compress -> encrypt -> store
func (c *CAS) WriteChunk(ctx context.Context, data []byte) (hash string, onDiskBytes int64, err error) {
	// Compute hash on plaintext for content addressing
	hash = c.contentHash(data)
	onDiskBytes, err = c.writeChunkKnown(ctx, data, hash)
	return hash, onDiskBytes, err
}

// writeChunkKnown is like WriteChunk but accepts a pre-computed content hash,
// avoiding a redundant SHA-256 when the caller has already computed it
// (e.g. StreamingChunker.NextChunk returns the hash alongside the chunk data).
//
// knownHash must be the SHA-256 hex digest of data; callers are responsible for
// ensuring this invariant before calling. All internal call sites either:
//   - receive the hash from StreamingChunker.NextChunk (which computes it),
//   - validate the hash against ContentHash(data) before calling (fetch paths), or
//   - receive the hash as a trusted parameter already verified by WriteChunkDirect.
//
// ctx is accepted for API symmetry with WriteChunk and WriteChunkRaw; it is not
// currently used inside this function (file I/O is not context-aware), but is
// retained so callers can pass it without branching and future changes can wire it in.
func (c *CAS) writeChunkKnown(ctx context.Context, data []byte, knownHash string) (onDiskBytes int64, err error) {
	chunkPath := c.chunkPath(knownHash)

	// Check if chunk already exists (dedup)
	// Safe without lock: os.Stat is atomic and we only need an approximate check.
	if fileExists(chunkPath) {
		return 0, nil // dedup hit
	}

	// Compress
	compressed, err := c.compress(data)
	if err != nil {
		return 0, fmt.Errorf("compress chunk: %w", err)
	}

	// Encrypt with convergent encryption (deterministic key/nonce from content hash)
	encrypted, err := c.encrypt(compressed, knownHash)
	if err != nil {
		return 0, fmt.Errorf("encrypt chunk: %w", err)
	}

	// Write atomically via unique temp file.
	// Using os.CreateTemp avoids collisions between concurrent writes of
	// the same or different hashes (the old fixed ".tmp" suffix was the
	// reason a global mutex was needed). Convergent encryption guarantees
	// identical plaintext produces identical ciphertext, so concurrent
	// writes of the same hash are safe — the last rename wins and the
	// file content is byte-identical either way.
	tmpFile, err := os.CreateTemp(filepath.Dir(chunkPath), ".chunk-*.tmp")
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.Write(encrypted); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("write chunk: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("close temp file: %w", err)
	}

	onDiskBytes = int64(len(encrypted))

	if err := os.Rename(tmpPath, chunkPath); err != nil {
		_ = os.Remove(tmpPath)
		// On Windows, rename over an existing file fails with "Access is
		// denied." Since convergent encryption guarantees identical content,
		// a concurrent writer already placed the correct file — treat as
		// successful dedup.
		if fileExists(chunkPath) {
			return 0, nil // concurrent dedup
		}
		return 0, fmt.Errorf("rename chunk: %w", err)
	}

	return onDiskBytes, nil
}

// ReadChunk retrieves and decrypts a chunk by its hash.
// Pipeline: read -> decrypt -> decompress -> verify hash -> return plaintext
func (c *CAS) ReadChunk(ctx context.Context, hash string) ([]byte, error) {
	chunkPath := c.chunkPath(hash)

	// Safe without lock: writes use atomic rename, so readers always see
	// either the complete file or get "not found".
	encrypted, err := os.ReadFile(chunkPath)

	if os.IsNotExist(err) {
		return nil, fmt.Errorf("chunk not found: %s", hash)
	}
	if err != nil {
		return nil, fmt.Errorf("read chunk: %w", err)
	}

	// Decrypt
	compressed, err := c.decrypt(encrypted, hash)
	if err != nil {
		// MAC failure means the stored bytes were encrypted with a different key
		// (e.g., replicated from a peer before all coordinators adopted the shared
		// CAS key). Delete the corrupted file so the caller can re-fetch from a
		// peer that has the correct plaintext, which will re-encrypt with our key.
		if strings.Contains(err.Error(), macFailureMsg) {
			// Gate the callback on a successful remove: if two concurrent ReadChunk
			// calls both hit the same corrupt chunk, only the first os.Remove
			// succeeds. Without this guard both goroutines would invoke onSelfHeal,
			// double-decrementing statsChunkCount and statsChunkBytes.
			if removeErr := os.Remove(chunkPath); removeErr == nil {
				if c.onSelfHeal != nil {
					c.onSelfHeal(hash, int64(len(encrypted)))
				}
			}
			return nil, fmt.Errorf("chunk not found: %s", hash)
		}
		return nil, fmt.Errorf("decrypt chunk: %w", err)
	}

	// Decompress
	data, err := c.decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress chunk: %w", err)
	}

	// Verify hash matches content (detect corruption)
	actualHash := c.contentHash(data)
	if actualHash != hash {
		return nil, fmt.Errorf("chunk hash mismatch: expected %s, got %s (data corruption)", hash, actualHash)
	}

	return data, nil
}

// ReadChunkRaw reads the raw encrypted+compressed bytes from disk without
// decrypting or decompressing. Used by the replication sender to transfer
// chunks without paying unnecessary crypto overhead.
func (c *CAS) ReadChunkRaw(ctx context.Context, hash string) ([]byte, error) {
	chunkPath := c.chunkPath(hash)

	raw, err := os.ReadFile(chunkPath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("chunk not found: %s", hash)
	}
	if err != nil {
		return nil, fmt.Errorf("read raw chunk: %w", err)
	}

	return raw, nil
}

// WriteChunkRaw writes pre-encrypted+compressed chunk bytes directly to disk.
// Used by the replication receiver to store incoming chunks without re-encrypting.
// The hash is used only for naming/dedup; the caller is responsible for ensuring
// the raw bytes were produced by a CAS with the same master key.
// Returns onDiskBytes=0 if the chunk already exists (dedup hit).
func (c *CAS) WriteChunkRaw(ctx context.Context, hash string, raw []byte) (onDiskBytes int64, err error) {
	chunkPath := c.chunkPath(hash)

	// Dedup check: skip if chunk already exists.
	if fileExists(chunkPath) {
		return 0, nil
	}

	// Write atomically via temp file + rename (same pattern as WriteChunk).
	tmpFile, err := os.CreateTemp(filepath.Dir(chunkPath), ".chunk-*.tmp")
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.Write(raw); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("write raw chunk: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, chunkPath); err != nil {
		_ = os.Remove(tmpPath)
		if fileExists(chunkPath) {
			return 0, nil // concurrent dedup
		}
		return 0, fmt.Errorf("rename raw chunk: %w", err)
	}

	return int64(len(raw)), nil
}

// DeleteChunk removes a chunk from storage and returns the freed bytes.
// Returns 0 if the chunk didn't exist.
func (c *CAS) DeleteChunk(ctx context.Context, hash string) (int64, error) {
	chunkPath := c.chunkPath(hash)

	info, err := os.Stat(chunkPath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("stat chunk: %w", err)
	}
	freedBytes := info.Size()

	// Safe without lock: os.Remove is atomic on POSIX.
	if err := os.Remove(chunkPath); err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("delete chunk: %w", err)
	}
	return freedBytes, nil
}

// ChunkExists checks if a chunk exists in storage.
func (c *CAS) ChunkExists(hash string) bool {
	return fileExists(c.chunkPath(hash))
}

// ChunkSize returns the size of a chunk on disk (encrypted size).
func (c *CAS) ChunkSize(ctx context.Context, hash string) (int64, error) {
	info, err := os.Stat(c.chunkPath(hash))
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// TotalSize returns the total size of all chunks in storage.
func (c *CAS) TotalSize(ctx context.Context) (int64, error) {
	var total int64
	err := filepath.Walk(c.chunksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// contentHash computes SHA-256 of plaintext data.
func (c *CAS) contentHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// chunkPath returns the filesystem path for a chunk.
// Uses two-level directory structure to avoid too many files in one directory.
func (c *CAS) chunkPath(hash string) string {
	// Use first 2 chars as subdirectory: chunks/ab/abcdef123...
	if len(hash) < 2 {
		return filepath.Join(c.chunksDir, hash)
	}
	subdir := hash[:2]
	// Only call MkdirAll once per unique subdir prefix; there are at most 256
	// possible values (00–ff) so knownDirs reaches steady state quickly.
	if _, ok := c.knownDirs.Load(subdir); !ok {
		_ = os.MkdirAll(filepath.Join(c.chunksDir, subdir), 0755)
		c.knownDirs.Store(subdir, struct{}{})
	}
	return filepath.Join(c.chunksDir, subdir, hash)
}

// deriveChunkKey derives a per-chunk encryption key using HKDF.
// This enables convergent encryption: same plaintext -> same key -> same ciphertext.
func (c *CAS) deriveChunkKey(hash string) ([32]byte, error) {
	var key [32]byte
	hkdfReader := hkdf.New(sha256.New, c.masterKey[:], []byte(hash), []byte("tunnelmesh-s3-chunk"))
	if _, err := io.ReadFull(hkdfReader, key[:]); err != nil {
		return key, fmt.Errorf("derive chunk key: %w", err)
	}
	return key, nil
}

// deriveNonce derives a deterministic nonce from the master key and content hash.
// This enables convergent encryption where same content produces same ciphertext,
// while keeping nonces unpredictable to attackers who don't know the master key.
func (c *CAS) deriveNonce(hash string) ([24]byte, error) {
	var nonce [24]byte
	// Derive nonce from masterKey || hash to keep nonces secret
	hkdfReader := hkdf.New(sha256.New, append(c.masterKey[:], []byte(hash)...), nil, []byte("tunnelmesh-s3-nonce"))
	if _, err := io.ReadFull(hkdfReader, nonce[:]); err != nil {
		return nonce, fmt.Errorf("derive nonce: %w", err)
	}
	return nonce, nil
}

// encrypt encrypts data using XChaCha20-Poly1305 with convergent encryption.
func (c *CAS) encrypt(plaintext []byte, hash string) ([]byte, error) {
	key, err := c.deriveChunkKey(hash)
	if err != nil {
		return nil, err
	}

	nonce, err := c.deriveNonce(hash)
	if err != nil {
		return nil, err
	}

	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	// Encrypt in place with authentication tag
	ciphertext := aead.Seal(nil, nonce[:], plaintext, nil)
	return ciphertext, nil
}

// decrypt decrypts data using XChaCha20-Poly1305.
func (c *CAS) decrypt(ciphertext []byte, hash string) ([]byte, error) {
	key, err := c.deriveChunkKey(hash)
	if err != nil {
		return nil, err
	}

	nonce, err := c.deriveNonce(hash)
	if err != nil {
		return nil, err
	}

	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	plaintext, err := aead.Open(nil, nonce[:], ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}

// compress compresses data using zstd.
func (c *CAS) compress(data []byte) ([]byte, error) {
	return c.encoder.EncodeAll(data, nil), nil
}

// decompress decompresses zstd-compressed data.
func (c *CAS) decompress(data []byte) ([]byte, error) {
	dec := c.decoderPool.Get().(*zstd.Decoder)
	defer c.decoderPool.Put(dec)

	return dec.DecodeAll(data, nil)
}

// fileExists checks if a file exists.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ContentHash computes the SHA-256 hash of data (exported for use by Store).
func ContentHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
