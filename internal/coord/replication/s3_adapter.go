package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

// S3StoreAdapter adapts the internal s3.Store to the replication.S3Store interface.
type S3StoreAdapter struct {
	store *s3.Store
}

// NewS3StoreAdapter creates a new S3 store adapter for replication.
func NewS3StoreAdapter(store *s3.Store) *S3StoreAdapter {
	return &S3StoreAdapter{store: store}
}

// Get retrieves an object from S3.
func (a *S3StoreAdapter) Get(ctx context.Context, bucket, key string) ([]byte, map[string]string, error) {
	reader, meta, err := a.store.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, nil, fmt.Errorf("get object: %w", err)
	}
	defer func() { _ = reader.Close() }()

	// Read all data
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("read object data: %w", err)
	}

	// Extract metadata
	metadata := make(map[string]string)
	if meta.Metadata != nil {
		for k, v := range meta.Metadata {
			metadata[k] = v
		}
	}

	return data, metadata, nil
}

// Put stores an object in S3.
func (a *S3StoreAdapter) Put(ctx context.Context, bucket, key string, data []byte, contentType string, metadata map[string]string) error {
	reader := bytes.NewReader(data)
	size := int64(len(data))

	_, err := a.store.PutObject(ctx, bucket, key, reader, size, contentType, metadata)
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}

	return nil
}

// List lists all objects in a bucket with pagination support.
func (a *S3StoreAdapter) List(ctx context.Context, bucket string) ([]string, error) {
	var allKeys []string
	marker := ""
	const maxKeys = 1000

	// Paginate through all objects
	for {
		objects, _, nextMarker, err := a.store.ListObjects(ctx, bucket, "", marker, maxKeys)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}

		// Collect keys from this page (all objects in meta/ are live)
		for _, obj := range objects {
			allKeys = append(allKeys, obj.Key)
		}

		// Check if there are more pages
		if nextMarker == "" {
			break
		}
		marker = nextMarker

		// Check context cancellation between pages
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("list objects in bucket %s cancelled: %w", bucket, ctx.Err())
		default:
		}
	}

	return allKeys, nil
}

// Delete removes an object from S3.
func (a *S3StoreAdapter) Delete(ctx context.Context, bucket, key string) error {
	err := a.store.DeleteObject(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	return nil
}

// ListBuckets lists all buckets.
func (a *S3StoreAdapter) ListBuckets(ctx context.Context) ([]string, error) {
	buckets, err := a.store.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}

	names := make([]string, 0, len(buckets))
	for _, bucket := range buckets {
		names = append(names, bucket.Name)
	}

	return names, nil
}

// GetObjectMeta retrieves object metadata without loading chunk data (Phase 4).
func (a *S3StoreAdapter) GetObjectMeta(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	meta, err := a.store.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("get object meta: %w", err)
	}

	// Convert s3.ObjectMeta to replication.ObjectMeta
	result := &ObjectMeta{
		Key:           meta.Key,
		Size:          meta.Size,
		ContentType:   meta.ContentType,
		Metadata:      meta.Metadata,
		Chunks:        meta.Chunks,
		VersionVector: meta.VersionVector,
	}

	// Convert chunk metadata
	if meta.ChunkMetadata != nil {
		result.ChunkMetadata = make(map[string]*ChunkMetadata, len(meta.ChunkMetadata))
		for hash, chunkMeta := range meta.ChunkMetadata {
			result.ChunkMetadata[hash] = &ChunkMetadata{
				Hash:          chunkMeta.Hash,
				Size:          chunkMeta.Size,
				VersionVector: chunkMeta.VersionVector,
			}
		}
	}

	return result, nil
}

// ChunkExists checks if a chunk exists in CAS without reading its data.
func (a *S3StoreAdapter) ChunkExists(_ context.Context, hash string) bool {
	return a.store.ChunkExists(hash)
}

// ReadChunk reads a chunk from CAS by hash (Phase 4).
func (a *S3StoreAdapter) ReadChunk(ctx context.Context, hash string) ([]byte, error) {
	data, err := a.store.ReadChunk(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("read chunk: %w", err)
	}

	return data, nil
}

// WriteChunkDirect writes chunk data directly to CAS (Phase 4).
func (a *S3StoreAdapter) WriteChunkDirect(ctx context.Context, hash string, data []byte) error {
	err := a.store.WriteChunkDirect(ctx, hash, data)
	if err != nil {
		return fmt.Errorf("write chunk direct: %w", err)
	}

	return nil
}

// ImportObjectMeta writes object metadata directly (for replication receiver).
func (a *S3StoreAdapter) ImportObjectMeta(ctx context.Context, bucket, key string, metaJSON []byte, bucketOwner string) ([]string, error) {
	chunks, err := a.store.ImportObjectMeta(ctx, bucket, key, metaJSON, bucketOwner)
	if err != nil {
		return nil, fmt.Errorf("import object meta: %w", err)
	}

	return chunks, nil
}

// DeleteUnreferencedChunks checks each chunk hash and deletes unreferenced ones.
func (a *S3StoreAdapter) DeleteUnreferencedChunks(ctx context.Context, chunkHashes []string) int64 {
	return a.store.DeleteUnreferencedChunks(ctx, chunkHashes)
}

// GetBucketReplicationFactor returns the replication factor for a bucket.
func (a *S3StoreAdapter) GetBucketReplicationFactor(ctx context.Context, bucket string) int {
	meta, err := a.store.HeadBucket(ctx, bucket)
	if err != nil {
		return 0
	}
	return meta.ReplicationFactor
}

// DeleteChunk removes a chunk from CAS by hash (for cleanup after replication).
func (a *S3StoreAdapter) DeleteChunk(ctx context.Context, hash string) error {
	err := a.store.DeleteChunk(ctx, hash)
	if err != nil {
		return fmt.Errorf("delete chunk: %w", err)
	}

	return nil
}

// PurgeObject permanently removes an object, its versions, and unreferenced chunks.
func (a *S3StoreAdapter) PurgeObject(ctx context.Context, bucket, key string) error {
	err := a.store.PurgeObject(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("purge object: %w", err)
	}

	return nil
}

// GetVersionHistory returns all version entries for an object.
func (a *S3StoreAdapter) GetVersionHistory(ctx context.Context, bucket, key string) ([]VersionEntry, error) {
	entries, err := a.store.GetVersionHistory(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("get version history: %w", err)
	}

	result := make([]VersionEntry, len(entries))
	for i, e := range entries {
		result[i] = VersionEntry{
			VersionID: e.VersionID,
			MetaJSON:  json.RawMessage(e.MetaJSON),
		}
	}
	return result, nil
}

// ImportVersionHistory imports version entries, deduplicating by versionID.
func (a *S3StoreAdapter) ImportVersionHistory(ctx context.Context, bucket, key string, versions []VersionEntry) (int, []string, error) {
	// Convert replication.VersionEntry to s3.VersionHistoryEntry
	storeVersions := make([]s3.VersionHistoryEntry, len(versions))
	for i, v := range versions {
		storeVersions[i] = s3.VersionHistoryEntry{
			VersionID: v.VersionID,
			MetaJSON:  []byte(v.MetaJSON),
		}
	}

	count, chunksToCheck, err := a.store.ImportVersionHistory(ctx, bucket, key, storeVersions)
	if err != nil {
		return 0, nil, fmt.Errorf("import version history: %w", err)
	}
	return count, chunksToCheck, nil
}

// GetBucketOwner returns the owner of a bucket (empty string if not found).
func (a *S3StoreAdapter) GetBucketOwner(ctx context.Context, bucket string) string {
	return a.store.GetBucketOwner(ctx, bucket)
}

// GetAllObjectKeys returns all object keys grouped by bucket.
func (a *S3StoreAdapter) GetAllObjectKeys(ctx context.Context) (map[string][]string, error) {
	result, err := a.store.GetAllObjectKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("get all object keys: %w", err)
	}
	return result, nil
}
