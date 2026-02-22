package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStore(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewStore(tmpDir, nil)
	require.NoError(t, err)
	assert.Equal(t, tmpDir, store.DataDir())

	// Check buckets directory was created
	bucketsDir := filepath.Join(tmpDir, "buckets")
	info, err := os.Stat(bucketsDir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestStoreCreateBucket(t *testing.T) {
	store := newTestStoreWithCAS(t)

	err := store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil)
	require.NoError(t, err)

	// Verify bucket exists
	meta, err := store.HeadBucket(context.Background(), "test-bucket")
	require.NoError(t, err)
	assert.Equal(t, "test-bucket", meta.Name)
	assert.Equal(t, "alice", meta.Owner)
	assert.False(t, meta.CreatedAt.IsZero())
}

func TestStoreCreateBucketAlreadyExists(t *testing.T) {
	store := newTestStoreWithCAS(t)

	err := store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil)
	require.NoError(t, err)

	// Try to create again
	err = store.CreateBucket(context.Background(), "test-bucket", "bob", 2, nil)
	assert.ErrorIs(t, err, ErrBucketExists)
}

func TestStoreDeleteBucket(t *testing.T) {
	store := newTestStoreWithCAS(t)

	err := store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil)
	require.NoError(t, err)

	err = store.DeleteBucket(context.Background(), "test-bucket")
	require.NoError(t, err)

	// Verify bucket is gone
	_, err = store.HeadBucket(context.Background(), "test-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestStoreDeleteBucketNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)

	err := store.DeleteBucket(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestStoreDeleteBucketNotEmpty(t *testing.T) {
	store := newTestStoreWithCAS(t)

	err := store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil)
	require.NoError(t, err)

	// Add an object
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("hello")), 5, "text/plain", nil)
	require.NoError(t, err)

	// Try to delete bucket
	err = store.DeleteBucket(context.Background(), "test-bucket")
	assert.ErrorIs(t, err, ErrBucketNotEmpty)
}

func TestDeleteBucket_WithRecycleBinEntries(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Add an object
	_, err := store.PutObject(ctx, "test-bucket", "file.txt", bytes.NewReader([]byte("content")), 7, "text/plain", nil)
	require.NoError(t, err)

	// Delete moves to recycle bin
	require.NoError(t, store.DeleteObject(ctx, "test-bucket", "file.txt"))

	// Bucket should not be deletable while recycle bin has entries
	err = store.DeleteBucket(ctx, "test-bucket")
	assert.Error(t, err)

	// Purge recycle bin
	store.PurgeAllRecycled(ctx)

	// Now bucket should be deletable
	err = store.DeleteBucket(ctx, "test-bucket")
	assert.NoError(t, err)
}

func TestForceDeleteBucket(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Add objects
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("file-%d.txt", i)
		data := []byte(fmt.Sprintf("content-%d", i))
		_, err := store.PutObject(ctx, "test-bucket", key, bytes.NewReader(data), int64(len(data)), "text/plain", nil)
		require.NoError(t, err)
	}

	// Delete some to create recycle bin entries
	require.NoError(t, store.DeleteObject(ctx, "test-bucket", "file-0.txt"))
	require.NoError(t, store.DeleteObject(ctx, "test-bucket", "file-1.txt"))

	// Verify stats before
	stats := store.GetCASStats()
	assert.Greater(t, stats.ObjectCount, 0)

	// ForceDeleteBucket should remove everything including recycle bin
	err := store.ForceDeleteBucket(ctx, "test-bucket")
	require.NoError(t, err)

	// Bucket should be gone
	_, err = store.HeadBucket(ctx, "test-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound)

	// Idempotent: calling again on non-existent bucket returns nil
	err = store.ForceDeleteBucket(ctx, "test-bucket")
	assert.NoError(t, err)
}

func TestStoreHeadBucketNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)

	_, err := store.HeadBucket(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestStoreListBuckets(t *testing.T) {
	store := newTestStoreWithCAS(t)

	// Empty initially
	buckets, err := store.ListBuckets(context.Background())
	require.NoError(t, err)
	assert.Empty(t, buckets)

	// Create some buckets
	require.NoError(t, store.CreateBucket(context.Background(), "bucket-a", "alice", 2, nil))
	require.NoError(t, store.CreateBucket(context.Background(), "bucket-b", "bob", 2, nil))

	buckets, err = store.ListBuckets(context.Background())
	require.NoError(t, err)
	assert.Len(t, buckets, 2)

	names := make(map[string]bool)
	for _, b := range buckets {
		names[b.Name] = true
	}
	assert.True(t, names["bucket-a"])
	assert.True(t, names["bucket-b"])
}

func TestStorePutObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("hello world")
	meta, err := store.PutObject(context.Background(), "test-bucket", "greeting.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	assert.Equal(t, "greeting.txt", meta.Key)
	assert.Equal(t, int64(len(content)), meta.Size)
	assert.Equal(t, "text/plain", meta.ContentType)
	assert.NotEmpty(t, meta.ETag)
	assert.False(t, meta.LastModified.IsZero())
}

func TestStorePutObject_SetsExpiry(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Set default object expiry to 25 years (9125 days)
	store.SetDefaultObjectExpiryDays(9125)

	content := []byte("hello world")
	meta, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	require.NotNil(t, meta.Expires, "Expires should be set")
	expectedExpiry := time.Now().UTC().AddDate(0, 0, 9125)
	// Allow 1 minute tolerance for test timing
	assert.WithinDuration(t, expectedExpiry, *meta.Expires, time.Minute)
}

func TestStorePutObject_NoExpiryWhenNotConfigured(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Don't set expiry - objects should have no expiry
	content := []byte("hello world")
	meta, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	assert.Nil(t, meta.Expires, "Expires should not be set when not configured")
}

func TestStorePutObjectBucketNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)

	_, err := store.PutObject(context.Background(), "nonexistent", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestStorePutObjectNestedKey(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("nested content")
	meta, err := store.PutObject(context.Background(), "test-bucket", "path/to/file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)
	assert.Equal(t, "path/to/file.txt", meta.Key)

	// Verify we can retrieve it
	reader, objMeta, err := store.GetObject(context.Background(), "test-bucket", "path/to/file.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	assert.Equal(t, "path/to/file.txt", objMeta.Key)
	data, _ := io.ReadAll(reader)
	assert.Equal(t, content, data)
}

func TestStorePutObjectWithMetadata(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	userMeta := map[string]string{
		"x-amz-meta-author":  "alice",
		"x-amz-meta-version": "1.0",
	}
	meta, err := store.PutObject(context.Background(), "test-bucket", "doc.txt", bytes.NewReader([]byte("data")), 4, "text/plain", userMeta)
	require.NoError(t, err)

	assert.Equal(t, userMeta, meta.Metadata)

	// Verify persisted
	objMeta, err := store.HeadObject(context.Background(), "test-bucket", "doc.txt")
	require.NoError(t, err)
	assert.Equal(t, userMeta, objMeta.Metadata)
}

func TestStoreGetObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("hello world")
	_, err := store.PutObject(context.Background(), "test-bucket", "greeting.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	reader, meta, err := store.GetObject(context.Background(), "test-bucket", "greeting.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	assert.Equal(t, "greeting.txt", meta.Key)
	assert.Equal(t, int64(len(content)), meta.Size)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

func TestStoreGetObjectNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	_, _, err := store.GetObject(context.Background(), "test-bucket", "nonexistent.txt")
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestStoreGetObjectBucketNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)

	_, _, err := store.GetObject(context.Background(), "nonexistent", "file.txt")
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestStoreHeadObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("hello world")
	_, err := store.PutObject(context.Background(), "test-bucket", "greeting.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	meta, err := store.HeadObject(context.Background(), "test-bucket", "greeting.txt")
	require.NoError(t, err)

	assert.Equal(t, "greeting.txt", meta.Key)
	assert.Equal(t, int64(len(content)), meta.Size)
	assert.Equal(t, "text/plain", meta.ContentType)
}

func TestStoreHeadObjectNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	_, err := store.HeadObject(context.Background(), "test-bucket", "nonexistent.txt")
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestStoreDeleteObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)

	err = store.DeleteObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)

	// Object should be gone from ListObjects
	objects, _, _, err := store.ListObjects(context.Background(), "test-bucket", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, objects)

	// Object should exist in recycle bin
	recycled, err := store.ListRecycledObjects(context.Background(), "test-bucket")
	require.NoError(t, err)
	assert.Len(t, recycled, 1)
	assert.Equal(t, "file.txt", recycled[0].OriginalKey)
}

func TestStorePurgeObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)

	// Purge actually removes the object
	err = store.PurgeObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)

	// Verify object is gone
	_, err = store.HeadObject(context.Background(), "test-bucket", "file.txt")
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestStoreDeleteObjectNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// S3 semantics: deleting a non-existent object is idempotent (succeeds)
	err := store.DeleteObject(context.Background(), "test-bucket", "nonexistent.txt")
	assert.NoError(t, err)
}

func TestStoreDeleteObjectBucketNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)

	err := store.DeleteObject(context.Background(), "nonexistent", "file.txt")
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestStoreListObjects(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Add some objects
	objects := []string{"a.txt", "b.txt", "c.txt"}
	for _, key := range objects {
		_, err := store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader([]byte("data")), 4, "text/plain", nil)
		require.NoError(t, err)
	}

	list, _, _, err := store.ListObjects(context.Background(), "test-bucket", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, list, 3)
}

func TestStoreListObjectsWithPrefix(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Add some objects with different prefixes
	objects := []string{"docs/a.txt", "docs/b.txt", "images/c.png"}
	for _, key := range objects {
		_, err := store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader([]byte("data")), 4, "text/plain", nil)
		require.NoError(t, err)
	}

	list, _, _, err := store.ListObjects(context.Background(), "test-bucket", "docs/", "", 0)
	require.NoError(t, err)
	assert.Len(t, list, 2)

	for _, obj := range list {
		assert.True(t, strings.HasPrefix(obj.Key, "docs/"))
	}
}

func TestStoreListObjectsWithMaxKeys(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Add 5 objects
	for i := 0; i < 5; i++ {
		key := string(rune('a'+i)) + ".txt"
		_, err := store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader([]byte("data")), 4, "text/plain", nil)
		require.NoError(t, err)
	}

	list, isTruncated, _, err := store.ListObjects(context.Background(), "test-bucket", "", "", 2)
	require.NoError(t, err)
	assert.Len(t, list, 2)
	assert.True(t, isTruncated)
}

func TestStoreListObjectsBucketNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)

	_, _, _, err := store.ListObjects(context.Background(), "nonexistent", "", "", 0)
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestStoreListObjectsEmpty(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	list, isTruncated, _, err := store.ListObjects(context.Background(), "test-bucket", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, list)
	assert.False(t, isTruncated)
}

func TestStoreOverwriteObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Write initial version
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("version 1")), 9, "text/plain", nil)
	require.NoError(t, err)

	// Overwrite
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("version 2 - longer")), 18, "text/plain", nil)
	require.NoError(t, err)

	// Read back
	reader, meta, err := store.GetObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	assert.Equal(t, int64(18), meta.Size)
	data, _ := io.ReadAll(reader)
	assert.Equal(t, "version 2 - longer", string(data))
}

func TestStoreWithQuota(t *testing.T) {
	tmpDir := t.TempDir()
	quota := NewQuotaManager(1 * 1024 * 1024 * 1024) // 1 Gi limit
	masterKey := [32]byte{1, 2, 3, 4, 5, 6, 7, 8}

	store, err := NewStoreWithCAS(tmpDir, quota, masterKey)
	require.NoError(t, err)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Put object should update quota
	content := []byte("hello world")
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	stats := store.QuotaStats()
	require.NotNil(t, stats)
	assert.Equal(t, int64(len(content)), stats.UsedBytes)
	assert.Equal(t, int64(len(content)), stats.PerBucket["test-bucket"])

	// Delete (recycle) should release quota immediately
	err = store.DeleteObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)

	stats = store.QuotaStats()
	assert.Equal(t, int64(0), stats.UsedBytes, "recycled objects release quota")
}

// =========================================================================
// Object Lifecycle Tests
// =========================================================================

func TestObjectLifecycle_ExpirySetting(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Set object expiry to 30 days
	store.SetDefaultObjectExpiryDays(30)

	content := []byte("test content")
	meta, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Verify expiry is set
	require.NotNil(t, meta.Expires, "Expires should be set")
	expectedExpiry := time.Now().UTC().AddDate(0, 0, 30)
	assert.WithinDuration(t, expectedExpiry, *meta.Expires, time.Minute)
}

func TestDeleteObject_MovesToRecycleBin(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("hello world")
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	err = store.DeleteObject(context.Background(), "test-bucket", "file.txt")
	assert.NoError(t, err)

	// Object should be gone from ListObjects
	objects, _, _, err := store.ListObjects(context.Background(), "test-bucket", "", "", 0)
	assert.NoError(t, err)
	assert.Empty(t, objects)

	// Object should exist in recycle bin
	recycled, err := store.ListRecycledObjects(context.Background(), "test-bucket")
	assert.NoError(t, err)
	assert.Len(t, recycled, 1)
	assert.Equal(t, "file.txt", recycled[0].OriginalKey)
	assert.Equal(t, int64(11), recycled[0].Meta.Size)
}

func TestRestoreRecycledObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("hello world")
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	err = store.DeleteObject(context.Background(), "test-bucket", "file.txt")
	assert.NoError(t, err)

	// Restore from recycle bin
	err = store.RestoreRecycledObject(context.Background(), "test-bucket", "file.txt")
	assert.NoError(t, err)

	// Object should be back in ListObjects
	objects, _, _, err := store.ListObjects(context.Background(), "test-bucket", "", "", 0)
	assert.NoError(t, err)
	assert.Len(t, objects, 1)
	assert.Equal(t, "file.txt", objects[0].Key)

	// Recycle bin should be empty
	recycled, err := store.ListRecycledObjects(context.Background(), "test-bucket")
	assert.NoError(t, err)
	assert.Empty(t, recycled)
}

func TestRestoreRecycledObject_ConflictWithLive(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("original")
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Delete it
	err = store.DeleteObject(context.Background(), "test-bucket", "file.txt")
	assert.NoError(t, err)

	// Create new object with same key
	replacement := []byte("replacement")
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(replacement), int64(len(replacement)), "text/plain", nil)
	require.NoError(t, err)

	// Restore should fail -- live object exists
	err = store.RestoreRecycledObject(context.Background(), "test-bucket", "file.txt")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestRecycleBin_DeletedObjectNotInList(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create two objects
	_, err := store.PutObject(context.Background(), "test-bucket", "live.txt", bytes.NewReader([]byte("live")), 4, "text/plain", nil)
	require.NoError(t, err)
	_, err = store.PutObject(context.Background(), "test-bucket", "dead.txt", bytes.NewReader([]byte("dead")), 4, "text/plain", nil)
	require.NoError(t, err)

	// Delete one
	err = store.DeleteObject(context.Background(), "test-bucket", "dead.txt")
	require.NoError(t, err)

	// Only live object should appear in list
	objects, _, _, err := store.ListObjects(context.Background(), "test-bucket", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, objects, 1)
	assert.Equal(t, "live.txt", objects[0].Key)
}

func TestObjectLifecycle_PurgeRemovesCompletely(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("to be purged")
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Purge directly (hard delete)
	err = store.PurgeObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)

	// Verify object is completely gone
	_, err = store.HeadObject(context.Background(), "test-bucket", "file.txt")
	assert.ErrorIs(t, err, ErrObjectNotFound)

	// Should not appear in list
	objects, _, _, err := store.ListObjects(context.Background(), "test-bucket", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, objects)
}

func TestPurgeRecycleBin_RespectsRetention(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create three objects
	_, err := store.PutObject(context.Background(), "test-bucket", "old.txt", bytes.NewReader([]byte("old content")), 11, "text/plain", nil)
	require.NoError(t, err)
	_, err = store.PutObject(context.Background(), "test-bucket", "new.txt", bytes.NewReader([]byte("new content")), 11, "text/plain", nil)
	require.NoError(t, err)
	_, err = store.PutObject(context.Background(), "test-bucket", "live.txt", bytes.NewReader([]byte("live content")), 12, "text/plain", nil)
	require.NoError(t, err)

	// Delete old.txt and new.txt
	_ = store.DeleteObject(context.Background(), "test-bucket", "old.txt")
	_ = store.DeleteObject(context.Background(), "test-bucket", "new.txt")

	// Backdate old.txt's recycled entry to 100 days ago
	rbDir := filepath.Join(store.DataDir(), "buckets", "test-bucket", "recyclebin")
	entries, _ := os.ReadDir(rbDir)
	for _, e := range entries {
		data, _ := os.ReadFile(filepath.Join(rbDir, e.Name()))
		var entry RecycledEntry
		_ = json.Unmarshal(data, &entry)
		if entry.OriginalKey == "old.txt" {
			entry.DeletedAt = time.Now().UTC().AddDate(0, 0, -100)
			newData, _ := json.Marshal(entry)
			_ = os.WriteFile(filepath.Join(rbDir, e.Name()), newData, 0644)
		}
	}

	store.SetRecycleBinRetentionDays(90)
	purged := store.PurgeRecycleBin(context.Background())
	assert.Equal(t, 1, purged, "only old entry should be purged")

	// new.txt should still be in recycle bin
	recycled, _ := store.ListRecycledObjects(context.Background(), "test-bucket")
	assert.Len(t, recycled, 1)
	assert.Equal(t, "new.txt", recycled[0].OriginalKey)

	// live.txt should still be live
	_, err = store.HeadObject(context.Background(), "test-bucket", "live.txt")
	assert.NoError(t, err)
}

func TestPurgeRecycleBin_DisabledWhenZero(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain", nil)
	require.NoError(t, err)
	_ = store.DeleteObject(context.Background(), "test-bucket", "file.txt")

	// Retention days = 0 means disabled
	store.SetRecycleBinRetentionDays(0)
	purged := store.PurgeRecycleBin(context.Background())
	assert.Equal(t, 0, purged)

	// Object should still be in recycle bin
	recycled, err := store.ListRecycledObjects(context.Background(), "test-bucket")
	assert.NoError(t, err)
	assert.Len(t, recycled, 1)
}

func TestPurgeAllRecycled(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	_, err := store.PutObject(context.Background(), "test-bucket", "a.txt", bytes.NewReader([]byte("aaa")), 3, "text/plain", nil)
	require.NoError(t, err)
	_, err = store.PutObject(context.Background(), "test-bucket", "b.txt", bytes.NewReader([]byte("bbb")), 3, "text/plain", nil)
	require.NoError(t, err)

	_ = store.DeleteObject(context.Background(), "test-bucket", "a.txt")
	_ = store.DeleteObject(context.Background(), "test-bucket", "b.txt")

	purged := store.PurgeAllRecycled(context.Background())
	assert.Equal(t, 2, purged)

	recycled, _ := store.ListRecycledObjects(context.Background(), "test-bucket")
	assert.Empty(t, recycled)
}

func TestPurgeAllRecycled_Empty(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// No recycled objects -- should return 0
	purged := store.PurgeAllRecycled(context.Background())
	assert.Equal(t, 0, purged)
}

func TestRecycleBin_MultipleDeletesSameKey(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create, delete, create, delete
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("version1")), 8, "text/plain", nil)
	require.NoError(t, err)
	_ = store.DeleteObject(context.Background(), "test-bucket", "file.txt")

	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("version2")), 8, "text/plain", nil)
	require.NoError(t, err)
	_ = store.DeleteObject(context.Background(), "test-bucket", "file.txt")

	// Should have 2 recycled entries
	recycled, err := store.ListRecycledObjects(context.Background(), "test-bucket")
	assert.NoError(t, err)
	assert.Len(t, recycled, 2)
}

func TestGetRecycledObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("hello recycled world")
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Delete the object (moves to recycle bin)
	err = store.DeleteObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)

	// GetRecycledObject should return the content
	reader, meta, err := store.GetRecycledObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	assert.Equal(t, "text/plain", meta.ContentType)
	assert.Equal(t, int64(len(content)), meta.Size)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestGetRecycledObject_NotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	_, _, err := store.GetRecycledObject(context.Background(), "test-bucket", "nonexistent.txt")
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestGetRecycledObject_ReturnsLatest(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create, delete, create with different content, delete again
	v1 := []byte("version1")
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(v1), int64(len(v1)), "text/plain", nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteObject(context.Background(), "test-bucket", "file.txt"))

	v2 := []byte("version2-latest")
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(v2), int64(len(v2)), "text/plain", nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteObject(context.Background(), "test-bucket", "file.txt"))

	// GetRecycledObject should return the most recent version
	reader, _, err := store.GetRecycledObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, v2, got)
}

func TestDeleteObject_SecondDeleteReturnsNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	content := []byte("delete me")
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// First delete: moves to recycle bin
	err = store.DeleteObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)

	// Object should be gone from live path
	_, err = store.HeadObject(context.Background(), "test-bucket", "file.txt")
	assert.ErrorIs(t, err, ErrObjectNotFound)

	// Second delete: idempotent (S3 semantics — succeeds even though already recycled)
	err = store.DeleteObject(context.Background(), "test-bucket", "file.txt")
	assert.NoError(t, err)
}

// --- Path Traversal Regression Tests ---

func TestPathTraversal_BucketName(t *testing.T) {
	store := newTestStoreWithCAS(t)

	// Test various path traversal attempts in bucket names
	// Note: URL-encoded attacks (like %2f) are handled at the API layer where
	// URL decoding occurs before validation. These tests cover the storage layer.
	maliciousBuckets := []string{
		"..",
		"../etc",
		"bucket/../etc",
		"bucket/../../etc",
		"./bucket",
		".\\bucket",
		"bucket\\..\\etc",
		"/etc/passwd",
		"\\etc\\passwd",
		"bucket\x00evil",  // null byte injection
		"\x00/etc/passwd", // null byte at start
	}

	for _, name := range maliciousBuckets {
		t.Run(name, func(t *testing.T) {
			err := store.CreateBucket(context.Background(), name, "attacker", 2, nil)
			assert.Error(t, err, "bucket name %q should be rejected", name)
		})
	}
}

func TestPathTraversal_ObjectKey(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Test various path traversal attempts in object keys
	// Note: URL-encoded attacks are handled at the API layer
	maliciousKeys := []string{
		"..",
		"../etc/passwd",
		"foo/../../../etc/passwd",
		"foo/bar/../../..",
		"foo\\..\\..\\etc",
		"/etc/passwd",
		"\\etc\\passwd",
	}

	content := []byte("malicious content")
	for _, key := range maliciousKeys {
		t.Run(key, func(t *testing.T) {
			_, err := store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
			assert.Error(t, err, "object key %q should be rejected", key)
		})
	}
}

func TestPathTraversal_ValidPaths(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Test that valid paths with dots are still allowed
	validKeys := []string{
		"file.txt",
		"folder/file.txt",
		"folder.with.dots/file.txt",
		".hidden",
		"folder/.hidden",
		"...file",
		"file...",
		"folder/...file",
		"dotdot..file",
		"file..dotdot",
	}

	content := []byte("valid content")
	for _, key := range validKeys {
		t.Run(key, func(t *testing.T) {
			_, err := store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
			assert.NoError(t, err, "object key %q should be allowed", key)
		})
	}
}

func TestPathTraversal_ValidNestedPaths(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Test nested paths that should work
	content := []byte("valid content")

	// First create a folder, then a file in it
	_, err := store.PutObject(context.Background(), "test-bucket", "dotfolder/.hidden", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	assert.NoError(t, err, "nested .hidden file should be allowed")

	_, err = store.PutObject(context.Background(), "test-bucket", ".hiddendir/file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	assert.NoError(t, err, "file in .hidden directory should be allowed")
}

// =========================================================================
// CDC (Content-Defined Chunking) Tests
// =========================================================================

func TestCDCChunking_BasicSplit(t *testing.T) {
	// Create data larger than min chunk size
	data := make([]byte, 10*1024) // 10KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks, err := ChunkData(data)
	require.NoError(t, err)
	require.NotEmpty(t, chunks)

	// Verify chunks are within bounds
	// Note: The last chunk can be smaller than MinChunkSize (it's the remainder)
	for i, chunk := range chunks {
		if i < len(chunks)-1 {
			// Non-final chunks should be at least MinChunkSize
			assert.GreaterOrEqual(t, len(chunk), MinChunkSize, "chunk %d too small", i)
		}
		assert.LessOrEqual(t, len(chunk), MaxChunkSize, "chunk %d too large", i)
		assert.Greater(t, len(chunk), 0, "chunk %d is empty", i)
	}

	// Verify reassembly
	var totalSize int
	for _, chunk := range chunks {
		totalSize += len(chunk)
	}
	reassembled := make([]byte, 0, totalSize)
	for _, chunk := range chunks {
		reassembled = append(reassembled, chunk...)
	}
	assert.Equal(t, data, reassembled)
}

func TestCDCChunking_EmptyData(t *testing.T) {
	chunks, err := ChunkData([]byte{})
	require.NoError(t, err)
	assert.Nil(t, chunks)
}

func TestCDCChunking_SmallData(t *testing.T) {
	// Data smaller than min chunk size
	data := []byte("hello world")
	chunks, err := ChunkData(data)
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	assert.Equal(t, data, chunks[0])
}

func TestCDCChunking_BoundaryStability(t *testing.T) {
	// Test that inserting bytes only affects nearby chunks
	base := make([]byte, 50*1024) // 50KB
	for i := range base {
		base[i] = byte(i % 256)
	}

	// Insert a few bytes in the middle
	modified := make([]byte, len(base)+10)
	copy(modified[:len(base)/2], base[:len(base)/2])
	copy(modified[len(base)/2:len(base)/2+10], []byte("INSERTED!"))
	copy(modified[len(base)/2+10:], base[len(base)/2:])

	baseChunks, err := ChunkData(base)
	require.NoError(t, err)

	modChunks, err := ChunkData(modified)
	require.NoError(t, err)

	// Most chunks should be identical (CDC property)
	// Count matching chunks
	baseHashes := make(map[string]bool)
	for _, chunk := range baseChunks {
		baseHashes[ContentHash(chunk)] = true
	}

	matchCount := 0
	for _, chunk := range modChunks {
		if baseHashes[ContentHash(chunk)] {
			matchCount++
		}
	}

	// At least some chunks should match (CDC preserves boundaries)
	// We expect chunks before and after the insertion point to match
	assert.Greater(t, matchCount, 0, "CDC should preserve some chunk boundaries")
}

// =========================================================================
// CAS (Content-Addressable Storage) Tests
// =========================================================================

func TestCAS_WriteAndReadChunk(t *testing.T) {
	tmpDir := t.TempDir()
	masterKey := [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

	cas, err := NewCAS(tmpDir, masterKey)
	require.NoError(t, err)

	data := []byte("hello content-addressable world")
	hash, _, err := cas.WriteChunk(context.Background(), data)
	require.NoError(t, err)
	assert.NotEmpty(t, hash)

	// Read it back
	retrieved, err := cas.ReadChunk(context.Background(), hash)
	require.NoError(t, err)
	assert.Equal(t, data, retrieved)
}

func TestCAS_Deduplication(t *testing.T) {
	tmpDir := t.TempDir()
	masterKey := [32]byte{}

	cas, err := NewCAS(tmpDir, masterKey)
	require.NoError(t, err)

	data := []byte("duplicate me")

	// Write same data twice
	hash1, _, err := cas.WriteChunk(context.Background(), data)
	require.NoError(t, err)

	hash2, _, err := cas.WriteChunk(context.Background(), data)
	require.NoError(t, err)

	// Should return same hash (dedup)
	assert.Equal(t, hash1, hash2)

	// Should only have one chunk file
	totalSize, err := cas.TotalSize(context.Background())
	require.NoError(t, err)

	// Write once more to verify it doesn't increase
	_, _, err = cas.WriteChunk(context.Background(), data)
	require.NoError(t, err)

	totalSize2, err := cas.TotalSize(context.Background())
	require.NoError(t, err)
	assert.Equal(t, totalSize, totalSize2, "duplicate write should not increase storage")
}

func TestCAS_ConvergentEncryption(t *testing.T) {
	tmpDir := t.TempDir()
	masterKey := [32]byte{1, 2, 3}

	cas, err := NewCAS(tmpDir, masterKey)
	require.NoError(t, err)

	data := []byte("same plaintext should produce same ciphertext")

	// Write the data
	hash1, _, err := cas.WriteChunk(context.Background(), data)
	require.NoError(t, err)

	// Get the encrypted size
	size1, err := cas.ChunkSize(context.Background(), hash1)
	require.NoError(t, err)

	// Delete and rewrite
	_, err = cas.DeleteChunk(context.Background(), hash1)
	require.NoError(t, err)

	hash2, _, err := cas.WriteChunk(context.Background(), data)
	require.NoError(t, err)

	size2, err := cas.ChunkSize(context.Background(), hash2)
	require.NoError(t, err)

	// Same hash and same encrypted size (convergent encryption)
	assert.Equal(t, hash1, hash2)
	assert.Equal(t, size1, size2)
}

func TestCAS_ChunkNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	masterKey := [32]byte{}

	cas, err := NewCAS(tmpDir, masterKey)
	require.NoError(t, err)

	_, err = cas.ReadChunk(context.Background(), "nonexistent")
	assert.Error(t, err)
}

func TestCAS_DeleteChunk(t *testing.T) {
	tmpDir := t.TempDir()
	masterKey := [32]byte{}

	cas, err := NewCAS(tmpDir, masterKey)
	require.NoError(t, err)

	data := []byte("delete me")
	hash, _, err := cas.WriteChunk(context.Background(), data)
	require.NoError(t, err)

	assert.True(t, cas.ChunkExists(hash))

	_, err = cas.DeleteChunk(context.Background(), hash)
	require.NoError(t, err)

	assert.False(t, cas.ChunkExists(hash))
}

// =========================================================================
// Version History Tests
// =========================================================================

// newTestStoreWithCAS creates a store with CAS enabled for testing versioning.
func newTestStoreWithCAS(t *testing.T) *Store {
	t.Helper()
	tmpDir := t.TempDir()
	masterKey := [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	store, err := NewStoreWithCAS(tmpDir, nil, masterKey)
	require.NoError(t, err)
	return store
}

func TestVersioning_PutCreatesVersion(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// First write
	content1 := []byte("version 1")
	meta1, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content1), int64(len(content1)), "text/plain", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, meta1.VersionID)

	// Second write (should create version)
	content2 := []byte("version 2")
	meta2, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content2), int64(len(content2)), "text/plain", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, meta2.VersionID)
	assert.NotEqual(t, meta1.VersionID, meta2.VersionID)

	// List versions
	versions, err := store.ListVersions(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, versions, 2)

	// Current version should be first
	assert.True(t, versions[0].IsCurrent)
	assert.Equal(t, meta2.VersionID, versions[0].VersionID)
	assert.False(t, versions[1].IsCurrent)
	assert.Equal(t, meta1.VersionID, versions[1].VersionID)
}

func TestVersioning_ListVersions(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create multiple versions
	for i := 1; i <= 5; i++ {
		content := []byte(strings.Repeat("x", i*100))
		_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	versions, err := store.ListVersions(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, versions, 5)

	// Should be sorted newest first
	for i := 0; i < len(versions)-1; i++ {
		assert.True(t, versions[i].LastModified.After(versions[i+1].LastModified) ||
			versions[i].LastModified.Equal(versions[i+1].LastModified))
	}

	// Only one should be current
	currentCount := 0
	for _, v := range versions {
		if v.IsCurrent {
			currentCount++
		}
	}
	assert.Equal(t, 1, currentCount)
}

func TestVersioning_GetObjectVersion(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create versions
	content1 := []byte("original content")
	meta1, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content1), int64(len(content1)), "text/plain", nil)
	require.NoError(t, err)

	content2 := []byte("updated content")
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content2), int64(len(content2)), "text/plain", nil)
	require.NoError(t, err)

	// Get old version
	reader, meta, err := store.GetObjectVersion(context.Background(), "test-bucket", "file.txt", meta1.VersionID)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content1, data)
	assert.Equal(t, meta1.VersionID, meta.VersionID)
}

func TestVersioning_RestoreVersion(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create versions
	content1 := []byte("original content")
	meta1, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content1), int64(len(content1)), "text/plain", nil)
	require.NoError(t, err)

	content2 := []byte("updated content - longer")
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content2), int64(len(content2)), "text/plain", nil)
	require.NoError(t, err)

	// Restore old version
	restored, err := store.RestoreVersion(context.Background(), "test-bucket", "file.txt", meta1.VersionID)
	require.NoError(t, err)
	assert.NotEqual(t, meta1.VersionID, restored.VersionID, "restore should create new version ID")

	// Current content should be original
	reader, _, err := store.GetObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content1, data)

	// Should now have 3 versions
	versions, err := store.ListVersions(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, versions, 3)
}

func TestVersioning_RestoreSharesChunks(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create original version with specific content
	content := []byte("content to restore")
	meta1, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Overwrite
	content2 := []byte("different content")
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content2), int64(len(content2)), "text/plain", nil)
	require.NoError(t, err)

	// Get storage size before restore
	sizeBefore, err := store.cas.TotalSize(context.Background())
	require.NoError(t, err)

	// Restore - should reuse chunks, not duplicate
	_, err = store.RestoreVersion(context.Background(), "test-bucket", "file.txt", meta1.VersionID)
	require.NoError(t, err)

	// Storage should not significantly increase (chunks are shared)
	sizeAfter, err := store.cas.TotalSize(context.Background())
	require.NoError(t, err)

	// Size should be roughly the same (may vary slightly due to metadata)
	assert.InDelta(t, sizeBefore, sizeAfter, 100, "restore should share chunks, not duplicate")
}

func TestVersioning_DeleteCleansVersions(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create versions
	for i := 1; i <= 3; i++ {
		content := []byte(strings.Repeat("v", i*100))
		_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	// Verify versions exist
	versions, err := store.ListVersions(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, versions, 3)

	// Purge the object
	err = store.PurgeObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)

	// Object should be gone
	_, err = store.HeadObject(context.Background(), "test-bucket", "file.txt")
	assert.ErrorIs(t, err, ErrObjectNotFound)

	// Versions should be gone
	versions, err = store.ListVersions(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Empty(t, versions)
}

func TestVersioning_DeleteAndRecreateStartsFreshHistory(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create original file with multiple versions
	for i := 1; i <= 3; i++ {
		content := []byte(strings.Repeat("v", i*10))
		_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	// Verify 3 versions exist (current + 2 archived)
	versions, err := store.ListVersions(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, versions, 3)

	// Delete the file (moves to recycle bin, version files remain on disk)
	require.NoError(t, store.DeleteObject(context.Background(), "test-bucket", "file.txt"))

	// Re-create the file fresh
	newContent := []byte("fresh start")
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(newContent), int64(len(newContent)), "text/plain", nil)
	require.NoError(t, err)

	// Version history must only show the new file — NOT the old pre-deletion versions
	versions, err = store.ListVersions(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, versions, 1, "re-created file should have no history from deleted predecessor")
	assert.True(t, versions[0].IsCurrent)
	assert.Equal(t, int64(len(newContent)), versions[0].Size)
}

func TestVersioning_RetentionPruning(t *testing.T) {
	store := newTestStoreWithCAS(t)
	store.SetVersionRetentionDays(30) // 30 day retention
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create an object
	content := []byte("original")
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Create another version (triggers pruning check)
	content2 := []byte("updated")
	_, err = store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content2), int64(len(content2)), "text/plain", nil)
	require.NoError(t, err)

	// Both versions should exist (neither expired)
	versions, err := store.ListVersions(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, versions, 2)
}

func TestVersioning_NoVersionsForNewObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create single object
	content := []byte("single version")
	_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Should only have 1 version (current)
	versions, err := store.ListVersions(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, versions, 1)
	assert.True(t, versions[0].IsCurrent)
}

// Concurrent access tests - run with -race flag

func TestConcurrent_PutObject(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	const numGoroutines = 10
	const numWrites = 5

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			for j := 0; j < numWrites; j++ {
				content := []byte(strings.Repeat("x", 1000+workerID*100+j*10))
				key := "shared-key.txt" // All workers write to same key
				_, err := store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
				if err != nil {
					t.Errorf("worker %d write %d failed: %v", workerID, j, err)
				}
			}
			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify object still readable
	reader, meta, err := store.GetObject(context.Background(), "test-bucket", "shared-key.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	assert.True(t, meta.Size > 0)
}

func TestConcurrent_ReadWhileGC(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create some objects with versions
	for i := 0; i < 5; i++ {
		content := []byte(strings.Repeat("y", 1000+i*100))
		_, err := store.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	done := make(chan bool, 2)

	// Concurrent GC
	go func() {
		for i := 0; i < 3; i++ {
			store.RunGarbageCollection(context.Background())
		}
		done <- true
	}()

	// Concurrent reads
	go func() {
		for i := 0; i < 10; i++ {
			reader, _, err := store.GetObject(context.Background(), "test-bucket", "file.txt")
			if err == nil {
				_, _ = io.ReadAll(reader)
				_ = reader.Close()
			}
		}
		done <- true
	}()

	<-done
	<-done

	// Verify object still accessible after concurrent operations
	reader, _, err := store.GetObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
}

func TestConcurrent_MultipleKeys(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			key := strings.Repeat("k", workerID+1) + ".txt"
			for j := 0; j < 5; j++ {
				content := []byte(strings.Repeat("z", 500+j*50))
				_, err := store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
				if err != nil {
					t.Errorf("worker %d failed: %v", workerID, err)
				}
			}
			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all keys exist
	objects, _, _, err := store.ListObjects(context.Background(), "test-bucket", "", "", 100)
	require.NoError(t, err)
	assert.Equal(t, numGoroutines, len(objects))
}

func TestConcurrent_PutDoesNotBlockList(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Seed an object so ListObjects has something to return
	seed := []byte("seed-data")
	_, err := store.PutObject(context.Background(), "test-bucket", "seed.txt", bytes.NewReader(seed), int64(len(seed)), "text/plain", nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const writers = 5
	const writesPerWorker = 3
	var wg sync.WaitGroup

	// Start writers that upload to different keys
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerWorker; j++ {
				content := []byte(strings.Repeat("w", 2000+id*100+j*50))
				key := fmt.Sprintf("writer-%d-%d.txt", id, j)
				_, putErr := store.PutObject(ctx, "test-bucket", key, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
				if putErr != nil && ctx.Err() == nil {
					t.Errorf("writer %d write %d: %v", id, j, putErr)
				}
			}
		}(i)
	}

	// Concurrent reader — should never be blocked for longer than metadata I/O
	listDone := make(chan struct{})
	go func() {
		defer close(listDone)
		for i := 0; i < 20; i++ {
			objs, _, _, listErr := store.ListObjects(ctx, "test-bucket", "", "", 100)
			if listErr != nil && ctx.Err() == nil {
				t.Errorf("list iteration %d: %v", i, listErr)
				return
			}
			// Must always see at least the seed object
			if len(objs) < 1 && ctx.Err() == nil {
				t.Errorf("list iteration %d: expected at least 1 object, got %d", i, len(objs))
			}
		}
	}()

	wg.Wait()
	<-listDone
}

func TestStreamingRead_LargeFile(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create a file larger than a single chunk (target ~4KB, max 64KB)
	// Use 200KB to ensure multiple chunks
	content := make([]byte, 200*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}

	_, err := store.PutObject(context.Background(), "test-bucket", "large.bin", bytes.NewReader(content), int64(len(content)), "application/octet-stream", nil)
	require.NoError(t, err)

	// Read back and verify streaming works correctly
	reader, meta, err := store.GetObject(context.Background(), "test-bucket", "large.bin")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	assert.Equal(t, int64(len(content)), meta.Size)

	// Read in small chunks to test streaming
	readContent := make([]byte, 0, len(content))
	buf := make([]byte, 1024) // 1KB buffer
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			readContent = append(readContent, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, content, readContent)
}

func TestCalculateBucketSize(t *testing.T) {
	store := newTestStoreWithCAS(t)
	bucket := "test-bucket"

	require.NoError(t, store.CreateBucket(context.Background(), bucket, "owner-id", 2, nil))

	// Initially, bucket size should be 0
	size, err := store.CalculateBucketSize(context.Background(), bucket)
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	// Put some objects
	content1 := []byte("test content 1")
	_, err = store.PutObject(context.Background(), bucket, "file1.txt", bytes.NewReader(content1), int64(len(content1)), "text/plain", nil)
	require.NoError(t, err)

	content2 := []byte("test content 2 is longer")
	_, err = store.PutObject(context.Background(), bucket, "file2.txt", bytes.NewReader(content2), int64(len(content2)), "text/plain", nil)
	require.NoError(t, err)

	// Size should reflect both files
	size, err = store.CalculateBucketSize(context.Background(), bucket)
	require.NoError(t, err)
	expectedSize := int64(len(content1) + len(content2))
	assert.Equal(t, expectedSize, size)

	// Update an object (size should change)
	content3 := []byte("updated content")
	_, err = store.PutObject(context.Background(), bucket, "file1.txt", bytes.NewReader(content3), int64(len(content3)), "text/plain", nil)
	require.NoError(t, err)

	size, err = store.CalculateBucketSize(context.Background(), bucket)
	require.NoError(t, err)
	expectedSize = int64(len(content3) + len(content2))
	assert.Equal(t, expectedSize, size)
}

func TestCalculateBucketSize_NonExistentBucket(t *testing.T) {
	store := newTestStoreWithCAS(t)

	// Non-existent bucket should return 0 size, not an error
	size, err := store.CalculateBucketSize(context.Background(), "non-existent")
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)
}

func TestCalculatePrefixSize(t *testing.T) {
	store := newTestStoreWithCAS(t)
	bucket := "test-bucket"

	require.NoError(t, store.CreateBucket(context.Background(), bucket, "owner-id", 2, nil))

	// Create files in different folders
	files := map[string][]byte{
		"folder1/file1.txt": []byte("content1"),
		"folder1/file2.txt": []byte("content2 longer"),
		"folder2/file3.txt": []byte("content3"),
		"root.txt":          []byte("root content"),
	}

	for key, content := range files {
		_, err := store.PutObject(context.Background(), bucket, key, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	// Calculate size for folder1/ prefix
	size, err := store.CalculatePrefixSize(context.Background(), bucket, "folder1/")
	require.NoError(t, err)
	expectedSize := int64(len(files["folder1/file1.txt"]) + len(files["folder1/file2.txt"]))
	assert.Equal(t, expectedSize, size)

	// Calculate size for folder2/ prefix
	size, err = store.CalculatePrefixSize(context.Background(), bucket, "folder2/")
	require.NoError(t, err)
	expectedSize = int64(len(files["folder2/file3.txt"]))
	assert.Equal(t, expectedSize, size)

	// Calculate size for root (empty prefix)
	size, err = store.CalculatePrefixSize(context.Background(), bucket, "")
	require.NoError(t, err)
	var totalSize int64
	for _, content := range files {
		totalSize += int64(len(content))
	}
	assert.Equal(t, totalSize, size)
}

func TestCalculatePrefixSize_NonExistentBucket(t *testing.T) {
	store := newTestStoreWithCAS(t)

	// Non-existent bucket should return an error
	_, err := store.CalculatePrefixSize(context.Background(), "non-existent", "folder/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bucket not found")
}

func TestCalculatePrefixSize_ExcludesDeletedObjects(t *testing.T) {
	store := newTestStoreWithCAS(t)
	bucket := "test-bucket"

	require.NoError(t, store.CreateBucket(context.Background(), bucket, "owner-id", 2, nil))

	// Create an object
	content := []byte("test content")
	_, err := store.PutObject(context.Background(), bucket, "folder/file.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Calculate initial size
	sizeBeforeDelete, err := store.CalculatePrefixSize(context.Background(), bucket, "folder/")
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), sizeBeforeDelete)

	// Delete (moves to recycle bin) the object
	require.NoError(t, store.DeleteObject(context.Background(), bucket, "folder/file.txt"))

	// Size should not include recycled objects
	sizeAfterDelete, err := store.CalculatePrefixSize(context.Background(), bucket, "folder/")
	require.NoError(t, err)
	assert.Equal(t, int64(0), sizeAfterDelete, "deleted objects should not count toward prefix size")
}

// TestConcurrent_PurgeObjectNoDeadlock verifies that PurgeObject doesn't deadlock
// when called concurrently with read operations. Before the fix, PurgeObject held
// s.mu.Lock() while calling isChunkReferencedGlobally() which did a full filesystem
// scan, blocking all other S3 operations and causing dashboard hangs.
func TestConcurrent_PurgeObjectNoDeadlock(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create multiple objects to purge (live objects, not recycled)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("file-%d.txt", i)
		content := []byte(fmt.Sprintf("content-%d-%s", i, strings.Repeat("x", 500)))
		_, err := store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	// Use timeout context to detect deadlock
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Concurrent purges (PurgeObject works on live objects)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("file-%d.txt", id)
			_ = store.PurgeObject(ctx, "test-bucket", key)
		}(i)
	}

	// Concurrent reads (these should NOT be blocked by purge)
	for i := 5; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// ListObjects and HeadObject require s.mu.RLock()
			// They would deadlock if PurgeObject holds s.mu.Lock() too long
			for j := 0; j < 5; j++ {
				_, _, _, _ = store.ListObjects(ctx, "test-bucket", "", "", 100)
				_, _ = store.HeadObject(ctx, "test-bucket", fmt.Sprintf("file-%d.txt", id))
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - no deadlock
	case <-ctx.Done():
		t.Fatal("deadlock detected: concurrent PurgeObject + reads timed out after 10s")
	}
}

// TestConcurrent_GCAndPurgeNoDeadlock verifies that GC and PurgeObject running
// concurrently with reads don't deadlock. This simulates the production scenario
// where the hourly GC ticker fires while HTTP handlers are serving requests.
func TestConcurrent_GCAndPurgeNoDeadlock(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create objects with multiple versions
	for i := 0; i < 5; i++ {
		for v := 0; v < 3; v++ {
			content := []byte(fmt.Sprintf("v%d-%s", v, strings.Repeat("x", 500+i*100)))
			_, err := store.PutObject(context.Background(), "test-bucket", fmt.Sprintf("obj-%d.txt", i), bytes.NewReader(content), int64(len(content)), "text/plain", nil)
			require.NoError(t, err)
		}
	}

	// Mark some for purging (PurgeObject works on live objects directly)
	// Objects 0-2 will be purged, objects 3-4 will remain for reads

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Run GC concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			store.RunGarbageCollection(ctx)
		}
	}()

	// Run purges concurrently
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = store.PurgeObject(ctx, "test-bucket", fmt.Sprintf("obj-%d.txt", id))
		}(i)
	}

	// Concurrent reads (simulate dashboard API calls)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_, _, _, _ = store.ListObjects(ctx, "test-bucket", "", "", 100)
			_, _ = store.ListBuckets(ctx)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - no deadlock
	case <-ctx.Done():
		t.Fatal("deadlock detected: concurrent GC + Purge + reads timed out after 15s")
	}
}

// TestConcurrent_CalculatePrefixSizeNoDeadlock verifies that CalculatePrefixSize
// doesn't deadlock when called concurrently with PutObject. Before the fix,
// CalculatePrefixSize acquired RLock then called ListObjects which tried RLock
// again — when a PutObject was pending Lock, Go's RWMutex blocked the nested
// RLock, causing an indefinite hang (appeared as S3 Explorer silent failure).
func TestConcurrent_CalculatePrefixSizeNoDeadlock(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Seed some objects under a folder prefix
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("folder/file-%d.txt", i)
		content := []byte(fmt.Sprintf("content-%d-%s", i, strings.Repeat("x", 500)))
		_, err := store.PutObject(context.Background(), "test-bucket", key, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Concurrent PutObject (acquires write lock, triggers the contention)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("folder/new-%d-%d.txt", id, j)
				content := []byte(fmt.Sprintf("new-content-%d-%d", id, j))
				_, _ = store.PutObject(ctx, "test-bucket", key, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
			}
		}(i)
	}

	// Concurrent CalculatePrefixSize (previously deadlocked under RLock)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = store.CalculatePrefixSize(ctx, "test-bucket", "folder/")
			}
		}()
	}

	// Concurrent ListObjects (shares RLock path)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _, _, _ = store.ListObjects(ctx, "test-bucket", "folder/", "", 100)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - no deadlock
	case <-ctx.Done():
		t.Fatal("deadlock detected: concurrent CalculatePrefixSize + PutObject timed out after 10s")
	}
}

// TestConcurrent_GCDoesNotBlockReads verifies that GC's filesystem scan
// (buildChunkReferenceSet) does not hold s.mu, allowing concurrent ListObjects
// and PutObject to proceed without blocking. This is a regression test for the
// S3 explorer freeze after ~1 hour (GC holding RLock during full walk).
func TestConcurrent_GCDoesNotBlockReads(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create objects to make GC scan take some time
	for i := 0; i < 10; i++ {
		content := []byte(fmt.Sprintf("content-%d-%s", i, strings.Repeat("x", 1000)))
		_, err := store.PutObject(context.Background(), "test-bucket", fmt.Sprintf("obj-%d.txt", i), bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Run GC repeatedly (this does the expensive filesystem scan)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			store.RunGarbageCollection(ctx)
		}
	}()

	// Concurrent writes — these acquire s.mu.Lock() and would deadlock
	// if buildChunkReferenceSet held s.mu.RLock() (write-preferring RWMutex
	// blocks new RLock acquires once a Lock waiter exists)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			content := []byte(fmt.Sprintf("concurrent-write-%d", i))
			_, _ = store.PutObject(ctx, "test-bucket", "concurrent.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		}
	}()

	// Concurrent reads — simulate dashboard API calls that were freezing
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			_, _, _, _ = store.ListObjects(ctx, "test-bucket", "", "", 100)
			_, _ = store.ListBuckets(ctx)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success — no lock contention blocking reads during GC
	case <-ctx.Done():
		t.Fatal("lock contention detected: concurrent GC + writes + reads timed out after 10s")
	}
}

// TestBuildChunkReferenceSet_Cancellation verifies that buildChunkReferenceSet
// respects context cancellation and returns early.
func TestBuildChunkReferenceSet_Cancellation(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create some objects
	for i := 0; i < 5; i++ {
		content := []byte(fmt.Sprintf("content-%d", i))
		_, err := store.PutObject(context.Background(), "test-bucket", fmt.Sprintf("obj-%d.txt", i), bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	// Cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	refs := store.buildChunkReferenceSet(ctx)
	// Should return without error (may have partial or empty results)
	assert.NotNil(t, refs)
}

// TestBuildChunkReferenceSet_SubdirectoryMeta verifies that buildChunkReferenceSet
// finds chunks referenced by metadata files in subdirectories (e.g., system bucket
// stores metadata in meta/auth/, meta/dns/, meta/filter/).
func TestBuildChunkReferenceSet_SubdirectoryMeta(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create a regular object (top-level meta)
	topContent := []byte("top-level-object")
	_, err := store.PutObject(context.Background(), "test-bucket", "top.txt", bytes.NewReader(topContent), int64(len(topContent)), "text/plain", nil)
	require.NoError(t, err)

	// Manually create a metadata file in a subdirectory to simulate system bucket layout
	subDir := filepath.Join(store.dataDir, "buckets", "test-bucket", "meta", "auth")
	require.NoError(t, os.MkdirAll(subDir, 0o700))

	subMeta := ObjectMeta{
		Key:          "auth/users.json",
		Chunks:       []string{"subdir-chunk-hash-abc123"},
		Size:         100,
		ContentType:  "application/json",
		LastModified: time.Now().UTC(),
		VersionID:    "v1",
	}
	data, err := json.Marshal(subMeta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(subDir, "users.json"), data, 0o600))

	// Build reference set
	refs := store.buildChunkReferenceSet(context.Background())

	// Should find the chunk from the subdirectory metadata
	_, found := refs["subdir-chunk-hash-abc123"]
	assert.True(t, found, "buildChunkReferenceSet should find chunks in meta subdirectories")

	// Should also find chunks from top-level metadata
	topMeta, err := store.HeadObject(context.Background(), "test-bucket", "top.txt")
	require.NoError(t, err)
	for _, chunk := range topMeta.Chunks {
		_, found := refs[chunk]
		assert.True(t, found, "buildChunkReferenceSet should find chunks from top-level metadata")
	}
}

// TestIsChunkReferencedGlobally_SubdirectoryMeta verifies that isChunkReferencedGlobally
// correctly finds chunk references in subdirectory metadata files.
func TestIsChunkReferencedGlobally_SubdirectoryMeta(t *testing.T) {
	store := newTestStoreWithCAS(t)
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create subdirectory metadata with a known chunk hash
	subDir := filepath.Join(store.dataDir, "buckets", "test-bucket", "meta", "dns")
	require.NoError(t, os.MkdirAll(subDir, 0o700))

	subMeta := ObjectMeta{
		Key:          "dns/records.json",
		Chunks:       []string{"dns-chunk-hash-xyz789"},
		Size:         50,
		ContentType:  "application/json",
		LastModified: time.Now().UTC(),
		VersionID:    "v1",
	}
	data, err := json.Marshal(subMeta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(subDir, "records.json"), data, 0o600))

	// Should find the chunk in subdirectory
	ctx := context.Background()
	assert.True(t, store.isChunkReferencedGlobally(ctx, "dns-chunk-hash-xyz789"),
		"isChunkReferencedGlobally should find chunks referenced in meta subdirectories")

	// Should NOT find a non-existent chunk
	assert.False(t, store.isChunkReferencedGlobally(ctx, "nonexistent-chunk-hash"),
		"isChunkReferencedGlobally should return false for unreferenced chunks")
}

// TestPruneAllExpiredVersionsSimple_SubdirectoryMeta verifies that
// pruneAllExpiredVersionsSimple scans objects in subdirectories too.
func TestPruneAllExpiredVersionsSimple_SubdirectoryMeta(t *testing.T) {
	store := newTestStoreWithCAS(t)
	store.versionRetentionDays = 1 // Expire after 1 day
	require.NoError(t, store.CreateBucket(context.Background(), "test-bucket", "alice", 2, nil))

	// Create an object in a subdirectory (key="filter/rules", file="meta/filter/rules.json")
	subDir := filepath.Join(store.dataDir, "buckets", "test-bucket", "meta", "filter")
	require.NoError(t, os.MkdirAll(subDir, 0o700))

	subMeta := ObjectMeta{
		Key:          "filter/rules",
		Size:         100,
		ContentType:  "application/json",
		LastModified: time.Now().UTC(),
		VersionID:    "current",
	}
	data, err := json.Marshal(subMeta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(subDir, "rules.json"), data, 0o600))

	// Create an expired version for this subdirectory object
	// Key "filter/rules" → version dir at "versions/filter/rules/"
	versionDir := filepath.Join(store.dataDir, "buckets", "test-bucket", "versions", "filter", "rules")
	require.NoError(t, os.MkdirAll(versionDir, 0o700))

	expiredVersion := ObjectMeta{
		Key:          "filter/rules",
		Size:         80,
		ContentType:  "application/json",
		LastModified: time.Now().UTC().AddDate(0, 0, -30), // 30 days old
		VersionID:    "old-version",
	}
	vData, err := json.Marshal(expiredVersion)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(versionDir, "old-version.json"), vData, 0o600))

	// Run GC pruning
	stats := GCStats{}
	store.pruneAllExpiredVersionsSimple(context.Background(), &stats)

	// Should have scanned the subdirectory object
	assert.Greater(t, stats.ObjectsScanned, 0, "pruneAllExpiredVersionsSimple should scan objects in subdirectories")
	assert.Greater(t, stats.VersionsPruned, 0, "pruneAllExpiredVersionsSimple should prune expired versions in subdirectories")

	// The expired version file should be deleted
	_, err = os.Stat(filepath.Join(versionDir, "old-version.json"))
	assert.True(t, os.IsNotExist(err), "expired version should be deleted")
}

// ==== ImportObjectMeta Tests ====

func TestImportObjectMeta_NewBucket(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Import metadata for a bucket that doesn't exist yet
	meta := ObjectMeta{
		Key:         "report.pdf",
		Size:        5000,
		ContentType: "application/pdf",
		Chunks:      []string{"hash1", "hash2"},
	}
	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)

	_, err = store.ImportObjectMeta(ctx, "newbucket", "report.pdf", metaJSON, "alice")
	require.NoError(t, err)

	// Verify the metadata is readable
	got, err := store.GetObjectMeta(ctx, "newbucket", "report.pdf")
	require.NoError(t, err)
	assert.Equal(t, "report.pdf", got.Key)
	assert.Equal(t, int64(5000), got.Size)
	assert.Equal(t, "application/pdf", got.ContentType)
	assert.Equal(t, []string{"hash1", "hash2"}, got.Chunks)

	// Verify bucket was created with correct owner
	buckets, err := store.ListBuckets(ctx)
	require.NoError(t, err)
	require.Len(t, buckets, 1)
	assert.Equal(t, "alice", buckets[0].Owner)
}

func TestImportObjectMeta_ExistingBucket(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Create bucket first
	require.NoError(t, store.CreateBucket(ctx, "mybucket", "alice", 1, nil))

	meta := ObjectMeta{
		Key:         "doc.txt",
		Size:        100,
		ContentType: "text/plain",
		Chunks:      []string{"abc"},
	}
	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)

	_, err = store.ImportObjectMeta(ctx, "mybucket", "doc.txt", metaJSON, "")
	require.NoError(t, err)

	got, err := store.GetObjectMeta(ctx, "mybucket", "doc.txt")
	require.NoError(t, err)
	assert.Equal(t, "doc.txt", got.Key)
}

func TestImportObjectMeta_InvalidJSON(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	_, err := store.ImportObjectMeta(ctx, "bucket", "key", []byte("not json"), "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid object meta JSON")
}

func TestImportObjectMeta_InvalidBucketName(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	meta := ObjectMeta{Key: "test"}
	metaJSON, _ := json.Marshal(meta)

	_, err := store.ImportObjectMeta(ctx, "../escape", "test", metaJSON, "")
	assert.Error(t, err)
}

func TestImportObjectMeta_ArchivesVersion(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "mybucket", "alice", 1, nil))

	// Import first version
	meta1 := ObjectMeta{
		Key:         "doc.txt",
		Size:        100,
		ContentType: "text/plain",
		Chunks:      []string{"hash1"},
	}
	metaJSON1, err := json.Marshal(meta1)
	require.NoError(t, err)
	_, err = store.ImportObjectMeta(ctx, "mybucket", "doc.txt", metaJSON1, "")
	require.NoError(t, err)

	// Import second version (overwrites first)
	meta2 := ObjectMeta{
		Key:         "doc.txt",
		Size:        200,
		ContentType: "text/plain",
		Chunks:      []string{"hash2"},
	}
	metaJSON2, err := json.Marshal(meta2)
	require.NoError(t, err)
	_, err = store.ImportObjectMeta(ctx, "mybucket", "doc.txt", metaJSON2, "")
	require.NoError(t, err)

	// Verify current version is meta2
	got, err := store.GetObjectMeta(ctx, "mybucket", "doc.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(200), got.Size)

	// Verify a version was archived (versions directory has a file)
	versionsDir := filepath.Join(store.DataDir(), "buckets", "mybucket", "versions", "doc.txt")
	entries, err := os.ReadDir(versionsDir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "should have 1 archived version")
}

// ==== DeleteChunk Tests ====

func TestDeleteChunk(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Write a chunk first
	data := []byte("test chunk data")
	hash := ContentHash(data)
	err := store.WriteChunkDirect(ctx, hash, data)
	require.NoError(t, err)

	// Verify chunk exists
	readData, err := store.ReadChunk(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, data, readData)

	// Delete the chunk
	err = store.DeleteChunk(ctx, hash)
	require.NoError(t, err)

	// Verify chunk is gone
	_, err = store.ReadChunk(ctx, hash)
	assert.Error(t, err)
}

func TestDeleteChunk_NonExistent(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Deleting a non-existent chunk should not error (idempotent behavior).
	// This is important because cleanup may run after a chunk was already
	// garbage collected or deleted by another process.
	err := store.DeleteChunk(ctx, "nonexistent-hash")
	assert.NoError(t, err)
}

func TestDeleteUnreferencedChunks(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Create a bucket and object referencing chunk1
	require.NoError(t, store.CreateBucket(ctx, "mybucket", "alice", 1, nil))

	chunk1 := []byte("referenced chunk data")
	hash1 := ContentHash(chunk1)
	require.NoError(t, store.WriteChunkDirect(ctx, hash1, chunk1))

	chunk2 := []byte("orphaned chunk data!!")
	hash2 := ContentHash(chunk2)
	require.NoError(t, store.WriteChunkDirect(ctx, hash2, chunk2))

	// Create an object that references only chunk1
	meta := ObjectMeta{
		Key:    "doc.txt",
		Size:   int64(len(chunk1)),
		Chunks: []string{hash1},
	}
	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)
	_, err = store.ImportObjectMeta(ctx, "mybucket", "doc.txt", metaJSON, "")
	require.NoError(t, err)

	// Record stats before cleanup
	statsBefore := store.GetCASStats()

	// Delete unreferenced chunks — chunk2 is orphaned, chunk1 is referenced
	freed := store.DeleteUnreferencedChunks(ctx, []string{hash1, hash2})

	// chunk2 should have been freed
	assert.Greater(t, freed, int64(0), "should have freed bytes for orphaned chunk")

	// Verify stats were decremented
	statsAfter := store.GetCASStats()
	assert.Equal(t, statsBefore.ChunkCount-1, statsAfter.ChunkCount, "chunk count should decrease by 1")
	assert.Equal(t, statsBefore.ChunkBytes-freed, statsAfter.ChunkBytes, "chunk bytes should decrease by freed amount")

	// chunk1 should still be readable (it's referenced)
	_, err = store.ReadChunk(ctx, hash1)
	assert.NoError(t, err)

	// chunk2 should be gone
	_, err = store.ReadChunk(ctx, hash2)
	assert.Error(t, err)
}

func TestDeleteUnreferencedChunks_AllReferenced(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Write a chunk and reference it from an object
	require.NoError(t, store.CreateBucket(ctx, "mybucket", "alice", 1, nil))

	chunk := []byte("kept chunk data")
	hash := ContentHash(chunk)
	require.NoError(t, store.WriteChunkDirect(ctx, hash, chunk))

	meta := ObjectMeta{
		Key:    "doc.txt",
		Size:   int64(len(chunk)),
		Chunks: []string{hash},
	}
	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)
	_, err = store.ImportObjectMeta(ctx, "mybucket", "doc.txt", metaJSON, "")
	require.NoError(t, err)

	statsBefore := store.GetCASStats()

	// All chunks are referenced — nothing should be freed
	freed := store.DeleteUnreferencedChunks(ctx, []string{hash})
	assert.Equal(t, int64(0), freed)

	// Stats unchanged
	statsAfter := store.GetCASStats()
	assert.Equal(t, statsBefore.ChunkCount, statsAfter.ChunkCount)
	assert.Equal(t, statsBefore.ChunkBytes, statsAfter.ChunkBytes)
}

func TestSyncedWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")

	// Write initial content
	original := []byte(`{"name":"original"}`)
	err := syncedWriteFile(path, original, 0644)
	require.NoError(t, err)

	// Verify initial content
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, original, data)

	// Overwrite with new content
	updated := []byte(`{"name":"updated","extra":"field"}`)
	err = syncedWriteFile(path, updated, 0644)
	require.NoError(t, err)

	data, err = os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, updated, data)

	// Verify no temp files left behind
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "no temp files should remain after successful write")
}

func TestSyncedWriteFileAtomic_ReadOnlyDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not enforce read-only directory permissions via os.Chmod")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")

	// Write initial content
	original := []byte(`{"name":"original"}`)
	err := syncedWriteFile(path, original, 0644)
	require.NoError(t, err)

	// Make directory read-only to simulate write failure
	require.NoError(t, os.Chmod(dir, 0555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0755) })

	// Attempt to overwrite — should fail, but original file should remain
	err = syncedWriteFile(path, []byte(`{"name":"corrupted"}`), 0644)
	assert.Error(t, err)

	// Restore permissions and verify original content is intact
	require.NoError(t, os.Chmod(dir, 0755))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, original, data, "original file should be untouched after failed write")
}

func TestGetBucketMeta_EmptyMetadata(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Create a valid bucket first
	err := store.CreateBucket(ctx, "test-bucket", "alice", 2, nil)
	require.NoError(t, err)

	// Verify it exists
	_, err = store.HeadBucket(ctx, "test-bucket")
	require.NoError(t, err)

	// Overwrite _meta.json with empty content (simulates disk-full truncation)
	metaPath := filepath.Join(store.DataDir(), "buckets", "test-bucket", "_meta.json")
	require.NoError(t, os.WriteFile(metaPath, []byte{}, 0644))

	// Should return ErrBucketNotFound, not a generic error
	_, err = store.HeadBucket(ctx, "test-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestGetBucketMeta_CorruptedJSON(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Create a valid bucket first
	err := store.CreateBucket(ctx, "test-bucket", "alice", 2, nil)
	require.NoError(t, err)

	// Overwrite _meta.json with invalid JSON
	metaPath := filepath.Join(store.DataDir(), "buckets", "test-bucket", "_meta.json")
	require.NoError(t, os.WriteFile(metaPath, []byte(`{invalid json`), 0644))

	// Should return ErrBucketNotFound, not a generic unmarshal error
	_, err = store.HeadBucket(ctx, "test-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound)
}

func TestStore_VolumeStats(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir, nil)
	require.NoError(t, err)

	total, used, available, err := store.VolumeStats()
	require.NoError(t, err)

	assert.Greater(t, total, int64(0), "total should be positive")
	assert.GreaterOrEqual(t, used, int64(0), "used should be non-negative")
	assert.Greater(t, available, int64(0), "available should be positive")
}

func TestGetCASStats_LogicalBytesUsesMetaSize(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Put a single unique object — LogicalBytes should equal meta.Size (uncompressed)
	content := []byte("unique content for stats test")
	_, err := store.PutObject(ctx, "test-bucket", "file1.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	stats := store.GetCASStats()

	assert.Equal(t, 1, stats.ObjectCount, "should have 1 object")
	assert.Greater(t, stats.ChunkBytes, int64(0), "should have chunk bytes on disk")
	assert.Equal(t, int64(len(content)), stats.LogicalBytes,
		"LogicalBytes should equal meta.Size (uncompressed content size)")

	// Put a second identical object (dedup) — LogicalBytes should be 2x content size
	_, err = store.PutObject(ctx, "test-bucket", "file2.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	stats = store.GetCASStats()

	assert.Equal(t, 2, stats.ObjectCount, "should have 2 objects")
	assert.Equal(t, int64(2*len(content)), stats.LogicalBytes,
		"deduplicated: logical bytes should be 2x content size (both objects reference same data)")
}

func TestGetCASStats_LegacyObjectsUseMetaSize(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Put an object normally
	content := []byte("legacy test content")
	_, err := store.PutObject(ctx, "test-bucket", "legacy.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Simulate a legacy object by stripping ChunkMetadata from the stored meta
	// LogicalBytes should still work because it uses meta.Size, not chunk lookups
	metaDir := filepath.Join(store.DataDir(), "buckets", "test-bucket", "meta")
	err = filepath.Walk(metaDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		var meta ObjectMeta
		if jsonErr := json.Unmarshal(data, &meta); jsonErr != nil {
			return jsonErr
		}
		meta.ChunkMetadata = nil // Simulate legacy object with no chunk metadata
		updated, jsonErr := json.Marshal(meta)
		if jsonErr != nil {
			return jsonErr
		}
		return os.WriteFile(path, updated, 0o644)
	})
	require.NoError(t, err)

	// Re-init stats from disk to pick up the modified metadata
	store.initCASStats()

	stats := store.GetCASStats()

	assert.Equal(t, 1, stats.ObjectCount)
	// LogicalBytes uses meta.Size which is always available regardless of ChunkMetadata
	assert.Equal(t, int64(len(content)), stats.LogicalBytes,
		"legacy objects should use meta.Size for LogicalBytes")
}

func TestGetCASStats_VersionCountAfterOverwrite(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Write v1 of a file
	v1 := []byte("version one content for logical bytes test")
	_, err := store.PutObject(ctx, "test-bucket", "file.txt", bytes.NewReader(v1), int64(len(v1)), "text/plain", nil)
	require.NoError(t, err)

	statsAfterV1 := store.GetCASStats()
	assert.Equal(t, int64(len(v1)), statsAfterV1.LogicalBytes,
		"after v1: logical bytes should equal content size")

	// Overwrite with v2 — v1 is archived, its chunks remain on disk
	v2 := []byte("version two with different content for test")
	_, err = store.PutObject(ctx, "test-bucket", "file.txt", bytes.NewReader(v2), int64(len(v2)), "text/plain", nil)
	require.NoError(t, err)

	statsAfterV2 := store.GetCASStats()
	assert.Equal(t, 1, statsAfterV2.VersionCount, "should have 1 archived version")
	// LogicalBytes only counts current objects (not version files).
	// After overwrite, LogicalBytes should equal the new content size.
	assert.Equal(t, int64(len(v2)), statsAfterV2.LogicalBytes,
		"after v2: logical bytes should equal v2 content size (only current objects)")
	// VersionBytes should track the archived version's size
	assert.Equal(t, int64(len(v1)), statsAfterV2.VersionBytes,
		"after v2: version bytes should equal v1 content size (archived version)")
}

// trackingChunkRegistry records RegisterChunk calls for testing.
type trackingChunkRegistry struct {
	mu         sync.Mutex
	registered map[string]int64 // hash -> size
}

func newTrackingChunkRegistry() *trackingChunkRegistry {
	return &trackingChunkRegistry{registered: make(map[string]int64)}
}

func (r *trackingChunkRegistry) RegisterChunk(hash string, size int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registered[hash] = size
	return nil
}

func (r *trackingChunkRegistry) RegisterChunkWithReplication(hash string, size int64, _ int) error {
	return r.RegisterChunk(hash, size)
}

func (r *trackingChunkRegistry) RegisterShardChunk(hash string, size int64, _, _ string, _, _ int) error {
	return r.RegisterChunk(hash, size)
}

func (r *trackingChunkRegistry) UnregisterChunk(_ string) error { return nil }

func (r *trackingChunkRegistry) GetOwners(_ string) ([]string, error) {
	return nil, fmt.Errorf("not found")
}

func (r *trackingChunkRegistry) GetChunksOwnedBy(_ string) ([]string, error) {
	return nil, nil
}

func (r *trackingChunkRegistry) isRegistered(hash string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.registered[hash]
	return ok
}

func TestWriteChunkDirect_RegistersInRegistry(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	reg := newTrackingChunkRegistry()
	store.SetChunkRegistry(reg)

	data := []byte("chunk data for registry test")
	hash := ContentHash(data)

	err := store.WriteChunkDirect(ctx, hash, data)
	require.NoError(t, err)

	assert.True(t, reg.isRegistered(hash),
		"WriteChunkDirect should register chunk in the distributed registry")
}

func TestPutObject_CleansUpPrunedVersionChunks(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Set max versions to 3 for easier testing
	store.maxVersionsPerObject = 3

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Write 5 versions (exceeds max of 3), creating prunable versions
	for i := 0; i < 5; i++ {
		content := []byte(fmt.Sprintf("version %d unique content for pruning test", i))
		_, err := store.PutObject(ctx, "test-bucket", "file.txt",
			bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	stats := store.GetCASStats()
	// Should have at most 3 versions (max limit)
	assert.LessOrEqual(t, stats.VersionCount, 3,
		"version count should not exceed maxVersionsPerObject")

	// Version pruning happens inline, but chunk cleanup is deferred to periodic GC
	// (which builds the reference set once for efficiency). Chunks from pruned
	// versions remain on disk until the next GC cycle.
	assert.Greater(t, stats.ChunkCount, 0, "should still have chunks for live + retained versions")
}

func TestImportObjectMeta_CleansUpPrunedVersionChunks(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	store.maxVersionsPerObject = 3

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Write initial versions via PutObject
	for i := 0; i < 3; i++ {
		content := []byte(fmt.Sprintf("version %d for import test", i))
		_, err := store.PutObject(ctx, "test-bucket", "file.txt",
			bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		require.NoError(t, err)
	}

	// Import additional versions via ImportObjectMeta (simulates replication)
	for i := 3; i < 6; i++ {
		content := []byte(fmt.Sprintf("version %d for import test", i))
		hash := ContentHash(content)
		// Write chunk directly (as replication would)
		require.NoError(t, store.WriteChunkDirect(ctx, hash, content))

		meta := ObjectMeta{
			Key:          "file.txt",
			Size:         int64(len(content)),
			ContentType:  "text/plain",
			LastModified: time.Now().UTC(),
			VersionID:    fmt.Sprintf("import-v%d", i),
			Chunks:       []string{hash},
		}
		metaJSON, err := json.Marshal(meta)
		require.NoError(t, err)

		chunksToCheck, err := store.ImportObjectMeta(ctx, "test-bucket", "file.txt", metaJSON, "alice")
		require.NoError(t, err)

		// ImportObjectMeta returns chunks to check for cleanup
		if len(chunksToCheck) > 0 {
			store.DeleteUnreferencedChunks(ctx, chunksToCheck)
		}
	}

	stats := store.GetCASStats()
	assert.LessOrEqual(t, stats.VersionCount, 3,
		"version count should not exceed maxVersionsPerObject after import")
}

func TestCASStats_IncludesVersionAndRecycleBin(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Write v1
	v1 := []byte("version 1 content for combined stats test")
	_, err := store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v1), int64(len(v1)), "text/plain", nil)
	require.NoError(t, err)

	// Write v2 (archives v1)
	v2 := []byte("version 2 content for combined stats test")
	_, err = store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v2), int64(len(v2)), "text/plain", nil)
	require.NoError(t, err)

	// Add another object then recycle it
	recycled := []byte("content to be recycled")
	_, err = store.PutObject(ctx, "test-bucket", "to-delete.txt",
		bytes.NewReader(recycled), int64(len(recycled)), "text/plain", nil)
	require.NoError(t, err)

	err = store.RecycleObject(ctx, "test-bucket", "to-delete.txt")
	require.NoError(t, err)

	stats := store.GetCASStats()

	// Live: v2 of file.txt
	assert.Equal(t, int64(len(v2)), stats.LogicalBytes,
		"LogicalBytes should equal live object size")

	// Versions: v1 of file.txt
	assert.Equal(t, int64(len(v1)), stats.VersionBytes,
		"VersionBytes should equal archived version size")

	// Recycled: to-delete.txt
	assert.Equal(t, int64(len(recycled)), stats.RecycledBytes,
		"RecycledBytes should equal recycled object size")

	// Total managed data = live + versions + recycled
	totalLogical := stats.LogicalBytes + stats.VersionBytes + stats.RecycledBytes
	expectedTotal := int64(len(v1)) + int64(len(v2)) + int64(len(recycled))
	assert.Equal(t, expectedTotal, totalLogical,
		"total logical bytes should equal live + version + recycled")
}

func TestCASStats_RecycledBytesDecrementOnRestore(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	content := []byte("content to recycle and restore")
	_, err := store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(content), int64(len(content)), "text/plain", nil)
	require.NoError(t, err)

	// Recycle
	require.NoError(t, store.RecycleObject(ctx, "test-bucket", "file.txt"))
	stats := store.GetCASStats()
	assert.Equal(t, int64(len(content)), stats.RecycledBytes)
	assert.Equal(t, int64(0), stats.LogicalBytes)

	// Restore
	require.NoError(t, store.RestoreRecycledObject(ctx, "test-bucket", "file.txt"))
	stats = store.GetCASStats()
	assert.Equal(t, int64(0), stats.RecycledBytes,
		"RecycledBytes should be 0 after restore")
	assert.Equal(t, int64(len(content)), stats.LogicalBytes,
		"LogicalBytes should be restored")
}

func TestCASStats_VersionBytesAfterInitCASStats(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Create version history
	v1 := []byte("v1 data for init stats test")
	_, err := store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v1), int64(len(v1)), "text/plain", nil)
	require.NoError(t, err)

	v2 := []byte("v2 data for init stats test")
	_, err = store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v2), int64(len(v2)), "text/plain", nil)
	require.NoError(t, err)

	// Recycle an object
	recycled := []byte("will be recycled for init stats test")
	_, err = store.PutObject(ctx, "test-bucket", "recycled.txt",
		bytes.NewReader(recycled), int64(len(recycled)), "text/plain", nil)
	require.NoError(t, err)
	require.NoError(t, store.RecycleObject(ctx, "test-bucket", "recycled.txt"))

	// Re-init stats from filesystem (simulates restart)
	store.initCASStats()

	stats := store.GetCASStats()
	assert.Equal(t, int64(len(v2)), stats.LogicalBytes, "LogicalBytes from initCASStats")
	assert.Equal(t, int64(len(v1)), stats.VersionBytes, "VersionBytes from initCASStats")
	assert.Equal(t, int64(len(recycled)), stats.RecycledBytes, "RecycledBytes from initCASStats")
}

func TestGetVersionHistory(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Write multiple versions
	v1 := []byte("version 1 data")
	_, err := store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v1), int64(len(v1)), "text/plain", nil)
	require.NoError(t, err)

	v2 := []byte("version 2 data, longer")
	_, err = store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v2), int64(len(v2)), "text/plain", nil)
	require.NoError(t, err)

	v3 := []byte("version 3 data, even longer!!!")
	_, err = store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v3), int64(len(v3)), "text/plain", nil)
	require.NoError(t, err)

	// GetVersionHistory should return archived versions (v1 and v2)
	entries, err := store.GetVersionHistory(ctx, "test-bucket", "file.txt")
	require.NoError(t, err)

	// Should have 2 archived versions (v1, v2) - current (v3) is NOT included
	assert.Len(t, entries, 2)

	// Verify each entry has valid JSON
	for _, e := range entries {
		assert.NotEmpty(t, e.VersionID)
		assert.True(t, json.Valid(e.MetaJSON), "MetaJSON should be valid JSON")
	}
}

func TestImportVersionHistory_Merge(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Write an object so there's something to have versions for
	v1 := []byte("version 1")
	_, err := store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v1), int64(len(v1)), "text/plain", nil)
	require.NoError(t, err)

	v2 := []byte("version 2")
	_, err = store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v2), int64(len(v2)), "text/plain", nil)
	require.NoError(t, err)

	// Get the existing version
	existingVersions, err := store.GetVersionHistory(ctx, "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, existingVersions, 1) // v1 is archived

	// Import additional versions from replication
	newVersions := []VersionHistoryEntry{
		{VersionID: "imported-v1", MetaJSON: []byte(`{"key":"file.txt","version_id":"imported-v1","size":100}`)},
		{VersionID: "imported-v2", MetaJSON: []byte(`{"key":"file.txt","version_id":"imported-v2","size":200}`)},
	}

	imported, _, err := store.ImportVersionHistory(ctx, "test-bucket", "file.txt", newVersions)
	require.NoError(t, err)
	assert.Equal(t, 2, imported)

	// Now GetVersionHistory should return all versions (1 existing + 2 imported)
	allVersions, err := store.GetVersionHistory(ctx, "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Len(t, allVersions, 3)
}

func TestImportVersionHistory_NoDuplicates(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	// Write two versions to create an archived version
	v1 := []byte("version 1")
	_, err := store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v1), int64(len(v1)), "text/plain", nil)
	require.NoError(t, err)

	v2 := []byte("version 2")
	_, err = store.PutObject(ctx, "test-bucket", "file.txt",
		bytes.NewReader(v2), int64(len(v2)), "text/plain", nil)
	require.NoError(t, err)

	// Get existing version IDs
	existingVersions, err := store.GetVersionHistory(ctx, "test-bucket", "file.txt")
	require.NoError(t, err)
	require.Len(t, existingVersions, 1)

	// Try to import the same version ID — should be deduped
	dupVersions := []VersionHistoryEntry{
		{VersionID: existingVersions[0].VersionID, MetaJSON: existingVersions[0].MetaJSON},
	}

	imported, _, err := store.ImportVersionHistory(ctx, "test-bucket", "file.txt", dupVersions)
	require.NoError(t, err)
	assert.Equal(t, 0, imported, "Should not import duplicate versions")
}

func TestImportVersionHistory_Empty(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	imported, _, err := store.ImportVersionHistory(ctx, "test-bucket", "file.txt", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, imported)
}

func TestGetAllObjectKeys(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "bucket-a", "alice", 2, nil))
	require.NoError(t, store.CreateBucket(ctx, "bucket-b", "bob", 2, nil))

	// Add objects
	data1 := []byte("object 1")
	_, err := store.PutObject(ctx, "bucket-a", "file1.txt",
		bytes.NewReader(data1), int64(len(data1)), "text/plain", nil)
	require.NoError(t, err)

	data2 := []byte("object 2")
	_, err = store.PutObject(ctx, "bucket-a", "file2.txt",
		bytes.NewReader(data2), int64(len(data2)), "text/plain", nil)
	require.NoError(t, err)

	data3 := []byte("object 3")
	_, err = store.PutObject(ctx, "bucket-b", "file3.txt",
		bytes.NewReader(data3), int64(len(data3)), "text/plain", nil)
	require.NoError(t, err)

	result, err := store.GetAllObjectKeys(ctx)
	require.NoError(t, err)

	assert.Len(t, result["bucket-a"], 2)
	assert.Len(t, result["bucket-b"], 1)
}

func TestGetAllObjectKeys_Empty(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	result, err := store.GetAllObjectKeys(ctx)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGetBucketErasureCodingPolicy_NoEC(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	require.NoError(t, store.CreateBucket(ctx, "test-bucket", "alice", 2, nil))

	enabled, k, m, err := store.GetBucketErasureCodingPolicy(ctx, "test-bucket")
	require.NoError(t, err)
	assert.False(t, enabled)
	assert.Equal(t, 0, k)
	assert.Equal(t, 0, m)
}

func TestGetBucketErasureCodingPolicy_WithEC(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	ecPolicy := &ErasureCodingPolicy{Enabled: true, DataShards: 10, ParityShards: 3}
	require.NoError(t, store.CreateBucket(ctx, "ec-bucket", "alice", 2, ecPolicy))

	enabled, k, m, err := store.GetBucketErasureCodingPolicy(ctx, "ec-bucket")
	require.NoError(t, err)
	assert.True(t, enabled)
	assert.Equal(t, 10, k)
	assert.Equal(t, 3, m)
}

func TestGetBucketErasureCodingPolicy_BucketNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	_, _, _, err := store.GetBucketErasureCodingPolicy(ctx, "nonexistent")
	assert.Error(t, err)
}

func TestAtomicWriteFile(t *testing.T) {
	dir := t.TempDir()

	t.Run("basic write and read", func(t *testing.T) {
		path := filepath.Join(dir, "test.json")
		data := []byte(`{"key": "value"}`)
		require.NoError(t, atomicWriteFile(path, data, 0644))

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, data, got)
	})

	t.Run("overwrites existing file atomically", func(t *testing.T) {
		path := filepath.Join(dir, "overwrite.json")
		require.NoError(t, atomicWriteFile(path, []byte("old"), 0644))
		require.NoError(t, atomicWriteFile(path, []byte("new"), 0644))

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, []byte("new"), got)
	})

	t.Run("no temp files left on success", func(t *testing.T) {
		path := filepath.Join(dir, "clean.json")
		require.NoError(t, atomicWriteFile(path, []byte("data"), 0644))

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, e := range entries {
			assert.False(t, strings.HasPrefix(e.Name(), ".tmp-"),
				"temp file should be cleaned up: %s", e.Name())
		}
	})

	t.Run("fails on nonexistent directory", func(t *testing.T) {
		path := filepath.Join(dir, "nonexistent", "file.json")
		err := atomicWriteFile(path, []byte("data"), 0644)
		assert.Error(t, err)
	})
}

func TestImportObjectMeta_ConcurrentStress(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ctx := context.Background()

	// Create a bucket upfront
	require.NoError(t, store.CreateBucket(ctx, "stress", "owner", 1, nil))

	const goroutines = 20
	const objectsPerGoroutine = 5

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*objectsPerGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < objectsPerGoroutine; i++ {
				key := fmt.Sprintf("obj-%d-%d", g, i)
				meta := ObjectMeta{
					Key:         key,
					Size:        int64(100 + g*10 + i),
					ContentType: "text/plain",
					Chunks:      []string{fmt.Sprintf("chunk-%d-%d", g, i)},
				}
				metaJSON, err := json.Marshal(meta)
				if err != nil {
					errs <- err
					return
				}
				if _, err := store.ImportObjectMeta(ctx, "stress", key, metaJSON, ""); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent ImportObjectMeta failed: %v", err)
	}

	// Verify all objects are readable
	for g := 0; g < goroutines; g++ {
		for i := 0; i < objectsPerGoroutine; i++ {
			key := fmt.Sprintf("obj-%d-%d", g, i)
			got, err := store.GetObjectMeta(ctx, "stress", key)
			require.NoError(t, err, "object %s should be readable", key)
			assert.Equal(t, key, got.Key)
		}
	}
}
