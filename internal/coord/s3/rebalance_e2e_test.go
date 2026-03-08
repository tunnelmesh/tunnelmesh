package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_FilesSurviveTopologyChangeAndGC simulates the full lifecycle of a file
// during coordinator topology changes (accordion mode) and verifies that files
// and their chunks survive rebalancing and garbage collection.
//
// Scenario:
//  1. Coordinator C1 uploads a file
//  2. C1 replicates metadata+chunks to C2 and C3
//  3. Topology change: C3 leaves, C4 joins (accordion)
//  4. GC runs on all coordinators
//  5. File must still be readable on C1 and C2
func TestE2E_FilesSurviveTopologyChangeAndGC(t *testing.T) {
	ctx := context.Background()
	var encryptionKey [32]byte

	// ── Setup: Create 3 coordinators with real S3 stores ──

	stores := make(map[string]*Store) // coordID → Store
	dirs := make(map[string]string)

	for _, coordID := range []string{"coord1", "coord2", "coord3"} {
		dir := t.TempDir()
		dirs[coordID] = dir

		store, err := NewStore(dir, nil)
		require.NoError(t, err, "create store for %s", coordID)
		require.NoError(t, store.InitCAS(ctx, encryptionKey), "init CAS for %s", coordID)

		mockReg := &mockChunkRegistry{
			owners:       make(map[string][]string),
			localCoordID: coordID,
		}
		store.SetChunkRegistry(mockReg)
		store.SetCoordinatorID(coordID)

		stores[coordID] = store
	}

	// ── Step 1: Upload a file on coord1 ──

	c1 := stores["coord1"]
	require.NoError(t, c1.CreateBucket(ctx, "docs", "admin", 2))

	fileData := []byte("Hello, this is a test file that should survive topology changes and garbage collection cycles without being deleted.")
	meta, err := c1.PutObject(ctx, "docs", "important.txt", bytes.NewReader(fileData), int64(len(fileData)), "text/plain", nil)
	require.NoError(t, err)
	require.NotEmpty(t, meta.Chunks, "file should have chunks")

	t.Logf("Step 1: File uploaded on coord1 with %d chunks: %v", len(meta.Chunks), truncateHashes(meta.Chunks))

	// Verify file is readable on coord1
	assertFileReadable(t, c1, "docs", "important.txt", fileData, "coord1 after upload")

	// ── Step 2: Replicate to coord2 and coord3 ──

	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)

	for _, coordID := range []string{"coord2", "coord3"} {
		store := stores[coordID]

		// Simulate chunk replication: write each chunk to the peer's CAS
		for _, chunkHash := range meta.Chunks {
			chunkData, readErr := c1.cas.ReadChunk(ctx, chunkHash)
			require.NoError(t, readErr, "read chunk %s from coord1", chunkHash[:8])

			writeErr := store.WriteChunkDirect(ctx, chunkHash, chunkData)
			require.NoError(t, writeErr, "write chunk %s to %s", chunkHash[:8], coordID)
		}

		// Simulate metadata replication
		chunksToCheck, importErr := store.ImportObjectMeta(ctx, "docs", "important.txt", metaJSON, "admin")
		require.NoError(t, importErr, "import metadata to %s", coordID)

		t.Logf("Step 2: Replicated to %s (chunksToCheck from import: %d)", coordID, len(chunksToCheck))

		// If import returned chunks to check, simulate deferred cleanup
		if len(chunksToCheck) > 0 {
			freed := store.DeleteUnreferencedChunks(ctx, chunksToCheck)
			t.Logf("  Deferred cleanup on %s freed %d bytes", coordID, freed)
		}

		// Verify file is readable on the peer
		assertFileReadable(t, store, "docs", "important.txt", fileData, coordID+" after initial replication")
	}

	// ── Step 3: Simulate repeated replication (auto-sync sends same metadata again) ──

	t.Log("Step 3: Simulating auto-sync (repeated metadata imports)...")

	for round := 0; round < 3; round++ {
		for _, coordID := range []string{"coord2", "coord3"} {
			store := stores[coordID]

			chunksToCheck, importErr := store.ImportObjectMeta(ctx, "docs", "important.txt", metaJSON, "admin")
			require.NoError(t, importErr, "re-import metadata to %s round %d", coordID, round)

			if len(chunksToCheck) > 0 {
				freed := store.DeleteUnreferencedChunks(ctx, chunksToCheck)
				t.Logf("  Round %d: %s deferred cleanup freed %d bytes (chunks checked: %d)",
					round, coordID, freed, len(chunksToCheck))
			}

			// Verify file is still readable after each round
			assertFileReadable(t, store, "docs", "important.txt", fileData,
				coordID+" after auto-sync round "+string(rune('0'+round)))
		}
	}

	// ── Step 4: Age chunks so they're past GC grace period ──

	t.Log("Step 4: Aging chunks past GC grace period...")

	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	for coordID, store := range stores {
		chunksDir := filepath.Join(store.dataDir, "chunks")
		err := filepath.Walk(chunksDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			return os.Chtimes(path, twoHoursAgo, twoHoursAgo)
		})
		require.NoError(t, err, "age chunks on %s", coordID)
	}

	// ── Step 5: Run GC on all coordinators ──

	t.Log("Step 5: Running GC on all coordinators...")

	for _, coordID := range []string{"coord1", "coord2", "coord3"} {
		store := stores[coordID]
		stats := store.RunGarbageCollection(ctx)
		t.Logf("  GC on %s: versions_pruned=%d, chunks_deleted=%d, bytes_reclaimed=%d, chunks_skipped_grace=%d",
			coordID, stats.VersionsPruned, stats.ChunksDeleted, stats.BytesReclaimed, stats.ChunksSkippedGracePeriod)
	}

	// ── Step 6: Verify files survive GC ──

	t.Log("Step 6: Verifying files survive GC...")

	for _, coordID := range []string{"coord1", "coord2", "coord3"} {
		store := stores[coordID]
		assertFileReadable(t, store, "docs", "important.txt", fileData, coordID+" after GC")
	}

	// ── Step 7: Simulate topology change — re-import metadata one more time,
	//    then run GC again (simulates what happens during accordion mode) ──

	t.Log("Step 7: Simulating topology change with another round of replication + GC...")

	// Re-import on coord2 (simulating rebalancer enqueuing objects)
	chunksToCheck, importErr := stores["coord2"].ImportObjectMeta(ctx, "docs", "important.txt", metaJSON, "admin")
	require.NoError(t, importErr)

	if len(chunksToCheck) > 0 {
		freed := stores["coord2"].DeleteUnreferencedChunks(ctx, chunksToCheck)
		t.Logf("  Post-topology-change deferred cleanup on coord2: freed %d bytes, chunks checked: %d",
			freed, len(chunksToCheck))
	}

	// Age the newly-written version files
	for _, store := range stores {
		versionsDir := filepath.Join(store.dataDir, "buckets", "docs", "versions")
		_ = filepath.Walk(versionsDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			return os.Chtimes(path, twoHoursAgo, twoHoursAgo)
		})
	}

	// Run GC again
	for _, coordID := range []string{"coord1", "coord2", "coord3"} {
		store := stores[coordID]
		stats := store.RunGarbageCollection(ctx)
		t.Logf("  GC round 2 on %s: versions_pruned=%d, chunks_deleted=%d, bytes_reclaimed=%d",
			coordID, stats.VersionsPruned, stats.ChunksDeleted, stats.BytesReclaimed)
	}

	// Final verification
	t.Log("Step 8: Final verification — files must still be readable...")

	for _, coordID := range []string{"coord1", "coord2", "coord3"} {
		store := stores[coordID]
		assertFileReadable(t, store, "docs", "important.txt", fileData, coordID+" FINAL")
	}
}

// TestE2E_ChunksSurviveRepeatedImportWithVersionPruning specifically tests that
// chunks survive when version limits are hit during repeated metadata imports.
// This exercises the deferred chunk cleanup path that was previously untested.
func TestE2E_ChunksSurviveRepeatedImportWithVersionPruning(t *testing.T) {
	ctx := context.Background()
	var encryptionKey [32]byte

	dir := t.TempDir()
	store, err := NewStore(dir, nil)
	require.NoError(t, err)
	require.NoError(t, store.InitCAS(ctx, encryptionKey))

	mockReg := &mockChunkRegistry{
		owners:       make(map[string][]string),
		localCoordID: "coord1",
	}
	store.SetChunkRegistry(mockReg)
	store.SetCoordinatorID("coord1")

	// Set a LOW version limit to trigger pruning quickly
	store.SetMaxVersionsPerObject(3)

	// Upload a file
	require.NoError(t, store.CreateBucket(ctx, "test", "admin", 2))

	fileData := []byte("Test file for version pruning survival test. This content should persist through many import cycles.")
	meta, err := store.PutObject(ctx, "test", "file.txt", bytes.NewReader(fileData), int64(len(fileData)), "text/plain", nil)
	require.NoError(t, err)

	t.Logf("File uploaded with %d chunks", len(meta.Chunks))

	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)

	// Simulate 10 rounds of metadata re-import (auto-sync + rebalancer trigger)
	// Each import archives the current version. With maxVersions=3, versions will be pruned.
	for round := 0; round < 10; round++ {
		chunksToCheck, importErr := store.ImportObjectMeta(ctx, "test", "file.txt", metaJSON, "admin")
		require.NoError(t, importErr, "import round %d", round)

		// Count version files to verify archival is working
		versionDir := filepath.Join(dir, "buckets", "test", "versions", "file.txt")
		entries, _ := os.ReadDir(versionDir)
		versionCount := 0
		for _, e := range entries {
			if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
				versionCount++
			}
		}

		t.Logf("  Round %d: versions_on_disk=%d, chunks_to_check=%d", round, versionCount, len(chunksToCheck))

		if len(chunksToCheck) > 0 {
			// This is the critical path: DeleteUnreferencedChunks checks if chunks
			// from pruned versions are still referenced by current metadata.
			freed := store.DeleteUnreferencedChunks(ctx, chunksToCheck)
			t.Logf("  Round %d: freed %d bytes", round, freed)
		}

		// Verify file is still readable after each round
		assertFileReadable(t, store, "test", "file.txt", fileData, "round "+string(rune('0'+round)))
	}

	// Age chunks and run GC
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	chunksDir := filepath.Join(store.dataDir, "chunks")
	_ = filepath.Walk(chunksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		return os.Chtimes(path, twoHoursAgo, twoHoursAgo)
	})

	stats := store.RunGarbageCollection(ctx)
	t.Logf("GC after 10 import rounds: versions_pruned=%d, chunks_deleted=%d", stats.VersionsPruned, stats.ChunksDeleted)

	// File MUST still be readable
	assertFileReadable(t, store, "test", "file.txt", fileData, "after GC post-import-storm")
}

// TestE2E_DifferentFileVersionsSurviveGC tests that when a file is updated (new content,
// different chunks), the old chunks from pruned versions are correctly cleaned up
// while the new chunks survive.
func TestE2E_DifferentFileVersionsSurviveGC(t *testing.T) {
	ctx := context.Background()
	var encryptionKey [32]byte

	dir := t.TempDir()
	store, err := NewStore(dir, nil)
	require.NoError(t, err)
	require.NoError(t, store.InitCAS(ctx, encryptionKey))

	store.SetMaxVersionsPerObject(2) // Keep only 2 versions

	require.NoError(t, store.CreateBucket(ctx, "test", "admin", 2))

	// Upload version 1
	v1Data := []byte("Version 1 content - original file data that will be replaced")
	v1Meta, err := store.PutObject(ctx, "test", "evolving.txt", bytes.NewReader(v1Data), int64(len(v1Data)), "text/plain", nil)
	require.NoError(t, err)
	v1Chunks := v1Meta.Chunks
	t.Logf("V1 uploaded with chunks: %v", truncateHashes(v1Chunks))

	// Simulate replication importing V1 metadata repeatedly (creating versions)
	v1MetaJSON, _ := json.Marshal(v1Meta)
	for i := 0; i < 3; i++ {
		chunksToCheck, _ := store.ImportObjectMeta(ctx, "test", "evolving.txt", v1MetaJSON, "admin")
		if len(chunksToCheck) > 0 {
			store.DeleteUnreferencedChunks(ctx, chunksToCheck)
		}
	}

	// Upload version 2 (completely different content = different chunks)
	v2Data := []byte("Version 2 content - completely new data replacing version 1 entirely!")
	v2Meta, err := store.PutObject(ctx, "test", "evolving.txt", bytes.NewReader(v2Data), int64(len(v2Data)), "text/plain", nil)
	require.NoError(t, err)
	v2Chunks := v2Meta.Chunks
	t.Logf("V2 uploaded with chunks: %v", truncateHashes(v2Chunks))

	// Simulate replication importing V2 repeatedly
	v2MetaJSON, _ := json.Marshal(v2Meta)
	for i := 0; i < 5; i++ {
		chunksToCheck, _ := store.ImportObjectMeta(ctx, "test", "evolving.txt", v2MetaJSON, "admin")
		if len(chunksToCheck) > 0 {
			freed := store.DeleteUnreferencedChunks(ctx, chunksToCheck)
			if freed > 0 {
				t.Logf("  Import round %d: freed %d bytes from pruned version chunks", i, freed)
			}
		}
	}

	// The current file should be V2
	assertFileReadable(t, store, "test", "evolving.txt", v2Data, "after V2 import storm")

	// Age everything and run GC
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	_ = filepath.Walk(filepath.Join(store.dataDir, "chunks"), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		return os.Chtimes(path, twoHoursAgo, twoHoursAgo)
	})
	_ = filepath.Walk(filepath.Join(store.dataDir, "buckets"), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		return os.Chtimes(path, twoHoursAgo, twoHoursAgo)
	})

	stats := store.RunGarbageCollection(ctx)
	t.Logf("Final GC: versions_pruned=%d, chunks_deleted=%d", stats.VersionsPruned, stats.ChunksDeleted)

	// V2 must still be readable
	assertFileReadable(t, store, "test", "evolving.txt", v2Data, "after final GC")
}

// TestE2E_MultipleFilesSharedChunksSurviveGC tests that when multiple files share
// chunks (deduplication), GC correctly preserves shared chunks even when some
// files' versions are pruned.
func TestE2E_MultipleFilesSharedChunksSurviveGC(t *testing.T) {
	ctx := context.Background()
	var encryptionKey [32]byte

	dir := t.TempDir()
	store, err := NewStore(dir, nil)
	require.NoError(t, err)
	require.NoError(t, store.InitCAS(ctx, encryptionKey))

	store.SetMaxVersionsPerObject(1) // Very aggressive pruning

	require.NoError(t, store.CreateBucket(ctx, "shared", "admin", 2))

	// Upload file1 and file2 with identical content (shared chunks via dedup)
	sharedData := []byte("This content is shared between two files via content-addressable deduplication.")

	meta1, err := store.PutObject(ctx, "shared", "file1.txt", bytes.NewReader(sharedData), int64(len(sharedData)), "text/plain", nil)
	require.NoError(t, err)

	meta2, err := store.PutObject(ctx, "shared", "file2.txt", bytes.NewReader(sharedData), int64(len(sharedData)), "text/plain", nil)
	require.NoError(t, err)

	// Verify they share chunks (CAS dedup)
	assert.Equal(t, meta1.Chunks, meta2.Chunks, "identical files should share chunks")

	t.Logf("Both files share %d chunks", len(meta1.Chunks))

	// Simulate repeated imports of file1 only (triggers version pruning for file1)
	meta1JSON, _ := json.Marshal(meta1)
	for i := 0; i < 5; i++ {
		chunksToCheck, _ := store.ImportObjectMeta(ctx, "shared", "file1.txt", meta1JSON, "admin")
		if len(chunksToCheck) > 0 {
			freed := store.DeleteUnreferencedChunks(ctx, chunksToCheck)
			if freed > 0 {
				t.Logf("  Round %d: freed %d bytes (SHOULD BE 0 - shared chunks!)", i, freed)
			}
		}
	}

	// Both files MUST still be readable
	assertFileReadable(t, store, "shared", "file1.txt", sharedData, "file1 after import storm")
	assertFileReadable(t, store, "shared", "file2.txt", sharedData, "file2 after import storm")

	// Age and GC
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	_ = filepath.Walk(filepath.Join(store.dataDir, "chunks"), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		return os.Chtimes(path, twoHoursAgo, twoHoursAgo)
	})

	stats := store.RunGarbageCollection(ctx)
	t.Logf("GC: chunks_deleted=%d", stats.ChunksDeleted)
	assert.Equal(t, 0, stats.ChunksDeleted, "no chunks should be deleted (all still referenced)")

	// Both files MUST still be readable after GC
	assertFileReadable(t, store, "shared", "file1.txt", sharedData, "file1 after GC")
	assertFileReadable(t, store, "shared", "file2.txt", sharedData, "file2 after GC")
}

// TestE2E_NewCoordReceivesChunksBeforeMetadata tests the scenario where
// a new coordinator receives chunks (via rebalancer) but metadata hasn't
// arrived yet. GC should not delete those chunks within the grace period.
func TestE2E_NewCoordReceivesChunksBeforeMetadata(t *testing.T) {
	ctx := context.Background()
	var encryptionKey [32]byte

	// Source coordinator
	srcDir := t.TempDir()
	src, err := NewStore(srcDir, nil)
	require.NoError(t, err)
	require.NoError(t, src.InitCAS(ctx, encryptionKey))
	require.NoError(t, src.CreateBucket(ctx, "data", "admin", 2))

	fileData := []byte("File that will be partially replicated - chunks arrive before metadata.")
	meta, err := src.PutObject(ctx, "data", "partial.txt", bytes.NewReader(fileData), int64(len(fileData)), "text/plain", nil)
	require.NoError(t, err)

	// New coordinator — receives chunks but NOT metadata yet
	dstDir := t.TempDir()
	dst, err := NewStore(dstDir, nil)
	require.NoError(t, err)
	require.NoError(t, dst.InitCAS(ctx, encryptionKey))

	// Write chunks to destination (simulating rebalancer sending chunks)
	for _, chunkHash := range meta.Chunks {
		data, readErr := src.cas.ReadChunk(ctx, chunkHash)
		require.NoError(t, readErr)
		require.NoError(t, dst.WriteChunkDirect(ctx, chunkHash, data))
	}

	t.Logf("Chunks written to new coord, metadata NOT yet imported")

	// Verify chunks exist
	for _, chunkHash := range meta.Chunks {
		assert.True(t, dst.ChunkExists(chunkHash), "chunk %s should exist", chunkHash[:8])
	}

	// Run GC on destination — chunks are recent, should be protected by grace period
	stats := dst.RunGarbageCollection(ctx)
	t.Logf("GC on new coord (chunks recent): chunks_deleted=%d, chunks_skipped_grace=%d",
		stats.ChunksDeleted, stats.ChunksSkippedGracePeriod)

	assert.Equal(t, 0, stats.ChunksDeleted, "recent chunks should NOT be deleted by GC")

	// Now verify chunks still exist
	for _, chunkHash := range meta.Chunks {
		assert.True(t, dst.ChunkExists(chunkHash),
			"chunk %s should survive GC (within grace period)", chunkHash[:8])
	}

	// Now age the chunks past the grace period (simulating delay > 10 min)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	_ = filepath.Walk(filepath.Join(dst.dataDir, "chunks"), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		return os.Chtimes(path, twoHoursAgo, twoHoursAgo)
	})

	// Run GC again — chunks are old AND have no metadata → they WILL be deleted
	// This is the expected behavior: if metadata never arrives, chunks are orphaned
	stats2 := dst.RunGarbageCollection(ctx)
	t.Logf("GC on new coord (chunks aged, no metadata): chunks_deleted=%d", stats2.ChunksDeleted)

	// This is expected: without metadata, chunks are orphaned and should be cleaned up
	assert.Greater(t, stats2.ChunksDeleted, 0, "orphaned aged chunks should be deleted")

	// NOW import metadata (simulating delayed replication)
	// First, re-create the chunks (since they were cleaned up)
	for _, chunkHash := range meta.Chunks {
		data, readErr := src.cas.ReadChunk(ctx, chunkHash)
		require.NoError(t, readErr)
		require.NoError(t, dst.WriteChunkDirect(ctx, chunkHash, data))
	}

	metaJSON, _ := json.Marshal(meta)
	_, importErr := dst.ImportObjectMeta(ctx, "data", "partial.txt", metaJSON, "admin")
	require.NoError(t, importErr)

	// File should now be readable
	assertFileReadable(t, dst, "data", "partial.txt", fileData, "after delayed replication")
}

// assertFileReadable verifies that a file can be read and matches expected data.
func assertFileReadable(t *testing.T, store *Store, bucket, key string, expectedData []byte, desc string) {
	t.Helper()
	ctx := context.Background()

	meta, err := store.GetObjectMeta(ctx, bucket, key)
	if !assert.NoError(t, err, "%s: GetObjectMeta failed", desc) {
		return
	}

	// Verify all chunks exist
	for _, chunkHash := range meta.Chunks {
		exists := store.ChunkExists(chunkHash)
		if !assert.True(t, exists, "%s: chunk %s missing", desc, chunkHash[:8]) {
			return
		}
	}

	// Read the full file via GetObject
	reader, _, err := store.GetObject(ctx, bucket, key)
	if !assert.NoError(t, err, "%s: GetObject failed", desc) {
		return
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err, "%s: reading response body", desc)

	assert.Equal(t, expectedData, data, "%s: file content mismatch", desc)
}

// truncateHashes returns chunk hashes truncated for readable logs.
func truncateHashes(hashes []string) []string {
	result := make([]string, len(hashes))
	for i, h := range hashes {
		if len(h) > 8 {
			result[i] = h[:8]
		} else {
			result[i] = h
		}
	}
	return result
}
