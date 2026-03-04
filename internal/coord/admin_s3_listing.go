package coord

import (
	"context"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

// listingIndex is published by each coordinator to the system store.
type listingIndex struct {
	Buckets map[string]*bucketListing `json:"buckets"`
	Seq     uint64                    `json:"seq"` // Monotonic counter incremented on each incremental update
}

// bucketListing holds object and recycled entry listings for a single bucket.
type bucketListing struct {
	Objects  []S3ObjectInfo `json:"objects,omitempty"`
	Recycled []S3ObjectInfo `json:"recycled,omitempty"`
}

// peerListings holds pre-merged peer listing data for instant access.
type peerListings struct {
	Objects  map[string][]S3ObjectInfo // bucket -> objects from all peers
	Recycled map[string][]S3ObjectInfo // bucket -> recycled from all peers
}

// mergeObjectListings deduplicates object listings from multiple coordinators.
// For duplicate keys, the entry with the most recent LastModified wins.
// On equal timestamps, the local (first) entry is kept as an implicit tie-breaker.
// Prefix entries (folders) are deduplicated by key; their sizes are summed.
func mergeObjectListings(local, remote []S3ObjectInfo) []S3ObjectInfo {
	if len(remote) == 0 {
		return local
	}

	seen := make(map[string]int, len(local)) // key -> index in result
	result := make([]S3ObjectInfo, 0, len(local)+len(remote))

	for _, obj := range local {
		seen[obj.Key] = len(result)
		result = append(result, obj)
	}

	for _, obj := range remote {
		if idx, exists := seen[obj.Key]; exists {
			existing := result[idx]
			if existing.IsPrefix {
				// Sum folder sizes across coordinators
				result[idx].Size += obj.Size
			}
			// For files, keep whichever has a newer LastModified
			if !existing.IsPrefix && obj.LastModified > existing.LastModified {
				result[idx] = obj
			}
			continue
		}
		seen[obj.Key] = len(result)
		result = append(result, obj)
	}

	return result
}

// updateListingIndex incrementally updates the local listing index after a write event.
// It uses copy-on-write semantics so concurrent readers see a consistent snapshot.
//
//   - op = "put": upsert entry in objects list
//   - op = "delete": remove from objects, add to recycled (with DeletedAt)
//   - op = "undelete": remove from recycled, add to objects
//   - op = "remove": hard delete — remove from both objects and recycled (replication deletes, PurgeObject)
func (s *Server) updateListingIndex(bucket, key string, info *S3ObjectInfo, op string) {
	for {
		old := s.localListingIndex.Load()

		// Build a new index as a shallow copy of the old one
		newIdx := &listingIndex{Buckets: make(map[string]*bucketListing)}
		if old != nil {
			for k, v := range old.Buckets {
				newIdx.Buckets[k] = v
			}
			newIdx.Seq = old.Seq
		}

		// Get or create bucket listing (copy-on-write for the bucket too).
		// Each case only copies the slice(s) it actually mutates; unchanged slices
		// are shared with the old listing (safe because CAS ensures no concurrent writes).
		bl := newIdx.Buckets[bucket]
		var newBL bucketListing
		if bl != nil {
			switch op {
			case "put":
				// upsertObjectList builds a new slice from bl.Objects directly.
				// Recycled is unchanged — share the pointer.
				if info != nil {
					newBL.Objects = upsertObjectList(bl.Objects, key, *info)
				} else {
					newBL.Objects = bl.Objects
				}
				newBL.Recycled = bl.Recycled

			case "delete":
				var removed *S3ObjectInfo
				newBL.Objects, removed = removeFromObjectList(bl.Objects, key)
				if removed != nil {
					recycled := *removed
					recycled.DeletedAt = time.Now().UTC().Format(time.RFC3339)
					newBL.Recycled = upsertObjectList(bl.Recycled, key, recycled)
				} else {
					newBL.Recycled = bl.Recycled
				}

			case "undelete":
				var removed *S3ObjectInfo
				newBL.Recycled, removed = removeFromObjectList(bl.Recycled, key)
				if removed != nil {
					restored := *removed
					restored.DeletedAt = ""
					newBL.Objects = make([]S3ObjectInfo, len(bl.Objects)+1)
					copy(newBL.Objects, bl.Objects)
					newBL.Objects[len(bl.Objects)] = restored
				} else {
					newBL.Objects = bl.Objects
				}

			case "remove":
				// Hard delete: remove from both live objects and recycled list
				newBL.Objects, _ = removeFromObjectList(bl.Objects, key)
				newBL.Recycled, _ = removeFromObjectList(bl.Recycled, key)
			}
		} else if op == "put" && info != nil {
			// No existing bucket listing — seed with the new entry
			newBL.Objects = []S3ObjectInfo{*info}
		}

		// For "remove" ops, drop the bucket key entirely if both slices are now empty
		// rather than storing an empty entry. reconcileLocalIndex would clean it up
		// within 5 min anyway, but removing it eagerly keeps the map lean.
		if op == "remove" && len(newBL.Objects) == 0 && len(newBL.Recycled) == 0 {
			delete(newIdx.Buckets, bucket)
		} else {
			newIdx.Buckets[bucket] = &newBL
		}
		newIdx.Seq++ // Monotonic increment for reconcile staleness detection

		// CAS to avoid lost updates from concurrent calls
		if s.localListingIndex.CompareAndSwap(old, newIdx) {
			break
		}
		// Another goroutine updated the index concurrently — retry with fresh snapshot
	}

	s.listingIndexDirty.Store(true)

	// Non-blocking signal to background indexer
	select {
	case s.listingIndexNotify <- struct{}{}:
	default:
	}
}

// removeListingBucket atomically removes an entire bucket from the local listing index
// and from peerListings. Used by onBucketRemovedCallback when ForceDeleteBucket wipes
// a bucket directory. O(1) bucket removal rather than per-key updates for bulk bucket deletions.
func (s *Server) removeListingBucket(bucket string) {
	// Remove from localListingIndex
	for {
		old := s.localListingIndex.Load()
		if old == nil {
			break
		}
		if _, exists := old.Buckets[bucket]; !exists {
			break
		}
		newIdx := &listingIndex{
			Buckets: make(map[string]*bucketListing, len(old.Buckets)),
			Seq:     old.Seq + 1,
		}
		for b, bl := range old.Buckets {
			if b != bucket {
				newIdx.Buckets[b] = bl
			}
		}
		if s.localListingIndex.CompareAndSwap(old, newIdx) {
			s.listingIndexDirty.Store(true)
			select {
			case s.listingIndexNotify <- struct{}{}:
			default:
			}
			break
		}
		// CAS failed — concurrent update, retry
	}

	// Also remove from peerListings (optimistic forwarded entries)
	for {
		old := s.peerListings.Load()
		if old == nil {
			return
		}
		if _, exists := old.Objects[bucket]; !exists {
			if _, exists := old.Recycled[bucket]; !exists {
				return // Bucket not in peerListings
			}
		}
		newPL := &peerListings{
			Objects:  make(map[string][]S3ObjectInfo, len(old.Objects)),
			Recycled: make(map[string][]S3ObjectInfo, len(old.Recycled)),
		}
		for b, objs := range old.Objects {
			if b != bucket {
				newPL.Objects[b] = objs
			}
		}
		for b, objs := range old.Recycled {
			if b != bucket {
				newPL.Recycled[b] = objs
			}
		}
		if s.peerListings.CompareAndSwap(old, newPL) {
			return
		}
		// CAS failed — concurrent update, retry
	}
}

// findObjectSourceIP returns the SourceIP of the first peer listing entry
// matching the given bucket/key. Returns "" if not found.
func (s *Server) findObjectSourceIP(bucket, key string) string {
	pl := s.peerListings.Load()
	if pl == nil {
		return ""
	}
	for _, obj := range pl.Objects[bucket] {
		if obj.Key == key && obj.SourceIP != "" {
			return obj.SourceIP
		}
	}
	return ""
}

// findRecycledObjectSourceIP returns the SourceIP of the first peer recycled
// listing entry matching the given bucket/key. Returns "" if not found.
func (s *Server) findRecycledObjectSourceIP(bucket, key string) string {
	pl := s.peerListings.Load()
	if pl == nil {
		return ""
	}
	for _, obj := range pl.Recycled[bucket] {
		if obj.Key == key && obj.SourceIP != "" {
			return obj.SourceIP
		}
	}
	return ""
}

// getPeerObjectListing returns cached peer object listings for a bucket.
func (s *Server) getPeerObjectListing(bucket string) []S3ObjectInfo {
	pl := s.peerListings.Load()
	if pl == nil {
		return nil
	}
	return pl.Objects[bucket]
}

// getPeerRecycledListing returns cached peer recycled listings for a bucket.
func (s *Server) getPeerRecycledListing(bucket string) []S3ObjectInfo {
	pl := s.peerListings.Load()
	if pl == nil {
		return nil
	}
	return pl.Recycled[bucket]
}

// filterByPrefixDelimiter applies S3-style prefix/delimiter filtering to a flat object list.
// With delimiter, objects beneath the prefix are grouped into "folder" prefixes.
func filterByPrefixDelimiter(objs []S3ObjectInfo, prefix, delimiter string) []S3ObjectInfo {
	if len(objs) == 0 {
		return nil
	}

	var result []S3ObjectInfo
	prefixIdx := make(map[string]int) // commonPrefix -> index in result (for O(1) size summation)

	for _, obj := range objs {
		// Skip entries that don't match the prefix
		if prefix != "" && !strings.HasPrefix(obj.Key, prefix) {
			continue
		}

		// If no delimiter, include all matching entries
		if delimiter == "" {
			result = append(result, obj)
			continue
		}

		// Apply delimiter grouping
		keyAfterPrefix := strings.TrimPrefix(obj.Key, prefix)
		if idx := strings.Index(keyAfterPrefix, delimiter); idx >= 0 {
			// This key contains the delimiter after the prefix → group as folder
			commonPrefix := prefix + keyAfterPrefix[:idx+1]
			if ri, exists := prefixIdx[commonPrefix]; !exists {
				prefixIdx[commonPrefix] = len(result)
				result = append(result, S3ObjectInfo{
					Key:      commonPrefix,
					Size:     obj.Size,
					IsPrefix: true,
				})
			} else {
				result[ri].Size += obj.Size
			}
		} else {
			// No delimiter after prefix → include as-is
			result = append(result, obj)
		}
	}

	return result
}

// runListingIndexer runs the background goroutine that persists the local listing
// index to the system store and loads peer indexes for merged reads.
func (s *Server) runListingIndexer(ctx context.Context) {
	persistTicker := time.NewTicker(10 * time.Second)
	defer persistTicker.Stop()

	// Stagger the first reconcile to avoid synchronized spikes across coordinators.
	// s.listingReconcileStagger is 0 in tests — stagger is skipped.
	if s.listingReconcileStagger > 0 {
		staggerTimer := time.NewTimer(s.listingReconcileStagger)
		select {
		case <-ctx.Done():
			staggerTimer.Stop()
			return
		case <-staggerTimer.C:
		}
	}
	reconcileTicker := time.NewTicker(5 * time.Minute)
	defer reconcileTicker.Stop()

	var lastPersist time.Time

	for {
		select {
		case <-ctx.Done():
			// Final persist to avoid losing recent writes on shutdown
			if s.listingIndexDirty.Load() {
				s.persistLocalIndex(context.Background())
			}
			return

		case <-persistTicker.C:
			if s.listingIndexDirty.Load() {
				s.persistLocalIndex(ctx)
				lastPersist = time.Now()
			}
			s.loadPeerIndexes(ctx)

		case <-reconcileTicker.C:
			s.reconcileLocalIndex(ctx)
			s.loadPeerIndexes(ctx)

		case <-s.listingIndexNotify:
			// Debounce: skip if last persist was < 1s ago.
			// Under sustained burst writes, this means the notify path effectively
			// degrades to the 10s persistTicker cadence, which is acceptable.
			if time.Since(lastPersist) < time.Second {
				continue
			}
			if s.listingIndexDirty.Load() {
				s.persistLocalIndex(ctx)
				lastPersist = time.Now()
			}
			s.loadPeerIndexes(ctx)
		}
	}
}

// persistLocalIndex saves the local listing index to the system store.
func (s *Server) persistLocalIndex(ctx context.Context) {
	if s.s3SystemStore == nil {
		return
	}

	idx := s.localListingIndex.Load()
	if idx == nil {
		return
	}

	ips := s.GetCoordMeshIPs()
	if len(ips) == 0 {
		return
	}
	selfIP := ips[0]

	if err := s.s3SystemStore.SaveJSON(ctx, "listings/"+selfIP+".json", idx); err != nil {
		log.Warn().Err(err).Msg("failed to persist listing index")
		return
	}
	s.listingIndexDirty.Store(false)

	// Immediately enqueue the updated listing file for replication so peer coordinators
	// receive it within ~50 seconds rather than waiting up to 5 minutes for the next
	// auto-sync cycle. This is critical for accordion mode where stale listing files
	// from previous iterations would otherwise accumulate in the merged view.
	if s.replicator != nil {
		s.replicator.EnqueueReplication(s3.SystemBucket, "listings/"+selfIP+".json", "put")
	}
}

// loadPeerIndexes loads replicated peer listing indexes from the system store
// and merges them into the peerListings atomic pointer.
func (s *Server) loadPeerIndexes(ctx context.Context) {
	if s.s3SystemStore == nil {
		return
	}

	ips := s.GetCoordMeshIPs()
	if len(ips) <= 1 {
		return
	}

	merged := &peerListings{
		Objects:  make(map[string][]S3ObjectInfo),
		Recycled: make(map[string][]S3ObjectInfo),
	}

	for _, peerIP := range ips[1:] {
		var idx listingIndex
		if err := s.s3SystemStore.LoadJSON(ctx, "listings/"+peerIP+".json", &idx); err != nil {
			continue // Peer index not available yet
		}
		for bucket, bl := range idx.Buckets {
			for i := range bl.Objects {
				bl.Objects[i].SourceIP = peerIP
			}
			for i := range bl.Recycled {
				bl.Recycled[i].SourceIP = peerIP
			}
			merged.Objects[bucket] = append(merged.Objects[bucket], bl.Objects...)
			merged.Recycled[bucket] = append(merged.Recycled[bucket], bl.Recycled...)
		}
	}

	// Preserve forwarded entries from the previous peerListings that haven't
	// been superseded by system store data yet. mergeObjectListings uses the
	// system store result as "local" (wins ties) so forwarded entries are
	// naturally dropped once the remote peer persists.
	// Entries older than forwardedEntryTTL are expired to prevent unbounded accumulation.
	// The TTL covers the peer listing pipeline delay: a remote coordinator persists
	// its listing to the system store (10s), the system store replicates it (up to a
	// few minutes), and loadPeerIndexes loads it (up to 60s). The reconcile cycle
	// runs every 5 minutes, so in practice the delay is ≤ 5 min + replication lag.
	// 7 minutes provides a safety margin over the 5-minute reconcile interval
	// while still tightening the stale-data window versus the previous 10 minutes.
	const forwardedEntryTTL = 7 * time.Minute

	if old := s.peerListings.Load(); old != nil {
		now := time.Now()
		for bucket, objs := range old.Objects {
			var forwarded []S3ObjectInfo
			for _, obj := range objs {
				if obj.Forwarded && now.Sub(obj.ForwardedAt) < forwardedEntryTTL {
					forwarded = append(forwarded, obj)
				}
			}
			if len(forwarded) > 0 {
				merged.Objects[bucket] = mergeObjectListings(merged.Objects[bucket], forwarded)
			}
		}
		for bucket, objs := range old.Recycled {
			var forwarded []S3ObjectInfo
			for _, obj := range objs {
				if obj.Forwarded && now.Sub(obj.ForwardedAt) < forwardedEntryTTL {
					forwarded = append(forwarded, obj)
				}
			}
			if len(forwarded) > 0 {
				merged.Recycled[bucket] = mergeObjectListings(merged.Recycled[bucket], forwarded)
			}
		}
	}

	s.peerListings.Store(merged)
}

// reconcileLocalIndex rebuilds the local listing index from a full filesystem scan.
// This catches objects that arrived via replication or were removed by GC.
func (s *Server) reconcileLocalIndex(ctx context.Context) {
	if s.s3Store == nil {
		return
	}

	// Capture the Seq before the filesystem scan. If incremental updates
	// happen during the scan (Seq advances), the merge-and-CAS loop below
	// preserves those updates while using the filesystem scan as ground truth.
	var preSeq uint64
	if pre := s.localListingIndex.Load(); pre != nil {
		preSeq = pre.Seq
	}

	buckets, err := s.s3Store.ListBuckets(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("reconcile: failed to list buckets")
		return
	}

	newIdx := &listingIndex{Buckets: make(map[string]*bucketListing)}

	for _, bkt := range buckets {
		if bkt.Name == auth.SystemBucket {
			continue
		}

		bl := &bucketListing{}

		// Get owner name for the bucket
		bucketMeta, _ := s.s3Store.HeadBucket(ctx, bkt.Name)
		var ownerName string
		if bucketMeta != nil && bucketMeta.Owner != "" {
			ownerName = s.getPeerName(bucketMeta.Owner)
		}

		// Scan all objects without holding the store mutex. Using the paginated
		// ListObjects API would hold the global RLock once per page (each page
		// re-walks the full metadata tree from the start), producing O(N²) lock
		// hold time and blocking all concurrent writes for ~9 seconds after hours
		// of accumulated objects. ScanObjectsForReconcile does a single O(N) walk
		// with no lock; concurrent-delete gaps are silently skipped and filled in
		// by the merge-CAS step below.
		objects, err := s.s3Store.ScanObjectsForReconcile(ctx, bkt.Name)
		if err != nil {
			log.Warn().Err(err).Str("bucket", bkt.Name).Msg("reconcile: failed to scan objects")
		} else {
			for _, obj := range objects {
				info := S3ObjectInfo{
					Key:          obj.Key,
					Size:         obj.Size,
					LastModified: obj.LastModified.Format(time.RFC3339),
					Owner:        ownerName,
					ContentType:  obj.ContentType,
				}
				if obj.Expires != nil {
					info.Expires = obj.Expires.Format(time.RFC3339)
				}
				bl.Objects = append(bl.Objects, info)
			}
		}

		// Scan recycled objects (also lock-free for the same reasons)
		recycled, err := s.s3Store.ScanRecycledForReconcile(ctx, bkt.Name)
		if err != nil {
			log.Warn().Err(err).Str("bucket", bkt.Name).Msg("reconcile: failed to list recycled objects")
		} else {
			for _, entry := range recycled {
				bl.Recycled = append(bl.Recycled, S3ObjectInfo{
					Key:          entry.OriginalKey,
					Size:         entry.Meta.Size,
					LastModified: entry.Meta.LastModified.Format(time.RFC3339),
					ContentType:  entry.Meta.ContentType,
					DeletedAt:    entry.DeletedAt.Format(time.RFC3339),
				})
			}
		}

		if len(bl.Objects) > 0 || len(bl.Recycled) > 0 {
			newIdx.Buckets[bkt.Name] = bl
		}
	}

	// Measure and report listing index drift (entries in current that are absent from filesystem).
	// This should be 0 in steady state; non-zero indicates a missed deletion callback.
	current := s.localListingIndex.Load()
	stale := countStaleListingEntries(current, newIdx)
	if m := s3.GetS3Metrics(); m != nil {
		m.ListingIndexStaleEntries.Set(float64(stale))
		if stale > 0 {
			m.ListingIndexStaleCleaned.Add(float64(stale))
		}
	}

	// Merge filesystem scan (ground truth) with any concurrent incremental
	// updates that arrived during the scan. Use CAS to avoid lost updates.
	for {
		current := s.localListingIndex.Load()
		merged := mergeReconcileWithCurrent(newIdx, current, preSeq)

		if listingIndexEqual(current, merged) {
			break
		}
		if s.localListingIndex.CompareAndSwap(current, merged) {
			s.listingIndexDirty.Store(true)
			select {
			case s.listingIndexNotify <- struct{}{}:
			default:
			}
			break
		}
		// CAS failed (concurrent incremental update), retry merge
	}
}

// mergeReconcileWithCurrent merges a filesystem scan result (ground truth) with the
// current listing index. Incremental updates that arrived during the scan (detected
// by Seq advancing past preSeq) are preserved so they aren't lost.
func mergeReconcileWithCurrent(scan, current *listingIndex, preSeq uint64) *listingIndex {
	merged := &listingIndex{Buckets: make(map[string]*bucketListing)}
	for k, v := range scan.Buckets {
		merged.Buckets[k] = v
	}

	if current == nil {
		return merged
	}

	merged.Seq = current.Seq

	// No incremental updates during scan — filesystem result is complete
	if current.Seq <= preSeq {
		return merged
	}

	// Preserve entries added by incremental updates during the scan
	for bucket, currentBL := range current.Buckets {
		scanBL := merged.Buckets[bucket]
		if scanBL == nil {
			merged.Buckets[bucket] = currentBL
			continue
		}
		scanBL.Objects = appendMissing(scanBL.Objects, currentBL.Objects)
		scanBL.Recycled = appendMissing(scanBL.Recycled, currentBL.Recycled)
	}

	return merged
}

// appendMissing appends entries from src that are not already present (by key) in dst.
// For keys present in both, dst's version wins. This means metadata (size, content-type)
// may temporarily show the filesystem's version until the next reconcile cycle.
func appendMissing(dst, src []S3ObjectInfo) []S3ObjectInfo {
	existing := make(map[string]struct{}, len(dst))
	for _, obj := range dst {
		existing[obj.Key] = struct{}{}
	}
	for _, obj := range src {
		if _, ok := existing[obj.Key]; !ok {
			dst = append(dst, obj)
		}
	}
	return dst
}

// listingIndexEqual compares two listing indexes for equality.
// Uses map-based comparison so ordering differences don't cause false negatives.
func listingIndexEqual(a, b *listingIndex) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a.Buckets) != len(b.Buckets) {
		return false
	}
	for name, aBL := range a.Buckets {
		bBL, ok := b.Buckets[name]
		if !ok {
			return false
		}
		if len(aBL.Objects) != len(bBL.Objects) || len(aBL.Recycled) != len(bBL.Recycled) {
			return false
		}
		// Build map from b's objects for O(1) lookup
		bObjs := make(map[string]S3ObjectInfo, len(bBL.Objects))
		for _, obj := range bBL.Objects {
			bObjs[obj.Key] = obj
		}
		for _, obj := range aBL.Objects {
			bObj, ok := bObjs[obj.Key]
			if !ok || obj.Size != bObj.Size || obj.LastModified != bObj.LastModified {
				return false
			}
		}
		// Build map from b's recycled for O(1) lookup
		bRecycled := make(map[string]S3ObjectInfo, len(bBL.Recycled))
		for _, obj := range bBL.Recycled {
			bRecycled[obj.Key] = obj
		}
		for _, obj := range aBL.Recycled {
			bObj, ok := bRecycled[obj.Key]
			if !ok || obj.Size != bObj.Size || obj.DeletedAt != bObj.DeletedAt {
				return false
			}
		}
	}
	return true
}

// upsertObjectList returns a new slice with info replacing any existing entry with
// the same key, or appended if no match exists.
func upsertObjectList(objs []S3ObjectInfo, key string, info S3ObjectInfo) []S3ObjectInfo {
	result := make([]S3ObjectInfo, 0, len(objs)+1)
	found := false
	for _, obj := range objs {
		if obj.Key == key {
			result = append(result, info)
			found = true
		} else {
			result = append(result, obj)
		}
	}
	if !found {
		result = append(result, info)
	}
	return result
}

// countStaleListingEntries counts Objects entries in current that are absent from the filesystem scan.
// Returns 0 if current is nil. Non-zero indicates phantom entries (missed deletion callbacks).
func countStaleListingEntries(current, filesystem *listingIndex) int {
	if current == nil {
		return 0
	}
	n := 0
	for bucket, bl := range current.Buckets {
		fsBL := filesystem.Buckets[bucket]
		if fsBL == nil {
			// Entire bucket missing from filesystem — all entries are stale
			n += len(bl.Objects)
			continue
		}
		// Build a set from filesystem objects for O(1) lookups
		fsKeys := make(map[string]struct{}, len(fsBL.Objects))
		for _, obj := range fsBL.Objects {
			fsKeys[obj.Key] = struct{}{}
		}
		// Count current entries not in filesystem
		for _, obj := range bl.Objects {
			if _, exists := fsKeys[obj.Key]; !exists {
				n++
			}
		}
	}
	return n
}

// removeFromObjectList returns a new slice with the entry matching key removed.
func removeFromObjectList(objs []S3ObjectInfo, key string) ([]S3ObjectInfo, *S3ObjectInfo) {
	var removed *S3ObjectInfo
	result := make([]S3ObjectInfo, 0, len(objs))
	for _, obj := range objs {
		if obj.Key == key && removed == nil {
			o := obj
			removed = &o
		} else {
			result = append(result, obj)
		}
	}
	return result, removed
}
