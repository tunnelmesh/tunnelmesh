package replication

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// replQueueEntry represents a pending replication operation in the queue.
type replQueueEntry struct {
	bucket     string
	key        string
	op         string // "put" or "delete"
	enqueuedAt time.Time
	retries    int
}

// EnqueueReplication adds a replication operation to the background queue.
// This is O(1) and non-blocking. Last-writer-wins dedup ensures that if the
// same key is enqueued multiple times, only the latest operation is processed.
func (r *Replicator) EnqueueReplication(bucket, key, op string) {
	compositeKey := bucket + "\x00" + key
	r.replPending.Store(compositeKey, &replQueueEntry{
		bucket:     bucket,
		key:        key,
		op:         op,
		enqueuedAt: time.Now(),
	})

	// Non-blocking signal to wake up the worker
	select {
	case r.replQueue <- struct{}{}:
	default:
	}
}

// runReplicationQueueWorker is the background goroutine that processes
// queued replication operations. It wakes up on signal or every 5 seconds.
func (r *Replicator) runReplicationQueueWorker() {
	defer r.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.replQueue:
			r.drainReplicationQueue(r.ctx)
		case <-ticker.C:
			r.drainReplicationQueue(r.ctx)
		}
	}
}

// drainReplicationQueue snapshots the pending map, clears it, and processes
// all entries with bounded concurrency.
func (r *Replicator) drainReplicationQueue(ctx context.Context) {
	// Snapshot and clear pending entries
	var entries []*replQueueEntry
	r.replPending.Range(func(key, value any) bool {
		entry := value.(*replQueueEntry)
		entries = append(entries, entry)
		r.replPending.Delete(key)
		return true
	})

	if len(entries) == 0 {
		return
	}

	r.logger.Info().
		Int("entries", len(entries)).
		Msg("Draining replication queue")

	// Process with bounded concurrency
	const maxConcurrent = 5
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return
		default:
		}

		wg.Add(1)
		go func(e *replQueueEntry) {
			defer wg.Done()

			// Acquire concurrency slot
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			r.processQueueEntry(ctx, e)
		}(entry)
	}

	wg.Wait()
}

// processQueueEntry handles a single replication queue entry.
func (r *Replicator) processQueueEntry(ctx context.Context, entry *replQueueEntry) {
	peers := r.GetPeers()
	if len(peers) == 0 {
		return
	}

	switch entry.op {
	case "put":
		r.processQueuePut(ctx, entry, peers)
	case "delete":
		r.processQueueDelete(ctx, entry, peers)
	}
}

// processQueuePut replicates an object to all peers in parallel.
// Each peer gets its own goroutine with an independent 60s timeout, so one slow/unreachable
// peer cannot starve others of time (the root cause of uneven distribution).
// Note: chunk cleanup is deferred to GC — the rebalancer no longer deletes chunks
// directly, instead relying on EnqueueReplication to propagate metadata so GC can
// safely identify orphaned chunks.
func (r *Replicator) processQueuePut(ctx context.Context, entry *replQueueEntry, peers []string) {
	ctx, span := tracing.Tracer("tunnelmesh/replication").Start(ctx, "replication.put")
	defer span.End()
	span.SetAttributes(
		attribute.String("s3.bucket", entry.bucket),
		attribute.String("s3.key", entry.key),
		attribute.Int("replication.peer_count", len(peers)),
		attribute.Int("replication.retry", entry.retries),
	)

	if r.chunkRegistry == nil {
		// No chunk registry — fall back to file-level replication
		span.SetStatus(otelcodes.Ok, "no chunk registry")
		return
	}

	type peerResult struct {
		peerID string
		err    error
	}
	results := make(chan peerResult, len(peers))

	for _, peerID := range peers {
		go func(pid string) {
			peerCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			results <- peerResult{pid, r.ReplicateObject(peerCtx, entry.bucket, entry.key, pid)}
		}(peerID)
	}

	allSucceeded := true
	for range peers {
		res := <-results
		span.AddEvent("replication.peer", trace.WithAttributes(
			attribute.String("peer.id", res.peerID),
			attribute.Bool("peer.succeeded", res.err == nil),
		))
		if res.err != nil {
			r.logger.Error().Err(res.err).
				Str("peer", res.peerID).
				Str("bucket", entry.bucket).
				Str("key", entry.key).
				Int("retry", entry.retries).
				Msg("Queued replication failed")
			span.RecordError(res.err)
			allSucceeded = false
		}
	}

	if !allSucceeded {
		span.SetStatus(otelcodes.Error, "one or more peers failed")
		r.reEnqueueOnFailure(entry)
	} else {
		sc := span.SpanContext()
		r.logger.Debug().
			Str("bucket", entry.bucket).
			Str("key", entry.key).
			Int("peers", len(peers)).
			Str("trace_id", sc.TraceID().String()).
			Str("span_id", sc.SpanID().String()).
			Msg("replication complete")
		span.SetStatus(otelcodes.Ok, "")
	}
}

// processQueueDelete replicates a delete operation to all peers.
func (r *Replicator) processQueueDelete(ctx context.Context, entry *replQueueEntry, peers []string) {
	ctx, span := tracing.Tracer("tunnelmesh/replication").Start(ctx, "replication.delete")
	defer span.End()
	span.SetAttributes(
		attribute.String("s3.bucket", entry.bucket),
		attribute.String("s3.key", entry.key),
		attribute.Int("replication.peer_count", len(peers)),
		attribute.Int("replication.retry", entry.retries),
	)

	entryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := r.ReplicateDelete(entryCtx, entry.bucket, entry.key); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		r.logger.Error().Err(err).
			Str("bucket", entry.bucket).
			Str("key", entry.key).
			Int("retry", entry.retries).
			Msg("Queued delete replication failed")
		r.reEnqueueOnFailure(entry)
	} else {
		sc := span.SpanContext()
		span.SetStatus(otelcodes.Ok, "")
		r.logger.Debug().
			Str("bucket", entry.bucket).
			Str("key", entry.key).
			Int("peers", len(peers)).
			Str("trace_id", sc.TraceID().String()).
			Str("span_id", sc.SpanID().String()).
			Msg("delete replication complete")
	}
}

// reEnqueueOnFailure re-enqueues a failed entry up to 3 retries.
func (r *Replicator) reEnqueueOnFailure(entry *replQueueEntry) {
	const maxRetries = 3
	if entry.retries >= maxRetries {
		r.logger.Warn().
			Str("bucket", entry.bucket).
			Str("key", entry.key).
			Str("op", entry.op).
			Int("retries", entry.retries).
			Msg("Replication failed after max retries, dropping")
		return
	}

	compositeKey := entry.bucket + "\x00" + entry.key

	// Only re-enqueue if no newer entry was added
	_, loaded := r.replPending.LoadOrStore(compositeKey, &replQueueEntry{
		bucket:     entry.bucket,
		key:        entry.key,
		op:         entry.op,
		enqueuedAt: time.Now(),
		retries:    entry.retries + 1,
	})

	if !loaded {
		// Successfully stored, signal the worker
		select {
		case r.replQueue <- struct{}{}:
		default:
		}
	}
}

// drainReplicationQueueFinal performs a final drain during shutdown with a timeout.
func (r *Replicator) drainReplicationQueueFinal() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.drainReplicationQueue(ctx)
}

// runAutoSyncWorker periodically re-enqueues all objects for replication.
// This is a safety net to catch any objects that were missed due to failures.
func (r *Replicator) runAutoSyncWorker() {
	defer r.wg.Done()

	// Initial delay to let the cluster stabilize, but wake early on on-demand trigger.
	initialTimer := time.NewTimer(r.autoSyncInitialDelay)
	select {
	case <-r.ctx.Done():
		initialTimer.Stop()
		return
	case <-r.manifestSyncCh:
		initialTimer.Stop()
		r.runAutoSyncCycle()
		// Drain any trigger that arrived during the cycle — the cycle already
		// captured the latest state, so an immediately queued trigger is redundant.
		select {
		case <-r.manifestSyncCh:
		default:
		}
	case <-initialTimer.C:
	}

	ticker := time.NewTicker(r.autoSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.runAutoSyncCycle()
		case <-r.manifestSyncCh:
			r.runAutoSyncCycle()
			// Drain any trigger that arrived during the cycle.
			select {
			case <-r.manifestSyncCh:
			default:
			}
		}
	}
}

// runAutoSyncCycle enqueues locally-owned objects for replication.
// Only buckets owned by this coordinator are synced — replicated buckets
// (owned by peer coordinators) are skipped to prevent ping-pong replication
// where replicas re-replicate received objects back to the source.
func (r *Replicator) runAutoSyncCycle() {
	if r.preSyncHook != nil {
		r.preSyncHook()
	}

	peers := r.GetPeers()
	if len(peers) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(r.ctx, 3*time.Minute)
	defer cancel()

	allKeys, err := r.s3.GetAllObjectKeys(ctx)
	if err != nil {
		r.logger.Warn().Err(err).Msg("Auto-sync: failed to get all object keys")
		return
	}

	// Build peer node ID set for O(1) lookups. Bucket owners are stored as
	// coordinator names (e.g. "coordinator-1"), not mesh IPs, so we need the
	// name mapping to correctly identify replicated buckets.
	peerNodeIDs := r.getPeerNodeIDs()

	// Filter to locally-owned buckets only. On replicas, buckets imported via
	// replication have their owner set to the source coordinator's node ID.
	// We skip those to avoid re-replicating objects back to the source.
	localKeys := make(map[string][]string, len(allKeys))
	var skippedBuckets int
	for bucket, keys := range allKeys {
		owner := r.s3.GetBucketOwner(ctx, bucket)
		if _, isPeerNode := peerNodeIDs[owner]; isPeerNode {
			skippedBuckets++
			continue
		}
		localKeys[bucket] = keys
	}

	var total int
	for bucket, keys := range localKeys {
		for _, key := range keys {
			r.EnqueueReplication(bucket, key, "put")
			total++
		}
		_ = bucket
	}

	if total > 0 || skippedBuckets > 0 {
		r.logger.Info().
			Int("objects", total).
			Int("peers", len(peers)).
			Int("skipped_replica_buckets", skippedBuckets).
			Msg("Auto-sync: enqueued locally-owned objects for replication")
	}

	// Send object manifest for reconciliation — replicas purge objects
	// not in the manifest to recover from lost delete messages.
	// Only includes locally-owned buckets: each coordinator is authoritative
	// for its own buckets and should not send manifests for replicated ones.
	r.sendObjectManifest(ctx, localKeys, peers)
}

// sendObjectManifest sends the set of live objects to each peer so they can
// purge any local objects not in the manifest. This is fire-and-forget:
// reconciliation is idempotent and will be retried on the next auto-sync cycle.
//
// Safety: we never send an empty manifest. During transient states (startup,
// S3 not yet loaded) an empty manifest would cause replicas to purge all
// their objects. Skipping is safe — the next cycle will send a full manifest.
func (r *Replicator) sendObjectManifest(ctx context.Context, allKeys map[string][]string, peers []string) {
	if len(allKeys) == 0 || len(peers) == 0 {
		return
	}

	payload := ObjectManifestPayload{BucketKeys: allKeys}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		r.logger.Error().Err(err).Msg("Auto-sync: failed to marshal object manifest")
		return
	}

	msg := &Message{
		Version: ProtocolVersion,
		Type:    MessageTypeObjectManifest,
		ID:      uuid.New().String(),
		From:    r.nodeID,
		Payload: json.RawMessage(payloadJSON),
	}

	data, err := msg.Marshal()
	if err != nil {
		r.logger.Error().Err(err).Msg("Auto-sync: failed to marshal manifest message")
		return
	}

	for _, peerID := range peers {
		if err := r.acquireSendSlot(ctx); err != nil {
			r.logger.Warn().Err(err).Msg("Auto-sync: context cancelled during manifest send")
			return
		}
		sendErr := r.transport.SendToCoordinator(ctx, peerID, data)
		r.releaseSendSlot()
		if sendErr != nil {
			r.logger.Warn().Err(sendErr).
				Str("peer", peerID).
				Msg("Auto-sync: failed to send object manifest")
		}
	}

	r.logger.Debug().
		Int("peers", len(peers)).
		Int("manifest_bytes", len(payloadJSON)).
		Msg("Auto-sync: sent object manifest for reconciliation")
}
