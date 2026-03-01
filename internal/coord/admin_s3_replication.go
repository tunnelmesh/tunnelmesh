package coord

import (
	"hash/fnv"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"
)

// coordForwardBreaker is a per-target circuit breaker for S3 write forwarding.
// When a target coordinator consistently returns errors, the circuit opens and
// forwarding is skipped (falling through to local handling) until the backoff
// expires. This prevents log floods and avoids the connection-timeout latency
// on every upload when a peer coordinator is unreachable.
type coordForwardBreaker struct {
	mu    sync.Mutex
	state map[string]*coordBreakerState
}

type coordBreakerState struct {
	consecutiveFails int
	cooldownUntil    time.Time
}

// IsOpen returns true if the circuit for the given target is currently open
// (i.e. forwarding should be skipped). Once the cooldown expires the caller
// will attempt a forward again (half-open probe).
func (cb *coordForwardBreaker) IsOpen(target string) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	st := cb.state[target]
	return st != nil && time.Now().Before(st.cooldownUntil)
}

// RecordFailure records a forwarding failure for target. Returns true on the
// first failure (so the caller can decide to log at WARN level) and false on
// subsequent failures while the circuit is already open.
func (cb *coordForwardBreaker) RecordFailure(target string) (firstFailure bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == nil {
		cb.state = make(map[string]*coordBreakerState)
	}
	st := cb.state[target]
	if st == nil {
		st = &coordBreakerState{}
		cb.state[target] = st
	}
	first := st.consecutiveFails == 0
	st.consecutiveFails++
	// Exponential backoff: 10s × 2^(n-1), capped at 5 min.
	backoff := (10 * time.Second) << (st.consecutiveFails - 1)
	if backoff > 5*time.Minute {
		backoff = 5 * time.Minute
	}
	st.cooldownUntil = time.Now().Add(backoff)
	return first
}

// RecordSuccess resets the circuit for target. Returns true if the circuit was
// previously open (so the caller can log a recovery message).
func (cb *coordForwardBreaker) RecordSuccess(target string) (wasOpen bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == nil {
		return false
	}
	st := cb.state[target]
	if st == nil || st.consecutiveFails == 0 {
		return false
	}
	delete(cb.state, target)
	return true
}

// objectPrimaryCoordinator returns the mesh IP of the coordinator that should own
// the given bucket/key combination. Returns "" if this coordinator is the primary
// or if there's only one coordinator (no forwarding needed).
func (s *Server) objectPrimaryCoordinator(bucket, key string) string {
	ips := s.GetCoordMeshIPs()
	if len(ips) <= 1 {
		return ""
	}

	selfIP := ips[0] // Self is always first (invariant of GetCoordMeshIPs)

	// Use cached sorted list (updated when coordinator list changes)
	sorted := s.getSortedCoordIPs()
	if len(sorted) <= 1 {
		return ""
	}

	// FNV-1a hash of bucket+key (null separator prevents ambiguity between
	// bucket="a", key="b/c" and bucket="a/b", key="c")
	h := fnv.New32a()
	h.Write([]byte(bucket))
	h.Write([]byte{0})
	h.Write([]byte(key))
	primaryIdx := int(h.Sum32()) % len(sorted)

	primary := sorted[primaryIdx]
	if primary == selfIP {
		return ""
	}
	if s.forwardBreaker.IsOpen(primary) {
		return ""
	}
	return primary
}

// discardResponseWriter captures the status code and headers from a
// forwarded request while discarding the response body.  This allows the
// caller to inspect the result and decide whether to use it or fall back
// to local handling.
type discardResponseWriter struct {
	header http.Header
	status int
}

func (d *discardResponseWriter) Header() http.Header         { return d.header }
func (d *discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (d *discardResponseWriter) WriteHeader(code int)        { d.status = code }

// updatePeerListingsAfterForward immediately updates the peerListings atomic pointer
// after a successful forwarded write, so the listing handler shows the object without
// waiting for the background indexer to persist and reload.
// It also persists the entry to localListingIndex so it survives background reloads.
func (s *Server) updatePeerListingsAfterForward(bucket, key, targetIP string, r *http.Request) {
	for {
		old := s.peerListings.Load()

		newPL := &peerListings{
			Objects:  make(map[string][]S3ObjectInfo),
			Recycled: make(map[string][]S3ObjectInfo),
		}
		if old != nil {
			for k, v := range old.Objects {
				newPL.Objects[k] = v
			}
			for k, v := range old.Recycled {
				newPL.Recycled[k] = v
			}
		}

		switch r.Method {
		case http.MethodPut:
			info := S3ObjectInfo{
				Key:          key,
				Size:         r.ContentLength,
				LastModified: time.Now().UTC().Format(time.RFC3339),
				ContentType:  r.Header.Get("Content-Type"),
				Forwarded:    true,
				ForwardedAt:  time.Now(),
				SourceIP:     targetIP,
			}
			if info.ContentType == "" {
				info.ContentType = "application/octet-stream"
			}
			if bucketMeta, err := s.s3Store.HeadBucket(r.Context(), bucket); err == nil && bucketMeta.Owner != "" {
				info.Owner = s.getPeerName(bucketMeta.Owner)
			}
			newPL.Objects[bucket] = upsertObjectList(newPL.Objects[bucket], key, info)

		case http.MethodDelete:
			var removed *S3ObjectInfo
			newPL.Objects[bucket], removed = removeFromObjectList(newPL.Objects[bucket], key)
			if removed != nil {
				recycled := *removed
				recycled.DeletedAt = time.Now().UTC().Format(time.RFC3339)
				recycled.Forwarded = true
				recycled.ForwardedAt = time.Now()
				recycled.SourceIP = targetIP
				newPL.Recycled[bucket] = upsertObjectList(newPL.Recycled[bucket], key, recycled)
			}
		}

		if s.peerListings.CompareAndSwap(old, newPL) {
			break
		}
	}

	// NOTE: Forwarded entries are NOT added to localListingIndex because:
	// 1. localListingIndex should only contain locally stored files
	// 2. When persisted to system store and loaded by other coordinators,
	//    the SourceIP field would be overwritten with the wrong coordinator IP
	// 3. Forwarded entries in peerListings have a 10-minute TTL for cleanup
}

// forwardS3Request proxies an S3 request to the target coordinator.
// bucket is used to include the bucket owner in the forwarded request so the
// target can create the bucket on-the-fly (avoids race with share replication).
func (s *Server) forwardS3Request(w http.ResponseWriter, r *http.Request, targetIP, bucket string) {
	// Look up bucket owner to include in forwarded request
	var bucketOwner string
	if meta, err := s.s3Store.HeadBucket(r.Context(), bucket); err == nil {
		bucketOwner = meta.Owner
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "https"
			req.URL.Host = targetIP
			req.Host = targetIP
			req.Header.Set("X-TunnelMesh-Forwarded", "true")
			if bucketOwner != "" {
				req.Header.Set("X-TunnelMesh-Bucket-Owner", bucketOwner)
			}
		},
		Transport: s.s3ForwardTransport,
	}
	proxy.ServeHTTP(w, r)
}

// NotifyObjectDeleted implements s3.DeleteNotifier. It enqueues a delete replication
// so that replica coordinators remove the object.
func (s *Server) NotifyObjectDeleted(bucket, key string) {
	if s.replicator != nil {
		s.replicator.EnqueueReplication(bucket, key, "delete")
	}
}

// ForwardS3Request implements s3.RequestForwarder. It checks if the given bucket/key
// should be handled by a different coordinator and forwards the request if so.
// The port parameter specifies the target port (e.g. "9000" for S3 API, "" for default 443).
func (s *Server) ForwardS3Request(w http.ResponseWriter, r *http.Request, bucket, key, port string) bool {
	if r.Header.Get("X-TunnelMesh-Forwarded") != "" {
		return false
	}
	var target string
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		target = s.findObjectSourceIP(bucket, key)
	}
	if target == "" {
		target = s.objectPrimaryCoordinator(bucket, key)
	}
	if target == "" {
		return false
	}
	if port != "" {
		target = net.JoinHostPort(target, port)
	}
	s.forwardS3Request(w, r, target, bucket)
	return true
}
