package coord

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

// handleShares handles GET (list) and POST (create) for file shares.
func (s *Server) handleShares(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleSharesList(w, r)
	case http.MethodPost:
		s.handleShareCreate(w, r)
	default:
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// FileShareResponse extends FileShare with additional computed fields.
type FileShareResponse struct {
	*s3.FileShare
	OwnerName string `json:"owner_name,omitempty"` // Human-readable owner name (looked up from peer ID)
	SizeBytes int64  `json:"size_bytes"`           // Actual size of all objects in the share bucket
}

// handleSharesList returns all file shares.
func (s *Server) handleSharesList(w http.ResponseWriter, r *http.Request) {
	if s.fileShareMgr == nil {
		s.jsonError(w, "file shares not enabled", http.StatusServiceUnavailable)
		return
	}

	shares := s.fileShareMgr.List()

	// Convert shares to response format with owner names and calculated sizes
	response := make([]FileShareResponse, len(shares))
	for i, share := range shares {
		// Calculate bucket size (returns 0 on error, which is fine for display)
		bucketName := s.fileShareMgr.BucketName(share.Name)
		sizeBytes, _ := s.s3Store.CalculateBucketSize(r.Context(), bucketName)

		response[i] = FileShareResponse{
			FileShare: share,
			OwnerName: s.getPeerName(share.Owner),
			SizeBytes: sizeBytes,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// ShareCreateRequest is the request body for creating a file share.
type ShareCreateRequest struct {
	Name              string `json:"name"`
	Description       string `json:"description,omitempty"`
	QuotaBytes        int64  `json:"quota_bytes,omitempty"`        // Per-share quota in bytes (0 = unlimited within global quota)
	ExpiresAt         string `json:"expires_at,omitempty"`         // ISO 8601 date when share expires (empty = use default)
	GuestRead         *bool  `json:"guest_read,omitempty"`         // Allow all mesh users to read (nil = true, false = owner only)
	ReplicationFactor int    `json:"replication_factor,omitempty"` // Number of replicas (1-3), defaults to 2
}

// validateShareName checks that a share name is DNS-safe (alphanumeric, hyphens, underscores, 1-63 chars).
func validateShareName(name string) error {
	if len(name) > 63 {
		return fmt.Errorf("name too long (max 63 characters)")
	}
	for i, c := range name {
		isAlphaNum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		isValidHyphen := c == '-' && i > 0 && i < len(name)-1
		isUnderscore := c == '_'
		if !isAlphaNum && !isValidHyphen && !isUnderscore {
			return fmt.Errorf("name must be alphanumeric with optional hyphens (not at start/end)")
		}
	}
	return nil
}

// validateShareQuota checks that share quota is valid.
func validateShareQuota(quotaBytes int64) error {
	const maxQuotaBytes = 1024 * 1024 * 1024 * 1024 // 1TB
	if quotaBytes < 0 {
		return fmt.Errorf("quota_bytes must be non-negative")
	}
	if quotaBytes > maxQuotaBytes {
		return fmt.Errorf("quota_bytes exceeds maximum (1TB)")
	}
	return nil
}

// handleShareCreate creates a new file share.
func (s *Server) handleShareCreate(w http.ResponseWriter, r *http.Request) {
	if s.fileShareMgr == nil {
		s.jsonError(w, "file shares not enabled", http.StatusServiceUnavailable)
		return
	}

	var req ShareCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		s.jsonError(w, "name is required", http.StatusBadRequest)
		return
	}

	// Get owner from TLS client certificate - required for share creation
	ownerID := s.getRequestOwner(r)
	if ownerID == "" {
		s.jsonError(w, "client certificate required for share creation", http.StatusUnauthorized)
		return
	}

	// Auto-prefix share name with peer name to prevent name squatting
	peerName := s.getPeerName(ownerID)
	if peerName != "" && peerName != ownerID {
		req.Name = peerName + "_" + req.Name
	}

	// Validate share name and quota
	if err := validateShareName(req.Name); err != nil {
		s.jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateShareQuota(req.QuotaBytes); err != nil {
		s.jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build share options
	opts := &s3.FileShareOptions{}
	if req.ExpiresAt != "" {
		expiresAt, err := time.Parse(time.RFC3339, req.ExpiresAt)
		if err != nil {
			// Try parsing as date-only (YYYY-MM-DD)
			expiresAt, err = time.Parse("2006-01-02", req.ExpiresAt)
			if err != nil {
				s.jsonError(w, "invalid expires_at format (use ISO 8601)", http.StatusBadRequest)
				return
			}
			// Set to end of day in UTC
			expiresAt = expiresAt.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
		}
		if expiresAt.Before(time.Now()) {
			s.jsonError(w, "expires_at must be in the future", http.StatusBadRequest)
			return
		}
		opts.ExpiresAt = expiresAt.UTC()
	}
	if req.GuestRead != nil {
		opts.GuestRead = *req.GuestRead
		opts.GuestReadSet = true
	}
	// Set replication factor (default to 2)
	if req.ReplicationFactor == 0 {
		opts.ReplicationFactor = 2
	} else {
		opts.ReplicationFactor = req.ReplicationFactor
	}

	share, err := s.fileShareMgr.Create(r.Context(), req.Name, req.Description, ownerID, req.QuotaBytes, opts)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			s.jsonError(w, err.Error(), http.StatusConflict)
		} else {
			s.jsonError(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Persist group bindings (file share creates them)
	if s.s3SystemStore != nil {
		if err := s.s3SystemStore.SaveGroupBindings(r.Context(), s.s3Authorizer.GroupBindings.List()); err != nil {
			log.Warn().Err(err).Msg("failed to persist group bindings")
		}
	}

	// Trigger immediate replication of file share metadata so other coordinators
	// learn about the new share before PurgeOrphanedFileShareBuckets runs.
	// Without this, share metadata relies on auto-sync (up to 7 min delay),
	// during which other coordinators may purge the fs+ bucket as orphaned.
	if s.replicator != nil {
		s.replicator.EnqueueReplication(s3.SystemBucket, s3.FileSharesPath, "put")
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(share)
}

// handleShareByName handles GET and DELETE for a specific file share.
func (s *Server) handleShareByName(w http.ResponseWriter, r *http.Request) {
	if s.fileShareMgr == nil {
		s.jsonError(w, "file shares not enabled", http.StatusServiceUnavailable)
		return
	}

	// Parse path: /api/shares/{name}
	path := strings.TrimPrefix(r.URL.Path, "/api/shares/")
	shareName := strings.TrimSuffix(path, "/")

	if shareName == "" {
		s.jsonError(w, "share name required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		share := s.fileShareMgr.Get(shareName)
		if share == nil {
			s.jsonError(w, "share not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(share)

	case http.MethodDelete:
		share := s.fileShareMgr.Get(shareName)
		if share == nil {
			s.jsonError(w, "share not found", http.StatusNotFound)
			return
		}

		if err := s.fileShareMgr.Delete(r.Context(), shareName); err != nil {
			s.jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Persist group bindings (file share removes them)
		if s.s3SystemStore != nil {
			if err := s.s3SystemStore.SaveGroupBindings(r.Context(), s.s3Authorizer.GroupBindings.List()); err != nil {
				log.Warn().Err(err).Msg("failed to persist group bindings")
			}
		}

		// Trigger immediate replication of share deletion so other coordinators
		// know to purge the orphaned fs+ bucket promptly.
		if s.replicator != nil {
			s.replicator.EnqueueReplication(s3.SystemBucket, s3.FileSharesPath, "put")
			// Immediately broadcast manifest so peers purge deleted bucket's objects
			// without waiting for the next 5-minute auto-sync cycle.
			s.replicator.TriggerManifestSync()
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})

	default:
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
