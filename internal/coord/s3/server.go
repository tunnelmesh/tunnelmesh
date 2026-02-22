package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// statusRecorder wraps http.ResponseWriter to capture the HTTP status code.
// Note: Not thread-safe. Must only be used within a single request handler.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
		r.ResponseWriter.WriteHeader(code)
	}
	// Subsequent calls are silently ignored to prevent "http: superfluous response.WriteHeader call" warnings
}

// getStatus returns the recorded status, defaulting to 200 if WriteHeader was never called.
func (r *statusRecorder) getStatus() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// ClassifyS3Status converts HTTP status code to metric status string.
func ClassifyS3Status(httpStatus int) string {
	switch {
	case httpStatus >= 200 && httpStatus < 300:
		return "success"
	case httpStatus == http.StatusNotFound:
		return "not_found"
	case httpStatus == http.StatusForbidden:
		return "access_denied" // Could be access_denied or quota_exceeded
	case httpStatus >= 400 && httpStatus < 500:
		return "error"
	case httpStatus >= 500:
		return "error"
	default:
		return "error"
	}
}

// ClassifyS3StatusWithError converts HTTP status and error to metric status string.
// This allows distinguishing between quota_exceeded and access_denied (both use 403).
func ClassifyS3StatusWithError(httpStatus int, err error) string {
	// Check error type first for semantic classification
	if err != nil {
		switch {
		case errors.Is(err, ErrQuotaExceeded):
			return "quota_exceeded"
		case errors.Is(err, ErrAccessDenied):
			return "access_denied"
		case errors.Is(err, ErrBucketNotFound), errors.Is(err, ErrObjectNotFound):
			return "not_found"
		}
	}

	// Fall back to HTTP status classification
	return ClassifyS3Status(httpStatus)
}

// withMetrics wraps an S3 handler with metrics instrumentation.
// The handler function receives a statusRecorder as its http.ResponseWriter,
// and metrics are automatically recorded when the handler returns.
func (s *Server) withMetrics(w http.ResponseWriter, operation string, fn func(http.ResponseWriter)) {
	m := s.metrics.Load()
	startTime := time.Now()
	rec := &statusRecorder{ResponseWriter: w}
	defer func() {
		if m != nil {
			duration := time.Since(startTime).Seconds()
			status := ClassifyS3Status(rec.getStatus())
			m.RecordRequest(operation, status, duration)
		}
	}()
	fn(rec)
}

// RequestForwarder can forward S3 requests to the correct primary coordinator.
type RequestForwarder interface {
	// ForwardS3Request forwards the request if this coordinator is not the primary
	// for the given bucket/key. The port parameter specifies the target port on the
	// remote coordinator (e.g. "9000" for S3 API, "" for default HTTPS 443).
	// Returns true if the request was forwarded.
	ForwardS3Request(w http.ResponseWriter, r *http.Request, bucket, key, port string) (forwarded bool)
}

// Server provides an S3-compatible HTTP interface.
type Server struct {
	store       *Store
	authorizer  Authorizer
	metrics     atomic.Pointer[S3Metrics]
	recoverer   BucketRecoverer
	forwarder   RequestForwarder
	delNotifier DeleteNotifier
}

// Authorizer is the interface for checking S3 permissions.
type Authorizer interface {
	// AuthorizeRequest authenticates and authorizes an S3 request.
	// The objectKey parameter enables object-level prefix permission checks.
	// Returns the user ID if authorized, or an error if not.
	AuthorizeRequest(r *http.Request, verb, resource, bucket, objectKey string) (userID string, err error)

	// GetAllowedPrefixes returns the object prefixes a user can access in a bucket.
	// Returns nil if user has unrestricted access (no filtering needed).
	// Returns empty slice if user has no access to the bucket.
	GetAllowedPrefixes(userID, bucket string) []string
}

// DeleteNotifier is called after an object is deleted via the S3 API.
// The coordinator implements this to enqueue delete replication.
type DeleteNotifier interface {
	NotifyObjectDeleted(bucket, key string)
}

// BucketRecoverer can recreate missing buckets for existing shares.
type BucketRecoverer interface {
	EnsureBucketForShare(ctx context.Context, bucketName string) error
}

// SetBucketRecoverer sets the bucket recoverer for on-demand bucket recreation.
func (s *Server) SetBucketRecoverer(r BucketRecoverer) {
	s.recoverer = r
}

// SetRequestForwarder sets the request forwarder for distributing requests across coordinators.
func (s *Server) SetRequestForwarder(f RequestForwarder) {
	s.forwarder = f
}

// SetDeleteNotifier sets the notifier called after objects are deleted via the S3 API.
func (s *Server) SetDeleteNotifier(n DeleteNotifier) {
	s.delNotifier = n
}

// SetMetrics atomically sets or replaces the metrics instance on the server.
// Safe to call while HTTP handlers are running.
func (s *Server) SetMetrics(m *S3Metrics) {
	s.metrics.Store(m)
}

// NewServer creates a new S3 server.
// If metrics is nil, metrics will not be recorded until SetMetrics is called.
func NewServer(store *Store, authorizer Authorizer, metrics *S3Metrics) *Server {
	srv := &Server{
		store:      store,
		authorizer: authorizer,
	}
	if metrics != nil {
		srv.metrics.Store(metrics)
	}
	return srv
}

// Handler returns the HTTP handler for S3 requests.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.handleRequest)
}

// handleRequest routes S3 requests based on path and method.
func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// Parse bucket and key from path
	// Path format: /{bucket} or /{bucket}/{key...}
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 2)
	bucket := ""
	key := ""

	if len(parts) >= 1 && parts[0] != "" {
		bucket = parts[0]
	}
	if len(parts) >= 2 {
		key = parts[1]
	}

	log.Debug().
		Str("method", r.Method).
		Str("path", path).
		Str("bucket", bucket).
		Str("key", key).
		Msg("S3 request")

	// Route based on path and method
	switch {
	case bucket == "":
		// Service-level operations (list buckets)
		s.handleService(w, r)
	case key == "":
		// Bucket-level operations
		s.handleBucket(w, r, bucket)
	default:
		// Object-level operations
		s.handleObject(w, r, bucket, key)
	}
}

// handleService handles service-level operations (GET /).
func (s *Server) handleService(w http.ResponseWriter, r *http.Request) {
	s.withMetrics(w, "ListBuckets", func(rec http.ResponseWriter) {
		if r.Method != http.MethodGet {
			s.writeError(rec, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed")
			return
		}

		// Authorize: list buckets
		userID, err := s.authorizer.AuthorizeRequest(r, "list", "buckets", "", "")
		if err != nil {
			s.handleAuthError(rec, err)
			return
		}

		buckets, err := s.store.ListBuckets(r.Context())
		if err != nil {
			s.writeError(rec, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		// Build XML response
		resp := ListAllMyBucketsResult{
			Owner: Owner{
				ID:          userID,
				DisplayName: userID,
			},
		}

		for _, b := range buckets {
			resp.Buckets.Bucket = append(resp.Buckets.Bucket, BucketInfo{
				Name:         b.Name,
				CreationDate: b.CreatedAt.Format(time.RFC3339),
			})
		}

		s.writeXML(rec, http.StatusOK, resp)
	})
}

// handleBucket handles bucket-level operations.
func (s *Server) handleBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodGet:
		// Check for list-type query param (ListObjectsV2)
		if r.URL.Query().Get("list-type") == "2" {
			s.listObjectsV2(w, r, bucket)
			return
		}
		// Otherwise list objects (V1)
		s.listObjects(w, r, bucket)
	case http.MethodPut:
		s.createBucket(w, r, bucket)
	case http.MethodDelete:
		s.deleteBucket(w, r, bucket)
	case http.MethodHead:
		s.headBucket(w, r, bucket)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed")
	}
}

// handleObject handles object-level operations.
func (s *Server) handleObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	// Forward writes and deletes proactively to primary coordinator.
	// Reads try local first, then forward on miss (inside getObject/headObject).
	if (r.Method == http.MethodPut || r.Method == http.MethodDelete) && s.forwarder != nil {
		if s.forwarder.ForwardS3Request(w, r, bucket, key, "9000") {
			return
		}
	}

	switch r.Method {
	case http.MethodGet:
		s.getObject(w, r, bucket, key)
	case http.MethodPut:
		s.putObject(w, r, bucket, key)
	case http.MethodDelete:
		s.deleteObject(w, r, bucket, key)
	case http.MethodHead:
		s.headObject(w, r, bucket, key)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed")
	}
}

// createBucket handles PUT /{bucket}.
func (s *Server) createBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	s.withMetrics(w, "CreateBucket", func(rec http.ResponseWriter) {
		userID, err := s.authorizer.AuthorizeRequest(r, "create", "buckets", bucket, "")
		if err != nil {
			s.handleAuthError(rec, err)
			return
		}

		if err := s.store.CreateBucket(r.Context(), bucket, userID, 2, nil); err != nil {
			if errors.Is(err, ErrBucketExists) {
				s.writeError(rec, http.StatusConflict, "BucketAlreadyExists", "Bucket already exists")
				return
			}
			s.writeError(rec, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		rec.WriteHeader(http.StatusOK)
	})
}

// deleteBucket handles DELETE /{bucket}.
func (s *Server) deleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	s.withMetrics(w, "DeleteBucket", func(rec http.ResponseWriter) {
		_, err := s.authorizer.AuthorizeRequest(r, "delete", "buckets", bucket, "")
		if err != nil {
			s.handleAuthError(rec, err)
			return
		}

		if err := s.store.DeleteBucket(r.Context(), bucket); err != nil {
			switch {
			case errors.Is(err, ErrBucketNotFound):
				s.writeError(rec, http.StatusNotFound, "NoSuchBucket", "Bucket not found")
			case errors.Is(err, ErrBucketNotEmpty):
				s.writeError(rec, http.StatusConflict, "BucketNotEmpty", "Bucket is not empty")
			default:
				s.writeError(rec, http.StatusInternalServerError, "InternalError", err.Error())
			}
			return
		}

		rec.WriteHeader(http.StatusNoContent)
	})
}

// headBucket handles HEAD /{bucket}.
func (s *Server) headBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	s.withMetrics(w, "HeadBucket", func(rec http.ResponseWriter) {
		_, err := s.authorizer.AuthorizeRequest(r, "get", "buckets", bucket, "")
		if err != nil {
			s.handleAuthError(rec, err)
			return
		}

		if _, err := s.store.HeadBucket(r.Context(), bucket); err != nil {
			if errors.Is(err, ErrBucketNotFound) {
				rec.WriteHeader(http.StatusNotFound)
				return
			}
			s.writeError(rec, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		rec.WriteHeader(http.StatusOK)
	})
}

// listObjects handles GET /{bucket} (V1).
func (s *Server) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	s.withMetrics(w, "ListObjects", func(rec http.ResponseWriter) {
		userID, err := s.authorizer.AuthorizeRequest(r, "list", "objects", bucket, "")
		if err != nil {
			s.handleAuthError(rec, err)
			return
		}

		prefix := r.URL.Query().Get("prefix")
		marker := r.URL.Query().Get("marker")
		maxKeys := 1000 // default
		if mk := r.URL.Query().Get("max-keys"); mk != "" {
			if parsed, err := strconv.Atoi(mk); err == nil && parsed > 0 && parsed <= 1000 {
				maxKeys = parsed
			}
		}

		objects, isTruncated, nextMarker, err := s.store.ListObjects(r.Context(), bucket, prefix, marker, maxKeys)
		if err != nil {
			if errors.Is(err, ErrBucketNotFound) {
				s.writeError(rec, http.StatusNotFound, "NoSuchBucket", "Bucket not found")
				return
			}
			s.writeError(rec, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		// Filter objects by allowed prefixes
		allowedPrefixes := s.authorizer.GetAllowedPrefixes(userID, bucket)
		if allowedPrefixes != nil {
			objects = filterByPrefixes(objects, allowedPrefixes)
		}

		resp := ListBucketResult{
			Name:        bucket,
			Prefix:      prefix,
			Marker:      marker,
			MaxKeys:     maxKeys,
			IsTruncated: isTruncated,
			NextMarker:  nextMarker,
		}

		for _, obj := range objects {
			info := ObjectInfo{
				Key:          obj.Key,
				LastModified: obj.LastModified.Format(time.RFC3339),
				ETag:         obj.ETag,
				Size:         obj.Size,
			}
			if obj.Expires != nil {
				info.Expires = obj.Expires.Format(time.RFC3339)
			}
			resp.Contents = append(resp.Contents, info)
		}

		s.writeXML(rec, http.StatusOK, resp)
	})
}

// listObjectsV2 handles GET /{bucket}?list-type=2.
func (s *Server) listObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	s.withMetrics(w, "ListObjectsV2", func(rec http.ResponseWriter) {
		userID, err := s.authorizer.AuthorizeRequest(r, "list", "objects", bucket, "")
		if err != nil {
			s.handleAuthError(rec, err)
			return
		}

		prefix := r.URL.Query().Get("prefix")
		startAfter := r.URL.Query().Get("start-after")
		continuationToken := r.URL.Query().Get("continuation-token")
		maxKeys := 1000 // default
		if mk := r.URL.Query().Get("max-keys"); mk != "" {
			if parsed, err := strconv.Atoi(mk); err == nil && parsed > 0 && parsed <= 1000 {
				maxKeys = parsed
			}
		}

		// continuation-token takes precedence over start-after
		marker := startAfter
		if continuationToken != "" {
			marker = continuationToken
		}

		objects, isTruncated, nextMarker, err := s.store.ListObjects(r.Context(), bucket, prefix, marker, maxKeys)
		if err != nil {
			if errors.Is(err, ErrBucketNotFound) {
				s.writeError(rec, http.StatusNotFound, "NoSuchBucket", "Bucket not found")
				return
			}
			s.writeError(rec, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		// Filter objects by allowed prefixes
		allowedPrefixes := s.authorizer.GetAllowedPrefixes(userID, bucket)
		if allowedPrefixes != nil {
			objects = filterByPrefixes(objects, allowedPrefixes)
		}

		resp := ListBucketResultV2{
			Name:                  bucket,
			Prefix:                prefix,
			StartAfter:            startAfter,
			ContinuationToken:     continuationToken,
			MaxKeys:               maxKeys,
			KeyCount:              len(objects),
			IsTruncated:           isTruncated,
			NextContinuationToken: nextMarker,
		}

		for _, obj := range objects {
			info := ObjectInfo{
				Key:          obj.Key,
				LastModified: obj.LastModified.Format(time.RFC3339),
				ETag:         obj.ETag,
				Size:         obj.Size,
			}
			if obj.Expires != nil {
				info.Expires = obj.Expires.Format(time.RFC3339)
			}
			resp.Contents = append(resp.Contents, info)
		}

		s.writeXML(rec, http.StatusOK, resp)
	})
}

// getObject handles GET /{bucket}/{key}.
func (s *Server) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m := s.metrics.Load()
	startTime := time.Now()
	rec := &statusRecorder{ResponseWriter: w}
	var storeErr error // Capture error for metrics classification
	var forwarded bool
	defer func() {
		if m != nil && !forwarded {
			duration := time.Since(startTime).Seconds()
			status := ClassifyS3StatusWithError(rec.getStatus(), storeErr)
			m.RecordRequest("GetObject", status, duration)
		}
	}()

	_, err := s.authorizer.AuthorizeRequest(r, "get", "objects", bucket, key)
	if err != nil {
		s.handleAuthError(rec, err)
		return
	}

	reader, meta, err := s.store.GetObject(r.Context(), bucket, key)
	if err != nil {
		// Try forwarding to primary if not found locally
		if (errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrBucketNotFound)) && s.forwarder != nil {
			if s.forwarder.ForwardS3Request(w, r, bucket, key, "9000") {
				forwarded = true
				return
			}
		}
		storeErr = err // Capture for metrics
		switch {
		case errors.Is(err, ErrBucketNotFound):
			s.writeError(rec, http.StatusNotFound, "NoSuchBucket", "Bucket not found")
		case errors.Is(err, ErrObjectNotFound):
			s.writeError(rec, http.StatusNotFound, "NoSuchKey", "Object not found")
		default:
			s.writeError(rec, http.StatusInternalServerError, "InternalError", err.Error())
		}
		return
	}
	defer func() { _ = reader.Close() }()

	// Set response headers
	rec.Header().Set("Content-Type", meta.ContentType)
	rec.Header().Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	rec.Header().Set("ETag", meta.ETag)
	rec.Header().Set("Last-Modified", meta.LastModified.Format(http.TimeFormat))

	// Copy user metadata
	for k, v := range meta.Metadata {
		rec.Header().Set(k, v)
	}

	n, err := io.Copy(rec, reader)
	if err != nil {
		log.Error().Err(err).Msg("Failed to stream object")
	}
	if m != nil && n > 0 {
		m.RecordDownload(n)
	}
}

// putObject handles PUT /{bucket}/{key}.
func (s *Server) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m := s.metrics.Load()
	startTime := time.Now()
	rec := &statusRecorder{ResponseWriter: w}
	var storeErr error // Capture error for metrics classification
	defer func() {
		if m != nil {
			duration := time.Since(startTime).Seconds()
			status := ClassifyS3StatusWithError(rec.getStatus(), storeErr)
			m.RecordRequest("PutObject", status, duration)
		}
	}()

	_, err := s.authorizer.AuthorizeRequest(r, "put", "objects", bucket, key)
	if err != nil {
		s.handleAuthError(rec, err)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Extract user metadata from headers
	metadata := make(map[string]string)
	for k, v := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") && len(v) > 0 {
			metadata[k] = v[0]
		}
	}

	// Attempt to recover missing bucket for share (no-op if bucket exists)
	if s.recoverer != nil {
		_ = s.recoverer.EnsureBucketForShare(r.Context(), bucket)
	}

	meta, err := s.store.PutObject(r.Context(), bucket, key, r.Body, r.ContentLength, contentType, metadata)
	if err != nil {
		storeErr = err // Capture for metrics
		switch {
		case errors.Is(err, ErrBucketNotFound):
			s.writeError(rec, http.StatusNotFound, "NoSuchBucket", "Bucket not found")
		case errors.Is(err, ErrQuotaExceeded):
			s.writeError(rec, http.StatusForbidden, "QuotaExceeded", "Storage quota exceeded")
		default:
			s.writeError(rec, http.StatusInternalServerError, "InternalError", err.Error())
		}
		return
	}

	rec.Header().Set("ETag", meta.ETag)
	rec.WriteHeader(http.StatusOK)

	if m != nil && meta.Size > 0 {
		m.RecordUpload(meta.Size)
	}
}

// deleteObject handles DELETE /{bucket}/{key}.
func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	s.withMetrics(w, "DeleteObject", func(rec http.ResponseWriter) {
		_, err := s.authorizer.AuthorizeRequest(r, "delete", "objects", bucket, key)
		if err != nil {
			s.handleAuthError(rec, err)
			return
		}

		if err := s.store.DeleteObject(r.Context(), bucket, key); err != nil {
			switch {
			case errors.Is(err, ErrBucketNotFound):
				s.writeError(rec, http.StatusNotFound, "NoSuchBucket", "Bucket not found")
			case errors.Is(err, ErrObjectNotFound):
				rec.WriteHeader(http.StatusNoContent)
				return
			default:
				s.writeError(rec, http.StatusInternalServerError, "InternalError", err.Error())
			}
			return
		}

		if s.delNotifier != nil {
			s.delNotifier.NotifyObjectDeleted(bucket, key)
		}

		rec.WriteHeader(http.StatusNoContent)
	})
}

// headObject handles HEAD /{bucket}/{key}.
func (s *Server) headObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m := s.metrics.Load()
	startTime := time.Now()
	rec := &statusRecorder{ResponseWriter: w}
	var forwarded bool
	defer func() {
		if m != nil && !forwarded {
			duration := time.Since(startTime).Seconds()
			status := ClassifyS3Status(rec.getStatus())
			m.RecordRequest("HeadObject", status, duration)
		}
	}()

	_, err := s.authorizer.AuthorizeRequest(r, "get", "objects", bucket, key)
	if err != nil {
		s.handleAuthError(rec, err)
		return
	}

	meta, err := s.store.HeadObject(r.Context(), bucket, key)
	if err != nil {
		// Try forwarding to primary if not found locally
		if (errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrBucketNotFound)) && s.forwarder != nil {
			if s.forwarder.ForwardS3Request(w, r, bucket, key, "9000") {
				forwarded = true
				return
			}
		}
		switch {
		case errors.Is(err, ErrBucketNotFound):
			rec.WriteHeader(http.StatusNotFound)
		case errors.Is(err, ErrObjectNotFound):
			rec.WriteHeader(http.StatusNotFound)
		default:
			rec.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	rec.Header().Set("Content-Type", meta.ContentType)
	rec.Header().Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	rec.Header().Set("ETag", meta.ETag)
	rec.Header().Set("Last-Modified", meta.LastModified.Format(http.TimeFormat))

	for k, v := range meta.Metadata {
		rec.Header().Set(k, v)
	}

	rec.WriteHeader(http.StatusOK)
}

// handleAuthError writes the appropriate error response for auth errors.
func (s *Server) handleAuthError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrAccessDenied) {
		s.writeError(w, http.StatusForbidden, "AccessDenied", "Access denied")
		return
	}
	s.writeError(w, http.StatusUnauthorized, "InvalidAccessKeyId", "Authentication failed")
}

// writeError writes an S3-style XML error response.
func (s *Server) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)

	resp := ErrorResponse{
		Code:    code,
		Message: message,
	}

	if err := xml.NewEncoder(w).Encode(resp); err != nil {
		log.Error().Err(err).Msg("Failed to encode error response")
	}
}

// writeXML writes an XML response.
func (s *Server) writeXML(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)

	if err := xml.NewEncoder(w).Encode(v); err != nil {
		log.Error().Err(err).Msg("Failed to encode XML response")
	}
}

// filterByPrefixes filters objects to only include those matching any of the allowed prefixes.
// If prefixes is empty, no objects match (returns empty slice).
func filterByPrefixes(objects []ObjectMeta, prefixes []string) []ObjectMeta {
	if len(prefixes) == 0 {
		return []ObjectMeta{}
	}

	result := make([]ObjectMeta, 0, len(objects))
	for _, obj := range objects {
		for _, prefix := range prefixes {
			if strings.HasPrefix(obj.Key, prefix) {
				result = append(result, obj)
				break
			}
		}
	}
	return result
}

// XML response types

// ErrorResponse represents an S3 error.
type ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// ListAllMyBucketsResult is the response for listing buckets.
type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   Owner    `xml:"Owner"`
	Buckets struct {
		Bucket []BucketInfo `xml:"Bucket"`
	} `xml:"Buckets"`
}

// Owner represents a bucket/object owner.
type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// BucketInfo represents a bucket in a listing.
type BucketInfo struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// ListBucketResult is the response for listing objects (V1).
type ListBucketResult struct {
	XMLName     xml.Name     `xml:"ListBucketResult"`
	Name        string       `xml:"Name"`
	Prefix      string       `xml:"Prefix"`
	Marker      string       `xml:"Marker,omitempty"`
	MaxKeys     int          `xml:"MaxKeys"`
	IsTruncated bool         `xml:"IsTruncated"`
	NextMarker  string       `xml:"NextMarker,omitempty"`
	Contents    []ObjectInfo `xml:"Contents"`
}

// ListBucketResultV2 is the response for listing objects (V2).
type ListBucketResultV2 struct {
	XMLName               xml.Name     `xml:"ListBucketResult"`
	Name                  string       `xml:"Name"`
	Prefix                string       `xml:"Prefix"`
	StartAfter            string       `xml:"StartAfter,omitempty"`
	ContinuationToken     string       `xml:"ContinuationToken,omitempty"`
	MaxKeys               int          `xml:"MaxKeys"`
	KeyCount              int          `xml:"KeyCount"`
	IsTruncated           bool         `xml:"IsTruncated"`
	NextContinuationToken string       `xml:"NextContinuationToken,omitempty"`
	Contents              []ObjectInfo `xml:"Contents"`
}

// ObjectInfo represents an object in a listing.
type ObjectInfo struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	Expires      string `xml:"Expires,omitempty"` // Custom: object expiration date
}
