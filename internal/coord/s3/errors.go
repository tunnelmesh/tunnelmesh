package s3

import "errors"

// S3 error types.
var (
	ErrBucketExists        = errors.New("bucket already exists")
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrObjectNotFound      = errors.New("object not found")
	ErrAccessDenied        = errors.New("access denied")
	ErrInvalidRequest      = errors.New("invalid request")
	ErrQuotaExceeded       = errors.New("storage quota exceeded")
	ErrBucketQuotaExceeded = errors.New("bucket quota exceeded")
)
