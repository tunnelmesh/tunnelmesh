package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"golang.org/x/crypto/hkdf"
)

// CredentialStore manages S3 credential lookups.
type CredentialStore struct {
	// accessKeyToUser maps access key -> user ID
	accessKeyToUser map[string]string
	// userSecrets maps user ID -> secret key
	userSecrets map[string]string
	mu          sync.RWMutex
}

// NewCredentialStore creates a new credential store.
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{
		accessKeyToUser: make(map[string]string),
		userSecrets:     make(map[string]string),
	}
}

// RegisterUser registers a user's S3 credentials.
// The access key and secret key are derived from the user's public key.
func (cs *CredentialStore) RegisterUser(userID, publicKey string) (accessKey, secretKey string, err error) {
	accessKey, secretKey, err = DeriveS3Credentials(publicKey)
	if err != nil {
		return "", "", err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.accessKeyToUser[accessKey] = userID
	cs.userSecrets[userID] = secretKey

	return accessKey, secretKey, nil
}

// LookupUser returns the user ID for an access key.
func (cs *CredentialStore) LookupUser(accessKey string) (userID string, ok bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	userID, ok = cs.accessKeyToUser[accessKey]
	return
}

// UserCount returns the number of registered users.
func (cs *CredentialStore) UserCount() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.userSecrets)
}

// GetSecret returns the secret key for a user ID.
func (cs *CredentialStore) GetSecret(userID string) (secretKey string, ok bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	secretKey, ok = cs.userSecrets[userID]
	return
}

// DeriveS3Credentials derives S3 access and secret keys from a public key.
// Access key: 20 character hex string from HKDF(pubkey, "s3-access-key")
// Secret key: 40 character hex string from HKDF(pubkey, "s3-secret-key")
func DeriveS3Credentials(publicKey string) (accessKey, secretKey string, err error) {
	pubKeyBytes := []byte(publicKey)

	// Derive access key (20 chars = 10 bytes)
	accessReader := hkdf.New(sha256.New, pubKeyBytes, nil, []byte("s3-access-key"))
	accessBytes := make([]byte, 10)
	if _, err := accessReader.Read(accessBytes); err != nil {
		return "", "", err
	}
	accessKey = strings.ToUpper(hex.EncodeToString(accessBytes))

	// Derive secret key (40 chars = 20 bytes)
	secretReader := hkdf.New(sha256.New, pubKeyBytes, nil, []byte("s3-secret-key"))
	secretBytes := make([]byte, 20)
	if _, err := secretReader.Read(secretBytes); err != nil {
		return "", "", err
	}
	secretKey = hex.EncodeToString(secretBytes)

	return accessKey, secretKey, nil
}

// RBACAuthorizer implements the Authorizer interface using RBAC.
type RBACAuthorizer struct {
	credentials *CredentialStore
	authorizer  *auth.Authorizer
}

// NewRBACAuthorizer creates a new RBAC-based authorizer.
func NewRBACAuthorizer(credentials *CredentialStore, authorizer *auth.Authorizer) *RBACAuthorizer {
	return &RBACAuthorizer{
		credentials: credentials,
		authorizer:  authorizer,
	}
}

// AuthorizeRequest authenticates and authorizes an S3 request.
// It extracts credentials from the Authorization header and checks RBAC permissions.
func (a *RBACAuthorizer) AuthorizeRequest(r *http.Request, verb, resource, bucket, objectKey string) (userID string, err error) {
	var accessKey, secret string
	var isBasicAuth bool

	// Extract access key from Authorization header
	accessKey, secret = parseAuthHeader(r.Header.Get("Authorization"))
	if accessKey == "" {
		// Try Basic auth for simple clients
		accessKey, secret, isBasicAuth = parseBasicAuthFull(r)
	}

	if accessKey == "" {
		log.Info().Str("method", r.Method).Str("path", r.URL.Path).Msg("S3 access denied: no credentials")
		return "", ErrAccessDenied
	}

	// Lookup user by access key
	userID, ok := a.credentials.LookupUser(accessKey)
	if !ok {
		log.Info().Str("access_key", accessKey[:min(8, len(accessKey))]).Msg("S3 access denied: unknown access key")
		return "", ErrAccessDenied
	}

	// Get stored secret key
	secretKey, ok := a.credentials.GetSecret(userID)
	if !ok {
		log.Info().Str("user_id", userID).Msg("S3 access denied: no secret key")
		return "", ErrAccessDenied
	}

	// Verify credentials
	if secret != "" {
		if isBasicAuth {
			// For Basic auth, password is the secret key directly
			if !hmac.Equal([]byte(secretKey), []byte(secret)) {
				log.Info().Str("user_id", userID).Msg("S3 access denied: invalid password")
				return "", ErrAccessDenied
			}
		} else {
			// For AWS-style auth, verify signature
			if !verifySimpleSignature(accessKey, secretKey, secret) {
				log.Info().Str("user_id", userID).Msg("S3 access denied: invalid signature")
				return "", ErrAccessDenied
			}
		}
	}

	// Check RBAC permissions
	if !a.authorizer.Authorize(userID, verb, resource, bucket, objectKey) {
		log.Info().Str("user_id", userID).Str("verb", verb).Str("resource", resource).Str("bucket", bucket).Str("object", objectKey).Msg("S3 access denied: permission denied")
		return "", ErrAccessDenied
	}

	return userID, nil
}

// GetAllowedPrefixes returns the object prefixes a user can access in a bucket.
func (a *RBACAuthorizer) GetAllowedPrefixes(userID, bucket string) []string {
	return a.authorizer.GetAllowedPrefixes(userID, bucket)
}

// parseAuthHeader parses an AWS-style Authorization header.
// Supports:
// - "AWS4-HMAC-SHA256 Credential=ACCESS_KEY/..."
// - "AWS ACCESS_KEY:SIGNATURE"
// - "Bearer ACCESS_KEY"
func parseAuthHeader(header string) (accessKey, signature string) {
	if header == "" {
		return "", ""
	}

	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 {
		return "", ""
	}

	authType := strings.ToUpper(parts[0])

	switch authType {
	case "AWS4-HMAC-SHA256":
		// AWS Signature V4: Credential=ACCESS_KEY/date/region/s3/aws4_request
		for _, field := range strings.Split(parts[1], ", ") {
			if strings.HasPrefix(field, "Credential=") {
				cred := strings.TrimPrefix(field, "Credential=")
				// Access key is before the first /
				if idx := strings.Index(cred, "/"); idx > 0 {
					accessKey = cred[:idx]
				}
			}
			if strings.HasPrefix(field, "Signature=") {
				signature = strings.TrimPrefix(field, "Signature=")
			}
		}
		return accessKey, signature

	case "AWS":
		// Legacy AWS Signature V2: ACCESS_KEY:SIGNATURE
		credParts := strings.SplitN(parts[1], ":", 2)
		if len(credParts) == 2 {
			return credParts[0], credParts[1]
		}
		return credParts[0], ""

	case "BEARER":
		// Simple Bearer token: just the access key
		return parts[1], ""

	default:
		return "", ""
	}
}

// parseBasicAuthFull extracts credentials from Basic auth header with flag.
func parseBasicAuthFull(r *http.Request) (accessKey, secretKey string, isBasic bool) {
	username, password, ok := r.BasicAuth()
	if !ok {
		return "", "", false
	}
	return username, password, true
}

// verifySimpleSignature verifies a simple HMAC signature.
// This is a simplified verification - real AWS Sig V4 is more complex.
func verifySimpleSignature(accessKey, secretKey, signature string) bool {
	// For simplified auth, just verify the signature is a valid HMAC of the access key
	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write([]byte(accessKey))
	expected := hex.EncodeToString(mac.Sum(nil))

	// Compare (note: real impl should use constant-time compare)
	return hmac.Equal([]byte(expected), []byte(signature))
}
