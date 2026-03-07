package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
)

// AllowAllAuthorizer is a test-only authorizer that allows all requests.
// Never use in production handlers.
type AllowAllAuthorizer struct {
	UserID string
}

func (a *AllowAllAuthorizer) AuthorizeRequest(r *http.Request, verb, resource, bucket, objectKey string) (string, error) {
	return a.UserID, nil
}

func (a *AllowAllAuthorizer) GetAllowedPrefixes(userID, bucket string) []string {
	return nil
}

func TestDeriveS3Credentials(t *testing.T) {
	publicKey := "test-public-key-12345"

	accessKey, secretKey, err := DeriveS3Credentials(publicKey)
	require.NoError(t, err)

	// Access key should be 20 chars uppercase hex
	assert.Len(t, accessKey, 20)
	for _, c := range accessKey {
		assert.True(t, (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F'))
	}

	// Secret key should be 40 chars hex
	assert.Len(t, secretKey, 40)
	for _, c := range secretKey {
		assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
	}
}

func TestDeriveS3CredentialsDeterministic(t *testing.T) {
	publicKey := "same-public-key"

	ak1, sk1, err := DeriveS3Credentials(publicKey)
	require.NoError(t, err)

	ak2, sk2, err := DeriveS3Credentials(publicKey)
	require.NoError(t, err)

	assert.Equal(t, ak1, ak2, "access keys should be deterministic")
	assert.Equal(t, sk1, sk2, "secret keys should be deterministic")
}

func TestDeriveS3CredentialsDifferentKeys(t *testing.T) {
	ak1, sk1, err := DeriveS3Credentials("public-key-1")
	require.NoError(t, err)

	ak2, sk2, err := DeriveS3Credentials("public-key-2")
	require.NoError(t, err)

	assert.NotEqual(t, ak1, ak2, "different public keys should have different access keys")
	assert.NotEqual(t, sk1, sk2, "different public keys should have different secret keys")
}

func TestCredentialStoreRegisterUser(t *testing.T) {
	cs := NewCredentialStore()

	accessKey, secretKey, err := cs.RegisterUser("alice", "alice-public-key")
	require.NoError(t, err)
	assert.NotEmpty(t, accessKey)
	assert.NotEmpty(t, secretKey)

	// Should be able to lookup
	userID, ok := cs.LookupUser(accessKey)
	assert.True(t, ok)
	assert.Equal(t, "alice", userID)

	// Should get secret
	secret, ok := cs.GetSecret("alice")
	assert.True(t, ok)
	assert.Equal(t, secretKey, secret)
}

func TestCredentialStoreLookupNotFound(t *testing.T) {
	cs := NewCredentialStore()

	_, ok := cs.LookupUser("nonexistent")
	assert.False(t, ok)
}

func TestCredentialStoreGetSecretNotFound(t *testing.T) {
	cs := NewCredentialStore()

	_, ok := cs.GetSecret("nonexistent")
	assert.False(t, ok)
}

func TestParseAuthHeaderAWS4(t *testing.T) {
	header := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123"

	accessKey, signature := parseAuthHeader(header)
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", accessKey)
	assert.Equal(t, "abc123", signature)
}

func TestParseAuthHeaderAWSLegacy(t *testing.T) {
	header := "AWS AKIAIOSFODNN7EXAMPLE:signature123"

	accessKey, signature := parseAuthHeader(header)
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", accessKey)
	assert.Equal(t, "signature123", signature)
}

func TestParseAuthHeaderBearer(t *testing.T) {
	header := "Bearer myaccesskey"

	accessKey, signature := parseAuthHeader(header)
	assert.Equal(t, "myaccesskey", accessKey)
	assert.Empty(t, signature)
}

func TestParseAuthHeaderEmpty(t *testing.T) {
	accessKey, signature := parseAuthHeader("")
	assert.Empty(t, accessKey)
	assert.Empty(t, signature)
}

func TestParseAuthHeaderInvalid(t *testing.T) {
	accessKey, signature := parseAuthHeader("invalid")
	assert.Empty(t, accessKey)
	assert.Empty(t, signature)
}

func TestRBACAuthorizerAuthorize(t *testing.T) {
	cs := NewCredentialStore()
	authz := auth.NewAuthorizer()

	// Register user and add binding
	accessKey, _, err := cs.RegisterUser("alice", "alice-public-key")
	require.NoError(t, err)
	authz.Bindings.Add(auth.NewRoleBinding("alice", auth.RoleAdmin, ""))

	rbac := NewRBACAuthorizer(cs, authz)

	// Create request with Bearer auth
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+accessKey)

	userID, err := rbac.AuthorizeRequest(req, "get", "buckets", "", "")
	require.NoError(t, err)
	assert.Equal(t, "alice", userID)
}

func TestRBACAuthorizerBasicAuth(t *testing.T) {
	cs := NewCredentialStore()
	authz := auth.NewAuthorizer()

	accessKey, secretKey, err := cs.RegisterUser("bob", "bob-public-key")
	require.NoError(t, err)
	authz.Bindings.Add(auth.NewRoleBinding("bob", auth.RoleBucketRead, ""))

	rbac := NewRBACAuthorizer(cs, authz)

	req := httptest.NewRequest(http.MethodGet, "/my-bucket", nil)
	req.SetBasicAuth(accessKey, secretKey)

	userID, err := rbac.AuthorizeRequest(req, "get", "buckets", "my-bucket", "")
	require.NoError(t, err)
	assert.Equal(t, "bob", userID)
}

func TestRBACAuthorizerDeniedNoAuth(t *testing.T) {
	cs := NewCredentialStore()
	authz := auth.NewAuthorizer()
	rbac := NewRBACAuthorizer(cs, authz)

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	_, err := rbac.AuthorizeRequest(req, "get", "buckets", "", "")
	assert.ErrorIs(t, err, ErrAccessDenied)
}

func TestRBACAuthorizerDeniedUnknownUser(t *testing.T) {
	cs := NewCredentialStore()
	authz := auth.NewAuthorizer()
	rbac := NewRBACAuthorizer(cs, authz)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer unknownaccesskey")

	_, err := rbac.AuthorizeRequest(req, "get", "buckets", "", "")
	assert.ErrorIs(t, err, ErrAccessDenied)
}

func TestRBACAuthorizerDeniedNoPermission(t *testing.T) {
	cs := NewCredentialStore()
	authz := auth.NewAuthorizer()

	// Register user but don't add any bindings
	accessKey, _, err := cs.RegisterUser("charlie", "charlie-public-key")
	require.NoError(t, err)

	rbac := NewRBACAuthorizer(cs, authz)

	req := httptest.NewRequest(http.MethodDelete, "/my-bucket", nil)
	req.Header.Set("Authorization", "Bearer "+accessKey)

	_, err = rbac.AuthorizeRequest(req, "delete", "buckets", "my-bucket", "")
	assert.ErrorIs(t, err, ErrAccessDenied)
}

func TestRBACAuthorizerScopedPermission(t *testing.T) {
	cs := NewCredentialStore()
	authz := auth.NewAuthorizer()

	accessKey, _, err := cs.RegisterUser("dave", "dave-public-key")
	require.NoError(t, err)
	// Dave can only write to "my-bucket"
	authz.Bindings.Add(auth.NewRoleBinding("dave", auth.RoleBucketWrite, "my-bucket"))

	rbac := NewRBACAuthorizer(cs, authz)

	// Create request
	req := httptest.NewRequest(http.MethodPut, "/", nil)
	req.Header.Set("Authorization", "Bearer "+accessKey)

	// Can write to my-bucket
	_, err = rbac.AuthorizeRequest(req, "put", "objects", "my-bucket", "")
	require.NoError(t, err)

	// Cannot write to other-bucket
	_, err = rbac.AuthorizeRequest(req, "put", "objects", "other-bucket", "")
	assert.ErrorIs(t, err, ErrAccessDenied)
}

func TestAllowAllAuthorizer(t *testing.T) {
	authz := &AllowAllAuthorizer{UserID: "test-user"}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	userID, err := authz.AuthorizeRequest(req, "get", "buckets", "", "")

	require.NoError(t, err)
	assert.Equal(t, "test-user", userID)
}

func TestVerifySimpleSignature(t *testing.T) {
	accessKey := "MYACCESSKEY"
	secretKey := "mysecretkey"

	// Generate a valid signature
	// This is what a client would compute
	assert.True(t, verifySimpleSignature(accessKey, secretKey, generateSimpleSignature(accessKey, secretKey)))

	// Invalid signature
	assert.False(t, verifySimpleSignature(accessKey, secretKey, "invalidsignature"))
}

// generateSimpleSignature creates a simple HMAC signature for testing.
func generateSimpleSignature(accessKey, secretKey string) string {
	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write([]byte(accessKey))
	return hex.EncodeToString(mac.Sum(nil))
}
