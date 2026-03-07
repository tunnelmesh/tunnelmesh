package coord

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCertificateAuthority_Generate(t *testing.T) {
	dir := t.TempDir()
	ca, err := NewCertificateAuthority(dir, "")
	require.NoError(t, err)
	require.NotNil(t, ca)

	assert.NotEmpty(t, ca.Name(), "CA should have a common name")
	assert.Contains(t, ca.Name(), "TunnelMesh", "CA name should contain TunnelMesh")
}

func TestNewCertificateAuthority_PersistAndLoad(t *testing.T) {
	dir := t.TempDir()

	// Generate and save
	ca1, err := NewCertificateAuthority(dir, "")
	require.NoError(t, err)
	name1 := ca1.Name()
	caPEM1 := ca1.CACertPEM()

	// Load from the same directory — should not regenerate
	ca2, err := NewCertificateAuthority(dir, "")
	require.NoError(t, err)
	require.Equal(t, name1, ca2.Name(), "reloaded CA should have the same name")

	caPEM2 := ca2.CACertPEM()
	assert.Equal(t, caPEM1, caPEM2, "reloaded CA cert PEM must match original")
}

func TestCACertPEM_ParseRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ca, err := NewCertificateAuthority(dir, "")
	require.NoError(t, err)

	pemBytes := ca.CACertPEM()
	block, rest := pem.Decode(pemBytes)
	require.NotNil(t, block, "PEM block must be present")
	assert.Equal(t, "CERTIFICATE", block.Type)
	assert.Empty(t, rest, "no trailing data expected after PEM block")

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	assert.True(t, cert.IsCA, "certificate should be a CA")
	assert.True(t, cert.BasicConstraintsValid)
}

func TestGeneratePeerCert_SANsAndSigning(t *testing.T) {
	dir := t.TempDir()
	ca, err := NewCertificateAuthority(dir, "")
	require.NoError(t, err)

	certPEM, keyPEM, err := ca.GeneratePeerCert("mynode", "", "100.64.0.1")
	require.NoError(t, err)
	require.NotEmpty(t, certPEM)
	require.NotEmpty(t, keyPEM)

	// Parse the certificate
	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	// Verify it is signed by the CA
	pool := x509.NewCertPool()
	pool.AddCert(ca.caCert)
	_, err = cert.Verify(x509.VerifyOptions{Roots: pool})
	require.NoError(t, err, "peer cert must verify against CA")

	// DNS names must include the peer name
	foundPeer := false
	for _, dns := range cert.DNSNames {
		if strings.HasPrefix(dns, "mynode") {
			foundPeer = true
			break
		}
	}
	assert.True(t, foundPeer, "peer cert should include peer name in SAN DNS names")

	// IP SAN must match meshIP
	foundIP := false
	for _, ip := range cert.IPAddresses {
		if ip.Equal(net.ParseIP("100.64.0.1")) {
			foundIP = true
			break
		}
	}
	assert.True(t, foundIP, "peer cert should include meshIP in SAN IP addresses")

	// Key PEM must be parseable as an EC key
	keyBlock, _ := pem.Decode(keyPEM)
	require.NotNil(t, keyBlock)
	assert.Equal(t, "EC PRIVATE KEY", keyBlock.Type)
	_, err = x509.ParseECPrivateKey(keyBlock.Bytes)
	require.NoError(t, err)
}

func TestGetClientTLSConfig(t *testing.T) {
	dir := t.TempDir()
	ca, err := NewCertificateAuthority(dir, "")
	require.NoError(t, err)

	cfg := ca.GetClientTLSConfig()
	require.NotNil(t, cfg)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	assert.False(t, cfg.InsecureSkipVerify)
	require.NotNil(t, cfg.RootCAs, "RootCAs should be set")
}

func TestGeneratePeerCert_EmptyMeshIP(t *testing.T) {
	dir := t.TempDir()
	ca, err := NewCertificateAuthority(dir, "")
	require.NoError(t, err)

	// Should succeed even with empty mesh IP (no IP SAN added)
	certPEM, _, err := ca.GeneratePeerCert("nodeB", "", "")
	require.NoError(t, err)

	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	assert.Empty(t, cert.IPAddresses, "no IP SAN expected for empty meshIP")
}
