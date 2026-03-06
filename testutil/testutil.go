// Package testutil provides shared test utilities and mocks for tunnelmesh tests.
package testutil

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// TempDir creates a temporary directory for testing and returns a cleanup function.
func TempDir(t *testing.T) (string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "tunnelmesh-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	return dir, func() {
		_ = os.RemoveAll(dir)
	}
}

// TempFile creates a temporary file with the given content and returns its path.
func TempFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return path
}

// GenerateSSHKeyPair generates an ED25519 SSH key pair for testing.
// Returns the private key PEM bytes and the public key.
func GenerateSSHKeyPair(t *testing.T) ([]byte, ssh.PublicKey) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to create SSH public key: %v", err)
	}

	// Marshal private key to OpenSSH format
	pemBlock, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatalf("failed to marshal private key: %v", err)
	}

	// Encode to PEM format
	privBytes := pem.EncodeToMemory(pemBlock)

	return privBytes, sshPub
}

// WriteSSHKeyPair writes an SSH key pair to files in the given directory.
// Returns paths to the private and public key files.
func WriteSSHKeyPair(t *testing.T, dir string) (privPath, pubPath string) {
	t.Helper()

	privBytes, pubKey := GenerateSSHKeyPair(t)

	privPath = filepath.Join(dir, "id_ed25519")
	pubPath = filepath.Join(dir, "id_ed25519.pub")

	if err := os.WriteFile(privPath, privBytes, 0600); err != nil {
		t.Fatalf("failed to write private key: %v", err)
	}

	pubBytes := ssh.MarshalAuthorizedKey(pubKey)
	if err := os.WriteFile(pubPath, pubBytes, 0644); err != nil {
		t.Fatalf("failed to write public key: %v", err)
	}

	return privPath, pubPath
}

// FreePort returns an available TCP port on localhost.
func FreePort(t *testing.T) int {
	t.Helper()

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to resolve address: %v", err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	return l.Addr().(*net.TCPAddr).Port
}

// FreeUDPPort returns an available UDP port on 127.0.0.1.
// Use this instead of FreePort when the caller will bind a UDP socket — on
// Windows the TCP and UDP port spaces can diverge, so a port that is free for
// TCP may already be occupied for UDP.
func FreeUDPPort(t *testing.T) int {
	t.Helper()

	l, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("failed to find free UDP port: %v", err)
	}
	port := l.LocalAddr().(*net.UDPAddr).Port
	_ = l.Close()
	return port
}

// MockConn is a mock net.Conn for testing.
type MockConn struct {
	ReadData  []byte
	ReadErr   error
	WriteData []byte
	WriteErr  error
	Closed    bool
}

func (m *MockConn) Read(b []byte) (n int, err error) {
	if m.ReadErr != nil {
		return 0, m.ReadErr
	}
	if len(m.ReadData) == 0 {
		return 0, io.EOF
	}
	n = copy(b, m.ReadData)
	m.ReadData = m.ReadData[n:]
	return n, nil
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	if m.WriteErr != nil {
		return 0, m.WriteErr
	}
	m.WriteData = append(m.WriteData, b...)
	return len(b), nil
}

func (m *MockConn) Close() error {
	m.Closed = true
	return nil
}

func (m *MockConn) LocalAddr() net.Addr                { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0} }
func (m *MockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0} }
func (m *MockConn) SetDeadline(_ time.Time) error      { return nil }
func (m *MockConn) SetReadDeadline(_ time.Time) error  { return nil }
func (m *MockConn) SetWriteDeadline(_ time.Time) error { return nil }

// OpenFile opens a file for reading.
func OpenFile(path string) (io.ReadCloser, error) {
	return os.Open(path)
}
