package tunnel

import (
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/testutil"
	"golang.org/x/crypto/ssh"
)

func TestSSHServer_Accept(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Generate server keys
	serverPrivPath, _ := testutil.WriteSSHKeyPair(t, dir)
	serverKey, err := ssh.ParsePrivateKey(mustReadFile(t, serverPrivPath))
	require.NoError(t, err)

	// Generate client keys
	clientPrivBytes, clientPub := testutil.GenerateSSHKeyPair(t)
	clientKey, err := ssh.ParsePrivateKey(clientPrivBytes)
	require.NoError(t, err)

	// Create authorized keys callback
	authorizedKeys := []ssh.PublicKey{clientPub}

	// Start server
	port := testutil.FreePort(t)
	addr := "127.0.0.1:" + strconv.Itoa(port)

	srv := NewSSHServer(serverKey, authorizedKeys)
	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	// Accept connections in background
	connChan := make(chan *SSHConnection, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		sshConn, err := srv.Accept(conn)
		if err == nil {
			connChan <- sshConn
		}
	}()

	// Connect as client
	clientConfig := &ssh.ClientConfig{
		User: "tunnelmesh",
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(clientKey),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}

	clientConn, err := ssh.Dial("tcp", addr, clientConfig)
	require.NoError(t, err)
	defer func() { _ = clientConn.Close() }()

	// Wait for server to accept
	select {
	case <-connChan:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for connection")
	}
}

func TestSSHClient_Connect(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Generate server keys
	serverPrivPath, serverPubPath := testutil.WriteSSHKeyPair(t, dir)
	serverKey, err := ssh.ParsePrivateKey(mustReadFile(t, serverPrivPath))
	require.NoError(t, err)
	serverPubData := mustReadFile(t, serverPubPath)
	serverPub, _, _, _, err := ssh.ParseAuthorizedKey(serverPubData)
	require.NoError(t, err)

	// Generate client keys
	clientPrivBytes, clientPub := testutil.GenerateSSHKeyPair(t)
	clientKey, err := ssh.ParsePrivateKey(clientPrivBytes)
	require.NoError(t, err)

	// Start server
	port := testutil.FreePort(t)
	addr := "127.0.0.1:" + strconv.Itoa(port)

	srv := NewSSHServer(serverKey, []ssh.PublicKey{clientPub})
	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			_, _ = srv.Accept(conn)
		}
	}()

	// Connect as client
	client := NewSSHClient(clientKey, serverPub)
	sshClient, err := client.Connect(addr)
	require.NoError(t, err)
	defer func() { _ = sshClient.Close() }()

	assert.NotNil(t, sshClient)
}

func TestDataChannel(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Setup keys
	serverPrivPath, _ := testutil.WriteSSHKeyPair(t, dir)
	serverKey, _ := ssh.ParsePrivateKey(mustReadFile(t, serverPrivPath))
	clientPrivBytes, clientPub := testutil.GenerateSSHKeyPair(t)
	clientKey, _ := ssh.ParsePrivateKey(clientPrivBytes)

	// Start server
	port := testutil.FreePort(t)
	addr := "127.0.0.1:" + strconv.Itoa(port)

	srv := NewSSHServer(serverKey, []ssh.PublicKey{clientPub})
	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	serverDataChan := make(chan ssh.Channel, 1)
	go func() {
		conn, _ := listener.Accept()
		if conn == nil {
			return
		}
		sshConn, err := srv.Accept(conn)
		if err != nil {
			return
		}

		// Accept channels from Channels
		for newChan := range sshConn.Channels {
			if newChan.ChannelType() != "tunnelmesh-data" {
				_ = newChan.Reject(ssh.UnknownChannelType, "unknown channel type")
				continue
			}
			ch, _, _ := newChan.Accept()
			serverDataChan <- ch
			break
		}
	}()

	// Connect as client
	config := &ssh.ClientConfig{
		User:            "tunnelmesh",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(clientKey)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}

	sshClient, err := ssh.Dial("tcp", addr, config)
	require.NoError(t, err)
	defer func() { _ = sshClient.Close() }()

	// Open data channel
	clientChan, _, err := sshClient.OpenChannel("tunnelmesh-data", nil)
	require.NoError(t, err)

	// Wait for server to receive channel
	var serverChan ssh.Channel
	select {
	case serverChan = <-serverDataChan:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for server channel")
	}

	// Test bidirectional data transfer
	testData := []byte("hello from client")
	go func() {
		_, _ = clientChan.Write(testData)
	}()

	buf := make([]byte, 100)
	n, err := serverChan.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, testData, buf[:n])

	// Test reverse direction
	serverData := []byte("hello from server")
	go func() {
		_, _ = serverChan.Write(serverData)
	}()

	n, err = clientChan.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, serverData, buf[:n])
}

// mockTransportConn is a minimal ReadWriteCloserWithName for testing ConnectionAdapter.
type mockTransportConn struct {
	healthy bool
	closed  bool
}

func (m *mockTransportConn) Read(p []byte) (int, error)  { return 0, nil }
func (m *mockTransportConn) Write(p []byte) (int, error) { return len(p), nil }
func (m *mockTransportConn) Close() error                { m.closed = true; return nil }
func (m *mockTransportConn) PeerName() string            { return "mock-peer" }
func (m *mockTransportConn) IsHealthy() bool             { return m.healthy }

// mockNoHealthConn doesn't implement HealthChecker.
type mockNoHealthConn struct{}

func (m *mockNoHealthConn) Read(p []byte) (int, error)  { return 0, nil }
func (m *mockNoHealthConn) Write(p []byte) (int, error) { return len(p), nil }
func (m *mockNoHealthConn) Close() error                { return nil }
func (m *mockNoHealthConn) PeerName() string            { return "no-health-peer" }

func TestConnectionAdapterIsHealthy_DelegatesToUnderlying(t *testing.T) {
	mock := &mockTransportConn{healthy: true}
	adapter := NewConnectionAdapter(mock, "test-peer")

	assert.True(t, adapter.IsHealthy(), "should be healthy when underlying is healthy")

	mock.healthy = false
	assert.False(t, adapter.IsHealthy(), "should be unhealthy when underlying is unhealthy")
}

func TestConnectionAdapterIsHealthy_AssumeHealthyWithNoChecker(t *testing.T) {
	mock := &mockNoHealthConn{}
	adapter := NewConnectionAdapter(mock, "test-peer")

	assert.True(t, adapter.IsHealthy(), "should be assumed healthy when no health checker")
}

func TestConnectionAdapterIsHealthy_FalseWhenClosed(t *testing.T) {
	mock := &mockTransportConn{healthy: true}
	adapter := NewConnectionAdapter(mock, "test-peer")

	require.NoError(t, adapter.Close())
	assert.False(t, adapter.IsHealthy(), "should be unhealthy after close")
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
