package control

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
)

func TestServer_StartStop(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	filter := routing.NewPacketFilter(true)
	server := NewServer(socketPath, filter, "test-peer")

	err := server.Start()
	require.NoError(t, err)

	// Check socket exists
	_, err = os.Stat(socketPath)
	require.NoError(t, err)

	// Stop
	err = server.Stop()
	require.NoError(t, err)

	// Check socket removed
	_, err = os.Stat(socketPath)
	assert.True(t, os.IsNotExist(err))
}

func TestClient_FilterList(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	filter := routing.NewPacketFilter(true)
	filter.SetPeerConfigRules([]routing.FilterRule{
		{Port: 22, Protocol: routing.ProtoTCP, Action: routing.ActionAllow},
		{Port: 80, Protocol: routing.ProtoTCP, Action: routing.ActionAllow},
	})

	server := NewServer(socketPath, filter, "test-peer")
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	// Wait for socket to be ready
	time.Sleep(10 * time.Millisecond)

	client := NewClient(socketPath)
	resp, err := client.FilterList()
	require.NoError(t, err)

	assert.True(t, resp.DefaultDeny)
	assert.Len(t, resp.Rules, 2)
}

func TestClient_FilterAdd(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	filter := routing.NewPacketFilter(true)

	server := NewServer(socketPath, filter, "test-peer")
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	time.Sleep(10 * time.Millisecond)

	client := NewClient(socketPath)

	// Add a rule
	err := client.FilterAdd(8080, "tcp", "allow", 0)
	require.NoError(t, err)

	// Verify rule was added
	resp, err := client.FilterList()
	require.NoError(t, err)
	assert.Len(t, resp.Rules, 1)
	assert.Equal(t, uint16(8080), resp.Rules[0].Port)
	assert.Equal(t, "tcp", resp.Rules[0].Protocol)
	assert.Equal(t, "allow", resp.Rules[0].Action)
	assert.Equal(t, "temporary", resp.Rules[0].Source)
}

func TestClient_FilterAddWithTTL(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	filter := routing.NewPacketFilter(true)

	server := NewServer(socketPath, filter, "test-peer")
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	time.Sleep(10 * time.Millisecond)

	client := NewClient(socketPath)

	// Add a rule with TTL
	err := client.FilterAdd(3000, "tcp", "allow", 3600)
	require.NoError(t, err)

	// Verify rule has expiry
	resp, err := client.FilterList()
	require.NoError(t, err)
	assert.Len(t, resp.Rules, 1)
	assert.Greater(t, resp.Rules[0].Expires, int64(0))
}

func TestClient_FilterRemove(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	filter := routing.NewPacketFilter(true)
	filter.AddTemporaryRule(routing.FilterRule{
		Port: 8080, Protocol: routing.ProtoTCP, Action: routing.ActionAllow,
	})

	server := NewServer(socketPath, filter, "test-peer")
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	time.Sleep(10 * time.Millisecond)

	client := NewClient(socketPath)

	// Verify rule exists
	resp, err := client.FilterList()
	require.NoError(t, err)
	assert.Len(t, resp.Rules, 1)

	// Remove rule
	err = client.FilterRemove(8080, "tcp")
	require.NoError(t, err)

	// Verify rule removed
	resp, err = client.FilterList()
	require.NoError(t, err)
	assert.Len(t, resp.Rules, 0)
}

func TestClient_InvalidProtocol(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	filter := routing.NewPacketFilter(true)

	server := NewServer(socketPath, filter, "test-peer")
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	time.Sleep(10 * time.Millisecond)

	client := NewClient(socketPath)

	err := client.FilterAdd(8080, "icmp", "allow", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid protocol")
	assert.Contains(t, err.Error(), "must be one of: tcp, udp")
}

func TestClient_MissingPort(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	filter := routing.NewPacketFilter(true)

	server := NewServer(socketPath, filter, "test-peer")
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	time.Sleep(10 * time.Millisecond)

	client := NewClient(socketPath)

	err := client.FilterAdd(0, "tcp", "allow", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "port is required")
	assert.Contains(t, err.Error(), "1 and 65535")
}

func TestClient_ConnectionRefused(t *testing.T) {
	client := NewClient("/nonexistent/socket.sock")

	_, err := client.FilterList()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connect to control socket")
}

func TestDefaultSocketPath(t *testing.T) {
	path := DefaultSocketPath()
	assert.Equal(t, "/var/run/tunnelmesh.sock", path)
}

func TestFilterChangedCallback(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	filter := routing.NewPacketFilter(true)
	server := NewServer(socketPath, filter, "test-peer")

	// Track callback invocations
	callbackCount := 0
	server.SetFilterChangedHandler(func() {
		callbackCount++
	})

	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	time.Sleep(10 * time.Millisecond)

	client := NewClient(socketPath)

	// Add a rule - should trigger callback
	err := client.FilterAdd(8080, "tcp", "allow", 0)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	assert.Greater(t, callbackCount, 0, "callback should have been invoked after FilterAdd")

	initialCount := callbackCount

	// Remove a rule - should trigger callback again
	err = client.FilterRemove(8080, "tcp")
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	assert.Greater(t, callbackCount, initialCount, "callback should have been invoked after FilterRemove")
}
