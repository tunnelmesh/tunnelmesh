package benchmark

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/testutil"
)

func TestServer_StartStop(t *testing.T) {
	port := testutil.FreePort(t)
	addr := "127.0.0.1"

	server := NewServer(addr, port)
	require.NotNil(t, server)

	// Start
	err := server.Start()
	require.NoError(t, err)

	// Verify listening
	conn, err := net.DialTimeout("tcp", server.Addr(), time.Second)
	require.NoError(t, err)
	_ = conn.Close()

	// Stop
	err = server.Stop()
	assert.NoError(t, err)

	// Verify stopped
	_, err = net.DialTimeout("tcp", server.Addr(), 100*time.Millisecond)
	assert.Error(t, err)
}

func TestServer_HandleBenchmark_Upload(t *testing.T) {
	port := testutil.FreePort(t)
	server := NewServer("127.0.0.1", port)
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	// Connect as client
	conn, err := net.DialTimeout("tcp", server.Addr(), time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Send start message
	start := StartMessage{
		Size:      1024,
		Direction: DirectionUpload,
	}
	err = WriteMessage(conn, MsgStart, &start)
	require.NoError(t, err)

	// Read ack
	mt, data, err := ReadMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, MsgAck, mt)

	var ack AckMessage
	err = ack.Decode(bytesReader(data))
	require.NoError(t, err)
	assert.True(t, ack.Accepted)

	// Send data
	chunk := make([]byte, 1024)
	dataMsg := DataMessage{SeqNum: 0, Data: chunk}
	err = WriteMessage(conn, MsgData, &dataMsg)
	require.NoError(t, err)

	// Send complete
	complete := CompleteMessage{
		BytesTransferred: 1024,
		DurationNs:       int64(100 * time.Millisecond),
	}
	err = WriteMessage(conn, MsgComplete, &complete)
	require.NoError(t, err)

	// Read complete from server
	mt, data, err = ReadMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, MsgComplete, mt)

	var serverComplete CompleteMessage
	err = serverComplete.Decode(bytesReader(data))
	require.NoError(t, err)
	assert.Equal(t, int64(1024), serverComplete.BytesTransferred)
}

func TestServer_HandleBenchmark_Download(t *testing.T) {
	port := testutil.FreePort(t)
	server := NewServer("127.0.0.1", port)
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	// Connect as client
	conn, err := net.DialTimeout("tcp", server.Addr(), time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Send start message for download
	start := StartMessage{
		Size:      ChunkSize, // 64KB
		Direction: DirectionDownload,
	}
	err = WriteMessage(conn, MsgStart, &start)
	require.NoError(t, err)

	// Read ack
	mt, data, err := ReadMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, MsgAck, mt)

	var ack AckMessage
	err = ack.Decode(bytesReader(data))
	require.NoError(t, err)
	assert.True(t, ack.Accepted)

	// Read data from server
	var totalReceived int64
	for totalReceived < start.Size {
		mt, data, err = ReadMessage(conn)
		require.NoError(t, err)

		if mt == MsgData {
			var dataMsg DataMessage
			err = dataMsg.Decode(bytesReader(data))
			require.NoError(t, err)
			totalReceived += int64(len(dataMsg.Data))
		} else if mt == MsgComplete {
			break
		}
	}

	// Should have received data
	assert.GreaterOrEqual(t, totalReceived, start.Size)
}

func TestServer_PingPong(t *testing.T) {
	port := testutil.FreePort(t)
	server := NewServer("127.0.0.1", port)
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	// Connect
	conn, err := net.DialTimeout("tcp", server.Addr(), time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Start a benchmark first
	start := StartMessage{Size: 1024, Direction: DirectionUpload}
	_ = WriteMessage(conn, MsgStart, &start)
	_, _, _ = ReadMessage(conn) // Read ack

	// Send ping
	now := time.Now().UnixNano()
	ping := PingMessage{SeqNum: 1, Timestamp: now}
	err = WriteMessage(conn, MsgPing, &ping)
	require.NoError(t, err)

	// Read pong
	mt, data, err := ReadMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, MsgPong, mt)

	var pong PongMessage
	err = pong.Decode(bytesReader(data))
	require.NoError(t, err)
	assert.Equal(t, uint32(1), pong.SeqNum)
	assert.Equal(t, now, pong.PingTimestamp)
}

func TestServer_ConcurrentConnections(t *testing.T) {
	port := testutil.FreePort(t)
	server := NewServer("127.0.0.1", port)
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	// Multiple concurrent connections
	numConns := 5
	done := make(chan error, numConns)

	for i := 0; i < numConns; i++ {
		go func(id int) {
			conn, err := net.DialTimeout("tcp", server.Addr(), time.Second)
			if err != nil {
				done <- err
				return
			}
			defer func() { _ = conn.Close() }()

			// Send start
			start := StartMessage{Size: 512, Direction: DirectionUpload}
			if err := WriteMessage(conn, MsgStart, &start); err != nil {
				done <- err
				return
			}

			// Read ack
			mt, _, err := ReadMessage(conn)
			if err != nil {
				done <- err
				return
			}
			if mt != MsgAck {
				done <- io.ErrUnexpectedEOF
				return
			}

			done <- nil
		}(i)
	}

	// Wait for all to complete
	for i := 0; i < numConns; i++ {
		err := <-done
		assert.NoError(t, err)
	}
}

func TestServer_ContextCancellation(t *testing.T) {
	port := testutil.FreePort(t)
	server := NewServer("127.0.0.1", port)
	require.NoError(t, server.Start())

	// Connect and send a start message, then complete the benchmark
	conn, err := net.DialTimeout("tcp", server.Addr(), time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Send start
	start := StartMessage{Size: 1024, Direction: DirectionUpload}
	_ = WriteMessage(conn, MsgStart, &start)
	_, _, _ = ReadMessage(conn) // Read ack

	// Send data and complete to finish cleanly
	dataMsg := DataMessage{SeqNum: 0, Data: make([]byte, 1024)}
	_ = WriteMessage(conn, MsgData, &dataMsg)

	complete := CompleteMessage{BytesTransferred: 1024, DurationNs: 1000000}
	_ = WriteMessage(conn, MsgComplete, &complete)
	_, _, _ = ReadMessage(conn) // Read server's complete

	// Stop server - should complete quickly now
	err = server.Stop()
	assert.NoError(t, err)
}

func TestServer_Addr(t *testing.T) {
	server := NewServer("10.99.0.1", 9443)
	assert.Equal(t, "10.99.0.1:9443", server.Addr())
}

// Helper to create a bytes reader
func bytesReader(data []byte) io.Reader {
	return &bytesReaderWrapper{data: data}
}

type bytesReaderWrapper struct {
	data []byte
	pos  int
}

func (r *bytesReaderWrapper) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// Benchmark server connection handling
func BenchmarkServer_Connection(b *testing.B) {
	port := 19998 // Fixed port for benchmark
	server := NewServer("127.0.0.1", port)
	if err := server.Start(); err != nil {
		b.Fatalf("failed to start server: %v", err)
	}
	defer func() { _ = server.Stop() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := net.Dial("tcp", server.Addr())
		if err != nil {
			b.Fatalf("dial failed: %v", err)
		}
		_ = conn.Close()
	}
}

// Test that the server handles the benchmark lifecycle correctly
func TestServer_FullBenchmarkCycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	port := testutil.FreePort(t)
	server := NewServer("127.0.0.1", port)
	require.NoError(t, server.Start())
	defer func() { _ = server.Stop() }()

	// Connect
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", server.Addr())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// 1. Start benchmark
	start := StartMessage{Size: 128, Direction: DirectionUpload}
	require.NoError(t, WriteMessage(conn, MsgStart, &start))

	// 2. Read ack
	mt, data, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, MsgAck, mt)
	var ack AckMessage
	require.NoError(t, ack.Decode(bytesReader(data)))
	require.True(t, ack.Accepted)

	// 3. Send ping
	ping := PingMessage{SeqNum: 1, Timestamp: time.Now().UnixNano()}
	require.NoError(t, WriteMessage(conn, MsgPing, &ping))

	// 4. Read pong
	mt, _, err = ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, MsgPong, mt)

	// 5. Send data
	dataMsg := DataMessage{SeqNum: 0, Data: make([]byte, 128)}
	require.NoError(t, WriteMessage(conn, MsgData, &dataMsg))

	// 6. Send complete
	complete := CompleteMessage{BytesTransferred: 128, DurationNs: int64(time.Millisecond)}
	require.NoError(t, WriteMessage(conn, MsgComplete, &complete))

	// 7. Read server's complete
	mt, _, err = ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, MsgComplete, mt)
}

// TestHTTPHandler_UploadCycle verifies that NewHTTPHandler correctly performs
// the HTTP upgrade and then runs a complete upload benchmark cycle.
func TestHTTPHandler_UploadCycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ts := httptest.NewTLSServer(NewHTTPHandler())
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)

	conn, err := dialBenchmarkHTTP(ctx, "127.0.0.1:"+strconv.Itoa(port))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// 1. Send start
	start := StartMessage{Size: 256, Direction: DirectionUpload}
	require.NoError(t, WriteMessage(conn, MsgStart, &start))

	// 2. Read ack
	mt, data, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, MsgAck, mt)
	var ack AckMessage
	require.NoError(t, ack.Decode(bytesReader(data)))
	require.True(t, ack.Accepted)

	// 3. Send data
	dataMsg := DataMessage{SeqNum: 0, Data: make([]byte, 256)}
	require.NoError(t, WriteMessage(conn, MsgData, &dataMsg))

	// 4. Send complete
	complete := CompleteMessage{BytesTransferred: 256, DurationNs: int64(time.Millisecond)}
	require.NoError(t, WriteMessage(conn, MsgComplete, &complete))

	// 5. Read server's complete
	mt, _, err = ReadMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, MsgComplete, mt)
}

// TestHTTPHandler_BadUpgradeHeader verifies that a request without the correct
// Upgrade header receives a 400 Bad Request response.
func TestHTTPHandler_BadUpgradeHeader(t *testing.T) {
	ts := httptest.NewTLSServer(NewHTTPHandler())
	defer ts.Close()
	tsClient := ts.Client() // TLS client that trusts the test cert

	resp, err := tsClient.Get(ts.URL + BenchmarkPath)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, 400, resp.StatusCode, "missing Upgrade header should return 400")
}
