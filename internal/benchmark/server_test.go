package benchmark

import (
	"context"
	"io"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// 3. Send ping
	ping := PingMessage{SeqNum: 1, Timestamp: time.Now().UnixNano()}
	require.NoError(t, WriteMessage(conn, MsgPing, &ping))

	// 4. Read pong
	mt, _, err = ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, MsgPong, mt)

	// 5. Send data
	dataMsg := DataMessage{SeqNum: 0, Data: make([]byte, 256)}
	require.NoError(t, WriteMessage(conn, MsgData, &dataMsg))

	// 6. Send complete
	complete := CompleteMessage{BytesTransferred: 256, DurationNs: int64(time.Millisecond)}
	require.NoError(t, WriteMessage(conn, MsgComplete, &complete))

	// 7. Read server's complete
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
