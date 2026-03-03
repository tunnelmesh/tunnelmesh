package benchmark

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// Client runs benchmark tests against a remote peer.
type Client struct {
	localPeer  string
	remoteAddr string
}

// NewClient creates a new benchmark client.
func NewClient(localPeer, remoteAddr string) *Client {
	return &Client{
		localPeer:  localPeer,
		remoteAddr: remoteAddr,
	}
}

// Run executes a benchmark with the given configuration.
func (c *Client) Run(ctx context.Context, cfg Config) (*Result, error) {
	cfg = cfg.WithDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	result := &Result{
		ID:            uuid.New().String(),
		Timestamp:     time.Now(),
		LocalPeer:     c.localPeer,
		RemotePeer:    cfg.PeerName,
		Direction:     cfg.Direction,
		RequestedSize: cfg.Size,
	}

	if cfg.Chaos.IsEnabled() {
		result.Chaos = &cfg.Chaos
	}

	// Connect to the benchmark endpoint on the remote admin server via HTTP upgrade + TLS.
	addr := fmt.Sprintf("%s:%d", c.remoteAddr, cfg.Port)
	conn, err := dialBenchmarkHTTP(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	log.Debug().Str("addr", addr).Msg("connected to benchmark server")

	// Send start message
	start := StartMessage{
		Size:      cfg.Size,
		Direction: cfg.Direction,
	}
	if err := WriteMessage(conn, MsgStart, &start); err != nil {
		return nil, fmt.Errorf("failed to send start: %w", err)
	}

	// Read ack
	mt, data, err := ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read ack: %w", err)
	}

	if mt != MsgAck {
		if mt == MsgError {
			var errMsg ErrorMessage
			_ = errMsg.Decode(bytes.NewReader(data))
			return nil, fmt.Errorf("server rejected benchmark: %s", errMsg.Error)
		}
		return nil, fmt.Errorf("expected Ack, got %s", mt.String())
	}

	var ack AckMessage
	if err := ack.Decode(bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("failed to decode ack: %w", err)
	}

	if !ack.Accepted {
		return nil, fmt.Errorf("server rejected benchmark: %s", ack.Error)
	}

	// Run benchmark based on direction
	var runErr error
	switch cfg.Direction {
	case DirectionUpload:
		runErr = c.runUpload(ctx, conn, cfg, result)
	case DirectionDownload:
		runErr = c.runDownload(ctx, conn, cfg, result)
	}

	if runErr != nil {
		result.Success = false
		result.Error = runErr.Error()
		return result, runErr
	}

	result.Success = true
	result.CalculateThroughput()
	return result, nil
}

func (c *Client) runUpload(ctx context.Context, conn net.Conn, cfg Config, result *Result) error {
	startTime := time.Now()
	var sent int64
	var seqNum uint32
	var latencies []float64

	// Prepare data chunk
	chunk := make([]byte, ChunkSize)
	_, _ = rand.Read(chunk)

	// Wrap connection with chaos if configured
	var writer io.Writer = conn
	if cfg.Chaos.IsEnabled() {
		writer = NewChaosWriter(conn, cfg.Chaos)
	}

	// Send initial ping for latency baseline
	latency, err := c.measureLatency(conn, 0)
	if err == nil && latency > 0 {
		latencies = append(latencies, latency)
	}

	// Send data
	for sent < cfg.Size {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Calculate chunk size
		remaining := cfg.Size - sent
		dataSize := int64(ChunkSize)
		if remaining < dataSize {
			dataSize = remaining
		}

		// Encode data message
		dataMsg := DataMessage{
			SeqNum: seqNum,
			Data:   chunk[:dataSize],
		}

		// Write through potentially chaos-wrapped writer
		var buf bytes.Buffer
		if err := WriteMessage(&buf, MsgData, &dataMsg); err != nil {
			return fmt.Errorf("failed to encode data: %w", err)
		}
		if _, err := writer.Write(buf.Bytes()); err != nil {
			return fmt.Errorf("failed to send data: %w", err)
		}

		sent += dataSize
		seqNum++

		// Periodic ping for latency measurement (every ~10 chunks)
		if seqNum%10 == 0 {
			latency, err := c.measureLatency(conn, seqNum)
			if err == nil && latency > 0 {
				latencies = append(latencies, latency)
			}
		}
	}

	// Final ping
	latency, err = c.measureLatency(conn, seqNum+1)
	if err == nil && latency > 0 {
		latencies = append(latencies, latency)
	}

	// Send complete
	duration := time.Since(startTime)
	complete := CompleteMessage{
		BytesTransferred: sent,
		DurationNs:       duration.Nanoseconds(),
	}
	if err := WriteMessage(conn, MsgComplete, &complete); err != nil {
		return fmt.Errorf("failed to send complete: %w", err)
	}

	// Read server's complete
	mt, data, err := ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to read server complete: %w", err)
	}

	if mt != MsgComplete {
		return fmt.Errorf("expected Complete, got %s", mt.String())
	}

	var serverComplete CompleteMessage
	if err := serverComplete.Decode(bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to decode server complete: %w", err)
	}

	// Update result
	result.TransferredSize = sent
	result.DurationMs = duration.Milliseconds()
	c.calculateLatencyStats(latencies, result)

	return nil
}

func (c *Client) runDownload(ctx context.Context, conn net.Conn, cfg Config, result *Result) error {
	startTime := time.Now()
	var received int64
	var latencies []float64

	// Send initial ping
	latency, err := c.measureLatency(conn, 0)
	if err == nil && latency > 0 {
		latencies = append(latencies, latency)
	}

	// Receive data
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(ReadTimeout))

		mt, data, err := ReadMessage(conn)
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch mt {
		case MsgData:
			var dataMsg DataMessage
			if err := dataMsg.Decode(bytes.NewReader(data)); err != nil {
				return fmt.Errorf("failed to decode data: %w", err)
			}
			received += int64(len(dataMsg.Data))

		case MsgComplete:
			var serverComplete CompleteMessage
			if err := serverComplete.Decode(bytes.NewReader(data)); err != nil {
				return fmt.Errorf("failed to decode complete: %w", err)
			}

			// Update result
			duration := time.Since(startTime)
			result.TransferredSize = received
			result.DurationMs = duration.Milliseconds()
			c.calculateLatencyStats(latencies, result)
			return nil

		default:
			log.Warn().Str("type", mt.String()).Msg("unexpected message type during download")
		}
	}
}

func (c *Client) measureLatency(conn net.Conn, seqNum uint32) (float64, error) {
	now := time.Now()
	ping := PingMessage{
		SeqNum:    seqNum,
		Timestamp: now.UnixNano(),
	}

	if err := WriteMessage(conn, MsgPing, &ping); err != nil {
		return 0, err
	}

	_ = conn.SetReadDeadline(time.Now().Add(LatencyPingTimeout))
	mt, data, err := ReadMessage(conn)
	if err != nil {
		return 0, err
	}

	if mt != MsgPong {
		return 0, fmt.Errorf("expected Pong, got %s", mt.String())
	}

	var pong PongMessage
	if err := pong.Decode(bytes.NewReader(data)); err != nil {
		return 0, err
	}

	rtt := time.Since(now)
	return float64(rtt.Nanoseconds()) / 1e6, nil // Convert to milliseconds
}

func (c *Client) calculateLatencyStats(latencies []float64, result *Result) {
	if len(latencies) == 0 {
		return
	}

	// Sort for percentile calculation
	sorted := make([]float64, len(latencies))
	copy(sorted, latencies)
	sort.Float64s(sorted)

	// Min, Max
	result.LatencyMinMs = sorted[0]
	result.LatencyMaxMs = sorted[len(sorted)-1]

	// Average
	var sum float64
	for _, l := range latencies {
		sum += l
	}
	result.LatencyAvgMs = sum / float64(len(latencies))
}
