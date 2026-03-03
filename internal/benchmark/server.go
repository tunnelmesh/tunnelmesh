package benchmark

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/rs/zerolog/log"
)

// serveConn runs the benchmark binary protocol on an already-established net.Conn.
// It is called from the HTTP-upgrade handler in http.go via NewHTTPHandler.
func serveConn(ctx context.Context, conn net.Conn) {
	log.Debug().Str("remote", conn.RemoteAddr().String()).Msg("benchmark connection accepted")

	// Set read deadline for initial message
	_ = conn.SetReadDeadline(time.Now().Add(ReadTimeout))

	// Read start message
	mt, data, err := ReadMessage(conn)
	if err != nil {
		log.Error().Err(err).Msg("failed to read start message")
		return
	}

	if mt != MsgStart {
		log.Error().Str("type", mt.String()).Msg("expected Start message")
		connSendError(conn, "expected Start message")
		return
	}

	var start StartMessage
	if err := start.Decode(bytes.NewReader(data)); err != nil {
		log.Error().Err(err).Msg("failed to decode start message")
		connSendError(conn, "failed to decode start message")
		return
	}

	// Send ack
	ack := AckMessage{Accepted: true}
	if err := WriteMessage(conn, MsgAck, &ack); err != nil {
		log.Error().Err(err).Msg("failed to send ack")
		return
	}

	// Clear deadline for data transfer
	_ = conn.SetReadDeadline(time.Time{})

	// Handle based on direction
	switch start.Direction {
	case DirectionUpload:
		connHandleUpload(ctx, conn, start.Size)
	case DirectionDownload:
		connHandleDownload(ctx, conn, start.Size)
	default:
		connSendError(conn, fmt.Sprintf("unknown direction: %s", start.Direction))
	}
}

func connHandleUpload(ctx context.Context, conn net.Conn, size int64) {
	startTime := time.Now()
	var received int64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set read deadline
		_ = conn.SetReadDeadline(time.Now().Add(ReadTimeout))

		mt, data, err := ReadMessage(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Error().Err(err).Msg("failed to read message during upload")
			return
		}

		switch mt {
		case MsgData:
			var dataMsg DataMessage
			if err := dataMsg.Decode(bytes.NewReader(data)); err != nil {
				log.Error().Err(err).Msg("failed to decode data message")
				return
			}
			received += int64(len(dataMsg.Data))

		case MsgPing:
			var ping PingMessage
			if err := ping.Decode(bytes.NewReader(data)); err != nil {
				log.Error().Err(err).Msg("failed to decode ping")
				continue
			}
			pong := PongMessage{SeqNum: ping.SeqNum, PingTimestamp: ping.Timestamp}
			if err := WriteMessage(conn, MsgPong, &pong); err != nil {
				log.Error().Err(err).Msg("failed to send pong")
				return
			}

		case MsgComplete:
			// Client is done, send our stats
			duration := time.Since(startTime)
			complete := CompleteMessage{
				BytesTransferred: received,
				DurationNs:       duration.Nanoseconds(),
			}
			if err := WriteMessage(conn, MsgComplete, &complete); err != nil {
				log.Error().Err(err).Msg("failed to send complete")
			}
			return

		default:
			log.Warn().Str("type", mt.String()).Msg("unexpected message type during upload")
		}
	}
}

func connHandleDownload(ctx context.Context, conn net.Conn, size int64) {
	startTime := time.Now()
	var sent int64
	var seqNum uint32

	// Generate random data chunk
	chunk := make([]byte, ChunkSize)
	_, _ = rand.Read(chunk)

	// Handle any incoming pings before starting data transfer
	connHandlePendingPings(conn)

	for sent < size {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Calculate chunk size for this iteration
		remaining := size - sent
		dataSize := int64(ChunkSize)
		if remaining < dataSize {
			dataSize = remaining
		}

		// Send data
		dataMsg := DataMessage{
			SeqNum: seqNum,
			Data:   chunk[:dataSize],
		}
		if err := WriteMessage(conn, MsgData, &dataMsg); err != nil {
			log.Error().Err(err).Msg("failed to send data")
			return
		}

		sent += dataSize
		seqNum++
	}

	// Send complete
	duration := time.Since(startTime)
	complete := CompleteMessage{
		BytesTransferred: sent,
		DurationNs:       duration.Nanoseconds(),
	}
	if err := WriteMessage(conn, MsgComplete, &complete); err != nil {
		log.Error().Err(err).Msg("failed to send complete")
	}
}

// connHandlePendingPings reads and responds to any pending ping messages.
func connHandlePendingPings(conn net.Conn) {
	// Set a short timeout to check for pings
	_ = conn.SetReadDeadline(time.Now().Add(PingCheckTimeout))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	for {
		mt, data, err := ReadMessage(conn)
		if err != nil {
			// No more pending messages
			return
		}

		if mt == MsgPing {
			var ping PingMessage
			if err := ping.Decode(bytes.NewReader(data)); err != nil {
				return
			}
			pong := PongMessage{SeqNum: ping.SeqNum, PingTimestamp: ping.Timestamp}
			_ = WriteMessage(conn, MsgPong, &pong)
		}
	}
}

func connSendError(conn net.Conn, msg string) {
	errMsg := ErrorMessage{Error: msg}
	_ = WriteMessage(conn, MsgError, &errMsg)
}
