// Package connection implements a finite state machine for peer connection management.

package connection

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
)

// PeerConnection manages the connection state for a single peer.
// It provides thread-safe state transitions and notifies observers of changes.
type PeerConnection struct {
	mu sync.RWMutex

	// Identity
	peerName string
	meshIP   string

	// State
	state     State
	lastError error

	// Tunnel (active connection)
	tunnel        io.ReadWriteCloser
	transportType string // "udp", "ssh", "relay", etc.

	// Cancel function for outbound connection attempts
	cancelFunc context.CancelFunc

	// Observers for state changes
	observers []Observer

	// Timestamps
	createdAt      time.Time
	lastTransition time.Time
	connectedSince time.Time
	reconnectCount int
}

// PeerConnectionConfig holds configuration for creating a PeerConnection.
type PeerConnectionConfig struct {
	PeerName  string
	MeshIP    string
	Observers []Observer
}

// NewPeerConnection creates a new PeerConnection in the Disconnected state.
func NewPeerConnection(cfg PeerConnectionConfig) *PeerConnection {
	now := time.Now()
	return &PeerConnection{
		peerName:       cfg.PeerName,
		meshIP:         cfg.MeshIP,
		state:          StateDisconnected,
		observers:      cfg.Observers,
		createdAt:      now,
		lastTransition: now,
	}
}

// PeerName returns the peer's name.
func (pc *PeerConnection) PeerName() string {
	return pc.peerName
}

// MeshIP returns the peer's mesh IP address.
func (pc *PeerConnection) MeshIP() string {
	return pc.meshIP
}

// State returns the current connection state.
func (pc *PeerConnection) State() State {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.state
}

// LastError returns the last error that caused a state transition.
func (pc *PeerConnection) LastError() error {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.lastError
}

// Tunnel returns the current tunnel, or nil if not connected.
func (pc *PeerConnection) Tunnel() io.ReadWriteCloser {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.tunnel
}

// HasTunnel returns true if the connection has an active tunnel.
func (pc *PeerConnection) HasTunnel() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.tunnel != nil
}

// TransportType returns the transport type of the current tunnel (e.g., "udp", "ssh", "relay").
// Returns empty string if not connected.
func (pc *PeerConnection) TransportType() string {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if pc.state != StateConnected {
		return ""
	}
	return pc.transportType
}

// ConnectedSince returns when the connection was established.
// Returns zero time if not currently connected.
func (pc *PeerConnection) ConnectedSince() time.Time {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if pc.state != StateConnected {
		return time.Time{}
	}
	return pc.connectedSince
}

// ReconnectCount returns the number of times this connection has reconnected.
func (pc *PeerConnection) ReconnectCount() int {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.reconnectCount
}

// AddObserver adds an observer to receive state change notifications.
func (pc *PeerConnection) AddObserver(o Observer) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.observers = append(pc.observers, o)
}

// SetCancelFunc sets a cancel function for an outbound connection attempt.
// This is called to cancel the attempt when an inbound connection arrives first.
func (pc *PeerConnection) SetCancelFunc(cancel context.CancelFunc) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.cancelFunc = cancel
}

// CancelOutbound calls the cancel function if one was set, clearing it afterwards.
// Returns true if a cancel function was called.
func (pc *PeerConnection) CancelOutbound() bool {
	pc.mu.Lock()
	cancel := pc.cancelFunc
	pc.cancelFunc = nil
	pc.mu.Unlock()

	if cancel != nil {
		cancel()
		return true
	}
	return false
}

// TransitionTo attempts to transition to the target state.
// Returns an error if the transition is invalid.
// On success, notifies all observers of the transition.
// ctx is used to parent the OTel span — callers with a meaningful trace context
// should pass it; background goroutines should pass context.Background().
func (pc *PeerConnection) TransitionTo(ctx context.Context, target State, reason string, err error) error {
	pc.mu.Lock()

	from := pc.state

	// Check if transition is valid
	if !from.CanTransitionTo(target) {
		pc.mu.Unlock()
		return NewTransitionError(from, target, pc.peerName, "invalid transition")
	}

	// Perform transition
	now := time.Now()
	pc.state = target
	pc.lastError = err
	pc.lastTransition = now

	// Track connected time and reconnect count
	if target == StateConnected {
		if from == StateReconnecting {
			pc.reconnectCount++
		}
		pc.connectedSince = now
		// Clear cancel function - outbound dial has succeeded, don't allow cancellation
		// This prevents incoming connections from cancelling the HandleTunnel context
		pc.cancelFunc = nil
	}

	// Build transition event
	transition := Transition{
		PeerName:  pc.peerName,
		From:      from,
		To:        target,
		Timestamp: now,
		Reason:    reason,
		Error:     err,
	}

	// Copy observers slice to avoid holding lock during callbacks
	observers := make([]Observer, len(pc.observers))
	copy(observers, pc.observers)

	pc.mu.Unlock()

	// Emit an OTel span for this state transition.
	// Each transition is nearly instantaneous, so the span duration is ~0.
	// When ctx carries a span (e.g. from peer.establish_tunnel), this span
	// becomes a child of that trace, making the full connection waterfall
	// visible in Tempo. Background callers (lifecycle goroutines, Close) pass
	// context.Background() so their transitions appear as root spans.
	_, span := tracing.Tracer("tunnelmesh/connection").Start(
		ctx,
		"connection.transition",
	)
	span.SetAttributes(
		attribute.String("peer.name", pc.peerName),
		attribute.String("peer.mesh_ip", pc.meshIP),
		attribute.String("transition.from", from.String()),
		attribute.String("transition.to", target.String()),
		attribute.String("transition.reason", reason),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
	}
	span.End()

	// Log the transition (include trace_id for Loki → Tempo correlation)
	sc := span.SpanContext()
	logEvent := log.Debug().
		Str("peer", pc.peerName).
		Str("from", from.String()).
		Str("to", target.String()).
		Str("reason", reason).
		Str("trace_id", sc.TraceID().String()).
		Str("span_id", sc.SpanID().String())
	if err != nil {
		logEvent = logEvent.Err(err)
	}
	logEvent.Msg("connection state transition")

	// Notify observers (outside lock)
	for _, o := range observers {
		o.OnTransition(transition)
	}

	return nil
}

// SetTunnel sets the active tunnel for this connection.
// This should be called when transitioning to Connected state.
// transportType should be "udp", "ssh", "relay", etc.
func (pc *PeerConnection) SetTunnel(tunnel io.ReadWriteCloser, transportType string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.tunnel = tunnel
	pc.transportType = transportType
}

// ClearTunnel clears the tunnel reference without closing it.
// Use this when the tunnel is managed externally.
func (pc *PeerConnection) ClearTunnel() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.tunnel = nil
	pc.transportType = ""
}

// Close closes the tunnel and transitions to Closed state.
// This is a terminal operation - the PeerConnection cannot be reused.
func (pc *PeerConnection) Close() error {
	pc.mu.Lock()
	tunnel := pc.tunnel
	pc.tunnel = nil
	state := pc.state
	pc.mu.Unlock()

	// Close the tunnel if present
	var tunnelErr error
	if tunnel != nil {
		tunnelErr = tunnel.Close()
	}

	// Transition to closed (if not already). Close is terminal and is called
	// from background goroutines, so we use Background context.
	if !state.IsTerminal() {
		if err := pc.TransitionTo(context.Background(), StateClosed, "explicit close", tunnelErr); err != nil {
			// Log but don't fail - we still want to clean up
			log.Debug().
				Str("peer", pc.peerName).
				Err(err).
				Msg("error transitioning to closed state")
		}
	}

	return tunnelErr
}

// Disconnect transitions to Disconnected state and closes the tunnel.
// Unlike Close(), the connection can be reused after Disconnect().
// ctx is propagated to the OTel span — callers with a meaningful trace context
// should pass it; background callers should pass context.Background().
func (pc *PeerConnection) Disconnect(ctx context.Context, reason string, err error) error {
	pc.mu.Lock()
	tunnel := pc.tunnel
	pc.tunnel = nil
	state := pc.state
	pc.mu.Unlock()

	// Close the tunnel if present
	if tunnel != nil {
		_ = tunnel.Close()
	}

	// Transition to disconnected (if valid)
	if state.CanTransitionTo(StateDisconnected) {
		return pc.TransitionTo(ctx, StateDisconnected, reason, err)
	}

	return nil
}

// StartConnecting transitions to Connecting state.
func (pc *PeerConnection) StartConnecting(reason string) error {
	return pc.TransitionTo(context.Background(), StateConnecting, reason, nil)
}

// Connected transitions to Connected state and sets the tunnel.
// transportType should be "udp", "ssh", "relay", etc.
// ctx is propagated to the OTel span — pass the call-site context so the
// transition span appears as a child of the enclosing trace.
func (pc *PeerConnection) Connected(ctx context.Context, tunnel io.ReadWriteCloser, transportType string, reason string) error {
	pc.SetTunnel(tunnel, transportType)
	if err := pc.TransitionTo(ctx, StateConnected, reason, nil); err != nil {
		// Roll back tunnel assignment on failure
		pc.ClearTunnel()
		return err
	}
	return nil
}

// StartReconnecting transitions to Reconnecting state.
// The tunnel is cleared but not closed (caller should close it).
func (pc *PeerConnection) StartReconnecting(reason string, err error) error {
	pc.ClearTunnel()
	return pc.TransitionTo(context.Background(), StateReconnecting, reason, err)
}

// nolint:revive // ConnectionInfo name kept for clarity despite stuttering

// ConnectionInfo contains snapshot information about a connection.
type ConnectionInfo struct {
	PeerName       string
	MeshIP         string
	State          State
	LastError      error
	ConnectedSince time.Time
	ReconnectCount int
	HasTunnel      bool
	TransportType  string // "ssh", "udp", "relay", or "" if not connected
}

// Info returns a snapshot of the connection's current state.
func (pc *PeerConnection) Info() ConnectionInfo {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	return ConnectionInfo{
		PeerName:       pc.peerName,
		MeshIP:         pc.meshIP,
		State:          pc.state,
		LastError:      pc.lastError,
		ConnectedSince: pc.connectedSince,
		ReconnectCount: pc.reconnectCount,
		HasTunnel:      pc.tunnel != nil,
		TransportType:  pc.transportType,
	}
}
