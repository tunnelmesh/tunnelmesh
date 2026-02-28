package connection

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// mockTunnel implements io.ReadWriteCloser for testing.
type mockTunnel struct {
	closed bool
	mu     sync.Mutex
}

// nolint:revive // p required by io.Reader interface but not used in mock
func (m *mockTunnel) Read(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, io.EOF
	}
	return 0, nil
}

func (m *mockTunnel) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

func (m *mockTunnel) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockTunnel) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// transitionRecorder records transitions for testing.
type transitionRecorder struct {
	mu          sync.Mutex
	transitions []Transition
}

func (r *transitionRecorder) OnTransition(t Transition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transitions = append(r.transitions, t)
}

func (r *transitionRecorder) Transitions() []Transition {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Transition, len(r.transitions))
	copy(result, r.transitions)
	return result
}

func (r *transitionRecorder) Last() *Transition {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.transitions) == 0 {
		return nil
	}
	t := r.transitions[len(r.transitions)-1]
	return &t
}

func TestNewPeerConnection(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	if pc.PeerName() != "test-peer" {
		t.Errorf("PeerName() = %q, want %q", pc.PeerName(), "test-peer")
	}
	if pc.MeshIP() != "10.0.0.1" {
		t.Errorf("MeshIP() = %q, want %q", pc.MeshIP(), "10.0.0.1")
	}
	if pc.State() != StateDisconnected {
		t.Errorf("State() = %v, want %v", pc.State(), StateDisconnected)
	}
	if pc.Tunnel() != nil {
		t.Error("Tunnel() should be nil initially")
	}
}

func TestPeerConnection_TransitionTo(t *testing.T) {
	recorder := &transitionRecorder{}
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName:  "test-peer",
		MeshIP:    "10.0.0.1",
		Observers: []Observer{recorder},
	})

	// Valid transition: Disconnected -> Connecting
	if err := pc.TransitionTo(StateConnecting, "dial started", nil); err != nil {
		t.Fatalf("TransitionTo(Connecting) failed: %v", err)
	}
	if pc.State() != StateConnecting {
		t.Errorf("State() = %v, want %v", pc.State(), StateConnecting)
	}

	// Check observer was notified
	last := recorder.Last()
	if last == nil {
		t.Fatal("Observer was not notified")
	}
	if last.From != StateDisconnected || last.To != StateConnecting {
		t.Errorf("Transition = %v->%v, want %v->%v", last.From, last.To, StateDisconnected, StateConnecting)
	}
	if last.Reason != "dial started" {
		t.Errorf("Reason = %q, want %q", last.Reason, "dial started")
	}

	// Valid transition: Connecting -> Connected
	if err := pc.TransitionTo(StateConnected, "handshake complete", nil); err != nil {
		t.Fatalf("TransitionTo(Connected) failed: %v", err)
	}
	if pc.State() != StateConnected {
		t.Errorf("State() = %v, want %v", pc.State(), StateConnected)
	}

	// Invalid transition: Connected -> Connecting
	if err := pc.TransitionTo(StateConnecting, "should fail", nil); err == nil {
		t.Error("TransitionTo(Connecting) from Connected should fail")
	}
	if pc.State() != StateConnected {
		t.Errorf("State() = %v, want %v (unchanged)", pc.State(), StateConnected)
	}
}

func TestPeerConnection_TransitionWithError(t *testing.T) {
	recorder := &transitionRecorder{}
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName:  "test-peer",
		MeshIP:    "10.0.0.1",
		Observers: []Observer{recorder},
	})

	_ = pc.TransitionTo(StateConnecting, "dial", nil)
	testErr := errors.New("connection refused")
	_ = pc.TransitionTo(StateDisconnected, "dial failed", testErr)

	if !errors.Is(pc.LastError(), testErr) {
		t.Errorf("LastError() = %v, want %v", pc.LastError(), testErr)
	}

	last := recorder.Last()
	if !errors.Is(last.Error, testErr) {
		t.Errorf("Transition.Error = %v, want %v", last.Error, testErr)
	}
}

func TestPeerConnection_SetTunnel(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	tunnel := &mockTunnel{}
	pc.SetTunnel(tunnel, "test")

	if pc.Tunnel() != tunnel {
		t.Error("Tunnel() should return the set tunnel")
	}

	pc.ClearTunnel()
	if pc.Tunnel() != nil {
		t.Error("Tunnel() should be nil after ClearTunnel()")
	}
}

func TestPeerConnection_Connected(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	tunnel := &mockTunnel{}
	if err := pc.Connected(tunnel, "test", "incoming connection"); err != nil {
		t.Fatalf("Connected() failed: %v", err)
	}

	if pc.State() != StateConnected {
		t.Errorf("State() = %v, want %v", pc.State(), StateConnected)
	}
	if pc.Tunnel() != tunnel {
		t.Error("Tunnel() should be set after Connected()")
	}
	if pc.ConnectedSince().IsZero() {
		t.Error("ConnectedSince() should be set after Connected()")
	}
}

func TestPeerConnection_StartReconnecting(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	tunnel := &mockTunnel{}
	_ = pc.Connected(tunnel, "test", "connected")

	err := errors.New("connection lost")
	if err := pc.StartReconnecting("network error", err); err != nil {
		t.Fatalf("StartReconnecting() failed: %v", err)
	}

	if pc.State() != StateReconnecting {
		t.Errorf("State() = %v, want %v", pc.State(), StateReconnecting)
	}
	if pc.Tunnel() != nil {
		t.Error("Tunnel() should be nil after StartReconnecting()")
	}
}

func TestPeerConnection_ReconnectCount(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	// Initial connect
	tunnel1 := &mockTunnel{}
	_ = pc.Connected(tunnel1, "test", "connected")
	if pc.ReconnectCount() != 0 {
		t.Errorf("ReconnectCount() = %d, want 0 after initial connect", pc.ReconnectCount())
	}

	// Reconnect
	_ = pc.StartReconnecting("network error", nil)
	tunnel2 := &mockTunnel{}
	_ = pc.Connected(tunnel2, "test", "reconnected")
	if pc.ReconnectCount() != 1 {
		t.Errorf("ReconnectCount() = %d, want 1 after first reconnect", pc.ReconnectCount())
	}

	// Another reconnect
	_ = pc.StartReconnecting("another error", nil)
	tunnel3 := &mockTunnel{}
	_ = pc.Connected(tunnel3, "test", "reconnected again")
	if pc.ReconnectCount() != 2 {
		t.Errorf("ReconnectCount() = %d, want 2 after second reconnect", pc.ReconnectCount())
	}
}

func TestPeerConnection_Close(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	tunnel := &mockTunnel{}
	_ = pc.Connected(tunnel, "test", "connected")

	if err := pc.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	if pc.State() != StateClosed {
		t.Errorf("State() = %v, want %v", pc.State(), StateClosed)
	}
	if !tunnel.IsClosed() {
		t.Error("Tunnel should be closed after Close()")
	}
	if pc.Tunnel() != nil {
		t.Error("Tunnel() should be nil after Close()")
	}

	// Cannot transition from closed
	if err := pc.TransitionTo(StateConnected, "should fail", nil); err == nil {
		t.Error("TransitionTo() from Closed should fail")
	}
}

func TestPeerConnection_CloseIdempotent(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	tunnel := &mockTunnel{}
	_ = pc.Connected(tunnel, "test", "connected")

	// Close multiple times should not panic
	_ = pc.Close()
	_ = pc.Close()
	_ = pc.Close()

	if pc.State() != StateClosed {
		t.Errorf("State() = %v, want %v", pc.State(), StateClosed)
	}
}

func TestPeerConnection_Disconnect(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	tunnel := &mockTunnel{}
	_ = pc.Connected(tunnel, "test", "connected")

	if err := pc.Disconnect("user requested", nil); err != nil {
		t.Fatalf("Disconnect() failed: %v", err)
	}

	if pc.State() != StateDisconnected {
		t.Errorf("State() = %v, want %v", pc.State(), StateDisconnected)
	}
	if !tunnel.IsClosed() {
		t.Error("Tunnel should be closed after Disconnect()")
	}

	// Can reconnect after Disconnect (unlike Close)
	tunnel2 := &mockTunnel{}
	if err := pc.Connected(tunnel2, "test", "reconnected"); err != nil {
		t.Fatalf("Connected() after Disconnect() failed: %v", err)
	}
	if pc.State() != StateConnected {
		t.Errorf("State() = %v, want %v", pc.State(), StateConnected)
	}
}

func TestPeerConnection_Info(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	info := pc.Info()
	if info.PeerName != "test-peer" {
		t.Errorf("Info().PeerName = %q, want %q", info.PeerName, "test-peer")
	}
	if info.MeshIP != "10.0.0.1" {
		t.Errorf("Info().MeshIP = %q, want %q", info.MeshIP, "10.0.0.1")
	}
	if info.State != StateDisconnected {
		t.Errorf("Info().State = %v, want %v", info.State, StateDisconnected)
	}
	if info.HasTunnel {
		t.Error("Info().HasTunnel should be false")
	}

	tunnel := &mockTunnel{}
	_ = pc.Connected(tunnel, "test", "connected")

	info = pc.Info()
	if info.State != StateConnected {
		t.Errorf("Info().State = %v, want %v", info.State, StateConnected)
	}
	if !info.HasTunnel {
		t.Error("Info().HasTunnel should be true")
	}
	if info.ConnectedSince.IsZero() {
		t.Error("Info().ConnectedSince should be set")
	}
}

func TestPeerConnection_Info_TransportTypeGuardedByState(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	tunnel := &mockTunnel{}
	_ = pc.Connected(tunnel, "udp", "connected via UDP")

	// When Connected, TransportType should be returned.
	info := pc.Info()
	if info.TransportType != "udp" {
		t.Errorf("Info().TransportType = %q while Connected, want %q", info.TransportType, "udp")
	}

	// Disconnect — transportType field is NOT cleared but state changes.
	_ = pc.Disconnect("test disconnect", nil)

	// After disconnect, Info() must NOT expose the stale transport type.
	info = pc.Info()
	if info.TransportType != "" {
		t.Errorf("Info().TransportType = %q after Disconnect, want empty string", info.TransportType)
	}
}

func TestPeerConnection_AddObserver(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	recorder := &transitionRecorder{}
	pc.AddObserver(recorder)

	_ = pc.TransitionTo(StateConnecting, "test", nil)

	if len(recorder.Transitions()) != 1 {
		t.Errorf("Observer should receive 1 transition, got %d", len(recorder.Transitions()))
	}
}

func TestPeerConnection_ConcurrentTransitions(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	// Run many concurrent transitions
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Go(func() {
			_ = pc.StartConnecting("concurrent test")
			_ = pc.Disconnect("concurrent test", nil)
		})
	}

	wg.Wait()

	// Should end up in a valid state
	state := pc.State()
	if state != StateDisconnected && state != StateConnecting {
		t.Errorf("Unexpected final state: %v", state)
	}
}

func TestObserverFunc(t *testing.T) {
	var received *Transition
	observer := ObserverFunc(func(t Transition) {
		received = &t
	})

	transition := Transition{
		PeerName:  "test",
		From:      StateDisconnected,
		To:        StateConnecting,
		Timestamp: time.Now(),
		Reason:    "test",
	}

	observer.OnTransition(transition)

	if received == nil {
		t.Fatal("ObserverFunc was not called")
	}
	if received.PeerName != "test" {
		t.Errorf("Received.PeerName = %q, want %q", received.PeerName, "test")
	}
}

func TestMultiObserver(t *testing.T) {
	recorder1 := &transitionRecorder{}
	recorder2 := &transitionRecorder{}

	multi := NewMultiObserver(recorder1, recorder2)

	transition := Transition{
		PeerName:  "test",
		From:      StateDisconnected,
		To:        StateConnecting,
		Timestamp: time.Now(),
		Reason:    "test",
	}

	multi.OnTransition(transition)

	if len(recorder1.Transitions()) != 1 {
		t.Errorf("Recorder1 should have 1 transition, got %d", len(recorder1.Transitions()))
	}
	if len(recorder2.Transitions()) != 1 {
		t.Errorf("Recorder2 should have 1 transition, got %d", len(recorder2.Transitions()))
	}

	// Add another observer
	recorder3 := &transitionRecorder{}
	multi.Add(recorder3)

	multi.OnTransition(transition)

	if len(recorder3.Transitions()) != 1 {
		t.Errorf("Recorder3 should have 1 transition, got %d", len(recorder3.Transitions()))
	}
}

// pipeRWC wraps an io.Pipe for testing, implementing io.ReadWriteCloser.
type pipeRWC struct {
	reader *io.PipeReader
	writer *io.PipeWriter
}

func newPipeRWC() *pipeRWC {
	r, w := io.Pipe()
	return &pipeRWC{reader: r, writer: w}
}

func (p *pipeRWC) Read(data []byte) (int, error) {
	return p.reader.Read(data)
}

func (p *pipeRWC) Write(data []byte) (int, error) {
	return p.writer.Write(data)
}

func (p *pipeRWC) Close() error {
	_ = p.reader.Close()
	return p.writer.Close()
}

// Test that cancel function is cleared when transitioning to Connected
// This prevents incoming connections from cancelling the HandleTunnel context
func TestPeerConnection_CancelFuncClearedOnConnected(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	// Simulate outbound connection flow: Disconnected -> Connecting -> Connected
	cancelled := false
	pc.SetCancelFunc(func() { cancelled = true })
	_ = pc.StartConnecting("dial")

	// Connect with a tunnel
	tunnel := &mockTunnel{}
	_ = pc.Connected(tunnel, "test", "test")

	// The cancel function should have been cleared during transition to Connected
	// Calling CancelOutbound should NOT cancel anything
	wasCancelled := pc.CancelOutbound()
	if wasCancelled {
		t.Error("CancelOutbound() should return false after Connected transition")
	}
	if cancelled {
		t.Error("Cancel function should NOT have been called - it should have been cleared")
	}
}

// Test that cancel function is called when CancelOutbound is called before Connected
func TestPeerConnection_CancelOutboundDuringConnecting(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	// Simulate outbound connection flow starting
	cancelled := false
	pc.SetCancelFunc(func() { cancelled = true })
	_ = pc.StartConnecting("dial")

	// While still connecting, cancel outbound (simulates incoming connection arriving)
	wasCancelled := pc.CancelOutbound()
	if !wasCancelled {
		t.Error("CancelOutbound() should return true when cancel func is set")
	}
	if !cancelled {
		t.Error("Cancel function should have been called")
	}

	// Subsequent calls should return false
	wasCancelled = pc.CancelOutbound()
	if wasCancelled {
		t.Error("CancelOutbound() should return false when called again")
	}
}

// Test that ConnectedSince returns zero when tunnel is set but state is not Connected.
// This is important for relay packet handling: if a tunnel is set but state is Connecting,
// we must not use time.Since(zero) to calculate tunnel age as it would return a huge duration.
func TestPeerConnection_ConnectedSinceZeroWhenNotConnected(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	// Set a tunnel directly without transitioning to Connected state
	tunnel := &mockTunnel{}
	pc.SetTunnel(tunnel, "test")

	// Verify tunnel is set
	if !pc.HasTunnel() {
		t.Error("HasTunnel() should return true")
	}

	// State should still be Disconnected
	if pc.State() != StateDisconnected {
		t.Errorf("State() = %v, want %v", pc.State(), StateDisconnected)
	}

	// ConnectedSince should return zero time since we're not in Connected state
	connectedSince := pc.ConnectedSince()
	if !connectedSince.IsZero() {
		t.Errorf("ConnectedSince() = %v, want zero time when state is not Connected", connectedSince)
	}

	// Now transition to Connecting - should still return zero
	_ = pc.TransitionTo(StateConnecting, "dial started", nil)
	connectedSince = pc.ConnectedSince()
	if !connectedSince.IsZero() {
		t.Errorf("ConnectedSince() = %v, want zero time when state is Connecting", connectedSince)
	}

	// Now properly connect - should have non-zero time
	_ = pc.TransitionTo(StateConnected, "connected", nil)
	connectedSince = pc.ConnectedSince()
	if connectedSince.IsZero() {
		t.Error("ConnectedSince() should be non-zero after transitioning to Connected")
	}
}

// Test that tunnels with actual data work
func TestPeerConnection_TunnelDataFlow(t *testing.T) {
	pc := NewPeerConnection(PeerConnectionConfig{
		PeerName: "test-peer",
		MeshIP:   "10.0.0.1",
	})

	// Create a pipe-based tunnel
	pipe := newPipeRWC()
	defer func() { _ = pipe.Close() }()

	pc.SetTunnel(pipe, "test")

	// Verify tunnel is accessible
	gotTunnel := pc.Tunnel()
	if gotTunnel == nil {
		t.Fatal("Tunnel() should return the set tunnel")
	}

	// Write data in a goroutine
	go func() {
		_, _ = pipe.writer.Write([]byte("hello"))
	}()

	// Read from tunnel
	buf := make([]byte, 5)
	n, err := pipe.reader.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read from tunnel: %v", err)
	}
	if !bytes.Equal(buf[:n], []byte("hello")) {
		t.Errorf("Read %q, want %q", buf[:n], "hello")
	}
}
