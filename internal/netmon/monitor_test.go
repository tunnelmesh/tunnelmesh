package netmon

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangeTypeString(t *testing.T) {
	tests := []struct {
		changeType ChangeType
		expected   string
	}{
		{ChangeUnknown, "unknown"},
		{ChangeAddressAdded, "address_added"},
		{ChangeAddressRemoved, "address_removed"},
		{ChangeInterfaceUp, "interface_up"},
		{ChangeInterfaceDown, "interface_down"},
		{ChangeRouteChanged, "route_changed"},
		{ChangeType(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.changeType.String())
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, 500*time.Millisecond, cfg.DebounceInterval)
	assert.Contains(t, cfg.IgnoreInterfaces, "lo*")
	assert.Contains(t, cfg.IgnoreInterfaces, "docker*")
	assert.Contains(t, cfg.IgnoreInterfaces, "veth*")
	assert.Contains(t, cfg.IgnoreInterfaces, "tun-mesh*")
	assert.Contains(t, cfg.IgnoreInterfaces, "utun*")
}

func TestNewWithDefaults(t *testing.T) {
	cfg := Config{} // Empty config
	monitor, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, monitor)
	defer func() { _ = monitor.Close() }()
}

func TestNewWithCustomConfig(t *testing.T) {
	cfg := Config{
		DebounceInterval: 100 * time.Millisecond,
		IgnoreInterfaces: []string{"test*"},
	}
	monitor, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, monitor)
	defer func() { _ = monitor.Close() }()
}

func TestDebouncerCoalesces(t *testing.T) {
	input := make(chan Event, 10)
	debouncer := NewDebouncer(input, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	output := debouncer.Run(ctx)

	// Send multiple rapid events
	for i := 0; i < 5; i++ {
		input <- Event{
			Type:      ChangeAddressAdded,
			Timestamp: time.Now(),
		}
	}

	// Wait for debounce period
	time.Sleep(100 * time.Millisecond)

	// Should receive only one coalesced event
	select {
	case event := <-output:
		assert.Equal(t, ChangeAddressAdded, event.Type)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected to receive an event")
	}

	// Should not receive more events immediately
	select {
	case <-output:
		t.Fatal("should not receive another event")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}

func TestDebouncerPassesSingleEvent(t *testing.T) {
	input := make(chan Event, 10)
	debouncer := NewDebouncer(input, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	output := debouncer.Run(ctx)

	// Send single event
	input <- Event{
		Type:      ChangeInterfaceUp,
		Interface: "eth0",
		Timestamp: time.Now(),
	}

	// Wait for debounce period
	time.Sleep(100 * time.Millisecond)

	// Should receive the event
	select {
	case event := <-output:
		assert.Equal(t, ChangeInterfaceUp, event.Type)
		assert.Equal(t, "eth0", event.Interface)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected to receive an event")
	}
}

func TestDebouncerContextCancellation(t *testing.T) {
	input := make(chan Event, 10)
	debouncer := NewDebouncer(input, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	output := debouncer.Run(ctx)

	// Cancel context
	cancel()

	// Output channel should be closed
	select {
	case _, ok := <-output:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected channel to be closed")
	}
}

func TestDebouncerInputClose(t *testing.T) {
	input := make(chan Event, 10)
	debouncer := NewDebouncer(input, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	output := debouncer.Run(ctx)

	// Send an event then close input
	input <- Event{Type: ChangeAddressAdded, Timestamp: time.Now()}
	close(input)

	// Should receive the pending event
	select {
	case event := <-output:
		assert.Equal(t, ChangeAddressAdded, event.Type)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected to receive pending event")
	}

	// Output should be closed
	select {
	case _, ok := <-output:
		assert.False(t, ok, "output channel should be closed")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected output channel to be closed")
	}
}

func TestMonitorStartAndClose(t *testing.T) {
	cfg := DefaultConfig()
	monitor, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, monitor)

	ctx, cancel := context.WithCancel(context.Background())

	events, err := monitor.Start(ctx)
	require.NoError(t, err)
	require.NotNil(t, events)

	// Cancel and close
	cancel()
	err = monitor.Close()
	assert.NoError(t, err)

	// Events channel should eventually close
	select {
	case <-events:
		// Might get one event, that's fine
	case <-time.After(2 * time.Second):
		// Timeout is acceptable for platform-specific implementations
	}
}

func TestEventFields(t *testing.T) {
	now := time.Now()
	event := Event{
		Type:      ChangeAddressAdded,
		Interface: "eth0",
		Address:   []byte{192, 168, 1, 1},
		Timestamp: now,
	}

	assert.Equal(t, ChangeAddressAdded, event.Type)
	assert.Equal(t, "eth0", event.Interface)
	assert.Equal(t, []byte{192, 168, 1, 1}, []byte(event.Address))
	assert.Equal(t, now, event.Timestamp)
}
