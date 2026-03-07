// Package netmon provides network interface change monitoring.
package netmon

import (
	"context"
	"net"
	"time"
)

// ChangeType represents the type of network change detected.
type ChangeType int

const (
	ChangeUnknown ChangeType = iota
	ChangeAddressAdded
	ChangeAddressRemoved
	ChangeInterfaceUp
	ChangeInterfaceDown
	ChangeRouteChanged
)

// String returns a string representation of the change type.
func (c ChangeType) String() string {
	switch c {
	case ChangeAddressAdded:
		return "address_added"
	case ChangeAddressRemoved:
		return "address_removed"
	case ChangeInterfaceUp:
		return "interface_up"
	case ChangeInterfaceDown:
		return "interface_down"
	case ChangeRouteChanged:
		return "route_changed"
	default:
		return "unknown"
	}
}

// Event represents a network change event.
type Event struct {
	Type      ChangeType
	Interface string
	Address   net.IP
	Timestamp time.Time
}

// Monitor watches for network interface changes.
type Monitor interface {
	// Start begins monitoring for network changes.
	// Events are sent to the returned channel.
	// The channel is closed when the context is cancelled or an error occurs.
	Start(ctx context.Context) (<-chan Event, error)

	// Close releases any resources held by the monitor.
	Close() error
}

// Config holds monitor configuration.
type Config struct {
	// DebounceInterval is the minimum time between emitting events.
	// Multiple rapid changes are coalesced into a single event.
	// Default: 500ms
	DebounceInterval time.Duration

	// IgnoreInterfaces contains interface name patterns to ignore.
	// Common values: "lo", "docker*", "veth*", "br-*"
	IgnoreInterfaces []string
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() Config {
	return Config{
		DebounceInterval: 500 * time.Millisecond,
		// lo* matches lo (Linux) and lo0 (macOS)
		// utun* matches macOS auto-assigned TUN interfaces (utun0, utun1, ...)
		// The mesh TUN on macOS is always named utunN because macOS does not allow
		// custom TUN interface names; on Linux it is named tun-mesh0 (matched by tun-mesh*).
		IgnoreInterfaces: []string{"lo*", "docker*", "veth*", "br-*", "tun-mesh*", "utun*"},
	}
}

// New creates a new platform-specific network monitor.
func New(cfg Config) (Monitor, error) {
	if cfg.DebounceInterval == 0 {
		cfg.DebounceInterval = 500 * time.Millisecond
	}
	return newPlatformMonitor(cfg)
}
