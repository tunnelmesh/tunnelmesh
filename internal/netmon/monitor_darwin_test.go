//go:build darwin

package netmon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDarwinShouldIgnoreEmptyInterface(t *testing.T) {
	m := &darwinMonitor{cfg: DefaultConfig()}

	event := &Event{
		Type:      ChangeAddressAdded,
		Interface: "",
		Timestamp: time.Now(),
	}

	assert.True(t, m.shouldIgnore(event), "empty interface should be ignored on Darwin")
}

func TestDarwinShouldIgnoreKnownPatterns(t *testing.T) {
	m := &darwinMonitor{cfg: DefaultConfig()}

	tests := []struct {
		iface  string
		ignore bool
	}{
		{"docker0", true},
		{"veth1a2b3c", true},
		{"tun-mesh0", true},
		{"en0", false},
		{"en1", false},
		{"utun3", true}, // macOS auto-assigns utunN names for all TUN devices including mesh
		{"utun0", true}, // all utunN variants ignored
		{"lo0", true},   // lo* matches lo0 (macOS loopback)
		{"lo", true},    // lo* also matches lo (Linux loopback)
	}

	for _, tt := range tests {
		t.Run(tt.iface, func(t *testing.T) {
			event := &Event{
				Type:      ChangeAddressAdded,
				Interface: tt.iface,
				Timestamp: time.Now(),
			}
			assert.Equal(t, tt.ignore, m.shouldIgnore(event))
		})
	}
}
