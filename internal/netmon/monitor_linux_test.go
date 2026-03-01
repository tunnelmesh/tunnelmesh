//go:build linux

package netmon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLinuxShouldIgnoreEmptyInterface(t *testing.T) {
	m := &linuxMonitor{cfg: DefaultConfig()}

	event := &Event{
		Type:      ChangeAddressAdded,
		Interface: "",
		Timestamp: time.Now(),
	}

	assert.True(t, m.shouldIgnore(event), "empty interface should be ignored on Linux")
}

func TestLinuxShouldIgnoreKnownPatterns(t *testing.T) {
	m := &linuxMonitor{cfg: DefaultConfig()}

	tests := []struct {
		iface  string
		ignore bool
	}{
		{"lo", true},
		{"docker0", true},
		{"veth1a2b3c", true},
		{"tun-mesh0", true},
		{"eth0", false},
		{"ens3", false},
		{"wlan0", false},
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
