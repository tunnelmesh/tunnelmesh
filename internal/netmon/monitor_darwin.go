//go:build darwin

package netmon

import (
	"context"
	"net"
	"path/filepath"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Route message types for Darwin
const (
	rtmNewAddr = 0xc // RTM_NEWADDR
	rtmDelAddr = 0xd // RTM_DELADDR
	rtmIfInfo  = 0xe // RTM_IFINFO
)

type darwinMonitor struct {
	cfg    Config
	fd     int
	events chan Event
}

func newPlatformMonitor(cfg Config) (Monitor, error) {
	// Create a routing socket to receive network change notifications
	fd, err := unix.Socket(unix.AF_ROUTE, unix.SOCK_RAW, 0)
	if err != nil {
		return nil, err
	}

	return &darwinMonitor{
		cfg:    cfg,
		fd:     fd,
		events: make(chan Event, 16),
	}, nil
}

func (m *darwinMonitor) Start(ctx context.Context) (<-chan Event, error) {
	go m.readLoop(ctx)

	debouncer := NewDebouncer(m.events, m.cfg.DebounceInterval)
	return debouncer.Run(ctx), nil
}

func (m *darwinMonitor) readLoop(ctx context.Context) {
	defer close(m.events)

	buf := make([]byte, 2048)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set read timeout for periodic context checks
		tv := unix.Timeval{Sec: 1}
		_ = unix.SetsockoptTimeval(m.fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)

		n, err := unix.Read(m.fd, buf)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EINTR || err == unix.EWOULDBLOCK {
				continue
			}
			return
		}

		if n < 4 {
			continue
		}

		event := m.parseRouteMessage(buf[:n])
		if event != nil && !m.shouldIgnore(event) {
			select {
			case m.events <- *event:
			default: // Drop if channel full
			}
		}
	}
}

// rtMsghdr represents the route message header on Darwin
type rtMsghdr struct {
	Msglen  uint16
	Version uint8
	Type    uint8
	Index   uint16
	Flags   int32
	Addrs   int32
	Pid     int32
	Seq     int32
	Errno   int32
	Fmask   int32
	Inits   uint32
}

func (m *darwinMonitor) parseRouteMessage(buf []byte) *Event {
	if len(buf) < int(unsafe.Sizeof(rtMsghdr{})) {
		return nil
	}

	hdr := (*rtMsghdr)(unsafe.Pointer(&buf[0]))
	var changeType ChangeType

	switch hdr.Type {
	case rtmNewAddr:
		changeType = ChangeAddressAdded
	case rtmDelAddr:
		changeType = ChangeAddressRemoved
	case rtmIfInfo:
		changeType = ChangeInterfaceUp
	default:
		return nil
	}

	// Try to get interface name from index
	var ifName string
	if hdr.Index > 0 {
		iface, err := interfaceByIndex(int(hdr.Index))
		if err == nil {
			ifName = iface
		}
	}

	return &Event{
		Type:      changeType,
		Interface: ifName,
		Timestamp: time.Now(),
	}
}

func interfaceByIndex(index int) (string, error) {
	iface, err := net.InterfaceByIndex(index)
	if err != nil {
		return "", err
	}
	return iface.Name, nil
}

func (m *darwinMonitor) shouldIgnore(event *Event) bool {
	if event.Interface == "" {
		return true // InterfaceByIndex lookup failed — ignore unidentifiable event
	}

	for _, pattern := range m.cfg.IgnoreInterfaces {
		matched, _ := filepath.Match(pattern, event.Interface)
		if matched {
			return true
		}
	}

	return false
}

func (m *darwinMonitor) Close() error {
	return unix.Close(m.fd)
}
