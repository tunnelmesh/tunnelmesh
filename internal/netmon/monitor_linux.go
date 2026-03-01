//go:build linux

package netmon

import (
	"context"
	"net"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type linuxMonitor struct {
	cfg    Config
	fd     int
	events chan Event
}

func newPlatformMonitor(cfg Config) (Monitor, error) {
	// Create netlink socket for route messages
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_DGRAM, unix.NETLINK_ROUTE)
	if err != nil {
		return nil, err
	}

	// Bind to multicast groups for link and address changes
	addr := &unix.SockaddrNetlink{
		Family: unix.AF_NETLINK,
		Groups: unix.RTMGRP_LINK | unix.RTMGRP_IPV4_IFADDR,
	}
	if err := unix.Bind(fd, addr); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}

	return &linuxMonitor{
		cfg:    cfg,
		fd:     fd,
		events: make(chan Event, 16),
	}, nil
}

func (m *linuxMonitor) Start(ctx context.Context) (<-chan Event, error) {
	go m.readLoop(ctx)

	debouncer := NewDebouncer(m.events, m.cfg.DebounceInterval)
	return debouncer.Run(ctx), nil
}

func (m *linuxMonitor) readLoop(ctx context.Context) {
	defer close(m.events)

	buf := make([]byte, 4096)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set read timeout for periodic context checks
		tv := unix.Timeval{Sec: 1}
		_ = unix.SetsockoptTimeval(m.fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)

		n, _, err := unix.Recvfrom(m.fd, buf, 0)
		if err != nil {
			//nolint:errorlint // Unix errors are sentinel errors, == is correct and more efficient than errors.Is
			if err == unix.EAGAIN || err == unix.EINTR || err == unix.EWOULDBLOCK {
				continue
			}
			return
		}

		// Parse netlink messages
		msgs, err := syscall.ParseNetlinkMessage(buf[:n])
		if err != nil {
			continue
		}

		for _, msg := range msgs {
			event := m.parseNetlinkMessage(&msg)
			if event != nil && !m.shouldIgnore(event) {
				select {
				case m.events <- *event:
				default: // Drop if channel full
				}
			}
		}
	}
}

func (m *linuxMonitor) parseNetlinkMessage(msg *syscall.NetlinkMessage) *Event {
	var changeType ChangeType
	var ifName string

	switch msg.Header.Type {
	case syscall.RTM_NEWADDR:
		changeType = ChangeAddressAdded
	case syscall.RTM_DELADDR:
		changeType = ChangeAddressRemoved
	case syscall.RTM_NEWLINK:
		changeType = ChangeInterfaceUp
	case syscall.RTM_DELLINK:
		changeType = ChangeInterfaceDown
	default:
		return nil
	}

	// Try to extract interface name from message attributes
	if msg.Header.Type == syscall.RTM_NEWLINK || msg.Header.Type == syscall.RTM_DELLINK {
		// Parse IfInfomsg and attributes
		if len(msg.Data) >= syscall.SizeofIfInfomsg {
			attrs, err := syscall.ParseNetlinkRouteAttr(msg)
			if err == nil {
				for _, attr := range attrs {
					if attr.Attr.Type == syscall.IFLA_IFNAME {
						ifName = string(attr.Value[:len(attr.Value)-1]) // Remove null terminator
						break
					}
				}
			}
		}
	} else if msg.Header.Type == syscall.RTM_NEWADDR || msg.Header.Type == syscall.RTM_DELADDR {
		// Parse IfAddrmsg and attributes
		if len(msg.Data) >= syscall.SizeofIfAddrmsg {
			attrs, err := syscall.ParseNetlinkRouteAttr(msg)
			if err == nil {
				for _, attr := range attrs {
					if attr.Attr.Type == syscall.IFA_LABEL {
						ifName = string(attr.Value[:len(attr.Value)-1])
						break
					}
				}
			}

			// Fallback: if IFA_LABEL absent (e.g., IPv6 events), resolve via interface index.
			// IfAddrmsg layout: [Family(1)][Prefixlen(1)][Flags(1)][Scope(1)][Index(4)]
			if ifName == "" && len(msg.Data) >= 8 {
				idx := int(uint32(msg.Data[4]) |
					uint32(msg.Data[5])<<8 |
					uint32(msg.Data[6])<<16 |
					uint32(msg.Data[7])<<24)
				if iface, err := net.InterfaceByIndex(idx); err == nil {
					ifName = iface.Name
				}
			}
		}
	}

	return &Event{
		Type:      changeType,
		Interface: ifName,
		Timestamp: time.Now(),
	}
}

func (m *linuxMonitor) shouldIgnore(event *Event) bool {
	if event.Interface == "" {
		return true // Unknown interface — safe to ignore
	}

	for _, pattern := range m.cfg.IgnoreInterfaces {
		matched, _ := filepath.Match(pattern, event.Interface)
		if matched {
			return true
		}
	}

	return false
}

func (m *linuxMonitor) Close() error {
	return unix.Close(m.fd)
}
