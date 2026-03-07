package routing

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_AddRoute(t *testing.T) {
	r := NewRouter()

	// Add route for peer
	r.AddRoute("10.42.0.2", "peer1")
	r.AddRoute("10.42.0.3", "peer2")

	// Lookup
	peer, ok := r.Lookup(net.ParseIP("10.42.0.2"))
	assert.True(t, ok)
	assert.Equal(t, "peer1", peer)

	peer, ok = r.Lookup(net.ParseIP("10.42.0.3"))
	assert.True(t, ok)
	assert.Equal(t, "peer2", peer)
}

func TestRouter_RemoveRoute(t *testing.T) {
	r := NewRouter()

	r.AddRoute("10.42.0.2", "peer1")
	assert.Equal(t, 1, r.Count())

	r.RemoveRoute("10.42.0.2")
	assert.Equal(t, 0, r.Count())

	_, ok := r.Lookup(net.ParseIP("10.42.0.2"))
	assert.False(t, ok)
}

func TestRouter_LookupNotFound(t *testing.T) {
	r := NewRouter()

	_, ok := r.Lookup(net.ParseIP("10.42.0.99"))
	assert.False(t, ok)
}

func TestRouter_UpdateRoutes(t *testing.T) {
	r := NewRouter()

	// Add initial routes
	r.AddRoute("10.42.0.2", "peer1")
	r.AddRoute("10.42.0.3", "peer2")

	// Bulk update
	r.UpdateRoutes(map[string]string{
		"10.42.0.4": "peer3",
		"10.42.0.5": "peer4",
	})

	// Old routes gone
	_, ok := r.Lookup(net.ParseIP("10.42.0.2"))
	assert.False(t, ok)

	// New routes exist
	peer, ok := r.Lookup(net.ParseIP("10.42.0.4"))
	assert.True(t, ok)
	assert.Equal(t, "peer3", peer)
}

func TestRouter_ListRoutes(t *testing.T) {
	r := NewRouter()

	r.AddRoute("10.42.0.2", "peer1")
	r.AddRoute("10.42.0.3", "peer2")

	routes := r.ListRoutes()
	assert.Len(t, routes, 2)
	assert.Equal(t, "peer1", routes["10.42.0.2"])
	assert.Equal(t, "peer2", routes["10.42.0.3"])
}

func TestBuildIPv4Packet(t *testing.T) {
	src := net.ParseIP("10.42.0.1").To4()
	dst := net.ParseIP("10.42.0.2").To4()
	payload := []byte("hello")

	packet := BuildIPv4Packet(src, dst, ProtoUDP, payload)

	// Verify header
	assert.Equal(t, uint8(0x45), packet[0]) // Version 4, IHL 5
	assert.Equal(t, src, net.IP(packet[12:16]))
	assert.Equal(t, dst, net.IP(packet[16:20]))
	assert.Equal(t, uint8(ProtoUDP), packet[9])

	// Verify payload
	assert.Equal(t, payload, packet[20:])
}

func TestParseIPv4Packet(t *testing.T) {
	src := net.ParseIP("10.42.0.1").To4()
	dst := net.ParseIP("10.42.0.2").To4()
	payload := []byte("test data")

	packet := BuildIPv4Packet(src, dst, ProtoTCP, payload)

	info, err := ParseIPv4Packet(packet)
	require.NoError(t, err)

	assert.Equal(t, src, info.SrcIP.To4())
	assert.Equal(t, dst, info.DstIP.To4())
	assert.Equal(t, uint8(ProtoTCP), info.Protocol)
	assert.Equal(t, payload, info.Payload)
}

func TestParseIPv4Packet_TooShort(t *testing.T) {
	_, err := ParseIPv4Packet([]byte{0x45, 0x00})
	assert.Error(t, err)
}

func TestParseIPv4Packet_NotIPv4(t *testing.T) {
	packet := make([]byte, 20)
	packet[0] = 0x60 // IPv6 version
	_, err := ParseIPv4Packet(packet)
	assert.Error(t, err)
}

func TestPacketBuffer(t *testing.T) {
	buf := NewPacketBuffer(1500)

	data := []byte("test packet data")
	copy(buf.Data(), data)
	buf.SetLength(len(data))

	assert.Equal(t, len(data), buf.Length())
	assert.Equal(t, data, buf.Bytes())
}

func TestPacketBufferPool(t *testing.T) {
	pool := NewPacketBufferPool(1500)

	buf1 := pool.Get()
	assert.NotNil(t, buf1)
	assert.Equal(t, 1500, cap(buf1.Data()))

	buf2 := pool.Get()
	assert.NotNil(t, buf2)

	pool.Put(buf1)
	pool.Put(buf2)

	// Should get recycled buffers
	buf3 := pool.Get()
	assert.NotNil(t, buf3)
}

func TestChecksumIPv4(t *testing.T) {
	// Create a simple IPv4 header
	header := []byte{
		0x45, 0x00, 0x00, 0x73, 0x00, 0x00, 0x40, 0x00,
		0x40, 0x11, 0x00, 0x00, // Checksum will be calculated
		0xc0, 0xa8, 0x00, 0x01, // 192.168.0.1
		0xc0, 0xa8, 0x00, 0xc7, // 192.168.0.199
	}

	checksum := CalculateIPv4Checksum(header)
	assert.NotEqual(t, uint16(0), checksum)

	// Set checksum and verify
	header[10] = byte(checksum >> 8)
	header[11] = byte(checksum)

	// Recalculate should be 0 (or 0xFFFF)
	verify := CalculateIPv4Checksum(header)
	assert.True(t, verify == 0 || verify == 0xFFFF)
}

// BenchmarkRouterLookup benchmarks the router lookup with binary IP keys.
func BenchmarkRouterLookup(b *testing.B) {
	r := NewRouter()

	// Add 100 routes
	for i := 0; i < 100; i++ {
		ip := net.IPv4(10, 99, byte(i/256), byte(i%256)).String()
		r.AddRoute(ip, "peer"+string(rune(i)))
	}

	lookupIP := net.ParseIP("10.42.0.50")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Lookup(lookupIP)
	}
}

// BenchmarkForwardPacket benchmarks packet forwarding with buffer pooling.
func BenchmarkForwardPacket(b *testing.B) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnelMgr.Add("peer1", tunnel)

	fwd := NewForwarder(router, tunnelMgr)

	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	payload := make([]byte, 1000) // 1KB payload
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, payload)

	b.ResetTimer()
	b.SetBytes(int64(len(packet)))

	for i := 0; i < b.N; i++ {
		_ = fwd.ForwardPacket(packet)
	}
}

// BenchmarkForwardPacketZeroCopy benchmarks zero-copy packet forwarding.
func BenchmarkForwardPacketZeroCopy(b *testing.B) {
	router := NewRouter()
	router.AddRoute("10.42.0.2", "peer1")

	tunnelMgr := NewMockTunnelManager()
	tunnel := newMockTunnel()
	tunnelMgr.Add("peer1", tunnel)

	fwd := NewForwarder(router, tunnelMgr)

	srcIP := net.ParseIP("10.42.0.1").To4()
	dstIP := net.ParseIP("10.42.0.2").To4()
	payload := make([]byte, 1000) // 1KB payload
	packet := BuildIPv4Packet(srcIP, dstIP, ProtoUDP, payload)

	// Create zero-copy buffer and copy packet into it (simulating TUN read)
	zcPool := NewZeroCopyBufferPool(FrameHeaderSize, MaxPacketSize)
	zcBuf := zcPool.Get()
	copy(zcBuf.DataSlice(), packet)
	zcBuf.SetLength(len(packet))

	b.ResetTimer()
	b.SetBytes(int64(len(packet)))

	for i := 0; i < b.N; i++ {
		_ = fwd.ForwardPacketZeroCopy(zcBuf, len(packet))
	}

	zcPool.Put(zcBuf)
}

// BenchmarkZeroCopyBuffer benchmarks zero-copy buffer frame creation.
func BenchmarkZeroCopyBuffer(b *testing.B) {
	zcBuf := NewZeroCopyBuffer(FrameHeaderSize, 1500)
	payload := make([]byte, 1400)
	copy(zcBuf.DataSlice(), payload)

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		_ = zcBuf.Frame(len(payload))
	}
}

// =============================================================================
// Router Concurrent Read/Write Tests (Test Gap 5)
// =============================================================================

// TestRouter_ConcurrentUpdateAndLookup verifies that concurrent UpdateRoutes and
// Lookup calls do not race. Run with -race to detect data races.
func TestRouter_ConcurrentUpdateAndLookup(t *testing.T) {
	r := NewRouter()

	// Pre-populate with some routes
	initial := map[string]string{}
	for i := 0; i < 10; i++ {
		ip := net.IPv4(10, 0, 0, byte(i)).String()
		initial[ip] = "peer" + string(rune('a'+i))
	}
	r.UpdateRoutes(initial)

	const goroutines = 8
	const iterations = 200
	done := make(chan struct{}, goroutines*2)

	// Writers: repeatedly bulk-update routes
	for w := 0; w < goroutines; w++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			routes := map[string]string{}
			for i := 0; i < 10; i++ {
				ip := net.IPv4(10, byte(id), 0, byte(i)).String()
				routes[ip] = "peer"
			}
			for i := 0; i < iterations; i++ {
				r.UpdateRoutes(routes)
			}
		}(w)
	}

	// Readers: repeatedly look up IPs
	for rd := 0; rd < goroutines; rd++ {
		go func() {
			defer func() { done <- struct{}{} }()
			ip := net.ParseIP("10.0.0.1")
			for i := 0; i < iterations; i++ {
				_, _ = r.Lookup(ip)
			}
		}()
	}

	for i := 0; i < goroutines*2; i++ {
		<-done
	}
}

// TestRouter_ConcurrentAddRemoveAndLookup exercises concurrent AddRoute / RemoveRoute / Lookup.
func TestRouter_ConcurrentAddRemoveAndLookup(t *testing.T) {
	r := NewRouter()

	const goroutines = 6
	const iterations = 100
	done := make(chan struct{}, goroutines*3)

	// Adders
	for a := 0; a < goroutines; a++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < iterations; i++ {
				ip := net.IPv4(10, byte(id), 0, byte(i%256)).String()
				r.AddRoute(ip, "peer")
			}
		}(a)
	}

	// Removers
	for rm := 0; rm < goroutines; rm++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < iterations; i++ {
				ip := net.IPv4(10, byte(id), 0, byte(i%256)).String()
				r.RemoveRoute(ip)
			}
		}(rm)
	}

	// Readers
	for rd := 0; rd < goroutines; rd++ {
		go func() {
			defer func() { done <- struct{}{} }()
			ip := net.ParseIP("10.0.0.0")
			for i := 0; i < iterations; i++ {
				_, _ = r.Lookup(ip)
			}
		}()
	}

	for i := 0; i < goroutines*3; i++ {
		<-done
	}
}

// BenchmarkRouterConcurrentLookup benchmarks concurrent read access via Lookup.
func BenchmarkRouterConcurrentLookup(b *testing.B) {
	r := NewRouter()
	for i := 0; i < 100; i++ {
		ip := net.IPv4(10, 42, byte(i/256), byte(i%256)).String()
		r.AddRoute(ip, "peer"+string(rune(i)))
	}

	lookupIP := net.ParseIP("10.42.0.50")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = r.Lookup(lookupIP)
		}
	})
}
