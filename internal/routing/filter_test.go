package routing

import (
	"net"
	"sync"
	"testing"
	"time"
)

func TestNewPacketFilter(t *testing.T) {
	t.Run("default deny mode", func(t *testing.T) {
		f := NewPacketFilter(true)
		if !f.IsDefaultDeny() {
			t.Error("expected default deny to be true")
		}
		if f.RuleCount() != 0 {
			t.Errorf("expected 0 rules, got %d", f.RuleCount())
		}
	})

	t.Run("default allow mode", func(t *testing.T) {
		f := NewPacketFilter(false)
		if f.IsDefaultDeny() {
			t.Error("expected default deny to be false")
		}
	})
}

func TestPacketFilter_SetCoordinatorRules(t *testing.T) {
	f := NewPacketFilter(true)

	rules := []FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
		{Port: 80, Protocol: ProtoTCP, Action: ActionAllow},
	}
	f.SetCoordinatorRules(rules)

	if f.RuleCount() != 2 {
		t.Errorf("expected 2 rules, got %d", f.RuleCount())
	}

	// Replace with new rules
	newRules := []FilterRule{
		{Port: 443, Protocol: ProtoTCP, Action: ActionAllow},
	}
	f.SetCoordinatorRules(newRules)

	if f.RuleCount() != 1 {
		t.Errorf("expected 1 rule after replacement, got %d", f.RuleCount())
	}
}

func TestPacketFilter_SetPeerConfigRules(t *testing.T) {
	f := NewPacketFilter(true)

	rules := []FilterRule{
		{Port: 8080, Protocol: ProtoTCP, Action: ActionAllow},
	}
	f.SetPeerConfigRules(rules)

	if f.RuleCount() != 1 {
		t.Errorf("expected 1 rule, got %d", f.RuleCount())
	}
}

func TestPacketFilter_SetServiceRules(t *testing.T) {
	f := NewPacketFilter(true)

	// Add service rules (like coordinator service ports)
	rules := []FilterRule{
		{Port: 9443, Protocol: ProtoTCP, Action: ActionAllow},
		{Port: 443, Protocol: ProtoTCP, Action: ActionAllow},
	}
	f.SetServiceRules(rules)

	if f.RuleCount() != 2 {
		t.Errorf("expected 2 rules, got %d", f.RuleCount())
	}

	// Verify service rules show in list
	allRules := f.ListRules()
	serviceCount := 0
	for _, r := range allRules {
		if r.Source == SourceService {
			serviceCount++
		}
	}
	if serviceCount != 2 {
		t.Errorf("expected 2 service rules, got %d", serviceCount)
	}

	// Replace with new rules
	newRules := []FilterRule{
		{Port: 8080, Protocol: ProtoTCP, Action: ActionAllow},
	}
	f.SetServiceRules(newRules)

	if f.RuleCount() != 1 {
		t.Errorf("expected 1 rule after replacement, got %d", f.RuleCount())
	}

	// Verify service rules allow traffic (SYN to port 8080)
	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")
	packet := buildTCPPacket(src, dst, 8080)
	if f.ShouldDrop(packet) {
		t.Error("expected service rule to allow SYN to port 8080")
	}
}

func TestPacketFilter_TemporaryRules(t *testing.T) {
	f := NewPacketFilter(true)

	// Add rule
	f.AddTemporaryRule(FilterRule{Port: 3000, Protocol: ProtoTCP, Action: ActionAllow})
	if f.RuleCount() != 1 {
		t.Errorf("expected 1 rule, got %d", f.RuleCount())
	}

	// Add another
	f.AddTemporaryRule(FilterRule{Port: 3001, Protocol: ProtoTCP, Action: ActionAllow})
	if f.RuleCount() != 2 {
		t.Errorf("expected 2 rules, got %d", f.RuleCount())
	}

	// Remove first
	f.RemoveTemporaryRule(3000, ProtoTCP)
	if f.RuleCount() != 1 {
		t.Errorf("expected 1 rule after removal, got %d", f.RuleCount())
	}

	// Clear all
	f.ClearTemporaryRules()
	if f.RuleCount() != 0 {
		t.Errorf("expected 0 rules after clear, got %d", f.RuleCount())
	}
}

func TestPacketFilter_ExpiredRules(t *testing.T) {
	f := NewPacketFilter(true)

	// Add expired rule
	f.AddTemporaryRule(FilterRule{
		Port:     4000,
		Protocol: ProtoTCP,
		Action:   ActionAllow,
		Expires:  time.Now().Unix() - 10, // Expired 10 seconds ago
	})

	// Expired rules should not be counted
	if f.RuleCount() != 0 {
		t.Errorf("expected 0 rules (expired), got %d", f.RuleCount())
	}

	// Add non-expired rule
	f.AddTemporaryRule(FilterRule{
		Port:     4001,
		Protocol: ProtoTCP,
		Action:   ActionAllow,
		Expires:  time.Now().Unix() + 3600, // Expires in 1 hour
	})

	if f.RuleCount() != 1 {
		t.Errorf("expected 1 rule, got %d", f.RuleCount())
	}
}

func TestPacketFilter_ListRules(t *testing.T) {
	f := NewPacketFilter(true)

	f.SetCoordinatorRules([]FilterRule{{Port: 22, Protocol: ProtoTCP, Action: ActionAllow}})
	f.SetPeerConfigRules([]FilterRule{{Port: 80, Protocol: ProtoTCP, Action: ActionAllow}})
	f.AddTemporaryRule(FilterRule{Port: 443, Protocol: ProtoTCP, Action: ActionAllow})

	rules := f.ListRules()
	if len(rules) != 3 {
		t.Errorf("expected 3 rules, got %d", len(rules))
	}

	// Check sources
	sources := make(map[RuleSource]int)
	for _, r := range rules {
		sources[r.Source]++
	}

	if sources[SourceCoordinator] != 1 {
		t.Errorf("expected 1 coordinator rule, got %d", sources[SourceCoordinator])
	}
	if sources[SourcePeerConfig] != 1 {
		t.Errorf("expected 1 peer config rule, got %d", sources[SourcePeerConfig])
	}
	if sources[SourceTemporary] != 1 {
		t.Errorf("expected 1 temporary rule, got %d", sources[SourceTemporary])
	}
}

// buildTCPPacket creates a minimal TCP SYN packet for testing (new connection attempt).
// The source port is hardcoded to 12345 for simplicity.
func buildTCPPacket(srcIP, dstIP net.IP, dstPort uint16) []byte {
	return buildTCPPacketWithFlags(srcIP, dstIP, 12345, dstPort, 0x02) // SYN flag
}

// buildTCPPacketWithFlags creates a TCP packet with specific flags for testing.
// Common flags: SYN=0x02, ACK=0x10, SYN+ACK=0x12, FIN=0x01
func buildTCPPacketWithFlags(srcIP, dstIP net.IP, srcPort, dstPort uint16, flags byte) []byte {
	// 20 bytes IP header + 20 bytes TCP header (minimum)
	packet := make([]byte, 40)

	// IP header
	packet[0] = 0x45     // Version 4, IHL 5 (20 bytes)
	packet[9] = ProtoTCP // Protocol
	copy(packet[12:16], srcIP.To4())
	copy(packet[16:20], dstIP.To4())

	// TCP header (ports are at offset 0 and 2 of TCP header)
	packet[20] = byte(srcPort >> 8)
	packet[21] = byte(srcPort)
	packet[22] = byte(dstPort >> 8)
	packet[23] = byte(dstPort)
	// Data offset (5 = 20 bytes header) and reserved
	packet[32] = 0x50
	// TCP flags at offset 13 of TCP header (offset 33 of packet)
	packet[33] = flags

	return packet
}

// buildUDPPacket creates a minimal UDP packet for testing.
func buildUDPPacket(srcIP, dstIP net.IP, srcPort, dstPort uint16) []byte {
	// 20 bytes IP header + 8 bytes UDP header
	packet := make([]byte, 28)

	// IP header
	packet[0] = 0x45     // Version 4, IHL 5 (20 bytes)
	packet[9] = ProtoUDP // Protocol
	copy(packet[12:16], srcIP.To4())
	copy(packet[16:20], dstIP.To4())

	// UDP header (ports are at offset 0 and 2 of UDP header)
	packet[20] = byte(srcPort >> 8)
	packet[21] = byte(srcPort)
	packet[22] = byte(dstPort >> 8)
	packet[23] = byte(dstPort)

	return packet
}

func TestPacketFilter_ShouldDrop_DefaultDeny(t *testing.T) {
	f := NewPacketFilter(true) // Default deny

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")

	// No rules - should drop by default
	packet := buildTCPPacket(src, dst, 22)
	if !f.ShouldDrop(packet) {
		t.Error("expected packet to be dropped (default deny, no rules)")
	}

	// Add allow rule for port 22
	f.SetPeerConfigRules([]FilterRule{{Port: 22, Protocol: ProtoTCP, Action: ActionAllow}})

	if f.ShouldDrop(packet) {
		t.Error("expected packet to be allowed (port 22 allowed)")
	}

	// Packet to different port should still be dropped
	packet2 := buildTCPPacket(src, dst, 80)
	if !f.ShouldDrop(packet2) {
		t.Error("expected packet to port 80 to be dropped")
	}
}

func TestPacketFilter_ShouldDrop_DefaultAllow(t *testing.T) {
	f := NewPacketFilter(false) // Default allow

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")

	// No rules - should allow by default
	packet := buildTCPPacket(src, dst, 22)
	if f.ShouldDrop(packet) {
		t.Error("expected packet to be allowed (default allow)")
	}

	// Add deny rule for port 22
	f.SetPeerConfigRules([]FilterRule{{Port: 22, Protocol: ProtoTCP, Action: ActionDeny}})

	if !f.ShouldDrop(packet) {
		t.Error("expected packet to be dropped (port 22 denied)")
	}
}

func TestPacketFilter_ShouldDrop_UDP(t *testing.T) {
	f := NewPacketFilter(true)

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")

	// Allow UDP port 53
	f.SetPeerConfigRules([]FilterRule{{Port: 53, Protocol: ProtoUDP, Action: ActionAllow}})

	// UDP to port 53 should be allowed
	packet := buildUDPPacket(src, dst, 12345, 53)
	if f.ShouldDrop(packet) {
		t.Error("expected UDP packet to port 53 to be allowed")
	}

	// TCP to port 53 should be dropped (only UDP allowed)
	tcpPacket := buildTCPPacket(src, dst, 53)
	if !f.ShouldDrop(tcpPacket) {
		t.Error("expected TCP packet to port 53 to be dropped")
	}
}

func TestPacketFilter_ShouldDrop_ICMP(t *testing.T) {
	f := NewPacketFilter(true)

	// ICMP packet (protocol 1)
	packet := make([]byte, 28)
	packet[0] = 0x45      // Version 4, IHL 5
	packet[9] = ProtoICMP // ICMP

	// ICMP should not be filtered (allow by default)
	if f.ShouldDrop(packet) {
		t.Error("expected ICMP to be allowed (not filtered)")
	}
}

func TestPacketFilter_MostRestrictiveWins(t *testing.T) {
	f := NewPacketFilter(true)

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")

	// Coordinator allows port 22
	f.SetCoordinatorRules([]FilterRule{{Port: 22, Protocol: ProtoTCP, Action: ActionAllow}})

	packet := buildTCPPacket(src, dst, 22)

	// Should be allowed (coordinator allows)
	if f.ShouldDrop(packet) {
		t.Error("expected packet to be allowed (coordinator allows)")
	}

	// Peer config denies port 22 - should override coordinator allow
	f.SetPeerConfigRules([]FilterRule{{Port: 22, Protocol: ProtoTCP, Action: ActionDeny}})

	if !f.ShouldDrop(packet) {
		t.Error("expected packet to be dropped (peer config deny overrides coordinator allow)")
	}

	// Temporary rule allows it - but deny still wins
	f.AddTemporaryRule(FilterRule{Port: 22, Protocol: ProtoTCP, Action: ActionAllow})

	if !f.ShouldDrop(packet) {
		t.Error("expected packet to be dropped (deny wins over allow from any layer)")
	}

	// Remove deny rule from peer config
	f.SetPeerConfigRules(nil)

	// Now should be allowed (coordinator + temporary both allow)
	if f.ShouldDrop(packet) {
		t.Error("expected packet to be allowed after removing deny rule")
	}
}

func TestPacketFilter_ShortPacket(t *testing.T) {
	f := NewPacketFilter(true)

	// Packet too short (less than 20 bytes)
	packet := make([]byte, 10)
	if f.ShouldDrop(packet) {
		t.Error("expected short packet to not be dropped (handled elsewhere)")
	}
}

func TestPacketFilter_MalformedPacket(t *testing.T) {
	f := NewPacketFilter(true)

	// Packet with invalid IHL (claims 60 bytes header but packet is only 28 bytes)
	packet := make([]byte, 28)
	packet[0] = 0x4F // Version 4, IHL 15 (60 bytes) - but packet is too short
	packet[9] = ProtoTCP

	if f.ShouldDrop(packet) {
		t.Error("expected malformed packet to not be dropped (handled elsewhere)")
	}
}

func TestFilterRule_IsExpired(t *testing.T) {
	tests := []struct {
		name    string
		expires int64
		want    bool
	}{
		{"zero (permanent)", 0, false},
		{"future", time.Now().Unix() + 3600, false},
		{"past", time.Now().Unix() - 10, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := FilterRule{Expires: tt.expires}
			if got := r.IsExpired(); got != tt.want {
				t.Errorf("IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFilterAction_String(t *testing.T) {
	if ActionAllow.String() != "allow" {
		t.Errorf("expected 'allow', got %s", ActionAllow.String())
	}
	if ActionDeny.String() != "deny" {
		t.Errorf("expected 'deny', got %s", ActionDeny.String())
	}
}

func TestParseFilterAction(t *testing.T) {
	if ParseFilterAction("allow") != ActionAllow {
		t.Error("expected ActionAllow")
	}
	if ParseFilterAction("deny") != ActionDeny {
		t.Error("expected ActionDeny")
	}
	if ParseFilterAction("invalid") != ActionDeny {
		t.Error("expected ActionDeny for invalid input")
	}
}

func TestRuleSource_String(t *testing.T) {
	tests := []struct {
		source RuleSource
		want   string
	}{
		{SourceCoordinator, "coordinator"},
		{SourcePeerConfig, "config"},
		{SourceTemporary, "temporary"},
		{RuleSource(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.source.String(); got != tt.want {
			t.Errorf("RuleSource(%d).String() = %s, want %s", tt.source, got, tt.want)
		}
	}
}

func TestPacketFilter_CheckPacket(t *testing.T) {
	f := NewPacketFilter(true)

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")

	// TCP packet to blocked port
	tcpPacket := buildTCPPacket(src, dst, 22)
	result := f.CheckPacket(tcpPacket)
	if !result.Drop {
		t.Error("expected TCP packet to be dropped (default deny)")
	}
	if result.Protocol != ProtoTCP {
		t.Errorf("expected protocol=TCP (6), got %d", result.Protocol)
	}

	// UDP packet to blocked port
	udpPacket := buildUDPPacket(src, dst, 12345, 53)
	result = f.CheckPacket(udpPacket)
	if !result.Drop {
		t.Error("expected UDP packet to be dropped (default deny)")
	}
	if result.Protocol != ProtoUDP {
		t.Errorf("expected protocol=UDP (17), got %d", result.Protocol)
	}

	// Add allow rule
	f.SetPeerConfigRules([]FilterRule{{Port: 22, Protocol: ProtoTCP, Action: ActionAllow}})
	result = f.CheckPacket(tcpPacket)
	if result.Drop {
		t.Error("expected TCP packet to be allowed")
	}
	if result.Protocol != ProtoTCP {
		t.Errorf("expected protocol=TCP (6), got %d", result.Protocol)
	}

	// ICMP packet should not be dropped
	icmpPacket := make([]byte, 28)
	icmpPacket[0] = 0x45
	icmpPacket[9] = ProtoICMP
	result = f.CheckPacket(icmpPacket)
	if result.Drop {
		t.Error("expected ICMP to not be dropped")
	}
	if result.Protocol != 0 {
		t.Errorf("expected protocol=0 for non-filtered, got %d", result.Protocol)
	}
}

func TestPacketFilter_RuleCountBySource(t *testing.T) {
	f := NewPacketFilter(true)

	// Initially empty
	counts := f.RuleCountBySource()
	if counts.Coordinator != 0 || counts.PeerConfig != 0 || counts.Temporary != 0 {
		t.Error("expected all counts to be 0")
	}

	// Add coordinator rules
	f.SetCoordinatorRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
		{Port: 80, Protocol: ProtoTCP, Action: ActionAllow},
	})

	counts = f.RuleCountBySource()
	if counts.Coordinator != 2 {
		t.Errorf("expected Coordinator=2, got %d", counts.Coordinator)
	}

	// Add peer config rules
	f.SetPeerConfigRules([]FilterRule{
		{Port: 443, Protocol: ProtoTCP, Action: ActionAllow},
	})

	counts = f.RuleCountBySource()
	if counts.PeerConfig != 1 {
		t.Errorf("expected PeerConfig=1, got %d", counts.PeerConfig)
	}

	// Add temporary rules
	f.AddTemporaryRule(FilterRule{Port: 8080, Protocol: ProtoTCP, Action: ActionAllow})
	f.AddTemporaryRule(FilterRule{Port: 3000, Protocol: ProtoTCP, Action: ActionAllow})
	f.AddTemporaryRule(FilterRule{Port: 5000, Protocol: ProtoTCP, Action: ActionAllow})

	counts = f.RuleCountBySource()
	if counts.Temporary != 3 {
		t.Errorf("expected Temporary=3, got %d", counts.Temporary)
	}

	// Total should be sum
	total := counts.Coordinator + counts.PeerConfig + counts.Temporary
	if total != 6 {
		t.Errorf("expected total=6, got %d", total)
	}
}

func TestPacketFilter_IsDefaultDeny(t *testing.T) {
	// Default deny mode
	f1 := NewPacketFilter(true)
	if !f1.IsDefaultDeny() {
		t.Error("expected IsDefaultDeny()=true")
	}

	// Default allow mode
	f2 := NewPacketFilter(false)
	if f2.IsDefaultDeny() {
		t.Error("expected IsDefaultDeny()=false")
	}
}

// Tests for per-peer filtering

func TestPacketFilter_PeerSpecificRules(t *testing.T) {
	f := NewPacketFilter(true)

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")
	packet := buildTCPPacket(src, dst, 22)

	// Add global allow rule for port 22
	f.SetCoordinatorRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
	})

	// Without peer context, should be allowed (global rule matches)
	result := f.CheckPacketFromPeer(packet, "")
	if result.Drop {
		t.Error("expected packet to be allowed (global rule)")
	}

	// Add peer-specific deny rule for port 22 from "untrusted"
	f.AddTemporaryRule(FilterRule{
		Port:       22,
		Protocol:   ProtoTCP,
		Action:     ActionDeny,
		SourcePeer: "untrusted",
	})

	// Packet from "untrusted" should be denied (peer-specific deny)
	result = f.CheckPacketFromPeer(packet, "untrusted")
	if !result.Drop {
		t.Error("expected packet from 'untrusted' to be dropped")
	}
	if result.SourcePeer != "untrusted" {
		t.Errorf("expected SourcePeer='untrusted', got %s", result.SourcePeer)
	}

	// Packet from "trusted" should still be allowed (no specific rule, uses global)
	result = f.CheckPacketFromPeer(packet, "trusted")
	if result.Drop {
		t.Error("expected packet from 'trusted' to be allowed")
	}
}

func TestPacketFilter_PeerSpecificAllow(t *testing.T) {
	f := NewPacketFilter(true) // Default deny

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")
	packet := buildTCPPacket(src, dst, 22)

	// No global rule - should be denied by default
	result := f.CheckPacketFromPeer(packet, "peer1")
	if !result.Drop {
		t.Error("expected packet to be dropped (default deny)")
	}

	// Add peer-specific allow rule for port 22 from "trusted"
	f.AddTemporaryRule(FilterRule{
		Port:       22,
		Protocol:   ProtoTCP,
		Action:     ActionAllow,
		SourcePeer: "trusted",
	})

	// Packet from "trusted" should be allowed
	result = f.CheckPacketFromPeer(packet, "trusted")
	if result.Drop {
		t.Error("expected packet from 'trusted' to be allowed")
	}

	// Packet from other peer should still be denied
	result = f.CheckPacketFromPeer(packet, "other")
	if !result.Drop {
		t.Error("expected packet from 'other' to be dropped")
	}
}

func TestPacketFilter_PeerSpecificDenyOverridesGlobalAllow(t *testing.T) {
	f := NewPacketFilter(true)

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")
	packet := buildTCPPacket(src, dst, 22)

	// Global allow for port 22
	f.SetCoordinatorRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
	})

	// Peer-specific deny for "badpeer"
	f.SetPeerConfigRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionDeny, SourcePeer: "badpeer"},
	})

	// "badpeer" should be denied (peer-specific deny wins over global allow)
	result := f.CheckPacketFromPeer(packet, "badpeer")
	if !result.Drop {
		t.Error("expected peer-specific deny to override global allow")
	}

	// Other peers should still be allowed via global rule
	result = f.CheckPacketFromPeer(packet, "goodpeer")
	if result.Drop {
		t.Error("expected 'goodpeer' to be allowed via global rule")
	}
}

func TestPacketFilter_ListRulesWithPeer(t *testing.T) {
	f := NewPacketFilter(true)

	// Add global rule
	f.SetCoordinatorRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
	})

	// Add peer-specific rule
	f.AddTemporaryRule(FilterRule{
		Port:       22,
		Protocol:   ProtoTCP,
		Action:     ActionDeny,
		SourcePeer: "specific-peer",
	})

	rules := f.ListRules()
	if len(rules) != 2 {
		t.Errorf("expected 2 rules, got %d", len(rules))
	}

	// Find the peer-specific rule
	var foundPeerRule bool
	for _, r := range rules {
		if r.Rule.SourcePeer == "specific-peer" {
			foundPeerRule = true
			if r.Rule.Action != ActionDeny {
				t.Error("expected peer-specific rule to have ActionDeny")
			}
		}
	}

	if !foundPeerRule {
		t.Error("expected to find peer-specific rule in list")
	}
}

func TestPacketFilter_RemoveTemporaryRuleWithPeer(t *testing.T) {
	f := NewPacketFilter(true)

	// Add peer-specific rule
	f.AddTemporaryRule(FilterRule{
		Port:       22,
		Protocol:   ProtoTCP,
		Action:     ActionDeny,
		SourcePeer: "peer1",
	})

	// Add global rule for same port
	f.AddTemporaryRule(FilterRule{
		Port:     22,
		Protocol: ProtoTCP,
		Action:   ActionAllow,
	})

	if f.RuleCount() != 2 {
		t.Errorf("expected 2 rules, got %d", f.RuleCount())
	}

	// Remove peer-specific rule only
	f.RemoveTemporaryRuleForPeer(22, ProtoTCP, "peer1")

	if f.RuleCount() != 1 {
		t.Errorf("expected 1 rule after removal, got %d", f.RuleCount())
	}

	// Global rule should still exist
	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")
	packet := buildTCPPacket(src, dst, 22)
	result := f.CheckPacketFromPeer(packet, "peer1")
	if result.Drop {
		t.Error("expected global allow rule to still be active")
	}
}

func TestFilterResult_SourcePeer(t *testing.T) {
	f := NewPacketFilter(true)

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")
	packet := buildTCPPacket(src, dst, 22)

	// Add global allow rule
	f.SetCoordinatorRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
	})

	// Check with peer - result should include peer
	result := f.CheckPacketFromPeer(packet, "test-peer")
	if result.SourcePeer != "test-peer" {
		t.Errorf("expected SourcePeer='test-peer', got '%s'", result.SourcePeer)
	}

	// Check without peer
	result = f.CheckPacketFromPeer(packet, "")
	if result.SourcePeer != "" {
		t.Errorf("expected empty SourcePeer, got '%s'", result.SourcePeer)
	}
}

// TestPacketFilter_TCPResponsesAllowed verifies that TCP response packets (non-SYN)
// are allowed through even in deny-by-default mode. This enables outgoing connections
// to work while still filtering incoming connection attempts.
func TestPacketFilter_TCPResponsesAllowed(t *testing.T) {
	f := NewPacketFilter(true) // Default deny

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")

	// Test SYN packet (new connection) - should be filtered
	synPacket := buildTCPPacketWithFlags(src, dst, 12345, 8080, 0x02) // SYN only
	result := f.CheckPacketFromPeer(synPacket, "peer1")
	if !result.Drop {
		t.Error("expected SYN packet to port 8080 to be dropped (not in allow list)")
	}

	// Test SYN-ACK packet (response to SYN) - should be allowed
	synAckPacket := buildTCPPacketWithFlags(src, dst, 8080, 12345, 0x12) // SYN+ACK
	result = f.CheckPacketFromPeer(synAckPacket, "peer1")
	if result.Drop {
		t.Error("expected SYN-ACK packet to be allowed (response traffic)")
	}

	// Test ACK packet (established connection) - should be allowed
	ackPacket := buildTCPPacketWithFlags(src, dst, 8080, 12345, 0x10) // ACK only
	result = f.CheckPacketFromPeer(ackPacket, "peer1")
	if result.Drop {
		t.Error("expected ACK packet to be allowed (established traffic)")
	}

	// Test pure data packet (PSH+ACK) - should be allowed
	pshAckPacket := buildTCPPacketWithFlags(src, dst, 8080, 12345, 0x18) // PSH+ACK
	result = f.CheckPacketFromPeer(pshAckPacket, "peer1")
	if result.Drop {
		t.Error("expected PSH+ACK packet to be allowed (data traffic)")
	}

	// Test FIN packet - should be allowed
	finPacket := buildTCPPacketWithFlags(src, dst, 8080, 12345, 0x01) // FIN only
	result = f.CheckPacketFromPeer(finPacket, "peer1")
	if result.Drop {
		t.Error("expected FIN packet to be allowed")
	}

	// Now add an allow rule and verify SYN to allowed port works
	f.SetCoordinatorRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
	})

	synToAllowed := buildTCPPacketWithFlags(src, dst, 12345, 22, 0x02) // SYN to port 22
	result = f.CheckPacketFromPeer(synToAllowed, "peer1")
	if result.Drop {
		t.Error("expected SYN to allowed port 22 to pass")
	}
}

// Benchmark the hot path
func BenchmarkPacketFilter_ShouldDrop(b *testing.B) {
	f := NewPacketFilter(true)

	// Add some rules
	f.SetCoordinatorRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
		{Port: 80, Protocol: ProtoTCP, Action: ActionAllow},
		{Port: 443, Protocol: ProtoTCP, Action: ActionAllow},
	})

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")
	packet := buildTCPPacket(src, dst, 443)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ShouldDrop(packet)
	}
}

func BenchmarkPacketFilter_ShouldDrop_NoMatch(b *testing.B) {
	f := NewPacketFilter(true)

	// Add some rules
	f.SetCoordinatorRules([]FilterRule{
		{Port: 22, Protocol: ProtoTCP, Action: ActionAllow},
		{Port: 80, Protocol: ProtoTCP, Action: ActionAllow},
		{Port: 443, Protocol: ProtoTCP, Action: ActionAllow},
	})

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")
	packet := buildTCPPacket(src, dst, 8080) // Port not in rules

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ShouldDrop(packet)
	}
}

// TestTemporaryRuleTTLExpiry_RaceFree verifies that an expired temporary rule is
// no longer enforced, and that concurrent packet checks alongside concurrent rule
// writes do not produce data races. Run with -race for full validation.
func TestTemporaryRuleTTLExpiry_RaceFree(t *testing.T) {
	f := NewPacketFilter(true) // default deny

	// Add a rule that is already past its expiry (Expires is a Unix second timestamp;
	// setting it 2 seconds in the past guarantees IsExpired() returns true immediately).
	f.AddTemporaryRule(FilterRule{
		Port:     9999,
		Protocol: ProtoTCP,
		Action:   ActionAllow,
		Expires:  time.Now().Add(-2 * time.Second).Unix(), // already expired
	})

	src := net.ParseIP("10.0.0.1")
	dst := net.ParseIP("10.0.0.2")
	packet := buildTCPPacket(src, dst, 9999)

	// The rule is expired — default-deny should drop the packet.
	if !f.ShouldDrop(packet) {
		t.Error("expired temporary rule should no longer allow packet; default deny should apply")
	}

	// Concurrent readers + concurrent writes to verify no data races.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				f.ShouldDrop(packet)
			}
		}()
	}
	// Concurrent writes (adding and removing rules) while readers are active
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			f.AddTemporaryRule(FilterRule{Port: 8888, Protocol: ProtoTCP, Action: ActionAllow})
			f.RemoveTemporaryRule(8888, ProtoTCP)
		}
	}()
	wg.Wait()
}
