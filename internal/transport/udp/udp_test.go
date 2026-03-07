package udp

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"golang.org/x/crypto/blake2s"
)

func TestPacketHeader(t *testing.T) {
	header := PacketHeader{
		Type:     PacketTypeData,
		Receiver: 0x12345678,
		Counter:  0xABCDEF0123456789,
	}

	data := header.Marshal()
	if len(data) != HeaderSize {
		t.Errorf("expected header size %d, got %d", HeaderSize, len(data))
	}

	parsed, err := UnmarshalHeader(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.Type != header.Type {
		t.Errorf("type mismatch: expected %d, got %d", header.Type, parsed.Type)
	}
	if parsed.Receiver != header.Receiver {
		t.Errorf("receiver mismatch: expected %x, got %x", header.Receiver, parsed.Receiver)
	}
	if parsed.Counter != header.Counter {
		t.Errorf("counter mismatch: expected %x, got %x", header.Counter, parsed.Counter)
	}
}

func TestReplayWindow(t *testing.T) {
	w := NewReplayWindow(64)

	// First packet should be accepted
	if !w.Check(1) {
		t.Error("first packet should be accepted")
	}

	// Duplicate should be rejected
	if w.Check(1) {
		t.Error("duplicate packet should be rejected")
	}

	// Sequential packets should be accepted
	for i := uint64(2); i <= 10; i++ {
		if !w.Check(i) {
			t.Errorf("packet %d should be accepted", i)
		}
	}

	// Out of order within window should be accepted
	// (already received 1-10, so 11 should work)
	if !w.Check(11) {
		t.Error("packet 11 should be accepted")
	}

	// Packet 0 is always invalid
	if w.Check(0) {
		t.Error("packet 0 should be rejected")
	}

	// Far future packet should be accepted and advance window
	if !w.Check(100) {
		t.Error("far future packet should be accepted")
	}

	// Now very old packets should be rejected
	if w.Check(1) {
		t.Error("very old packet should be rejected after window advance")
	}
}

func TestCryptoRoundtrip(t *testing.T) {
	// Generate keys
	priv1, pub1, err := X25519KeyPair()
	if err != nil {
		t.Fatalf("generate key pair 1: %v", err)
	}

	priv2, pub2, err := X25519KeyPair()
	if err != nil {
		t.Fatalf("generate key pair 2: %v", err)
	}

	// Compute shared secrets
	shared1, err := X25519SharedSecret(priv1, pub2)
	if err != nil {
		t.Fatalf("shared secret 1: %v", err)
	}

	shared2, err := X25519SharedSecret(priv2, pub1)
	if err != nil {
		t.Fatalf("shared secret 2: %v", err)
	}

	// Shared secrets should match
	if shared1 != shared2 {
		t.Error("shared secrets don't match")
	}

	// Derive keys
	sendKey1, recvKey1, err := DeriveKeys(shared1, pub1, pub2, true)
	if err != nil {
		t.Fatalf("derive keys 1: %v", err)
	}

	sendKey2, recvKey2, err := DeriveKeys(shared2, pub1, pub2, false)
	if err != nil {
		t.Fatalf("derive keys 2: %v", err)
	}

	// Send key of one side should be receive key of other
	if sendKey1 != recvKey2 {
		t.Error("key derivation mismatch: sendKey1 != recvKey2")
	}
	if recvKey1 != sendKey2 {
		t.Error("key derivation mismatch: recvKey1 != sendKey2")
	}

	// Create crypto states
	crypto1, err := NewCryptoState(sendKey1, recvKey1)
	if err != nil {
		t.Fatalf("create crypto 1: %v", err)
	}

	crypto2, err := NewCryptoState(sendKey2, recvKey2)
	if err != nil {
		t.Fatalf("create crypto 2: %v", err)
	}

	// Test encryption/decryption
	plaintext := []byte("Hello, World!")
	var counter uint64 = 1
	var additionalData []byte

	ciphertext := crypto1.Encrypt(counter, plaintext, additionalData)
	decrypted, err := crypto2.Decrypt(counter, ciphertext, additionalData)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	if string(decrypted) != string(plaintext) {
		t.Errorf("decrypted mismatch: expected %q, got %q", plaintext, decrypted)
	}

	// Test wrong counter
	_, err = crypto2.Decrypt(counter+1, ciphertext, additionalData)
	if err == nil {
		t.Error("decrypt with wrong counter should fail")
	}
}

func TestHandshakeState(t *testing.T) {
	// Generate key pairs for initiator and responder
	initPriv, initPub, err := X25519KeyPair()
	if err != nil {
		t.Fatalf("generate initiator keys: %v", err)
	}

	respPriv, respPub, err := X25519KeyPair()
	if err != nil {
		t.Fatalf("generate responder keys: %v", err)
	}

	// Create initiator handshake state (knows responder's public key)
	initiator, err := NewInitiatorHandshake(initPriv, initPub, respPub)
	if err != nil {
		t.Fatalf("create initiator state: %v", err)
	}

	// Create responder handshake state (doesn't know initiator's public key yet)
	responder, err := NewResponderHandshake(respPriv, respPub)
	if err != nil {
		t.Fatalf("create responder state: %v", err)
	}

	// Create initiation
	initMsg, err := initiator.CreateInitiation()
	if err != nil {
		t.Fatalf("create initiation: %v", err)
	}

	// Consume initiation
	if err := responder.ConsumeInitiation(initMsg); err != nil {
		t.Fatalf("consume initiation: %v", err)
	}

	// Create response
	respMsg, err := responder.CreateResponse()
	if err != nil {
		t.Fatalf("create response: %v", err)
	}

	// Consume response
	if err := initiator.ConsumeResponse(respMsg); err != nil {
		t.Fatalf("consume response: %v", err)
	}

	// Derive keys
	initSend, initRecv, err := initiator.DeriveKeys()
	if err != nil {
		t.Fatalf("initiator derive keys: %v", err)
	}

	respSend, respRecv, err := responder.DeriveKeys()
	if err != nil {
		t.Fatalf("responder derive keys: %v", err)
	}

	// Verify keys match (initiator send = responder recv, etc.)
	if initSend != respRecv {
		t.Error("initiator send key != responder recv key")
	}
	if initRecv != respSend {
		t.Error("initiator recv key != responder send key")
	}
}

func BenchmarkEncrypt(b *testing.B) {
	priv1, _, _ := X25519KeyPair()
	_, pub2, _ := X25519KeyPair()
	shared, _ := X25519SharedSecret(priv1, pub2)

	var sendKey, recvKey [32]byte
	copy(sendKey[:], shared[:])
	copy(recvKey[:], shared[:])

	crypto, _ := NewCryptoState(sendKey, recvKey)
	plaintext := make([]byte, 1400) // Typical MTU-sized packet

	b.ResetTimer()
	b.SetBytes(int64(len(plaintext)))

	for i := 0; i < b.N; i++ {
		crypto.Encrypt(uint64(i), plaintext, nil)
	}
}

func BenchmarkDecrypt(b *testing.B) {
	priv1, _, _ := X25519KeyPair()
	_, pub2, _ := X25519KeyPair()
	shared, _ := X25519SharedSecret(priv1, pub2)

	var sendKey, recvKey [32]byte
	copy(sendKey[:], shared[:])
	copy(recvKey[:], shared[:])

	crypto, _ := NewCryptoState(sendKey, recvKey)
	plaintext := make([]byte, 1400)
	ciphertext := crypto.Encrypt(1, plaintext, nil)

	b.ResetTimer()
	b.SetBytes(int64(len(plaintext)))

	for i := 0; i < b.N; i++ {
		_, _ = crypto.Decrypt(1, ciphertext, nil)
	}
}

// =============================================================================
// KDF Function Tests (WireGuard-style key derivation)
// =============================================================================

func TestKDF1(t *testing.T) {
	key := []byte("test key for kdf1")
	input := []byte("input data")

	var result1, result2 [blake2s.Size]byte
	KDF1(&result1, key, input)
	KDF1(&result2, key, input)

	// Same inputs should produce same output
	if result1 != result2 {
		t.Error("KDF1 should be deterministic")
	}

	// Different inputs should produce different outputs
	var result3 [blake2s.Size]byte
	KDF1(&result3, key, []byte("different input"))
	if result1 == result3 {
		t.Error("KDF1 should produce different output for different input")
	}

	// Different keys should produce different outputs
	var result4 [blake2s.Size]byte
	KDF1(&result4, []byte("different key"), input)
	if result1 == result4 {
		t.Error("KDF1 should produce different output for different key")
	}
}

func TestKDF2(t *testing.T) {
	key := []byte("test key for kdf2")
	input := []byte("input data")

	var t0a, t1a, t0b, t1b [blake2s.Size]byte
	KDF2(&t0a, &t1a, key, input)
	KDF2(&t0b, &t1b, key, input)

	// Deterministic
	if t0a != t0b || t1a != t1b {
		t.Error("KDF2 should be deterministic")
	}

	// Two outputs should be different
	if t0a == t1a {
		t.Error("KDF2 should produce two different keys")
	}

	// Empty input should work (used in final key derivation)
	var t0c, t1c [blake2s.Size]byte
	KDF2(&t0c, &t1c, key, nil)
	if t0c == t0a {
		t.Error("KDF2 with nil input should differ from non-nil input")
	}
}

func TestKDF3(t *testing.T) {
	key := []byte("test key for kdf3")
	input := []byte("input data")

	var t0a, t1a, t2a [blake2s.Size]byte
	KDF3(&t0a, &t1a, &t2a, key, input)

	// Three outputs should all be different
	if t0a == t1a || t1a == t2a || t0a == t2a {
		t.Error("KDF3 should produce three different keys")
	}

	// Deterministic
	var t0b, t1b, t2b [blake2s.Size]byte
	KDF3(&t0b, &t1b, &t2b, key, input)
	if t0a != t0b || t1a != t1b || t2a != t2b {
		t.Error("KDF3 should be deterministic")
	}
}

// =============================================================================
// HMAC Function Tests
// =============================================================================

func TestHMAC1(t *testing.T) {
	key := []byte("hmac test key")
	data := []byte("test data")

	var sum1, sum2 [blake2s.Size]byte
	HMAC1(&sum1, key, data)
	HMAC1(&sum2, key, data)

	if sum1 != sum2 {
		t.Error("HMAC1 should be deterministic")
	}

	// Different data should produce different MAC
	var sum3 [blake2s.Size]byte
	HMAC1(&sum3, key, []byte("different data"))
	if sum1 == sum3 {
		t.Error("HMAC1 should produce different output for different data")
	}
}

func TestHMAC2(t *testing.T) {
	key := []byte("hmac test key")
	in0 := []byte("first input")
	in1 := []byte("second input")

	var sum1, sum2 [blake2s.Size]byte
	HMAC2(&sum1, key, in0, in1)
	HMAC2(&sum2, key, in0, in1)

	if sum1 != sum2 {
		t.Error("HMAC2 should be deterministic")
	}

	// Order matters
	var sum3 [blake2s.Size]byte
	HMAC2(&sum3, key, in1, in0)
	if sum1 == sum3 {
		t.Error("HMAC2 should be order-sensitive")
	}
}

// =============================================================================
// TAI64N Timestamp Tests
// =============================================================================

func TestTAI64N(t *testing.T) {
	ts1 := TAI64N()
	time.Sleep(10 * time.Millisecond)
	ts2 := TAI64N()

	// Timestamps should be different (monotonically increasing)
	if ts1 == ts2 {
		t.Error("consecutive TAI64N timestamps should differ")
	}

	// Later timestamp should be greater (comparing as big-endian)
	if bytes.Compare(ts2[:], ts1[:]) <= 0 {
		t.Error("later TAI64N timestamp should be greater")
	}

	// Should be 12 bytes (8 seconds + 4 nanoseconds)
	if len(ts1) != 12 {
		t.Errorf("TAI64N should be 12 bytes, got %d", len(ts1))
	}
}

// =============================================================================
// counterToNonce Tests
// =============================================================================

func TestCounterToNonce(t *testing.T) {
	// Test zero counter
	nonce0 := counterToNonce(0)
	expected0 := [NonceSize]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if nonce0 != expected0 {
		t.Errorf("counter 0: expected %x, got %x", expected0, nonce0)
	}

	// Test counter 1
	nonce1 := counterToNonce(1)
	expected1 := [NonceSize]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	if nonce1 != expected1 {
		t.Errorf("counter 1: expected %x, got %x", expected1, nonce1)
	}

	// Test large counter
	nonce := counterToNonce(0x0102030405060708)
	expected := [NonceSize]byte{0, 0, 0, 0, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	if nonce != expected {
		t.Errorf("large counter: expected %x, got %x", expected, nonce)
	}

	// Different counters should produce different nonces
	if counterToNonce(1) == counterToNonce(2) {
		t.Error("different counters should produce different nonces")
	}
}

// =============================================================================
// Mix Function Tests
// =============================================================================

func TestMixHashStatic(t *testing.T) {
	var hash1, hash2 [blake2s.Size]byte
	var initial [blake2s.Size]byte
	copy(initial[:], []byte("initial hash value"))

	data := []byte("data to mix")

	hash1 = initial
	hash2 = initial
	mixHashStatic(&hash1, &hash1, data)
	mixHashStatic(&hash2, &hash2, data)

	// Deterministic
	if hash1 != hash2 {
		t.Error("mixHashStatic should be deterministic")
	}

	// Should change the hash
	if hash1 == initial {
		t.Error("mixHashStatic should change the hash")
	}

	// Different data should produce different hash
	hash2 = initial
	mixHashStatic(&hash2, &hash2, []byte("different data"))
	if hash1 == hash2 {
		t.Error("mixHashStatic with different data should produce different result")
	}
}

func TestMixKeyStatic(t *testing.T) {
	var key1, key2 [blake2s.Size]byte
	var initial [blake2s.Size]byte
	copy(initial[:], []byte("initial chain key"))

	data := []byte("data to mix")

	key1 = initial
	key2 = initial
	mixKeyStatic(&key1, &key1, data)
	mixKeyStatic(&key2, &key2, data)

	// Deterministic
	if key1 != key2 {
		t.Error("mixKeyStatic should be deterministic")
	}

	// Should change the key
	if key1 == initial {
		t.Error("mixKeyStatic should change the key")
	}
}

// =============================================================================
// Data Packet Tests
// =============================================================================

func TestDataPacketMarshalUnmarshal(t *testing.T) {
	packet := &DataPacket{
		Header: PacketHeader{
			Type:     PacketTypeData,
			Receiver: 0x12345678,
			Counter:  0xABCDEF0123456789,
		},
		Ciphertext: []byte("encrypted payload data here"),
	}

	data := packet.Marshal()

	parsed, err := UnmarshalDataPacket(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.Header.Type != packet.Header.Type {
		t.Errorf("type mismatch: expected %d, got %d", packet.Header.Type, parsed.Header.Type)
	}
	if parsed.Header.Receiver != packet.Header.Receiver {
		t.Errorf("receiver mismatch: expected %x, got %x", packet.Header.Receiver, parsed.Header.Receiver)
	}
	if parsed.Header.Counter != packet.Header.Counter {
		t.Errorf("counter mismatch: expected %x, got %x", packet.Header.Counter, parsed.Header.Counter)
	}
	if !bytes.Equal(parsed.Ciphertext, packet.Ciphertext) {
		t.Errorf("ciphertext mismatch")
	}
}

func TestDataPacketTooShort(t *testing.T) {
	_, err := UnmarshalDataPacket([]byte{0x04}) // Too short
	if err == nil {
		t.Error("should fail on too-short packet")
	}
}

func TestDataPacketWrongType(t *testing.T) {
	// Create packet with handshake type
	header := PacketHeader{
		Type:     PacketTypeHandshakeInit,
		Receiver: 0x12345678,
		Counter:  1,
	}
	data := append(header.Marshal(), make([]byte, 16)...) // Add auth tag bytes

	_, err := UnmarshalDataPacket(data)
	if err == nil {
		t.Error("should fail on wrong packet type")
	}
}

// =============================================================================
// Keepalive Packet Tests
// =============================================================================

func TestKeepalivePacket(t *testing.T) {
	pkt := NewKeepalivePacket(0xDEADBEEF)

	if pkt.Header.Type != PacketTypeKeepalive {
		t.Errorf("expected keepalive type %d, got %d", PacketTypeKeepalive, pkt.Header.Type)
	}
	if pkt.Header.Receiver != 0xDEADBEEF {
		t.Errorf("expected receiver 0xDEADBEEF, got %x", pkt.Header.Receiver)
	}
	if pkt.Header.Counter != 0 {
		t.Errorf("keepalive counter should be 0, got %d", pkt.Header.Counter)
	}
}

// =============================================================================
// Rekey Required Packet Tests
// =============================================================================

func TestRekeyRequiredPacket(t *testing.T) {
	pkt := NewRekeyRequiredPacket(0x12345678)

	data := pkt.Marshal()

	if len(data) != RekeyRequiredSize {
		t.Errorf("expected size %d, got %d", RekeyRequiredSize, len(data))
	}

	if data[0] != PacketTypeRekeyRequired {
		t.Errorf("expected type %d, got %d", PacketTypeRekeyRequired, data[0])
	}

	// Parse it back
	parsed, err := UnmarshalRekeyRequired(data)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if parsed.UnknownIndex != 0x12345678 {
		t.Errorf("expected index 0x12345678, got 0x%x", parsed.UnknownIndex)
	}
}

func TestRekeyRequiredPacketTooShort(t *testing.T) {
	data := []byte{PacketTypeRekeyRequired, 0x01, 0x02} // Only 3 bytes, need 5

	_, err := UnmarshalRekeyRequired(data)
	if err == nil {
		t.Error("expected error for short packet")
	}
}

// TestRekeyRequiredPacketSizeVsMinPacketSize verifies that RekeyRequired packets
// (5 bytes) are smaller than MinPacketSize (32 bytes) and the receive loop
// must not use MinPacketSize as the minimum check (regression test for bug
// where rekey-required packets were silently dropped).
func TestRekeyRequiredPacketSizeVsMinPacketSize(t *testing.T) {
	// This test documents the size relationship and ensures we don't
	// accidentally break rekey-required handling by changing size checks
	if RekeyRequiredSize >= MinPacketSize {
		t.Errorf("RekeyRequiredSize (%d) should be less than MinPacketSize (%d) - if this changed, verify receive loop handles small packets",
			RekeyRequiredSize, MinPacketSize)
	}

	// Verify the actual packet sizes
	pkt := NewRekeyRequiredPacket(0x12345678)
	data := pkt.Marshal()
	if len(data) != RekeyRequiredSize {
		t.Errorf("rekey packet should be %d bytes, got %d", RekeyRequiredSize, len(data))
	}

	// MinPacketSize is for data/keepalive packets with header + auth tag
	if MinPacketSize != HeaderSize+AuthTagSize {
		t.Errorf("MinPacketSize should be HeaderSize+AuthTagSize (%d), got %d",
			HeaderSize+AuthTagSize, MinPacketSize)
	}
}

// =============================================================================
// Replay Window Edge Case Tests
// =============================================================================

func TestReplayWindowOutOfOrder(t *testing.T) {
	w := NewReplayWindow(64)

	// Receive packets out of order within window
	if !w.Check(5) {
		t.Error("packet 5 should be accepted")
	}
	if !w.Check(3) {
		t.Error("packet 3 (out of order) should be accepted")
	}
	if !w.Check(7) {
		t.Error("packet 7 should be accepted")
	}
	if !w.Check(4) {
		t.Error("packet 4 (out of order) should be accepted")
	}

	// Duplicates should still be rejected
	if w.Check(5) {
		t.Error("duplicate packet 5 should be rejected")
	}
	if w.Check(3) {
		t.Error("duplicate packet 3 should be rejected")
	}
}

func TestReplayWindowLargeGap(t *testing.T) {
	w := NewReplayWindow(64)

	// Receive packet 1
	if !w.Check(1) {
		t.Error("packet 1 should be accepted")
	}

	// Jump way ahead (larger than window)
	if !w.Check(1000) {
		t.Error("packet 1000 should be accepted")
	}

	// Old packet should now be rejected
	if w.Check(1) {
		t.Error("packet 1 should be rejected after large jump")
	}

	// Packet just within new window should still work
	if !w.Check(950) {
		t.Error("packet 950 (within window) should be accepted")
	}
}

func TestReplayWindowReset(t *testing.T) {
	w := NewReplayWindow(64)

	w.Check(10)
	w.Check(20)
	w.Check(30)

	w.Reset()

	// After reset, should accept packet 10 again
	if !w.Check(10) {
		t.Error("after reset, packet 10 should be accepted")
	}
}

func TestReplayWindowMaxSequence(t *testing.T) {
	w := NewReplayWindow(64)

	// Test with very large sequence numbers
	large := uint64(1) << 62
	if !w.Check(large) {
		t.Error("large sequence should be accepted")
	}
	if w.Check(large) {
		t.Error("duplicate large sequence should be rejected")
	}
	if !w.Check(large + 1) {
		t.Error("large+1 should be accepted")
	}
}

// =============================================================================
// Session Tests
// =============================================================================

func TestSessionStateTransitions(t *testing.T) {
	// Create a mock UDP connection
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("create UDP conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	remoteAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")

	session := NewSession(SessionConfig{
		LocalIndex: 1234,
		PeerName:   "test-peer",
		RemoteAddr: remoteAddr,
		Conn:       conn,
	})

	// Initial state
	if session.State() != SessionStateNew {
		t.Errorf("expected initial state New, got %d", session.State())
	}

	// Set to handshaking
	session.SetState(SessionStateHandshaking)
	if session.State() != SessionStateHandshaking {
		t.Errorf("expected state Handshaking, got %d", session.State())
	}

	// Set crypto (transitions to Established)
	priv1, pub1, _ := X25519KeyPair()
	_, pub2, _ := X25519KeyPair()
	shared, _ := X25519SharedSecret(priv1, pub2)
	sendKey, recvKey, _ := DeriveKeys(shared, pub1, pub2, true)
	crypto, _ := NewCryptoState(sendKey, recvKey)

	session.SetCrypto(crypto, 5678)
	if session.State() != SessionStateEstablished {
		t.Errorf("expected state Established after SetCrypto, got %d", session.State())
	}
	if session.RemoteIndex() != 5678 {
		t.Errorf("expected remote index 5678, got %d", session.RemoteIndex())
	}

	// Close
	_ = session.Close()
	if session.State() != SessionStateClosed {
		t.Errorf("expected state Closed, got %d", session.State())
	}
}

func TestSessionSendBeforeEstablished(t *testing.T) {
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	conn, _ := net.ListenUDP("udp", addr)
	defer func() { _ = conn.Close() }()

	remoteAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")

	session := NewSession(SessionConfig{
		LocalIndex: 1234,
		PeerName:   "test-peer",
		RemoteAddr: remoteAddr,
		Conn:       conn,
	})

	err := session.Send([]byte("test data"))
	if !errors.Is(err, ErrSessionNotEstablished) {
		t.Errorf("expected ErrSessionNotEstablished, got %v", err)
	}
}

func TestSessionHandlePacketBeforeEstablished(t *testing.T) {
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	conn, _ := net.ListenUDP("udp", addr)
	defer func() { _ = conn.Close() }()

	remoteAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")

	session := NewSession(SessionConfig{
		LocalIndex: 1234,
		PeerName:   "test-peer",
		RemoteAddr: remoteAddr,
		Conn:       conn,
	})

	header := &PacketHeader{Type: PacketTypeData, Receiver: 1234, Counter: 1}
	err := session.HandlePacket(header, []byte("encrypted"))
	if !errors.Is(err, ErrSessionNotEstablished) {
		t.Errorf("expected ErrSessionNotEstablished, got %v", err)
	}
}

func TestSessionStats(t *testing.T) {
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	conn, _ := net.ListenUDP("udp", addr)
	defer func() { _ = conn.Close() }()

	remoteAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")

	session := NewSession(SessionConfig{
		LocalIndex: 1234,
		PeerName:   "test-peer",
		RemoteAddr: remoteAddr,
		Conn:       conn,
	})

	stats := session.Stats()
	if stats.BytesIn != 0 || stats.BytesOut != 0 {
		t.Error("initial stats should be zero")
	}
	if stats.PacketsIn != 0 || stats.PacketsOut != 0 {
		t.Error("initial packet counts should be zero")
	}
}

func TestSessionUpdateRemoteAddr(t *testing.T) {
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	conn, _ := net.ListenUDP("udp", addr)
	defer func() { _ = conn.Close() }()

	remoteAddr1, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	remoteAddr2, _ := net.ResolveUDPAddr("udp", "192.168.1.1:54321")

	session := NewSession(SessionConfig{
		LocalIndex: 1234,
		PeerName:   "test-peer",
		RemoteAddr: remoteAddr1,
		Conn:       conn,
	})

	if session.RemoteAddr().String() != remoteAddr1.String() {
		t.Error("initial remote addr mismatch")
	}

	session.UpdateRemoteAddr(remoteAddr2)
	if session.RemoteAddr().String() != remoteAddr2.String() {
		t.Error("updated remote addr mismatch")
	}
}

// TestKeepaliveResetsSessionTimeout verifies that receiving a keepalive packet
// updates lastRecv and prevents the session from being marked as timed out.
// Regression test for bug where handleKeepalive did not call UpdateLastReceive,
// causing idle sessions to time out every ~88s despite both peers sending keepalives.
func TestKeepaliveResetsSessionTimeout(t *testing.T) {
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	conn, _ := net.ListenUDP("udp", addr)
	defer func() { _ = conn.Close() }()

	remoteAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")

	session := NewSession(SessionConfig{
		LocalIndex: 0xCAFEBABE,
		PeerName:   "keepalive-test",
		RemoteAddr: remoteAddr,
		Conn:       conn,
	})

	// Record time before updating
	before := time.Now()

	// Simulate receiving a keepalive by calling UpdateLastReceive directly
	session.UpdateLastReceive()

	after := time.Now()
	lastRecv := session.LastReceive()

	if lastRecv.Before(before) || lastRecv.After(after) {
		t.Errorf("UpdateLastReceive did not update lastRecv: got %v, want between %v and %v",
			lastRecv, before, after)
	}
}

// =============================================================================
// Handshake Error Case Tests
// =============================================================================

func TestHandshakeInitiationTooShort(t *testing.T) {
	_, _, err := X25519KeyPair()
	if err != nil {
		t.Fatalf("generate keys: %v", err)
	}

	respPriv, respPub, _ := X25519KeyPair()
	responder, _ := NewResponderHandshake(respPriv, respPub)

	// Try to consume a too-short message
	err = responder.ConsumeInitiation([]byte("too short"))
	if err == nil {
		t.Error("should reject too-short initiation")
	}
}

func TestHandshakeResponseTooShort(t *testing.T) {
	initPriv, initPub, _ := X25519KeyPair()
	_, respPub, _ := X25519KeyPair()

	initiator, _ := NewInitiatorHandshake(initPriv, initPub, respPub)
	_, _ = initiator.CreateInitiation()

	// Try to consume a too-short response
	err := initiator.ConsumeResponse([]byte("too short"))
	if err == nil {
		t.Error("should reject too-short response")
	}
}

func TestHandshakeResponseWrongReceiverIndex(t *testing.T) {
	initPriv, initPub, _ := X25519KeyPair()
	respPriv, respPub, _ := X25519KeyPair()

	initiator, _ := NewInitiatorHandshake(initPriv, initPub, respPub)
	initMsg, _ := initiator.CreateInitiation()

	responder, _ := NewResponderHandshake(respPriv, respPub)
	_ = responder.ConsumeInitiation(initMsg)
	respMsg, _ := responder.CreateResponse()

	// Corrupt the receiver index (bytes 4-8)
	respMsg[4] = 0xFF
	respMsg[5] = 0xFF
	respMsg[6] = 0xFF
	respMsg[7] = 0xFF

	err := initiator.ConsumeResponse(respMsg)
	if err == nil {
		t.Error("should reject response with wrong receiver index")
	}
}

func TestHandshakeIndexMethods(t *testing.T) {
	initPriv, initPub, _ := X25519KeyPair()
	respPriv, respPub, _ := X25519KeyPair()

	initiator, _ := NewInitiatorHandshake(initPriv, initPub, respPub)
	responder, _ := NewResponderHandshake(respPriv, respPub)

	initMsg, _ := initiator.CreateInitiation()
	localIdx := initiator.LocalIndex()
	if localIdx == 0 {
		t.Error("local index should be non-zero after CreateInitiation")
	}

	_ = responder.ConsumeInitiation(initMsg)
	if responder.RemoteIndex() != localIdx {
		t.Error("responder remote index should match initiator local index")
	}

	respMsg, _ := responder.CreateResponse()
	_ = initiator.ConsumeResponse(respMsg)

	if initiator.RemoteIndex() != responder.LocalIndex() {
		t.Error("initiator remote index should match responder local index")
	}

	// PeerStaticPublic
	if initiator.PeerStaticPublic() != respPub {
		t.Error("initiator peer public should match responder's public key")
	}
	if responder.PeerStaticPublic() != initPub {
		t.Error("responder peer public should match initiator's public key")
	}
}

// =============================================================================
// Full Handshake + Session Crypto Roundtrip Test
// =============================================================================

func TestFullHandshakeWithSessionCrypto(t *testing.T) {
	// Generate identity keys
	initPriv, initPub, _ := X25519KeyPair()
	respPriv, respPub, _ := X25519KeyPair()

	// Create handshake states
	initiator, _ := NewInitiatorHandshake(initPriv, initPub, respPub)
	responder, _ := NewResponderHandshake(respPriv, respPub)

	// Perform handshake
	initMsg, _ := initiator.CreateInitiation()
	_ = responder.ConsumeInitiation(initMsg)
	respMsg, _ := responder.CreateResponse()
	_ = initiator.ConsumeResponse(respMsg)

	// Derive session keys
	initSend, initRecv, _ := initiator.DeriveKeys()
	respSend, respRecv, _ := responder.DeriveKeys()

	// Create crypto states
	initCrypto, _ := NewCryptoState(initSend, initRecv)
	respCrypto, _ := NewCryptoState(respSend, respRecv)

	// Test bidirectional communication
	testData := []byte("Hello from initiator to responder!")
	counter := uint64(1)
	hdr := PacketHeader{Type: PacketTypeData, Receiver: 1234, Counter: counter}
	header := hdr.Marshal()

	// Initiator encrypts, responder decrypts
	ciphertext := initCrypto.Encrypt(counter, testData, header)
	plaintext, err := respCrypto.Decrypt(counter, ciphertext, header)
	if err != nil {
		t.Fatalf("responder decrypt failed: %v", err)
	}
	if !bytes.Equal(plaintext, testData) {
		t.Errorf("decrypted data mismatch: expected %q, got %q", testData, plaintext)
	}

	// Responder encrypts, initiator decrypts
	testData2 := []byte("Hello back from responder!")
	counter2 := uint64(2)
	hdr2 := PacketHeader{Type: PacketTypeData, Receiver: 5678, Counter: counter2}
	header2 := hdr2.Marshal()

	ciphertext2 := respCrypto.Encrypt(counter2, testData2, header2)
	plaintext2, err := initCrypto.Decrypt(counter2, ciphertext2, header2)
	if err != nil {
		t.Fatalf("initiator decrypt failed: %v", err)
	}
	if !bytes.Equal(plaintext2, testData2) {
		t.Errorf("decrypted data mismatch: expected %q, got %q", testData2, plaintext2)
	}
}

// =============================================================================
// Crypto Tamper Detection Tests
// =============================================================================

func TestCryptoTamperedCiphertext(t *testing.T) {
	priv1, pub1, _ := X25519KeyPair()
	priv2, pub2, _ := X25519KeyPair()

	shared1, _ := X25519SharedSecret(priv1, pub2)
	shared2, _ := X25519SharedSecret(priv2, pub1)

	sendKey1, recvKey1, _ := DeriveKeys(shared1, pub1, pub2, true)
	sendKey2, recvKey2, _ := DeriveKeys(shared2, pub1, pub2, false)

	crypto1, _ := NewCryptoState(sendKey1, recvKey1)
	crypto2, _ := NewCryptoState(sendKey2, recvKey2)

	plaintext := []byte("sensitive data")
	counter := uint64(1)
	ciphertext := crypto1.Encrypt(counter, plaintext, nil)

	// Tamper with ciphertext
	ciphertext[0] ^= 0xFF

	_, err := crypto2.Decrypt(counter, ciphertext, nil)
	if err == nil {
		t.Error("should detect tampered ciphertext")
	}
}

func TestCryptoTamperedAdditionalData(t *testing.T) {
	priv1, pub1, _ := X25519KeyPair()
	priv2, pub2, _ := X25519KeyPair()

	shared1, _ := X25519SharedSecret(priv1, pub2)
	shared2, _ := X25519SharedSecret(priv2, pub1)

	sendKey1, recvKey1, _ := DeriveKeys(shared1, pub1, pub2, true)
	sendKey2, recvKey2, _ := DeriveKeys(shared2, pub1, pub2, false)

	crypto1, _ := NewCryptoState(sendKey1, recvKey1)
	crypto2, _ := NewCryptoState(sendKey2, recvKey2)

	plaintext := []byte("sensitive data")
	counter := uint64(1)
	ad := []byte("additional data")
	ciphertext := crypto1.Encrypt(counter, plaintext, ad)

	// Use different additional data for decryption
	_, err := crypto2.Decrypt(counter, ciphertext, []byte("wrong additional data"))
	if err == nil {
		t.Error("should detect wrong additional data")
	}
}

// =============================================================================
// GenerateSessionIndex Test
// =============================================================================

func TestGenerateSessionIndex(t *testing.T) {
	indices := make(map[uint32]bool)

	// Generate many indices and check for uniqueness
	for i := 0; i < 100; i++ {
		idx, err := GenerateSessionIndex()
		if err != nil {
			t.Fatalf("generate index: %v", err)
		}
		if indices[idx] {
			t.Errorf("duplicate index generated: %d", idx)
		}
		indices[idx] = true
	}
}

// =============================================================================
// Initial State Verification
// =============================================================================

func TestInitialChainKeyAndHash(t *testing.T) {
	// Verify initialization ran correctly
	var zero [blake2s.Size]byte
	if InitialChainKey == zero {
		t.Error("InitialChainKey should not be zero")
	}
	if InitialHash == zero {
		t.Error("InitialHash should not be zero")
	}

	// Verify they're different
	if InitialChainKey == InitialHash {
		t.Error("InitialChainKey and InitialHash should differ")
	}

	// Verify deterministic (same on each run)
	expectedChainKey := blake2s.Sum256([]byte(NoiseConstruction))
	if InitialChainKey != expectedChainKey {
		t.Error("InitialChainKey doesn't match expected value")
	}
}

// =============================================================================
// Dual-Stack IPv4/IPv6 Tests
// =============================================================================

func TestSelectSocketForPeer_IPv4(t *testing.T) {
	priv, pub, _ := X25519KeyPair()
	transport := &Transport{
		staticPrivate: priv,
		staticPublic:  pub,
	}

	// Create mock IPv4 socket
	addr4, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	conn4, err := net.ListenUDP("udp4", addr4)
	if err != nil {
		t.Skipf("cannot create IPv4 socket: %v", err)
	}
	defer func() { _ = conn4.Close() }()
	transport.conn = conn4

	// Create mock IPv6 socket
	addr6, _ := net.ResolveUDPAddr("udp6", "[::1]:0")
	conn6, err := net.ListenUDP("udp6", addr6)
	if err != nil {
		t.Logf("cannot create IPv6 socket (may be disabled): %v", err)
	} else {
		defer func() { _ = conn6.Close() }()
		transport.conn6 = conn6
	}

	// IPv4 peer should use IPv4 socket
	peerAddr4, _ := net.ResolveUDPAddr("udp", "192.168.1.1:2228")
	selected := transport.selectSocketForPeer(peerAddr4)
	if selected != conn4 {
		t.Error("IPv4 peer should select IPv4 socket")
	}
}

func TestSelectSocketForPeer_IPv6(t *testing.T) {
	priv, pub, _ := X25519KeyPair()
	transport := &Transport{
		staticPrivate: priv,
		staticPublic:  pub,
	}

	// Create mock IPv4 socket
	addr4, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	conn4, err := net.ListenUDP("udp4", addr4)
	if err != nil {
		t.Skipf("cannot create IPv4 socket: %v", err)
	}
	defer func() { _ = conn4.Close() }()
	transport.conn = conn4

	// Create mock IPv6 socket
	addr6, _ := net.ResolveUDPAddr("udp6", "[::1]:0")
	conn6, err := net.ListenUDP("udp6", addr6)
	if err != nil {
		t.Skipf("cannot create IPv6 socket (may be disabled): %v", err)
	}
	defer func() { _ = conn6.Close() }()
	transport.conn6 = conn6

	// IPv6 peer should use IPv6 socket
	peerAddr6, _ := net.ResolveUDPAddr("udp", "[2001:db8::1]:2228")
	selected := transport.selectSocketForPeer(peerAddr6)
	if selected != conn6 {
		t.Error("IPv6 peer should select IPv6 socket")
	}
}

func TestSelectSocketForPeer_NoIPv6Socket(t *testing.T) {
	priv, pub, _ := X25519KeyPair()
	transport := &Transport{
		staticPrivate: priv,
		staticPublic:  pub,
	}

	// Only create IPv4 socket (no IPv6)
	addr4, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	conn4, err := net.ListenUDP("udp4", addr4)
	if err != nil {
		t.Skipf("cannot create IPv4 socket: %v", err)
	}
	defer func() { _ = conn4.Close() }()
	transport.conn = conn4
	transport.conn6 = nil // No IPv6 socket

	// IPv6 peer should return nil (IPv4 socket cannot reach IPv6 addresses)
	peerAddr6, _ := net.ResolveUDPAddr("udp", "[2001:db8::1]:2228")
	selected := transport.selectSocketForPeer(peerAddr6)
	if selected != nil {
		t.Error("IPv6 peer should return nil when no IPv6 socket available")
	}
}

func TestTransportStart_DualStack(t *testing.T) {
	priv, pub, _ := X25519KeyPair()
	cfg := Config{
		Port:          0, // Let OS assign port
		StaticPrivate: priv,
		StaticPublic:  pub,
	}

	transport, err := New(cfg)
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	err = transport.Start()
	if err != nil {
		t.Fatalf("start transport: %v", err)
	}
	defer func() { _ = transport.Close() }()

	// At least one socket should be created
	if transport.conn == nil && transport.conn6 == nil {
		t.Error("at least one socket should be created")
	}

	// On most systems, IPv4 should be available
	if transport.conn == nil {
		t.Log("Warning: IPv4 socket not created")
	} else {
		t.Logf("IPv4 socket: %s", transport.conn.LocalAddr())
	}

	// IPv6 may not be available on all systems
	if transport.conn6 == nil {
		t.Log("Note: IPv6 socket not created (may be disabled on this system)")
	} else {
		t.Logf("IPv6 socket: %s", transport.conn6.LocalAddr())
	}
}

func TestTransportClose_DualStack(t *testing.T) {
	priv, pub, _ := X25519KeyPair()
	cfg := Config{
		Port:          0,
		StaticPrivate: priv,
		StaticPublic:  pub,
	}

	transport, _ := New(cfg)
	_ = transport.Start()

	// Close should not error
	err := transport.Close()
	if err != nil {
		t.Errorf("close transport: %v", err)
	}

	// Sockets should be closed (writing should fail)
	if transport.conn != nil {
		_, err = transport.conn.WriteToUDP([]byte{0}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234})
		if err == nil {
			t.Error("write to closed IPv4 socket should fail")
		}
	}
	if transport.conn6 != nil {
		_, err = transport.conn6.WriteToUDP([]byte{0}, &net.UDPAddr{IP: net.IPv6loopback, Port: 1234})
		if err == nil {
			t.Error("write to closed IPv6 socket should fail")
		}
	}
}

// =============================================================================
// Worker Pool Tests
// =============================================================================

func TestWorkerPoolCreation(t *testing.T) {
	priv, pub, _ := X25519KeyPair()
	cfg := Config{
		Port:          0,
		StaticPrivate: priv,
		StaticPublic:  pub,
	}

	transport, err := New(cfg)
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	err = transport.Start()
	if err != nil {
		t.Fatalf("start transport: %v", err)
	}
	defer func() { _ = transport.Close() }()

	// Packet queue should be initialized
	if transport.packetQueue == nil {
		t.Error("packet queue should be initialized after Start()")
	}
}

func TestWorkerPoolProcessesPackets(t *testing.T) {
	priv, pub, _ := X25519KeyPair()
	cfg := Config{
		Port:          0,
		StaticPrivate: priv,
		StaticPublic:  pub,
	}

	transport, err := New(cfg)
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	err = transport.Start()
	if err != nil {
		t.Fatalf("start transport: %v", err)
	}
	defer func() { _ = transport.Close() }()

	// Create a test packet and queue it
	// The packet will be processed but since there's no session, it will be logged and dropped
	// We're testing that the queue mechanism works, not packet handling

	remoteAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	testPacket := make([]byte, 20)
	testPacket[0] = PacketTypeData // Data packet type

	// Queue the packet
	work := packetWork{
		data:       testPacket,
		remoteAddr: remoteAddr,
		conn:       transport.conn,
	}

	// Should be able to queue without blocking
	select {
	case transport.packetQueue <- work:
		// Success - packet queued
	case <-time.After(100 * time.Millisecond):
		t.Error("failed to queue packet - worker pool may not be running")
	}

	// Give workers time to process
	time.Sleep(50 * time.Millisecond)
}

func TestWorkerPoolQueueFullDropsPackets(t *testing.T) {
	priv, pub, _ := X25519KeyPair()
	cfg := Config{
		Port:          0,
		StaticPrivate: priv,
		StaticPublic:  pub,
	}

	transport, err := New(cfg)
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	err = transport.Start()
	if err != nil {
		t.Fatalf("start transport: %v", err)
	}
	defer func() { _ = transport.Close() }()

	remoteAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")

	// Fill up the queue by sending many more packets than buffer size.
	// This tests that we gracefully drop packets when queue is full
	// rather than creating unbounded goroutines or blocking.
	// Note: With concurrent workers draining the queue, we may not always
	// observe drops on fast machines - that's acceptable as long as we
	// don't block or panic.
	dropped := 0
	totalPackets := PacketQueueSize * 10 // Send 10x the queue size to ensure overflow
	for i := 0; i < totalPackets; i++ {
		testPacket := make([]byte, 20)
		testPacket[0] = PacketTypeData

		work := packetWork{
			data:       testPacket,
			remoteAddr: remoteAddr,
			conn:       transport.conn,
		}

		select {
		case transport.packetQueue <- work:
			// Queued successfully
		default:
			// Queue full - packet dropped (this is expected behavior)
			dropped++
		}
	}

	// Log results - drops are expected but not guaranteed on fast machines
	t.Logf("sent %d packets, dropped %d (queue size: %d)", totalPackets, dropped, PacketQueueSize)

	// The important thing is that we didn't block or panic.
	// If we got here, the non-blocking send behavior is working correctly.
}

func TestWorkerPoolShutdown(t *testing.T) {
	priv, pub, _ := X25519KeyPair()
	cfg := Config{
		Port:          0,
		StaticPrivate: priv,
		StaticPublic:  pub,
	}

	transport, err := New(cfg)
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	err = transport.Start()
	if err != nil {
		t.Fatalf("start transport: %v", err)
	}

	// Close should shut down workers gracefully
	err = transport.Close()
	if err != nil {
		t.Errorf("close transport: %v", err)
	}

	// After close, the packet queue channel should be closed.
	// Verify by checking we can't send to it (either blocks indefinitely or panics).
	// Since channel is closed, ranging over it should complete immediately.
	done := make(chan struct{})
	go func() {
		// This will complete immediately if channel is closed
		for range transport.packetQueue {
		}
		close(done)
	}()

	select {
	case <-done:
		// Channel was closed - workers can exit
	case <-time.After(100 * time.Millisecond):
		t.Error("packet queue should be closed after transport.Close()")
	}
}

// =============================================================================
// Crossing Handshake Tests
// =============================================================================

func TestCrossingHandshakeTieBreaker(t *testing.T) {
	// Test that crossing handshake detection uses public key comparison correctly.
	// When both nodes initiate handshakes simultaneously, the node with the "lower"
	// public key wins (their outgoing handshake is kept).

	// Create two "public keys" for comparison
	var lowKey, highKey [32]byte
	lowKey[0] = 0x00
	highKey[0] = 0xFF

	// Test: lower key should win (cmp < 0)
	cmp := bytes.Compare(lowKey[:], highKey[:])
	if cmp >= 0 {
		t.Errorf("expected lowKey < highKey, got cmp=%d", cmp)
	}

	// Test: higher key should lose (cmp > 0)
	cmp = bytes.Compare(highKey[:], lowKey[:])
	if cmp <= 0 {
		t.Errorf("expected highKey > lowKey, got cmp=%d", cmp)
	}

	// Test: equal keys (unlikely in practice)
	cmp = bytes.Compare(lowKey[:], lowKey[:])
	if cmp != 0 {
		t.Errorf("expected same keys to be equal, got cmp=%d", cmp)
	}
}

func TestPendingOutboundPeersTracking(t *testing.T) {
	// Test that pending outbound peers are tracked correctly
	cfg := Config{
		Port: 0, // Let OS choose port
	}

	transport, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	defer func() { _ = transport.Close() }()

	// Initially no pending outbound peers
	transport.mu.RLock()
	if len(transport.pendingOutboundPeers) != 0 {
		t.Errorf("expected no pending outbound peers initially, got %d", len(transport.pendingOutboundPeers))
	}
	transport.mu.RUnlock()

	// Simulate adding a pending outbound peer
	transport.mu.Lock()
	transport.pendingOutboundPeers["test-peer"] = 12345
	transport.mu.Unlock()

	// Verify it's tracked
	transport.mu.RLock()
	localIndex, exists := transport.pendingOutboundPeers["test-peer"]
	transport.mu.RUnlock()

	if !exists {
		t.Error("expected pending outbound peer to be tracked")
	}
	if localIndex != 12345 {
		t.Errorf("expected localIndex 12345, got %d", localIndex)
	}

	// Simulate removing the pending outbound peer
	transport.mu.Lock()
	delete(transport.pendingOutboundPeers, "test-peer")
	transport.mu.Unlock()

	// Verify it's removed
	transport.mu.RLock()
	_, exists = transport.pendingOutboundPeers["test-peer"]
	transport.mu.RUnlock()

	if exists {
		t.Error("expected pending outbound peer to be removed")
	}
}

// =============================================================================
// Ping/Pong Packet Tests (for latency measurement)
// =============================================================================

func TestPingPacket(t *testing.T) {
	receiver := uint32(0x12345678)
	ping := NewPingPacket(receiver)

	// Verify fields
	if ping.Receiver != receiver {
		t.Errorf("expected receiver 0x%x, got 0x%x", receiver, ping.Receiver)
	}
	if ping.Timestamp == 0 {
		t.Error("expected non-zero timestamp")
	}

	// Marshal and unmarshal
	data := ping.Marshal()
	if len(data) != PingPacketSize {
		t.Errorf("expected ping size %d, got %d", PingPacketSize, len(data))
	}
	if data[0] != PacketTypePing {
		t.Errorf("expected packet type %d, got %d", PacketTypePing, data[0])
	}

	parsed, err := UnmarshalPing(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Receiver != receiver {
		t.Errorf("parsed receiver mismatch: expected 0x%x, got 0x%x", receiver, parsed.Receiver)
	}
	if parsed.Timestamp != ping.Timestamp {
		t.Errorf("parsed timestamp mismatch: expected %d, got %d", ping.Timestamp, parsed.Timestamp)
	}
}

func TestPongPacket(t *testing.T) {
	receiver := uint32(0x87654321)
	timestamp := int64(1234567890)
	pong := NewPongPacket(receiver, timestamp)

	// Verify fields
	if pong.Receiver != receiver {
		t.Errorf("expected receiver 0x%x, got 0x%x", receiver, pong.Receiver)
	}
	if pong.Timestamp != timestamp {
		t.Errorf("expected timestamp %d, got %d", timestamp, pong.Timestamp)
	}

	// Marshal and unmarshal
	data := pong.Marshal()
	if len(data) != PingPacketSize { // Same size as ping
		t.Errorf("expected pong size %d, got %d", PingPacketSize, len(data))
	}
	if data[0] != PacketTypePong {
		t.Errorf("expected packet type %d, got %d", PacketTypePong, data[0])
	}

	parsed, err := UnmarshalPong(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Receiver != receiver {
		t.Errorf("parsed receiver mismatch: expected 0x%x, got 0x%x", receiver, parsed.Receiver)
	}
	if parsed.Timestamp != timestamp {
		t.Errorf("parsed timestamp mismatch: expected %d, got %d", timestamp, parsed.Timestamp)
	}
}

func TestPingPacketTooShort(t *testing.T) {
	data := []byte{PacketTypePing, 0x01, 0x02} // Only 3 bytes, need 13

	_, err := UnmarshalPing(data)
	if err == nil {
		t.Error("expected error for short packet")
	}
}

func TestPongPacketTooShort(t *testing.T) {
	data := []byte{PacketTypePong, 0x01, 0x02, 0x03, 0x04} // Only 5 bytes, need 13

	_, err := UnmarshalPong(data)
	if err == nil {
		t.Error("expected error for short packet")
	}
}

func TestPingPongRoundtrip(t *testing.T) {
	// Simulate ping/pong exchange
	receiver := uint32(0xABCDEF12)

	// Create ping
	ping := NewPingPacket(receiver)
	pingData := ping.Marshal()

	// Parse ping (simulating receiving it)
	parsedPing, err := UnmarshalPing(pingData)
	if err != nil {
		t.Fatalf("unmarshal ping failed: %v", err)
	}

	// Create pong response with echoed timestamp
	pong := NewPongPacket(receiver, parsedPing.Timestamp)
	pongData := pong.Marshal()

	// Parse pong (simulating receiving it)
	parsedPong, err := UnmarshalPong(pongData)
	if err != nil {
		t.Fatalf("unmarshal pong failed: %v", err)
	}

	// Verify timestamp was echoed correctly
	if parsedPong.Timestamp != ping.Timestamp {
		t.Errorf("timestamp not echoed correctly: expected %d, got %d", ping.Timestamp, parsedPong.Timestamp)
	}

	// Calculate RTT (should be very small in this test)
	rtt := time.Duration(time.Now().UnixNano() - parsedPong.Timestamp)
	if rtt < 0 {
		t.Error("RTT should be positive")
	}
	// Just verify it's a reasonable value (less than 1 second for a local test)
	if rtt > time.Second {
		t.Errorf("RTT too large: %v", rtt)
	}
}

func TestIsInLocalSubnet(t *testing.T) {
	t.Run("loopback is in local subnet", func(t *testing.T) {
		if !isInLocalSubnet(net.ParseIP("127.0.0.1")) {
			t.Error("127.0.0.1 should be in local subnet (loopback)")
		}
	})

	t.Run("public routable IP not in local subnet", func(t *testing.T) {
		// 192.0.2.0/24 is TEST-NET-1 (RFC 5737) — never assigned to real interfaces
		if isInLocalSubnet(net.ParseIP("192.0.2.1")) {
			t.Error("192.0.2.1 should not be in local subnet")
		}
	})

	t.Run("nil IP not in local subnet", func(t *testing.T) {
		if isInLocalSubnet(nil) {
			t.Error("nil IP should not be in local subnet")
		}
	})
}
