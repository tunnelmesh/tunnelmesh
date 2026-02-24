package udp

import "sync"

// ReplayWindow implements a sliding window for replay attack protection.
// Based on RFC 6479 (IPsec Anti-Replay Algorithm without Bit Shifting).
type ReplayWindow struct {
	mu     sync.Mutex
	bitmap []uint64 // Bitmap for tracking received packets
	top    uint64   // Highest sequence number received
	size   int      // Window size in bits
}

const (
	// DefaultWindowSize is the default replay window size (2048 packets)
	DefaultWindowSize = 2048
	// DefaultRecvBufSize is the default receive channel buffer size (512 packets ~700KB at MTU 1400)
	DefaultRecvBufSize = 512
	bitsPerWord        = 64
)

// NewReplayWindow creates a new replay window with the given size.
func NewReplayWindow(size int) *ReplayWindow {
	if size <= 0 {
		size = DefaultWindowSize
	}
	// Round up to nearest 64 bits
	words := (size + bitsPerWord - 1) / bitsPerWord
	return &ReplayWindow{
		bitmap: make([]uint64, words),
		size:   words * bitsPerWord,
	}
}

// Check returns true if the sequence number is acceptable (not a replay).
// It also updates the window if the packet is accepted.
func (w *ReplayWindow) Check(seq uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	// New packet must have seq > top - windowSize
	if seq == 0 {
		return false // Sequence 0 is reserved/invalid
	}

	// Check if sequence is too old
	if w.top >= uint64(w.size) && seq <= w.top-uint64(w.size) {
		return false // Too old, outside window
	}

	// Check if sequence is ahead of window
	if seq > w.top {
		// Advance the window
		diff := seq - w.top
		if diff >= uint64(w.size) {
			// Clear entire bitmap
			for i := range w.bitmap {
				w.bitmap[i] = 0
			}
		} else {
			// Shift out old entries
			w.shiftBitmap(int(diff))
		}
		w.top = seq
		w.setBit(seq)
		return true
	}

	// Check if already received
	if w.getBit(seq) {
		return false // Replay
	}

	// Mark as received
	w.setBit(seq)
	return true
}

// setBit marks a sequence number as received.
func (w *ReplayWindow) setBit(seq uint64) {
	idx := seq % uint64(w.size)
	wordIdx := idx / bitsPerWord
	bitIdx := idx % bitsPerWord
	w.bitmap[wordIdx] |= 1 << bitIdx
}

// getBit checks if a sequence number has been received.
func (w *ReplayWindow) getBit(seq uint64) bool {
	idx := seq % uint64(w.size)
	wordIdx := idx / bitsPerWord
	bitIdx := idx % bitsPerWord
	return (w.bitmap[wordIdx] & (1 << bitIdx)) != 0
}

// shiftBitmap shifts the bitmap to accommodate new sequence numbers.
func (w *ReplayWindow) shiftBitmap(shift int) {
	if shift >= w.size {
		for i := range w.bitmap {
			w.bitmap[i] = 0
		}
		return
	}

	// For simplicity, just clear bits that are now outside the window
	// This is not optimal but works correctly
	oldTop := w.top
	for i := uint64(0); i < uint64(shift) && oldTop >= i; i++ {
		seq := oldTop - uint64(w.size) + 1 + i
		if seq > 0 {
			w.clearBit(seq)
		}
	}
}

// clearBit marks a sequence number as not received.
func (w *ReplayWindow) clearBit(seq uint64) {
	idx := seq % uint64(w.size)
	wordIdx := idx / bitsPerWord
	bitIdx := idx % bitsPerWord
	w.bitmap[wordIdx] &^= 1 << bitIdx
}

// Reset clears the replay window.
func (w *ReplayWindow) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.bitmap {
		w.bitmap[i] = 0
	}
	w.top = 0
}
