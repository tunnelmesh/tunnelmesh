// Package benchmark provides peer-to-peer speed testing over the mesh network.
// Benchmark traffic flows through the actual TUN/tunnel path to measure
// real-world performance for file transfers and gaming.
package benchmark

import (
	"fmt"
	"time"
)

// Default values for benchmark configuration.
const (
	// DefaultPort is the port of the admin server where the benchmark HTTP handler
	// is registered. Benchmark traffic flows through HTTPS (TLS + HTTP upgrade)
	// rather than a dedicated plain-TCP listener.
	DefaultPort    = 9443
	DefaultSize    = 10 * 1024 * 1024 // 10 MB
	DefaultTimeout = 120 * time.Second
	ChunkSize      = 64 * 1024 // 64 KB chunks

	// Timeout constants for network operations.
	ReadTimeout        = 30 * time.Second
	LatencyPingTimeout = 5 * time.Second
	PingCheckTimeout   = 100 * time.Millisecond
)

// Direction constants for benchmark data flow.
const (
	DirectionUpload   = "upload"
	DirectionDownload = "download"
)

// ChaosConfig defines chaos testing parameters for simulating network conditions.
type ChaosConfig struct {
	// PacketLossPercent is the percentage of packets to drop (0-100).
	PacketLossPercent float64 `json:"packet_loss_percent,omitempty"`

	// Latency is the fixed latency to add to each write.
	Latency time.Duration `json:"latency_ms,omitempty"`

	// Jitter is the random variation added to latency (+/- jitter).
	Jitter time.Duration `json:"jitter_ms,omitempty"`

	// BandwidthBps is the bandwidth limit in bytes per second (0 = unlimited).
	BandwidthBps int64 `json:"bandwidth_bps,omitempty"`
}

// Validate validates the chaos configuration.
func (c ChaosConfig) Validate() error {
	if c.PacketLossPercent < 0 || c.PacketLossPercent > 100 {
		return fmt.Errorf("packet_loss_percent must be between 0 and 100, got %v", c.PacketLossPercent)
	}
	if c.Latency < 0 {
		return fmt.Errorf("latency cannot be negative, got %v", c.Latency)
	}
	if c.Jitter < 0 {
		return fmt.Errorf("jitter cannot be negative, got %v", c.Jitter)
	}
	if c.BandwidthBps < 0 {
		return fmt.Errorf("bandwidth cannot be negative, got %v", c.BandwidthBps)
	}
	return nil
}

// IsEnabled returns true if any chaos testing is configured.
func (c ChaosConfig) IsEnabled() bool {
	return c.PacketLossPercent > 0 || c.Latency > 0 || c.Jitter > 0 || c.BandwidthBps > 0
}

// Config defines the parameters for a benchmark run.
type Config struct {
	// PeerName is the name of the target peer.
	PeerName string `json:"peer_name"`

	// Size is the number of bytes to transfer.
	Size int64 `json:"size"`

	// Direction is "upload" or "download".
	Direction string `json:"direction"`

	// Timeout is the maximum duration for the benchmark.
	Timeout time.Duration `json:"timeout"`

	// Port is the benchmark server port (default: 9998).
	Port int `json:"port"`

	// Chaos contains optional chaos testing parameters.
	Chaos ChaosConfig `json:"chaos,omitempty"`
}

// Validate validates the benchmark configuration.
func (c Config) Validate() error {
	if c.PeerName == "" {
		return fmt.Errorf("peer_name is required")
	}
	if c.Size <= 0 {
		return fmt.Errorf("size must be positive, got %d", c.Size)
	}
	if c.Direction != "" && c.Direction != DirectionUpload && c.Direction != DirectionDownload {
		return fmt.Errorf("direction must be %q or %q, got %q", DirectionUpload, DirectionDownload, c.Direction)
	}
	if err := c.Chaos.Validate(); err != nil {
		return fmt.Errorf("chaos config: %w", err)
	}
	return nil
}

// WithDefaults returns a copy of the config with default values applied.
func (c Config) WithDefaults() Config {
	if c.Direction == "" {
		c.Direction = DirectionUpload
	}
	if c.Port == 0 {
		c.Port = DefaultPort
	}
	if c.Timeout == 0 {
		c.Timeout = DefaultTimeout
	}
	return c
}

// Result contains the results of a benchmark run.
type Result struct {
	// Metadata
	ID         string    `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	LocalPeer  string    `json:"local_peer"`
	RemotePeer string    `json:"remote_peer"`
	Direction  string    `json:"direction"`

	// Transfer metrics
	RequestedSize   int64   `json:"requested_size_bytes"`
	TransferredSize int64   `json:"transferred_size_bytes"`
	DurationMs      int64   `json:"duration_ms"`
	ThroughputBps   float64 `json:"throughput_bps"`
	ThroughputMbps  float64 `json:"throughput_mbps"`

	// Latency metrics (in milliseconds)
	LatencyMinMs float64 `json:"latency_min_ms"`
	LatencyMaxMs float64 `json:"latency_max_ms"`
	LatencyAvgMs float64 `json:"latency_avg_ms"`

	// Status
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`

	// Chaos config used (if any)
	Chaos *ChaosConfig `json:"chaos,omitempty"`
}

// CalculateThroughput calculates and sets throughput fields based on transferred size and duration.
func (r *Result) CalculateThroughput() {
	if r.DurationMs > 0 {
		durationSec := float64(r.DurationMs) / 1000.0
		r.ThroughputBps = float64(r.TransferredSize) / durationSec
		r.ThroughputMbps = r.ThroughputBps * 8 / 1000000 // Convert to megabits per second
	}
}
