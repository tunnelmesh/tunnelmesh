// Package config handles configuration loading and validation for tunnelmesh.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
	"github.com/tunnelmesh/tunnelmesh/pkg/bytesize"
	"gopkg.in/yaml.v3"
)

// MonitoringConfig holds configuration for reverse proxying to monitoring services.
type MonitoringConfig struct {
	PrometheusURL string `yaml:"prometheus_url"` // URL to proxy /prometheus/ to (e.g., "http://localhost:9090")
	GrafanaURL    string `yaml:"grafana_url"`    // URL to proxy /grafana/ to (e.g., "http://localhost:3000")
	OTLPEndpoint  string `yaml:"otlp_endpoint"`  // OTLP HTTP endpoint for distributed tracing (e.g., "http://127.0.0.1:4318")
}

// RelayConfig holds configuration for the relay server.
// Relay is always enabled when coordinator is enabled.
type RelayConfig struct {
	// No configuration needed - relay always runs with hardcoded 60s pair timeout
}

// CoordinatorConfig holds configuration for coordinator services (run by admin peers).
type CoordinatorConfig struct {
	Enabled            bool             `yaml:"enabled"`              // Enable coordinator services (auto-enabled if peer is admin)
	Listen             string           `yaml:"listen"`               // Coordination API listen address (e.g., ":8443")
	DataDir            string           `yaml:"data_dir"`             // Data directory for persistence (default: /var/lib/tunnelmesh)
	HeartbeatInterval  string           `yaml:"heartbeat_interval"`   // Heartbeat interval (default: 10s)
	UserExpirationDays int              `yaml:"user_expiration_days"` // Days until user expires after last seen (default: 270 = 9 months)
	AdminPeers         []string         `yaml:"admin_peers"`          // Peer names or peer IDs (SHA256 of SSH key) for admins group (e.g., ["honker", "abc123..."] - peer IDs preferred for security)
	Locations          bool             `yaml:"locations"`            // Enable node location tracking (requires external IP geolocation API)
	MemberlistSeeds    []string         `yaml:"memberlist_seeds"`     // Memberlist gossip cluster seed addresses (e.g., ["coord1.example.com:7946"])
	MemberlistBindAddr string           `yaml:"memberlist_bind_addr"` // Address to bind memberlist gossip (default: ":7946")
	Monitoring         MonitoringConfig `yaml:"monitoring"`           // Reverse proxy config for Prometheus/Grafana
	Relay              RelayConfig      `yaml:"relay"`                // WebSocket relay configuration
	S3                 S3Config         `yaml:"s3"`                   // S3-compatible storage configuration
	Filter             FilterConfig     `yaml:"filter"`               // Global packet filter rules for all peers
	ServicePorts       []uint16         `yaml:"service_ports"`        // Service ports to auto-allow on peers (default: [9443] for metrics)
	LandingPage        string           `yaml:"landing_page"`         // Path to custom landing page HTML file (default: built-in)
}

// S3Config holds configuration for the S3-compatible storage service.
// S3 is always enabled when coordinator is enabled (port hardcoded to 9000).
type S3Config struct {
	DataDir                  string                 `yaml:"data_dir"`                    // Storage directory for S3 objects (default: {data_dir}/s3)
	MaxSize                  bytesize.Size          `yaml:"max_size"`                    // Maximum storage size (e.g., "10Gi", "500Mi") - defaults to 1Gi
	MaxObjectSize            bytesize.Size          `yaml:"max_object_size"`             // Maximum size per individual object upload (e.g., "1Gi", "500Mi") - defaults to 1Gi
	ObjectExpiryDays         int                    `yaml:"object_expiry_days"`          // Days until objects expire (default: 9125 = 25 years)
	ShareExpiryDays          int                    `yaml:"share_expiry_days"`           // Days until file shares expire (default: 365 = 1 year)
	RecycleBinRetentionHours float64                `yaml:"recycle_bin_retention_hours"` // Hours to keep recycled items before deletion (default: 24h; fractions OK, e.g. 0.167 = 10 min)
	VersionRetentionDays     int                    `yaml:"version_retention_days"`      // Days to keep object versions (default: 30)
	MaxVersionsPerObject     int                    `yaml:"max_versions_per_object"`     // Max versions to keep per object (default: 100, 0 = unlimited)
	VersionRetention         VersionRetentionConfig `yaml:"version_retention"`           // Tiered version retention policy
	DefaultShareQuota        bytesize.Size          `yaml:"default_share_quota"`         // Auto-share quota per peer (default: 10Mi)
}

// VersionRetentionConfig configures smart tiered version retention.
// Older versions are kept at decreasing granularity to save space while maintaining history.
type VersionRetentionConfig struct {
	RecentDays    int `yaml:"recent_days"`    // Keep all versions from last N days (default: 7)
	WeeklyWeeks   int `yaml:"weekly_weeks"`   // Then keep one version per week for N weeks (default: 4)
	MonthlyMonths int `yaml:"monthly_months"` // Then keep one version per month for N months (default: 6)
}

// PeerConfig holds configuration for a peer node.
type PeerConfig struct {
	Name    string   `yaml:"name"`
	Servers []string `yaml:"-"` // CLI-only: passed via positional argument to 'join' command
	// AuthToken is the authentication credential for joining the mesh.
	// Must be 64 hex characters (32 bytes). Generate with: openssl rand -hex 32
	// CLI-only: loaded from TUNNELMESH_TOKEN env var
	AuthToken         string            `yaml:"-"`
	SSHPort           int               `yaml:"ssh_port"`
	PrivateKey        string            `yaml:"private_key"`
	ControlSocket     string            `yaml:"control_socket"`     // Unix socket path for CLI commands (default: /var/run/tunnelmesh.sock)
	HeartbeatInterval string            `yaml:"heartbeat_interval"` // Heartbeat interval (default: 10s)
	MetricsEnabled    *bool             `yaml:"metrics_enabled"`    // Enable Prometheus metrics (default: true). Disable for 10Gbps+ high-performance networks.
	MetricsPort       int               `yaml:"metrics_port"`       // Prometheus metrics port on mesh IP (default: 9443)
	LogLevel          string            `yaml:"log_level"`          // trace, debug, info, warn, error (default: info)
	TUN               TUNConfig         `yaml:"tun"`
	DNS               DNSConfig         `yaml:"dns"`
	Geolocation       GeolocationConfig `yaml:"geolocation"`        // Manual geolocation coordinates
	ExitPeer          string            `yaml:"exit_peer"`          // Name of peer to route internet traffic through
	AllowExitTraffic  bool              `yaml:"allow_exit_traffic"` // Allow this peer to act as exit peer for other peers
	Filter            FilterConfig      `yaml:"filter"`             // Local packet filter rules
	Loki              LokiConfig        `yaml:"loki"`               // Loki log shipping configuration
	Docker            DockerConfig      `yaml:"docker"`             // Docker container orchestration
	Coordinator       CoordinatorConfig `yaml:"coordinator"`        // Coordinator services (optional, auto-enabled if admin)
}

// TUNConfig holds configuration for the TUN interface.
type TUNConfig struct {
	Name string `yaml:"name"`
	MTU  int    `yaml:"mtu"`
}

// DNSConfig holds configuration for the local DNS resolver.
// DNS is always enabled for all peers.
type DNSConfig struct {
	Listen   string   `yaml:"listen"`
	CacheTTL int      `yaml:"cache_ttl"`
	Aliases  []string `yaml:"aliases,omitempty"` // Custom DNS aliases for this peer
}

// GeolocationConfig holds manual geolocation coordinates for a peer.
type GeolocationConfig struct {
	Latitude  float64 `yaml:"latitude"`  // Manual latitude (-90 to 90)
	Longitude float64 `yaml:"longitude"` // Manual longitude (-180 to 180)
	City      string  `yaml:"city"`      // Optional city name for display
}

// LokiConfig holds configuration for shipping logs to Loki.
type LokiConfig struct {
	Enabled       bool   `yaml:"enabled"`        // Enable Loki log shipping
	URL           string `yaml:"url"`            // Loki push URL (e.g., "http://10.42.0.1:3100")
	BatchSize     int    `yaml:"batch_size"`     // Max entries before flush (default: 100)
	FlushInterval string `yaml:"flush_interval"` // Flush interval (default: "5s")
}

// DockerConfig holds configuration for Docker container orchestration.
type DockerConfig struct {
	Socket          string `yaml:"socket"`            // Docker socket path (default: unix:///var/run/docker.sock)
	AutoPortForward *bool  `yaml:"auto_port_forward"` // Auto-create filter rules for container ports (default: true if nil)
}

// FilterRule represents a single packet filter rule for config files.
type FilterRule struct {
	Port       uint16 `yaml:"port"`        // Port number (1-65535)
	Protocol   string `yaml:"protocol"`    // "tcp" or "udp"
	Action     string `yaml:"action"`      // "allow" or "deny"
	SourcePeer string `yaml:"source_peer"` // Source peer name (empty = any peer)
}

// ProtocolNumber returns the IP protocol number (6 for TCP, 17 for UDP).
func (r *FilterRule) ProtocolNumber() uint8 {
	switch strings.ToLower(r.Protocol) {
	case "tcp":
		return 6
	case "udp":
		return 17
	default:
		return 0
	}
}

// FilterConfig holds packet filter configuration.
// When default_deny is true (allowlist mode), all ports are blocked unless explicitly allowed.
type FilterConfig struct {
	DefaultDeny *bool        `yaml:"default_deny"` // Block all by default (default: true)
	Rules       []FilterRule `yaml:"rules"`        // Filter rules
}

// IsDefaultDeny returns whether the filter defaults to denying traffic.
// Returns true by default (allowlist mode) if not explicitly set.
func (f *FilterConfig) IsDefaultDeny() bool {
	return f.DefaultDeny == nil || *f.DefaultDeny
}

// Validate checks if filter configuration is valid.
func (f *FilterConfig) Validate() error {
	for i, rule := range f.Rules {
		if rule.Port == 0 {
			return fmt.Errorf("filter.rules[%d].port is required", i)
		}
		proto := strings.ToLower(rule.Protocol)
		if proto != "tcp" && proto != "udp" {
			return fmt.Errorf("filter.rules[%d].protocol must be 'tcp' or 'udp', got %q", i, rule.Protocol)
		}
		if rule.Action != "allow" && rule.Action != "deny" {
			return fmt.Errorf("filter.rules[%d].action must be 'allow' or 'deny', got %q", i, rule.Action)
		}
	}
	return nil
}

// Validate checks if the geolocation coordinates are within valid ranges.
func (g *GeolocationConfig) Validate() error {
	if g.Latitude < -90 || g.Latitude > 90 {
		return fmt.Errorf("geolocation.latitude must be between -90 and 90, got %f", g.Latitude)
	}
	if g.Longitude < -180 || g.Longitude > 180 {
		return fmt.Errorf("geolocation.longitude must be between -180 and 180, got %f", g.Longitude)
	}
	return nil
}

// IsSet returns true if both latitude and longitude are configured (non-zero).
func (g *GeolocationConfig) IsSet() bool {
	return g.Latitude != 0 && g.Longitude != 0
}

// LoadServerConfig loads server configuration from a YAML file.
// LoadPeerConfig loads peer configuration from a YAML file.
//
//nolint:gocyclo // Config loading requires handling many optional fields with defaults
func LoadPeerConfig(path string) (*PeerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	cfg := &PeerConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	// Apply defaults
	if cfg.SSHPort == 0 {
		cfg.SSHPort = 2222
	}
	if cfg.TUN.Name == "" {
		cfg.TUN.Name = "tun-mesh0"
	}
	if cfg.TUN.MTU == 0 {
		cfg.TUN.MTU = 1400
	}
	if cfg.DNS.Listen == "" {
		cfg.DNS.Listen = "127.0.0.53:5353"
	}
	if cfg.DNS.CacheTTL == 0 {
		cfg.DNS.CacheTTL = 300
	}
	// DNS is always enabled for all peers
	if cfg.HeartbeatInterval == "" {
		cfg.HeartbeatInterval = "10s"
	}
	if cfg.MetricsPort == 0 {
		cfg.MetricsPort = 9443
	}

	// Expand home directory in private key path
	if strings.HasPrefix(cfg.PrivateKey, "~/") {
		homeDir, err := os.UserHomeDir()
		if err == nil {
			cfg.PrivateKey = filepath.Join(homeDir, cfg.PrivateKey[2:])
		}
	}

	// Loki defaults
	if cfg.Loki.Enabled {
		if cfg.Loki.BatchSize == 0 {
			cfg.Loki.BatchSize = 100
		}
		if cfg.Loki.FlushInterval == "" {
			cfg.Loki.FlushInterval = "5s"
		}
	}

	// Docker defaults
	if cfg.Docker.Socket == "" {
		cfg.Docker.Socket = "unix:///var/run/docker.sock"
	}
	// AutoPortForward defaults to true (opt-out model) if not explicitly set

	// Coordinator defaults
	if cfg.Coordinator.DataDir == "" {
		cfg.Coordinator.DataDir = "/var/lib/tunnelmesh"
	}
	if cfg.Coordinator.HeartbeatInterval == "" {
		cfg.Coordinator.HeartbeatInterval = "10s"
	}
	if cfg.Coordinator.UserExpirationDays == 0 {
		cfg.Coordinator.UserExpirationDays = 270
	}
	if cfg.Coordinator.MemberlistBindAddr == "" {
		cfg.Coordinator.MemberlistBindAddr = ":7946"
	}
	if len(cfg.Coordinator.ServicePorts) == 0 {
		cfg.Coordinator.ServicePorts = []uint16{9443}
	}
	// Expand home directory in coordinator data dir
	if strings.HasPrefix(cfg.Coordinator.DataDir, "~/") {
		homeDir, err := os.UserHomeDir()
		if err == nil {
			cfg.Coordinator.DataDir = filepath.Join(homeDir, cfg.Coordinator.DataDir[2:])
		}
	}
	// S3 defaults for coordinator (always enabled when coordinator enabled)
	if cfg.Coordinator.Enabled {
		if cfg.Coordinator.S3.DataDir == "" {
			cfg.Coordinator.S3.DataDir = filepath.Join(cfg.Coordinator.DataDir, "s3")
		}
		if cfg.Coordinator.S3.ObjectExpiryDays == 0 {
			cfg.Coordinator.S3.ObjectExpiryDays = 9125
		}
		if cfg.Coordinator.S3.ShareExpiryDays == 0 {
			cfg.Coordinator.S3.ShareExpiryDays = 365
		}
		if cfg.Coordinator.S3.RecycleBinRetentionHours == 0 {
			cfg.Coordinator.S3.RecycleBinRetentionHours = 24.0
		}
		if cfg.Coordinator.S3.VersionRetentionDays == 0 {
			cfg.Coordinator.S3.VersionRetentionDays = 30
		}
		if cfg.Coordinator.S3.MaxVersionsPerObject == 0 {
			cfg.Coordinator.S3.MaxVersionsPerObject = 10
		}
		if cfg.Coordinator.S3.VersionRetention.RecentDays == 0 {
			cfg.Coordinator.S3.VersionRetention.RecentDays = 7
		}
		if cfg.Coordinator.S3.VersionRetention.WeeklyWeeks == 0 {
			cfg.Coordinator.S3.VersionRetention.WeeklyWeeks = 4
		}
		if cfg.Coordinator.S3.VersionRetention.MonthlyMonths == 0 {
			cfg.Coordinator.S3.VersionRetention.MonthlyMonths = 6
		}
	}

	// Default S3 to 1Gi if coordinator enabled (mandatory for state persistence)
	if cfg.Coordinator.Enabled {
		if cfg.Coordinator.S3.MaxSize.Bytes() == 0 {
			cfg.Coordinator.S3.MaxSize = bytesize.Size(1 << 30) // 1Gi default
		}
		if cfg.Coordinator.S3.DefaultShareQuota.Bytes() == 0 {
			cfg.Coordinator.S3.DefaultShareQuota = bytesize.Size(10 * 1024 * 1024) // 10Mi default
		}
	}

	return cfg, nil
}

// IsMetricsEnabled returns whether Prometheus metrics collection is enabled.
// Returns true by default (when MetricsEnabled is nil or explicitly true).
// Disable for 10Gbps+ high-performance networks where atomic counter overhead matters.
func (c *PeerConfig) IsMetricsEnabled() bool {
	return c.MetricsEnabled == nil || *c.MetricsEnabled
}

// Validate checks if the peer configuration is valid.
// Note: Server URL and auth token are validated in the join command since they're CLI-only.
func (c *PeerConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name is required")
	}
	if c.SSHPort <= 0 || c.SSHPort > 65535 {
		return fmt.Errorf("ssh_port must be between 1 and 65535")
	}
	if c.TUN.MTU < 576 || c.TUN.MTU > 65535 {
		return fmt.Errorf("tun.mtu must be between 576 and 65535")
	}
	// Validate geolocation if any coordinate is set
	if c.Geolocation.Latitude != 0 || c.Geolocation.Longitude != 0 {
		if err := c.Geolocation.Validate(); err != nil {
			return err
		}
	}
	// Validate DNS aliases
	if err := c.DNS.ValidateAliases(c.Name); err != nil {
		return err
	}
	// Validate filter config
	if err := c.Filter.Validate(); err != nil {
		return err
	}
	// Validate coordinator config if enabled
	if c.Coordinator.Enabled {
		if c.Coordinator.Listen == "" {
			return fmt.Errorf("coordinator.listen is required when coordinator is enabled")
		}
		if err := c.Coordinator.Filter.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// PrimaryServer returns the first server in the Servers list, or empty string if none configured.
// This provides safe access to the primary coordinator without risking index out of bounds.
func (c *PeerConfig) PrimaryServer() string {
	if len(c.Servers) == 0 {
		return ""
	}
	return c.Servers[0]
}

// ValidateAliases checks if DNS aliases are valid DNS labels.
func (d *DNSConfig) ValidateAliases(peerName string) error {
	seen := make(map[string]bool)
	for _, alias := range d.Aliases {
		if err := validateDNSLabel(alias); err != nil {
			return fmt.Errorf("dns.aliases: %q is invalid: %w", alias, err)
		}
		if alias == peerName {
			return fmt.Errorf("dns.aliases: %q cannot be the same as peer name", alias)
		}
		if seen[alias] {
			return fmt.Errorf("dns.aliases: duplicate alias %q", alias)
		}
		seen[alias] = true
	}
	return nil
}

// validateDNSLabel checks if a string is a valid DNS label (RFC 1123).
func validateDNSLabel(label string) error {
	if len(label) == 0 {
		return fmt.Errorf("empty label")
	}
	if len(label) > 63 {
		return fmt.Errorf("exceeds 63 characters")
	}
	if label != strings.ToLower(label) {
		return fmt.Errorf("must be lowercase")
	}
	// Must start and end with alphanumeric
	if !isAlphanumeric(label[0]) {
		return fmt.Errorf("must start with alphanumeric character")
	}
	if !isAlphanumeric(label[len(label)-1]) {
		return fmt.Errorf("must end with alphanumeric character")
	}
	// Only alphanumeric and hyphens allowed
	for _, c := range label {
		if !isAlphanumeric(byte(c)) && c != '-' && c != '.' {
			return fmt.Errorf("contains invalid character %q", c)
		}
	}
	return nil
}

func isAlphanumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

// ApplyLogLevel parses the log level string and sets zerolog's global level.
// Returns true if the level was successfully applied, false if the level string
// was empty or invalid.
func ApplyLogLevel(level string) bool {
	if level == "" {
		return false
	}
	parsed, err := zerolog.ParseLevel(level)
	if err != nil {
		return false
	}
	zerolog.SetGlobalLevel(parsed)
	return true
}
