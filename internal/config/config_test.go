package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/testutil"
)

func TestLoadPeerConfig(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	content := `
name: "mynode"
ssh_port: 2222
private_key: "/path/to/key"
tun:
  name: "tun-mesh0"
  mtu: 1400
dns:
  enabled: true
  listen: "127.0.0.53:5353"
  cache_ttl: 60
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.Equal(t, "mynode", cfg.Name)
	assert.Equal(t, 2222, cfg.SSHPort)
	assert.Equal(t, "/path/to/key", cfg.PrivateKey)
	assert.Equal(t, "tun-mesh0", cfg.TUN.Name)
	assert.Equal(t, 1400, cfg.TUN.MTU)
	// DNS is always enabled now
	assert.Equal(t, "127.0.0.53:5353", cfg.DNS.Listen)
	assert.Equal(t, 60, cfg.DNS.CacheTTL)
}

func TestLoadPeerConfig_Defaults(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	content := `
name: "testnode"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	// Check defaults
	assert.Equal(t, 2222, cfg.SSHPort)
	assert.Equal(t, "tun-mesh0", cfg.TUN.Name)
	assert.Equal(t, 1400, cfg.TUN.MTU)
	// DNS is always enabled now
	assert.Equal(t, "127.0.0.53:5353", cfg.DNS.Listen)
	assert.Equal(t, 300, cfg.DNS.CacheTTL)
}

func TestLoadPeerConfig_ExpandHomePath(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	content := `
name: "testnode"
private_key: "~/.tunnelmesh/id_ed25519"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	// Should expand ~ to home directory
	homeDir, _ := os.UserHomeDir()
	expected := filepath.Join(homeDir, ".tunnelmesh/id_ed25519")
	assert.Equal(t, expected, cfg.PrivateKey)
}

func TestPeerConfig_Validate(t *testing.T) {
	validConfig := func() PeerConfig {
		return PeerConfig{
			Name:       "testnode",
			SSHPort:    2222,
			PrivateKey: "/path/to/key",
			TUN: TUNConfig{
				Name: "tun-mesh0",
				MTU:  1400,
			},
			DNS: DNSConfig{
				Listen:   "127.0.0.53:5353",
				CacheTTL: 60,
			},
		}
	}

	tests := []struct {
		name    string
		modify  func(*PeerConfig)
		wantErr bool
	}{
		{
			name:    "valid config",
			modify:  func(c *PeerConfig) {},
			wantErr: false,
		},
		{
			name:    "missing name",
			modify:  func(c *PeerConfig) { c.Name = "" },
			wantErr: true,
		},
		{
			name:    "invalid ssh port",
			modify:  func(c *PeerConfig) { c.SSHPort = 0 },
			wantErr: true,
		},
		{
			name:    "invalid mtu",
			modify:  func(c *PeerConfig) { c.TUN.MTU = 100 },
			wantErr: true,
		},
		{
			name:    "metrics_port out of range",
			modify:  func(c *PeerConfig) { c.MetricsPort = 99999 },
			wantErr: true,
		},
		{
			name:    "metrics_port negative",
			modify:  func(c *PeerConfig) { c.MetricsPort = -1 },
			wantErr: true,
		},
		{
			name:    "heartbeat_interval invalid string",
			modify:  func(c *PeerConfig) { c.HeartbeatInterval = "notaduration" },
			wantErr: true,
		},
		{
			name:    "heartbeat_interval negative",
			modify:  func(c *PeerConfig) { c.HeartbeatInterval = "-1s" },
			wantErr: true,
		},
		{
			name:    "heartbeat_interval zero",
			modify:  func(c *PeerConfig) { c.HeartbeatInterval = "0s" },
			wantErr: true,
		},
		{
			name:    "heartbeat_interval valid",
			modify:  func(c *PeerConfig) { c.HeartbeatInterval = "30s" },
			wantErr: false,
		},
		{
			name:    "metrics_port valid custom",
			modify:  func(c *PeerConfig) { c.MetricsPort = 9090 },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoadPeerConfig_WithGeolocation(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	content := `
name: "geonode"
geolocation:
  latitude: 51.5074
  longitude: -0.1278
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.Equal(t, 51.5074, cfg.Geolocation.Latitude)
	assert.Equal(t, -0.1278, cfg.Geolocation.Longitude)
}

func TestGeolocationConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		geo     GeolocationConfig
		wantErr bool
	}{
		{
			name: "valid location",
			geo: GeolocationConfig{
				Latitude:  40.7128,
				Longitude: -74.0060,
			},
			wantErr: false,
		},
		{
			name: "zero location (null island)",
			geo: GeolocationConfig{
				Latitude:  0,
				Longitude: 0,
			},
			wantErr: false,
		},
		{
			name: "latitude too high",
			geo: GeolocationConfig{
				Latitude:  91.0,
				Longitude: 0,
			},
			wantErr: true,
		},
		{
			name: "latitude too low",
			geo: GeolocationConfig{
				Latitude:  -91.0,
				Longitude: 0,
			},
			wantErr: true,
		},
		{
			name: "longitude too high",
			geo: GeolocationConfig{
				Latitude:  0,
				Longitude: 181.0,
			},
			wantErr: true,
		},
		{
			name: "longitude too low",
			geo: GeolocationConfig{
				Latitude:  0,
				Longitude: -181.0,
			},
			wantErr: true,
		},
		{
			name: "edge case: max valid",
			geo: GeolocationConfig{
				Latitude:  90.0,
				Longitude: 180.0,
			},
			wantErr: false,
		},
		{
			name: "edge case: min valid",
			geo: GeolocationConfig{
				Latitude:  -90.0,
				Longitude: -180.0,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.geo.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGeolocationConfig_IsSet(t *testing.T) {
	tests := []struct {
		name   string
		geo    GeolocationConfig
		expect bool
	}{
		{
			name:   "not set (zero values)",
			geo:    GeolocationConfig{},
			expect: false,
		},
		{
			name: "only latitude",
			geo: GeolocationConfig{
				Latitude: 51.5074,
			},
			expect: false,
		},
		{
			name: "only longitude",
			geo: GeolocationConfig{
				Longitude: -0.1278,
			},
			expect: false,
		},
		{
			name: "both set",
			geo: GeolocationConfig{
				Latitude:  51.5074,
				Longitude: -0.1278,
			},
			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, tt.geo.IsSet())
		})
	}
}

func TestPeerConfig_ValidateGeolocation(t *testing.T) {
	validConfig := func() PeerConfig {
		return PeerConfig{
			Name:       "testnode",
			SSHPort:    2222,
			PrivateKey: "/path/to/key",
			TUN: TUNConfig{
				Name: "tun-mesh0",
				MTU:  1400,
			},
		}
	}

	tests := []struct {
		name    string
		modify  func(*PeerConfig)
		wantErr bool
	}{
		{
			name:    "valid without geolocation",
			modify:  func(c *PeerConfig) {},
			wantErr: false,
		},
		{
			name: "valid with geolocation",
			modify: func(c *PeerConfig) {
				c.Geolocation = GeolocationConfig{
					Latitude:  40.7128,
					Longitude: -74.0060,
				}
			},
			wantErr: false,
		},
		{
			name: "invalid geolocation latitude",
			modify: func(c *PeerConfig) {
				c.Geolocation = GeolocationConfig{
					Latitude:  91.0,
					Longitude: 0,
				}
			},
			wantErr: true,
		},
		{
			name: "invalid geolocation longitude",
			modify: func(c *PeerConfig) {
				c.Geolocation = GeolocationConfig{
					Latitude:  0,
					Longitude: 181.0,
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Exit Node Feature Tests

func TestLoadPeerConfig_WithExitPeer(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	content := `
name: "client-node"
exit_peer: "exit-server"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.Equal(t, "client-node", cfg.Name)
	assert.Equal(t, "exit-server", cfg.ExitPeer)
}

func TestLoadPeerConfig_WithAllowExitTraffic(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	content := `
name: "exit-server"
allow_exit_traffic: true
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.Equal(t, "exit-server", cfg.Name)
	assert.True(t, cfg.AllowExitTraffic)
}

func TestLoadPeerConfig_ExitPeerDefaults(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Config without exit fields - they should default to empty/false
	content := `
name: "regular-node"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.Equal(t, "", cfg.ExitPeer, "exit_node should default to empty string")
	assert.False(t, cfg.AllowExitTraffic, "allow_exit_traffic should default to false")
}

// DNS Alias Tests

func TestValidateDNSLabel(t *testing.T) {
	tests := []struct {
		name    string
		label   string
		wantErr bool
	}{
		{
			name:    "valid simple label",
			label:   "myhost",
			wantErr: false,
		},
		{
			name:    "valid with hyphen",
			label:   "my-host",
			wantErr: false,
		},
		{
			name:    "valid with numbers",
			label:   "host123",
			wantErr: false,
		},
		{
			name:    "valid with dots",
			label:   "web.server",
			wantErr: false,
		},
		{
			name:    "valid subdomain style",
			label:   "api.v1.myservice",
			wantErr: false,
		},
		{
			name:    "empty label",
			label:   "",
			wantErr: true,
		},
		{
			name:    "too long (64 chars)",
			label:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr: true,
		},
		{
			name:    "max length (63 chars)",
			label:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr: false,
		},
		{
			name:    "uppercase not allowed",
			label:   "MyHost",
			wantErr: true,
		},
		{
			name:    "starts with hyphen",
			label:   "-myhost",
			wantErr: true,
		},
		{
			name:    "ends with hyphen",
			label:   "myhost-",
			wantErr: true,
		},
		{
			name:    "underscore not allowed",
			label:   "my_host",
			wantErr: true,
		},
		{
			name:    "space not allowed",
			label:   "my host",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDNSLabel(tt.label)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDNSConfig_ValidateAliases(t *testing.T) {
	tests := []struct {
		name     string
		aliases  []string
		peerName string
		wantErr  bool
	}{
		{
			name:     "no aliases",
			aliases:  nil,
			peerName: "mynode",
			wantErr:  false,
		},
		{
			name:     "empty aliases",
			aliases:  []string{},
			peerName: "mynode",
			wantErr:  false,
		},
		{
			name:     "valid single alias",
			aliases:  []string{"webserver"},
			peerName: "mynode",
			wantErr:  false,
		},
		{
			name:     "valid multiple aliases",
			aliases:  []string{"webserver", "api", "db"},
			peerName: "mynode",
			wantErr:  false,
		},
		{
			name:     "alias same as peer name",
			aliases:  []string{"mynode"},
			peerName: "mynode",
			wantErr:  true,
		},
		{
			name:     "duplicate alias",
			aliases:  []string{"webserver", "webserver"},
			peerName: "mynode",
			wantErr:  true,
		},
		{
			name:     "invalid alias format",
			aliases:  []string{"INVALID"},
			peerName: "mynode",
			wantErr:  true,
		},
		{
			name:     "one invalid among valid",
			aliases:  []string{"valid", "INVALID", "also-valid"},
			peerName: "mynode",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dns := DNSConfig{Aliases: tt.aliases}
			err := dns.ValidateAliases(tt.peerName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPeerConfig_ValidateAliases(t *testing.T) {
	validConfig := func() PeerConfig {
		return PeerConfig{
			Name:       "testnode",
			SSHPort:    2222,
			PrivateKey: "/path/to/key",
			TUN: TUNConfig{
				Name: "tun-mesh0",
				MTU:  1400,
			},
		}
	}

	tests := []struct {
		name    string
		modify  func(*PeerConfig)
		wantErr bool
	}{
		{
			name:    "valid without aliases",
			modify:  func(c *PeerConfig) {},
			wantErr: false,
		},
		{
			name: "valid with aliases",
			modify: func(c *PeerConfig) {
				c.DNS.Aliases = []string{"webserver", "api"}
			},
			wantErr: false,
		},
		{
			name: "alias equals peer name",
			modify: func(c *PeerConfig) {
				c.DNS.Aliases = []string{"testnode"}
			},
			wantErr: true,
		},
		{
			name: "invalid alias format",
			modify: func(c *PeerConfig) {
				c.DNS.Aliases = []string{"Invalid-Name"}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoadPeerConfig_WithAliases(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	content := `
name: "mynode"
dns:
  enabled: true
  aliases:
    - "webserver"
    - "api.mynode"
    - "db-primary"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.Equal(t, "mynode", cfg.Name)
	// DNS is always enabled now
	assert.Equal(t, []string{"webserver", "api.mynode", "db-primary"}, cfg.DNS.Aliases)
}

func TestLoadPeerConfig_MetricsEnabled_DefaultTrue(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Config without metrics_enabled - should default to true
	content := `
name: "testnode"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	// MetricsEnabled should default to true (nil pointer means enabled)
	assert.True(t, cfg.IsMetricsEnabled(), "metrics should be enabled by default")
}

func TestLoadPeerConfig_MetricsEnabled_ExplicitFalse(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Config with metrics_enabled explicitly set to false
	content := `
name: "testnode"
metrics_enabled: false
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.False(t, cfg.IsMetricsEnabled(), "metrics should be disabled when explicitly set to false")
}

func TestLoadPeerConfig_MetricsEnabled_ExplicitTrue(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Config with metrics_enabled explicitly set to true
	content := `
name: "testnode"
metrics_enabled: true
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.True(t, cfg.IsMetricsEnabled(), "metrics should be enabled when explicitly set to true")
}

// Log Level Configuration Tests

func TestLoadPeerConfig_WithLogLevel(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	content := `
name: "mynode"
log_level: "warn"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.Equal(t, "mynode", cfg.Name)
	assert.Equal(t, "warn", cfg.LogLevel)
}

func TestLoadPeerConfig_LogLevelDefaults(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Config without log_level - should default to empty string
	content := `
name: "testnode"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.Equal(t, "", cfg.LogLevel, "log_level should default to empty string")
}

func TestLoadPeerConfig_AllLogLevels(t *testing.T) {
	levels := []string{"trace", "debug", "info", "warn", "error"}

	for _, level := range levels {
		t.Run(level, func(t *testing.T) {
			dir, cleanup := testutil.TempDir(t)
			defer cleanup()

			content := `
name: "testnode"
log_level: "` + level + `"
`
			configPath := testutil.TempFile(t, dir, "peer.yaml", content)

			cfg, err := LoadPeerConfig(configPath)
			require.NoError(t, err)
			assert.Equal(t, level, cfg.LogLevel)
		})
	}
}

func TestApplyLogLevel(t *testing.T) {
	// Save original level to restore after test
	originalLevel := zerolog.GlobalLevel()
	defer zerolog.SetGlobalLevel(originalLevel)

	tests := []struct {
		name          string
		level         string
		expectApplied bool
		expectLevel   zerolog.Level
	}{
		{
			name:          "empty level",
			level:         "",
			expectApplied: false,
		},
		{
			name:          "trace level",
			level:         "trace",
			expectApplied: true,
			expectLevel:   zerolog.TraceLevel,
		},
		{
			name:          "debug level",
			level:         "debug",
			expectApplied: true,
			expectLevel:   zerolog.DebugLevel,
		},
		{
			name:          "info level",
			level:         "info",
			expectApplied: true,
			expectLevel:   zerolog.InfoLevel,
		},
		{
			name:          "warn level",
			level:         "warn",
			expectApplied: true,
			expectLevel:   zerolog.WarnLevel,
		},
		{
			name:          "error level",
			level:         "error",
			expectApplied: true,
			expectLevel:   zerolog.ErrorLevel,
		},
		{
			name:          "invalid level",
			level:         "invalid",
			expectApplied: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset to known state before each test
			zerolog.SetGlobalLevel(zerolog.InfoLevel)

			applied := ApplyLogLevel(tt.level)
			assert.Equal(t, tt.expectApplied, applied)

			if tt.expectApplied {
				assert.Equal(t, tt.expectLevel, zerolog.GlobalLevel())
			}
		})
	}
}

func TestFilterConfig_IsDefaultDeny(t *testing.T) {
	tests := []struct {
		name   string
		config FilterConfig
		want   bool
	}{
		{
			name:   "nil defaults to true",
			config: FilterConfig{},
			want:   true,
		},
		{
			name:   "explicit true",
			config: FilterConfig{DefaultDeny: boolPtr(true)},
			want:   true,
		},
		{
			name:   "explicit false",
			config: FilterConfig{DefaultDeny: boolPtr(false)},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.config.IsDefaultDeny())
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func TestFilterConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  FilterConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "empty rules is valid",
			config:  FilterConfig{},
			wantErr: false,
		},
		{
			name: "valid TCP allow rule",
			config: FilterConfig{
				Rules: []FilterRule{
					{Port: 22, Protocol: "tcp", Action: "allow"},
				},
			},
			wantErr: false,
		},
		{
			name: "valid UDP deny rule",
			config: FilterConfig{
				Rules: []FilterRule{
					{Port: 53, Protocol: "udp", Action: "deny"},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple valid rules",
			config: FilterConfig{
				Rules: []FilterRule{
					{Port: 22, Protocol: "tcp", Action: "allow"},
					{Port: 80, Protocol: "tcp", Action: "allow"},
					{Port: 443, Protocol: "tcp", Action: "allow"},
					{Port: 53, Protocol: "udp", Action: "allow"},
				},
			},
			wantErr: false,
		},
		{
			name: "case insensitive protocol",
			config: FilterConfig{
				Rules: []FilterRule{
					{Port: 22, Protocol: "TCP", Action: "allow"},
					{Port: 53, Protocol: "UDP", Action: "allow"},
				},
			},
			wantErr: false,
		},
		{
			name: "missing port",
			config: FilterConfig{
				Rules: []FilterRule{
					{Port: 0, Protocol: "tcp", Action: "allow"},
				},
			},
			wantErr: true,
			errMsg:  "port is required",
		},
		{
			name: "invalid protocol",
			config: FilterConfig{
				Rules: []FilterRule{
					{Port: 22, Protocol: "icmp", Action: "allow"},
				},
			},
			wantErr: true,
			errMsg:  "protocol must be 'tcp' or 'udp'",
		},
		{
			name: "invalid action",
			config: FilterConfig{
				Rules: []FilterRule{
					{Port: 22, Protocol: "tcp", Action: "block"},
				},
			},
			wantErr: true,
			errMsg:  "action must be 'allow' or 'deny'",
		},
		{
			name: "empty action",
			config: FilterConfig{
				Rules: []FilterRule{
					{Port: 22, Protocol: "tcp", Action: ""},
				},
			},
			wantErr: true,
			errMsg:  "action must be 'allow' or 'deny'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoadPeerConfig_WithFilter(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	content := `
name: "mynode"
filter:
  default_deny: true
  rules:
    - port: 22
      protocol: tcp
      action: allow
    - port: 80
      protocol: tcp
      action: allow
    - port: 53
      protocol: udp
      action: allow
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	assert.True(t, cfg.Filter.IsDefaultDeny())
	assert.Len(t, cfg.Filter.Rules, 3)
	assert.Equal(t, uint16(22), cfg.Filter.Rules[0].Port)
	assert.Equal(t, "tcp", cfg.Filter.Rules[0].Protocol)
	assert.Equal(t, uint8(6), cfg.Filter.Rules[0].ProtocolNumber())
	assert.Equal(t, "allow", cfg.Filter.Rules[0].Action)
}

func TestFilterRule_ProtocolNumber(t *testing.T) {
	tests := []struct {
		protocol string
		want     uint8
	}{
		{"tcp", 6},
		{"TCP", 6},
		{"udp", 17},
		{"UDP", 17},
		{"icmp", 0},
		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.protocol, func(t *testing.T) {
			rule := FilterRule{Protocol: tt.protocol}
			assert.Equal(t, tt.want, rule.ProtocolNumber())
		})
	}
}

// CLI-only Parameter Tests

func TestAuthTokenNotLoadedFromYAML(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Config file with auth_token field - should be ignored due to yaml:"-" tag
	content := `
name: "testnode"
auth_token: "should-be-ignored-0123456789abcdef0123456789abcdef0123456789abcdef"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	// AuthToken should be empty because of yaml:"-" tag
	assert.Empty(t, cfg.AuthToken, "AuthToken should not be loaded from YAML (CLI-only parameter)")
}

func TestServersNotLoadedFromYAML(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Config file with servers field - should be ignored due to yaml:"-" tag
	content := `
name: "testnode"
servers:
  - "https://coord1.example.com:8443"
  - "https://coord2.example.com:8443"
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	// Servers should be empty because of yaml:"-" tag
	assert.Empty(t, cfg.Servers, "Servers should not be loaded from YAML (CLI-only parameter)")
}

func TestCLIOnlyFieldsRemainEmpty(t *testing.T) {
	dir, cleanup := testutil.TempDir(t)
	defer cleanup()

	// Config with both CLI-only fields - both should be ignored
	content := `
name: "testnode"
servers:
  - "https://coord.example.com:8443"
auth_token: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
ssh_port: 2222
`
	configPath := testutil.TempFile(t, dir, "peer.yaml", content)

	cfg, err := LoadPeerConfig(configPath)
	require.NoError(t, err)

	// CLI-only fields should be empty
	assert.Empty(t, cfg.Servers, "Servers should not be loaded from YAML")
	assert.Empty(t, cfg.AuthToken, "AuthToken should not be loaded from YAML")

	// Regular fields should load normally
	assert.Equal(t, "testnode", cfg.Name)
	assert.Equal(t, 2222, cfg.SSHPort)
}
