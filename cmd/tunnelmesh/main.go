// tunnelmesh is the P2P SSH tunnel mesh network tool.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/tunnelmesh/tunnelmesh/internal/admin"
	"github.com/tunnelmesh/tunnelmesh/internal/benchmark"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	meshctx "github.com/tunnelmesh/tunnelmesh/internal/context"
	"github.com/tunnelmesh/tunnelmesh/internal/control"
	"github.com/tunnelmesh/tunnelmesh/internal/coord"
	meshdns "github.com/tunnelmesh/tunnelmesh/internal/dns"
	"github.com/tunnelmesh/tunnelmesh/internal/docker"
	"github.com/tunnelmesh/tunnelmesh/internal/logging/loki"
	"github.com/tunnelmesh/tunnelmesh/internal/mesh"
	"github.com/tunnelmesh/tunnelmesh/internal/metrics"
	"github.com/tunnelmesh/tunnelmesh/internal/netmon"
	"github.com/tunnelmesh/tunnelmesh/internal/peer"
	peerwg "github.com/tunnelmesh/tunnelmesh/internal/peer/wireguard"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
	"github.com/tunnelmesh/tunnelmesh/internal/svc"
	"github.com/tunnelmesh/tunnelmesh/internal/tracing"
	"github.com/tunnelmesh/tunnelmesh/internal/transport"
	sshtransport "github.com/tunnelmesh/tunnelmesh/internal/transport/ssh"
	udptransport "github.com/tunnelmesh/tunnelmesh/internal/transport/udp"
	"github.com/tunnelmesh/tunnelmesh/internal/tun"
	"github.com/tunnelmesh/tunnelmesh/internal/tunnel"
	"github.com/tunnelmesh/tunnelmesh/pkg/proto"
	"golang.org/x/crypto/ssh"
)

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

var (
	cfgFile  string
	logLevel string

	// WireGuard concentrator flag
	wireguardEnabled bool

	// Geolocation flags
	latitude  float64
	longitude float64
	city      string

	// Exit peer flags
	exitPeerFlag     string
	allowExitTraffic bool

	// Context flag for join command
	joinContext string

	// Deterministic key generation (TESTING ONLY)
	keygenSeed string

	// Server feature flags
	// Tracing flag
	enableTracing bool

	// Service mode flags (hidden, used when running as a service)
	serviceRun     bool
	serviceRunMode string
)

func main() {
	// Check if running as a service (invoked by service manager)
	if svc.IsServiceMode(os.Args) {
		runAsService()
		return
	}

	rootCmd := &cobra.Command{
		Use:   "tunnelmesh",
		Short: "TunnelMesh - P2P SSH tunnel mesh network",
		Long: `TunnelMesh creates encrypted P2P tunnels between peers using SSH.

QUICK START - Bootstrap coordinator (first node):

  # Generate a secure token (save this securely - you'll need it for peers):
  export TUNNELMESH_TOKEN=$(openssl rand -hex 32)

  # Start coordinator (no server URL = auto-coordinator mode):
  tunnelmesh join

  # Save token to secure file for later use:
  echo "$TUNNELMESH_TOKEN" > ~/.tunnelmesh/mesh-token.txt
  chmod 600 ~/.tunnelmesh/mesh-token.txt

QUICK START - Join an existing mesh:

  # Set token and join using coordinator URL:
  export TUNNELMESH_TOKEN="your-token"
  tunnelmesh join coord.example.com:8443 --context work

  # Identity is automatic - derived from your SSH key
  # Admin access: configure admin_peers in coordinator.yaml

  # Install as system service (optional):
  tunnelmesh service install

MANAGING BUCKETS:

  tunnelmesh buckets list
  tunnelmesh buckets create my-data
  tunnelmesh buckets objects my-data

For more help on any command, use: tunnelmesh <command> --help`,
	}

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file path")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "info", "log level")

	// Hidden service mode flags (used when running as a service)
	rootCmd.PersistentFlags().BoolVar(&serviceRun, "service-run", false, "Run as a service (internal use)")
	rootCmd.PersistentFlags().StringVar(&serviceRunMode, "service-mode", "", "Service mode: serve or join (internal use)")
	_ = rootCmd.PersistentFlags().MarkHidden("service-run")
	_ = rootCmd.PersistentFlags().MarkHidden("service-mode")

	// Join command
	joinCmd := &cobra.Command{
		Use:   "join [server-url]",
		Short: "Join the mesh network",
		Long: `Join the mesh network by connecting to a coordinator.

Examples:
  # Bootstrap a new mesh (first coordinator - no server URL)
  export TUNNELMESH_TOKEN=$(openssl rand -hex 32)
  tunnelmesh join
  # Save token securely: echo "$TUNNELMESH_TOKEN" > ~/.tunnelmesh/mesh-token.txt && chmod 600 $_

  # Join an existing mesh (regular peer)
  export TUNNELMESH_TOKEN="your-token"
  tunnelmesh join coord.example.com:8443

When no server URL is provided, automatically bootstraps as coordinator.
Server URLs automatically use HTTPS. Omit scheme in the URL.
Auth token must be set via TUNNELMESH_TOKEN environment variable.`,
		Args: cobra.MaximumNArgs(1),
		RunE: runJoin,
	}
	joinCmd.Flags().BoolVar(&wireguardEnabled, "wireguard", false, "enable WireGuard concentrator mode")
	joinCmd.Flags().Float64Var(&latitude, "latitude", 0, "manual geolocation latitude (-90 to 90)")
	joinCmd.Flags().Float64Var(&longitude, "longitude", 0, "manual geolocation longitude (-180 to 180)")
	joinCmd.Flags().StringVar(&city, "city", "", "city name for manual geolocation (shown in admin UI)")
	joinCmd.Flags().StringVar(&exitPeerFlag, "exit-peer", "", "name of peer to route internet traffic through")
	joinCmd.Flags().BoolVar(&allowExitTraffic, "allow-exit-traffic", false, "allow this peer to act as exit peer for other peers")
	joinCmd.Flags().BoolVar(&enableTracing, "enable-tracing", false, "enable runtime tracing (exposes /debug/trace endpoint)")
	joinCmd.Flags().StringVar(&joinContext, "context", "", "save/update context with this name after joining")
	joinCmd.Flags().StringVar(&keygenSeed, "keygen-seed", "", "seed for deterministic key generation (TESTING ONLY - reduces security)")
	rootCmd.AddCommand(joinCmd)

	// Status command
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show mesh status",
		RunE:  runStatus,
	}
	rootCmd.AddCommand(statusCmd)

	// Peers command
	peersCmd := &cobra.Command{
		Use:   "peers",
		Short: "List mesh peers",
		RunE:  runPeers,
	}
	rootCmd.AddCommand(peersCmd)

	// Resolve command
	resolveCmd := &cobra.Command{
		Use:   "resolve <hostname>",
		Short: "Resolve a mesh hostname",
		Args:  cobra.ExactArgs(1),
		RunE:  runResolve,
	}
	rootCmd.AddCommand(resolveCmd)

	// Leave command
	leaveCmd := &cobra.Command{
		Use:   "leave",
		Short: "Leave the mesh network",
		RunE:  runLeave,
	}
	rootCmd.AddCommand(leaveCmd)

	// Version command
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("tunnelmesh %s\n", Version)
			fmt.Printf("  Commit:     %s\n", Commit)
			fmt.Printf("  Build Time: %s\n", BuildTime)
		},
	}
	rootCmd.AddCommand(versionCmd)

	// Init command - generate keys and config
	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize tunnelmesh (generate keys and config)",
		Long: `Initialize TunnelMesh by generating SSH keys and example configuration files.

Examples:
  # Generate SSH keys only
  tunnelmesh init

  # Generate server config (coordinator)
  tunnelmesh init --server

  # Generate peer config
  tunnelmesh init --peer

  # Generate both (server that also joins as peer)
  tunnelmesh init --server --peer`,
		RunE: runInit,
	}
	initCmd.Flags().Bool("server", false, "Generate server.yaml config")
	initCmd.Flags().Bool("peer", false, "Generate peer.yaml config")
	initCmd.Flags().StringP("output", "o", ".", "Output directory for config files")
	rootCmd.AddCommand(initCmd)

	// Service command - manage system service
	rootCmd.AddCommand(newServiceCmd())

	// Context command - manage multiple mesh contexts
	rootCmd.AddCommand(newContextCmd())

	// Update command - self-update
	rootCmd.AddCommand(newUpdateCmd())

	// Trust CA command - install mesh CA cert in system trust store

	// Benchmark command - speed test between peers
	rootCmd.AddCommand(newBenchmarkCmd())

	// Filter command - manage packet filter rules
	rootCmd.AddCommand(newFilterCmd())

	// Buckets command - manage S3 buckets
	rootCmd.AddCommand(newBucketsCmd())

	// Group command - manage groups
	rootCmd.AddCommand(newGroupCmd())

	// Share command - manage file shares
	rootCmd.AddCommand(newShareCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// ensureCoordinatorConfig enables coordinator mode when no server URL is provided (bootstrap).
// This allows the first node in a mesh to automatically become a coordinator.
func ensureCoordinatorConfig(cfg *config.PeerConfig) {
	if len(cfg.Servers) == 0 && !cfg.Coordinator.Enabled {
		log.Warn().Msg("no server URL provided - automatically enabling coordinator mode for bootstrap (set TUNNELMESH_SERVER to join existing mesh)")
		cfg.Coordinator.Enabled = true
		// Set default coordinator listen if not configured
		if cfg.Coordinator.Listen == "" {
			cfg.Coordinator.Listen = ":8443"
		}
	}
}

// runAsService runs the application as a system service.
// This is called when the service manager starts the application with --service-run flag.
func runAsService() {
	// Set up logging directly to a file since launchd/kardianos-service
	// may not properly redirect stderr
	setupServiceLogging()
	logStartupBanner()

	// Parse the service-specific flags manually
	var mode, configPath string
	for i, arg := range os.Args {
		if arg == "--service-mode" && i+1 < len(os.Args) {
			mode = os.Args[i+1]
		}
		if (arg == "--config" || arg == "-c") && i+1 < len(os.Args) {
			configPath = os.Args[i+1]
		}
	}

	if mode == "" {
		log.Fatal().Msg("service mode not specified")
	}
	if configPath == "" {
		configPath = svc.DefaultConfigPath(mode)
	}

	log.Info().
		Str("mode", mode).
		Str("config", configPath).
		Msg("starting as service")

	// Create service configuration
	cfg := &svc.ServiceConfig{
		Name:        svc.DefaultServiceName(mode),
		DisplayName: svc.DefaultDisplayName(mode),
		Description: svc.DefaultDescription(mode),
		Mode:        mode,
		ConfigPath:  configPath,
	}

	// Create program with runner functions
	prg := &svc.Program{
		Mode:       mode,
		ConfigPath: configPath,
		RunJoin:    runJoinFromService,
	}

	// Run as service
	if err := svc.Run(prg, cfg); err != nil {
		log.Fatal().Err(err).Msg("service error")
	}
}

// isLocalhostURL returns true if the URL points to localhost/loopback addresses.
// These are considered safe for HTTP since traffic never leaves the machine.
func isLocalhostURL(u *url.URL) bool {
	host := u.Hostname() // strips port if present

	// Empty hostname is not localhost (reject malformed URLs like "http://:8443")
	if host == "" {
		return false
	}

	// Check common localhost names
	if host == "localhost" {
		return true
	}

	// Check IPv4/IPv6 loopback (127.0.0.0/8, ::1)
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			return true
		}
		// Allow private network ranges (for Docker, internal networks)
		// 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
		if ip.IsPrivate() {
			return true
		}
	}

	return false
}

// normalizeServerURL adds https:// scheme and :8443 port if missing.
func normalizeServerURL(serverURL string) (string, error) {
	if serverURL == "" {
		return "", nil
	}

	// Add scheme if missing
	if !strings.HasPrefix(serverURL, "http://") && !strings.HasPrefix(serverURL, "https://") {
		serverURL = "https://" + serverURL
	}

	// Parse URL to check for port
	parsedURL, err := url.Parse(serverURL)
	if err != nil {
		return "", fmt.Errorf("invalid server URL %q: %w", serverURL, err)
	}

	// Validate scheme (require HTTPS except for localhost or if explicitly allowed)
	allowHTTP := os.Getenv("TUNNELMESH_ALLOW_HTTP") == "true"
	if parsedURL.Scheme == "http" && !isLocalhostURL(parsedURL) && !allowHTTP {
		return "", fmt.Errorf("server URL must use HTTPS for remote servers (HTTP is only allowed for localhost): %q", serverURL)
	}

	// Warn if using HTTP with remote server (insecure)
	if parsedURL.Scheme == "http" && allowHTTP && !isLocalhostURL(parsedURL) {
		log.Warn().Msgf("Using HTTP for remote server %q (TUNNELMESH_ALLOW_HTTP=true) - THIS IS INSECURE", serverURL)
	}

	// Validate that path is empty or just "/"
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		return "", fmt.Errorf("server URL should not include path: %q", serverURL)
	}

	// Validate no query parameters
	if parsedURL.RawQuery != "" {
		return "", fmt.Errorf("server URL should not include query parameters: %q", serverURL)
	}

	// Add default port :8443 if no port specified
	if parsedURL.Port() == "" {
		parsedURL.Host += ":8443"
		serverURL = parsedURL.String()
	}

	return serverURL, nil
}

// runServeFromService runs the server mode from within a service.
// runJoinFromService runs the peer mode from within a service.
func runJoinFromService(ctx context.Context, configPath string) error {
	log.Info().Str("config_path", configPath).Msg("runJoinFromService starting")

	var cfg *config.PeerConfig
	var err error

	if configPath != "" {
		cfg, err = config.LoadPeerConfig(configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
	} else {
		return fmt.Errorf("config file required")
	}

	// Apply configured log level
	if config.ApplyLogLevel(cfg.LogLevel) {
		log.Info().Str("level", cfg.LogLevel).Msg("log level configured")
	}

	// When running as service, read server URL and token from environment
	if envServer := os.Getenv("TUNNELMESH_SERVER"); envServer != "" {
		// Normalize server URL: add https:// and :8443 if missing
		normalized, err := normalizeServerURL(envServer)
		if err != nil {
			return fmt.Errorf("normalize TUNNELMESH_SERVER env var: %w", err)
		}
		cfg.Servers = []string{normalized}
		log.Info().Str("server", normalized).Msg("loaded server from TUNNELMESH_SERVER env var")
	}
	if envToken := os.Getenv("TUNNELMESH_TOKEN"); envToken != "" {
		if !isValidAuthToken(envToken) {
			return fmt.Errorf("invalid TUNNELMESH_TOKEN: must be 64 hex characters (generate with: openssl rand -hex 32)")
		}
		cfg.AuthToken = envToken
		log.Info().Msg("loaded auth token from TUNNELMESH_TOKEN env var")
	}

	log.Info().
		Str("servers", strings.Join(cfg.Servers, ", ")).
		Int("ssh_port", cfg.SSHPort).
		Msg("config loaded")

	// Enable coordinator mode for bootstrap if no server URL
	ensureCoordinatorConfig(cfg)

	if cfg.AuthToken == "" {
		return fmt.Errorf("auth token required: set TUNNELMESH_TOKEN environment variable")
	}

	// Log network interface information for debugging
	interfaces, _ := net.Interfaces()
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagLoopback == 0 {
			addrs, _ := iface.Addrs()
			for _, addr := range addrs {
				log.Debug().
					Str("interface", iface.Name).
					Str("address", addr.String()).
					Msg("network interface")
			}
		}
	}

	return runJoinWithConfig(ctx, cfg)
}

// nolint:revive // args required by cobra.Command RunE signature
func runInit(cmd *cobra.Command, args []string) error {
	setupLogging()

	genServer, _ := cmd.Flags().GetBool("server")
	genPeer, _ := cmd.Flags().GetBool("peer")
	outputDir, _ := cmd.Flags().GetString("output")

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("get home dir: %w", err)
	}

	keyDir := filepath.Join(homeDir, ".tunnelmesh")
	keyPath := filepath.Join(keyDir, "id_ed25519")

	// Generate keys if they don't exist
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		if err := config.GenerateKeyPair(keyPath); err != nil {
			return fmt.Errorf("generate keys: %w", err)
		}
		fmt.Printf("SSH keys generated: %s\n", keyPath)
	} else {
		fmt.Printf("SSH keys exist: %s\n", keyPath)
	}

	// Generate auth token for configs
	authToken := generateAuthToken()

	// Generate server config
	if genServer {
		serverPath := filepath.Join(outputDir, "server.yaml")
		if err := writeServerConfig(serverPath, authToken, genPeer); err != nil {
			return fmt.Errorf("write server config: %w", err)
		}
		fmt.Printf("Server config generated: %s\n", serverPath)
	}

	// Generate peer config
	if genPeer && !genServer {
		// Only generate standalone peer config if not combined with server
		peerPath := filepath.Join(outputDir, "peer.yaml")
		if err := writePeerConfig(peerPath, authToken); err != nil {
			return fmt.Errorf("write peer config: %w", err)
		}
		fmt.Printf("Peer config generated: %s\n", peerPath)
	}

	if genServer {
		fmt.Println("\nNext steps:")
		fmt.Println("  1. Edit server.yaml with your settings")
		fmt.Println("  2. tunnelmesh serve --config server.yaml")
		fmt.Println("  3. Configure admin_peers field with peer names or IDs for admin access")
	} else if genPeer {
		fmt.Println("\nNext steps:")
		fmt.Println("  1. Edit peer.yaml with server URL and token")
		fmt.Println("  2. tunnelmesh join --config peer.yaml --context <name>")
		fmt.Println("  Identity is automatic - derived from your SSH key")
	}

	return nil
}

func generateAuthToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func writeServerConfig(path, authToken string, includePeer bool) error {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "coordinator"
	}

	// Generate unified peer config with coordinator section
	config := fmt.Sprintf(`# TunnelMesh Peer Config - Coordinator Mode
name: "%s"
servers:
  - "http://localhost:8080"
auth_token: "%s"

dns:
  enabled: true

coordinator:
  enabled: true
  listen: ":8080"

  admin:
    enabled: true

  relay:
    enabled: true

  s3:
    enabled: true
`, hostname, authToken)

	return os.WriteFile(path, []byte(config), 0600)
}

func writePeerConfig(path, authToken string) error {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "mynode"
	}

	config := fmt.Sprintf(`# TunnelMesh Peer - see peer.yaml.example for all options
name: "%s"

# Server URL passed as positional arg, auth token via environment variable:
# export TUNNELMESH_TOKEN="your-token"
# tunnelmesh join coord.example.com:8443 --config peer.yaml

dns:
  enabled: true
`, hostname)

	return os.WriteFile(path, []byte(config), 0600)
}

func runJoin(cmd *cobra.Command, args []string) error {
	setupLogging()
	logStartupBanner()

	// Display security warning if using deterministic key generation
	if keygenSeed != "" {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		fmt.Fprintln(os.Stderr, "@           SECURITY WARNING: DETERMINISTIC KEYS         @")
		fmt.Fprintln(os.Stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Using --keygen-seed generates SSH keys deterministically from")
		fmt.Fprintln(os.Stderr, "the provided seed value. This REDUCES SECURITY and should")
		fmt.Fprintln(os.Stderr, "ONLY be used for testing environments.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "  ⚠️  Predictable keys are vulnerable to cryptographic attacks")
		fmt.Fprintln(os.Stderr, "  ⚠️  Anyone with the seed can generate your private keys")
		fmt.Fprintln(os.Stderr, "  ⚠️  Do NOT use in production or with sensitive data")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "For production deployments, remove --keygen-seed to use")
		fmt.Fprintln(os.Stderr, "cryptographically secure random key generation.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		fmt.Fprintln(os.Stderr)
	}

	// Initialize tracing if enabled
	if enableTracing {
		if err := tracing.Init(true, tracing.DefaultBufferSize); err != nil {
			log.Warn().Err(err).Msg("failed to initialize tracing")
		} else {
			log.Info().Msg("runtime tracing enabled")
			defer tracing.Stop()
		}
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// Get server URL from positional argument
	var joinServerURL string
	if len(args) > 0 {
		joinServerURL = args[0]
	}

	// Normalize server URL: add https:// if no scheme provided, add :8443 if no port
	// Server URL is CLI-only, not a config option
	if joinServerURL != "" {
		normalized, err := normalizeServerURL(joinServerURL)
		if err != nil {
			return err
		}
		cfg.Servers = []string{normalized}
	}

	// Read auth token from environment variable (never from CLI for security)
	if envToken := os.Getenv("TUNNELMESH_TOKEN"); envToken != "" {
		if !isValidAuthToken(envToken) {
			return fmt.Errorf("invalid TUNNELMESH_TOKEN: must be 64 hex characters (generate with: openssl rand -hex 32)")
		}
		cfg.AuthToken = envToken
	}

	if wireguardEnabled {
		cfg.WireGuard.Enabled = true
	}
	if latitude != 0 || longitude != 0 {
		cfg.Geolocation.Latitude = latitude
		cfg.Geolocation.Longitude = longitude
	}
	if city != "" {
		cfg.Geolocation.City = city
	}
	if exitPeerFlag != "" {
		cfg.ExitPeer = exitPeerFlag
	}
	if allowExitTraffic {
		cfg.AllowExitTraffic = true
	}

	// Enable coordinator mode for bootstrap if no server URL
	ensureCoordinatorConfig(cfg)

	if cfg.AuthToken == "" {
		return fmt.Errorf("auth token required: set TUNNELMESH_TOKEN environment variable\nExample: export TUNNELMESH_TOKEN=\"your-token\" && tunnelmesh join coord.example.com:8443")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Info().Msg("shutting down...")
		cancel()
	}()

	return runJoinWithConfig(ctx, cfg)
}

// OnJoinedFunc is called after successfully joining the mesh.
// It receives the mesh IP, TLS manager (if TLS cert was provided), and packet filter.
type OnJoinedFunc func(meshIP string, tlsMgr *peer.TLSManager, filter *routing.PacketFilter)

func runJoinWithConfig(ctx context.Context, cfg *config.PeerConfig) error {
	return runJoinWithConfigAndCallback(ctx, cfg, nil)
}

// nolint:gocyclo // Main join logic is inherently complex due to protocol state machine
// discoverAndRegisterWithCoordinator discovers coordinators and tries to register with them.
// Discovery hierarchy: Static config → DNS SRV → Retry with exponential backoff
// Returns the successful client and registration response.
func discoverAndRegisterWithCoordinator(
	ctx context.Context,
	cfg *config.PeerConfig,
	publicKey string,
	publicIPs, privateIPs []string,
	sshPort, udpPort int,
	behindNAT bool,
	version string,
	location *proto.GeoLocation,
) (*coord.Client, *proto.RegisterResponse, error) {
	// Build list of coordinators to try
	coordinators := make([]string, 0, len(cfg.Servers)+10)

	// Use static config servers (coordinators discover each other via peer list)
	coordinators = append(coordinators, cfg.Servers...)

	if len(coordinators) == 0 {
		return nil, nil, fmt.Errorf("no coordinators configured (check servers config)")
	}

	// Try each coordinator until one succeeds
	var lastErr error
	for _, coordURL := range coordinators {
		log.Info().Str("coordinator", coordURL).Msg("attempting registration")

		client := coord.NewClient(coordURL, cfg.AuthToken)
		hasMonitoring := cfg.Coordinator.Enabled &&
			(cfg.Coordinator.Monitoring.PrometheusURL != "" || cfg.Coordinator.Monitoring.GrafanaURL != "")
		resp, err := client.RegisterWithRetry(
			ctx,
			cfg.Name,
			publicKey,
			publicIPs,
			privateIPs,
			sshPort,
			udpPort,
			behindNAT,
			version,
			location,
			cfg.ExitPeer,
			cfg.AllowExitTraffic,
			cfg.DNS.Aliases,
			cfg.Coordinator.Enabled,
			hasMonitoring,
			coord.DefaultRetryConfig(),
		)

		if err == nil {
			log.Info().
				Str("coordinator", coordURL).
				Str("mesh_ip", resp.MeshIP).
				Msg("successfully registered with coordinator")

			return client, resp, nil
		}

		lastErr = err
		log.Warn().
			Err(err).
			Str("coordinator", coordURL).
			Msg("failed to register with coordinator, trying next")
	}

	return nil, nil, fmt.Errorf("failed to register with any coordinator (tried %d): %w", len(coordinators), lastErr)
}

//nolint:gocyclo // Join function coordinates multiple complex subsystems (peer, coordinator, TUN, DNS, etc)
func runJoinWithConfigAndCallback(ctx context.Context, cfg *config.PeerConfig, onJoined OnJoinedFunc) error {
	// Use configured name if provided, otherwise fallback to hostname
	if cfg.Name == "" {
		cfg.Name, _ = os.Hostname()
	}

	// Enable coordinator mode for bootstrap if no server URL
	ensureCoordinatorConfig(cfg)

	if cfg.AuthToken == "" {
		return fmt.Errorf("auth token required: set TUNNELMESH_TOKEN environment variable")
	}
	if !isValidAuthToken(cfg.AuthToken) {
		return fmt.Errorf("invalid auth token: must be 64 hex characters (generate with: openssl rand -hex 32)")
	}

	// Apply configured log level from config file
	if config.ApplyLogLevel(cfg.LogLevel) {
		log.Info().Str("level", cfg.LogLevel).Msg("log level configured")
	}

	// Tune GC for lower latency in packet forwarding
	// Start coordinator services if enabled (before registration)
	// This allows this peer to register with itself if it's the bootstrap coordinator
	var srv *coord.Server
	if cfg.Coordinator.Enabled {
		log.Info().Msg("coordinator mode enabled, starting coordination server")

		// Create coordinator server
		var err error
		srv, err = coord.NewServer(ctx, cfg)
		if err != nil {
			return fmt.Errorf("create coordinator: %w", err)
		}
		srv.SetVersion(Version)

		// Note: Cleanup handled by signal handler in production, t.Cleanup() in tests

		// Start periodic background tasks
		srv.StartPeriodicSave(ctx)
		srv.StartPeriodicCleanup(ctx)

		// Start replicator if clustering is enabled
		if err := srv.StartReplicator(); err != nil {
			log.Warn().Err(err).Msg("failed to start replicator (will run without replication)")
		}

		// Start coordinator HTTP server in background
		go func() {
			if err := srv.ListenAndServe(); err != nil {
				log.Error().Err(err).Msg("coordinator server error - server stopped")
				// Note: Process continues running but coordinator services unavailable
			}
		}()

		// Wait for coordinator server to be ready (up to 5 seconds)
		localServerURL := "http://127.0.0.1" + cfg.Coordinator.Listen
		healthURL := localServerURL + "/health"
		ready := false
		for i := 0; i < 50; i++ {
			resp, err := http.Get(healthURL)
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					ready = true
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !ready {
			return fmt.Errorf("coordinator server failed to become ready after 5 seconds")
		}

		// If no servers configured, use localhost coordinator
		if len(cfg.Servers) == 0 {
			cfg.Servers = []string{localServerURL}
			log.Info().
				Str("coordinator", localServerURL).
				Msg("using local coordinator for bootstrap")
		}

		log.Info().
			Str("listen", cfg.Coordinator.Listen).
			Msg("coordinator server started, now joining mesh as peer")
	}

	// Ensure keys exist (use deterministic seed if provided)
	signer, err := config.EnsureKeyPairExists(cfg.PrivateKey, keygenSeed)
	if err != nil {
		return fmt.Errorf("load keys: %w", err)
	}

	pubKeyEncoded := config.EncodePublicKey(signer.PublicKey())
	pubKeyFP := config.GetPublicKeyFingerprint(signer.PublicKey())
	log.Info().Str("fingerprint", pubKeyFP).Msg("using SSH key")

	// Get local IPs
	publicIPs, privateIPs, behindNAT := proto.GetLocalIPs()
	log.Debug().
		Strs("public", publicIPs).
		Strs("private", privateIPs).
		Bool("behind_nat", behindNAT).
		Msg("detected local IPs")

	// UDP port is SSH port + 1
	udpPort := cfg.SSHPort + 1

	// Build location from config if set
	var location *proto.GeoLocation
	if cfg.Geolocation.IsSet() {
		location = &proto.GeoLocation{
			Latitude:  cfg.Geolocation.Latitude,
			Longitude: cfg.Geolocation.Longitude,
			City:      cfg.Geolocation.City,
			Source:    "manual",
		}
		log.Info().
			Float64("latitude", location.Latitude).
			Float64("longitude", location.Longitude).
			Str("city", location.City).
			Msg("geolocation configured")
	}

	// Discover and connect to coordination server
	client, resp, err := discoverAndRegisterWithCoordinator(
		ctx, cfg, pubKeyEncoded, publicIPs, privateIPs,
		cfg.SSHPort, udpPort, behindNAT, Version, location,
	)
	if err != nil {
		return fmt.Errorf("register with server: %w", err)
	}

	log.Info().
		Str("mesh_ip", resp.MeshIP).
		Str("mesh_cidr", resp.MeshCIDR).
		Str("domain", resp.Domain).
		Msg("joined mesh network")

	// Check for version mismatch between client and server
	if resp.ServerVersion != "" && resp.ServerVersion != Version {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		fmt.Fprintln(os.Stderr, "@              VERSION MISMATCH DETECTED                @")
		fmt.Fprintln(os.Stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		fmt.Fprintf(os.Stderr, "  Client version: %s\n", Version)
		fmt.Fprintf(os.Stderr, "  Server version: %s\n", resp.ServerVersion)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Consider updating your client: tunnelmesh update")
		fmt.Fprintln(os.Stderr)
	}

	// Save/update context if --context flag is set
	// Skip when running as a service to avoid overwriting user's context file with root ownership
	if joinContext != "" && !serviceRun {
		store, err := meshctx.Load()
		if err != nil {
			log.Warn().Err(err).Msg("failed to load context store")
		} else {
			ctx := meshctx.Context{
				Name:       joinContext,
				ConfigPath: cfgFile,
				PeerName:   cfg.Name, // Save peer name so it persists across rejoins
				Server:     cfg.PrimaryServer(),
				Domain:     resp.Domain,
				MeshIP:     resp.MeshIP,
				DNSListen:  cfg.DNS.Listen,
			}
			// If no explicit config path, try to find what we used
			if ctx.ConfigPath == "" {
				if activeCtx := store.GetActive(); activeCtx != nil && activeCtx.Name == joinContext {
					ctx.ConfigPath = activeCtx.ConfigPath
				}
			}
			store.Add(ctx)
			// Make it active if it's the first context or was already active
			if store.Count() == 1 || store.Active == "" || store.Active == joinContext {
				store.Active = joinContext
			}
			if err := store.Save(); err != nil {
				log.Warn().Err(err).Msg("failed to save context")
			} else {
				log.Info().Str("context", joinContext).Msg("context saved")
			}
		}
	}

	// Store TLS certificate if provided
	var tlsMgr *peer.TLSManager
	if resp.TLSCert != "" && resp.TLSKey != "" {
		// Use the directory of the private key for TLS storage
		tlsDataDir := filepath.Dir(cfg.PrivateKey)
		tlsMgr = peer.NewTLSManager(tlsDataDir)
		if err := tlsMgr.StoreCert([]byte(resp.TLSCert), []byte(resp.TLSKey)); err != nil {
			log.Warn().Err(err).Msg("failed to store TLS certificate")
		} else {
			log.Info().
				Str("cert", tlsMgr.CertPath()).
				Str("key", tlsMgr.KeyPath()).
				Msg("TLS certificate stored")
		}

		// Fetch and store CA certificate locally
		caPEM, err := FetchCA(cfg.PrimaryServer())
		if err != nil {
			return fmt.Errorf("fetch CA certificate: %w", err)
		}
		if err := tlsMgr.StoreCA(caPEM); err != nil {
			return fmt.Errorf("store CA certificate: %w", err)
		}
		log.Info().Str("ca", tlsMgr.CAPath()).Msg("CA certificate stored")

		// Install/reinstall CA certificate automatically
		if err := InstallCA(caPEM, ""); err != nil {
			log.Warn().Err(err).Msg("failed to install CA certificate (you may need sudo)")
		}
	}

	// Create TUN device
	tunCfg := tun.Config{
		Name:    cfg.TUN.Name,
		MTU:     cfg.TUN.MTU,
		Address: resp.MeshIP + "/" + strings.Split(resp.MeshCIDR, "/")[1],
	}

	log.Info().
		Str("name", tunCfg.Name).
		Int("mtu", tunCfg.MTU).
		Str("address", tunCfg.Address).
		Msg("creating TUN device")

	tunDev, err := tun.Create(tunCfg)
	if err != nil {
		log.Error().Err(err).Msg("failed to create TUN device (requires root)")
		// Continue without TUN for now
	} else {
		defer func() { _ = tunDev.Close() }()
		log.Info().
			Str("name", tunDev.Name()).
			Str("ip", resp.MeshIP).
			Msg("TUN device created")
	}

	// Create peer identity and mesh node
	identity := peer.NewPeerIdentity(cfg, pubKeyEncoded, udpPort, Version, resp)
	identity.Location = location // Set location for heartbeat reporting

	// Log if peer was renamed due to hostname conflict
	if resp.PeerName != "" && resp.PeerName != cfg.Name {
		log.Info().
			Str("original", cfg.Name).
			Str("assigned", resp.PeerName).
			Msg("hostname conflict - using assigned name")
	}
	node := peer.NewMeshNode(identity, client)

	// Set up forwarder with node's tunnel manager and router
	forwarder := routing.NewForwarder(node.Router(), node.TunnelMgr())
	if tunDev != nil {
		forwarder.SetTUN(tunDev)
		forwarder.SetLocalIP(net.ParseIP(resp.MeshIP))
	}
	node.Forwarder = forwarder

	// Initialize packet filter from config
	filter := routing.NewPacketFilter(cfg.Filter.IsDefaultDeny())
	if len(cfg.Filter.Rules) > 0 {
		filterRules := make([]routing.FilterRule, 0, len(cfg.Filter.Rules))
		for _, r := range cfg.Filter.Rules {
			filterRules = append(filterRules, routing.FilterRule{
				Port:     r.Port,
				Protocol: r.ProtocolNumber(),
				Action:   routing.ParseFilterAction(r.Action),
			})
		}
		filter.SetPeerConfigRules(filterRules)
		log.Info().Int("rules", len(filterRules)).Msg("loaded filter rules from config")
	}
	forwarder.SetFilter(filter)

	// Notify callback if provided (used by server to start admin HTTPS and configure Docker)
	// Must be after TUN device and packet filter creation
	if onJoined != nil {
		onJoined(resp.MeshIP, tlsMgr, filter)
	}

	// Start coordinator admin services if running as coordinator
	if srv != nil && tlsMgr != nil {
		srv.SetCoordMeshIP(resp.MeshIP)
		srv.SetMetricsRegistry(metrics.Registry)

		// Add coordinator peers discovered during registration.
		// The registration response includes IPs of all known coordinators,
		// enabling immediate bidirectional replication.
		if replicator := srv.GetReplicator(); replicator != nil && len(resp.Coordinators) > 0 {
			for _, coordIP := range resp.Coordinators {
				name := resp.CoordinatorNames[coordIP]
				if name == "" {
					name = coordIP // fallback for older servers
				}
				replicator.AddPeer(coordIP, name)
			}
			log.Info().
				Int("peers", len(resp.Coordinators)).
				Msg("added coordinator peers from registration response")
		}

		// Load TLS certificate once for all services
		// TLS is mandatory for coordinators (admin, S3, NFS all require HTTPS)
		tlsCert, err := tls.LoadX509KeyPair(tlsMgr.CertPath(), tlsMgr.KeyPath())
		if err != nil {
			return fmt.Errorf("failed to load TLS certificate for coordinator services: %w", err)
		}

		// Configure mesh TLS for inter-coordinator replication and forwarding.
		// Trust the CA from registration (not the locally-generated CA) since all
		// coordinator certs are signed by the primary's CA. Include our own cert
		// for client authentication.
		caPEM, caErr := os.ReadFile(tlsMgr.CAPath())
		if caErr != nil {
			return fmt.Errorf("failed to load mesh CA for inter-coordinator TLS: %w", caErr)
		}
		caPool := x509.NewCertPool()
		caPool.AppendCertsFromPEM(caPEM)
		srv.SetMeshTLS(&tls.Config{
			Certificates: []tls.Certificate{tlsCert},
			RootCAs:      caPool,
			MinVersion:   tls.VersionTLS12,
		})

		{
			// Start admin HTTPS server if enabled
			// Admin always uses port 443 (standard HTTPS) on mesh IP for consistency
			// This ensures coordinator replication works (port must be predictable)
			// Admin is always enabled for coordinators
			adminAddr := net.JoinHostPort(resp.MeshIP, "443")
			if err := srv.StartAdminServer(adminAddr, &tlsCert); err != nil {
				log.Error().Err(err).Msg("failed to start admin server")
			}

			// Always start S3 and NFS servers on coordinators
			// S3 port hardcoded to 9000 (mesh-internal, critical for replication)
			s3Addr := net.JoinHostPort(resp.MeshIP, "9000")
			if err := srv.StartS3Server(s3Addr, &tlsCert); err != nil {
				log.Error().Err(err).Msg("failed to start S3 server")
			}

			// Start NFS server (automatically enabled with S3)
			nfsAddr := net.JoinHostPort(resp.MeshIP, fmt.Sprintf("%d", coord.NFSPort))
			if err := srv.StartNFSServer(nfsAddr, &tlsCert); err != nil {
				log.Error().Err(err).Msg("failed to start NFS server")
			}
		}

		// Start Docker manager for coordinator
		if cfg.Docker.Socket != "" {
			dockerMgr := docker.NewManager(&cfg.Docker, cfg.Name, filter, srv.GetSystemStore(), metrics.Registry)
			if err := dockerMgr.Start(ctx); err != nil {
				log.Warn().Err(err).Msg("failed to start Docker manager")
			} else {
				srv.SetDockerManager(dockerMgr)
			}
		}
	}

	// Start control socket for CLI commands
	socketPath := cfg.ControlSocket
	if socketPath == "" {
		socketPath = control.DefaultSocketPath()
	}
	ctrlServer := control.NewServer(socketPath, filter, cfg.Name)
	if err := ctrlServer.Start(); err != nil {
		log.Warn().Err(err).Msg("failed to start control socket, CLI commands may not work")
	} else {
		defer func() { _ = ctrlServer.Stop() }()
	}

	// Initialize Docker manager for automatic port forwarding (regular peers only)
	// Coordinators already initialized Docker manager above with system store
	if srv == nil {
		// Auto-detect Docker socket if not configured
		if cfg.Docker.Socket == "" {
			if _, err := os.Stat("/var/run/docker.sock"); err == nil {
				cfg.Docker.Socket = "unix:///var/run/docker.sock"
				log.Debug().Msg("Auto-detected Docker socket at /var/run/docker.sock")
			}
		}
		if cfg.Docker.Socket != "" {
			dockerMgr := docker.NewManager(&cfg.Docker, cfg.Name, filter, nil, metrics.Registry)
			if err := dockerMgr.Start(ctx); err != nil {
				log.Warn().Err(err).Msg("failed to start Docker manager")
			} else {
				defer func() { _ = dockerMgr.Stop() }()
				log.Info().Msg("Docker manager started")
			}
		}
	}

	// Configure exit node settings
	if tunDev != nil {
		// Parse mesh CIDR for split-tunnel detection
		_, meshNet, err := net.ParseCIDR(resp.MeshCIDR)
		if err != nil {
			log.Warn().Err(err).Str("cidr", resp.MeshCIDR).Msg("failed to parse mesh CIDR, exit routing disabled")
		} else {
			forwarder.SetMeshCIDR(meshNet)

			exitCfg := tun.ExitRouteConfig{
				InterfaceName: tunDev.Name(),
				MeshCIDR:      resp.MeshCIDR,
			}

			// Configure exit node routing (client side)
			if cfg.ExitPeer != "" {
				// Strip domain suffix if present (allow both "peer" and "peer.tunnelmesh")
				exitNodeName := cfg.ExitPeer
				if resp.Domain != "" && strings.HasSuffix(exitNodeName, resp.Domain) {
					exitNodeName = strings.TrimSuffix(exitNodeName, resp.Domain)
				}
				forwarder.SetExitPeer(exitNodeName)
				log.Info().Str("exit_node", exitNodeName).Msg("exit node configured, internet traffic will route through peer")

				if err := tun.ConfigureExitRoutes(exitCfg); err != nil {
					log.Warn().Err(err).Msg("failed to configure exit routes, manual setup may be required")
				} else {
					log.Info().Msg("exit routes configured (0.0.0.0/1, 128.0.0.0/1 via TUN)")
				}
			}

			// Configure NAT for exit node (server side)
			if cfg.AllowExitTraffic {
				log.Info().Msg("this node allows exit traffic from other peers")
				exitCfg.IsExitPeer = true

				if err := tun.ConfigureExitNAT(exitCfg); err != nil {
					log.Warn().Err(err).Msg("failed to configure exit NAT, manual setup may be required")
				} else {
					log.Info().Msg("exit NAT configured (IP forwarding + masquerade)")
				}
			}
		}
	}

	// When forwarder detects a dead tunnel (write fails), disconnect the peer
	// This removes the tunnel and triggers discovery for reconnection
	forwarder.SetOnDeadTunnel(func(peerName string) {
		if pc := node.Connections.Get(peerName); pc != nil {
			log.Debug().Str("peer", peerName).Msg("forwarder detected dead tunnel, disconnecting peer")
			_ = pc.Disconnect("tunnel write failed", nil)
		}
	})

	// Create transport registry with default order: UDP -> SSH
	// UDP is first for better performance (lower latency, no head-of-line blocking)
	// Falls back to SSH when UDP hole-punching fails or times out
	// Relay traffic is handled by PersistentRelay separately (DERP-like architecture)
	transportRegistry := transport.NewRegistry(transport.RegistryConfig{
		DefaultOrder: []transport.TransportType{
			transport.TransportUDP,
			transport.TransportSSH,
		},
	})
	node.TransportRegistry = transportRegistry

	// Create and register SSH transport
	sshTransport, err := sshtransport.New(sshtransport.Config{
		HostKey:      signer,
		ClientSigner: signer,
		ListenPort:   cfg.SSHPort,
	})
	if err != nil {
		return fmt.Errorf("create SSH transport: %w", err)
	}
	if err := transportRegistry.Register(sshTransport); err != nil {
		return fmt.Errorf("register SSH transport: %w", err)
	}
	node.SSHTransport = sshTransport

	// Start SSH listener via transport layer
	sshListener, err := sshTransport.Listen(ctx, transport.ListenOptions{
		Port: cfg.SSHPort,
	})
	if err != nil {
		return fmt.Errorf("create SSH listener: %w", err)
	}
	go node.HandleIncomingSSH(ctx, sshListener)
	log.Info().Int("port", cfg.SSHPort).Msg("SSH transport listening")

	// Create and register UDP transport (for lower latency when direct connection possible)
	edPrivKey, err := config.LoadED25519PrivateKey(cfg.PrivateKey)
	if err != nil {
		log.Warn().Err(err).Msg("failed to load ED25519 key for UDP transport, UDP disabled")
	} else {
		x25519Priv, x25519Pub, err := config.DeriveX25519KeyPair(edPrivKey)
		if err != nil {
			log.Warn().Err(err).Msg("failed to derive X25519 keys for UDP transport, UDP disabled")
		} else {
			var privKey, pubKey [32]byte
			copy(privKey[:], x25519Priv)
			copy(pubKey[:], x25519Pub)

			// Debug: Also convert ED25519 public key directly to verify matching
			ed25519PubBytes := edPrivKey.Public().(ed25519.PublicKey)
			x25519FromEd, _ := config.ED25519PublicToX25519(ed25519PubBytes)
			log.Debug().
				Hex("x25519_from_priv", x25519Pub).
				Hex("x25519_from_ed_pub", x25519FromEd).
				Bool("match", string(x25519Pub) == string(x25519FromEd)).
				Msg("X25519 key derivation check")

			// Cache for peer X25519 keys -> peer names
			// This allows handshakes to succeed even during network transitions
			// when API calls might fail temporarily
			var peerKeyCache sync.Map // [32]byte -> string

			// Helper to update cache from peers list
			updatePeerKeyCache := func(peers []proto.Peer) {
				for _, p := range peers {
					if p.PublicKey == "" {
						continue
					}
					// Parse the peer's SSH public key
					sshPubKey, err := config.DecodePublicKey(p.PublicKey)
					if err != nil {
						continue
					}
					cryptoPubKey, ok := sshPubKey.(ssh.CryptoPublicKey)
					if !ok {
						continue
					}
					edPubKey, ok := cryptoPubKey.CryptoPublicKey().(ed25519.PublicKey)
					if !ok {
						continue
					}
					peerX25519, err := config.ED25519PublicToX25519(edPubKey)
					if err != nil {
						continue
					}
					if len(peerX25519) == 32 {
						var peerKey [32]byte
						copy(peerKey[:], peerX25519)
						peerKeyCache.Store(peerKey, p.Name)
					}
				}
			}

			// Create peer resolver for incoming UDP connections
			peerResolver := func(x25519PubKey [32]byte) string {
				// Try to fetch fresh peer list
				peers, err := client.ListPeers()
				if err != nil {
					// API call failed (likely during network transition)
					// Fall back to cached data
					log.Debug().Err(err).Msg("failed to list peers for resolver, using cache")
					if name, ok := peerKeyCache.Load(x25519PubKey); ok {
						log.Debug().Str("peer", name.(string)).Msg("resolved peer from cache")
						return name.(string)
					}
					return ""
				}

				// Update cache with fresh data
				updatePeerKeyCache(peers)

				// Look up the peer
				if name, ok := peerKeyCache.Load(x25519PubKey); ok {
					return name.(string)
				}
				return ""
			}

			udpTransport, err := udptransport.New(udptransport.Config{
				Port:           cfg.SSHPort + 1, // Use SSH port + 1 for UDP
				LocalPeerName:  cfg.Name,
				StaticPrivate:  privKey,
				StaticPublic:   pubKey,
				CoordServerURL: cfg.PrimaryServer(),
				AuthToken:      cfg.AuthToken,
				PeerResolver:   peerResolver,
			})
			if err != nil {
				log.Warn().Err(err).Msg("failed to create UDP transport, UDP disabled")
			} else {
				// Wire up session invalidation callback for rekey-required handling
				node.SetupUDPSessionInvalidCallback(udpTransport)

				if err := transportRegistry.Register(udpTransport); err != nil {
					log.Warn().Err(err).Msg("failed to register UDP transport")
				} else {
					log.Info().Int("port", cfg.SSHPort+1).Msg("UDP transport registered")
					// Start UDP transport to accept incoming connections
					if err := udpTransport.Start(); err != nil {
						log.Warn().Err(err).Msg("failed to start UDP transport")
					} else {
						log.Info().Int("port", cfg.SSHPort+1).Msg("UDP transport started")
						// Start listening for incoming UDP connections
						udpListener, err := udpTransport.Listen(ctx, transport.ListenOptions{})
						if err != nil {
							log.Warn().Err(err).Msg("failed to create UDP listener")
						} else {
							go node.HandleIncomingUDP(ctx, udpListener)
							log.Info().Int("port", cfg.SSHPort+1).Msg("UDP listener started")
						}
					}
					// Register UDP endpoint with coordination server for hole-punching
					if err := udpTransport.RegisterUDPEndpoint(ctx, cfg.Name); err != nil {
						log.Warn().Err(err).Msg("failed to register UDP endpoint")
					}

					// Start periodic UDP endpoint refresh (endpoints expire after 5 minutes)
					go func() {
						ticker := time.NewTicker(2 * time.Minute)
						defer ticker.Stop()
						for {
							select {
							case <-ctx.Done():
								return
							case <-ticker.C:
								if err := udpTransport.RegisterUDPEndpoint(ctx, cfg.Name); err != nil {
									log.Debug().Err(err).Msg("failed to refresh UDP endpoint registration")
								} else {
									log.Debug().Msg("UDP endpoint registration refreshed")
								}
							}
						}
					}()

					// Create and start latency prober for peer latency measurement
					node.LatencyProber = peer.NewLatencyProber(udpTransport)
					go node.LatencyProber.Start(ctx)
					log.Info().Msg("latency prober started for peer RTT measurement")
				}
			}
		}
	}

	// Create transport negotiator
	// Note: Relay traffic is handled by PersistentRelay separately (DERP-like architecture)
	transportNegotiator := transport.NewNegotiator(transportRegistry, transport.NegotiatorConfig{
		ProbeTimeout:      5 * time.Second,
		ConnectionTimeout: 30 * time.Second,
		MaxRetries:        3,
		RetryDelay:        1 * time.Second,
		ParallelProbes:    3,
		EnableFallback:    true,
	})
	node.TransportNegotiator = transportNegotiator

	// Connect persistent relay for DERP-like instant connectivity
	// This provides immediate relay routing while direct connections are established in parallel
	if err := node.ConnectPersistentRelay(ctx); err != nil {
		log.Warn().Err(err).Msg("persistent relay not available, will use sequential fallback")
	} else {
		// Set the relay on the forwarder for fallback routing
		forwarder.SetRelay(node.PersistentRelay)
		log.Info().Msg("persistent relay enabled for instant connectivity")

		// Set up filter rule sync handlers
		node.PersistentRelay.SetFilterRulesSyncHandler(func(rules []tunnel.FilterRuleWire) {
			// Convert wire rules to routing filter rules and set as coordinator rules
			filterRules := make([]routing.FilterRule, 0, len(rules))
			for _, r := range rules {
				filterRules = append(filterRules, routing.FilterRule{
					Port:     r.Port,
					Protocol: routing.ProtocolFromString(r.Protocol),
					Action:   routing.ParseFilterAction(r.Action),
				})
			}
			filter.SetCoordinatorRules(filterRules)
			log.Info().Int("rules", len(rules)).Msg("synced coordinator filter rules")
		})

		node.PersistentRelay.SetFilterRuleAddHandler(func(rule tunnel.FilterRuleWire) {
			filter.AddTemporaryRule(routing.FilterRule{
				Port:       rule.Port,
				Protocol:   routing.ProtocolFromString(rule.Protocol),
				Action:     routing.ParseFilterAction(rule.Action),
				SourcePeer: rule.SourcePeer,
			})
			log.Info().
				Uint16("port", rule.Port).
				Str("protocol", rule.Protocol).
				Str("action", rule.Action).
				Str("source_peer", rule.SourcePeer).
				Msg("added temporary filter rule from coordinator")
		})

		node.PersistentRelay.SetFilterRuleRemoveHandler(func(port uint16, protocol string) {
			filter.RemoveTemporaryRule(port, routing.ProtocolFromString(protocol))
			log.Info().
				Uint16("port", port).
				Str("protocol", protocol).
				Msg("removed temporary filter rule from coordinator")
		})

		// Service ports handler - auto-allow coordinator service ports
		node.PersistentRelay.SetServicePortsHandler(func(ports []uint16) {
			rules := make([]routing.FilterRule, len(ports))
			for i, port := range ports {
				rules[i] = routing.FilterRule{
					Port:     port,
					Protocol: routing.ProtoTCP, // Service ports are typically TCP
					Action:   routing.ActionAllow,
				}
			}
			filter.SetServiceRules(rules)
			log.Info().Int("ports", len(ports)).Msg("added coordinator service ports to filter")
		})

		// Handler for coordinator to query our current filter rules
		node.PersistentRelay.SetGetFilterRulesHandler(func() []tunnel.FilterRuleWithSourceWire {
			allRules := filter.ListRules()
			wireRules := make([]tunnel.FilterRuleWithSourceWire, 0, len(allRules))
			for _, r := range allRules {
				wireRules = append(wireRules, tunnel.FilterRuleWithSourceWire{
					Port:       r.Rule.Port,
					Protocol:   routing.ProtocolToString(r.Rule.Protocol),
					Action:     r.Rule.Action.String(),
					SourcePeer: r.Rule.SourcePeer,
					Source:     r.Source.String(),
					Expires:    r.Rule.Expires,
				})
			}
			return wireRules
		})

		// Coordinator list update handler - updates DNS round-robin when coordinators join/leave
		node.PersistentRelay.SetCoordListUpdateHandler(func(coordIPs []string) {
			if node.Resolver != nil {
				node.Resolver.SetCoordMeshIPs(coordIPs)
			}
			log.Info().Strs("coord_ips", coordIPs).Msg("updated coordinator list from server push")
		})
	}

	// Initialize WireGuard concentrator if enabled
	var wgConcentrator *peerwg.Concentrator
	var wgRouter *peerwg.Router
	var wgStore *peerwg.ClientStore
	var wgAPIHandler *peerwg.APIHandler
	if cfg.WireGuard.Enabled {
		// Set default data dir if not specified
		dataDir := cfg.WireGuard.DataDir
		if dataDir == "" {
			homeDir, _ := os.UserHomeDir()
			dataDir = filepath.Join(homeDir, ".tunnelmesh", "wireguard")
		}

		// Create persistent client store
		var err error
		wgStore, err = peerwg.NewClientStore(resp.MeshCIDR, dataDir)
		if err != nil {
			log.Error().Err(err).Msg("failed to create WireGuard client store")
		} else {
			wgCfg := &peerwg.ConcentratorConfig{
				ServerURL:    cfg.PrimaryServer(),
				AuthToken:    cfg.AuthToken,
				ListenPort:   cfg.WireGuard.ListenPort,
				DataDir:      dataDir,
				SyncInterval: 30 * time.Second, // Not used for syncing from server anymore
				MeshCIDR:     resp.MeshCIDR,
			}

			wgConcentrator, err = peerwg.NewConcentrator(wgCfg)
			if err != nil {
				log.Error().Err(err).Msg("failed to create WireGuard concentrator")
			} else {
				// Create WG router for packet routing decisions
				wgRouter = peerwg.NewRouter(resp.MeshCIDR)

				// Create API handler for proxied requests
				wgAPIHandler = peerwg.NewAPIHandler(
					wgStore,
					wgConcentrator.PublicKey(),
					cfg.WireGuard.Endpoint,
					resp.MeshCIDR,
					resp.Domain,
				)

				// Set up relay to handle API requests if relay is connected
				if node.PersistentRelay != nil {
					node.PersistentRelay.SetAPIRequestHandler(func(reqID uint32, method string, body []byte) {
						response := wgAPIHandler.HandleRequest(method, body)
						if err := node.PersistentRelay.SendAPIResponse(reqID, response); err != nil {
							log.Error().Err(err).Uint32("req_id", reqID).Msg("failed to send API response")
						}
					})

					// Announce as WireGuard concentrator
					if err := node.PersistentRelay.AnnounceWGConcentrator(); err != nil {
						log.Error().Err(err).Msg("failed to announce as WireGuard concentrator")
					}
				}

				// Load initial clients from store and update router
				clients := wgStore.List()
				wgClients := make([]peerwg.Client, len(clients))
				for i, c := range clients {
					wgClients[i] = peerwg.Client{
						ID:        c.ID,
						Name:      c.Name,
						PublicKey: c.PublicKey,
						MeshIP:    c.MeshIP,
						DNSName:   c.DNSName,
						Enabled:   c.Enabled,
						CreatedAt: c.CreatedAt,
						LastSeen:  c.LastSeen,
					}
				}
				wgRouter.UpdateClients(wgClients)

				log.Info().
					Int("port", cfg.WireGuard.ListenPort).
					Str("public_key", wgConcentrator.PublicKey()).
					Int("clients", len(clients)).
					Msg("WireGuard concentrator initialized")

				// Start the WireGuard device if TUN is available
				if tunDev != nil {
					// Calculate concentrator's WG interface address
					// Use .1 in the first WG client subnet (e.g., 10.42.100.1/16)
					_, meshNet, _ := net.ParseCIDR(resp.MeshCIDR)
					baseIP := meshNet.IP.To4()
					wgAddr := fmt.Sprintf("%d.%d.100.1/16", baseIP[0], baseIP[1])

					if err := wgConcentrator.StartDevice("wg-mesh", wgAddr); err != nil {
						log.Error().Err(err).Msg("failed to start WireGuard device")
					} else {
						// Create packet handler for bidirectional forwarding
						wgPacketHandler := peerwg.NewPacketHandler(wgRouter, wgConcentrator)
						forwarder.SetWGHandler(wgPacketHandler)

						// Set up callback for packets from WG clients to mesh
						wgConcentrator.SetOnPacketFromWG(func(packet []byte) {
							if err := forwarder.ForwardPacket(packet); err != nil {
								log.Debug().Err(err).Msg("failed to forward WG packet to mesh")
							}
						})

						// Set up callback for API handler to update WG device when clients change
						wgAPIHandler.SetOnClientsChanged(func(clients []peerwg.Client) {
							// Update router
							wgRouter.UpdateClients(clients)

							// Update concentrator (which updates the WG device)
							if err := wgConcentrator.UpdateClients(clients); err != nil {
								log.Error().Err(err).Msg("failed to update WireGuard clients")
							} else {
								log.Info().Int("clients", len(clients)).Msg("WireGuard clients updated")
							}
						})

						log.Info().Str("address", wgAddr).Msg("WireGuard device started and integrated with forwarder")
					}
				}
			}
		}
	}

	// Suppress unused - wgStore is used via wgAPIHandler
	_ = wgStore

	// Start DNS resolver if enabled
	var dnsConfigured bool
	// DNS is always enabled for all peers
	resolver := meshdns.NewResolver(resp.Domain, cfg.DNS.CacheTTL)
	// Set coordinator mesh IPs for "this.tunnelmesh" round-robin resolution.
	// For coordinators, read the current list from the server (the registration response
	// may be stale — it was captured before SetCoordMeshIP populated the atomic).
	if srv != nil {
		if coordIPs := srv.GetCoordMeshIPs(); len(coordIPs) > 0 {
			resolver.SetCoordMeshIPs(coordIPs)
		}
		// Keep local resolver in sync when coordinators join/leave
		srv.OnCoordIPsChanged(func(ips []string) {
			resolver.SetCoordMeshIPs(ips)
		})
	} else if len(resp.CoordMeshIPs) > 0 {
		resolver.SetCoordMeshIPs(resp.CoordMeshIPs)
	}
	node.Resolver = resolver

	// Initial DNS sync
	if err := syncDNS(client, resolver); err != nil {
		log.Warn().Err(err).Msg("failed to sync DNS")
	}

	go func() {
		if err := resolver.ListenAndServe(cfg.DNS.Listen); err != nil {
			log.Error().Err(err).Msg("DNS server error")
		}
	}()
	log.Info().Str("listen", cfg.DNS.Listen).Msg("DNS server started")

	// Configure system resolver
	if err := configureSystemResolver(resp.Domain, cfg.DNS.Listen); err != nil {
		log.Warn().Err(err).Msg("failed to configure system resolver")
	} else {
		dnsConfigured = true
	}

	// Start packet forwarder if TUN is available
	if tunDev != nil {
		go func() {
			if err := forwarder.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error().Err(err).Msg("forwarder error")
			}
		}()
	}

	// Set up callback to trigger discovery when a tunnel is removed
	node.TunnelMgr().SetOnRemove(func() {
		if node.TriggerDiscovery() {
			log.Debug().Msg("triggered discovery due to tunnel removal")
		}
	})

	// Create network monitor for detecting network changes
	netMonitor, err := netmon.New(netmon.DefaultConfig())
	if err != nil {
		log.Warn().Err(err).Msg("failed to create network monitor, changes won't be detected")
	} else {
		networkChanges, err := netMonitor.Start(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("failed to start network monitor")
			_ = netMonitor.Close()
		} else {
			defer func() { _ = netMonitor.Close() }()
			go node.RunNetworkMonitor(ctx, networkChanges)
		}
	}

	// Initialize metrics
	peerMetrics := metrics.InitMetrics(cfg.Name, resp.MeshIP, Version)

	// Initialize Loki log shipping if enabled
	if cfg.Loki.Enabled && cfg.Loki.URL != "" {
		flushInterval, err := time.ParseDuration(cfg.Loki.FlushInterval)
		if err != nil {
			flushInterval = 5 * time.Second
		}
		lokiWriter := loki.NewWriter(loki.Config{
			URL:           cfg.Loki.URL,
			BatchSize:     cfg.Loki.BatchSize,
			FlushInterval: flushInterval,
			Labels: map[string]string{
				"peer":    cfg.Name,
				"mesh_ip": resp.MeshIP,
				"version": Version,
			},
		})
		lokiWriter.Start()
		defer lokiWriter.Stop()

		// Reconfigure logger to also write to Loki
		log.Logger = log.Output(zerolog.MultiLevelWriter(
			zerolog.ConsoleWriter{Out: os.Stderr},
			lokiWriter,
		))
		log.Info().Str("url", cfg.Loki.URL).Msg("Loki log shipping enabled")
	}

	// Create WireGuard metrics wrapper if enabled
	var wgWrapper metrics.WGConcentrator
	if wgConcentrator != nil {
		wgWrapper = metrics.NewWGConcentratorWrapper(
			wgConcentrator.IsDeviceRunning,
			func() (total, enabled int) {
				clients := wgConcentrator.Clients()
				for _, c := range clients {
					total++
					if c.Enabled {
						enabled++
					}
				}
				return
			},
		)
	}

	// Create metrics collector
	relayWrapper := metrics.NewRelayWrapper(node.PersistentRelay)
	metricsCollector := metrics.NewCollector(peerMetrics, metrics.CollectorConfig{
		Forwarder:           forwarder,
		TunnelMgr:           node.TunnelMgr(),
		Connections:         node.Connections,
		Relay:               relayWrapper,
		RTTProvider:         relayWrapper, // Also provides RTT for latency metrics
		PeerLatencyProvider: node.LatencyProber,
		Identity:            identity,
		AllowsExit:          cfg.AllowExitTraffic,
		WGEnabled:           cfg.WireGuard.Enabled,
		WGConcentrator:      wgWrapper,
		Filter:              filter,
	})

	// Register reconnect observer for metrics
	node.Connections.AddObserver(metricsCollector.ReconnectObserver())

	// Start metrics collection loop
	go metricsCollector.Run(ctx, 10*time.Second)

	// Start metrics admin server on mesh IP
	if tlsMgr != nil {
		tlsCert, err := tlsMgr.LoadCert()
		if err != nil {
			log.Warn().Err(err).Msg("failed to load TLS cert for metrics server")
		} else {
			metricsAddr := fmt.Sprintf("%s:%d", resp.MeshIP, cfg.MetricsPort)
			adminServer := admin.NewAdminServer()
			if err := adminServer.Start(metricsAddr, tlsCert); err != nil {
				log.Warn().Err(err).Msg("failed to start metrics admin server")
			} else {
				log.Info().
					Str("address", metricsAddr).
					Msg("metrics admin server started (HTTPS)")
				defer func() { _ = adminServer.Stop() }()
			}
		}
	}

	// Start benchmark server on mesh IP for speed tests
	benchServer := benchmark.NewServer(resp.MeshIP, benchmark.DefaultPort)
	if err := benchServer.Start(); err != nil {
		log.Warn().Err(err).Msg("failed to start benchmark server")
	} else {
		log.Info().
			Str("address", benchServer.Addr()).
			Msg("benchmark server started")
		defer func() { _ = benchServer.Stop() }()
	}

	// Start peer discovery and tunnel establishment loop
	go node.RunPeerDiscovery(ctx)

	// Start heartbeat loop
	go node.RunHeartbeat(ctx)

	// Show ready message
	fmt.Fprintf(os.Stderr, "\n  ✓ Connected to mesh as %s (%s)\n", cfg.Name, resp.MeshIP)
	fmt.Fprintf(os.Stderr, "  Opening https://this.tm in 3 seconds...\n")
	fmt.Fprintf(os.Stderr, "  Press CTRL+C to disconnect\n\n")

	// Open browser to mesh dashboard after delay
	go func() {
		time.Sleep(3 * time.Second)
		openBrowser("https://this.tm")
	}()

	// Wait for context cancellation (shutdown signal)
	<-ctx.Done()

	// Shutdown coordinator server if running
	if srv != nil {
		log.Info().Msg("shutting down coordinator server")
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Warn().Err(err).Msg("failed to shutdown coordinator server")
		}
	}

	// Clean up system resolver
	if dnsConfigured {
		removeSystemResolver(resp.Domain)
	}

	// Stop WireGuard device
	if wgConcentrator != nil && wgConcentrator.IsDeviceRunning() {
		if err := wgConcentrator.StopDevice(); err != nil {
			log.Warn().Err(err).Msg("failed to stop WireGuard device")
		}
	}

	// Close all connections via FSM (properly transitions states and triggers observers)
	node.Connections.CloseAll()

	if node.Resolver != nil {
		_ = node.Resolver.Shutdown()
	}

	// Show exit instructions
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  Disconnected from mesh.")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  To reconnect:         sudo tunnelmesh join")
	fmt.Fprintln(os.Stderr, "  To run as a service:  sudo tunnelmesh service install && sudo tunnelmesh service start")
	fmt.Fprintln(os.Stderr)

	return nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	setupLogging()

	cfg, err := loadConfig()
	if err != nil {
		fmt.Println("Status: Not connected")
		fmt.Println("  Config: not found or invalid")
		fmt.Printf("  Error:  %v\n", err)
		return nil
	}

	// Display configuration
	fmt.Println("TunnelMesh Status")
	fmt.Println("=================")
	fmt.Println()

	// Node info
	fmt.Println("Node:")
	if cfg.Name != "" {
		fmt.Printf("  Name:        %s\n", cfg.Name)
	} else {
		fmt.Println("  Name:        (not configured)")
	}
	fmt.Printf("  SSH Port:    %d\n", cfg.SSHPort)
	fmt.Printf("  Private Key: %s\n", cfg.PrivateKey)

	// Check if keys exist
	if _, err := os.Stat(cfg.PrivateKey); err == nil {
		signer, err := config.LoadPrivateKey(cfg.PrivateKey)
		if err == nil {
			fingerprint := config.GetPublicKeyFingerprint(signer.PublicKey())
			fmt.Printf("  Key FP:      %s\n", fingerprint)
		}
	} else {
		fmt.Println("  Key FP:      (keys not initialized - run 'tunnelmesh init')")
	}

	fmt.Println()

	// Server connection
	fmt.Println("Server:")
	if len(cfg.Servers) == 0 {
		fmt.Println("  URL:         (not configured)")
		fmt.Println("  Status:      disconnected")
		return nil
	}
	fmt.Printf("  URL:         %s\n", cfg.PrimaryServer())

	// Try to connect and get peer list
	client := coord.NewClient(cfg.PrimaryServer(), cfg.AuthToken)
	peers, err := client.ListPeers()
	if err != nil {
		fmt.Println("  Status:      unreachable")
		fmt.Printf("  Error:       %v\n", err)
		return nil
	}
	fmt.Println("  Status:      connected")

	fmt.Println()

	// Find ourselves in the peer list
	fmt.Println("Mesh:")
	var myPeer *proto.Peer
	onlineCount := 0
	for i, p := range peers {
		if p.Name == cfg.Name {
			myPeer = &peers[i]
		}
		// Consider peers seen in last 2 minutes as online
		if time.Since(p.LastSeen) < 2*time.Minute {
			onlineCount++
		}
	}

	if myPeer != nil {
		fmt.Printf("  Mesh IP:     %s\n", myPeer.MeshIP)
		fmt.Printf("  Last Seen:   %s\n", myPeer.LastSeen.Format("2006-01-02 15:04:05"))
		if myPeer.Connectable {
			fmt.Println("  Connectable: yes")
		} else {
			fmt.Println("  Connectable: no (behind NAT)")
		}
	} else {
		fmt.Println("  Mesh IP:     (not registered)")
		fmt.Println("  Note:        run 'tunnelmesh join' to join the mesh")
	}

	fmt.Printf("  Total Peers: %d\n", len(peers))
	fmt.Printf("  Online:      %d\n", onlineCount)

	fmt.Println()

	// TUN device info
	fmt.Println("TUN Device:")
	fmt.Printf("  Name:        %s\n", cfg.TUN.Name)
	fmt.Printf("  MTU:         %d\n", cfg.TUN.MTU)

	fmt.Println()

	// DNS info
	fmt.Println("DNS:")
	fmt.Println("  Enabled:     yes (always)")
	fmt.Printf("  Listen:      %s\n", cfg.DNS.Listen)
	fmt.Printf("  Cache TTL:   %ds\n", cfg.DNS.CacheTTL)

	return nil
}

func runPeers(cmd *cobra.Command, args []string) error {
	setupLogging()

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	client := coord.NewClient(cfg.PrimaryServer(), cfg.AuthToken)
	peers, err := client.ListPeers()
	if err != nil {
		return fmt.Errorf("list peers: %w", err)
	}

	if len(peers) == 0 {
		fmt.Println("No peers in mesh")
		return nil
	}

	fmt.Printf("%-20s %-15s %-20s %s\n", "NAME", "MESH IP", "PUBLIC IP", "LAST SEEN")
	fmt.Println("-------------------- --------------- -------------------- --------------------")

	for _, p := range peers {
		publicIP := "-"
		if len(p.PublicIPs) > 0 {
			publicIP = p.PublicIPs[0]
		}
		lastSeen := p.LastSeen.Format("2006-01-02 15:04:05")
		fmt.Printf("%-20s %-15s %-20s %s\n", p.Name, p.MeshIP, publicIP, lastSeen)
	}

	return nil
}

func runResolve(cmd *cobra.Command, args []string) error {
	setupLogging()

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	hostname := args[0]

	client := coord.NewClient(cfg.PrimaryServer(), cfg.AuthToken)
	records, err := client.GetDNSRecords()
	if err != nil {
		return fmt.Errorf("get DNS records: %w", err)
	}

	for _, r := range records {
		if r.Hostname == hostname {
			fmt.Printf("%s -> %s\n", hostname, r.MeshIP)
			return nil
		}
	}

	return fmt.Errorf("hostname not found: %s", hostname)
}

func runLeave(cmd *cobra.Command, args []string) error {
	setupLogging()

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	client := coord.NewClient(cfg.PrimaryServer(), cfg.AuthToken)
	if err := client.Deregister(cfg.Name); err != nil {
		return fmt.Errorf("deregister: %w", err)
	}

	log.Info().Msg("left mesh network")

	// Clean up TLS certificates
	tlsDataDir := filepath.Dir(cfg.PrivateKey)
	tlsMgr := peer.NewTLSManager(tlsDataDir)
	if tlsMgr.HasCert() {
		tlsDir := filepath.Join(tlsDataDir, "tls")
		if err := os.RemoveAll(tlsDir); err != nil {
			log.Warn().Err(err).Msg("failed to remove TLS certificates")
		} else {
			log.Info().Msg("TLS certificates removed")
		}
	}

	// Remove CA from system trust store
	if trusted, _ := IsCATrusted(); trusted {
		if err := RemoveCA(); err != nil {
			log.Warn().Err(err).Msg("failed to remove CA certificate")
		} else {
			log.Info().Msg("CA certificate removed from system trust store")
		}
	}

	return nil
}

func loadConfig() (*config.PeerConfig, error) {
	// If explicit --config flag is provided, use it
	if cfgFile != "" {
		return config.LoadPeerConfig(cfgFile)
	}

	homeDir, _ := os.UserHomeDir()

	// Check for active context
	store, err := meshctx.Load()
	if err == nil && store.HasActive() {
		activeCtx := store.GetActive()
		if activeCtx != nil {
			// If context has a config file, load it
			if activeCtx.ConfigPath != "" {
				if _, err := os.Stat(activeCtx.ConfigPath); err == nil {
					return config.LoadPeerConfig(activeCtx.ConfigPath)
				}
			}
			// If context has server info but no config file, use context values
			// This happens when joining with positional URL instead of --config
			// Auth token is read from TUNNELMESH_TOKEN environment variable
			if activeCtx.Server != "" {
				return &config.PeerConfig{
					Name:       activeCtx.PeerName, // Restore saved peer name
					Servers:    []string{activeCtx.Server},
					SSHPort:    2222,
					PrivateKey: filepath.Join(homeDir, ".tunnelmesh", "id_ed25519"),
					TUN: config.TUNConfig{
						Name: "tun-mesh0",
						MTU:  1400,
					},
					DNS: config.DNSConfig{
						Listen:   activeCtx.DNSListen,
						CacheTTL: 300,
					},
				}, nil
			}
		}
	}

	// Try default locations
	defaults := []string{
		filepath.Join(homeDir, ".tunnelmesh", "config.yaml"),
		"tunnelmesh.yaml",
		"peer.yaml",
	}

	for _, path := range defaults {
		if _, err := os.Stat(path); err == nil {
			return config.LoadPeerConfig(path)
		}
	}

	// Return empty config with defaults
	return &config.PeerConfig{
		SSHPort:    2222,
		PrivateKey: filepath.Join(homeDir, ".tunnelmesh", "id_ed25519"),
		TUN: config.TUNConfig{
			Name: "tun-mesh0",
			MTU:  1400,
		},
		DNS: config.DNSConfig{
			Listen:   "127.0.0.53:5353",
			CacheTTL: 300,
		},
	}, nil
}

func syncDNS(client *coord.Client, resolver *meshdns.Resolver) error {
	records, err := client.GetDNSRecords()
	if err != nil {
		return err
	}

	recordMap := make(map[string]string, len(records))
	for _, r := range records {
		recordMap[r.Hostname] = r.MeshIP
	}

	resolver.UpdateRecords(recordMap)
	return nil
}

func setupLogging() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

// logStartupBanner logs the application startup banner with version information.
func logStartupBanner() {
	banner := `
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║   ████████╗██╗   ██╗███╗   ██╗███╗   ██╗███████╗██╗            ║
║   ╚══██╔══╝██║   ██║████╗  ██║████╗  ██║██╔════╝██║            ║
║      ██║   ██║   ██║██╔██╗ ██║██╔██╗ ██║█████╗  ██║            ║
║      ██║   ██║   ██║██║╚██╗██║██║╚██╗██║██╔══╝  ██║            ║
║      ██║   ╚██████╔╝██║ ╚████║██║ ╚████║███████╗███████╗       ║
║      ╚═╝    ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═══╝╚══════╝╚══════╝       ║
║                                                                ║
║            ███╗   ███╗███████╗███████╗██╗  ██╗                 ║
║            ████╗ ████║██╔════╝██╔════╝██║  ██║                 ║
║            ██╔████╔██║█████╗  ███████╗███████║                 ║
║            ██║╚██╔╝██║██╔══╝  ╚════██║██╔══██║                 ║
║            ██║ ╚═╝ ██║███████╗███████║██║  ██║                 ║
║            ╚═╝     ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝                 ║
║                                                                ║
║            P2P Encrypted Mesh Network                          ║
║                   written by zombar                            ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝`

	fmt.Fprintln(os.Stderr, banner)
	fmt.Fprintf(os.Stderr, "\n  Version:    %s\n", Version)
	fmt.Fprintf(os.Stderr, "  Commit:     %s\n", Commit)
	fmt.Fprintf(os.Stderr, "  Build Time: %s\n", BuildTime)
	fmt.Fprintf(os.Stderr, "  Go:         %s\n", runtime.Version())
	fmt.Fprintf(os.Stderr, "  OS/Arch:    %s/%s\n\n", runtime.GOOS, runtime.GOARCH)
}

// setupServiceLogging configures logging for service mode.
// This writes directly to a file because launchd/kardianos-service
// may not properly redirect stderr.
// Default level is Info; can be overridden by config after loading.
func setupServiceLogging() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Try to open log file for direct writing
	logPath := "/var/log/tunnelmesh-service.log"
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		// Fall back to stderr if we can't open the log file
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		return
	}

	// Write to both file and stderr
	multi := io.MultiWriter(logFile, os.Stderr)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: multi, TimeFormat: time.RFC3339})
}

// configureSystemResolver sets up the system to resolve all mesh domains via our DNS server.
// Configures all supported suffixes: .tunnelmesh, .tm, .mesh
func configureSystemResolver(_, dnsAddr string) error {
	// Extract port from address
	parts := strings.Split(dnsAddr, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid DNS address: %s", dnsAddr)
	}
	port := parts[1]

	// Configure all supported domain suffixes
	for _, suffix := range mesh.AllSuffixes() {
		domain := strings.TrimPrefix(suffix, ".")

		var err error
		switch runtime.GOOS {
		case "darwin":
			err = configureDarwinResolver(domain, port)
		case "linux":
			err = configureLinuxResolver(domain, dnsAddr)
		case "windows":
			err = configureWindowsResolver(domain, dnsAddr)
		default:
			log.Warn().Str("os", runtime.GOOS).Msg("automatic DNS configuration not supported on this OS")
			return nil
		}
		if err != nil {
			return fmt.Errorf("configure resolver for %s: %w", suffix, err)
		}
	}
	return nil
}

// removeSystemResolver removes the system resolver configuration for all mesh domains.
func removeSystemResolver(_ string) {
	// Remove all supported domain suffixes
	for _, suffix := range mesh.AllSuffixes() {
		domain := strings.TrimPrefix(suffix, ".")

		var err error
		switch runtime.GOOS {
		case "darwin":
			err = removeDarwinResolver(domain)
		case "linux":
			err = removeLinuxResolver(domain)
		case "windows":
			err = removeWindowsResolver(domain)
		}
		if err != nil {
			log.Warn().Err(err).Str("domain", domain).Msg("failed to remove resolver")
		}
	}
}

func configureDarwinResolver(domain, port string) error {
	resolverDir := "/etc/resolver"
	resolverFile := filepath.Join(resolverDir, domain)

	// Check if we can write directly (running as root)
	content := fmt.Sprintf("nameserver 127.0.0.1\nport %s\n", port)

	// Try to create directory and file
	if err := os.MkdirAll(resolverDir, 0755); err != nil {
		// Need sudo - try with sudo
		log.Info().Msg("configuring system DNS resolver (requires sudo)...")

		// Create resolver directory
		cmd := exec.Command("sudo", "mkdir", "-p", resolverDir)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to create resolver directory: %w", err)
		}

		// Write resolver file using tee
		cmd = exec.Command("sudo", "tee", resolverFile)
		cmd.Stdin = strings.NewReader(content)
		cmd.Stdout = nil // Suppress tee output
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to write resolver file: %w", err)
		}

		log.Info().Str("file", resolverFile).Msg("system resolver configured")
		return nil
	}

	// We have permissions, write directly
	if err := os.WriteFile(resolverFile, []byte(content), 0644); err != nil {
		return fmt.Errorf("write resolver file: %w", err)
	}

	log.Info().Str("file", resolverFile).Msg("system resolver configured")
	return nil
}

func removeDarwinResolver(domain string) error {
	resolverFile := filepath.Join("/etc/resolver", domain)

	// Check if file exists
	if _, err := os.Stat(resolverFile); os.IsNotExist(err) {
		return nil
	}

	// Try to remove directly
	if err := os.Remove(resolverFile); err != nil {
		// Need sudo
		cmd := exec.Command("sudo", "rm", "-f", resolverFile)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to remove resolver file: %w", err)
		}
	}

	log.Info().Str("file", resolverFile).Msg("system resolver removed")
	return nil
}

func configureLinuxResolver(domain, dnsAddr string) error {
	parts := strings.Split(dnsAddr, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid DNS address: %s", dnsAddr)
	}
	ip := parts[0]
	port := parts[1]

	// Try systemd-resolved first (most common on modern Linux)
	if _, err := exec.LookPath("resolvectl"); err == nil {
		return configureSystemdResolved(domain, ip, port)
	}

	// Fallback: create a config file for systemd-resolved
	confDir := "/etc/systemd/resolved.conf.d"
	if _, err := os.Stat("/etc/systemd/resolved.conf"); err == nil {
		return configureSystemdResolvedFile(domain, ip, port, confDir)
	}

	log.Warn().
		Str("domain", domain).
		Str("dns", dnsAddr).
		Msg("could not auto-configure DNS; manually add DNS server or use: dig @127.0.0.1 -p PORT hostname.mesh")
	return nil
}

func configureSystemdResolved(domain, ip, port string) error {
	log.Info().Msg("configuring systemd-resolved (requires sudo)...")

	// Use resolvectl to set DNS for the mesh domain
	// Note: This sets a global DNS that only handles .mesh
	cmd := exec.Command("sudo", "resolvectl", "dns", "lo", ip+":"+port)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("resolvectl dns failed: %w", err)
	}

	cmd = exec.Command("sudo", "resolvectl", "domain", "lo", "~"+domain)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("resolvectl domain failed: %w", err)
	}

	log.Info().Str("domain", domain).Msg("systemd-resolved configured")
	return nil
}

func configureSystemdResolvedFile(domain, ip, port, confDir string) error {
	log.Info().Msg("configuring systemd-resolved via config file (requires sudo)...")

	content := fmt.Sprintf(`# TunnelMesh DNS configuration
[Resolve]
DNS=%s:%s
Domains=~%s
`, ip, port, domain)

	confFile := filepath.Join(confDir, "tunnelmesh.conf")

	// Create directory
	cmd := exec.Command("sudo", "mkdir", "-p", confDir)
	cmd.Stdin = os.Stdin
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mkdir failed: %w", err)
	}

	// Write config
	cmd = exec.Command("sudo", "tee", confFile)
	cmd.Stdin = strings.NewReader(content)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("write config failed: %w", err)
	}

	// Restart systemd-resolved
	cmd = exec.Command("sudo", "systemctl", "restart", "systemd-resolved")
	cmd.Stderr = os.Stderr
	_ = cmd.Run() // Ignore error, might not need restart

	log.Info().Str("file", confFile).Msg("systemd-resolved configured")
	return nil
}

func removeLinuxResolver(_ string) error {
	// Try resolvectl first
	if _, err := exec.LookPath("resolvectl"); err == nil {
		cmd := exec.Command("sudo", "resolvectl", "revert", "lo")
		cmd.Stdin = os.Stdin
		cmd.Stderr = os.Stderr
		_ = cmd.Run() // Ignore errors
	}

	// Remove config file if it exists
	confFile := "/etc/systemd/resolved.conf.d/tunnelmesh.conf"
	if _, err := os.Stat(confFile); err == nil {
		cmd := exec.Command("sudo", "rm", "-f", confFile)
		cmd.Stdin = os.Stdin
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("remove config failed: %w", err)
		}

		// Restart systemd-resolved
		cmd = exec.Command("sudo", "systemctl", "restart", "systemd-resolved")
		cmd.Stderr = os.Stderr
		_ = cmd.Run()

		log.Info().Msg("systemd-resolved configuration removed")
	}

	return nil
}

func configureWindowsResolver(domain, dnsAddr string) error {
	parts := strings.Split(dnsAddr, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid DNS address: %s", dnsAddr)
	}
	ip := parts[0]
	port := parts[1]

	log.Info().Msg("configuring Windows DNS (requires Administrator)...")

	// Windows doesn't natively support per-domain DNS or custom ports easily
	// We'll use NRPT (Name Resolution Policy Table) via PowerShell
	// This requires running as Administrator

	// PowerShell command to add NRPT rule
	psCmd := fmt.Sprintf(`Add-DnsClientNrptRule -Namespace ".%s" -NameServers "%s"`, domain, ip)

	if port != "53" {
		// Windows NRPT doesn't support custom ports directly
		// We'd need to run a local DNS proxy on port 53
		log.Warn().
			Str("port", port).
			Msg("Windows requires DNS on port 53; consider changing dns.listen to 127.0.0.1:53")
		return fmt.Errorf("windows requires DNS on port 53, got port %s", port)
	}

	cmd := exec.Command("powershell", "-Command", psCmd)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("PowerShell NRPT rule failed: %w", err)
	}

	log.Info().Str("domain", domain).Msg("Windows NRPT rule configured")
	return nil
}

func removeWindowsResolver(domain string) error {
	// Remove NRPT rule
	psCmd := fmt.Sprintf(`Get-DnsClientNrptRule | Where-Object {$_.Namespace -eq ".%s"} | Remove-DnsClientNrptRule -Force`, domain)

	cmd := exec.Command("powershell", "-Command", psCmd)
	cmd.Stdin = os.Stdin
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Warn().Err(err).Msg("failed to remove Windows NRPT rule")
		return err
	}

	log.Info().Msg("Windows NRPT rule removed")
	return nil
}

// openBrowser opens the specified URL in the default browser.
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default: // linux, freebsd, etc.
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}
