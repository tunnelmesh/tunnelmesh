package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/context"
	"github.com/tunnelmesh/tunnelmesh/internal/svc"
)

var (
	serviceMode       string
	serviceConfigPath string
	serviceContext    string
	serviceUser       string
	forceInstall      bool
	logsFollow        bool
	logsLines         int

	// authTokenRegex validates that tokens are exactly 64 hex characters (32 bytes)
	authTokenRegex = regexp.MustCompile("^[0-9a-fA-F]{64}$")
)

// isValidAuthToken checks if the token is 64 hex characters (32 bytes).
func isValidAuthToken(token string) bool {
	return authTokenRegex.MatchString(token)
}

func newServiceCmd() *cobra.Command {
	serviceCmd := &cobra.Command{
		Use:   "service",
		Short: "Manage TunnelMesh system service",
		Long: `Install, control, and manage TunnelMesh as a system service.

Supported platforms:
  - Linux (systemd)
  - macOS (launchd)
  - Windows (Service Control Manager)

Service names are derived from context names:
  - Context "work" → Service "tunnelmesh-work"
  - Context "default" → Service "tunnelmesh"

Examples:
  # Install service for active context
  sudo tunnelmesh service install

  # Install service for a specific context
  sudo tunnelmesh service install --context work

  # Install as server service (uses --mode serve)
  sudo tunnelmesh service install --mode serve --config /etc/tunnelmesh/server.yaml

  # Control the service
  sudo tunnelmesh service start
  sudo tunnelmesh service stop
  sudo tunnelmesh service status

  # View logs
  sudo tunnelmesh service logs --follow`,
	}

	// Install subcommand
	installCmd := &cobra.Command{
		Use:   "install",
		Short: "Install TunnelMesh as a system service",
		Long: `Install TunnelMesh as a system service that starts automatically at boot.

For peer mode (--mode join), uses the active context or --context flag to determine
the service name and configuration. For server mode (--mode serve), requires --config.

Requires administrator/root privileges.`,
		RunE: runServiceInstall,
	}
	installCmd.Flags().StringVar(&serviceMode, "mode", "join", "Service mode: 'serve' (coordination server) or 'join' (peer)")
	installCmd.Flags().StringVarP(&serviceConfigPath, "config", "c", "", "Path to configuration file (required for serve mode)")
	installCmd.Flags().StringVar(&serviceContext, "context", "", "Context name (uses active context if not specified)")
	installCmd.Flags().StringVar(&serviceUser, "user", "", "Run service as this user (Linux/macOS only)")
	installCmd.Flags().BoolVarP(&forceInstall, "force", "f", false, "Force reinstall if service already exists")
	serviceCmd.AddCommand(installCmd)

	// Uninstall subcommand
	uninstallCmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Remove the TunnelMesh system service",
		RunE:  runServiceUninstall,
	}
	uninstallCmd.Flags().StringVar(&serviceContext, "context", "", "Context name (uses active context if not specified)")
	serviceCmd.AddCommand(uninstallCmd)

	// Start subcommand
	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the TunnelMesh service",
		RunE:  runServiceStart,
	}
	startCmd.Flags().StringVar(&serviceContext, "context", "", "Context name (uses active context if not specified)")
	serviceCmd.AddCommand(startCmd)

	// Stop subcommand
	stopCmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop the TunnelMesh service",
		RunE:  runServiceStop,
	}
	stopCmd.Flags().StringVar(&serviceContext, "context", "", "Context name (uses active context if not specified)")
	serviceCmd.AddCommand(stopCmd)

	// Restart subcommand
	restartCmd := &cobra.Command{
		Use:   "restart",
		Short: "Restart the TunnelMesh service",
		RunE:  runServiceRestart,
	}
	restartCmd.Flags().StringVar(&serviceContext, "context", "", "Context name (uses active context if not specified)")
	serviceCmd.AddCommand(restartCmd)

	// Status subcommand
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show TunnelMesh service status",
		RunE:  runServiceStatus,
	}
	statusCmd.Flags().StringVar(&serviceContext, "context", "", "Context name (uses active context if not specified)")
	serviceCmd.AddCommand(statusCmd)

	// Logs subcommand
	logsCmd := &cobra.Command{
		Use:   "logs",
		Short: "View TunnelMesh service logs",
		Long: `View logs from the TunnelMesh service.

Log locations by platform:
  - Linux:   journalctl -u tunnelmesh
  - macOS:   log show/stream with subsystem filter
  - Windows: Event Viewer > Application log`,
		RunE: runServiceLogs,
	}
	logsCmd.Flags().StringVar(&serviceContext, "context", "", "Context name (uses active context if not specified)")
	logsCmd.Flags().BoolVarP(&logsFollow, "follow", "f", false, "Follow log output (like tail -f)")
	logsCmd.Flags().IntVar(&logsLines, "lines", 50, "Number of log lines to show")
	serviceCmd.AddCommand(logsCmd)

	return serviceCmd
}

// getServiceConfig returns service configuration based on context or explicit flags.
// For serve mode, it uses traditional config-based approach.
// For join mode, it uses the context system.
func getServiceConfig() (*svc.ServiceConfig, error) {
	mode := serviceMode
	if mode == "" {
		mode = "join"
	}

	// For serve mode, use traditional config-based approach
	if mode == "serve" {
		configPath := serviceConfigPath
		if configPath == "" {
			configPath = svc.DefaultConfigPath(mode)
		}
		return &svc.ServiceConfig{
			Name:        svc.DefaultServiceName(mode),
			DisplayName: svc.DefaultDisplayName(mode),
			Description: svc.DefaultDescription(mode),
			Mode:        mode,
			ConfigPath:  configPath,
			UserName:    serviceUser,
		}, nil
	}

	// For join mode, use context system
	store, err := context.Load()
	if err != nil {
		return nil, fmt.Errorf("load context store: %w", err)
	}

	// Use specified context or active context
	ctxName := serviceContext
	if ctxName == "" {
		ctxName = store.Active
	}
	if ctxName == "" {
		return nil, fmt.Errorf("no active context; use --context or 'tunnelmesh context use <name>'")
	}

	ctx := store.Get(ctxName)
	if ctx == nil {
		return nil, fmt.Errorf("context %q not found", ctxName)
	}

	// Auth token must be provided via TUNNELMESH_TOKEN environment variable (not stored in context)
	authToken := os.Getenv("TUNNELMESH_TOKEN")
	if authToken == "" {
		return nil, fmt.Errorf("TUNNELMESH_TOKEN environment variable not set; auth token required for service installation (use 'sudo -E' to preserve environment variables)")
	}

	// Validate token format (must be 64 hex characters = 32 bytes)
	if !isValidAuthToken(authToken) {
		return nil, fmt.Errorf("invalid TUNNELMESH_TOKEN: must be 64 hex characters (generate with: openssl rand -hex 32)")
	}

	// If explicit config path provided, use it
	configPath := serviceConfigPath
	if configPath == "" {
		configPath = ctx.ConfigPath
	}
	if configPath == "" {
		// No config path - generate one from context values if we have server
		if ctx.Server == "" {
			return nil, fmt.Errorf("context %q has no config path and no server URL; use --config to specify one", ctxName)
		}
		// Generate config file from context
		generatedPath, err := generateConfigFromContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("generate config from context: %w", err)
		}
		configPath = generatedPath
		log.Info().Str("path", configPath).Msg("generated config file from context")
	}

	// Use context server URL; fall back to TUNNELMESH_SERVER env var (set during service install
	// in automated deployments where Servers is yaml:"-" and cannot be read from the config file).
	server := ctx.Server
	if server == "" {
		server = os.Getenv("TUNNELMESH_SERVER")
	}

	return &svc.ServiceConfig{
		Name:        ctx.ServiceName(),
		DisplayName: fmt.Sprintf("TunnelMesh Peer (%s)", ctxName),
		Description: fmt.Sprintf("TunnelMesh P2P mesh network peer daemon for context %q", ctxName),
		Mode:        mode,
		ConfigPath:  configPath,
		UserName:    serviceUser,
		Server:      server,
		AuthToken:   authToken,
	}, nil
}

func runServiceInstall(cmd *cobra.Command, args []string) error {
	setupLogging()

	// Check privileges
	if err := svc.CheckPrivileges(); err != nil {
		return err
	}

	// Validate mode
	if serviceMode != "serve" && serviceMode != "join" {
		return fmt.Errorf("invalid mode %q: must be 'serve' or 'join'", serviceMode)
	}

	cfg, err := getServiceConfig()
	if err != nil {
		return err
	}

	// Validate config file exists
	if _, err := os.Stat(cfg.ConfigPath); os.IsNotExist(err) {
		return fmt.Errorf("config file not found: %s\nCreate the config file first or specify a different path with --config", cfg.ConfigPath)
	}

	// Show security warning based on filter configuration
	showServiceInstallWarning(cfg.ConfigPath)

	// Prompt for confirmation
	fmt.Print("Do you want to continue? [y/N]: ")
	reader := bufio.NewReader(os.Stdin)
	response, _ := reader.ReadString('\n')
	response = strings.TrimSpace(strings.ToLower(response))
	if response != "y" && response != "yes" {
		fmt.Println("Installation cancelled.")
		return nil
	}

	log.Info().
		Str("name", cfg.Name).
		Str("mode", cfg.Mode).
		Str("config", cfg.ConfigPath).
		Msg("installing service")

	if err := svc.Install(cfg, forceInstall); err != nil {
		return err
	}

	fmt.Printf("\nService %q installed successfully.\n", cfg.Name)
	fmt.Printf("\nTo start the service:\n")
	fmt.Printf("  tunnelmesh service start\n")
	fmt.Printf("\nTo view logs:\n")
	fmt.Printf("  tunnelmesh service logs\n")

	return nil
}

// showServiceInstallWarning displays a security warning before installing the service.
// The warning is stronger when the packet filter allows all traffic by default.
func showServiceInstallWarning(configPath string) {
	// Try to load config to check filter settings
	isDefaultDeny := true // Safe default
	if peerCfg, err := config.LoadPeerConfig(configPath); err == nil {
		isDefaultDeny = peerCfg.Filter.IsDefaultDeny()
	}

	fmt.Fprintln(os.Stderr)
	if isDefaultDeny {
		// Safer configuration - moderate warning
		fmt.Fprintln(os.Stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		fmt.Fprintln(os.Stderr, "@          INSTALLING MESH NETWORK SERVICE              @")
		fmt.Fprintln(os.Stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "This will install TunnelMesh as a permanent system service.")
		fmt.Fprintln(os.Stderr, "A new network interface will be created on this machine.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Filter mode: DENY by default (allowlist)")
		fmt.Fprintln(os.Stderr, "  → Only explicitly allowed ports will be accessible")
		fmt.Fprintln(os.Stderr)
	} else {
		// Dangerous configuration - strong warning
		fmt.Fprintln(os.Stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		fmt.Fprintln(os.Stderr, "@    WARNING: INSTALLING MESH NETWORK WITH OPEN ACCESS   @")
		fmt.Fprintln(os.Stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "This will install TunnelMesh as a permanent system service.")
		fmt.Fprintln(os.Stderr, "A new network interface will be created on this machine.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "  *** SECURITY WARNING ***")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Filter mode: ALLOW by default (denylist)")
		fmt.Fprintln(os.Stderr, "  → ALL ports on this machine will be accessible to mesh peers!")
		fmt.Fprintln(os.Stderr, "  → Any peer in the mesh can connect to any service on this host.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Consider setting 'filter.default_deny: true' in your config")
		fmt.Fprintln(os.Stderr, "to only allow specific ports.")
		fmt.Fprintln(os.Stderr)
	}
}

func runServiceUninstall(cmd *cobra.Command, args []string) error {
	setupLogging()

	if err := svc.CheckPrivileges(); err != nil {
		return err
	}

	cfg, err := getServiceConfig()
	if err != nil {
		return err
	}

	log.Info().Str("name", cfg.Name).Msg("uninstalling service")

	if err := svc.Uninstall(cfg); err != nil {
		return err
	}

	fmt.Printf("Service %q uninstalled successfully.\n", cfg.Name)
	return nil
}

func runServiceStart(cmd *cobra.Command, args []string) error {
	setupLogging()

	if err := svc.CheckPrivileges(); err != nil {
		return err
	}

	cfg, err := getServiceConfig()
	if err != nil {
		return err
	}

	log.Info().Str("name", cfg.Name).Msg("starting service")

	if err := svc.Start(cfg); err != nil {
		return err
	}

	fmt.Printf("Service %q started.\n", cfg.Name)
	return nil
}

func runServiceStop(cmd *cobra.Command, args []string) error {
	setupLogging()

	if err := svc.CheckPrivileges(); err != nil {
		return err
	}

	cfg, err := getServiceConfig()
	if err != nil {
		return err
	}

	log.Info().Str("name", cfg.Name).Msg("stopping service")

	if err := svc.Stop(cfg); err != nil {
		return err
	}

	fmt.Printf("Service %q stopped.\n", cfg.Name)
	return nil
}

func runServiceRestart(cmd *cobra.Command, args []string) error {
	setupLogging()

	if err := svc.CheckPrivileges(); err != nil {
		return err
	}

	cfg, err := getServiceConfig()
	if err != nil {
		return err
	}

	log.Info().Str("name", cfg.Name).Msg("restarting service")

	if err := svc.Restart(cfg); err != nil {
		return err
	}

	fmt.Printf("Service %q restarted.\n", cfg.Name)
	return nil
}

func runServiceStatus(cmd *cobra.Command, args []string) error {
	setupLogging()

	cfg, err := getServiceConfig()
	if err != nil {
		return err
	}

	status, err := svc.Status(cfg)
	if err != nil {
		// Service might not be installed
		fmt.Printf("Service: %s\n", cfg.Name)
		fmt.Printf("Status:  not installed or unknown\n")
		fmt.Printf("Error:   %v\n", err)
		return nil
	}

	fmt.Printf("Service: %s\n", cfg.Name)
	fmt.Printf("Status:  %s\n", svc.StatusString(status))
	fmt.Printf("Mode:    %s\n", cfg.Mode)
	fmt.Printf("Config:  %s\n", cfg.ConfigPath)

	return nil
}

func runServiceLogs(cmd *cobra.Command, args []string) error {
	cfg, err := getServiceConfig()
	if err != nil {
		return err
	}

	return svc.ViewLogs(svc.LogOptions{
		ServiceName: cfg.Name,
		Follow:      logsFollow,
		Lines:       logsLines,
	})
}

// generateConfigFromContext creates a config file from context values.
// This is used when installing a service from a context that was created
// with positional URL and --token flag instead of a config file.
func generateConfigFromContext(ctx *context.Context) (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("get home dir: %w", err)
	}

	// Create config directory if needed
	configDir := filepath.Join(homeDir, ".tunnelmesh")
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return "", fmt.Errorf("create config dir: %w", err)
	}

	// Config file path based on context name
	configPath := filepath.Join(configDir, fmt.Sprintf("config-%s.yaml", ctx.Name))

	// Generate minimal config
	dnsListen := ctx.DNSListen
	if dnsListen == "" {
		dnsListen = "127.0.0.53:5353"
	}

	// Note: server URL and auth_token are passed via environment variables, not config file
	configContent := fmt.Sprintf(`# Auto-generated from context %q
# Server URL (TUNNELMESH_SERVER) and auth token (TUNNELMESH_TOKEN) are passed via environment variables
private_key: %q

dns:
  enabled: true
  listen: %q
`, ctx.Name, filepath.Join(configDir, "id_ed25519"), dnsListen)

	if err := os.WriteFile(configPath, []byte(configContent), 0600); err != nil {
		return "", fmt.Errorf("write config file: %w", err)
	}

	return configPath, nil
}
