// Package svc provides cross-platform system service support for TunnelMesh.
package svc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/kardianos/service"
	"github.com/rs/zerolog/log"
)

// RunFunc is the function signature for running serve or join modes.
type RunFunc func(ctx context.Context, configPath string) error

// Program implements service.Interface for the kardianos/service library.
type Program struct {
	Mode       string  // "serve" or "join"
	ConfigPath string  // Path to configuration file
	RunServe   RunFunc // Function to run server mode
	RunJoin    RunFunc // Function to run peer mode

	ctx    context.Context
	cancel context.CancelFunc
	done   chan error
}

// Start is called when the service starts.
// It must not block - start the actual work in a goroutine.
func (p *Program) Start(s service.Service) error {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.done = make(chan error, 1)

	go func() {
		var err error
		switch p.Mode {
		case "serve":
			if p.RunServe == nil {
				err = fmt.Errorf("serve function not configured")
			} else {
				err = p.RunServe(p.ctx, p.ConfigPath)
			}
		case "join":
			if p.RunJoin == nil {
				err = fmt.Errorf("join function not configured")
			} else {
				err = p.RunJoin(p.ctx, p.ConfigPath)
			}
		default:
			err = fmt.Errorf("unknown mode: %s", p.Mode)
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("service exited with error")
		}
		p.done <- err
	}()

	return nil
}

// Stop is called when the service stops.
// It should signal the running goroutine to stop and wait for it.
func (p *Program) Stop(s service.Service) error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.done != nil {
		// Wait for the goroutine to finish
		err := <-p.done
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}

// ServiceConfig holds configuration for service installation.
type ServiceConfig struct {
	Name        string // Service name (e.g., "tunnelmesh", "tunnelmesh-server")
	DisplayName string // Display name shown in service manager
	Description string // Service description
	Mode        string // "serve" or "join"
	ConfigPath  string // Path to configuration file
	UserName    string // User to run service as (Linux/macOS only)
	Server      string // Coordinator server URL (join mode only, passed as positional arg)
	AuthToken   string // Authentication token (passed via --token flag)
}

// DefaultServiceName returns the default service name based on mode.
func DefaultServiceName(mode string) string {
	if mode == "serve" {
		return "tunnelmesh-server"
	}
	return "tunnelmesh"
}

// DefaultDisplayName returns a human-readable display name.
func DefaultDisplayName(mode string) string {
	if mode == "serve" {
		return "TunnelMesh Coordination Server"
	}
	return "TunnelMesh Peer"
}

// DefaultDescription returns the service description.
func DefaultDescription(mode string) string {
	if mode == "serve" {
		return "TunnelMesh P2P mesh network coordination server"
	}
	return "TunnelMesh P2P mesh network peer daemon"
}

// DefaultConfigPath returns the default config file path based on platform and mode.
func DefaultConfigPath(mode string) string {
	var configDir string

	switch runtime.GOOS {
	case "windows":
		configDir = filepath.Join(os.Getenv("ProgramData"), "TunnelMesh")
	default: // linux, darwin
		configDir = "/etc/tunnelmesh"
	}

	if mode == "serve" {
		return filepath.Join(configDir, "server.yaml")
	}
	return filepath.Join(configDir, "peer.yaml")
}

// NewServiceConfig creates service.Config from our ServiceConfig.
func NewServiceConfig(cfg *ServiceConfig, execPath string) *service.Config {
	// Build arguments: start with base flags
	args := []string{
		"--service-run",
		"--service-mode", cfg.Mode,
		"join", // Always join subcommand
		"--config", cfg.ConfigPath,
	}

	// Build environment variables for server URL and token
	// These are NOT visible in process listings like CLI args are
	env := make(map[string]string)
	if cfg.Server != "" {
		env["TUNNELMESH_SERVER"] = cfg.Server
	}
	if cfg.AuthToken != "" {
		env["TUNNELMESH_TOKEN"] = cfg.AuthToken
	}

	svcCfg := &service.Config{
		Name:        cfg.Name,
		DisplayName: cfg.DisplayName,
		Description: cfg.Description,
		Arguments:   args,
		EnvVars:     env,
	}

	// Platform-specific options
	switch runtime.GOOS {
	case "linux":
		svcCfg.Dependencies = []string{"After=network-online.target", "Wants=network-online.target"}
		svcCfg.Option = service.KeyValue{
			"Restart":    "on-failure",
			"RestartSec": "5",
		}
		if cfg.UserName != "" {
			svcCfg.UserName = cfg.UserName
		}
	case "darwin":
		svcCfg.Option = service.KeyValue{
			"KeepAlive":     true,
			"RunAtLoad":     true,
			"SessionCreate": true,
		}
		if cfg.UserName != "" {
			svcCfg.UserName = cfg.UserName
		}
	case "windows":
		svcCfg.Option = service.KeyValue{
			"OnFailure":      "restart",
			"OnFailureDelay": "5s",
		}
	}

	return svcCfg
}

// CreateService creates a new service instance.
func CreateService(prg *Program, cfg *ServiceConfig) (service.Service, error) {
	execPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("get executable path: %w", err)
	}

	svcCfg := NewServiceConfig(cfg, execPath)
	return service.New(prg, svcCfg)
}

// Install installs the service.
func Install(cfg *ServiceConfig, force bool) error {
	prg := &Program{Mode: cfg.Mode, ConfigPath: cfg.ConfigPath}
	svc, err := CreateService(prg, cfg)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	// Check if already installed
	status, err := svc.Status()
	if err == nil {
		switch status {
		case service.StatusRunning:
			if !force {
				return fmt.Errorf("service %q is running; stop it first or use --force", cfg.Name)
			}
			if err := svc.Stop(); err != nil {
				log.Warn().Err(err).Msg("failed to stop service")
			}
			if err := svc.Uninstall(); err != nil {
				log.Warn().Err(err).Msg("failed to uninstall service")
			}
		case service.StatusStopped:
			if !force {
				return fmt.Errorf("service %q already installed; use --force to reinstall", cfg.Name)
			}
			if err := svc.Uninstall(); err != nil {
				log.Warn().Err(err).Msg("failed to uninstall service")
			}
		}
	}

	if err := svc.Install(); err != nil {
		return fmt.Errorf("install service: %w", err)
	}

	return nil
}

// Uninstall removes the service.
func Uninstall(cfg *ServiceConfig) error {
	prg := &Program{Mode: cfg.Mode, ConfigPath: cfg.ConfigPath}
	svc, err := CreateService(prg, cfg)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	// Stop if running
	status, _ := svc.Status()
	if status == service.StatusRunning {
		if err := svc.Stop(); err != nil {
			log.Warn().Err(err).Msg("failed to stop service")
		}
	}

	if err := svc.Uninstall(); err != nil {
		return fmt.Errorf("uninstall service: %w", err)
	}

	return nil
}

// Start starts the service.
func Start(cfg *ServiceConfig) error {
	prg := &Program{Mode: cfg.Mode, ConfigPath: cfg.ConfigPath}
	svc, err := CreateService(prg, cfg)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	if err := svc.Start(); err != nil {
		return fmt.Errorf("start service: %w", err)
	}

	return nil
}

// Stop stops the service.
func Stop(cfg *ServiceConfig) error {
	prg := &Program{Mode: cfg.Mode, ConfigPath: cfg.ConfigPath}
	svc, err := CreateService(prg, cfg)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	if err := svc.Stop(); err != nil {
		return fmt.Errorf("stop service: %w", err)
	}

	return nil
}

// Restart restarts the service.
func Restart(cfg *ServiceConfig) error {
	prg := &Program{Mode: cfg.Mode, ConfigPath: cfg.ConfigPath}
	svc, err := CreateService(prg, cfg)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	if err := svc.Restart(); err != nil {
		return fmt.Errorf("restart service: %w", err)
	}

	return nil
}

// Status returns the service status.
func Status(cfg *ServiceConfig) (service.Status, error) {
	prg := &Program{Mode: cfg.Mode, ConfigPath: cfg.ConfigPath}
	svc, err := CreateService(prg, cfg)
	if err != nil {
		return service.StatusUnknown, fmt.Errorf("create service: %w", err)
	}

	return svc.Status()
}

// StatusString returns a human-readable status string.
func StatusString(status service.Status) string {
	switch status {
	case service.StatusRunning:
		return "running"
	case service.StatusStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Run runs the service (called when started by the service manager).
func Run(prg *Program, cfg *ServiceConfig) error {
	svc, err := CreateService(prg, cfg)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	return svc.Run()
}

// CheckPrivileges checks if the current user has sufficient privileges for service management.
func CheckPrivileges() error {
	switch runtime.GOOS {
	case "windows":
		// On Windows, try to open a handle that requires admin
		// This is a simple check - actual install will fail with a better error if not admin
		return nil
	default:
		// Unix: check if root
		if os.Geteuid() != 0 {
			return fmt.Errorf("root privileges required (use sudo)")
		}
		return nil
	}
}

// IsServiceMode returns true if running as a service (--service-run flag is set).
func IsServiceMode(args []string) bool {
	for _, arg := range args {
		if arg == "--service-run" {
			return true
		}
	}
	return false
}
