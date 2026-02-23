package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/context"
	"github.com/tunnelmesh/tunnelmesh/internal/peer"
	"github.com/tunnelmesh/tunnelmesh/internal/svc"
)

var (
	contextConfigPath string
)

func newContextCmd() *cobra.Command {
	contextCmd := &cobra.Command{
		Use:   "context",
		Short: "Manage TunnelMesh contexts",
		Long: `Manage multiple TunnelMesh mesh configurations.

Contexts allow you to switch between different mesh networks easily.
Each context stores configuration path, server URL, and connection state.

Examples:
  # Create a context from an existing config
  tunnelmesh context create work --config ~/.tunnelmesh/work.yaml

  # List all contexts
  tunnelmesh context list

  # Switch to a context (also switches DNS)
  tunnelmesh context use work

  # Show context details
  tunnelmesh context show work

  # Delete a context
  tunnelmesh context delete work`,
	}

	// Create subcommand
	createCmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new context",
		Long:  `Create a new context from a configuration file.`,
		Args:  cobra.ExactArgs(1),
		RunE:  runContextCreate,
	}
	createCmd.Flags().StringVarP(&contextConfigPath, "config", "c", "", "Path to configuration file (required)")
	_ = createCmd.MarkFlagRequired("config")
	contextCmd.AddCommand(createCmd)

	// List subcommand
	listCmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all contexts",
		RunE:    runContextList,
	}
	contextCmd.AddCommand(listCmd)

	// Use subcommand
	useCmd := &cobra.Command{
		Use:     "use <name>",
		Aliases: []string{"switch"},
		Short:   "Switch to a context",
		Long:    `Switch the active context. This also reconfigures the system DNS resolver.`,
		Args:    cobra.ExactArgs(1),
		RunE:    runContextUse,
	}
	contextCmd.AddCommand(useCmd)

	// Show subcommand
	showCmd := &cobra.Command{
		Use:     "show [name]",
		Aliases: []string{"get", "info"},
		Short:   "Show context details",
		Long:    `Show details for a context. If no name is provided, shows the active context.`,
		Args:    cobra.MaximumNArgs(1),
		RunE:    runContextShow,
	}
	contextCmd.AddCommand(showCmd)

	// Delete subcommand
	deleteCmd := &cobra.Command{
		Use:     "delete <name>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a context",
		Long:    `Delete a context. Prompts to stop service and clean up if running.`,
		Args:    cobra.ExactArgs(1),
		RunE:    runContextDelete,
	}
	contextCmd.AddCommand(deleteCmd)

	return contextCmd
}

func runContextCreate(cmd *cobra.Command, args []string) error {
	name := args[0]

	// Validate config file exists
	if _, err := os.Stat(contextConfigPath); os.IsNotExist(err) {
		return fmt.Errorf("config file not found: %s", contextConfigPath)
	}

	// Load config to extract server info
	cfg, err := config.LoadPeerConfig(contextConfigPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Load context store
	store, err := context.Load()
	if err != nil {
		return fmt.Errorf("load context store: %w", err)
	}

	// Check if context already exists
	if existing := store.Get(name); existing != nil {
		fmt.Printf("Context %q already exists. Overwrite? [y/N]: ", name)
		reader := bufio.NewReader(os.Stdin)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(strings.ToLower(response))
		if response != "y" && response != "yes" {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	// Resolve server URL: prefer config (CLI join sets Servers), fall back to env var.
	// YAML files never set Servers (yaml:"-"), so env var is the normal provisioning path.
	server := cfg.PrimaryServer()
	if server == "" {
		server = os.Getenv("TUNNELMESH_SERVER")
	}

	// Create context
	ctx := context.Context{
		Name:       name,
		ConfigPath: contextConfigPath,
		Server:     server,
		Domain:     "", // Will be populated on join
		MeshIP:     "", // Will be populated on join
		DNSListen:  cfg.DNS.Listen,
	}

	store.Add(ctx)

	// If this is the first context, make it active
	if store.Count() == 1 {
		store.Active = name
		fmt.Printf("Context %q created and set as active.\n", name)
	} else {
		fmt.Printf("Context %q created.\n", name)
	}

	if err := store.Save(); err != nil {
		return fmt.Errorf("save context store: %w", err)
	}

	return nil
}

func runContextList(cmd *cobra.Command, args []string) error {
	store, err := context.Load()
	if err != nil {
		return fmt.Errorf("load context store: %w", err)
	}

	if store.IsEmpty() {
		fmt.Println("No contexts configured.")
		fmt.Println("\nCreate one with:")
		fmt.Println("  tunnelmesh context create <name> --config <path>")
		fmt.Println("\nOr join a mesh with:")
		fmt.Println("  export TUNNELMESH_TOKEN=<token>")
		fmt.Println("  tunnelmesh join <coordinator-url> --context <name>")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tSERVER\tSTATUS\tACTIVE")

	for _, ctx := range store.List() {
		status := getContextServiceStatus(&ctx)
		active := ""
		if ctx.Name == store.Active {
			active = "*"
		}
		server := ctx.Server
		if server == "" {
			server = "(not configured)"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", ctx.Name, server, status, active)
	}
	_ = w.Flush()

	return nil
}

func runContextUse(cmd *cobra.Command, args []string) error {
	name := args[0]

	store, err := context.Load()
	if err != nil {
		return fmt.Errorf("load context store: %w", err)
	}

	ctx := store.Get(name)
	if ctx == nil {
		return fmt.Errorf("context %q not found", name)
	}

	// Get the old active context for DNS switching
	oldCtx := store.GetActive()

	// Switch DNS resolvers
	if err := switchContextDNS(oldCtx, ctx); err != nil {
		fmt.Printf("Warning: failed to switch DNS: %v\n", err)
	}

	// Set new active context
	if err := store.SetActive(name); err != nil {
		return err
	}

	if err := store.Save(); err != nil {
		return fmt.Errorf("save context store: %w", err)
	}

	fmt.Printf("Switched to context %q\n", name)

	// Show status
	status := getContextServiceStatus(ctx)
	if status != "-" {
		fmt.Printf("Service status: %s\n", status)
	}

	return nil
}

func runContextShow(cmd *cobra.Command, args []string) error {
	store, err := context.Load()
	if err != nil {
		return fmt.Errorf("load context store: %w", err)
	}

	var ctx *context.Context
	if len(args) > 0 {
		ctx = store.Get(args[0])
		if ctx == nil {
			return fmt.Errorf("context %q not found", args[0])
		}
	} else {
		ctx = store.GetActive()
		if ctx == nil {
			return fmt.Errorf("no active context; specify a context name or use 'context use <name>'")
		}
	}

	fmt.Printf("Name:        %s\n", ctx.Name)
	fmt.Printf("Config:      %s\n", ctx.ConfigPath)
	fmt.Printf("Server:      %s\n", valueOrNone(ctx.Server))
	fmt.Printf("Domain:      %s\n", valueOrNone(ctx.Domain))
	fmt.Printf("Mesh IP:     %s\n", valueOrNone(ctx.MeshIP))
	fmt.Printf("DNS Listen:  %s\n", valueOrNone(ctx.DNSListen))
	fmt.Printf("Service:     %s\n", ctx.ServiceName())
	fmt.Printf("Status:      %s\n", getContextServiceStatus(ctx))
	fmt.Printf("Active:      %v\n", ctx.Name == store.Active)

	return nil
}

func runContextDelete(cmd *cobra.Command, args []string) error {
	name := args[0]

	store, err := context.Load()
	if err != nil {
		return fmt.Errorf("load context store: %w", err)
	}

	ctx := store.Get(name)
	if ctx == nil {
		return fmt.Errorf("context %q not found", name)
	}

	reader := bufio.NewReader(os.Stdin)

	// Check if service is running
	svcName := ctx.ServiceName()
	svcCfg := &svc.ServiceConfig{Name: svcName, Mode: "join", ConfigPath: ctx.ConfigPath}
	status, err := svc.Status(svcCfg)
	if err == nil {
		switch status {
		case 1: // StatusRunning
			fmt.Printf("Service %q is running. Stop and uninstall? [Y/n]: ", svcName)
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))
			if response == "" || response == "y" || response == "yes" {
				if err := svc.Stop(svcCfg); err != nil {
					fmt.Printf("Warning: failed to stop service: %v\n", err)
				}
				if err := svc.Uninstall(svcCfg); err != nil {
					fmt.Printf("Warning: failed to uninstall service: %v\n", err)
				} else {
					fmt.Printf("Service %q uninstalled.\n", svcName)
				}
			}
		case 2: // StatusStopped
			fmt.Printf("Service %q is installed but stopped. Uninstall? [Y/n]: ", svcName)
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))
			if response == "" || response == "y" || response == "yes" {
				if err := svc.Uninstall(svcCfg); err != nil {
					fmt.Printf("Warning: failed to uninstall service: %v\n", err)
				} else {
					fmt.Printf("Service %q uninstalled.\n", svcName)
				}
			}
		}
	}

	// Remove DNS resolver entry if domain is set
	if ctx.Domain != "" {
		removeSystemResolver(ctx.Domain, "")
	}

	// Prompt to remove config/credentials
	if ctx.ConfigPath != "" {
		fmt.Printf("Also remove config file at %s? [y/N]: ", ctx.ConfigPath)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(strings.ToLower(response))
		if response == "y" || response == "yes" {
			// Load config to find TLS directory before removing
			if cfg, err := config.LoadPeerConfig(ctx.ConfigPath); err == nil {
				// Remove TLS certificates
				tlsDataDir := filepath.Dir(cfg.PrivateKey)
				tlsMgr := peer.NewTLSManager(tlsDataDir)
				if tlsMgr.HasCert() {
					tlsDir := filepath.Join(tlsDataDir, "tls")
					if err := os.RemoveAll(tlsDir); err != nil {
						fmt.Printf("Warning: failed to remove TLS certificates: %v\n", err)
					} else {
						fmt.Printf("TLS certificates removed.\n")
					}
				}
			}

			if err := os.Remove(ctx.ConfigPath); err != nil {
				fmt.Printf("Warning: failed to remove config: %v\n", err)
			} else {
				fmt.Printf("Config file removed.\n")
			}
		}
	}

	// Remove CA from system trust store
	if trusted, _ := IsCATrusted(); trusted {
		if err := RemoveCA(); err != nil {
			fmt.Printf("Warning: failed to remove CA certificate: %v\n", err)
		} else {
			fmt.Printf("CA certificate removed from system trust store.\n")
		}
	}

	// Remove from store
	store.Remove(name)

	if err := store.Save(); err != nil {
		return fmt.Errorf("save context store: %w", err)
	}

	fmt.Printf("Context %q deleted.\n", name)

	// If we deleted the active context, suggest setting a new one
	if !store.HasActive() && !store.IsEmpty() {
		contexts := store.List()
		fmt.Printf("\nNo active context. Set one with:\n")
		fmt.Printf("  tunnelmesh context use %s\n", contexts[0].Name)
	}

	return nil
}

// switchContextDNS removes the old context's DNS resolver and adds the new one.
func switchContextDNS(oldCtx, newCtx *context.Context) error {
	// Remove old resolver
	if oldCtx != nil && oldCtx.Domain != "" {
		removeSystemResolver(oldCtx.Domain, "")
	}

	// Add new resolver
	if newCtx != nil && newCtx.Domain != "" && newCtx.DNSListen != "" {
		if err := configureSystemResolver(newCtx.Domain, newCtx.DNSListen, ""); err != nil {
			return fmt.Errorf("configure DNS resolver for %s: %w", newCtx.Domain, err)
		}
	}

	return nil
}

// getContextServiceStatus returns the service status for a context.
func getContextServiceStatus(ctx *context.Context) string {
	svcCfg := &svc.ServiceConfig{Name: ctx.ServiceName(), Mode: "join", ConfigPath: ctx.ConfigPath}
	status, err := svc.Status(svcCfg)
	if err != nil {
		return "-"
	}
	return svc.StatusString(status)
}

func valueOrNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
