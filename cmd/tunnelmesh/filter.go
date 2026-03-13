package main

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/tunnelmesh/tunnelmesh/internal/control"
)

func newFilterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "filter",
		Short: "Manage packet filter rules",
		Long: `Manage packet filter rules for incoming traffic.

The packet filter controls which ports are accessible on this peer.
Rules can be added temporarily via CLI (persisted when S3 is enabled on
the coordinator) or permanently via the config file.

Rule precedence (most restrictive wins):
  1. Coordinator config (global rules)
  2. Peer config (local rules)
  3. Temporary rules (CLI/admin panel)

If any rule denies a port, it is denied regardless of other allow rules.`,
	}

	cmd.AddCommand(newFilterListCmd())
	cmd.AddCommand(newFilterAddCmd())
	cmd.AddCommand(newFilterRemoveCmd())

	return cmd
}

func newFilterListCmd() *cobra.Command {
	var socketPath string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all filter rules",
		Long:  `List all filter rules from all sources (coordinator, config, temporary).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if socketPath == "" {
				socketPath = control.DefaultSocketPath()
			}

			client := control.NewClient(socketPath)
			resp, err := client.FilterList()
			if err != nil {
				return fmt.Errorf("failed to list filter rules: %w", err)
			}

			// Print header
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintf(w, "PORT\tPROTOCOL\tACTION\tSOURCE PEER\tSOURCE\tEXPIRES\n")

			for _, rule := range resp.Rules {
				expires := "-"
				if rule.Expires > 0 {
					remaining := time.Until(time.Unix(rule.Expires, 0))
					if remaining > 0 {
						expires = formatDuration(remaining)
					} else {
						expires = "expired"
					}
				}
				sourcePeer := "*"
				if rule.SourcePeer != "" {
					sourcePeer = rule.SourcePeer
				}
				_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\n",
					rule.Port, strings.ToUpper(rule.Protocol), rule.Action, sourcePeer, rule.Source, expires)
			}
			_ = w.Flush()

			// Print summary
			fmt.Printf("\nDefault policy: %s\n", policyString(resp.DefaultDeny))
			fmt.Printf("Total rules: %d\n", len(resp.Rules))

			return nil
		},
	}

	cmd.Flags().StringVar(&socketPath, "socket", "", "Control socket path (default: ~/.tunnelmesh/control.sock)")

	return cmd
}

func newFilterAddCmd() *cobra.Command {
	var (
		socketPath string
		port       uint16
		protocol   string
		action     string
		ttl        int64
		sourcePeer string
	)

	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add a temporary filter rule",
		Long: `Add a filter rule to the temporary layer.

When S3 persistence is enabled on the coordinator, temporary rules
persist across restarts. Otherwise, they are lost on reboot.

Examples:
  # Allow SSH access from any peer
  tunnelmesh filter add --port 22 --protocol tcp

  # Allow HTTP with 1 hour expiry
  tunnelmesh filter add --port 80 --protocol tcp --ttl 3600

  # Deny UDP port 53
  tunnelmesh filter add --port 53 --protocol udp --action deny

  # Allow SSH only from specific peer
  tunnelmesh filter add --port 22 --protocol tcp --source-peer trusted-peer

  # Block specific peer from accessing port 80
  tunnelmesh filter add --port 80 --protocol tcp --action deny --source-peer untrusted-peer`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if port == 0 {
				return fmt.Errorf("--port is required - must be between 1 and 65535")
			}
			protocol = strings.ToLower(protocol)
			if protocol != "tcp" && protocol != "udp" {
				return fmt.Errorf("invalid protocol %q - must be one of: tcp, udp", protocol)
			}
			action = strings.ToLower(action)
			if action != "allow" && action != "deny" {
				return fmt.Errorf("invalid action %q - must be one of: allow, deny", action)
			}

			if socketPath == "" {
				socketPath = control.DefaultSocketPath()
			}

			client := control.NewClient(socketPath)
			if err := client.FilterAddForPeer(port, protocol, action, ttl, sourcePeer); err != nil {
				return fmt.Errorf("failed to add rule: %w", err)
			}

			ttlStr := "permanent"
			if ttl > 0 {
				ttlStr = fmt.Sprintf("expires in %s", formatDuration(time.Duration(ttl)*time.Second))
			}
			peerStr := "all peers"
			if sourcePeer != "" {
				peerStr = fmt.Sprintf("peer '%s'", sourcePeer)
			}
			fmt.Printf("Added rule: %s port %d/%s from %s (%s)\n", action, port, strings.ToUpper(protocol), peerStr, ttlStr)
			return nil
		},
	}

	cmd.Flags().StringVar(&socketPath, "socket", "", "Control socket path")
	cmd.Flags().Uint16Var(&port, "port", 0, "Port number (required)")
	cmd.Flags().StringVar(&protocol, "protocol", "tcp", "Protocol: tcp or udp")
	cmd.Flags().StringVar(&action, "action", "allow", "Action: allow or deny")
	cmd.Flags().Int64Var(&ttl, "ttl", 0, "Time to live in seconds (0 = permanent)")
	cmd.Flags().StringVar(&sourcePeer, "source-peer", "", "Source peer name (empty = any peer)")

	_ = cmd.MarkFlagRequired("port")

	return cmd
}

func newFilterRemoveCmd() *cobra.Command {
	var (
		socketPath string
		port       uint16
		protocol   string
		sourcePeer string
	)

	cmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove a temporary filter rule",
		Long: `Remove a temporary filter rule.

Note: Only temporary rules (added via CLI or admin panel) can be removed.
Rules from config files must be removed by editing the config.

Examples:
  # Remove global rule for port 22
  tunnelmesh filter remove --port 22 --protocol tcp

  # Remove peer-specific rule
  tunnelmesh filter remove --port 22 --protocol tcp --source-peer untrusted-peer`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if port == 0 {
				return fmt.Errorf("--port is required - must be between 1 and 65535")
			}
			protocol = strings.ToLower(protocol)
			if protocol != "tcp" && protocol != "udp" {
				return fmt.Errorf("invalid protocol %q - must be one of: tcp, udp", protocol)
			}

			if socketPath == "" {
				socketPath = control.DefaultSocketPath()
			}

			client := control.NewClient(socketPath)
			if err := client.FilterRemoveForPeer(port, protocol, sourcePeer); err != nil {
				return fmt.Errorf("failed to remove rule: %w", err)
			}

			peerStr := "all peers"
			if sourcePeer != "" {
				peerStr = fmt.Sprintf("peer '%s'", sourcePeer)
			}
			fmt.Printf("Removed temporary rule for port %d/%s from %s\n", port, strings.ToUpper(protocol), peerStr)
			return nil
		},
	}

	cmd.Flags().StringVar(&socketPath, "socket", "", "Control socket path")
	cmd.Flags().Uint16Var(&port, "port", 0, "Port number (required)")
	cmd.Flags().StringVar(&protocol, "protocol", "tcp", "Protocol: tcp or udp")
	cmd.Flags().StringVar(&sourcePeer, "source-peer", "", "Source peer name (empty = global rule)")

	_ = cmd.MarkFlagRequired("port")

	return cmd
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func policyString(defaultDeny bool) string {
	if defaultDeny {
		return "deny (allowlist mode - only allowed ports are accessible)"
	}
	return "allow (blocklist mode - all ports accessible unless denied)"
}
