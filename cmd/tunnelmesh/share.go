package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

func newShareCmd() *cobra.Command {
	shareCmd := &cobra.Command{
		Use:   "share",
		Short: "Manage file shares",
		Long: `Manage TunnelMesh file shares.

File shares are S3 buckets with special permissions:
- Everyone group gets bucket-read access by default
- Share owner gets bucket-admin access

Files in shares can be accessed via the S3 API or web UI.

Examples:
  # List all shares
  tunnelmesh share list

  # Create a new share
  tunnelmesh share create documents --description "Shared documents"

  # Delete a share
  tunnelmesh share delete documents`,
	}

	// List subcommand
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all file shares",
		RunE:  runShareList,
	}
	shareCmd.AddCommand(listCmd)

	// Create subcommand
	createCmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new file share",
		Args:  cobra.ExactArgs(1),
		RunE:  runShareCreate,
	}
	createCmd.Flags().StringP("description", "d", "", "Share description")
	shareCmd.AddCommand(createCmd)

	// Delete subcommand
	deleteCmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a file share",
		Args:  cobra.ExactArgs(1),
		RunE:  runShareDelete,
	}
	shareCmd.AddCommand(deleteCmd)

	// Info subcommand
	infoCmd := &cobra.Command{
		Use:   "info <name>",
		Short: "Show file share details",
		Args:  cobra.ExactArgs(1),
		RunE:  runShareInfo,
	}
	shareCmd.AddCommand(infoCmd)

	return shareCmd
}

func runShareList(_ *cobra.Command, _ []string) error {
	adminURL := getAdminURL()

	resp, err := makeAdminRequest("GET", adminURL+"/api/shares", nil)
	if err != nil {
		return fmt.Errorf("failed to connect to admin API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to list shares: %s", string(body))
	}

	var shares []*s3.FileShare
	if err := json.NewDecoder(resp.Body).Decode(&shares); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if len(shares) == 0 {
		fmt.Println("No file shares found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tOWNER\tCREATED\tDESCRIPTION")
	for _, s := range shares {
		created := s.CreatedAt.Format("2006-01-02")
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", s.Name, s.Owner, created, s.Description)
	}
	_ = w.Flush()

	return nil
}

func runShareCreate(cmd *cobra.Command, args []string) error {
	name := args[0]
	if err := validateBucketOrShareName(name); err != nil {
		return fmt.Errorf("invalid share name: %w", err)
	}
	description, _ := cmd.Flags().GetString("description")

	adminURL := getAdminURL()

	reqBody := map[string]string{
		"name":        name,
		"description": description,
	}
	body, _ := json.Marshal(reqBody)

	resp, err := makeAdminRequest("POST", adminURL+"/api/shares", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to connect to admin API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create share: %s", string(respBody))
	}

	fmt.Printf("File share '%s' created successfully\n", name)
	fmt.Printf("Access via S3: s3://fs+%s/\n", name)
	return nil
}

// nolint:revive // cmd required by cobra.Command RunE signature
func runShareDelete(cmd *cobra.Command, args []string) error {
	name := args[0]

	adminURL := getAdminURL()

	resp, err := makeAdminRequest("DELETE", adminURL+"/api/shares/"+name, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to admin API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete share: %s", string(body))
	}

	fmt.Printf("File share '%s' deleted\n", name)
	return nil
}

// nolint:revive // cmd required by cobra.Command RunE signature

func runShareInfo(cmd *cobra.Command, args []string) error {
	name := args[0]

	adminURL := getAdminURL()

	resp, err := makeAdminRequest("GET", adminURL+"/api/shares/"+name, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to admin API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("share not found: %s", string(body))
	}

	var share s3.FileShare
	if err := json.NewDecoder(resp.Body).Decode(&share); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	fmt.Printf("Name:        %s\n", share.Name)
	fmt.Printf("Description: %s\n", share.Description)
	fmt.Printf("Owner:       %s\n", share.Owner)
	fmt.Printf("Created:     %s\n", share.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("Bucket:      fs+%s\n", share.Name)
	fmt.Printf("S3 URL:      s3://fs+%s/\n", share.Name)

	return nil
}
