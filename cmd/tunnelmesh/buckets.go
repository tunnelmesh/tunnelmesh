package main

import (
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/context"
)

func newBucketsCmd() *cobra.Command {
	bucketsCmd := &cobra.Command{
		Use:   "buckets",
		Short: "Manage S3 buckets",
		Long: `Manage S3 buckets on the TunnelMesh coordinator.

Requires an active context (run 'tunnelmesh join' to connect to a mesh).

Examples:
  # List all buckets
  tunnelmesh buckets list

  # Create a new bucket
  tunnelmesh buckets create my-bucket

  # List objects in a bucket
  tunnelmesh buckets ls my-bucket

  # Delete an empty bucket
  tunnelmesh buckets delete my-bucket`,
	}

	// List buckets
	listCmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all buckets",
		RunE:    runBucketsList,
	}
	bucketsCmd.AddCommand(listCmd)

	// Create bucket
	createCmd := &cobra.Command{
		Use:   "create <bucket-name>",
		Short: "Create a new bucket",
		Args:  cobra.ExactArgs(1),
		RunE:  runBucketsCreate,
	}
	bucketsCmd.AddCommand(createCmd)

	// Delete bucket
	deleteCmd := &cobra.Command{
		Use:     "delete <bucket-name>",
		Aliases: []string{"rm"},
		Short:   "Delete an empty bucket",
		Args:    cobra.ExactArgs(1),
		RunE:    runBucketsDelete,
	}
	bucketsCmd.AddCommand(deleteCmd)

	// List objects in bucket
	lsCmd := &cobra.Command{
		Use:   "objects <bucket-name> [prefix]",
		Short: "List objects in a bucket",
		Args:  cobra.RangeArgs(1, 2),
		RunE:  runBucketsObjects,
	}
	bucketsCmd.AddCommand(lsCmd)

	return bucketsCmd
}

// s3Client holds the configuration for making S3 requests.
type s3Client struct {
	endpoint  string
	accessKey string
	secretKey string
	client    *http.Client
}

// newS3Client creates an S3 client from the active context.
func newS3Client() (*s3Client, error) {
	// Load context store
	store, err := context.Load()
	if err != nil {
		return nil, fmt.Errorf("load context: %w", err)
	}

	activeCtx := store.GetActive()
	if activeCtx == nil {
		return nil, fmt.Errorf("no active context. Run 'tunnelmesh context use <name>' first")
	}

	// Load registration for S3 credentials
	if activeCtx.RegistrationPath == "" {
		return nil, fmt.Errorf("not registered with this mesh. Run 'tunnelmesh user register' first")
	}

	reg, err := auth.LoadRegistration(activeCtx.RegistrationPath)
	if err != nil {
		return nil, fmt.Errorf("load registration: %w", err)
	}

	if reg.S3AccessKey == "" || reg.S3SecretKey == "" {
		return nil, fmt.Errorf("no S3 credentials found. Re-register with 'tunnelmesh user register'")
	}

	// Get server URL from context or config file
	var serverURL string
	switch {
	case activeCtx.Server != "":
		serverURL = activeCtx.Server
	case activeCtx.ConfigPath != "":
		peerCfg, err := config.LoadPeerConfig(activeCtx.ConfigPath)
		if err != nil {
			return nil, fmt.Errorf("load peer config: %w", err)
		}
		if len(peerCfg.Servers) > 0 {
			serverURL = peerCfg.PrimaryServer()
		}
	default:
		return nil, fmt.Errorf("context %q has no server URL or config path", activeCtx.Name)
	}

	// Build S3 endpoint URL
	if !strings.HasPrefix(serverURL, "http") {
		serverURL = "https://" + serverURL
	}
	// S3 runs on port 9000 by default
	endpoint := strings.TrimSuffix(serverURL, "/")
	if !strings.Contains(endpoint, ":9000") {
		// Replace the port with 9000 for S3
		if idx := strings.LastIndex(endpoint, ":"); idx > 8 { // after https://
			endpoint = endpoint[:idx] + ":9000"
		} else {
			endpoint += ":9000"
		}
	}

	return &s3Client{
		endpoint:  endpoint,
		accessKey: reg.S3AccessKey,
		secretKey: reg.S3SecretKey,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // Allow self-signed mesh CA certs
				},
			},
		},
	}, nil
}

// doRequest makes an authenticated S3 request.
func (c *s3Client) doRequest(method, path string) (*http.Response, error) {
	url := c.endpoint + path

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	// Use Basic Auth (simpler than AWS Signature V4)
	req.SetBasicAuth(c.accessKey, c.secretKey)

	return c.client.Do(req)
}

// XML response structures
type listBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Buckets struct {
		Bucket []struct {
			Name         string `xml:"Name"`
			CreationDate string `xml:"CreationDate"`
		} `xml:"Bucket"`
	} `xml:"Buckets"`
}

type listBucketResult struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	IsTruncated bool     `xml:"IsTruncated"`
	Contents    []struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
	} `xml:"Contents"`
}

type errorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func runBucketsList(cmd *cobra.Command, args []string) error {
	client, err := newS3Client()
	if err != nil {
		return err
	}

	resp, err := client.doRequest(http.MethodGet, "/")
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp errorResponse
		if xml.Unmarshal(body, &errResp) == nil && errResp.Message != "" {
			return fmt.Errorf("%s: %s", errResp.Code, errResp.Message)
		}
		return fmt.Errorf("request failed: %s", resp.Status)
	}

	var result listBucketsResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	if len(result.Buckets.Bucket) == 0 {
		fmt.Println("No buckets found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tCREATED")
	for _, bucket := range result.Buckets.Bucket {
		// Parse and format the date
		created := bucket.CreationDate
		if t, err := time.Parse(time.RFC3339, bucket.CreationDate); err == nil {
			created = t.Format("2006-01-02 15:04:05")
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\n", bucket.Name, created)
	}
	_ = w.Flush()

	return nil
}

// nolint:dupl // Create and delete operations follow similar pattern with different HTTP methods
func runBucketsCreate(cmd *cobra.Command, args []string) error {
	bucketName := args[0]
	if err := validateBucketOrShareName(bucketName); err != nil {
		return fmt.Errorf("invalid bucket name: %w", err)
	}

	client, err := newS3Client()
	if err != nil {
		return err
	}

	resp, err := client.doRequest(http.MethodPut, "/"+bucketName)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		var errResp errorResponse
		if xml.Unmarshal(body, &errResp) == nil && errResp.Message != "" {
			return fmt.Errorf("%s: %s", errResp.Code, errResp.Message)
		}
		return fmt.Errorf("request failed: %s", resp.Status)
	}

	fmt.Printf("Bucket '%s' created.\n", bucketName)
	return nil
}

// nolint:dupl // Companion to create - similar pattern with different method
func runBucketsDelete(cmd *cobra.Command, args []string) error {
	bucketName := args[0]

	client, err := newS3Client()
	if err != nil {
		return err
	}

	resp, err := client.doRequest(http.MethodDelete, "/"+bucketName)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		var errResp errorResponse
		if xml.Unmarshal(body, &errResp) == nil && errResp.Message != "" {
			return fmt.Errorf("%s: %s", errResp.Code, errResp.Message)
		}
		return fmt.Errorf("request failed: %s", resp.Status)
	}

	fmt.Printf("Bucket '%s' deleted.\n", bucketName)
	return nil
}

func runBucketsObjects(cmd *cobra.Command, args []string) error {
	bucketName := args[0]
	prefix := ""
	if len(args) > 1 {
		prefix = args[1]
	}

	client, err := newS3Client()
	if err != nil {
		return err
	}

	path := "/" + bucketName
	if prefix != "" {
		path += "?prefix=" + prefix
	}

	resp, err := client.doRequest(http.MethodGet, path)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp errorResponse
		if xml.Unmarshal(body, &errResp) == nil && errResp.Message != "" {
			return fmt.Errorf("%s: %s", errResp.Code, errResp.Message)
		}
		return fmt.Errorf("request failed: %s", resp.Status)
	}

	var result listBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	if len(result.Contents) == 0 {
		fmt.Println("No objects found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "KEY\tSIZE\tLAST MODIFIED")
	for _, obj := range result.Contents {
		// Format size
		size := formatBytes(obj.Size)
		// Parse and format the date
		modified := obj.LastModified
		if t, err := time.Parse(time.RFC3339, obj.LastModified); err == nil {
			modified = t.Format("2006-01-02 15:04:05")
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", obj.Key, size, modified)
	}
	_ = w.Flush()

	if result.IsTruncated {
		fmt.Println("\n(Results truncated. Use prefix to filter.)")
	}

	return nil
}

// formatBytes formats bytes into human-readable format.
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
