// tunnelmesh-s3bench is a story-driven S3 stress testing tool.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"encoding/json"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
	"github.com/tunnelmesh/tunnelmesh/internal/s3bench/mesh"
	"github.com/tunnelmesh/tunnelmesh/internal/s3bench/simulator"
	"github.com/tunnelmesh/tunnelmesh/internal/s3bench/story"
	"github.com/tunnelmesh/tunnelmesh/internal/s3bench/story/scenarios"
)

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

// Global flags
var (
	logLevel    string
	jsonOutput  string
	verbose     bool
	storageRoot string
	endpoint    string
)

// Run command flags
var (
	timeScale            float64
	concurrency          int
	enableMesh           bool
	enableAdversary      bool
	enableWorkflows      bool
	testDeletion         bool
	testExpiration       bool
	testPermissions      bool
	testQuota            bool
	testRetention        bool
	quotaOverrideMB      int64
	expiryOverride       time.Duration
	adversaryAttempts    int
	maxConcurrentUploads int

	// Mesh integration flags
	coordinatorURL string
	sshKeyPath     string
	insecureTLS    bool
	authToken      string

	// Accordion mode flags
	accordion      bool
	accordionCount int
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "tunnelmesh-s3bench",
	Short: "Story-driven S3 stress testing tool",
	Long: `tunnelmesh-s3bench is a narrative-driven S3 stress testing tool that generates
realistic workloads based on story scenarios. It tests S3 storage, deduplication,
versioning, RBAC, file shares, and mesh dynamics.`,
	Version: fmt.Sprintf("%s (commit: %s, built: %s)", Version, Commit, BuildTime),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		setupLogging()
	},
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&jsonOutput, "json", "", "Write results to JSON file")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")
	rootCmd.PersistentFlags().StringVar(&storageRoot, "storage-root", "", "Storage root directory (default: temp dir)")
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "http://localhost:8080", "S3 endpoint URL")

	// Add subcommands
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(describeCmd)

	// Run command flags
	runCmd.Flags().Float64Var(&timeScale, "time-scale", 1.0, "Time scaling factor (1.0=realtime, 100.0=100x faster)")
	runCmd.Flags().IntVar(&concurrency, "concurrency", 3, "Number of parallel users")
	runCmd.Flags().BoolVar(&enableMesh, "enable-mesh", false, "Use Docker mesh orchestration")
	runCmd.Flags().BoolVar(&enableAdversary, "enable-adversary", true, "Enable adversarial characters")
	runCmd.Flags().BoolVar(&enableWorkflows, "enable-workflows", true, "Enable workflow tests")
	runCmd.Flags().BoolVar(&testDeletion, "test-deletion", true, "Enable deletion workflow tests")
	runCmd.Flags().BoolVar(&testExpiration, "test-expiration", true, "Enable expiration workflow tests")
	runCmd.Flags().BoolVar(&testPermissions, "test-permissions", true, "Enable permission workflow tests")
	runCmd.Flags().BoolVar(&testQuota, "test-quota", true, "Enable quota workflow tests")
	runCmd.Flags().BoolVar(&testRetention, "test-retention", true, "Enable retention workflow tests")
	runCmd.Flags().Int64Var(&quotaOverrideMB, "quota-override", 0, "Override file share quotas in MB (0=use story defaults)")
	runCmd.Flags().DurationVar(&expiryOverride, "expiry-override", 0, "Override document expiries (0=use story defaults)")
	runCmd.Flags().IntVar(&adversaryAttempts, "adversary-attempts", 50, "Number of adversary attempts to generate")
	runCmd.Flags().IntVar(&maxConcurrentUploads, "max-concurrent-uploads", 10, "Maximum concurrent uploads")

	// Mesh integration flags
	runCmd.Flags().StringVar(&coordinatorURL, "coordinator", "", "Coordinator URL (enables mesh mode, e.g., https://coordinator.example.com:8443)")
	runCmd.Flags().StringVar(&sshKeyPath, "ssh-key", "", "SSH private key path (default: ~/.tunnelmesh/s3bench_key)")
	runCmd.Flags().BoolVar(&insecureTLS, "insecure-tls", true, "Skip TLS certificate verification (admin mux uses mesh CA)")
	runCmd.Flags().StringVar(&authToken, "auth-token", "", "Coordinator auth token (for protected coordinators)")

	// Accordion mode flags
	runCmd.Flags().BoolVar(&accordion, "accordion", false, "Loop mode: run → cleanup → repeat (requires --coordinator)")
	runCmd.Flags().IntVar(&accordionCount, "count", 0, "Run N iterations then stop (implies --accordion)")
}

var runCmd = &cobra.Command{
	Use:   "run <story>",
	Short: "Execute a story scenario",
	Long: `Execute a story-driven stress test scenario. Available stories:
  - alien_invasion: 72h alien invasion with 3 characters, 4 departments (default)

Example:
  # Quick test (1 minute)
  s3bench run alien_invasion --time-scale 4320

  # Stress test (5 minutes)
  s3bench run alien_invasion --time-scale 864 --enable-mesh --enable-adversary

  # Realistic demo (2 hours)
  s3bench run alien_invasion --time-scale 36

  # Accordion mode: run 3 iterations with cleanup between each
  s3bench run alien_invasion --time-scale 4320 --coordinator https://coord:8443 --count 3

  # Accordion mode: loop until Ctrl+C
  s3bench run alien_invasion --time-scale 4320 --coordinator https://coord:8443 --accordion`,
	Args: cobra.ExactArgs(1),
	RunE: runScenario,
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List available story scenarios",
	RunE: func(cmd *cobra.Command, args []string) error {
		stories := getAvailableStories()
		fmt.Println("Available story scenarios:")
		fmt.Println()
		for name, st := range stories {
			fmt.Printf("  %s\n", name)
			fmt.Printf("    %s\n", st.Description())
			fmt.Printf("    Duration: %v\n", st.Duration())
			fmt.Printf("    Characters: %d\n", len(st.Characters()))
			fmt.Printf("    Departments: %d\n", len(st.Departments()))
			fmt.Println()
		}
		return nil
	},
}

var describeCmd = &cobra.Command{
	Use:   "describe <story>",
	Short: "Show detailed information about a story",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		storyName := args[0]
		stories := getAvailableStories()
		st, ok := stories[storyName]
		if !ok {
			return fmt.Errorf("story %q not found", storyName)
		}

		printStoryDetails(st)
		return nil
	},
}

func setupLogging() {
	// Parse log level
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	// Setup console output with colors
	if !verbose {
		log.Logger = log.Output(zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.RFC3339,
			NoColor:    false,
		})
	}
}

func getAvailableStories() map[string]story.Story {
	return map[string]story.Story{
		"alien_invasion": &scenarios.AlienInvasion{},
	}
}

func runScenario(cmd *cobra.Command, args []string) error {
	// Validate accordion flags
	if accordionCount > 0 {
		accordion = true
	}
	if accordion && coordinatorURL == "" {
		return fmt.Errorf("--accordion requires --coordinator")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info().Msg("Received interrupt signal, shutting down...")
		cancel()
	}()

	// Get story
	storyName := args[0]
	stories := getAvailableStories()
	st, ok := stories[storyName]
	if !ok {
		return fmt.Errorf("story %q not found. Use 'list' to see available stories", storyName)
	}

	// Create storage root
	root := storageRoot
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "s3bench-*")
		if err != nil {
			return fmt.Errorf("creating temp dir: %w", err)
		}
		defer func() {
			if err := os.RemoveAll(root); err != nil {
				log.Warn().Err(err).Str("path", root).Msg("Failed to remove temp directory")
			}
		}()
		log.Info().Str("path", root).Msg("Created temporary storage directory")
	}

	// Create store
	quotaMgr := s3.NewQuotaManager(100 * 1024 * 1024 * 1024) // 100GB
	masterKey := [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	store, err := s3.NewStoreWithCAS(root, quotaMgr, masterKey)
	if err != nil {
		return fmt.Errorf("creating store: %w", err)
	}

	// Create components
	credentials := s3.NewCredentialStore()
	authorizer := auth.NewAuthorizerWithGroups()
	systemStore, err := s3.NewSystemStore(store, "s3bench")
	if err != nil {
		return fmt.Errorf("creating system store: %w", err)
	}
	shareManager := s3.NewFileShareManager(store, systemStore, authorizer)

	// Create user manager
	userMgr := simulator.NewUserManager(store, credentials, authorizer, shareManager, st)
	if quotaOverrideMB > 0 {
		userMgr.SetQuotaOverride(quotaOverrideMB)
	}

	// Setup users and file shares
	log.Info().Msg("Setting up users, credentials, and file shares...")
	if err := userMgr.Setup(ctx, endpoint); err != nil {
		return fmt.Errorf("setting up users: %w", err)
	}
	defer func() {
		if err := userMgr.Cleanup(); err != nil {
			log.Warn().Err(err).Msg("Failed to cleanup users")
		}
	}()

	// Mesh integration (optional)
	var meshClient *mesh.CoordinatorClient
	var meshInfo *mesh.MeshInfo
	if coordinatorURL != "" {
		log.Info().Str("coordinator", coordinatorURL).Msg("Mesh integration enabled")

		// Load or generate SSH credentials
		log.Info().Str("key_path", sshKeyPath).Msg("Loading SSH key")
		creds, err := mesh.LoadOrGenerateCredentials(sshKeyPath)
		if err != nil {
			return fmt.Errorf("loading credentials: %w", err)
		}

		// Register with coordinator
		log.Info().Msg("Registering with coordinator...")
		meshInfo, err = mesh.RegisterWithCoordinator(ctx, coordinatorURL, creds, insecureTLS, authToken)
		if err != nil {
			return fmt.Errorf("registration failed: %w", err)
		}

		log.Info().
			Str("peer_id", meshInfo.PeerID).
			Str("mesh_ip", meshInfo.MeshIP).
			Bool("is_admin", meshInfo.IsAdmin).
			Msg("Registration successful")

		// Derive S3 credentials
		log.Info().Str("access_key", creds.AccessKey).Msg("Derived S3 credentials")

		// Admin routes (/api/shares, /api/s3/*) are only served on the admin
		// mux bound to mesh_ip:443, not the public server. Use the coordinator's
		// mesh IP from the registration response.
		if len(meshInfo.CoordMeshIPs) == 0 {
			return fmt.Errorf("no coordinator mesh IPs in registration response")
		}
		adminURL := "https://" + meshInfo.CoordMeshIPs[0]

		log.Info().
			Str("s3_endpoint", adminURL).
			Msg("Using coordinator admin mux")
		meshClient = mesh.NewCoordinatorClient(adminURL, creds, insecureTLS)
	}

	// Print scenario summary
	printScenarioIntro(st, timeScale)

	// Determine iteration count
	totalIterations := 1
	if accordion {
		if accordionCount > 0 {
			totalIterations = accordionCount
		} else {
			totalIterations = 0 // 0 = infinite (until Ctrl+C)
		}
	}

	// Iteration loop
	for iteration := 1; totalIterations == 0 || iteration <= totalIterations; iteration++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if accordion {
			printIterationHeader(iteration, totalIterations)
		}

		sharePrefix, err := runIteration(ctx, st, userMgr, meshClient, meshInfo, iteration, totalIterations)
		if err != nil {
			return err
		}

		// Cleanup between iterations (not after the last one)
		moreIterations := totalIterations == 0 || iteration < totalIterations
		if accordion && moreIterations && meshClient != nil {
			peerName := ""
			if meshInfo != nil {
				peerName = meshInfo.PeerName
			}
			if err := cleanupMeshShares(ctx, meshClient, st, sharePrefix, peerName); err != nil {
				return err
			}
		}
	}

	return nil
}

func printIterationHeader(iteration, totalIterations int) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	if totalIterations > 0 {
		fmt.Printf("║  ITERATION %d / %d%s║\n", iteration, totalIterations,
			strings.Repeat(" ", 46-len(fmt.Sprintf("%d / %d", iteration, totalIterations))))
	} else {
		fmt.Printf("║  ITERATION %d (Ctrl+C to stop)%s║\n", iteration,
			strings.Repeat(" ", 28-len(fmt.Sprintf("%d", iteration))))
	}
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()
}

// runIteration runs a single benchmark iteration. Returns the share prefix used.
func runIteration(ctx context.Context, st story.Story, userMgr *simulator.UserManager, meshClient *mesh.CoordinatorClient, meshInfo *mesh.MeshInfo, iteration, totalIterations int) (string, error) {
	// Create shares on coordinator
	var sharePrefix string
	if meshClient != nil {
		var err error
		sharePrefix, err = createMeshShares(ctx, meshClient, st, meshInfo)
		if err != nil {
			return "", err
		}
	}

	// Configure simulator
	config := simulator.SimulatorConfig{
		Story:                st,
		TimeScale:            timeScale,
		EnableMesh:           enableMesh,
		EnableAdversary:      enableAdversary,
		EnableWorkflows:      enableWorkflows,
		QuotaOverrideMB:      quotaOverrideMB,
		ExpiryOverride:       expiryOverride,
		AdversaryAttempts:    adversaryAttempts,
		MaxConcurrentUploads: maxConcurrentUploads,
		UserManager:          userMgr,
		UseMesh:              meshClient != nil,
		MeshClient:           meshClient,
		SharePrefix:          sharePrefix,
		WorkflowTestsEnabled: map[simulator.WorkflowType]bool{
			simulator.WorkflowDeletion:    testDeletion,
			simulator.WorkflowExpiration:  testExpiration,
			simulator.WorkflowPermissions: testPermissions,
			simulator.WorkflowQuota:       testQuota,
			simulator.WorkflowRetention:   testRetention,
		},
	}

	// Create and run simulator
	log.Info().Msg("Creating simulator...")
	sim, err := simulator.NewSimulator(config)
	if err != nil {
		return "", fmt.Errorf("creating simulator: %w", err)
	}

	log.Info().Msg("Generating scenario...")
	if err := sim.GenerateScenario(ctx); err != nil {
		return "", fmt.Errorf("generating scenario: %w", err)
	}

	summary := sim.GetScenarioSummary()
	fmt.Println()
	fmt.Println(summary.String())
	fmt.Println()

	log.Info().Msg("Starting scenario execution...")
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println()

	metrics, err := sim.Run(ctx)
	if err != nil {
		return "", fmt.Errorf("running scenario: %w", err)
	}

	printFinalReport(metrics)

	if meshClient != nil {
		fmt.Println()
		log.Info().Msg("Documents uploaded to coordinator and viewable in Objects browser")
		if !insecureTLS {
			log.Info().Str("url", fmt.Sprintf("%s/#/data/s3", coordinatorURL)).Msg("Access web UI")
		}
	}

	if jsonOutput != "" {
		if err := writeJSONOutput(jsonOutput, metrics, st, meshClient != nil, coordinatorURL, iteration, totalIterations); err != nil {
			log.Error().Err(err).Str("file", jsonOutput).Msg("Failed to write JSON output")
			return "", fmt.Errorf("writing JSON output: %w", err)
		}
		log.Info().Str("file", jsonOutput).Msg("Results written to JSON file")
	}

	return sharePrefix, nil
}

// createMeshShares creates file shares on the coordinator for each story department.
// Returns the share prefix derived from the coordinator's auto-prefixed name.
func createMeshShares(ctx context.Context, client *mesh.CoordinatorClient, st story.Story, meshInfo *mesh.MeshInfo) (string, error) {
	log.Info().Msg("Creating shares on coordinator...")
	var sharePrefix string

	for _, dept := range st.Departments() {
		quotaMB := dept.QuotaMB
		if quotaOverrideMB > 0 {
			quotaMB = quotaOverrideMB
		}

		actualName, err := client.CreateShare(ctx, dept.FileShare, dept.Name, quotaMB)
		if err != nil {
			return "", fmt.Errorf("creating share %s: %w", dept.FileShare, err)
		}

		if actualName == "" {
			log.Info().Str("share", dept.FileShare).Msg("Share already exists, skipping creation")
		} else {
			if idx := strings.Index(actualName, "_"); idx > 0 {
				sharePrefix = actualName[:idx]
			}
			log.Info().
				Str("share", actualName).
				Int64("quota_mb", quotaMB).
				Msg("Created share")
		}
	}

	// Fallback: derive from peer name if all shares already existed
	if sharePrefix == "" {
		sharePrefix = meshInfo.PeerName
	}

	return sharePrefix, nil
}

// cleanupMeshShares deletes all shares, triggers GC, and deregisters the peer.
func cleanupMeshShares(ctx context.Context, client *mesh.CoordinatorClient, st story.Story, prefix string, peerName string) error {
	fmt.Println()
	log.Info().Msg("Cleaning up shares and triggering GC...")

	// Delete each department share
	for _, dept := range st.Departments() {
		shareName := prefix + "_" + dept.FileShare
		log.Info().Str("share", shareName).Msg("Deleting share")
		if err := client.DeleteShare(ctx, shareName); err != nil {
			return fmt.Errorf("deleting share %s: %w", shareName, err)
		}
	}

	// Trigger GC to purge recycled data and reclaim disk
	log.Info().Msg("Triggering garbage collection...")
	gcStats, err := client.TriggerGC(ctx, true)
	if err != nil {
		return fmt.Errorf("triggering GC: %w", err)
	}

	log.Info().
		Int("recycled_purged", gcStats.RecycledPurged).
		Int("versions_pruned", gcStats.VersionsPruned).
		Int("chunks_deleted", gcStats.ChunksDeleted).
		Int64("bytes_reclaimed", gcStats.BytesReclaimed).
		Msg("GC completed")

	// Deregister the peer so the coordinator releases the IP allocation immediately
	// rather than waiting for the stale-peer eviction TTL.
	if err := client.DeregisterPeer(ctx, peerName); err != nil {
		log.Warn().Err(err).Msg("failed to deregister peer (non-fatal)")
	} else {
		log.Info().Str("peer", peerName).Msg("peer deregistered from coordinator")
	}

	// Brief pause before next iteration
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(2 * time.Second):
	}

	return nil
}

func printScenarioIntro(st story.Story, timeScale float64) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Printf("║  %-55s  ║\n", "SCENARIO: "+st.Name())
	fmt.Println("║                                                            ║")
	fmt.Printf("║  %-55s  ║\n", st.Description())
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	realDuration := story.ScaledDuration(st.Duration(), timeScale)
	fmt.Printf("Story Duration: %v (%.1fx speed = %v real time)\n",
		st.Duration(), timeScale, realDuration)
	fmt.Println()
}

func printStoryDetails(st story.Story) {
	fmt.Println()
	fmt.Println("Story:", st.Name())
	fmt.Println("Description:", st.Description())
	fmt.Println("Duration:", st.Duration())
	fmt.Println()

	fmt.Println("Characters:")
	for _, char := range st.Characters() {
		alignment := "good"
		if char.IsAdversary() {
			alignment = "ADVERSARY"
		}
		fmt.Printf("  - %s (%s) - %s [%s]\n", char.Name, char.Role, alignment, char.Department)
		fmt.Printf("    Clearance: %d, Joins: %v", char.Clearance, char.JoinTime)
		if char.LeaveTime > 0 {
			fmt.Printf(", Leaves: %v", char.LeaveTime)
		}
		fmt.Println()
	}
	fmt.Println()

	fmt.Println("Departments (File Shares):")
	for _, dept := range st.Departments() {
		fmt.Printf("  - %s (%s)\n", dept.Name, dept.FileShare)
		fmt.Printf("    Members: %v\n", dept.Members)
		fmt.Printf("    Quota: %d MB, Guest Read: %v\n", dept.QuotaMB, dept.GuestRead)
	}
	fmt.Println()
}

func printFinalReport(metrics *simulator.SimulatorMetrics) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║                     FINAL REPORT                          ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Timing
	fmt.Println("Timing:")
	fmt.Printf("  Started:        %s\n", metrics.StartTime.Format(time.RFC3339))
	fmt.Printf("  Ended:          %s\n", metrics.EndTime.Format(time.RFC3339))
	fmt.Printf("  Real Duration:  %v\n", metrics.Duration)
	fmt.Printf("  Story Duration: %v\n", metrics.StoryDuration)
	fmt.Println()

	// Operations
	fmt.Println("Operations:")
	fmt.Printf("  Tasks Generated: %d\n", metrics.TasksGenerated)
	fmt.Printf("  Tasks Completed: %d\n", metrics.TasksCompleted)
	fmt.Printf("  Tasks Failed:    %d\n", metrics.TasksFailed)
	fmt.Printf("  Uploads:         %d\n", metrics.UploadCount)
	fmt.Printf("  Downloads:       %d\n", metrics.DownloadCount)
	fmt.Printf("  Updates:         %d\n", metrics.UpdateCount)
	fmt.Printf("  Deletes:         %d\n", metrics.DeleteCount)
	fmt.Println()

	// Data
	fmt.Println("Data:")
	fmt.Printf("  Bytes Uploaded:   %d (%.2f MB)\n", metrics.BytesUploaded, float64(metrics.BytesUploaded)/1024/1024)
	fmt.Printf("  Bytes Downloaded: %d (%.2f MB)\n", metrics.BytesDownloaded, float64(metrics.BytesDownloaded)/1024/1024)
	fmt.Printf("  Documents:        %d\n", metrics.DocumentsCreated)
	fmt.Printf("  Versions:         %d\n", metrics.VersionsCreated)
	fmt.Println()

	// Storage Efficiency
	if metrics.PhysicalBytes > 0 {
		fmt.Println("Storage Efficiency:")
		fmt.Printf("  Logical Bytes:      %d (%.2f MB)\n", metrics.LogicalBytes, float64(metrics.LogicalBytes)/1024/1024)
		fmt.Printf("  Physical Bytes:     %d (%.2f MB)\n", metrics.PhysicalBytes, float64(metrics.PhysicalBytes)/1024/1024)
		fmt.Printf("  Deduplication:      %.2fx\n", metrics.DeduplicationRatio)
		fmt.Printf("  Compression:        %.2fx\n", metrics.CompressionRatio)
		fmt.Println()
	}

	// Security
	if metrics.AdversaryAttempts > 0 {
		fmt.Println("Security (Adversary Simulation):")
		fmt.Printf("  Unauthorized Attempts: %d\n", metrics.AdversaryAttempts)
		fmt.Printf("  Denials:               %d\n", metrics.AdversaryDenials)
		fmt.Printf("  Denial Rate:           %.1f%%\n", metrics.AdversaryDenialRate*100)
		fmt.Println()
	}

	// Workflows
	if metrics.WorkflowTestsRun > 0 {
		fmt.Println("Workflow Tests:")
		fmt.Printf("  Tests Run:    %d\n", metrics.WorkflowTestsRun)
		fmt.Printf("  Tests Passed: %d\n", metrics.WorkflowTestsPassed)
		fmt.Printf("  Tests Failed: %d\n", metrics.WorkflowTestsFailed)
		fmt.Println()
	}

	// Mesh
	if metrics.PeerJoins > 0 || metrics.PeerLeaves > 0 {
		fmt.Println("Mesh Dynamics:")
		fmt.Printf("  Peer Joins:  %d\n", metrics.PeerJoins)
		fmt.Printf("  Peer Leaves: %d\n", metrics.PeerLeaves)
		if metrics.MeshDowntime > 0 {
			fmt.Printf("  Downtime:    %v\n", metrics.MeshDowntime)
		}
		fmt.Println()
	}

	// Errors
	if len(metrics.Errors) > 0 {
		fmt.Println("Errors:")
		for _, err := range metrics.Errors {
			fmt.Printf("  - %s\n", err)
		}
		fmt.Println()
	}

	fmt.Println("═══════════════════════════════════════════════════════════")
}

// JSONOutput represents the JSON output format for results.
type JSONOutput struct {
	// Metadata
	Scenario        string  `json:"scenario"`
	Description     string  `json:"description"`
	TimeScale       float64 `json:"time_scale"`
	Iteration       int     `json:"iteration,omitempty"`
	TotalIterations int     `json:"total_iterations,omitempty"`
	MeshMode        bool    `json:"mesh_mode,omitempty"`
	CoordinatorURL  string  `json:"coordinator_url,omitempty"`

	// Timing
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	Duration      string    `json:"duration"`
	StoryDuration string    `json:"story_duration"`

	// Operations
	Operations struct {
		TasksGenerated int `json:"tasks_generated"`
		TasksCompleted int `json:"tasks_completed"`
		TasksFailed    int `json:"tasks_failed"`
		Uploads        int `json:"uploads"`
		Downloads      int `json:"downloads"`
		Updates        int `json:"updates"`
		Deletes        int `json:"deletes"`
	} `json:"operations"`

	// Data
	Data struct {
		BytesUploaded    int64 `json:"bytes_uploaded"`
		BytesDownloaded  int64 `json:"bytes_downloaded"`
		DocumentsCreated int   `json:"documents_created"`
		VersionsCreated  int   `json:"versions_created"`
	} `json:"data"`

	// Performance
	Performance struct {
		AvgUploadLatency   string `json:"avg_upload_latency"`
		AvgDownloadLatency string `json:"avg_download_latency"`
		P95UploadLatency   string `json:"p95_upload_latency"`
		P99UploadLatency   string `json:"p99_upload_latency"`
	} `json:"performance"`

	// Storage Efficiency
	StorageEfficiency *struct {
		LogicalBytes       int64   `json:"logical_bytes"`
		PhysicalBytes      int64   `json:"physical_bytes"`
		DeduplicationRatio float64 `json:"deduplication_ratio"`
		CompressionRatio   float64 `json:"compression_ratio"`
	} `json:"storage_efficiency,omitempty"`

	// Security
	Security *struct {
		AdversaryAttempts   int     `json:"adversary_attempts"`
		AdversaryDenials    int     `json:"adversary_denials"`
		AdversaryDenialRate float64 `json:"adversary_denial_rate"`
	} `json:"security,omitempty"`

	// Workflows
	Workflows *struct {
		TestsRun    int `json:"tests_run"`
		TestsPassed int `json:"tests_passed"`
		TestsFailed int `json:"tests_failed"`
	} `json:"workflows,omitempty"`

	// Mesh
	Mesh *struct {
		PeerJoins    int    `json:"peer_joins"`
		PeerLeaves   int    `json:"peer_leaves"`
		MeshDowntime string `json:"mesh_downtime,omitempty"`
	} `json:"mesh,omitempty"`

	// Errors
	Errors []string `json:"errors,omitempty"`
}

// writeJSONOutput writes metrics to a JSON file.
func writeJSONOutput(filename string, metrics *simulator.SimulatorMetrics, st story.Story, meshMode bool, coordinatorURL string, iteration, totalIterations int) error {
	output := JSONOutput{
		Scenario:        st.Name(),
		Description:     st.Description(),
		TimeScale:       timeScale,
		Iteration:       iteration,
		TotalIterations: totalIterations,
		MeshMode:        meshMode,
		CoordinatorURL:  coordinatorURL,
		StartTime:       metrics.StartTime,
		EndTime:         metrics.EndTime,
		Duration:        metrics.Duration.String(),
		StoryDuration:   metrics.StoryDuration.String(),
	}

	// Operations
	output.Operations.TasksGenerated = metrics.TasksGenerated
	output.Operations.TasksCompleted = metrics.TasksCompleted
	output.Operations.TasksFailed = metrics.TasksFailed
	output.Operations.Uploads = metrics.UploadCount
	output.Operations.Downloads = metrics.DownloadCount
	output.Operations.Updates = metrics.UpdateCount
	output.Operations.Deletes = metrics.DeleteCount

	// Data
	output.Data.BytesUploaded = metrics.BytesUploaded
	output.Data.BytesDownloaded = metrics.BytesDownloaded
	output.Data.DocumentsCreated = metrics.DocumentsCreated
	output.Data.VersionsCreated = metrics.VersionsCreated

	// Performance
	output.Performance.AvgUploadLatency = metrics.AvgUploadLatency.String()
	output.Performance.AvgDownloadLatency = metrics.AvgDownloadLatency.String()
	output.Performance.P95UploadLatency = metrics.P95UploadLatency.String()
	output.Performance.P99UploadLatency = metrics.P99UploadLatency.String()

	// Storage Efficiency (optional)
	if metrics.PhysicalBytes > 0 {
		output.StorageEfficiency = &struct {
			LogicalBytes       int64   `json:"logical_bytes"`
			PhysicalBytes      int64   `json:"physical_bytes"`
			DeduplicationRatio float64 `json:"deduplication_ratio"`
			CompressionRatio   float64 `json:"compression_ratio"`
		}{
			LogicalBytes:       metrics.LogicalBytes,
			PhysicalBytes:      metrics.PhysicalBytes,
			DeduplicationRatio: metrics.DeduplicationRatio,
			CompressionRatio:   metrics.CompressionRatio,
		}
	}

	// Security (optional)
	if metrics.AdversaryAttempts > 0 {
		output.Security = &struct {
			AdversaryAttempts   int     `json:"adversary_attempts"`
			AdversaryDenials    int     `json:"adversary_denials"`
			AdversaryDenialRate float64 `json:"adversary_denial_rate"`
		}{
			AdversaryAttempts:   metrics.AdversaryAttempts,
			AdversaryDenials:    metrics.AdversaryDenials,
			AdversaryDenialRate: metrics.AdversaryDenialRate,
		}
	}

	// Workflows (optional)
	if metrics.WorkflowTestsRun > 0 {
		output.Workflows = &struct {
			TestsRun    int `json:"tests_run"`
			TestsPassed int `json:"tests_passed"`
			TestsFailed int `json:"tests_failed"`
		}{
			TestsRun:    metrics.WorkflowTestsRun,
			TestsPassed: metrics.WorkflowTestsPassed,
			TestsFailed: metrics.WorkflowTestsFailed,
		}
	}

	// Mesh (optional)
	if metrics.PeerJoins > 0 || metrics.PeerLeaves > 0 {
		output.Mesh = &struct {
			PeerJoins    int    `json:"peer_joins"`
			PeerLeaves   int    `json:"peer_leaves"`
			MeshDowntime string `json:"mesh_downtime,omitempty"`
		}{
			PeerJoins:  metrics.PeerJoins,
			PeerLeaves: metrics.PeerLeaves,
		}
		if metrics.MeshDowntime > 0 {
			output.Mesh.MeshDowntime = metrics.MeshDowntime.String()
		}
	}

	// Errors
	if len(metrics.Errors) > 0 {
		output.Errors = metrics.Errors
	}

	// Marshal to JSON with indentation
	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal JSON: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	return nil
}
