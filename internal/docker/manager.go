package docker

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
)

// packetFilter is an interface for packet filtering to enable testing.
type packetFilter interface {
	AddTemporaryRule(rule routing.FilterRule)
}

// dockerClient is an interface for Docker operations to enable testing.
type dockerClient interface {
	ListContainers(ctx context.Context) ([]ContainerInfo, error)
	InspectContainer(ctx context.Context, id string) (*ContainerInfo, error)
	GetContainerStats(ctx context.Context, id string) (*ContainerStats, error)
	ListNetworks(ctx context.Context) ([]NetworkInfo, error)
	WatchEvents(ctx context.Context, handler func(ContainerEvent)) error
	StartContainer(ctx context.Context, id string) error
	StopContainer(ctx context.Context, id string) error
	RestartContainer(ctx context.Context, id string) error
	Close() error
}

// Manager manages Docker container integration for a TunnelMesh peer.
type Manager struct {
	cfg         *config.DockerConfig
	peerName    string
	client      dockerClient
	filter      packetFilter
	systemStore *s3.SystemStore
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewManager creates a new Docker manager.
// If filter is nil, port forwarding will be disabled until SetFilter is called.
// If systemStore is nil, port forward mappings will not be persisted.
// Pass a Prometheus registry to register Docker metrics, or nil for default registry.
func NewManager(cfg *config.DockerConfig, peerName string, filter packetFilter, systemStore *s3.SystemStore, registry prometheus.Registerer) *Manager {
	// Initialize Docker metrics (singleton, safe to call multiple times)
	initMetrics(registry)

	return &Manager{
		cfg:         cfg,
		peerName:    peerName,
		filter:      filter,
		systemStore: systemStore,
	}
}

// SetFilter sets or updates the packet filter for port forwarding.
// This allows the filter to be set after manager creation (e.g., for coordinators joining the mesh).
func (m *Manager) SetFilter(filter packetFilter) {
	m.filter = filter
	log.Info().Str("peer", m.peerName).Msg("Docker manager packet filter configured")
}

// Start initializes the Docker manager and connects to the Docker daemon.
// Returns error if connection fails.
func (m *Manager) Start(ctx context.Context) error {
	// Check if Docker socket exists
	if !m.isDockerAvailable() {
		log.Debug().
			Str("socket", m.cfg.Socket).
			Str("peer", m.peerName).
			Msg("Docker socket not found, Docker integration disabled")
		return nil
	}

	log.Info().
		Str("socket", m.cfg.Socket).
		Str("peer", m.peerName).
		Msg("Starting Docker manager")

	// Create Docker client
	client, err := newRealDockerClient(m.cfg.Socket)
	if err != nil {
		log.Error().Err(err).
			Str("socket", m.cfg.Socket).
			Msg("Failed to connect to Docker daemon")
		return err
	}
	m.client = client

	// Create cancellable context for background goroutines
	m.ctx, m.cancel = context.WithCancel(ctx)

	// Sync port forwards for existing containers
	if err := m.syncAllPortForwards(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to sync initial port forwards")
	}

	// Start event watcher in background
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.watchEvents(m.ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("Docker event watcher stopped with error")
		}
	}()

	// Start periodic stats collection if S3 store is available
	if m.systemStore != nil {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.StartPeriodicStatsCollection(m.ctx, m.systemStore)
		}()
	}

	return nil
}

// syncAllPortForwards syncs port forwards for all running containers.
func (m *Manager) syncAllPortForwards(ctx context.Context) error {
	containers, err := m.ListContainers(ctx)
	if err != nil {
		return err
	}

	for _, container := range containers {
		if container.State == "running" {
			if err := m.syncPortForwards(ctx, container.ID); err != nil {
				log.Warn().Err(err).
					Str("container", container.Name).
					Msg("Failed to sync port forwards for container")
			}
		}
	}

	log.Info().Int("count", len(containers)).Msg("Synced port forwards for existing containers")
	return nil
}

// isDockerAvailable checks if Docker socket exists.
func (m *Manager) isDockerAvailable() bool {
	// For Unix sockets, check file existence
	if strings.HasPrefix(m.cfg.Socket, "unix://") {
		socketPath := strings.TrimPrefix(m.cfg.Socket, "unix://")
		if _, err := os.Stat(socketPath); err == nil {
			return true
		}
	}
	// For TCP sockets, assume available (will fail on connect if not)
	if strings.HasPrefix(m.cfg.Socket, "tcp://") {
		return true
	}
	return false
}

// Stop gracefully stops the Docker manager.
func (m *Manager) Stop() error {
	// Cancel background goroutines
	if m.cancel != nil {
		m.cancel()
	}

	// Wait for all goroutines to finish
	m.wg.Wait()

	// Close Docker client
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}

// ListContainers returns all containers on this peer.
func (m *Manager) ListContainers(ctx context.Context) ([]ContainerInfo, error) {
	if m.client == nil {
		return []ContainerInfo{}, nil
	}

	containers, err := m.client.ListContainers(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list Docker containers")
		return nil, err
	}

	// Inspect running containers in parallel to get StartedAt time and fetch stats
	// This prevents sequential blocking when there are many containers
	var wg sync.WaitGroup
	var mu sync.Mutex // Protect concurrent writes to containers slice

	for i := range containers {
		// Set short ID immediately
		if containers[i].ShortID == "" {
			containers[i].ShortID = shortID(containers[i].ID)
		}

		// Record container info to Prometheus (for all containers, not just running)
		m.recordContainerInfo(&containers[i])

		if containers[i].State == "running" {
			wg.Add(1)
			// Capture values needed in goroutine to avoid race conditions
			containerID := containers[i].ID
			containerName := containers[i].Name
			go func(index int, id, name string) {
				defer wg.Done()

				// Inspect container to get full details including StartedAt with per-container timeout
				inspectCtx, cancel := context.WithTimeout(ctx, containerInspectTimeout)
				inspected, err := m.client.InspectContainer(inspectCtx, id)
				cancel()
				if err != nil {
					log.Debug().Err(err).Str("container", name).Msg("Failed to inspect container")
					return
				}
				if inspected == nil || inspected.StartedAt.IsZero() {
					return
				}

				// Fetch resource usage stats with per-container timeout
				statsCtx, cancel := context.WithTimeout(ctx, containerStatsTimeout)
				stats, err := m.client.GetContainerStats(statsCtx, id)
				cancel()
				if err != nil {
					log.Debug().Err(err).Str("container", name).Msg("Failed to get container stats")
					return
				}

				// Update with inspected data and stats (protected by mutex)
				mu.Lock()
				containers[index].StartedAt = inspected.StartedAt
				containers[index].UptimeSeconds = calculateUptime(inspected.StartedAt)
				containers[index].DiskBytes = inspected.DiskBytes
				if stats != nil {
					containers[index].CPUPercent = stats.CPUPercent
					containers[index].MemoryBytes = stats.MemoryBytes
					containers[index].MemoryPercent = stats.MemoryPercent
					// DiskBytes already set from inspected data above (more accurate than stats)

					// Record stats to Prometheus
					m.recordStats(*stats, &containers[index])
				}
				mu.Unlock()
			}(i, containerID, containerName)
		}
	}

	// Wait for all goroutines to complete (with timeout to prevent hanging)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed successfully
	case <-ctx.Done():
		// Context cancelled - return what we have so far
		log.Warn().Msg("Context cancelled while fetching container stats")
	}

	return containers, nil
}

// InspectContainer returns detailed information about a specific container.
// Returns nil if container not found.
func (m *Manager) InspectContainer(ctx context.Context, id string) (*ContainerInfo, error) {
	if m.client == nil {
		return nil, nil
	}

	container, err := m.client.InspectContainer(ctx, id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Failed to inspect Docker container")
		return nil, err
	}

	if container != nil {
		// Calculate uptime
		if container.State == "running" && !container.StartedAt.IsZero() {
			container.UptimeSeconds = calculateUptime(container.StartedAt)
		}
		// Set short ID
		if container.ShortID == "" {
			container.ShortID = shortID(container.ID)
		}
	}

	return container, nil
}

// StartContainer starts a stopped container.
func (m *Manager) StartContainer(ctx context.Context, id string) error {
	if m.client == nil {
		return errors.New("docker client not available")
	}

	log.Info().Str("container", id).Msg("Starting Docker container")
	return m.client.StartContainer(ctx, id)
}

// StopContainer stops a running container.
func (m *Manager) StopContainer(ctx context.Context, id string) error {
	if m.client == nil {
		return errors.New("docker client not available")
	}

	log.Info().Str("container", id).Msg("Stopping Docker container")
	return m.client.StopContainer(ctx, id)
}

// RestartContainer restarts a container.
func (m *Manager) RestartContainer(ctx context.Context, id string) error {
	if m.client == nil {
		return errors.New("docker client not available")
	}

	log.Info().Str("container", id).Msg("Restarting Docker container")
	return m.client.RestartContainer(ctx, id)
}

// calculateUptime returns the number of seconds since the container started.
func calculateUptime(startedAt time.Time) int64 {
	if startedAt.IsZero() {
		return 0
	}
	return int64(time.Since(startedAt).Seconds())
}

// shortID returns the first 12 characters of a Docker ID.
func shortID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

// getComposeStack extracts the Docker Compose project name from labels.
func getComposeStack(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	return labels["com.docker.compose.project"]
}

// getComposeService extracts the Docker Compose service name from labels.
func getComposeService(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	return labels["com.docker.compose.service"]
}

// parseProtocol converts protocol string to number for filter rules.
func parseProtocol(proto string) uint8 {
	proto = strings.ToLower(proto)
	switch proto {
	case "tcp":
		return routing.ProtoTCP
	case "udp":
		return routing.ProtoUDP
	default:
		return routing.ProtoTCP // Default to TCP
	}
}
