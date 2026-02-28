package docker

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	once            sync.Once
	metricsRegistry *dockerMetrics
)

// dockerMetrics holds Prometheus metrics for Docker containers.
type dockerMetrics struct {
	cpuPercent      *prometheus.GaugeVec
	memoryBytes     *prometheus.GaugeVec
	memoryLimit     *prometheus.GaugeVec
	memoryPercent   *prometheus.GaugeVec
	diskBytes       *prometheus.GaugeVec
	pids            *prometheus.GaugeVec
	containerInfo   *prometheus.GaugeVec
	containerStatus *prometheus.GaugeVec

	// Stats collection metrics
	statsCollectionTotal     *prometheus.CounterVec
	statsCollectionErrors    *prometheus.CounterVec
	statsCollectionDuration  *prometheus.HistogramVec
	statsCollectionTimestamp *prometheus.GaugeVec
	statsPersistenceTotal    *prometheus.CounterVec
	statsPersistenceErrors   *prometheus.CounterVec
	statsPersistenceDuration *prometheus.HistogramVec
	statsContainersCollected *prometheus.GaugeVec
	statsCollectionEnabled   *prometheus.GaugeVec
	statsS3Available         *prometheus.GaugeVec
}

// initMetrics initializes Docker Prometheus metrics (singleton).
// Pass nil for registry to use the default Prometheus registry.
func initMetrics(registry prometheus.Registerer) *dockerMetrics {
	if registry == nil {
		registry = prometheus.DefaultRegisterer
	}

	once.Do(func() {
		metricsRegistry = &dockerMetrics{
			cpuPercent: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "docker_container_cpu_percent",
					Help: "CPU usage percentage for Docker container (0-100+ for multi-core)",
				},
				[]string{"peer", "container_id", "container_name", "image"},
			),
			memoryBytes: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "docker_container_memory_bytes",
					Help: "Current memory usage in bytes for Docker container",
				},
				[]string{"peer", "container_id", "container_name", "image"},
			),
			memoryLimit: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "docker_container_memory_limit_bytes",
					Help: "Memory limit in bytes for Docker container",
				},
				[]string{"peer", "container_id", "container_name", "image"},
			),
			memoryPercent: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "docker_container_memory_percent",
					Help: "Memory usage percentage for Docker container",
				},
				[]string{"peer", "container_id", "container_name", "image"},
			),
			diskBytes: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "docker_container_disk_bytes",
					Help: "Disk space used by Docker container in bytes",
				},
				[]string{"peer", "container_id", "container_name", "image"},
			),
			pids: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "docker_container_pids",
					Help: "Number of processes in Docker container",
				},
				[]string{"peer", "container_id", "container_name", "image"},
			),
			containerInfo: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "docker_container_info",
					Help: "Docker container information (always 1)",
				},
				[]string{"peer", "container_id", "container_name", "image", "status", "network_mode"},
			),
			containerStatus: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "docker_container_status",
					Help: "Docker container status (1=running, 0=stopped)",
				},
				[]string{"peer", "container_id", "container_name", "image"},
			),

			// Stats collection metrics
			statsCollectionTotal: promauto.With(registry).NewCounterVec(
				prometheus.CounterOpts{
					Name: "tunnelmesh_docker_stats_collection_total",
					Help: "Total successful Docker stats collections",
				},
				[]string{"peer"},
			),
			statsCollectionErrors: promauto.With(registry).NewCounterVec(
				prometheus.CounterOpts{
					Name: "tunnelmesh_docker_stats_collection_errors_total",
					Help: "Total Docker stats collection errors",
				},
				[]string{"peer", "error_type"},
			),
			statsCollectionDuration: promauto.With(registry).NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "tunnelmesh_docker_stats_collection_duration_seconds",
					Help:    "Docker stats collection latency in seconds",
					Buckets: prometheus.DefBuckets,
				},
				[]string{"peer"},
			),
			statsCollectionTimestamp: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "tunnelmesh_docker_stats_last_collection_timestamp",
					Help: "Unix timestamp of last successful Docker stats collection",
				},
				[]string{"peer"},
			),
			statsPersistenceTotal: promauto.With(registry).NewCounterVec(
				prometheus.CounterOpts{
					Name: "tunnelmesh_docker_stats_persistence_total",
					Help: "Total successful Docker stats S3 saves",
				},
				[]string{"peer"},
			),
			statsPersistenceErrors: promauto.With(registry).NewCounterVec(
				prometheus.CounterOpts{
					Name: "tunnelmesh_docker_stats_persistence_errors_total",
					Help: "Total Docker stats S3 save errors",
				},
				[]string{"peer", "error_type"},
			),
			statsPersistenceDuration: promauto.With(registry).NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "tunnelmesh_docker_stats_persistence_duration_seconds",
					Help:    "Docker stats S3 save latency in seconds",
					Buckets: prometheus.DefBuckets,
				},
				[]string{"peer"},
			),
			statsContainersCollected: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "tunnelmesh_docker_stats_containers_collected",
					Help: "Number of containers in last stats collection",
				},
				[]string{"peer"},
			),
			statsCollectionEnabled: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "tunnelmesh_docker_stats_collection_enabled",
					Help: "Whether Docker stats collection is enabled (1) or not (0)",
				},
				[]string{"peer"},
			),
			statsS3Available: promauto.With(registry).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "tunnelmesh_docker_stats_s3_available",
					Help: "Whether S3 is available for stats persistence (1) or not (0)",
				},
				[]string{"peer"},
			),
		}
	})
	return metricsRegistry
}

// recordStats records Docker container stats to Prometheus.
func (m *Manager) recordStats(stats ContainerStats, containerInfo *ContainerInfo) {
	if metricsRegistry == nil {
		return
	}

	labels := prometheus.Labels{
		"peer":           m.peerName,
		"container_id":   stats.ContainerID,
		"container_name": stats.ContainerName,
		"image":          containerInfo.Image,
	}

	metricsRegistry.cpuPercent.With(labels).Set(stats.CPUPercent)
	metricsRegistry.memoryBytes.With(labels).Set(float64(stats.MemoryBytes))
	metricsRegistry.memoryLimit.With(labels).Set(float64(stats.MemoryLimit))
	metricsRegistry.memoryPercent.With(labels).Set(stats.MemoryPercent)
	metricsRegistry.diskBytes.With(labels).Set(float64(stats.DiskBytes))
	metricsRegistry.pids.With(labels).Set(float64(stats.PIDs))
}

// cleanupContainerMetrics removes all Prometheus metric series for a container.
// Handles cardinality cleanup when container state changes or container is removed.
func (m *Manager) cleanupContainerMetrics(containerID string) {
	if metricsRegistry == nil {
		return
	}
	partial := prometheus.Labels{"peer": m.peerName, "container_id": containerID}
	metricsRegistry.containerInfo.DeletePartialMatch(partial)
	metricsRegistry.containerStatus.DeletePartialMatch(partial)
	metricsRegistry.cpuPercent.DeletePartialMatch(partial)
	metricsRegistry.memoryBytes.DeletePartialMatch(partial)
	metricsRegistry.memoryLimit.DeletePartialMatch(partial)
	metricsRegistry.memoryPercent.DeletePartialMatch(partial)
	metricsRegistry.diskBytes.DeletePartialMatch(partial)
	metricsRegistry.pids.DeletePartialMatch(partial)
}

// recordContainerInfo records Docker container info to Prometheus.
func (m *Manager) recordContainerInfo(container *ContainerInfo) {
	if metricsRegistry == nil {
		return
	}

	// Clean up stale series from previous calls (e.g. state transitions like running→exited)
	// before registering the new label combination, preventing unbounded cardinality growth.
	m.cleanupContainerMetrics(container.ID)

	infoLabels := prometheus.Labels{
		"peer":           m.peerName,
		"container_id":   container.ID,
		"container_name": container.Name,
		"image":          container.Image,
		"status":         container.State,
		"network_mode":   container.NetworkMode,
	}

	metricsRegistry.containerInfo.With(infoLabels).Set(1)

	statusLabels := prometheus.Labels{
		"peer":           m.peerName,
		"container_id":   container.ID,
		"container_name": container.Name,
		"image":          container.Image,
	}

	statusValue := 0.0
	if container.State == "running" {
		statusValue = 1.0
	}
	metricsRegistry.containerStatus.With(statusLabels).Set(statusValue)
}
