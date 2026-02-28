package docker

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tunnelmesh/tunnelmesh/internal/config"
)

// metricsTestMutex serializes tests that access global metrics registry
// to prevent data races when tests run in parallel.
var metricsTestMutex sync.Mutex

func TestInitMetrics(t *testing.T) {
	metricsTestMutex.Lock()
	defer metricsTestMutex.Unlock()

	metrics := initMetrics(nil)
	if metrics == nil {
		t.Fatal("initMetrics returned nil")
	}

	if metrics.cpuPercent == nil {
		t.Error("cpuPercent metric not initialized")
	}
	if metrics.memoryBytes == nil {
		t.Error("memoryBytes metric not initialized")
	}
	if metrics.memoryLimit == nil {
		t.Error("memoryLimit metric not initialized")
	}
	if metrics.memoryPercent == nil {
		t.Error("memoryPercent metric not initialized")
	}
	if metrics.diskBytes == nil {
		t.Error("diskBytes metric not initialized")
	}
	if metrics.pids == nil {
		t.Error("pids metric not initialized")
	}
	if metrics.containerInfo == nil {
		t.Error("containerInfo metric not initialized")
	}
	if metrics.containerStatus == nil {
		t.Error("containerStatus metric not initialized")
	}
}

func TestRecordStats(t *testing.T) {
	metricsTestMutex.Lock()
	defer metricsTestMutex.Unlock()

	initMetrics(nil)

	cfg := &config.DockerConfig{Socket: "unix:///var/run/docker.sock"}
	mgr := NewManager(cfg, "test-peer", nil, nil, nil)

	stats := ContainerStats{
		ContainerID:   "abc123",
		ContainerName: "test-nginx",
		Timestamp:     time.Now(),
		CPUPercent:    25.5,
		MemoryBytes:   268435456,
		MemoryLimit:   536870912,
		MemoryPercent: 50.0,
		DiskBytes:     1073741824,
		PIDs:          24,
	}

	container := &ContainerInfo{
		ID:    "abc123",
		Name:  "test-nginx",
		Image: "nginx:latest",
	}

	// Should not panic
	mgr.recordStats(stats, container)
}

func TestRecordContainerInfo(t *testing.T) {
	metricsTestMutex.Lock()
	defer metricsTestMutex.Unlock()

	initMetrics(nil)

	cfg := &config.DockerConfig{Socket: "unix:///var/run/docker.sock"}
	mgr := NewManager(cfg, "test-peer", nil, nil, nil)

	container := &ContainerInfo{
		ID:          "abc123",
		Name:        "test-nginx",
		Image:       "nginx:latest",
		Status:      "running",
		State:       "running",
		NetworkMode: "bridge",
	}

	// Should not panic
	mgr.recordContainerInfo(container)
}

// TestRecordContainerInfoUsesState verifies that recordContainerInfo uses the stable
// container.State field ("running", "exited", etc.) for the Prometheus status label
// rather than the dynamic container.Status string ("Up 37 minutes (healthy)") which
// would create unbounded time series cardinality.
func TestRecordContainerInfoUsesState(t *testing.T) {
	metricsTestMutex.Lock()
	defer metricsTestMutex.Unlock()

	initMetrics(nil) // idempotent singleton

	cfg := &config.DockerConfig{Socket: "unix:///var/run/docker.sock"}
	mgr := NewManager(cfg, "test-peer", nil, nil, nil)

	const testContainerID = "state-test-container-999"

	container := &ContainerInfo{
		ID:          testContainerID,
		Name:        "test-redis",
		Image:       "redis:7",
		Status:      "Up 37 minutes (healthy)", // dynamic uptime string — must NOT be used
		State:       "running",                 // stable machine-readable state — must be used
		NetworkMode: "bridge",
	}

	mgr.recordContainerInfo(container)

	// Verify via the global gatherer that docker_container_info for our test container
	// carries status="running" (State), not the human-readable uptime string (Status).
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	var found bool
	for _, mf := range mfs {
		if mf.GetName() != "docker_container_info" {
			continue
		}
		for _, m := range mf.GetMetric() {
			// Only inspect the series we just created.
			var isOurContainer bool
			var statusValue string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "container_id" && lp.GetValue() == testContainerID {
					isOurContainer = true
				}
				if lp.GetName() == "status" {
					statusValue = lp.GetValue()
				}
			}
			if !isOurContainer {
				continue
			}
			found = true
			if statusValue != "running" {
				t.Errorf("docker_container_info status label = %q, want %q; "+
					"recordContainerInfo must use container.State, not container.Status",
					statusValue, "running")
			}
		}
	}

	if !found {
		t.Error("docker_container_info metric not found for test container after recordContainerInfo")
	}
}
