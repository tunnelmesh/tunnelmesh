package s3

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func gaugeValue(g prometheus.Gauge) float64 {
	m := &dto.Metric{}
	_ = g.Write(m)
	return m.GetGauge().GetValue()
}

// newCASMetricsForTest creates a fresh S3Metrics instance backed by an isolated registry.
// Each call resets the once guard so tests get independent metric objects.
func newCASMetricsForTest(t *testing.T) *S3Metrics {
	t.Helper()
	s3MetricsOnce = sync.Once{} // reset singleton for test isolation
	reg := prometheus.NewRegistry()
	m := InitS3Metrics(reg)
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}
	t.Cleanup(func() { s3MetricsOnce = sync.Once{} })
	return m
}

func TestUpdateCASMetrics_DedupRatioBaselineIsOne(t *testing.T) {
	m := newCASMetricsForTest(t)

	// With no dedup savings: logical == dataChunkBytes → ratio should be exactly 1.0
	const dataChunkBytes = 1000
	m.UpdateCASMetrics(10, 1500, dataChunkBytes, dataChunkBytes, 0, 0, 0)

	if v := gaugeValue(m.DedupRatio); v != 1.0 {
		t.Errorf("DedupRatio = %v, want 1.0 (no dedup savings, EC-normalized baseline)", v)
	}
}

func TestUpdateCASMetrics_DedupRatioWithSavings(t *testing.T) {
	m := newCASMetricsForTest(t)

	// logical = 3000 (3 identical objects, 1000 bytes each), dataChunkBytes = 1000 (stored once)
	// → ratio should be 3.0
	m.UpdateCASMetrics(10, 1500, 1000, 3000, 0, 0, 0)

	if v := gaugeValue(m.DedupRatio); v != 3.0 {
		t.Errorf("DedupRatio = %v, want 3.0 (3x dedup savings)", v)
	}
}

func TestUpdateCASMetrics_DedupRatioBelowOneForPaddingWaste(t *testing.T) {
	m := newCASMetricsForTest(t)

	// Small objects in large EC blocks: logical (500) < dataChunkBytes (1000)
	// → ratio = 0.5, surfacing EC padding inefficiency (not clamped to 1.0)
	m.UpdateCASMetrics(10, 1500, 1000, 500, 0, 0, 0)

	if v := gaugeValue(m.DedupRatio); v != 0.5 {
		t.Errorf("DedupRatio = %v, want 0.5 (EC padding waste, sub-1.0 allowed)", v)
	}
}

func TestUpdateCASMetrics_DataChunkStorageBytesSet(t *testing.T) {
	m := newCASMetricsForTest(t)

	m.UpdateCASMetrics(10, 1500, 800, 1000, 0, 0, 0)

	if v := gaugeValue(m.DataChunkStorageBytes); v != 800 {
		t.Errorf("DataChunkStorageBytes = %v, want 800", v)
	}
}

func TestUpdateCASMetrics_DedupRatioZeroDataBytes(t *testing.T) {
	m := newCASMetricsForTest(t)

	// Zero dataChunkBytes (empty store) → safe default of 1.0
	m.UpdateCASMetrics(0, 0, 0, 0, 0, 0, 0)

	if v := gaugeValue(m.DedupRatio); v != 1.0 {
		t.Errorf("DedupRatio = %v, want 1.0 (empty store)", v)
	}
}

func TestUpdateVolumeMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := InitS3Metrics(reg)
	if metrics == nil {
		t.Fatal("expected non-nil metrics")
	}

	metrics.UpdateVolumeMetrics(1000, 600, 400)

	if v := gaugeValue(metrics.VolumeTotalBytes); v != 1000 {
		t.Errorf("VolumeTotalBytes = %v, want 1000", v)
	}
	if v := gaugeValue(metrics.VolumeUsedBytes); v != 600 {
		t.Errorf("VolumeUsedBytes = %v, want 600", v)
	}
	if v := gaugeValue(metrics.VolumeAvailableBytes); v != 400 {
		t.Errorf("VolumeAvailableBytes = %v, want 400", v)
	}

	// Update with new values
	metrics.UpdateVolumeMetrics(2000, 1500, 500)

	if v := gaugeValue(metrics.VolumeTotalBytes); v != 2000 {
		t.Errorf("VolumeTotalBytes = %v, want 2000", v)
	}
	if v := gaugeValue(metrics.VolumeUsedBytes); v != 1500 {
		t.Errorf("VolumeUsedBytes = %v, want 1500", v)
	}
	if v := gaugeValue(metrics.VolumeAvailableBytes); v != 500 {
		t.Errorf("VolumeAvailableBytes = %v, want 500", v)
	}
}
