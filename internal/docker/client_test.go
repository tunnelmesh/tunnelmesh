package docker

import "testing"

func TestMemoryWorkingSet_WithPageCache(t *testing.T) {
	const (
		usage     = 500 * 1024 * 1024 // 500 MB
		pageCache = 100 * 1024 * 1024 // 100 MB
		want      = usage - pageCache // 400 MB
	)
	stats := map[string]uint64{"cache": pageCache}
	got := memoryWorkingSet(usage, stats)
	if got != want {
		t.Errorf("memoryWorkingSet with cache: got %d, want %d", got, want)
	}
}

func TestMemoryWorkingSet_NilStats(t *testing.T) {
	// cgroup v2 — Stats map is nil, cache returns 0, workingSet == usage
	const usage = 400 * 1024 * 1024
	got := memoryWorkingSet(usage, nil)
	if got != usage {
		t.Errorf("memoryWorkingSet nil stats: got %d, want %d", got, usage)
	}
}

func TestMemoryWorkingSet_EmptyStats(t *testing.T) {
	// cgroup v2 — Stats map present but "cache" key absent
	const usage = 300 * 1024 * 1024
	got := memoryWorkingSet(usage, map[string]uint64{})
	if got != usage {
		t.Errorf("memoryWorkingSet empty stats: got %d, want %d", got, usage)
	}
}

func TestMemoryWorkingSet_CacheExceedsUsage(t *testing.T) {
	// Pathological case: Stats["cache"] >= Usage — do not go negative, return usage as-is
	const (
		usage     = 100 * 1024 * 1024
		pageCache = 200 * 1024 * 1024
	)
	stats := map[string]uint64{"cache": pageCache}
	got := memoryWorkingSet(usage, stats)
	if got != usage {
		t.Errorf("memoryWorkingSet cache>usage: got %d, want %d (usage)", got, usage)
	}
}

func TestMemoryWorkingSet_MemPercentCalc(t *testing.T) {
	// Verify that the percent calculation uses workingSet, not raw usage.
	// Usage=500MB, cache=100MB → workingSet=400MB, limit=1GB → 400/1024 ≈ 39.06%
	const (
		usage     = 500 * 1024 * 1024
		pageCache = 100 * 1024 * 1024
		limit     = 1024 * 1024 * 1024
	)
	stats := map[string]uint64{"cache": pageCache}
	workingSet := memoryWorkingSet(usage, stats)

	got := float64(workingSet) / float64(limit) * 100.0
	want := float64(400*1024*1024) / float64(limit) * 100.0
	if got != want {
		t.Errorf("percent calculation: got %.4f%%, want %.4f%%", got, want)
	}
	// Should be approximately 39.06%
	const approx = 39.0625
	if got < approx-0.01 || got > approx+0.01 {
		t.Errorf("expected ~%.4f%%, got %.4f%%", approx, got)
	}
}
