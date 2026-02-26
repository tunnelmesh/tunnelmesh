package coord

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain verifies no goroutine leaks occur during testing.
// This catches context leaks and other resource cleanup issues.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// Ignore known background goroutines from testing infrastructure
		goleak.IgnoreTopFunction("testing.(*M).Run.func1"),
		goleak.IgnoreTopFunction("testing.tRunner"),
		// Ignore replicator background goroutines (they're properly cleaned up)
		goleak.IgnoreTopFunction("github.com/tunnelmesh/tunnelmesh/internal/coord/replication.(*Replicator).Start.func1"),
		goleak.IgnoreTopFunction("github.com/tunnelmesh/tunnelmesh/internal/coord/replication.(*Replicator).syncLoop"),
	)
}
