package replication

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// initReplTestTracer sets up a synchronous in-memory tracer for span assertions.
func initReplTestTracer(t *testing.T) (*tracetest.InMemoryExporter, func()) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	return exp, func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	}
}

func createReplTracingTest(t *testing.T) (*Replicator, *mockS3Store) {
	t.Helper()
	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	r := NewReplicator(Config{
		NodeID:              "coord-test",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	r.AddPeer("peer1.mesh", "peer1.mesh")

	t.Cleanup(func() { _ = r.Stop() })
	return r, s3Store
}

//nolint:dupl
func TestProcessQueuePut_EmitsSpan(t *testing.T) {
	exp, cleanup := initReplTestTracer(t)
	defer cleanup()

	r, _ := createReplTracingTest(t)
	entry := &replQueueEntry{bucket: "mybucket", key: "mykey", op: "put"}
	peers := r.GetPeers()

	r.processQueuePut(context.Background(), entry, peers)

	spans := exp.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == "replication.put" {
			found = true
			// Verify bucket and key attributes
			var hasBucket, hasKey bool
			for _, attr := range s.Attributes {
				if attr.Key == "s3.bucket" && attr.Value.AsString() == "mybucket" {
					hasBucket = true
				}
				if attr.Key == "s3.key" && attr.Value.AsString() == "mykey" {
					hasKey = true
				}
			}
			if !hasBucket {
				t.Error("replication.put span missing s3.bucket attribute")
			}
			if !hasKey {
				t.Error("replication.put span missing s3.key attribute")
			}
			break
		}
	}
	if !found {
		t.Error("replication.put span not emitted")
	}
}

//nolint:dupl
func TestProcessQueueDelete_EmitsSpan(t *testing.T) {
	exp, cleanup := initReplTestTracer(t)
	defer cleanup()

	r, _ := createReplTracingTest(t)
	entry := &replQueueEntry{bucket: "delbucket", key: "delkey", op: "delete"}
	peers := r.GetPeers()

	r.processQueueDelete(context.Background(), entry, peers)

	spans := exp.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == "replication.delete" {
			found = true
			var hasBucket, hasKey bool
			for _, attr := range s.Attributes {
				if attr.Key == "s3.bucket" && attr.Value.AsString() == "delbucket" {
					hasBucket = true
				}
				if attr.Key == "s3.key" && attr.Value.AsString() == "delkey" {
					hasKey = true
				}
			}
			if !hasBucket {
				t.Error("replication.delete span missing s3.bucket attribute")
			}
			if !hasKey {
				t.Error("replication.delete span missing s3.key attribute")
			}
			break
		}
	}
	if !found {
		t.Error("replication.delete span not emitted")
	}
}

func TestProcessQueueDelete_RecordsErrorOnPeerFailure(t *testing.T) {
	exp, cleanup := initReplTestTracer(t)
	defer cleanup()

	transport := newMockTransport()
	s3Store := newMockS3Store()
	registry := newMockChunkRegistry()

	r := NewReplicator(Config{
		NodeID:              "coord-test",
		Transport:           transport,
		S3Store:             s3Store,
		ChunkRegistry:       registry,
		Logger:              zerolog.Nop(),
		ChunkPipelineWindow: 5,
		AutoSyncInterval:    0,
	})
	r.AddPeer("peer1.mesh", "peer1.mesh")
	t.Cleanup(func() { _ = r.Stop() })

	// Make ReplicateDelete fail by making the transport error
	// In the mock, ReplicateDelete calls through to the store. Let's put metadata
	// that we can check for error — actually, ReplicateDelete sends via transport.
	// Set sendErr on transport.
	transport.mu.Lock()
	transport.sendErr = context.DeadlineExceeded
	transport.mu.Unlock()

	entry := &replQueueEntry{bucket: "errbucket", key: "errkey", op: "delete", retries: 2}
	peers := r.GetPeers()

	r.processQueueDelete(context.Background(), entry, peers)

	spans := exp.GetSpans()
	for _, s := range spans {
		if s.Name == "replication.delete" {
			if s.Status.Code != otelcodes.Error {
				t.Errorf("replication.delete status = %v, want Error", s.Status.Code)
			}
			return
		}
	}
	t.Error("replication.delete span not found")
}

func TestProcessQueuePut_SpanEndsAfterAllPeers(t *testing.T) {
	exp, cleanup := initReplTestTracer(t)
	defer cleanup()

	r, _ := createReplTracingTest(t)
	entry := &replQueueEntry{bucket: "b", key: "k", op: "put"}
	peers := r.GetPeers()

	r.processQueuePut(context.Background(), entry, peers)

	for _, s := range exp.GetSpans() {
		if s.Name == "replication.put" {
			if s.EndTime.IsZero() {
				t.Error("replication.put span has zero EndTime (not ended)")
			}
			return
		}
	}
	t.Error("replication.put span not found")
}
