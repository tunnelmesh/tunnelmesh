package docker

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	maxConcurrentHandlers   = 10                     // Maximum concurrent event handlers
	defaultDebounceWindow   = 100 * time.Millisecond // Default debounce window for Docker events
	cleanupTickerInterval   = 5 * time.Minute        // Interval for debounce map cleanup
	cleanupMaxAge           = 10 * time.Minute       // Maximum age for debounce map entries
	containerInspectTimeout = 2 * time.Second        // Timeout for container inspect operations
	containerStatsTimeout   = 2 * time.Second        // Timeout for container stats operations
	containerStopTimeout    = 10                     // Container stop/restart timeout in seconds
	statsCollectionInterval = 30 * time.Second       // Interval for Docker stats collection
)

// eventWatcher watches Docker events and triggers callbacks.
type eventWatcher struct {
	handler        func(ContainerEvent)
	debounceWindow time.Duration
	lastEvent      map[string]time.Time // containerID -> last event time
	mu             sync.Mutex
	wg             *sync.WaitGroup // For tracking handler goroutines
	ctx            context.Context // For cancelling handlers
	semaphore      chan struct{}   // Limit concurrent handlers
}

// newEventWatcher creates a new event watcher with the given handler.
func newEventWatcher(handler func(ContainerEvent), wg *sync.WaitGroup, ctx context.Context) *eventWatcher {
	return &eventWatcher{
		handler:        handler,
		debounceWindow: defaultDebounceWindow,
		lastEvent:      make(map[string]time.Time),
		wg:             wg,
		ctx:            ctx,
		semaphore:      make(chan struct{}, maxConcurrentHandlers),
	}
}

// handleEvent processes a Docker event.
// Events are debounced to avoid rapid repeated calls for the same container.
func (w *eventWatcher) handleEvent(event ContainerEvent) {
	if !shouldHandleEvent(event) {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Check debounce
	key := event.ContainerID + ":" + event.Type
	lastTime, exists := w.lastEvent[key]
	if exists && time.Since(lastTime) < w.debounceWindow {
		log.Debug().
			Str("container", event.ContainerID).
			Str("type", event.Type).
			Msg("Debouncing Docker event")
		return
	}

	w.lastEvent[key] = time.Now()

	// Call handler asynchronously with concurrency limit
	w.wg.Add(1)
	go func(evt ContainerEvent) {
		defer w.wg.Done()

		// Acquire semaphore slot (blocks if at limit)
		select {
		case w.semaphore <- struct{}{}:
			defer func() { <-w.semaphore }()
		case <-w.ctx.Done():
			return
		}

		// Check context before calling handler
		select {
		case <-w.ctx.Done():
			return
		default:
			w.handler(evt)
		}
	}(event)
}

// cleanup removes old entries from the debounce map to prevent unbounded growth.
// Should be called periodically in a background goroutine.
func (w *eventWatcher) cleanup(maxAge time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for key, lastTime := range w.lastEvent {
		if lastTime.Before(cutoff) {
			delete(w.lastEvent, key)
		}
	}
}

// shouldHandleEvent returns true if we care about this event type.
func shouldHandleEvent(event ContainerEvent) bool {
	switch event.Type {
	case "start", "stop", "die", "destroy":
		return true
	default:
		return false
	}
}

// watchEvents watches Docker events and syncs port forwards on container lifecycle changes.
// Blocks until context is cancelled.
func (m *Manager) watchEvents(ctx context.Context) error {
	if m.client == nil {
		return nil
	}

	log.Info().Str("peer", m.peerName).Msg("Starting Docker event watcher")

	watcher := newEventWatcher(func(event ContainerEvent) {
		log.Debug().
			Str("container", event.ContainerID).
			Str("type", event.Type).
			Str("peer", m.peerName).
			Msg("Docker container event")

		// Handle port forwarding based on event type
		switch event.Type {
		case "start":
			// Container started, sync port forwards
			// Use m.ctx instead of parameter ctx to handle post-shutdown events properly
			if err := m.syncPortForwards(m.ctx, event.ContainerID); err != nil {
				log.Error().Err(err).
					Str("container", event.ContainerID).
					Msg("Failed to sync port forwards on container start")
			}
		case "stop", "die", "destroy":
			// Container stopped/destroyed, port forwards will expire naturally via TTL
			log.Debug().
				Str("container", event.ContainerID).
				Str("type", event.Type).
				Msg("Container stopped, port forwards will expire via TTL")
			if event.Type == "destroy" {
				m.cleanupContainerMetrics(event.ContainerID)
			}
		}
	}, &m.wg, m.ctx)

	// Start periodic cleanup of debounce map to prevent memory growth
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(cleanupTickerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Clean up old debounce entries
				watcher.cleanup(cleanupMaxAge)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Stream events from Docker daemon
	return m.client.WatchEvents(ctx, watcher.handleEvent)
}
