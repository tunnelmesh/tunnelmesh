package coord

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// sseClient represents a connected SSE client.
type sseClient struct {
	events chan string
	done   chan struct{}
}

// sseHub manages SSE client connections and broadcasts.
type sseHub struct {
	clients map[*sseClient]bool
	mu      sync.RWMutex
}

// newSSEHub creates a new SSE hub.
func newSSEHub() *sseHub {
	return &sseHub{
		clients: make(map[*sseClient]bool),
	}
}

// register adds a new client to the hub.
func (h *sseHub) register(client *sseClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[client] = true
	log.Debug().Int("clients", len(h.clients)).Msg("SSE client connected")
}

// unregister removes a client from the hub. Safe to call multiple times (idempotent):
// the existence check prevents double-close of client.events.
func (h *sseHub) unregister(client *sseClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.events)
		log.Debug().Int("clients", len(h.clients)).Msg("SSE client disconnected")
	}
}

// broadcast sends an event to all connected clients.
// If a client's buffer is full (slow consumer), it is disconnected.
func (h *sseHub) broadcast(event string) {
	h.mu.RLock()
	var slowClients []*sseClient
	for client := range h.clients {
		select {
		case client.events <- event:
		default:
			// Client buffer full — mark for disconnection.
			slowClients = append(slowClients, client)
		}
	}
	h.mu.RUnlock()

	// Disconnect slow clients outside the read lock to avoid lock inversion.
	for _, client := range slowClients {
		log.Warn().Msg("SSE client too slow, disconnecting")
		h.unregister(client)
	}
}

// clientCount returns the number of connected clients.
func (h *sseHub) clientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// handleSSE handles SSE connections for the admin dashboard.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	// Authenticate via JWT (Bearer header or ?token= query param for EventSource)
	tokenStr := ""
	if auth := r.Header.Get("Authorization"); auth != "" {
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) == 2 && parts[0] == "Bearer" {
			tokenStr = parts[1]
		}
	}
	if tokenStr == "" {
		tokenStr = r.URL.Query().Get("token")
	}
	if tokenStr == "" {
		http.Error(w, "missing authorization", http.StatusUnauthorized)
		return
	}
	if _, err := s.ValidateToken(tokenStr); err != nil {
		log.Debug().Err(err).Msg("SSE auth failed")
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	// Disable the server-level WriteTimeout for this connection — SSE streams are
	// long-lived by design and would be killed by the global 120s write deadline.
	// Client disconnect is detected via r.Context().Done() instead.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Check if we can flush
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Create client
	client := &sseClient{
		events: make(chan string, 64), // Buffer up to 64 events
		done:   make(chan struct{}),
	}

	// Register client
	s.sseHub.register(client)
	defer s.sseHub.unregister(client)

	// Send initial connection event
	_, _ = fmt.Fprintf(w, "event: connected\ndata: {}\n\n")
	flusher.Flush()

	// Stream events until client disconnects
	for {
		select {
		case event, ok := <-client.events:
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(w, "%s\n\n", event)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// notifyHeartbeat broadcasts a heartbeat event to all SSE clients.
func (s *Server) notifyHeartbeat(peerName string) {
	if s.sseHub == nil {
		return
	}

	nameJSON, _ := json.Marshal(peerName)
	event := fmt.Sprintf("event: heartbeat\ndata: {\"peer\":%s}", nameJSON)
	s.sseHub.broadcast(event)
}
