package coord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

// PeerPermissions is the response for the peer permissions endpoint.
type PeerPermissions struct {
	PeerID  string   `json:"peer_id"`
	IsAdmin bool     `json:"is_admin"`
	Panels  []string `json:"panels"`
}

// handlePeerPermissions returns the current peer's accessible panels.
func (s *Server) handlePeerPermissions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.s3Authorizer == nil {
		s.jsonError(w, "authorizer not configured", http.StatusServiceUnavailable)
		return
	}

	peerID := s.getRequestOwner(r)
	if peerID == "" {
		peerID = "guest"
	}

	isAdmin := s.s3Authorizer.IsAdmin(peerID)
	panels := s.s3Authorizer.GetAccessiblePanels(peerID)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(PeerPermissions{
		PeerID:  peerID,
		IsAdmin: isAdmin,
		Panels:  panels,
	})
}

// allowedPluginSchemes defines the allowed URL schemes for external panel plugins.
// Only https and http are allowed to prevent javascript:, file://, data:, etc.
var allowedPluginSchemes = map[string]bool{
	"https": true,
	"http":  true,
}

// validatePluginURL validates that a plugin URL is safe to load.
// Returns an error if the URL scheme is not allowed or the URL is invalid.
func validatePluginURL(pluginURL string) error {
	parsed, err := url.Parse(pluginURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	if parsed.Scheme == "" {
		return errors.New("URL must have a scheme (https:// or http://)")
	}

	if !allowedPluginSchemes[parsed.Scheme] {
		return fmt.Errorf("scheme %q not allowed (must be https or http)", parsed.Scheme)
	}

	if parsed.Host == "" {
		return errors.New("URL must have a host")
	}

	return nil
}

// persistExternalPanels saves external panels to the system store.
// Returns an error if persistence fails.
func (s *Server) persistExternalPanels() error {
	if s.s3SystemStore == nil || s.s3Authorizer == nil || s.s3Authorizer.PanelRegistry == nil {
		return nil // No storage configured, skip persistence
	}

	// Get all external panels
	externalPanels := s.s3Authorizer.PanelRegistry.ListExternal()
	panelPtrs := make([]*auth.PanelDefinition, len(externalPanels))
	for i := range externalPanels {
		panelPtrs[i] = &externalPanels[i]
	}

	return s.s3SystemStore.SavePanels(context.Background(), panelPtrs)
}

// handlePanels handles GET (list) and POST (register) for panels.
func (s *Server) handlePanels(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handlePanelsList(w, r)
	case http.MethodPost:
		s.handlePanelRegister(w, r)
	default:
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePanelsList returns all registered panels.
func (s *Server) handlePanelsList(w http.ResponseWriter, r *http.Request) {
	if s.s3Authorizer == nil || s.s3Authorizer.PanelRegistry == nil {
		s.jsonError(w, "panel registry not configured", http.StatusServiceUnavailable)
		return
	}

	// Filter by external if query param present
	externalOnly := r.URL.Query().Get("external") == "true"

	var panels []auth.PanelDefinition
	if externalOnly {
		panels = s.s3Authorizer.PanelRegistry.ListExternal()
	} else {
		panels = s.s3Authorizer.PanelRegistry.List()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"panels": panels,
	})
}

// handlePanelRegister registers a new external panel (admin only).
func (s *Server) handlePanelRegister(w http.ResponseWriter, r *http.Request) {
	if s.s3Authorizer == nil || s.s3Authorizer.PanelRegistry == nil {
		s.jsonError(w, "panel registry not configured", http.StatusServiceUnavailable)
		return
	}

	peerID := s.getRequestOwner(r)
	if !s.s3Authorizer.IsAdmin(peerID) {
		s.jsonError(w, "admin access required", http.StatusForbidden)
		return
	}

	var panel auth.PanelDefinition
	if err := json.NewDecoder(r.Body).Decode(&panel); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Force external flag for API-registered panels
	panel.External = true
	panel.CreatedBy = peerID

	// Validate PluginURL for external panels (security: prevent javascript:, file://, etc.)
	if panel.PluginURL != "" {
		if err := validatePluginURL(panel.PluginURL); err != nil {
			s.jsonError(w, fmt.Sprintf("invalid plugin URL: %v", err), http.StatusBadRequest)
			return
		}
	}

	if err := s.s3Authorizer.PanelRegistry.Register(panel); err != nil {
		s.jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Persist external panels - rollback on failure
	if err := s.persistExternalPanels(); err != nil {
		// Rollback: unregister the panel
		_ = s.s3Authorizer.PanelRegistry.Unregister(panel.ID)
		log.Error().Err(err).Str("panel_id", panel.ID).Msg("failed to persist panel, rolling back")
		s.jsonError(w, "failed to persist panel registration", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "registered",
		"panel":  s.s3Authorizer.PanelRegistry.Get(panel.ID),
	})
}

// handlePanelByID handles GET, PATCH, DELETE for a specific panel.
func (s *Server) handlePanelByID(w http.ResponseWriter, r *http.Request) {
	// Extract panel ID from path: /api/panels/{id}
	panelID := strings.TrimPrefix(r.URL.Path, "/api/panels/")
	if panelID == "" {
		s.jsonError(w, "panel ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handlePanelGet(w, r, panelID)
	case http.MethodPatch:
		s.handlePanelUpdate(w, r, panelID)
	case http.MethodDelete:
		s.handlePanelDelete(w, r, panelID)
	default:
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePanelGet returns a specific panel.
func (s *Server) handlePanelGet(w http.ResponseWriter, _ *http.Request, panelID string) {
	if s.s3Authorizer == nil || s.s3Authorizer.PanelRegistry == nil {
		s.jsonError(w, "panel registry not configured", http.StatusServiceUnavailable)
		return
	}

	panel := s.s3Authorizer.PanelRegistry.Get(panelID)
	if panel == nil {
		s.jsonError(w, "panel not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(panel)
}

// handlePanelUpdate updates a panel (admin only).
func (s *Server) handlePanelUpdate(w http.ResponseWriter, r *http.Request, panelID string) {
	if s.s3Authorizer == nil || s.s3Authorizer.PanelRegistry == nil {
		s.jsonError(w, "panel registry not configured", http.StatusServiceUnavailable)
		return
	}

	peerID := s.getRequestOwner(r)
	if !s.s3Authorizer.IsAdmin(peerID) {
		s.jsonError(w, "admin access required", http.StatusForbidden)
		return
	}

	panel := s.s3Authorizer.PanelRegistry.Get(panelID)
	if panel == nil {
		s.jsonError(w, "panel not found", http.StatusNotFound)
		return
	}

	var update auth.PanelDefinition
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Validate PluginURL if provided (security: prevent javascript:, file://, etc.)
	if update.PluginURL != "" {
		if err := validatePluginURL(update.PluginURL); err != nil {
			s.jsonError(w, fmt.Sprintf("invalid plugin URL: %v", err), http.StatusBadRequest)
			return
		}
	}

	if err := s.s3Authorizer.PanelRegistry.Update(panelID, update); err != nil {
		s.jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Persist external panels
	if err := s.persistExternalPanels(); err != nil {
		// Log but don't fail - panel is already updated in memory
		log.Warn().Err(err).Str("panel_id", panelID).Msg("failed to persist panel update")
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "updated",
		"panel":  s.s3Authorizer.PanelRegistry.Get(panelID),
	})
}

// handlePanelDelete unregisters an external panel (admin only).
func (s *Server) handlePanelDelete(w http.ResponseWriter, r *http.Request, panelID string) {
	if s.s3Authorizer == nil || s.s3Authorizer.PanelRegistry == nil {
		s.jsonError(w, "panel registry not configured", http.StatusServiceUnavailable)
		return
	}

	peerID := s.getRequestOwner(r)
	if !s.s3Authorizer.IsAdmin(peerID) {
		s.jsonError(w, "admin access required", http.StatusForbidden)
		return
	}

	// Get panel before deletion for potential rollback
	panel := s.s3Authorizer.PanelRegistry.Get(panelID)
	if panel == nil {
		s.jsonError(w, "panel not found", http.StatusNotFound)
		return
	}
	panelCopy := *panel

	if err := s.s3Authorizer.PanelRegistry.Unregister(panelID); err != nil {
		s.jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Persist external panels - rollback on failure
	if err := s.persistExternalPanels(); err != nil {
		// Rollback: re-register the panel
		_ = s.s3Authorizer.PanelRegistry.Register(panelCopy)
		log.Error().Err(err).Str("panel_id", panelID).Msg("failed to persist panel deletion, rolling back")
		s.jsonError(w, "failed to persist panel deletion", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "deleted",
	})
}

// handleSystemHealth checks the integrity of system metadata stored in S3.
// Returns version information and metadata health status for each critical file.
//
// Security: This endpoint is mounted on adminMux, which is only accessible from
// within the mesh network via HTTPS. Network-level access control is enforced by
// binding to the coordinator's mesh IP address.
//
// GET /api/system/health
func (s *Server) handleSystemHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.s3SystemStore == nil {
		s.jsonError(w, "S3 not enabled", http.StatusServiceUnavailable)
		return
	}

	results := make(map[string]interface{})
	store := s.s3SystemStore.Raw()

	// Check each critical file
	files := []string{
		s3.PeersPath,
		s3.BindingsPath,
		s3.GroupsPath,
		s3.GroupBindingsPath,
		s3.DNSCachePath,
		s3.DNSAliasPath,
	}

	for _, path := range files {
		versions, err := store.ListVersions(context.Background(), s3.SystemBucket, path)

		fileInfo := make(map[string]interface{})
		fileInfo["exists"] = err == nil && len(versions) > 0
		fileInfo["version_count"] = len(versions)

		if len(versions) > 0 {
			fileInfo["latest_version"] = versions[0].VersionID
			fileInfo["latest_timestamp"] = versions[0].LastModified
			fileInfo["latest_size_bytes"] = versions[0].Size
		}

		if err != nil {
			fileInfo["error"] = err.Error()
		}

		results[path] = fileInfo
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"files":  results,
	}); err != nil {
		log.Warn().Err(err).Msg("failed to encode health response")
	}
}
