package auth

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Built-in panel IDs
const (
	PanelVisualizer = "visualizer"
	PanelMap        = "map"
	PanelAlerts     = "alerts"
	PanelPeers      = "peers"
	PanelLogs       = "logs"
	PanelFilter     = "filter"
	PanelDNS        = "dns"
	PanelS3         = "s3"
	PanelShares     = "shares"
	PanelPeerMgmt   = "peers-mgmt"
	PanelGroups     = "groups"
	PanelBindings   = "bindings"
	PanelDocker     = "docker"
	PanelTracing    = "tracing"
)

// Panel tabs
const (
	PanelTabApp  = "app"
	PanelTabData = "data"
	PanelTabMesh = "mesh"
)

// Panel categories
const (
	PanelCategoryNetwork    = "network"
	PanelCategoryStorage    = "storage"
	PanelCategoryAdmin      = "admin"
	PanelCategoryMonitoring = "monitoring"
)

// PanelDefinition describes a registered panel.
type PanelDefinition struct {
	ID          string    `json:"id"`                    // Unique identifier
	Name        string    `json:"name"`                  // Display name
	Tab         string    `json:"tab"`                   // "app", "data", or "mesh"
	Category    string    `json:"category,omitempty"`    // For grouping: network, storage, admin
	Description string    `json:"description,omitempty"` // Help text
	Icon        string    `json:"icon,omitempty"`        // Optional icon identifier
	Public      bool      `json:"public"`                // Visible without authentication
	External    bool      `json:"external"`              // True if plugin-provided
	PluginURL   string    `json:"plugin_url,omitempty"`  // For external: iframe/script source
	PluginType  string    `json:"plugin_type,omitempty"` // "iframe" or "script"
	SortOrder   int       `json:"sort_order"`            // Display ordering
	Builtin     bool      `json:"builtin"`               // True for built-in panels
	CreatedAt   time.Time `json:"created_at,omitempty"`  // When registered (for external)
	CreatedBy   string    `json:"created_by,omitempty"`  // Who registered (for external)
}

// PanelRegistry manages dynamic panel registration.
type PanelRegistry struct {
	panels map[string]*PanelDefinition
	mu     sync.RWMutex
}

// NewPanelRegistry creates a new panel registry with built-in panels.
func NewPanelRegistry() *PanelRegistry {
	r := &PanelRegistry{
		panels: make(map[string]*PanelDefinition),
	}
	r.registerBuiltinPanels()
	return r
}

// registerBuiltinPanels registers all built-in panels.
func (r *PanelRegistry) registerBuiltinPanels() {
	builtins := []PanelDefinition{
		// Mesh tab panels
		{ID: PanelVisualizer, Name: "Network Topology", Tab: PanelTabMesh, Category: PanelCategoryNetwork, SortOrder: 10, Builtin: true},
		{ID: PanelMap, Name: "Node Locations", Tab: PanelTabMesh, Category: PanelCategoryNetwork, SortOrder: 20, Builtin: true},
		{ID: PanelAlerts, Name: "Active Alerts", Tab: PanelTabMesh, Category: PanelCategoryMonitoring, SortOrder: 35, Builtin: true},
		{ID: PanelPeers, Name: "Connected Peers", Tab: PanelTabMesh, Category: PanelCategoryNetwork, SortOrder: 40, Builtin: true},
		{ID: PanelLogs, Name: "Peer Logs", Tab: PanelTabMesh, Category: PanelCategoryMonitoring, SortOrder: 50, Builtin: true},
		{ID: PanelTracing, Name: "Recent Traces", Tab: PanelTabMesh, Category: PanelCategoryMonitoring, SortOrder: 60, Builtin: true},
		{ID: PanelFilter, Name: "Packet Filter", Tab: PanelTabMesh, Category: PanelCategoryAdmin, SortOrder: 70, Builtin: true},

		// App tab panels
		{ID: PanelS3, Name: "Objects", Tab: PanelTabApp, Category: PanelCategoryStorage, SortOrder: 10, Builtin: true},
		{ID: PanelShares, Name: "Shares", Tab: PanelTabApp, Category: PanelCategoryStorage, SortOrder: 20, Builtin: true},
		{ID: PanelDocker, Name: "Docker Containers", Tab: PanelTabApp, Category: PanelCategoryAdmin, SortOrder: 30, Builtin: true},

		// Data tab panels
		{ID: PanelPeerMgmt, Name: "Peers", Tab: PanelTabData, Category: PanelCategoryAdmin, SortOrder: 10, Builtin: true},
		{ID: PanelGroups, Name: "Groups", Tab: PanelTabData, Category: PanelCategoryAdmin, SortOrder: 20, Builtin: true},
		{ID: PanelBindings, Name: "Role Bindings", Tab: PanelTabData, Category: PanelCategoryAdmin, SortOrder: 30, Builtin: true},
		{ID: PanelDNS, Name: "DNS Records", Tab: PanelTabData, Category: PanelCategoryNetwork, SortOrder: 40, Builtin: true},
	}

	for _, p := range builtins {
		panel := p
		r.panels[p.ID] = &panel
	}
}

// Register adds a panel to the registry.
// Returns error if panel ID already exists (for built-in) or is invalid.
func (r *PanelRegistry) Register(panel PanelDefinition) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if panel.ID == "" {
		return fmt.Errorf("panel ID is required")
	}

	if existing, ok := r.panels[panel.ID]; ok {
		if existing.Builtin {
			return fmt.Errorf("cannot override built-in panel: %s", panel.ID)
		}
		// Allow re-registration of external panels (update)
	}

	if panel.Tab != PanelTabApp && panel.Tab != PanelTabData && panel.Tab != PanelTabMesh {
		return fmt.Errorf("invalid panel tab: %s (must be 'app', 'data', or 'mesh')", panel.Tab)
	}

	if panel.CreatedAt.IsZero() {
		panel.CreatedAt = time.Now()
	}

	r.panels[panel.ID] = &panel
	return nil
}

// Unregister removes a panel from the registry.
// Returns error if panel is built-in or doesn't exist.
func (r *PanelRegistry) Unregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	panel, ok := r.panels[id]
	if !ok {
		return fmt.Errorf("panel not found: %s", id)
	}

	if panel.Builtin {
		return fmt.Errorf("cannot unregister built-in panel: %s", id)
	}

	delete(r.panels, id)
	return nil
}

// Get returns a panel by ID, or nil if not found.
func (r *PanelRegistry) Get(id string) *PanelDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.panels[id]
}

// List returns all panels sorted by tab then sort order.
func (r *PanelRegistry) List() []PanelDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]PanelDefinition, 0, len(r.panels))
	for _, p := range r.panels {
		result = append(result, *p)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Tab != result[j].Tab {
			return result[i].Tab < result[j].Tab
		}
		return result[i].SortOrder < result[j].SortOrder
	})

	return result
}

// ListByTab returns all panels for a specific tab.
func (r *PanelRegistry) ListByTab(tab string) []PanelDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []PanelDefinition
	for _, p := range r.panels {
		if p.Tab == tab {
			result = append(result, *p)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].SortOrder < result[j].SortOrder
	})

	return result
}

// ListExternal returns all external (plugin) panels.
func (r *PanelRegistry) ListExternal() []PanelDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []PanelDefinition
	for _, p := range r.panels {
		if p.External {
			result = append(result, *p)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].SortOrder < result[j].SortOrder
	})

	return result
}

// ListIDs returns all panel IDs.
func (r *PanelRegistry) ListIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.panels))
	for id := range r.panels {
		ids = append(ids, id)
	}
	return ids
}

// ListPublic returns all public panels (visible without auth).
func (r *PanelRegistry) ListPublic() []PanelDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []PanelDefinition
	for _, p := range r.panels {
		if p.Public {
			result = append(result, *p)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Tab != result[j].Tab {
			return result[i].Tab < result[j].Tab
		}
		return result[i].SortOrder < result[j].SortOrder
	})

	return result
}

// SetPublic updates the public flag for a panel.
// Uses mutex for synchronization and copy-on-write semantics to ensure
// readers with existing panel references see consistent data.
func (r *PanelRegistry) SetPublic(id string, public bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, ok := r.panels[id]
	if !ok {
		return fmt.Errorf("panel not found: %s", id)
	}

	// Copy before modifying to avoid mutating panel visible to concurrent readers
	updated := *existing
	updated.Public = public
	r.panels[id] = &updated

	return nil
}

// Update updates a panel's metadata (non-ID fields).
// Only works for external panels.
// Uses mutex for synchronization and copy-on-write semantics to ensure
// readers with existing panel references see consistent data.
func (r *PanelRegistry) Update(id string, update PanelDefinition) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, ok := r.panels[id]
	if !ok {
		return fmt.Errorf("panel not found: %s", id)
	}

	// Copy before modifying to avoid mutating panel visible to concurrent readers
	updated := *existing

	// Allow updating public flag on any panel
	updated.Public = update.Public

	// Only allow full updates on external panels
	if !updated.Builtin {
		if update.Name != "" {
			updated.Name = update.Name
		}
		if update.Description != "" {
			updated.Description = update.Description
		}
		if update.Icon != "" {
			updated.Icon = update.Icon
		}
		if update.PluginURL != "" {
			updated.PluginURL = update.PluginURL
		}
		if update.PluginType != "" {
			updated.PluginType = update.PluginType
		}
		if update.SortOrder != 0 {
			updated.SortOrder = update.SortOrder
		}
	}

	// Atomically replace the pointer
	r.panels[id] = &updated

	return nil
}

// BuiltinPanelIDs returns all built-in panel IDs.
func BuiltinPanelIDs() []string {
	return []string{
		PanelVisualizer, PanelMap, PanelAlerts, PanelPeers,
		PanelLogs, PanelTracing, PanelFilter, PanelDNS,
		PanelS3, PanelShares, PanelPeerMgmt, PanelGroups, PanelBindings, PanelDocker,
	}
}

// DefaultPeerPanels returns panel IDs that peers get by default.
func DefaultPeerPanels() []string {
	return []string{
		PanelVisualizer, PanelMap, PanelAlerts, PanelS3, PanelShares,
	}
}

// DefaultAdminPanels returns panel IDs that only admins get by default.
// Note: Admins also inherit all panels from DefaultPeerPanels (alerts, visualizer, etc.)
func DefaultAdminPanels() []string {
	return []string{
		PanelPeers, PanelLogs, PanelTracing, PanelFilter, PanelDNS,
		PanelPeerMgmt, PanelGroups, PanelBindings, PanelDocker,
	}
}
