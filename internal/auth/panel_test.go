package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- PanelRegistry tests ---

func TestPanelRegistry_BuiltinPanels(t *testing.T) {
	registry := NewPanelRegistry()

	// Should have all built-in panels
	panels := registry.List()
	assert.Len(t, panels, 13) // visualizer, map, alerts, peers, logs, filter, dns, s3, shares, users, groups, bindings, docker

	// Check specific panels exist
	assert.NotNil(t, registry.Get(PanelVisualizer))
	assert.NotNil(t, registry.Get(PanelS3))
	assert.NotNil(t, registry.Get(PanelPeers))
}

func TestPanelRegistry_BuiltinPanelsAreMarked(t *testing.T) {
	registry := NewPanelRegistry()

	for _, id := range BuiltinPanelIDs() {
		panel := registry.Get(id)
		require.NotNil(t, panel, "Panel %s should exist", id)
		assert.True(t, panel.Builtin, "Panel %s should be marked as builtin", id)
	}
}

func TestPanelRegistry_ListByTab(t *testing.T) {
	registry := NewPanelRegistry()

	appPanels := registry.ListByTab(PanelTabApp)
	dataPanels := registry.ListByTab(PanelTabData)
	meshPanels := registry.ListByTab(PanelTabMesh)

	// App tab should have: s3, shares, docker
	assert.Len(t, appPanels, 3)

	// Data tab should have: peers-mgmt, groups, bindings, dns
	assert.Len(t, dataPanels, 4)

	// Mesh tab should have: visualizer, map, alerts, peers, logs, filter
	assert.Len(t, meshPanels, 6)
}

func TestPanelRegistry_RegisterExternal(t *testing.T) {
	registry := NewPanelRegistry()

	// Register external panel
	err := registry.Register(PanelDefinition{
		ID:        "my-plugin",
		Name:      "My Plugin",
		Tab:       PanelTabData,
		External:  true,
		PluginURL: "https://example.com/plugin.js",
		SortOrder: 100,
	})
	require.NoError(t, err)

	panel := registry.Get("my-plugin")
	require.NotNil(t, panel)
	assert.Equal(t, "My Plugin", panel.Name)
	assert.True(t, panel.External)
	assert.False(t, panel.Builtin)
}

func TestPanelRegistry_CannotOverrideBuiltin(t *testing.T) {
	registry := NewPanelRegistry()

	// Try to override built-in panel
	err := registry.Register(PanelDefinition{
		ID:   PanelS3,
		Name: "Hacked S3",
		Tab:  PanelTabData,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot override built-in panel")
}

func TestPanelRegistry_Unregister(t *testing.T) {
	registry := NewPanelRegistry()

	// Register and unregister external panel
	err := registry.Register(PanelDefinition{
		ID:       "temp-plugin",
		Name:     "Temporary",
		Tab:      PanelTabMesh,
		External: true,
	})
	require.NoError(t, err)

	err = registry.Unregister("temp-plugin")
	assert.NoError(t, err)
	assert.Nil(t, registry.Get("temp-plugin"))
}

func TestPanelRegistry_CannotUnregisterBuiltin(t *testing.T) {
	registry := NewPanelRegistry()

	err := registry.Unregister(PanelS3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot unregister built-in panel")
}

func TestPanelRegistry_SetPublic(t *testing.T) {
	registry := NewPanelRegistry()

	// Initially not public
	panel := registry.Get(PanelVisualizer)
	assert.False(t, panel.Public)

	// Set public
	err := registry.SetPublic(PanelVisualizer, true)
	require.NoError(t, err)

	panel = registry.Get(PanelVisualizer)
	assert.True(t, panel.Public)

	// ListPublic should include it
	publicPanels := registry.ListPublic()
	assert.Len(t, publicPanels, 1)
	assert.Equal(t, PanelVisualizer, publicPanels[0].ID)
}

func TestPanelRegistry_InvalidTab(t *testing.T) {
	registry := NewPanelRegistry()

	err := registry.Register(PanelDefinition{
		ID:   "bad-panel",
		Name: "Bad",
		Tab:  "invalid",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid panel tab")
}

// --- Panel authorization tests ---

func TestAuthorizer_CanAccessPanel_Admin(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	// Grant admin role
	auth.Bindings.Add(NewRoleBinding("alice", RoleAdmin, ""))

	// Admin can access all panels
	assert.True(t, auth.CanAccessPanel("alice", PanelS3))
	assert.True(t, auth.CanAccessPanel("alice", PanelPeers))
	assert.True(t, auth.CanAccessPanel("alice", PanelPeerMgmt))
}

func TestAuthorizer_CanAccessPanel_DirectBinding(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	// Grant panel-viewer role for specific panel
	auth.Bindings.Add(NewRoleBindingForPanel("bob", PanelS3))

	// Bob can access S3 panel
	assert.True(t, auth.CanAccessPanel("bob", PanelS3))

	// Bob cannot access other panels
	assert.False(t, auth.CanAccessPanel("bob", PanelPeers))
	assert.False(t, auth.CanAccessPanel("bob", PanelPeerMgmt))
}

func TestAuthorizer_CanAccessPanel_GroupBinding(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	// Add alice to everyone group
	_ = auth.Groups.AddMember(GroupEveryone, "alice")

	// Grant everyone group access to S3 and shares panels
	auth.GroupBindings.Add(NewGroupBindingForPanel(GroupEveryone, PanelS3))
	auth.GroupBindings.Add(NewGroupBindingForPanel(GroupEveryone, PanelShares))

	// Alice can access S3 and shares via group
	assert.True(t, auth.CanAccessPanel("alice", PanelS3))
	assert.True(t, auth.CanAccessPanel("alice", PanelShares))

	// Alice cannot access admin panels
	assert.False(t, auth.CanAccessPanel("alice", PanelPeers))
	assert.False(t, auth.CanAccessPanel("alice", PanelPeerMgmt))
}

func TestAuthorizer_CanAccessPanel_PublicPanel(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	// Set visualizer as public
	err := auth.PanelRegistry.SetPublic(PanelVisualizer, true)
	require.NoError(t, err)

	// Anyone can access public panel (even without any bindings)
	assert.True(t, auth.CanAccessPanel("anonymous", PanelVisualizer))
	assert.True(t, auth.CanAccessPanel("", PanelVisualizer))

	// But not non-public panels
	assert.False(t, auth.CanAccessPanel("anonymous", PanelS3))
}

func TestAuthorizer_CanAccessPanel_UnrestrictedBinding(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	// Grant panel-viewer with no panel scope (all panels)
	binding := NewRoleBinding("superuser", RolePanelViewer, "")
	binding.PanelScope = "" // Empty = all panels
	auth.Bindings.Add(binding)

	// User can access all panels
	assert.True(t, auth.CanAccessPanel("superuser", PanelS3))
	assert.True(t, auth.CanAccessPanel("superuser", PanelPeers))
	assert.True(t, auth.CanAccessPanel("superuser", PanelPeerMgmt))
}

func TestAuthorizer_GetAccessiblePanels_Admin(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	auth.Bindings.Add(NewRoleBinding("alice", RoleAdmin, ""))

	panels := auth.GetAccessiblePanels("alice")
	assert.Len(t, panels, 13) // All built-in panels
}

func TestAuthorizer_GetAccessiblePanels_DirectBindings(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	// Grant access to specific panels
	auth.Bindings.Add(NewRoleBindingForPanel("bob", PanelS3))
	auth.Bindings.Add(NewRoleBindingForPanel("bob", PanelShares))

	panels := auth.GetAccessiblePanels("bob")
	assert.Len(t, panels, 2)
	assert.Contains(t, panels, PanelS3)
	assert.Contains(t, panels, PanelShares)
}

func TestAuthorizer_GetAccessiblePanels_WithPublic(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	// Set visualizer as public
	_ = auth.PanelRegistry.SetPublic(PanelVisualizer, true)

	// Grant access to S3
	auth.Bindings.Add(NewRoleBindingForPanel("bob", PanelS3))

	panels := auth.GetAccessiblePanels("bob")
	assert.Len(t, panels, 2)
	assert.Contains(t, panels, PanelVisualizer) // Public
	assert.Contains(t, panels, PanelS3)         // Via binding
}

func TestAuthorizer_GetAccessiblePanels_NoBindings(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	panels := auth.GetAccessiblePanels("nobody")
	assert.Empty(t, panels)
}

func TestAuthorizer_GetAccessiblePanels_ViaGroup(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	// Add alice to everyone group
	_ = auth.Groups.AddMember(GroupEveryone, "alice")

	// Grant everyone group access to user panels
	auth.GroupBindings.Add(NewGroupBindingForPanel(GroupEveryone, PanelVisualizer))
	auth.GroupBindings.Add(NewGroupBindingForPanel(GroupEveryone, PanelMap))
	auth.GroupBindings.Add(NewGroupBindingForPanel(GroupEveryone, PanelS3))
	auth.GroupBindings.Add(NewGroupBindingForPanel(GroupEveryone, PanelShares))

	panels := auth.GetAccessiblePanels("alice")
	assert.Len(t, panels, 4)
}

func TestAuthorizer_GetAccessiblePanels_AdminViaGroup(t *testing.T) {
	auth := NewAuthorizerWithGroups()

	// Add alice to admins group
	_ = auth.Groups.AddMember(GroupAdmins, "alice")

	// admins should get all panels via IsAdmin check
	panels := auth.GetAccessiblePanels("alice")
	assert.Len(t, panels, 13) // All panels
}

// --- Default panel tests ---

func TestDefaultPeerPanels(t *testing.T) {
	defaults := DefaultPeerPanels()
	assert.Contains(t, defaults, PanelVisualizer)
	assert.Contains(t, defaults, PanelMap)
	assert.Contains(t, defaults, PanelS3)
	assert.Contains(t, defaults, PanelShares)
	assert.NotContains(t, defaults, PanelPeers)
	assert.NotContains(t, defaults, PanelPeerMgmt)
}

func TestDefaultAdminPanels(t *testing.T) {
	defaults := DefaultAdminPanels()
	assert.Contains(t, defaults, PanelPeers)
	assert.Contains(t, defaults, PanelLogs)
	assert.Contains(t, defaults, PanelPeerMgmt)
	assert.Contains(t, defaults, PanelBindings)
	assert.NotContains(t, defaults, PanelS3)
	assert.NotContains(t, defaults, PanelVisualizer)
}

// --- Binding scope tests ---

func TestRoleBinding_AppliesToPanel(t *testing.T) {
	// Binding with panel scope
	binding := NewRoleBindingForPanel("alice", PanelS3)
	assert.True(t, binding.AppliesToPanel(PanelS3))
	assert.False(t, binding.AppliesToPanel(PanelPeers))

	// Binding without panel scope (all panels)
	unscopedBinding := &RoleBinding{
		PeerID:   "bob",
		RoleName: RolePanelViewer,
	}
	assert.True(t, unscopedBinding.AppliesToPanel(PanelS3))
	assert.True(t, unscopedBinding.AppliesToPanel(PanelPeers))
}

func TestGroupBinding_AppliesToPanel(t *testing.T) {
	// Binding with panel scope
	binding := NewGroupBindingForPanel(GroupEveryone, PanelS3)
	assert.True(t, binding.AppliesToPanel(PanelS3))
	assert.False(t, binding.AppliesToPanel(PanelPeers))

	// Binding without panel scope (all panels)
	unscopedBinding := &GroupBinding{
		GroupName: GroupEveryone,
		RoleName:  RolePanelViewer,
	}
	assert.True(t, unscopedBinding.AppliesToPanel(PanelS3))
	assert.True(t, unscopedBinding.AppliesToPanel(PanelPeers))
}
