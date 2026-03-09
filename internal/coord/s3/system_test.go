package s3

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
)

func TestNewSystemStore(t *testing.T) {
	store := newTestStoreWithCAS(t)

	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// System bucket should exist
	meta, err := store.HeadBucket(context.Background(), SystemBucket)
	require.NoError(t, err)
	assert.Equal(t, SystemBucket, meta.Name)
	assert.Equal(t, "svc:coordinator", meta.Owner)

	// Raw should return the underlying store
	assert.Equal(t, store, ss.Raw())
}

func TestNewSystemStoreExistingBucket(t *testing.T) {
	store := newTestStoreWithCAS(t)

	// Pre-create the system bucket
	require.NoError(t, store.CreateBucket(context.Background(), SystemBucket, "svc:coordinator", 3))

	// Should not error when bucket exists
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)
	assert.NotNil(t, ss)
}

func TestSystemStoreSaveLoadPeers(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	peers := []*auth.Peer{
		{ID: "alice", Name: "Alice", PublicKey: "pk1", CreatedAt: time.Now().UTC()},
		{ID: "bob", Name: "Bob", PublicKey: "pk2", CreatedAt: time.Now().UTC()},
	}

	err = ss.SavePeers(context.Background(), peers)
	require.NoError(t, err)

	loaded, err := ss.LoadPeers(context.Background())
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	assert.Equal(t, "alice", loaded[0].ID)
	assert.Equal(t, "bob", loaded[1].ID)
}

func TestSystemStoreLoadPeersNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Should return nil (not error) when not found
	loaded, err := ss.LoadPeers(context.Background())
	require.NoError(t, err)
	assert.Nil(t, loaded)
}

func TestSystemStoreSaveLoadRoles(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	roles := []auth.Role{
		{Name: "custom-role", Rules: []auth.Rule{{Verbs: []string{"get"}, Resources: []string{"buckets"}}}},
	}

	err = ss.SaveRoles(context.Background(), roles)
	require.NoError(t, err)

	loaded, err := ss.LoadRoles(context.Background())
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, "custom-role", loaded[0].Name)
}

func TestSystemStoreSaveLoadBindings(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	bindings := []*auth.RoleBinding{
		{Name: "b1", PeerID: "alice", RoleName: "admin"},
		{Name: "b2", PeerID: "bob", RoleName: "bucket-read", BucketScope: "my-bucket"},
	}

	err = ss.SaveBindings(context.Background(), bindings)
	require.NoError(t, err)

	loaded, err := ss.LoadBindings(context.Background())
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	assert.Equal(t, "alice", loaded[0].PeerID)
	assert.Equal(t, "bob", loaded[1].PeerID)
	assert.Equal(t, "my-bucket", loaded[1].BucketScope)
}

func TestSystemStoreExists(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Initially doesn't exist
	assert.False(t, ss.Exists(context.Background(), PeersPath))

	// Save users
	err = ss.SavePeers(context.Background(), []*auth.Peer{{ID: "test", Name: "Test"}})
	require.NoError(t, err)

	// Now exists
	assert.True(t, ss.Exists(context.Background(), PeersPath))
}

func TestSystemStoreDelete(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Save and then delete
	err = ss.SavePeers(context.Background(), []*auth.Peer{{ID: "test", Name: "Test"}})
	require.NoError(t, err)
	assert.True(t, ss.Exists(context.Background(), PeersPath))

	err = ss.Delete(context.Background(), PeersPath)
	require.NoError(t, err)
	assert.False(t, ss.Exists(context.Background(), PeersPath))
}

func TestSystemStoreDeleteNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// PurgeObject is idempotent — deleting a non-existent object succeeds silently
	err = ss.Delete(context.Background(), "nonexistent.json")
	assert.NoError(t, err)
}

func TestSystemStoreSaveLoadFilterRules(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Test basic save/load
	rules := FilterRulesData{
		Temporary: []FilterRulePersisted{
			{Port: 22, Protocol: "tcp", Action: "allow", Expires: 0, SourcePeer: ""},
			{Port: 80, Protocol: "tcp", Action: "allow", Expires: time.Now().Unix() + 3600, SourcePeer: "peer1"},
			{Port: 443, Protocol: "tcp", Action: "deny", Expires: time.Now().Unix() + 7200, SourcePeer: ""},
		},
	}

	err = ss.SaveFilterRules(context.Background(), rules)
	require.NoError(t, err)

	loaded, err := ss.LoadFilterRules(context.Background())
	require.NoError(t, err)
	require.Len(t, loaded.Temporary, 3)
	assert.Equal(t, uint16(22), loaded.Temporary[0].Port)
	assert.Equal(t, "tcp", loaded.Temporary[0].Protocol)
	assert.Equal(t, "allow", loaded.Temporary[0].Action)
	assert.Equal(t, int64(0), loaded.Temporary[0].Expires)
	assert.Equal(t, "", loaded.Temporary[0].SourcePeer)
	assert.Equal(t, uint16(80), loaded.Temporary[1].Port)
	assert.Equal(t, "peer1", loaded.Temporary[1].SourcePeer)
}

func TestSystemStoreLoadFilterRulesNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Should return empty struct (not error) when not found
	loaded, err := ss.LoadFilterRules(context.Background())
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Nil(t, loaded.Temporary)
}

func TestSystemStoreFilterRulesWithExpiry(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Save rule with past expiry (already expired)
	pastTime := time.Now().Unix() - 1000
	rules := FilterRulesData{
		Temporary: []FilterRulePersisted{
			{Port: 22, Protocol: "tcp", Action: "allow", Expires: pastTime, SourcePeer: ""},
		},
	}

	err = ss.SaveFilterRules(context.Background(), rules)
	require.NoError(t, err)

	// Verify expired rules are still in storage (filtering happens in server.go recoverFilterRules)
	loaded, err := ss.LoadFilterRules(context.Background())
	require.NoError(t, err)
	require.Len(t, loaded.Temporary, 1)
	assert.Equal(t, pastTime, loaded.Temporary[0].Expires)
}

func TestSystemStoreFilterRulesEmptyArray(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Save empty rules
	rules := FilterRulesData{
		Temporary: []FilterRulePersisted{},
	}

	err = ss.SaveFilterRules(context.Background(), rules)
	require.NoError(t, err)

	loaded, err := ss.LoadFilterRules(context.Background())
	require.NoError(t, err)
	require.NotNil(t, loaded.Temporary)
	assert.Len(t, loaded.Temporary, 0)
}

func TestSystemStoreSaveLoadGroups(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Create groups with members
	groups := []*auth.Group{
		{
			Name:      "admins",
			Members:   []string{"alice", "bob"},
			CreatedAt: time.Now().UTC(),
		},
		{
			Name:      "developers",
			Members:   []string{"charlie", "david", "eve"},
			CreatedAt: time.Now().UTC(),
		},
	}

	err = ss.SaveGroups(context.Background(), groups)
	require.NoError(t, err)

	loaded, err := ss.LoadGroups(context.Background())
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	assert.Equal(t, "admins", loaded[0].Name)
	assert.Len(t, loaded[0].Members, 2)
	assert.Contains(t, loaded[0].Members, "alice")
	assert.Contains(t, loaded[0].Members, "bob")
	assert.Equal(t, "developers", loaded[1].Name)
	assert.Len(t, loaded[1].Members, 3)
	assert.Contains(t, loaded[1].Members, "charlie")
}

func TestSystemStoreLoadGroupsNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Should return nil (not error) when not found
	loaded, err := ss.LoadGroups(context.Background())
	require.NoError(t, err)
	assert.Nil(t, loaded)
}

func TestSystemStoreSaveGroupsEmpty(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Save empty group (no members)
	groups := []*auth.Group{
		{
			Name:      "empty-group",
			Members:   []string{},
			CreatedAt: time.Now().UTC(),
		},
	}

	err = ss.SaveGroups(context.Background(), groups)
	require.NoError(t, err)

	loaded, err := ss.LoadGroups(context.Background())
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, "empty-group", loaded[0].Name)
	assert.Len(t, loaded[0].Members, 0)
}

func TestSystemStoreSaveGroupsPersistence(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Create group with member
	groups := []*auth.Group{
		{
			Name:      "everyone",
			Members:   []string{"user1"},
			CreatedAt: time.Now().UTC(),
		},
	}

	err = ss.SaveGroups(context.Background(), groups)
	require.NoError(t, err)

	// Add another member
	groups[0].Members = append(groups[0].Members, "user2")
	err = ss.SaveGroups(context.Background(), groups)
	require.NoError(t, err)

	// Verify both members persisted
	loaded, err := ss.LoadGroups(context.Background())
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Len(t, loaded[0].Members, 2)
	assert.Contains(t, loaded[0].Members, "user1")
	assert.Contains(t, loaded[0].Members, "user2")
}

func TestSystemStoreSaveLoadGroupBindings(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	bindings := []*auth.GroupBinding{
		{Name: "gb1", GroupName: "admins", RoleName: "admin"},
		{Name: "gb2", GroupName: "developers", RoleName: "bucket-read", BucketScope: "dev-bucket"},
		{Name: "gb3", GroupName: "testers", RoleName: "bucket-read", BucketScope: "test-bucket", ObjectPrefix: "data/"},
	}

	err = ss.SaveGroupBindings(context.Background(), bindings)
	require.NoError(t, err)

	loaded, err := ss.LoadGroupBindings(context.Background())
	require.NoError(t, err)
	require.Len(t, loaded, 3)
	assert.Equal(t, "admins", loaded[0].GroupName)
	assert.Equal(t, "admin", loaded[0].RoleName)
	assert.Equal(t, "developers", loaded[1].GroupName)
	assert.Equal(t, "dev-bucket", loaded[1].BucketScope)
	assert.Equal(t, "testers", loaded[2].GroupName)
	assert.Equal(t, "data/", loaded[2].ObjectPrefix)
}

func TestSystemStoreLoadGroupBindingsNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Should return nil (not error) when not found
	loaded, err := ss.LoadGroupBindings(context.Background())
	require.NoError(t, err)
	assert.Nil(t, loaded)
}

func TestSystemStoreSaveGroupBindingsEmpty(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Save empty array
	err = ss.SaveGroupBindings(context.Background(), []*auth.GroupBinding{})
	require.NoError(t, err)

	loaded, err := ss.LoadGroupBindings(context.Background())
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Len(t, loaded, 0)
}

func TestSystemStoreSaveLoadIPAllocations(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	data := IPAllocationsData{
		Used: map[string]bool{
			"10.42.0.10": true,
			"10.42.0.11": true,
			"10.42.0.12": true,
		},
		PeerToIP: map[string]string{
			"alice": "10.42.0.10",
			"bob":   "10.42.0.11",
			"carol": "10.42.0.12",
		},
		Next: 13,
	}

	// Save
	err = ss.SaveIPAllocations(context.Background(), data)
	require.NoError(t, err)

	// Load
	loaded, err := ss.LoadIPAllocations(context.Background())
	require.NoError(t, err)
	require.NotNil(t, loaded)

	// Verify
	assert.Equal(t, data.Used, loaded.Used)
	assert.Equal(t, data.PeerToIP, loaded.PeerToIP)
	assert.Equal(t, data.Next, loaded.Next)
}

func TestSystemStoreLoadIPAllocationsNotFound(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Should return nil (not error) when not found
	loaded, err := ss.LoadIPAllocations(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, loaded) // Returns empty struct with initialized maps
	assert.NotNil(t, loaded.Used)
	assert.NotNil(t, loaded.PeerToIP)
	assert.Len(t, loaded.Used, 0)
	assert.Len(t, loaded.PeerToIP, 0)
}

func TestSystemStoreSaveIPAllocationsEmpty(t *testing.T) {
	store := newTestStoreWithCAS(t)
	ss, err := NewSystemStore(store, "svc:coordinator")
	require.NoError(t, err)

	// Save empty allocation state
	data := IPAllocationsData{
		Used:     make(map[string]bool),
		PeerToIP: make(map[string]string),
		Next:     1,
	}

	err = ss.SaveIPAllocations(context.Background(), data)
	require.NoError(t, err)

	loaded, err := ss.LoadIPAllocations(context.Background())
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Len(t, loaded.Used, 0)
	assert.Len(t, loaded.PeerToIP, 0)
	assert.Equal(t, uint32(1), loaded.Next)
}
