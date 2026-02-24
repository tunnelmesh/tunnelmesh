// Package s3 provides S3-compatible storage for the coordinator.
package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/auth"
)

// SystemBucket is an alias for auth.SystemBucket for convenience.
const SystemBucket = auth.SystemBucket

// SystemStore provides high-level access to the _tunnelmesh system bucket.
// This is used by the coordinator to store internal state like users, roles,
// bindings, and stats history.
type SystemStore struct {
	store *Store
	owner string // Service user ID that owns the system bucket
}

// NewSystemStore creates a new system store.
// It creates the _tunnelmesh bucket if it doesn't exist.
func NewSystemStore(store *Store, serviceUserID string) (*SystemStore, error) {
	ss := &SystemStore{
		store: store,
		owner: serviceUserID,
	}

	// Create system bucket if it doesn't exist
	if _, err := store.HeadBucket(context.Background(), SystemBucket); errors.Is(err, ErrBucketNotFound) {
		if err := store.CreateBucket(context.Background(), SystemBucket, serviceUserID, 3, nil); err != nil {
			return nil, fmt.Errorf("create system bucket: %w", err)
		}
	}

	return ss, nil
}

// Auth paths
const (
	PeersPath         = "auth/peers.json"
	RolesPath         = "auth/roles.json"
	BindingsPath      = "auth/bindings.json"
	GroupsPath        = "auth/groups.json"
	GroupBindingsPath = "auth/group_bindings.json"
	FileSharesPath    = "auth/file_shares.json"
	PanelsPath        = "auth/panels.json"
)

// DNS paths
const (
	DNSCachePath = "dns/cache.json"
	DNSAliasPath = "dns/aliases.json"
)

// Filter paths
const (
	FilterRulesPath = "filter/rules.json"
)

// FilterRulePersisted represents a filter rule for persistence.
type FilterRulePersisted struct {
	Port       uint16 `json:"port"`
	Protocol   string `json:"protocol"`    // "tcp" or "udp"
	Action     string `json:"action"`      // "allow" or "deny"
	Expires    int64  `json:"expires"`     // Unix timestamp (0 = no expiry)
	SourcePeer string `json:"source_peer"` // Empty = any peer
}

// FilterRulesData stores temporary filter rules.
// Service rules are not persisted as they always come from coordinator config.
type FilterRulesData struct {
	Temporary []FilterRulePersisted `json:"temporary"`
}

// IP Allocator paths
const (
	IPAllocationsPath  = "system/ip_allocations.json"
	CoordinatorIPsPath = "system/coordinator_ips.json"
)

// IPAllocationsData stores IP allocator state for persistence.
type IPAllocationsData struct {
	Used     map[string]bool   `json:"used"`       // IP -> allocated
	PeerToIP map[string]string `json:"peer_to_ip"` // Peer name -> IP
	Next     uint32            `json:"next"`       // Sequential counter (for reference)
}

// --- Peers ---

// SavePeers saves the peer list to S3 with checksum validation.
func (ss *SystemStore) SavePeers(ctx context.Context, peers []*auth.Peer) error {
	return ss.saveJSONWithChecksum(ctx, PeersPath, peers)
}

// LoadPeers loads the peer list from S3 with automatic rollback on corruption.
func (ss *SystemStore) LoadPeers(ctx context.Context) ([]*auth.Peer, error) {
	var peers []*auth.Peer
	if err := ss.loadJSONWithChecksum(ctx, PeersPath, &peers, 3); err != nil {
		return nil, err
	}
	return peers, nil
}

// --- Roles ---

// SaveRoles saves custom roles to S3 with checksum validation.
func (ss *SystemStore) SaveRoles(ctx context.Context, roles []auth.Role) error {
	return ss.saveJSONWithChecksum(ctx, RolesPath, roles)
}

// LoadRoles loads custom roles from S3 with automatic rollback on corruption.
func (ss *SystemStore) LoadRoles(ctx context.Context) ([]auth.Role, error) {
	var roles []auth.Role
	if err := ss.loadJSONWithChecksum(ctx, RolesPath, &roles, 3); err != nil {
		return nil, err
	}
	return roles, nil
}

// --- Role Bindings ---

// SaveBindings saves role bindings to S3 with checksum validation.
func (ss *SystemStore) SaveBindings(ctx context.Context, bindings []*auth.RoleBinding) error {
	return ss.saveJSONWithChecksum(ctx, BindingsPath, bindings)
}

// LoadBindings loads role bindings from S3 with automatic rollback on corruption.
func (ss *SystemStore) LoadBindings(ctx context.Context) ([]*auth.RoleBinding, error) {
	var bindings []*auth.RoleBinding
	if err := ss.loadJSONWithChecksum(ctx, BindingsPath, &bindings, 3); err != nil {
		return nil, err
	}
	return bindings, nil
}

// --- Groups ---

// SaveGroups saves groups to S3 with checksum validation.
func (ss *SystemStore) SaveGroups(ctx context.Context, groups []*auth.Group) error {
	return ss.saveJSONWithChecksum(ctx, GroupsPath, groups)
}

// LoadGroups loads groups from S3 with automatic rollback on corruption.
func (ss *SystemStore) LoadGroups(ctx context.Context) ([]*auth.Group, error) {
	var groups []*auth.Group
	if err := ss.loadJSONWithChecksum(ctx, GroupsPath, &groups, 3); err != nil {
		return nil, err
	}
	return groups, nil
}

// --- Group Bindings ---

// SaveGroupBindings saves group bindings to S3 with checksum validation.
func (ss *SystemStore) SaveGroupBindings(ctx context.Context, bindings []*auth.GroupBinding) error {
	return ss.saveJSONWithChecksum(ctx, GroupBindingsPath, bindings)
}

// LoadGroupBindings loads group bindings from S3 with automatic rollback on corruption.
func (ss *SystemStore) LoadGroupBindings(ctx context.Context) ([]*auth.GroupBinding, error) {
	var bindings []*auth.GroupBinding
	if err := ss.loadJSONWithChecksum(ctx, GroupBindingsPath, &bindings, 3); err != nil {
		return nil, err
	}
	return bindings, nil
}

// --- External Panels ---

// SavePanels saves external panel definitions to S3 with checksum validation.
// Only external (non-builtin) panels are persisted.
func (ss *SystemStore) SavePanels(ctx context.Context, panels []*auth.PanelDefinition) error {
	return ss.saveJSONWithChecksum(ctx, PanelsPath, panels)
}

// LoadPanels loads external panel definitions from S3 with automatic rollback on corruption.
func (ss *SystemStore) LoadPanels(ctx context.Context) ([]*auth.PanelDefinition, error) {
	var panels []*auth.PanelDefinition
	if err := ss.loadJSONWithChecksum(ctx, PanelsPath, &panels, 3); err != nil {
		return nil, err
	}
	return panels, nil
}

// --- File Shares ---

// FileShare represents a file sharing configuration backed by an S3 bucket.
type FileShare struct {
	Name              string    `json:"name"`                         // Share name (bucket will be "fs+{name}")
	Description       string    `json:"description"`                  // Human-readable description
	Owner             string    `json:"owner"`                        // PeerID of creator
	CreatedAt         time.Time `json:"created_at"`                   //
	ExpiresAt         time.Time `json:"expires_at,omitempty"`         // When the share expires (0 = never)
	QuotaBytes        int64     `json:"quota_bytes,omitempty"`        // Per-share quota in bytes (0 = unlimited within global quota)
	GuestRead         bool      `json:"guest_read"`                   // Allow all mesh peers to read (default: true)
	ReplicationFactor int       `json:"replication_factor,omitempty"` // Bucket replication factor (0 treated as 2)
}

// IsExpired returns true if the share has expired.
func (s *FileShare) IsExpired() bool {
	if s.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(s.ExpiresAt)
}

// FileShareBucketPrefix is the prefix for file share bucket names.
const FileShareBucketPrefix = "fs+"

// SaveFileShares saves file shares to S3 with checksum validation.
func (ss *SystemStore) SaveFileShares(ctx context.Context, shares []*FileShare) error {
	return ss.saveJSONWithChecksum(ctx, FileSharesPath, shares)
}

// LoadFileShares loads file shares from S3 with automatic rollback on corruption.
func (ss *SystemStore) LoadFileShares(ctx context.Context) ([]*FileShare, error) {
	var shares []*FileShare
	if err := ss.loadJSONWithChecksum(ctx, FileSharesPath, &shares, 3); err != nil {
		return nil, err
	}
	return shares, nil
}

// SaveJSON saves arbitrary JSON data to a specified path in the system bucket with checksum validation.
// This is a generic method for saving any stats or data to custom paths like "stats/{peer}.docker.json".
func (ss *SystemStore) SaveJSON(ctx context.Context, path string, data interface{}) error {
	return ss.saveJSONWithChecksum(ctx, path, data)
}

// LoadJSON loads arbitrary JSON data from a specified path in the system bucket with automatic rollback on corruption.
func (ss *SystemStore) LoadJSON(ctx context.Context, path string, target interface{}) error {
	return ss.loadJSONWithChecksum(ctx, path, target, 3)
}

// --- DNS Cache ---

// DNSCacheEntry represents a DNS cache entry with timestamp.
type DNSCacheEntry struct {
	Hostname  string    `json:"hostname"`
	MeshIP    string    `json:"mesh_ip"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SaveDNSCache saves the DNS cache to S3.
func (ss *SystemStore) SaveDNSCache(ctx context.Context, cache map[string]string) error {
	entries := make([]DNSCacheEntry, 0, len(cache))
	now := time.Now()
	for hostname, meshIP := range cache {
		entries = append(entries, DNSCacheEntry{
			Hostname:  hostname,
			MeshIP:    meshIP,
			UpdatedAt: now,
		})
	}
	return ss.saveJSONWithChecksum(ctx, DNSCachePath, entries)
}

// LoadDNSCache loads the DNS cache from S3 with TTL validation.
// Entries older than 7 days are filtered out.
func (ss *SystemStore) LoadDNSCache(ctx context.Context) (map[string]string, error) {
	var entries []DNSCacheEntry
	if err := ss.loadJSONWithChecksum(ctx, DNSCachePath, &entries, 3); err != nil {
		return nil, err
	}

	cache := make(map[string]string, len(entries))
	cutoff := time.Now().Add(-7 * 24 * time.Hour) // 7-day TTL

	for _, entry := range entries {
		// Skip stale entries
		if entry.UpdatedAt.Before(cutoff) {
			log.Debug().Str("hostname", entry.Hostname).
				Time("updated_at", entry.UpdatedAt).
				Msg("skipping stale DNS cache entry")
			continue
		}
		cache[entry.Hostname] = entry.MeshIP
	}

	return cache, nil
}

// --- DNS Aliases ---

// DNSAliasEntry represents a peer's DNS aliases.
type DNSAliasEntry struct {
	PeerName string   `json:"peer_name"`
	Aliases  []string `json:"aliases"`
}

// SaveDNSAliases saves peer DNS aliases to S3.
// aliasOwner maps alias -> peer name
// peerAliases maps peer name -> aliases list
func (ss *SystemStore) SaveDNSAliases(ctx context.Context, aliasOwner map[string]string, peerAliases map[string][]string) error {
	// Store as peer-centric data (peer -> aliases list)
	entries := make([]DNSAliasEntry, 0, len(peerAliases))
	for peerName, aliases := range peerAliases {
		if len(aliases) > 0 {
			entries = append(entries, DNSAliasEntry{
				PeerName: peerName,
				Aliases:  aliases,
			})
		}
	}
	return ss.saveJSONWithChecksum(ctx, DNSAliasPath, entries)
}

// LoadDNSAliases loads peer DNS aliases from S3.
// Returns both the peer->aliases map and the alias->peer reverse map.
func (ss *SystemStore) LoadDNSAliases(ctx context.Context) (peerAliases map[string][]string, aliasOwner map[string]string, err error) {
	var entries []DNSAliasEntry
	if err := ss.loadJSONWithChecksum(ctx, DNSAliasPath, &entries, 3); err != nil {
		return nil, nil, err
	}

	peerAliases = make(map[string][]string)
	aliasOwner = make(map[string]string)

	for _, entry := range entries {
		peerAliases[entry.PeerName] = entry.Aliases
		for _, alias := range entry.Aliases {
			aliasOwner[alias] = entry.PeerName
		}
	}

	return peerAliases, aliasOwner, nil
}

// --- Filter Rules ---

// SaveFilterRules saves filter rules to S3 with checksum validation.
func (ss *SystemStore) SaveFilterRules(ctx context.Context, data FilterRulesData) error {
	return ss.saveJSONWithChecksum(ctx, FilterRulesPath, data)
}

// LoadFilterRules loads filter rules from S3 with automatic rollback on corruption.
func (ss *SystemStore) LoadFilterRules(ctx context.Context) (*FilterRulesData, error) {
	var data FilterRulesData
	if err := ss.loadJSONWithChecksum(ctx, FilterRulesPath, &data, 3); err != nil {
		return nil, err
	}
	return &data, nil
}

// --- IP Allocations ---

// SaveIPAllocations saves IP allocator state to S3 with checksum validation.
func (ss *SystemStore) SaveIPAllocations(ctx context.Context, data IPAllocationsData) error {
	return ss.saveJSONWithChecksum(ctx, IPAllocationsPath, data)
}

// LoadIPAllocations loads IP allocator state from S3 with automatic rollback on corruption.
// Returns nil if the object doesn't exist (first coordinator startup).
func (ss *SystemStore) LoadIPAllocations(ctx context.Context) (*IPAllocationsData, error) {
	var data IPAllocationsData
	if err := ss.loadJSONWithChecksum(ctx, IPAllocationsPath, &data, 3); err != nil {
		return nil, err
	}

	// Initialize maps if they're nil (happens on first load or empty state)
	if data.Used == nil {
		data.Used = make(map[string]bool)
	}
	if data.PeerToIP == nil {
		data.PeerToIP = make(map[string]string)
	}

	return &data, nil
}

// --- Coordinator IPs ---

// SaveCoordinatorIPs persists the list of coordinator mesh IPs to the system bucket.
// This is replicated to all coordinators, giving them an authoritative list
// of all coordinator IPs regardless of registration order.
func (ss *SystemStore) SaveCoordinatorIPs(ctx context.Context, ips []string) error {
	return ss.saveJSONWithChecksum(ctx, CoordinatorIPsPath, ips)
}

// LoadCoordinatorIPs loads the coordinator IP list from the system bucket.
func (ss *SystemStore) LoadCoordinatorIPs(ctx context.Context) ([]string, error) {
	var ips []string
	if err := ss.loadJSONWithChecksum(ctx, CoordinatorIPsPath, &ips, 3); err != nil {
		return nil, err
	}
	return ips, nil
}

// --- Checksum-validated JSON helpers ---

// MetadataWrapper adds checksum validation to system metadata.
// This protects against corruption and enables automatic rollback to previous versions.
// The entire structure is stored as compact JSON (for checksum consistency).
// The Data field contains the actual payload as embedded JSON (not base64).
type MetadataWrapper struct {
	Version  int             `json:"version"`  // Schema version (for future migrations)
	Checksum string          `json:"checksum"` // SHA-256 of Data bytes
	Data     json.RawMessage `json:"data"`     // JSON payload (embedded, not base64)
}

// saveJSONWithChecksum saves JSON with a checksum wrapper for corruption detection.
func (ss *SystemStore) saveJSONWithChecksum(ctx context.Context, key string, v interface{}) error {
	// Marshal payload (compact for checksum consistency)
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}

	// Compute checksum on compact JSON bytes
	hash := sha256.Sum256(data)
	checksum := hex.EncodeToString(hash[:])

	// Wrap with metadata (store as RawMessage, not base64)
	wrapper := MetadataWrapper{
		Version:  1,
		Checksum: checksum,
		Data:     json.RawMessage(data),
	}

	// Marshal wrapper (compact to ensure checksum consistency)
	// Note: We use compact JSON to avoid whitespace variations that could
	// break checksum validation. The embedded data is still readable JSON.
	wrappedData, err := json.Marshal(wrapper)
	if err != nil {
		return fmt.Errorf("marshal wrapper: %w", err)
	}

	_, err = ss.store.PutObject(ctx, SystemBucket, key, bytes.NewReader(wrappedData),
		int64(len(wrappedData)), "application/json", nil)
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}

	return nil
}

// loadJSONWithChecksum loads and validates JSON with automatic rollback on corruption.
// If the current version is corrupted, it tries up to maxRetries previous versions.
// Returns nil if the object doesn't exist (same as loadJSON for backward compatibility).
func (ss *SystemStore) loadJSONWithChecksum(ctx context.Context, key string, target interface{}, maxRetries int) error {
	// Try loading current version first
	if err := ss.tryLoadVersion(ctx, key, "", target); err == nil {
		return nil // Success
	} else if errors.Is(err, ErrObjectNotFound) {
		return nil // Not found is OK - return nil with empty target
	} else {
		log.Warn().Err(err).Str("key", key).Msg("failed to load current version")
	}

	// Current version failed - try previous versions
	versions, err := ss.store.ListVersions(ctx, SystemBucket, key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrBucketNotFound) {
			return nil // No versions exist - return nil
		}
		return fmt.Errorf("list versions for %s: %w", key, err)
	}

	// Try up to maxRetries previous versions
	attempts := 0
	for _, ver := range versions {
		if attempts >= maxRetries {
			break
		}

		log.Info().Str("key", key).Str("version", ver.VersionID).Msg("attempting rollback")
		if err := ss.tryLoadVersion(ctx, key, ver.VersionID, target); err == nil {
			// Success - restore this version as current
			log.Warn().Str("key", key).Str("version", ver.VersionID).
				Msg("recovered from corrupted data using previous version")
			if _, err := ss.store.RestoreVersion(ctx, SystemBucket, key, ver.VersionID); err != nil {
				log.Error().Err(err).Msg("failed to restore recovered version")
			}
			return nil
		}

		attempts++
	}

	return fmt.Errorf("exhausted %d versions, all corrupted", attempts)
}

// tryLoadVersion attempts to load and validate a specific version.
func (ss *SystemStore) tryLoadVersion(ctx context.Context, key, versionID string, target interface{}) error {
	// Get object (specific version if provided)
	var reader io.ReadCloser
	var err error

	if versionID == "" {
		reader, _, err = ss.store.GetObject(ctx, SystemBucket, key)
	} else {
		reader, _, err = ss.store.GetObjectVersion(ctx, SystemBucket, key, versionID)
	}

	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Str("key", key).Msg("failed to close reader")
		}
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	// Try unwrapping checksum wrapper
	var wrapper MetadataWrapper
	if err := json.Unmarshal(data, &wrapper); err == nil && wrapper.Checksum != "" {
		// Has checksum - validate
		hash := sha256.Sum256(wrapper.Data)
		expectedChecksum := hex.EncodeToString(hash[:])

		if wrapper.Checksum != expectedChecksum {
			return fmt.Errorf("checksum mismatch: expected %s, got %s",
				expectedChecksum, wrapper.Checksum)
		}

		// Checksum valid - unmarshal payload
		if err := json.Unmarshal(wrapper.Data, target); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}

		return nil
	}

	return fmt.Errorf("invalid format: missing checksum wrapper")
}

// Exists checks if an object exists in the system bucket.
func (ss *SystemStore) Exists(ctx context.Context, key string) bool {
	_, err := ss.store.HeadObject(ctx, SystemBucket, key)
	return err == nil
}

// Delete permanently removes an object from the system bucket.
// Unlike user data, system configuration is not tombstoned.
func (ss *SystemStore) Delete(ctx context.Context, key string) error {
	return ss.store.PurgeObject(ctx, SystemBucket, key)
}

// Raw returns the underlying store for advanced operations.
func (ss *SystemStore) Raw() *Store {
	return ss.store
}
