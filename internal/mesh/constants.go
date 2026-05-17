package mesh

const (
	// DomainSuffix is the canonical .tunnelmesh domain for mesh nodes.
	DomainSuffix = ".tunnelmesh" // Canonical domain suffix
	// AliasTM is a short alias for .tunnelmesh domain.

	AliasTM   = ".tm"   // Short alias
	AliasMesh = ".mesh" // Alternative alias

	// Network configuration
	CIDR = "10.42.0.0/16" // Mesh network CIDR - all peers get IPs from this range
)

// AllSuffixes returns all supported domain suffixes (canonical first).
func AllSuffixes() []string {
	return []string{DomainSuffix, AliasTM, AliasMesh}
}

// IsValidSuffix returns true if the suffix is a recognized mesh domain suffix.
func IsValidSuffix(suffix string) bool {
	return suffix == DomainSuffix || suffix == AliasTM || suffix == AliasMesh
}
