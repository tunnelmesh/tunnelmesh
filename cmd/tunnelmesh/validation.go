package main

import (
	"fmt"
	"regexp"
	"strings"
)

// dnsCompliantNameRegex matches S3-style bucket/share names: lowercase, digits, hyphens only,
// no leading/trailing/consecutive hyphens. Used by validateBucketOrShareName.
var dnsCompliantNameRegex = regexp.MustCompile(`^[a-z0-9]([a-z0-9]*-?)*[a-z0-9]$`)

// validateBucketOrShareName checks that a bucket or share name is valid for S3.
// Names must be DNS-compliant: lowercase letters, numbers, hyphens only,
// 3-63 characters, no consecutive or leading/trailing hyphens.
// Returns a user-friendly error describing the rules when invalid.
func validateBucketOrShareName(name string) error {
	const minLen, maxLen = 3, 63
	if name == "" {
		return fmt.Errorf("name cannot be empty - use 3 to 63 characters (lowercase letters, numbers, hyphens)")
	}
	if len(name) < minLen {
		return fmt.Errorf("name %q is too short - must be between 3 and 63 characters (DNS-compliant, lowercase)", name)
	}
	if len(name) > maxLen {
		return fmt.Errorf("name %q is too long - must be between 3 and 63 characters (DNS-compliant, lowercase)", name)
	}
	if !dnsCompliantNameRegex.MatchString(name) {
		for _, r := range name {
			if r != '-' && (r < 'a' || r > 'z') && (r < '0' || r > '9') {
				return fmt.Errorf("invalid character %q in name %q - bucket/share names must be DNS-compliant: only lowercase letters, numbers, and hyphens (e.g. my-bucket, docs)", r, name)
			}
		}
		if strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") {
			return fmt.Errorf("name %q cannot start or end with a hyphen - must be DNS-compliant (e.g. my-bucket)", name)
		}
		if strings.Contains(name, "--") {
			return fmt.Errorf("name %q cannot contain consecutive hyphens - must be DNS-compliant (e.g. my-bucket)", name)
		}
		return fmt.Errorf("invalid name %q - must be DNS-compliant: 3-63 characters, lowercase letters, numbers, and hyphens only", name)
	}
	return nil
}
