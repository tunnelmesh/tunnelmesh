package coord

import "testing"

func TestIsValidDockerID(t *testing.T) {
	tests := []struct {
		name  string
		id    string
		valid bool
	}{
		{"valid 12-char short form", "abc123def456", true},
		{"valid 64-char full form", "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", true},
		{"empty string", "", false},
		{"path traversal", "../../../etc/passwd", false},
		{"non-hex chars", "xyz123abc456", false},
		{"too short (11 chars)", "abc123def45", false},
		{"too long (13 chars)", "abc123def4567", false},
		{"uppercase hex rejected", "ABC123DEF456", false},
		{"mixed case", "Abc123def456", false},
		{"slash in id", "abc123/ef456", false},
		{"null byte", "abc123def45\x00", false},
		{"space in id", "abc123def 56", false},
		{"63 chars (not 12 or 64)", "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidDockerID(tt.id)
			if got != tt.valid {
				t.Errorf("isValidDockerID(%q) = %v, want %v", tt.id, got, tt.valid)
			}
		})
	}
}
