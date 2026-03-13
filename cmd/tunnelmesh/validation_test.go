package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateBucketOrShareName(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		errSubstr string // substring that must appear in error when wantErr is true
	}{
		// Valid names
		{name: "valid short", input: "abc", wantErr: false},
		{name: "valid with hyphen", input: "my-bucket", wantErr: false},
		{name: "valid with digits", input: "bucket2", wantErr: false},
		{name: "valid mixed", input: "docs-2024", wantErr: false},
		{name: "valid max length", input: strings.Repeat("a", 63), wantErr: false},
		{name: "valid two segments", input: "a-b", wantErr: false},

		// Empty and length
		{name: "empty", input: "", wantErr: true, errSubstr: "cannot be empty"},
		{name: "too short one char", input: "a", wantErr: true, errSubstr: "too short"},
		{name: "too short two chars", input: "ab", wantErr: true, errSubstr: "too short"},
		{name: "too long", input: strings.Repeat("a", 64), wantErr: true, errSubstr: "too long"},

		// Invalid characters
		{name: "uppercase", input: "MyBucket", wantErr: true, errSubstr: "invalid character"},
		{name: "underscore", input: "my_bucket", wantErr: true, errSubstr: "invalid character"},
		{name: "space", input: "my bucket", wantErr: true, errSubstr: "invalid character"},
		{name: "dot", input: "my.bucket", wantErr: true, errSubstr: "invalid character"},
		{name: "special", input: "bucket!", wantErr: true, errSubstr: "invalid character"},

		// Hyphen rules
		{name: "leading hyphen", input: "-bucket", wantErr: true, errSubstr: "cannot start or end with a hyphen"},
		{name: "trailing hyphen", input: "bucket-", wantErr: true, errSubstr: "cannot start or end with a hyphen"},
		{name: "consecutive hyphens", input: "my--bucket", wantErr: true, errSubstr: "consecutive hyphens"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBucketOrShareName(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.errSubstr, "error message should contain %q", tt.errSubstr)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
