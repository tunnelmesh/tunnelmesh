// Package mesh provides constants and helpers for TunnelMesh domain suffixes.
//
// TunnelMesh peers resolve names under several suffixes (.tunnelmesh, .tm, .mesh)
// within the mesh network. Use IsValidSuffix to accept client-provided suffixes
// and AllSuffixes when enumerating supported values.
//
// Example:
//
//	if mesh.IsValidSuffix(".mesh") {
//	    suffixes := mesh.AllSuffixes()
//	}
package mesh
