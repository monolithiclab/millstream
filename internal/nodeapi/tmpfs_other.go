//go:build !linux

package nodeapi

// isTmpfs reports false off Linux: the working directories go to the system's temporary directory.
func isTmpfs(string) bool { return false }
