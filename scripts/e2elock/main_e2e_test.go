//go:build e2e

package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestRun: the command runs under the lock, its exit code passed on.
func TestRun(t *testing.T) {
	var b strings.Builder
	if code := run([]string{"--", "sh", "-c", "exit 3"}, filepath.Join(t.TempDir(), "lock"), &b); code != 3 {
		t.Errorf("exit code %d, stderr %q", code, b.String())
	}
}
