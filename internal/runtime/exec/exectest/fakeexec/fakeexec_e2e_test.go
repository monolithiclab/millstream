//go:build e2e

package fakeexec

import (
	"os"
	"path/filepath"
	"testing"
)

// TestE2E: the e2e build runs actions as processes (the runtime's own launchers), places the fake action binary,
// built once, and names it as the command.
func TestE2E(t *testing.T) {
	if l := Launchers(); l != nil {
		t.Fatalf("Launchers = %v, want nil", l)
	}
	bin, err := Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "fakeaction")
	if err := Place(dst); err != nil {
		t.Fatalf("Place: %v", err)
	}
	built, err := os.Stat(bin)
	if err != nil {
		t.Fatal(err)
	}
	placed, err := os.Stat(dst)
	if err != nil || placed.Size() != built.Size() || placed.Mode()&0o111 == 0 {
		t.Fatalf("placed %v, %v; built %v", placed, err, built)
	}
	Cleanup()
	if _, err := os.Stat(bin); !os.IsNotExist(err) {
		t.Fatalf("after Cleanup: %v", err)
	}
}
