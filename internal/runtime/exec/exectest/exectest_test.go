package exectest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildAndPlace(t *testing.T) {
	t.Parallel()
	bin, err := Build(t.TempDir())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if info, err := os.Stat(bin); err != nil || info.Mode()&0o111 == 0 {
		t.Errorf("binary %s: %v, %v", bin, info, err)
	}
	dst := filepath.Join(t.TempDir(), "actions", "fake", "fakeaction")
	if err := Place(bin, dst); err != nil {
		t.Fatalf("Place: %v", err)
	}
	if info, err := os.Stat(dst); err != nil || info.Mode()&0o111 == 0 {
		t.Errorf("placed binary %s: %v, %v", dst, info, err)
	}
	if err := Place(bin, dst); err != nil { // a file is there: Place replaces it
		t.Fatalf("Place again: %v", err)
	}
	if err := Place(bin, filepath.Join(dst, "under-a-file")); err == nil {
		t.Fatal("Place under a file succeeded")
	}
}
