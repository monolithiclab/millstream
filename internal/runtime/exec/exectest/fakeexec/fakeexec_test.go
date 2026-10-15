//go:build !e2e

package fakeexec

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// TestDefault: the default build runs exec actions in-process, places an executable stub (replacing a file already
// there), and names a placeholder command.
func TestDefault(t *testing.T) {
	t.Parallel()
	if _, ok := Launchers()[pipeline.KindExec].(exec.FuncLauncher); !ok {
		t.Fatalf("Launchers = %v, want an in-process exec launcher", Launchers())
	}
	dst := filepath.Join(t.TempDir(), "actions", "echo", "fakeaction")
	for range 2 {
		if err := Place(dst); err != nil {
			t.Fatalf("Place: %v", err)
		}
	}
	if info, err := os.Stat(dst); err != nil || info.Mode()&0o111 == 0 {
		t.Fatalf("stub %s: %v, %v", dst, info, err)
	}
	if err := Place(filepath.Join(dst, "under-a-file")); err == nil {
		t.Fatal("Place under a file succeeded")
	}
	if cmd, err := Command(); err != nil || cmd == "" {
		t.Fatalf("Command = %q, %v", cmd, err)
	}
	Cleanup()
}
