//go:build e2e

package exectest

import (
	"bytes"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestBuildAndPlace: Build compiles the fake action, Place ships it, and the binary runs the fake action as a
// process (Main): it echoes an item with its own pid.
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
	cmd := osexec.Command(dst) // #nosec G204 -- the binary the test built
	cmd.Stdin = strings.NewReader(`{"type":"hello","protocol":1,"config":{"mode":"echo"}}` + "\n" +
		`{"type":"item","delivery":"dl_1","payload":{}}` + "\n" + `{"type":"shutdown"}` + "\n")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	msgs, types := readAll(bytes.NewReader(stdout.Bytes()))
	if types != "ready emit done" || !strings.Contains(stdout.String(), `"pid":`) || len(msgs) != 3 {
		t.Fatalf("output %s", stdout.String())
	}
}

// TestBuildFor: BuildFor cross-compiles a static binary named after its platform.
func TestBuildFor(t *testing.T) {
	t.Parallel()
	bin, err := BuildFor(t.TempDir(), "linux", runtime.GOARCH)
	if err != nil || filepath.Base(bin) != "fakeaction-linux-"+runtime.GOARCH {
		t.Fatalf("BuildFor = %s, %v", bin, err)
	}
	if _, err := os.Stat(bin); err != nil {
		t.Fatal(err)
	}
}
