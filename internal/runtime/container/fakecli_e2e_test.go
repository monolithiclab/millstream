//go:build e2e

package container

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container/containertest"
)

// The runtimes of the e2e build run a script as their CLI, as processes; the default build's run it in-process.

// fakeRuntime returns a runtime whose CLI is the fake CLI script of containertest, and the invocations it records.
func fakeRuntime(t *testing.T, images map[string]containertest.Image, tags map[string][]string) (Runtime,
	func() []string,
) {
	t.Helper()
	dir := t.TempDir()
	bin, err := containertest.Write(dir, "docker", images, tags)
	if err != nil {
		t.Fatal(err)
	}
	return Runtime{Name: pipeline.RuntimeDocker, Bin: bin, Env: []string{"PATH=/usr/bin:/bin"}},
		func() []string { return containertest.Calls(dir) }
}

// rawRuntime returns a runtime named name whose CLI prints out, whatever it is asked.
func rawRuntime(t *testing.T, name, out string) Runtime {
	t.Helper()
	bin := filepath.Join(t.TempDir(), name)
	script := "#!/bin/sh\ncat <<'EOF'\n" + out + "\nEOF\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil { // #nosec G306 -- a fake CLI
		t.Fatal(err)
	}
	return Runtime{Name: name, Bin: bin}
}

// missingRuntime returns a runtime whose CLI is not there.
func missingRuntime(t *testing.T) Runtime {
	return Runtime{Name: pipeline.RuntimeDocker, Bin: filepath.Join(t.TempDir(), "absent")}
}
