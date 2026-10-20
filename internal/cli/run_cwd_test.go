//go:build !e2e

package cli

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// Test_runOnceCurrentDir runs testdata/run from its own directory with no directory argument, with Test_runOnce's
// checks: the actions start from absolute paths, since each runs in its action directory, where a path relative to
// the package no longer resolves. Not parallel: it changes the working directory.
func Test_runOnceCurrentDir(t *testing.T) {
	t.Chdir(fixture("run"))
	var mu sync.Mutex
	var progs []string
	launchers := map[string]exec.Launcher{pipeline.KindExec: exec.FuncLauncher{F: func(ctx context.Context,
		args, env []string, stdin io.Reader, stdout, stderr io.Writer,
	) int {
		mu.Lock()
		progs = append(progs, args[0])
		mu.Unlock()
		return serveAction(ctx, args, env, stdin, stdout, stderr)
	}}}
	var out, errb bytes.Buffer
	code := Run(t.Context(), []string{"--db", unusedDB(), "run", "--once", "--request-id", "r1"},
		IO{In: strings.NewReader(""), Out: &out, Err: &errb}, "dev", func(rt *Runtime) {
			rt.Env = func(string) (string, bool) { return "", false }
			rt.Launchers, rt.Store, rt.CurrentUser = launchers, memoryStore(t), noAccount
		})
	if code != 0 || out.String() != runOnceSummary {
		t.Fatalf("run --once: code %d\nstdout:\n%s\nstderr:\n%s", code, out.String(), errb.String())
	}
	checkRunLogs(t, errb.String())
	mu.Lock()
	defer mu.Unlock()
	if len(progs) == 0 {
		t.Fatal("no action started")
	}
	for _, p := range progs {
		if !filepath.IsAbs(p) {
			t.Errorf("action command %q is relative", p)
		}
	}
}
