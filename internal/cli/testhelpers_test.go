package cli

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
)

// fakeBin is the fake action, built once by TestMain and linked into fixture packages; testDir is TestMain's
// temporary directory, for what tests build once and share.
var fakeBin, testDir string

func TestMain(m *testing.M) {
	// The engine runs unsandboxed agents as its own executable's agent subcommand: under test, this binary.
	if len(os.Args) > 1 && os.Args[1] == "agent" {
		os.Exit(Main(context.Background(), os.Args[1:], StdIO(), "test"))
	}
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	dir, err := os.MkdirTemp("", "cli-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	testDir = dir
	if fakeBin, err = exectest.Build(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

// runPackage copies testdata/<name> to a temporary directory, places the fake action binary at
// actions/fake/fakeaction, and returns the copy's path.
func runPackage(t *testing.T, name string) string {
	t.Helper()
	dst := t.TempDir()
	src := fixture(name)
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		data, err := os.ReadFile(p) // #nosec G304 -- test fixtures
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := exectest.Place(fakeBin, filepath.Join(dst, "actions", "fake", "fakeaction")); err != nil {
		t.Fatal(err)
	}
	return dst
}

// dbPath returns a fresh database path under a temporary directory that does not exist yet.
func dbPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "state", "millstream.db")
}

// runCLICtx runs the CLI under ctx with env as the whole environment.
func runCLICtx(t *testing.T, ctx context.Context, env map[string]string, argv ...string) (code int, stdout,
	stderr string,
) {
	t.Helper()
	var out, errb bytes.Buffer
	code = Run(ctx, argv, IO{In: strings.NewReader(""), Out: &out, Err: &errb}, "dev", func(rt *Runtime) {
		rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	})
	return code, out.String(), errb.String()
}

// mustRun runs the CLI and fails the test unless it exits with want.
func mustRun(t *testing.T, want int, argv ...string) (stdout, stderr string) {
	t.Helper()
	code, out, errOut := runCLICtx(t, context.Background(), nil, argv...)
	if code != want {
		t.Fatalf("millstream %s: code %d, want %d\nstdout:\n%s\nstderr:\n%s", strings.Join(argv, " "), code, want,
			out, errOut)
	}
	return out, errOut
}

// syncBuffer is a buffer safe for a command writing while the test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
