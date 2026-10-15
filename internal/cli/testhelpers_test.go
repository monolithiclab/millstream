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

	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest/fakeexec"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// testDir is TestMain's temporary directory, for what tests build once and share.
var testDir string

func TestMain(m *testing.M) {
	// The engine runs unsandboxed agents as its own executable's agent subcommand: in the e2e build, which starts real
	// processes, this binary (the default build serves them in-process, see testLaunchers).
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
	defer fakeexec.Cleanup()
	testDir = dir
	return m.Run()
}

// runPackage copies testdata/<name> to a temporary directory, places the fake action at actions/fake/fakeaction (a
// stub in the default build, which runs it in-process, the built binary in the e2e build), and returns the copy's
// path.
func runPackage(t *testing.T, name string) string {
	t.Helper()
	dst := t.TempDir()
	if err := copyPackage(dst, name); err != nil {
		t.Fatal(err)
	}
	return dst
}

// copyPackage copies testdata/<name> to dst and places the fake action at actions/fake/fakeaction.
func copyPackage(dst, name string) error {
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
		return err
	}
	return fakeexec.Place(filepath.Join(dst, "actions", "fake", "fakeaction"))
}

// dbPath returns a fresh database path under a temporary directory that does not exist yet.
func dbPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "state", "millstream.db")
}

// migrated is a migrated state database's contents, built once: migratedDB copies it rather than have each test's
// first command migrate a fresh database.
var migrated = sync.OnceValues(func() ([]byte, error) {
	dir, err := os.MkdirTemp("", "cli-db-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "millstream.db")
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		return nil, err
	}
	if err := st.Close(); err != nil {
		return nil, err
	}
	return readDB(path)
})

// readDB reads the closed state database at path: closing the last connection checkpoints the write-ahead log into
// the database file and removes it, so the file is the whole database.
func readDB(path string) ([]byte, error) {
	if _, err := os.Stat(path + "-wal"); !os.IsNotExist(err) {
		return nil, fmt.Errorf("the write-ahead log outlived the store: %v", err)
	}
	return os.ReadFile(path) // #nosec G304 -- our own temporary file
}

// migratedDB returns the path of a fresh, already migrated state database under a temporary directory. Tests that do
// not test the database's creation use it: Test_runOnce starts from a path that does not exist.
func migratedDB(t *testing.T) string {
	t.Helper()
	data, err := migrated()
	if err != nil {
		t.Fatal(err)
	}
	return writeDB(t, data)
}

// writeDB writes data as the state database at a fresh dbPath and returns the path.
func writeDB(t *testing.T, data []byte) string {
	t.Helper()
	path := dbPath(t)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// runCLICtx runs the CLI under ctx with env as the whole environment.
func runCLICtx(t *testing.T, ctx context.Context, env map[string]string, argv ...string) (code int, stdout,
	stderr string,
) {
	t.Helper()
	var out, errb bytes.Buffer
	code = Run(ctx, argv, IO{In: strings.NewReader(""), Out: &out, Err: &errb}, "dev", func(rt *Runtime) {
		rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
		rt.Launchers = testLaunchers()
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

// watchBuffer is a syncBuffer that closes Done once what was written satisfies cond: tests wait on a command's
// output with it instead of polling.
type watchBuffer struct {
	syncBuffer
	cond func(string) bool
	Done chan struct{}
	once sync.Once
}

func newWatchBuffer(cond func(string) bool) *watchBuffer {
	return &watchBuffer{cond: cond, Done: make(chan struct{})}
}

func (b *watchBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	if b.cond(b.buf.String()) {
		b.once.Do(func() { close(b.Done) })
	}
	return n, err
}
