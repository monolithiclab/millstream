//go:build e2e

package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// e2eBuild says whether this is the e2e build: see build_test.go.
const e2eBuild = true

// prepareStates does nothing in the e2e build: its states are run --once databases, made on first use.
func prepareStates() error { return nil }

// newState copies the appliedDB database, with --var scoreMode=error for scoreErrors.
func newState(t *testing.T, scoreErrors bool) *state {
	t.Helper()
	var extra []string
	if scoreErrors {
		extra = []string{"--var", "scoreMode=error"}
	}
	s := &state{t: t}
	s.db, s.pkg = appliedDB(t, extra...)
	return s
}

// appliedDB returns a copy of a database where the run fixture is applied and one run settled by run --once with
// the extra flags, and the package it was applied from, which the test must not modify. The run happens once per
// set of flags and test process: the tests share its package and copy its database.
func appliedDB(t *testing.T, extra ...string) (db, pkg string) {
	t.Helper()
	f, _ := appliedRuns.LoadOrStore(strings.Join(extra, " "), sync.OnceValues(func() (appliedRun, error) {
		return applyRun(extra)
	}))
	run, err := f.(func() (appliedRun, error))()
	if err != nil {
		t.Fatal(err)
	}
	return writeDB(t, run.db), run.pkg
}

// appliedRuns holds a func() (appliedRun, error) per appliedDB flag set.
var appliedRuns sync.Map

// appliedRun is what appliedDB copies: the database after the run, and the package applied.
type appliedRun struct {
	db  []byte
	pkg string
}

// applyRun runs the run fixture once with run --once and the extra flags, under testDir.
func applyRun(extra []string) (appliedRun, error) {
	dir, err := os.MkdirTemp(testDir, "applied-")
	if err != nil {
		return appliedRun{}, err
	}
	pkg, db := filepath.Join(dir, "pkg"), filepath.Join(dir, "state", "millstream.db")
	if err := copyPackage(pkg, "run"); err != nil {
		return appliedRun{}, err
	}
	var errOut bytes.Buffer
	argv := append(append([]string{"--db", db, "run", "--once", "--request-id", "r1"}, extra...), pkg)
	code := Run(context.Background(), argv, IO{In: strings.NewReader(""), Out: io.Discard, Err: &errOut}, "dev",
		func(rt *Runtime) {
			rt.Env = func(string) (string, bool) { return "", false }
			rt.Launchers = testLaunchers()
		})
	if code > 1 {
		return appliedRun{}, fmt.Errorf("run: code %d\n%s", code, errOut.String())
	}
	data, err := readDB(db)
	return appliedRun{db: data, pkg: pkg}, err
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
