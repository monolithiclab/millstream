package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"

	"github.com/monolithiclab/millstream/internal/testdir"
)

func TestOpenFileAndReopen(t *testing.T) {
	noFsync(t)
	path := filepath.Join(testdir.New(t), "m.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	apply(t, s)
	// The database clock stamps the revision when no clock is injected.
	revs, err := s.Revision(ctx, 1)
	if err != nil {
		t.Fatalf("Revision: %v", err)
	}
	if d := time.Since(revs.AppliedAt); d < -time.Minute || d > time.Minute {
		t.Errorf("applied at %v, want about now", revs.AppliedAt)
	}
	// The read pool keeps every connection it may open: one closed when released is reopened by the next read,
	// parsing the schema again.
	var conns []*sql.Conn
	for range s.opts.ReadConns {
		c, err := s.read.Conn(ctx)
		if err != nil {
			t.Fatalf("Conn: %v", err)
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		_ = c.Close()
	}
	if st := s.read.Stats(); st.MaxIdleClosed != 0 || st.Idle != s.opts.ReadConns {
		t.Errorf("read pool after %d reads at once: %d idle, %d closed", s.opts.ReadConns, st.Idle, st.MaxIdleClosed)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	s, err = Open(path, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s.Close() }()
	var versions int
	if err := s.QueryRow(ctx, `SELECT COUNT(*) FROM schema_version`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, "schema versions", versions, len(files)) // each migration applied once
	if id, err := s.ActiveRevision(ctx, "listings"); err != nil || id != 1 {
		t.Fatalf("active revision after reopen = %d, %v", id, err)
	}
	var mode string
	if err := s.QueryRow(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	wantState(t, "journal mode", mode, "wal")
	// The read pool is query-only.
	if _, err := s.read.Exec(`INSERT INTO events (seq, ts, pipeline, run_id, kind, data) VALUES (1, '', '', '', '', '')`); err == nil {
		t.Fatal("a write through the read pool succeeded")
	}
}

// A clone holds the database as committed when it was taken, and goes its own way from there.
func TestClone(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	fire(t, s, "r1")
	c, err := s.Clone(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	fire(t, s, "r2")
	if _, err := c.Run(ctx, "listings", "r1"); err != nil {
		t.Fatalf("the clone lacks r1: %v", err)
	}
	if _, err := c.Run(ctx, "listings", "r2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the clone has r2: %v", err)
	}
	fire(t, c, "r3")
	if _, err := s.Run(ctx, "listings", "r3"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the clone's run reached the original: %v", err)
	}
}

func TestOpenErrors(t *testing.T) {
	if _, err := Open(filepath.Join(testdir.New(t), "missing", "dir", "m.db"), Options{}); err == nil {
		t.Fatal("Open in a missing directory succeeded")
	}
}

// A new store's image appears whole or not at all: a stray temporary file from a crash, two first opens racing, and
// a write that fails each leave either the complete image at path or nothing.
func Test_createFromImage(t *testing.T) {
	syncs := noFsync(t)
	dir := testdir.New(t)
	path := filepath.Join(dir, "m.db")
	wantImage := func(t *testing.T) {
		t.Helper()
		got, err := os.ReadFile(path) // #nosec G304 -- the test's own file
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, templateImage) {
			t.Fatalf("the store holds %d bytes, want the %d-byte image", len(got), len(templateImage))
		}
		if tmps, _ := filepath.Glob(filepath.Join(dir, ".m.db.new-*")); len(tmps) != 1 { // the stray one alone
			t.Fatalf("temporary files left: %v", tmps)
		}
	}

	if err := os.WriteFile(filepath.Join(dir, ".m.db.new-stray"), []byte("half an ima"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := createFromImage(path, errReader{}); err == nil {
		t.Fatal("a failed write succeeded")
	}
	if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a failed write left a file at the store's path: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Go(func() { errs[i] = createFromImage(path, bytes.NewReader(templateImage)) })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	wantImage(t)
	if n := syncs.Load(); n < 2 { // the image, then the directory, per opener that wrote one
		t.Errorf("%d syncs, want at least 2", n)
	}
	// An existing database is left as it is.
	if err := createFromImage(path, errReader{}); err != nil {
		t.Fatal(err)
	}
	wantImage(t)
}

// noFsync replaces fsync, for the test, with a counter of its calls: macOS's full sync takes milliseconds, and what
// the tests check is what the files hold, not that they survive a power cut.
func noFsync(t *testing.T) *atomic.Int32 {
	t.Helper()
	saved := fsync
	t.Cleanup(func() { fsync = saved })
	var n atomic.Int32
	fsync = func(*os.File) error { n.Add(1); return nil }
	return &n
}
