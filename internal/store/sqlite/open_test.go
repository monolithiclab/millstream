package sqlite

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestOpenFileAndReopen(t *testing.T) {
	noFsync(t)
	path := filepath.Join(t.TempDir(), "m.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	src := fixture
	if _, err := s.ApplyRevision(ctx, store.PackageRecord{Hash: hashOf(src), Pipeline: parse(t, src)}); err != nil {
		t.Fatalf("ApplyRevision: %v", err)
	}
	// The database clock stamps the revision when no clock is injected.
	revs, err := s.Revision(ctx, 1)
	if err != nil {
		t.Fatalf("Revision: %v", err)
	}
	if d := time.Since(revs.AppliedAt); d < -time.Minute || d > time.Minute {
		t.Errorf("applied at %v, want about now", revs.AppliedAt)
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
	if err := s.read.QueryRow(`SELECT COUNT(*) FROM schema_version`).Scan(&versions); err != nil {
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
	if err := s.read.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	wantState(t, "journal mode", mode, "wal")
	// The read pool is query-only.
	if _, err := s.read.Exec(`INSERT INTO events (ts, pipeline, run_id, kind, data) VALUES ('', '', '', '', '')`); err == nil {
		t.Fatal("a write through the read pool succeeded")
	}
}

// A clone holds the database as committed when it was taken, and goes its own way from there.
func TestClone(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	c, err := e.s.Clone(Options{Now: e.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	e.fire("r2")
	if _, err := c.Run(e.ctx, "listings", "r1"); err != nil {
		t.Fatalf("the clone lacks r1: %v", err)
	}
	if _, err := c.Run(e.ctx, "listings", "r2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the clone has r2: %v", err)
	}
	if created, err := c.Fire(e.ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "r3", RunID: "r3"}); err != nil || !created {
		t.Fatalf("Fire on the clone: %v, %v", created, err)
	}
	if _, err := e.s.Run(e.ctx, "listings", "r3"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the clone's run reached the original: %v", err)
	}
}

func TestOpenErrors(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "missing", "dir", "m.db"), Options{}); err == nil {
		t.Fatal("Open in a missing directory succeeded")
	}
}

func TestClosedStore(t *testing.T) {
	s, err := Open(Memory, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = s.ExpireLeases(context.Background())
	wantErr(t, err, store.ErrClosed)
}

func TestCancelledContext(t *testing.T) {
	e := newEnv(t, fixture)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.s.ExpireLeases(ctx)
	wantErr(t, err, context.Canceled)
}

func TestReads(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.clock.Advance(time.Second)
	e.fire("r2")

	runs, err := e.s.Runs(e.ctx, store.RunFilter{Pipeline: "listings"})
	if err != nil || len(runs) != 2 || runs[0].ID != "r2" {
		t.Fatalf("Runs = %v, %v; want r2 first", runs, err)
	}
	runs, err = e.s.Runs(e.ctx, store.RunFilter{State: store.RunRunning, Limit: 1})
	if err != nil || len(runs) != 1 {
		t.Fatalf("Runs limit 1 = %v, %v", runs, err)
	}
	r := e.run("r1")
	want := store.Run{
		Seq: 1, ID: "r1", Pipeline: "listings", Revision: 1, Trigger: "daily", TriggerKey: "r1", TriggerSource: store.SourceManual,
		Kind: store.RunKindTrigger,
		Vars: map[string]any{"minScore": int64(70)}, VarSources: map[string]string{"minScore": "default"},
		State: store.RunRunning, StartedAt: t0,
	}
	if !reflect.DeepEqual(r, want) {
		t.Errorf("Run = %+v\nwant  %+v", r, want)
	}
	_, err = e.s.Run(e.ctx, "listings", "nope")
	wantErr(t, err, store.ErrNotFound)

	items, err := e.s.Items(e.ctx, store.ItemFilter{RunID: "r1"})
	if err != nil || len(items) != 1 || items[0].Stream != "trigger.daily" || items[0].Step != "trigger.daily" {
		t.Fatalf("Items = %+v, %v", items, err)
	}
	if len(items[0].Parents) != 0 || string(items[0].Payload) != "{}" || !items[0].TS.Equal(t0) {
		t.Errorf("trigger item = %+v", items[0])
	}
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{State: store.DeliveryPending})
	if err != nil || len(ds) != 2 {
		t.Fatalf("Deliveries = %v, %v", ds, err)
	}
	evs, err := e.s.Events(e.ctx, 1, 2)
	if err != nil || len(evs) != 2 || evs[0].Seq != 2 {
		t.Fatalf("Events(1, 2) = %v, %v", evs, err)
	}
	if _, err := e.s.Events(e.ctx, 0, 0); err != nil {
		t.Fatalf("Events default limit: %v", err)
	}
	_, err = e.s.ActiveRevision(e.ctx, "nope")
	wantErr(t, err, store.ErrNotFound)
	_, err = e.s.Revision(e.ctx, 99)
	wantErr(t, err, store.ErrNotFound)
	_, err = e.s.PipelineState(e.ctx, "nope")
	wantErr(t, err, store.ErrNotFound)
}

// The reads of the HTTP API: paging cursors, lineage, a step's live state, the commit notifier.
func TestAPIReads(t *testing.T) {
	e := newEnv(t, fixture)
	var notified []int64
	e.s.OnCommit(func(seq int64) { notified = append(notified, seq) })
	e.fire("r1")
	e.fire("r2") // started at the same instant as r1: Seq breaks the tie
	last, err := e.s.LastEventSeq(e.ctx)
	if err != nil || last == 0 || len(notified) != 2 || notified[1] != last {
		t.Fatalf("LastEventSeq = %d, %v; notified %v", last, err, notified)
	}
	e.s.OnCommit(nil)
	page, err := e.s.Runs(e.ctx, store.RunFilter{Pipeline: "listings", Limit: 1})
	if err != nil || len(page) != 1 || page[0].ID != "r2" {
		t.Fatalf("first page = %v, %v", page, err)
	}
	next, err := e.s.Runs(e.ctx, store.RunFilter{Pipeline: "listings", Limit: 1,
		After: store.RunPosition{StartedAt: page[0].StartedAt, Seq: page[0].Seq}})
	if err != nil || len(next) != 1 || next[0].ID != "r1" {
		t.Fatalf("second page = %v, %v", next, err)
	}

	l := e.leaseStep("crawl")
	e.done(l, out("a", `{"score": 80}`), out("b", `{"score": 10}`))
	children, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: "listings", Parents: []string{l.Item.ID}})
	if err != nil || len(children) != 4 { // two outputs, each on listings.raw and listings.audit
		t.Fatalf("children = %v, %v", children, err)
	}
	got, err := e.s.Items(e.ctx, store.ItemFilter{IDs: []string{children[0].ID, l.Item.ID}})
	if err != nil || len(got) != 2 || got[0].ID != l.Item.ID {
		t.Fatalf("Items by id = %v, %v", got, err)
	}
	after, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: "listings", After: children[0].Seq, Limit: 2})
	if err != nil || len(after) != 2 || after[0].ID != children[1].ID {
		t.Fatalf("Items after %d = %v, %v", children[0].Seq, after, err)
	}
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: "listings", ID: l.DeliveryID, ItemID: l.Item.ID,
		Step: "crawl"})
	if err != nil || len(ds) != 1 || ds[0].ID != l.DeliveryID {
		t.Fatalf("Deliveries of the item = %v, %v", ds, err)
	}
	evs, err := e.s.PipelineEvents(e.ctx, store.EventFilter{Pipeline: "listings", RunID: l.RunID,
		Kinds: []string{store.EventDeliveryDone}})
	if err != nil || len(evs) != 1 || !strings.Contains(string(evs[0].Data), `"itemId":"`+l.Item.ID+`"`) {
		t.Fatalf("done events of the run = %v, %v", evs, err)
	}

	counts, err := e.s.StreamCounts(e.ctx, "listings", "")
	if err != nil || counts["trigger.daily"] != 2 || counts["listings.raw"] != 2 {
		t.Fatalf("StreamCounts = %v, %v", counts, err)
	}
	if counts, err = e.s.StreamCounts(e.ctx, "listings", l.RunID); err != nil || counts["trigger.daily"] != 1 {
		t.Fatalf("StreamCounts of %s = %v, %v", l.RunID, counts, err)
	}
	stats, err := e.s.StepStats(e.ctx, "listings", t0)
	if err != nil {
		t.Fatal(err)
	}
	byStep := map[string]store.StepStats{}
	for _, s := range stats {
		byStep[s.Step] = s
	}
	if c := byStep["crawl"]; c.Pending != 1 || c.Finished != 1 || len(c.Latencies) != 1 {
		t.Errorf("crawl stats = %+v", c)
	}
	if s := byStep["score"]; s.Pending != 2 {
		t.Errorf("score stats = %+v", s)
	}

	revs, err := e.s.Revisions(e.ctx, "listings")
	if err != nil || len(revs) != 1 || revs[0].PackageHash != hashOf(fixture) {
		t.Fatalf("Revisions = %v, %v", revs, err)
	}
	bundle, err := e.s.Package(e.ctx, revs[0].PackageHash)
	if err != nil || string(bundle) != fixture {
		t.Fatalf("Package = %q, %v", bundle, err)
	}
	_, err = e.s.Package(e.ctx, "nope")
	wantErr(t, err, store.ErrNotFound)
}

// errReader fails every read, as a write cut short would.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("disk full") }

// A new store's image appears whole or not at all: a stray temporary file from a crash, two first opens racing, and
// a write that fails each leave either the complete image at path or nothing.
func Test_createFromImage(t *testing.T) {
	syncs := noFsync(t)
	dir := t.TempDir()
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
