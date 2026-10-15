package sqlite

import (
	"context"
	"io/fs"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestOpenFileAndReopen(t *testing.T) {
	t.Parallel()
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
}

func TestOpenErrors(t *testing.T) {
	t.Parallel()
	if _, err := Open(filepath.Join(t.TempDir(), "missing", "dir", "m.db"), Options{}); err == nil {
		t.Fatal("Open in a missing directory succeeded")
	}
}

func TestReadPoolIsQueryOnly(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := e.s.read.Exec(`INSERT INTO events (ts, pipeline, run_id, kind, data) VALUES ('', '', '', '', '')`); err == nil {
		t.Fatal("a write through the read pool succeeded")
	}
}

func TestClosedStore(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	e := newEnv(t, fixture)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.s.ExpireLeases(ctx)
	wantErr(t, err, context.Canceled)
}

func TestReads(t *testing.T) {
	t.Parallel()
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
		ID: "r1", Pipeline: "listings", Revision: 1, Trigger: "daily", TriggerKey: "r1", TriggerSource: store.SourceManual,
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
