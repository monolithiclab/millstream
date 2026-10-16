package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
)

// A closed Memory store's database serves the next one, emptied, unless its schema changed meanwhile; the closed
// store's reads fail rather than read the next store's rows.
func TestMemoryPool(t *testing.T) {
	ctx := context.Background()
	open := func() *Store {
		t.Helper()
		s, err := Open(Memory, Options{})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	closeStore := func(s *Store) {
		t.Helper()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	first := open()
	if _, err := first.ApplyRevision(ctx, store.PackageRecord{Hash: hashOf(fixture), Pipeline: parse(t, fixture)}); err != nil {
		t.Fatal(err)
	}
	closeStore(first)
	if _, err := first.Pipelines(ctx); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("a read of a closed store = %v, want ErrClosed", err)
	}

	second := open()
	if second.write != first.write {
		t.Fatal("the next store did not take the closed one's database")
	}
	if names, err := second.Pipelines(ctx); err != nil || len(names) != 0 {
		t.Fatalf("pipelines of the reused database = %v, %v; want none", names, err)
	}
	e := &env{t: t, s: second, ctx: ctx}
	setTable(t, e, "events", "events_off")
	setTable(t, e, "events_off", "events")
	closeStore(second)

	third := open()
	defer closeStore(third)
	if third.write == second.write {
		t.Fatal("a database whose schema changed went back to the pool")
	}
}

// A store's image opens as a store holding the same history, apart from the original.
func TestImage(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	image, err := e.s.Image()
	if err != nil {
		t.Fatal(err)
	}
	copied, err := OpenImage(image, Options{Now: e.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = copied.Close() }()
	if r, err := copied.Run(e.ctx, "listings", "r1"); err != nil || r.ID != "r1" {
		t.Fatalf("run in the image = %+v, %v", r, err)
	}
	if _, err := OpenImage([]byte("not a database"), Options{}); err == nil {
		t.Error("a bad image opened")
	}
}
