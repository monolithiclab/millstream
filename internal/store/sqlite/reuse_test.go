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
