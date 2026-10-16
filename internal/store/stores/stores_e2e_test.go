//go:build e2e

package stores

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
	"github.com/monolithiclab/millstream/internal/store/postgres/pgtest"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

func TestOpen(t *testing.T) {
	dir := t.TempDir()
	dsn := pgtest.DSN(t)
	lookup := func(name string) (string, bool) { return dsn, name == "DSN" }
	for _, tt := range []struct {
		name string
		cfg  pipeline.StoreConfig
		want func(any) bool
	}{
		{"default path", pipeline.StoreConfig{}, func(s any) bool { _, ok := s.(*sqlite.Store); return ok }},
		{"sqlite path", pipeline.StoreConfig{SQLitePath: filepath.Join(dir, "a", "b", "m.db")},
			func(s any) bool { _, ok := s.(*sqlite.Store); return ok }},
		{"postgres", pipeline.StoreConfig{PostgresDSN: &pipeline.SecretSource{Env: "DSN"}},
			func(s any) bool { _, ok := s.(*postgres.Store); return ok }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st, err := Open(context.Background(), tt.cfg, filepath.Join(dir, "default", "m.db"), lookup)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = st.Close() }()
			if !tt.want(st) {
				t.Fatalf("Open opened a %T", st)
			}
			if _, err := st.Pipelines(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := os.Stat(filepath.Join(dir, "a", "b", "m.db")); err != nil {
		t.Fatalf("the SQLite path's directory was not created: %v", err)
	}
}

// A reader opens while an engine holds the leadership lock, without waiting for it, reads what the engine wrote,
// and refuses every unit: on Postgres the active engine is the only writer.
func TestOpenReaderBesideTheLeader(t *testing.T) {
	dsn := pgtest.DSN(t)
	lookup := func(name string) (string, bool) { return dsn, name == "DSN" }
	cfg := pipeline.StoreConfig{PostgresDSN: &pipeline.SecretSource{Env: "DSN"}}
	leader, err := Open(t.Context(), cfg, "", lookup)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = leader.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	reader, err := OpenReader(ctx, cfg, "", lookup)
	if err != nil {
		t.Fatalf("OpenReader beside the leader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if _, err := reader.Pipelines(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reader.SetPaused(ctx, "p", "", true); !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("a unit on a reader: %v, want ErrReadOnly", err)
	}
}
