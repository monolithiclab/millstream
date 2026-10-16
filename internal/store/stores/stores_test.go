package stores

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres/pgtest"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// TestMain loads SQLite before the tests, its initialisation being the process's, and removes the e2e build's
// Postgres container after them.
func TestMain(m *testing.M) {
	if s, err := sqlite.Open(sqlite.Memory, sqlite.Options{}); err == nil {
		_ = s.Close()
	}
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

// Without a store key, the store is SQLite at the default path; with one, at its path. Memory stands for a path
// here: the default build opens no database file.
func TestOpenSQLite(t *testing.T) {
	for i, cfg := range []pipeline.StoreConfig{{}, {SQLitePath: sqlite.Memory}, {}} {
		open := Open
		if i == 2 { // a reader on SQLite opens as a writer does: the file's locking serialises the writers
			open = OpenReader
		}
		st, err := open(context.Background(), cfg, sqlite.Memory, func(string) (string, bool) { return "", false })
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := st.(*sqlite.Store); !ok {
			t.Errorf("Open(%+v) opened a %T", cfg, st)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// A Postgres store whose DSN variable is unset is refused before any connection, naming the variable.
func TestOpenPostgresWithoutDSN(t *testing.T) {
	cfg := pipeline.StoreConfig{PostgresDSN: &pipeline.SecretSource{Env: "MILLSTREAM_DSN"}}
	for _, open := range []func(context.Context, pipeline.StoreConfig, string, func(string) (string, bool)) (
		store.Store, error){Open, OpenReader} {
		_, err := open(context.Background(), cfg, "unused.db", func(string) (string, bool) { return "", false })
		if err == nil || !strings.Contains(err.Error(), "store.postgres.dsn: environment variable MILLSTREAM_DSN") {
			t.Fatalf("Open = %v", err)
		}
	}
}
