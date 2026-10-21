package stores

import (
	"context"
	"os"
	"testing"

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

// A sqlite:// DSN opens SQLite at its path, for a writer and a reader alike: the file's locking serialises the
// writers. Memory stands for a path here: the default build opens no database file.
func TestOpenSQLite(t *testing.T) {
	for _, open := range []func(context.Context, string) (store.Store, error){Open, OpenReader} {
		st, err := open(context.Background(), "sqlite://"+sqlite.Memory)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := st.(*sqlite.Store); !ok {
			t.Errorf("opened a %T", st)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
