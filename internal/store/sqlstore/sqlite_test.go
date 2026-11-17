package sqlstore_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"
	"github.com/monolithiclab/millstream/internal/store/sqlstore"
)

// The package's own tests open SQLite Memory stores; the conformance suite (store/storetest) runs against them from
// the conformance/... packages, one per area, and against Postgres under the e2e tag.

func TestMain(m *testing.M) {
	sqlitetest.Main(m)
}

func init() {
	sqlstore.OpenForTest = func(t *testing.T, opts sqlstore.Options) *sqlstore.Store {
		t.Helper()
		s, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: opts.Now, Rand: opts.Rand})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		})
		return s.Store
	}
}
