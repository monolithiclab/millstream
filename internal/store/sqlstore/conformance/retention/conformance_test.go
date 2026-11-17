package retention_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"
	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// TestConformance runs the conformance suite's retention area (store/storetest) against SQLite Memory stores.
func TestConformance(t *testing.T) {
	storetest.RunArea(t, storetest.Retention, sqlitetest.Memory)
}

func TestMain(m *testing.M) {
	sqlitetest.Main(m)
}
