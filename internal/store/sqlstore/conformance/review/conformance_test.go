package review_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"
	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// TestConformance runs the conformance suite's review area (store/storetest) against SQLite Memory stores.
func TestConformance(t *testing.T) {
	storetest.RunArea(t, storetest.Review, sqlitetest.Memory)
}

func TestMain(m *testing.M) {
	sqlitetest.Main(m)
}
