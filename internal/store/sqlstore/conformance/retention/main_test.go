package retention_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"
)

// memoryStore is the wrappers' opener.
var memoryStore = sqlitetest.Memory

func TestMain(m *testing.M) {
	sqlitetest.Main(m)
}
