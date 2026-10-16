package sqlstore

import "testing"

// OpenForTest opens a store on a new SQLite Memory database for the package's internal tests, closed when the test
// ends; sqlite_test.go sets it, the SQLite store being the one package that can open a migrated database for these
// tests (store/sqlite imports this package).
var OpenForTest func(t *testing.T, opts Options) *Store
