//go:build e2e

package postgres_test

import (
	"os"
	"testing"

	"github.com/monolithiclab/millstream/internal/store/postgres/pgtest"
)

// TestMain removes the test binary's Postgres container after the tests.
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}
