//go:build e2e

package postgres_test

import (
	"context"
	"testing"

	"github.com/monolithiclab/millstream/internal/store/postgres"
	"github.com/monolithiclab/millstream/internal/store/postgres/pgtest"
	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// open is the conformance suite's opener: a store on a new database of the test binary's Postgres.
func open(t *testing.T, opts storetest.Options) storetest.Store {
	t.Helper()
	s, err := postgres.Open(context.Background(), pgtest.DSN(t), postgres.Options{Now: opts.Now, Rand: opts.Rand})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

// TestConformance runs the store conformance suite (store/storetest) against Postgres, each test on a database of
// its own.
func TestConformance(t *testing.T) {
	for _, tc := range storetest.Tests {
		t.Run(tc.Name, func(t *testing.T) { tc.Run(t, open) })
	}
}
