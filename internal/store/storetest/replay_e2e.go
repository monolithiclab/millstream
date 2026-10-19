//go:build e2e

package storetest

import (
	"slices"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
)

// Replay variants the e2e build adds: their paths are covered in the default build by the other tests.

func TestConcurrentReplaysTakeDistinctGenerations(t *testing.T, open Opener) {
	e := newEnv(t, open, dedupePipeline)
	raw := e.crawled()
	const n = 8
	gens := make([]int64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			g, created, err := e.s.StartReplay(e.ctx, store.ReplayStart{Pipeline: "dd", Step: "unique",
				RunID: "rp" + string(rune('a'+i)), Key: "k", Items: raw}) // #nosec G115 -- i < n, a handful
			if err != nil || !created {
				t.Errorf("StartReplay %d = %v, %v", i, created, err)
			}
			gens[i] = g
		})
	}
	wg.Wait()
	slices.Sort(gens)
	for i, g := range gens {
		if g != int64(i+1) {
			t.Fatalf("generations = %v, want 1 to %d", gens, n)
		}
	}
}
