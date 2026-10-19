//go:build e2e

package storetest

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Firings variants the e2e build adds: their paths are covered in the default build by the other tests.

func TestLeasePipelineSlots(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture, otherPipeline)
	for _, id := range []string{"r1", "r2", "r3"} {
		e.fire(id)
		e.firePipeline("other", "daily", id)
	}
	tests := []struct {
		name  string
		slots map[string]int
		want  map[string]int
	}{
		{"capped pipeline", map[string]int{"listings": 1}, map[string]int{"listings": 1, "other": 3}},
		{"zero", map[string]int{"listings": 0, "other": 0}, map[string]int{}},
		{"negative is zero", map[string]int{"other": -1}, map[string]int{"listings": 3}},
	}
	for _, tt := range tests {
		ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "n", Max: 10, TTL: time.Minute, PipelineSlots: tt.slots})
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]int{}
		for _, l := range ls {
			got[l.Pipeline]++
		}
		if len(got) != len(tt.want) || got["listings"] != tt.want["listings"] || got["other"] != tt.want["other"] {
			t.Fatalf("%s: leased %v, want %v", tt.name, got, tt.want)
		}
		e.clock.Advance(2 * time.Minute)
		if _, err := e.s.ExpireLeases(e.ctx); err != nil {
			t.Fatal(err)
		}
	}
}
