package sqlite

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestSkipFiringsAndLastFiring(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	last := func() string {
		t.Helper()
		k, err := e.s.LastFiring(e.ctx, "listings", "daily")
		if err != nil {
			t.Fatalf("LastFiring: %v", err)
		}
		return k
	}
	if k := last(); k != "" {
		t.Fatalf("LastFiring before any firing = %q", k)
	}
	e.firePipeline("listings", "daily", "2026-10-13T06:00Z")
	if k := last(); k != "2026-10-13T06:00Z" {
		t.Fatalf("LastFiring after a run = %q", k)
	}
	if err := e.s.SkipFirings(e.ctx, "listings", "daily", []string{"2026-10-14T06:00Z", "2026-10-15T06:00Z"}); err != nil {
		t.Fatalf("SkipFirings: %v", err)
	}
	if k := last(); k != "2026-10-15T06:00Z" {
		t.Fatalf("LastFiring after skips = %q", k)
	}
	if n := len(e.eventKinds(store.EventFiringSkipped)); n != 2 {
		t.Fatalf("%d firingSkipped events, want 2", n)
	}
	if k, err := e.s.LastFiring(e.ctx, "listings", "adhoc"); err != nil || k != "" {
		t.Fatalf("LastFiring of another trigger = %q, %v", k, err)
	}
	if err := e.s.SkipFirings(e.ctx, "listings", "daily", nil); err != nil {
		t.Fatalf("SkipFirings with no keys: %v", err)
	}
	wantErr(t, e.s.SkipFirings(e.ctx, "listings", "", []string{"x"}), store.ErrInvalid)
	wantErr(t, e.s.SkipFirings(e.ctx, "listings", "weekly", []string{"x"}), store.ErrInvalid)
	wantErr(t, e.s.SkipFirings(e.ctx, "nope", "daily", []string{"x"}), store.ErrNotFound)
}

func TestPipelines(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture, otherPipeline)
	if err := e.s.SetPaused(e.ctx, "other", "", true); err != nil {
		t.Fatal(err)
	}
	ps, err := e.s.Pipelines(e.ctx)
	if err != nil {
		t.Fatalf("Pipelines: %v", err)
	}
	if len(ps) != 2 || ps[0].Name != "listings" || ps[0].Paused || ps[1].Name != "other" || !ps[1].Paused ||
		ps[0].ActiveRevision == 0 {
		t.Fatalf("Pipelines = %+v", ps)
	}
}

func TestLeasePipelineSlots(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture, otherPipeline)
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
