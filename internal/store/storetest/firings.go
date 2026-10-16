package storetest

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestSkipFiringsAndLastFiring(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	last := func() time.Time {
		t.Helper()
		at, err := e.s.LastFiring(e.ctx, "listings", "daily")
		if err != nil {
			t.Fatalf("LastFiring: %v", err)
		}
		return at
	}
	day := func(d int) time.Time { return time.Date(2026, 10, d, 6, 0, 0, 0, time.UTC) }
	if at := last(); !at.IsZero() {
		t.Fatalf("LastFiring before any firing = %v", at)
	}
	e.fireCron("daily", day(13))
	if at := last(); !at.Equal(day(13)) {
		t.Fatalf("LastFiring after a run = %v", at)
	}
	// A manual firing of the cron trigger, under a request id that sorts after any cron key, is not a cron firing.
	if _, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "20261020T000000Z-ab",
		RunID: "run_manual", Source: store.SourceManual}); err != nil {
		t.Fatal(err)
	}
	if r := e.run("run_manual"); r.TriggerSource != store.SourceManual || !r.ScheduledAt.IsZero() {
		t.Fatalf("manual run = %+v", r)
	}
	if at := last(); !at.Equal(day(13)) {
		t.Fatalf("LastFiring after a manual firing = %v, want %v", at, day(13))
	}
	skipped := []time.Time{day(14), day(15)}
	if err := e.s.SkipFirings(e.ctx, "listings", "daily", skipped); err != nil {
		t.Fatalf("SkipFirings: %v", err)
	}
	if err := e.s.SkipFirings(e.ctx, "listings", "daily", skipped[1:]); err != nil { // again: a no-op
		t.Fatalf("SkipFirings again: %v", err)
	}
	if at := last(); !at.Equal(day(15)) {
		t.Fatalf("LastFiring after skips = %v", at)
	}
	if n := len(e.eventKinds(store.EventFiringSkipped)); n != 2 {
		t.Fatalf("%d firingSkipped events, want 2", n)
	}
	if at, err := e.s.LastFiring(e.ctx, "listings", "adhoc"); err != nil || !at.IsZero() {
		t.Fatalf("LastFiring of another trigger = %v, %v", at, err)
	}
	if err := e.s.SkipFirings(e.ctx, "listings", "daily", nil); err != nil {
		t.Fatalf("SkipFirings with no times: %v", err)
	}
	x := []time.Time{day(1)}
	wantErr(t, e.s.SkipFirings(e.ctx, "listings", "", x), store.ErrInvalid)
	wantErr(t, e.s.SkipFirings(e.ctx, "listings", "weekly", x), store.ErrInvalid)
	wantErr(t, e.s.SkipFirings(e.ctx, "nope", "daily", x), store.ErrNotFound)
	_, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "k", RunID: "r",
		Source: store.SourceCron})
	wantErr(t, err, store.ErrInvalid) // a cron firing needs its scheduled time
}

func TestPipelines(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture, otherPipeline)
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
