package engine

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func cronPipeline(catchup, extra string) string {
	return `
pipeline: listings
triggers:
  daily: { cron: "0 6 * * *", catchup: ` + catchup + extra + ` }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.daily
`
}

func runIDs(rs []store.Run) []string {
	var out []string
	for _, r := range rs {
		out = append(out, r.ID)
	}
	slices.Sort(out)
	return out
}

// TestCatchup applies the pipeline on 10 October, stops the engine until 13 October 08:00 (four 06:00 firings
// missed), then starts a fresh engine: catch-up runs on its first tick.
func TestCatchup(t *testing.T) {
	t.Parallel()
	missed := []string{
		"run_2026-10-10T06:00Z_daily", "run_2026-10-11T06:00Z_daily", "run_2026-10-12T06:00Z_daily",
		"run_2026-10-13T06:00Z_daily",
	}
	tests := []struct {
		catchup  string
		wantRuns []string
		skipped  int
	}{
		{"latest", missed[3:], 0},
		{"all", missed, 0},
		{"none", nil, 4},
	}
	for _, tt := range tests {
		t.Run(tt.catchup, func(t *testing.T) {
			t.Parallel()
			v := newEnv(t, Options{})
			v.clock.Set(time.Date(2026, 10, 10, 5, 0, 0, 0, time.UTC))
			pkg := writePackage(t, cronPipeline(tt.catchup, ""), "crawl")
			v.apply(pkg)
			v.clock.Set(time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC))

			e2, err := New(Options{Store: v.st, Now: v.clock.Now})
			if err != nil {
				t.Fatal(err)
			}
			e2.schedule(v.ctx)
			if got := runIDs(v.runs("listings")); !slices.Equal(got, tt.wantRuns) {
				t.Fatalf("runs = %v, want %v", got, tt.wantRuns)
			}
			if n := len(v.events(store.EventFiringSkipped)); n != tt.skipped {
				t.Fatalf("%d firingSkipped events, want %d", n, tt.skipped)
			}
			// A second tick, and a third engine started later the same morning, fire nothing again.
			e2.schedule(v.ctx)
			v.clock.Advance(time.Hour)
			e3, err := New(Options{Store: v.st, Now: v.clock.Now})
			if err != nil {
				t.Fatal(err)
			}
			e3.schedule(v.ctx)
			if got := runIDs(v.runs("listings")); !slices.Equal(got, tt.wantRuns) {
				t.Fatalf("runs after restart = %v, want %v", got, tt.wantRuns)
			}
			if n := len(v.events(store.EventFiringSkipped)); n != tt.skipped {
				t.Fatalf("%d firingSkipped events after restart, want %d", n, tt.skipped)
			}

			// The next firing is on time and fires, whatever the policy.
			v.clock.Set(time.Date(2026, 10, 14, 6, 0, 20, 0, time.UTC))
			e3.schedule(v.ctx)
			r := v.run("listings", "run_2026-10-14T06:00Z_daily")
			if r.TriggerKey != "2026-10-14T06:00Z" {
				t.Fatalf("on-time run = %+v", r)
			}
		})
	}
}

func TestCronPayloadAndTimezone(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.clock.Set(time.Date(2026, 10, 15, 3, 0, 0, 0, time.UTC))
	v.apply(writePackage(t, cronPipeline("latest", `, tz: "Europe/Paris"`), "crawl"))
	v.e.schedule(v.ctx)
	if rs := v.runs("listings"); len(rs) != 0 {
		t.Fatalf("runs before 06:00 Paris = %v", runIDs(rs))
	}
	v.clock.Set(time.Date(2026, 10, 15, 4, 0, 1, 0, time.UTC)) // 06:00:01 in Paris, summer time
	v.e.schedule(v.ctx)
	id := "run_2026-10-15T04:00Z_daily"
	v.run("listings", id)
	items, err := v.st.Items(v.ctx, store.ItemFilter{RunID: id})
	if err != nil || len(items) != 1 {
		t.Fatalf("items = %v, %v", items, err)
	}
	var payload map[string]any
	if err := json.Unmarshal(items[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["trigger"] != "daily" || payload["scheduledAt"] != "2026-10-15T04:00:00Z" {
		t.Fatalf("cron payload = %v", payload)
	}
}

func TestScheduleBadCronIsLoggedOnce(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(writePackage(t, strings.Replace(cronPipeline("latest", ""), "0 6 * * *", "61 6 * * *", 1), "crawl"))
	v.e.schedule(v.ctx)
	v.e.schedule(v.ctx)
	if n := strings.Count(v.logs.String(), "scheduler: cron trigger"); n != 1 {
		t.Fatalf("cron error logged %d times, want once:\n%s", n, v.logs.String())
	}
}

func TestScheduleFireErrorIsLogged(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	src := strings.Replace(cronPipeline("latest", ""), "triggers:", "vars: { required: null }\ntriggers:", 1)
	v.apply(writePackage(t, src, "crawl"))
	v.clock.Set(time.Date(2026, 10, 16, 6, 0, 5, 0, time.UTC))
	v.e.schedule(v.ctx)
	if rs := v.runs("listings"); len(rs) != 0 || !strings.Contains(v.logs.String(), "variable required is required") {
		t.Fatalf("runs = %v; logs:\n%s", runIDs(rs), v.logs.String())
	}
}

func TestScheduleDropsOldestBeyondBound(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.clock.Set(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	v.apply(writePackage(t, strings.Replace(cronPipeline("none", ""), "0 6 * * *", "* * * * *", 1), "crawl"))
	v.clock.Set(time.Date(2026, 1, 8, 0, 0, 30, 0, time.UTC)) // 7 days of minutes: more than maxDue
	v.e.schedule(v.ctx)
	if n := len(v.events(store.EventFiringSkipped)); n != maxDue {
		t.Fatalf("%d skipped, want %d", n, maxDue)
	}
	if !strings.Contains(v.logs.String(), "oldest dropped") {
		t.Fatal("dropping was not logged")
	}
	if rs := v.runs("listings"); len(rs) != 1 || rs[0].TriggerKey != "2026-01-08T00:00Z" {
		t.Fatalf("on-time runs = %v", runIDs(rs))
	}
}
