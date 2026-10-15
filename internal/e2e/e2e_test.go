package e2e

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
	"github.com/monolithiclab/millstream/internal/store"
)

// The fixture's generator parameters and thresholds, as pipeline.yaml sets them.
const (
	fixtureSeed     = 7
	fixtureListings = 50
	minScore        = 70
	minMarginPct    = 15
)

// expectedCounts computes, from the fixture's own generator and rules, the items per stream a run produces: every
// shortlisted listing is priced and reported except the one without a rent estimate, which dead-letters while
// rentability is strict.
func expectedCounts(fixed bool) map[string]int {
	shortlist := 0
	for _, l := range listing.Generate(fixtureSeed, fixtureListings) {
		if listing.Score(l) >= minScore {
			shortlist++
		}
	}
	priced := shortlist - 1
	if fixed {
		priced = shortlist
	}
	return map[string]int{
		"trigger.adhoc": 1, "listings.raw": fixtureListings, "listings.shortlist": shortlist,
		"listings.rejected": fixtureListings - shortlist, "listings.priced": priced, "reports.done": priced,
		"listings.failed": 1,
	}
}

// summary is what "run --once" prints for the run.
func summary(runID string, state store.RunState, counts map[string]int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s %s\n", pipelineName, runID, state)
	for _, stream := range slices.Sorted(maps.Keys(counts)) {
		fmt.Fprintf(&b, "  %s %d\n", stream, counts[stream])
	}
	return b.String()
}

// TestRunAndReplay is the M1 end-to-end scenario: the fixture runs once and completes with errors, leaving the one
// designed dead letter; a fixed revision is applied, the dead letter is replayed onto it with --migrate, and the run
// succeeds with the listing priced and reported.
func TestRunAndReplay(t *testing.T) {
	t.Parallel()
	db := dbPath(t)
	const runID = "run_e2e_adhoc"
	runArgs := func(dir string) []string { return []string{"run", "--once", "--request-id", "e2e", dir} }

	r := mustExit(t, 1, db, nil, runArgs(fixtureV1)...)
	want := expectedCounts(false)
	if got := summary(runID, store.RunCompletedWithErrors, want); r.stdout != got {
		t.Fatalf("summary:\n%s\nwant:\n%s", r.stdout, got)
	}
	if want["listings.shortlist"] < 5 || want["listings.rejected"] < 5 {
		t.Fatalf("the fixture's routes are lopsided: %v", want)
	}
	snap := readSnapshot(t, db)
	if got := snap.counts(); !maps.Equal(got, want) {
		t.Fatalf("counts = %v, want %v", got, want)
	}
	checkPayloads(t, snap, true)

	dls := mustExit(t, 0, db, nil, "dlq", "list", "--pipeline", pipelineName)
	if !regexp.MustCompile(`(?m)^it_\w+\s+m1-fixture\s+run_e2e_adhoc\s+rentability\s+dead\s+open\s+action: listing L013: no monthlyRentEur$`).
		MatchString(dls.stdout) || strings.Count(dls.stdout, "\n") != 2 {
		t.Fatalf("dlq list:\n%s", dls.stdout)
	}
	status := mustExit(t, 0, db, nil, "status")
	if !regexp.MustCompile(`(?m)^m1-fixture\s+active\s+1\s+-$`).MatchString(status.stdout) ||
		!regexp.MustCompile(`(?m)^run_e2e_adhoc\s+m1-fixture\s+completedWithErrors\s+\S+\s+\d+\s+1$`).
			MatchString(status.stdout) {
		t.Fatalf("status:\n%s", status.stdout)
	}

	// The fix: running v2 applies it as revision 2 and resumes the same, settled run: nothing new fires.
	mustExit(t, 1, db, nil, runArgs(fixtureV2)...)
	if s := readSnapshot(t, db); len(s.Runs) != 1 || s.Revisions[runID] != 1 {
		t.Fatalf("after applying v2: runs %v on revisions %v", s.Runs, s.Revisions)
	}
	r = mustExit(t, 0, db, nil, "replay", "--from", "dlq", "--pipeline", pipelineName, "--run", runID, "--migrate")
	if r.stdout != "1 dead letter replayed\n" {
		t.Fatalf("replay: %s", r.stdout)
	}
	r = mustExit(t, 0, db, nil, runArgs(fixtureV2)...)
	want = expectedCounts(true)
	if got := summary(runID, store.RunSucceeded, want); r.stdout != got {
		t.Fatalf("summary after the replay:\n%s\nwant:\n%s", r.stdout, got)
	}
	snap = readSnapshot(t, db)
	if snap.Runs[runID] != store.RunSucceeded || snap.Revisions[runID] != 2 || len(snap.Runs) != 1 {
		t.Fatalf("runs %v on revisions %v, want %s succeeded on revision 2", snap.Runs, snap.Revisions, runID)
	}
	if len(snap.DeadLetters) != 1 || slices.Collect(maps.Values(snap.DeadLetters))[0].State != store.DeadLetterReplayed {
		t.Fatalf("dead letters = %v, want the one replayed", snap.DeadLetters)
	}
	if got := snap.counts(); !maps.Equal(got, want) {
		t.Fatalf("counts after the replay = %v, want %v", got, want)
	}
	checkPayloads(t, snap, false)
}

// checkPayloads checks every priced and reported payload against the fixture's rules, from the scored listing that
// produced it.
func checkPayloads(t *testing.T, s snapshot, strict bool) {
	t.Helper()
	decode := func(id string, v any) {
		t.Helper()
		if err := json.Unmarshal([]byte(s.Items[id].Payload), v); err != nil {
			t.Fatalf("item %s payload: %v", id, err)
		}
	}
	for id, it := range s.Items {
		switch it.Stream {
		case "listings.priced":
			var got listing.Priced
			var in listing.Scored
			decode(id, &got)
			decode(it.Parents, &in)
			want, err := listing.Price(in, strict)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("priced %s = %+v, want %+v (%v)", id, got, want, err)
			}
		case "reports.done":
			var got listing.Report
			var in listing.Priced
			decode(id, &got)
			decode(it.Parents, &in)
			if want := listing.Summarize(in, minMarginPct); got != want {
				t.Errorf("report %s = %+v, want %+v", id, got, want)
			}
		}
	}
}
