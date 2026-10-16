//go:build e2e

package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
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
	return countsFor(listing.Generate(fixtureSeed, fixtureListings), fixed)
}

// countsFor computes the items per stream from score on, for the listings that reach listings.raw.
func countsFor(raw []listing.Listing, fixed bool) map[string]int {
	shortlist := 0
	for _, l := range raw {
		if listing.Score(l) >= minScore {
			shortlist++
		}
	}
	priced := shortlist - 1
	if fixed {
		priced = shortlist
	}
	return map[string]int{
		"trigger.adhoc": 1, "listings.raw": len(raw), "listings.shortlist": shortlist,
		"listings.rejected": len(raw) - shortlist, "listings.priced": priced, "reports.done": priced,
		"listings.failed": 1,
	}
}

// expectedBuiltinCounts computes the items per stream of a run of the builtins fixture: every listing passes the
// dedupe on its id, the filter keeps those not in good condition, and the review approves them all by rule.
func expectedBuiltinCounts() map[string]int {
	all := listing.Generate(fixtureSeed, fixtureListings)
	var raw []listing.Listing
	for _, l := range all {
		if l.Condition != listing.ConditionGood {
			raw = append(raw, l)
		}
	}
	counts := countsFor(raw, false)
	counts["listings.crawled"], counts["listings.unique"], counts["listings.affordable"] = len(all), len(all), len(raw)
	return counts
}

// TestBuiltinsFixture runs the builtins fixture once: the dedupe and filter steps run in the engine, and the counts
// per stream follow from the generator. The filtered deliveries are the good listings.
func TestBuiltinsFixture(t *testing.T) {
	t.Parallel()
	snap := cleanBuiltinsRun(t)
	want := expectedBuiltinCounts()
	if got := snap.counts(); !maps.Equal(got, want) {
		t.Fatalf("counts = %v, want %v", got, want)
	}
	filtered := 0
	for _, d := range snap.Deliveries {
		if d.State == store.DeliveryFiltered {
			if d.Step != "affordable" {
				t.Errorf("a delivery of %s is filtered", d.Step)
			}
			filtered++
		}
	}
	if filtered != want["listings.unique"]-want["listings.raw"] || filtered == 0 {
		t.Fatalf("%d deliveries filtered, want %d", filtered, want["listings.unique"]-want["listings.raw"])
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
	checkReportArtifacts(t, db, snap)

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
	checkReportArtifacts(t, db, snap)
}

// checkReportArtifacts checks that every report carries its text report as one artifact, stored under the
// SHA-256 of that text in the artifact store next to the database, with one reference per report.
func checkReportArtifacts(t *testing.T, db string, s snapshot) {
	t.Helper()
	refs := map[string]int64{}
	for id, it := range s.Items {
		if it.Stream != "reports.done" {
			continue
		}
		var r listing.Report
		var arts []store.ArtifactRef
		if err := errors.Join(json.Unmarshal([]byte(it.Payload), &r), json.Unmarshal([]byte(it.Artifacts), &arts)); err != nil {
			t.Fatalf("report %s: %v", id, err)
		}
		sum := sha256.Sum256([]byte(r.Text()))
		want := store.ArtifactRef{Name: r.Artifact, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(r.Text())),
			Type: "text/plain; charset=utf-8"}
		if len(arts) != 1 || arts[0] != want {
			t.Fatalf("report %s artifacts = %+v, want [%+v]", id, arts, want)
		}
		content, err := os.ReadFile(filepath.Join(filepath.Dir(db), "artifacts", pipelineName, want.SHA256))
		if err != nil || string(content) != r.Text() {
			t.Fatalf("report %s: stored artifact %q (%v), want %q", id, content, err, r.Text())
		}
		refs[want.SHA256]++
	}
	if len(refs) == 0 || !maps.Equal(refs, s.Refcounts) {
		t.Fatalf("artifact refcounts = %v, want %v", s.Refcounts, refs)
	}
}

// TestReplayStep replays score over a run of the fixture: the replay run re-scores every crawled listing in its own
// generation, so its outputs are new items (new ids, generation 1) with the same payloads as the original run's,
// and everything downstream runs again, the designed dead letter included.
func TestReplayStep(t *testing.T) {
	t.Parallel()
	snap := replayedStepRun(t)
	if snap.Runs[crashRun] != store.RunCompletedWithErrors || snap.Runs[replayStepRun] != store.RunCompletedWithErrors ||
		len(snap.Runs) != 2 {
		t.Fatalf("runs = %v", snap.Runs)
	}
	original, replay := onlyRun(snap, crashRun), onlyRun(snap, replayStepRun)
	want := expectedCounts(false)
	delete(want, "trigger.adhoc")
	delete(want, "listings.raw") // the replay reads the original run's crawl, it does not crawl again
	if got := replay.counts(); !maps.Equal(got, want) {
		t.Fatalf("replay counts = %v, want %v", got, want)
	}
	byParent := map[string]item{} // score's outputs are unkeyed: the listing they score is their parent
	for _, it := range original.Items {
		byParent[it.Stream+"/"+it.Parents] = it
	}
	for id, it := range replay.Items {
		if _, dup := original.Items[id]; dup || it.Generation != 1 {
			t.Fatalf("replay item %s: generation %d, also in the original run %v", id, it.Generation, dup)
		}
		if it.Stream != "listings.shortlist" && it.Stream != "listings.rejected" {
			continue
		}
		if o, ok := byParent[it.Stream+"/"+it.Parents]; !ok || o.Payload != it.Payload {
			t.Errorf("replay item %s on %s does not re-score the original's listing %s: %+v", id, it.Stream,
				it.Parents, o)
		}
	}
	for id, d := range replay.Deliveries {
		if d.Step == "crawl" {
			t.Errorf("the replay crawled again: %s", id)
		}
	}
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
