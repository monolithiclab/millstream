//go:build e2e

package e2e

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/store"
)

// reviewer decides every review of the e2e tests: the processes run without $USER in CI.
var reviewer = []string{"--as", "tester"}

// parkedReview is a review as "inbox --json" prints it, as far as the tests read it.
type parkedReview struct {
	DeliveryID string `json:"deliveryId"`
	Display    string `json:"display"`
	Item       struct {
		Key string `json:"key"`
	} `json:"item"`
}

// inbox lists the open reviews (or samples) on db, sorted by item key.
func inbox(t *testing.T, db string, args ...string) []parkedReview {
	t.Helper()
	r := mustExit(t, 0, db, nil, append([]string{"inbox"}, args...)...)
	var out []parkedReview
	for _, line := range strings.Split(strings.TrimSpace(r.stdout), "\n") {
		if line == "" {
			continue
		}
		var p parkedReview
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("inbox line %q: %v", line, err)
		}
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b parkedReview) int { return strings.Compare(a.Item.Key, b.Item.Key) })
	return out
}

// decideAll decides every open review the way the clean reference does, with env set on each command: the first by
// item key rejected as overpriced, the second approved with its rent edited, the others approved as they are. The
// first decision runs with first's environment (a failpoint) and must exit with firstCode; it is then made again.
func decideAll(t *testing.T, db string, first []string, firstCode int) {
	t.Helper()
	open := inbox(t, db, "list", "--json")
	if len(open) < 3 {
		t.Fatalf("%d reviews open, want the fixture's parked listings", len(open))
	}
	for i, r := range open {
		args := []string{"inbox", "approve", r.DeliveryID}
		switch i {
		case 0:
			args = []string{"inbox", "reject", r.DeliveryID, "--reason", "overpriced", "--text", "too dear"}
		case 1:
			args = append(args, "--set", "/monthlyRentEur=1234")
		}
		args = append(args, reviewer...)
		if i == 0 && first != nil {
			mustExit(t, firstCode, db, first, args...)
			if res := millstream(t, db, nil, args...); res.code != 0 && !strings.Contains(res.stderr, "already decided") {
				t.Fatalf("deciding again after a crash: %s", res)
			}
			continue
		}
		mustExit(t, 0, db, nil, args...)
	}
}

// expectedReviewCounts are the fixture's shortlisted listings: those scoring 85 or more approved by rule, the
// others parked.
func expectedReviewCounts() (auto, parked int) {
	for _, l := range listing.Generate(fixtureSeed, fixtureListings) {
		switch s := listing.Score(l); {
		case s >= 85:
			auto++
		case s >= minScore:
			parked++
		}
	}
	return auto, parked
}

// TestReviewFixture is the M3 review scenario on the CLI: the run parks the shortlisted listings the rule does not
// approve and waits for review; every one is decided with millstream inbox (one rejected with a reason code, one
// approved with an edit); a sample is decided; the run then settles, the review metrics roll up and the
// reviewPattern insight appears.
func TestReviewFixture(t *testing.T) {
	t.Parallel()
	db := dbPath(t)
	r := mustExit(t, 1, db, nil, runOnce(fixtureReview)...)
	if !strings.Contains(r.stdout, string(store.RunWaitingReview)) {
		t.Fatalf("the first run does not wait for review:\n%s", r.stdout)
	}
	auto, parked := expectedReviewCounts()
	open := inbox(t, db, "list", "--json", "--inbox", "analysts")
	if len(open) != parked || !strings.Contains(open[0].Display, "EUR, score") {
		t.Fatalf("%d reviews open (%+v), want %d", len(open), open, parked)
	}
	samples := inbox(t, db, "samples", "list", "--json")
	if len(samples) == 0 || len(samples) > auto {
		t.Fatalf("%d samples of %d auto-approved listings", len(samples), auto)
	}
	decideAll(t, db, nil, 0)
	mustExit(t, 0, db, nil, append([]string{"inbox", "sample", samples[0].DeliveryID, "disagree"}, reviewer...)...)

	r = mustExit(t, 1, db, nil, runOnce(fixtureReview)...)
	if !strings.Contains(r.stdout, string(store.RunCompletedWithErrors)) {
		t.Fatalf("the run after the decisions:\n%s", r.stdout)
	}
	snap := readSnapshot(t, db)
	counts := snap.counts()
	shortlist := auto + parked
	if counts["listings.shortlist"] != shortlist || counts["listings.vetted"] != shortlist-1 {
		t.Fatalf("counts = %v: want %d shortlisted and all but the rejected one vetted", counts, shortlist)
	}
	edited := false
	for _, it := range snap.Items {
		edited = edited || it.Stream == "listings.vetted" && strings.Contains(it.Payload, `"monthlyRentEur":1234`)
	}
	if !edited {
		t.Fatal("the edited listing did not pass on as edited")
	}
	m := mustExit(t, 0, db, nil, "metrics", "--since", "all", "--name", "review_decisions")
	for _, want := range []string{"decision=reject,reason=overpriced,step=vetting", "decision=autoApprove,step=vetting",
		"decision=approve,step=vetting"} {
		if !strings.Contains(m.stdout, want) {
			t.Fatalf("review_decisions lacks %s:\n%s", want, m.stdout)
		}
	}
	if in := mustExit(t, 0, db, nil, "insights", "list", "--json"); !strings.Contains(in.stdout, `"kind":"reviewPattern"`) ||
		!strings.Contains(in.stdout, `"severity":"warning"`) {
		t.Fatalf("no reviewPattern warning (one sample disagreed):\n%s", in.stdout)
	}
}

// reviewClean is the review fixture's crash reference: run once, every review decided by decideAll, run again.
var reviewClean reference

func reviewCleanRun(t *testing.T) snapshot {
	t.Helper()
	return reviewClean.compute(t, sqliteBackend, "review crash", func(db string) {
		mustExit(t, 1, db, nil, runOnce(fixtureReview)...)
		decideAll(t, db, nil, 0)
		mustExit(t, 1, db, nil, runOnce(fixtureReview)...)
	})
}

// reviewTimedOut is the timeout fixture's crash reference: run once, left until the reviews time out, run again.
var reviewTimedOut reference

// reviewWait outlasts the timeout fixture's one-second review timeout.
const reviewWait = 1200 * time.Millisecond

func reviewTimedOutRun(t *testing.T) snapshot {
	t.Helper()
	return reviewTimedOut.compute(t, sqliteBackend, "review timeout crash", func(db string) {
		mustExit(t, 1, db, nil, runOnce(fixtureReviewTimeout)...)
		time.Sleep(reviewWait)
		mustExit(t, 1, db, nil, runOnce(fixtureReviewTimeout)...)
	})
}

// TestReviewCrash covers the four review units at both failpoints: Park in the run that parks, Review decision and
// Sample decision in the inbox commands, Review timeout in the engine start that times overdue reviews out. Each
// crashes its process, the command is run again, and the end state is the one reached without a crash.
func TestReviewCrash(t *testing.T) {
	t.Parallel()
	var (
		mu  sync.Mutex
		hit = map[string]bool{}
	)
	record := func(name string, code int) {
		mu.Lock()
		hit[name] = code == failpoint.ExitCode
		mu.Unlock()
	}
	t.Run("units", func(t *testing.T) {
		for _, phase := range phases {
			t.Run("park."+phase, func(t *testing.T) {
				t.Parallel()
				want := reviewCleanRun(t)
				db := dbPath(t)
				name := "park." + phase
				r := millstream(t, db, failpointEnv(name), runOnce(fixtureReview)...)
				record(name, r.code)
				mustExit(t, 1, db, nil, runOnce(fixtureReview)...)
				decideAll(t, db, nil, 0)
				mustExit(t, 1, db, nil, runOnce(fixtureReview)...)
				wantSame(t, readSnapshot(t, db), want)
			})
			t.Run("reviewDecision."+phase, func(t *testing.T) {
				t.Parallel()
				want := reviewCleanRun(t)
				db := dbPath(t)
				name := "reviewDecision." + phase
				mustExit(t, 1, db, nil, runOnce(fixtureReview)...)
				decideAll(t, db, failpointEnv(name), failpoint.ExitCode)
				record(name, failpoint.ExitCode)
				mustExit(t, 1, db, nil, runOnce(fixtureReview)...)
				wantSame(t, readSnapshot(t, db), want)
			})
			t.Run("sampleDecision."+phase, func(t *testing.T) {
				t.Parallel()
				db := dbPath(t)
				name := "sampleDecision." + phase
				mustExit(t, 1, db, nil, runOnce(fixtureReview)...)
				samples := inbox(t, db, "samples", "list", "--json")
				if len(samples) == 0 {
					t.Fatal("no review sample to decide")
				}
				args := append([]string{"inbox", "sample", samples[0].DeliveryID, "agree"}, reviewer...)
				mustExit(t, failpoint.ExitCode, db, failpointEnv(name), args...)
				record(name, failpoint.ExitCode)
				// Before the commit nothing was decided; after it, the same decision sent again stands: both succeed.
				if res := millstream(t, db, nil, args...); res.code != 0 {
					t.Fatalf("deciding the sample again after a crash %s: %s", phase, res)
				}
				if left := inbox(t, db, "samples", "list", "--json"); len(left) != len(samples)-1 {
					t.Fatalf("%d samples open after deciding one of %d", len(left), len(samples))
				}
			})
			t.Run("reviewTimeout."+phase, func(t *testing.T) {
				t.Parallel()
				want := reviewTimedOutRun(t)
				db := dbPath(t)
				name := "reviewTimeout." + phase
				mustExit(t, 1, db, nil, runOnce(fixtureReviewTimeout)...)
				time.Sleep(reviewWait)
				r := millstream(t, db, failpointEnv(name), runOnce(fixtureReviewTimeout)...)
				record(name, r.code)
				mustExit(t, 1, db, nil, runOnce(fixtureReviewTimeout)...)
				got := readSnapshot(t, db)
				wantSame(t, got, want)
				timedOut := 0
				for _, d := range got.Deliveries {
					if d.Step == "vetting" && d.State == store.DeliveryFiltered {
						timedOut++
					}
				}
				if _, parked := expectedReviewCounts(); timedOut != parked {
					t.Fatalf("%d reviews rejected on timeout, want %d", timedOut, parked)
				}
			})
		}
	})
	if t.Failed() {
		return
	}
	for _, unit := range []string{"park", "reviewDecision", "sampleDecision", "reviewTimeout"} {
		for _, phase := range phases {
			if !hit[unit+"."+phase] {
				t.Errorf("failpoint %s.%s was never hit: the crash suite does not cover it", unit, phase)
			}
		}
	}
}
