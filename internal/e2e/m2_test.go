//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
	"github.com/monolithiclab/millstream/examples/m2-fixture/pool"
	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// The M2 fixture's names, model and thresholds, as examples/m2-fixture sets them.
const (
	m2Pipeline = "m2-fixture"
	m2Model    = "fake-scorer-1"
	m2MinScore = 70
	// lessonsV1Penalty and lessonsV2Penalty are the poor-condition penalties the two lessons versions ask for
	// (lessons/score.yaml and testdata/score-lessons-v2.yaml), which the fake model applied when recording.
	lessonsV1Penalty = 10
	lessonsV2Penalty = 25
	// crashListing is the listing of day 1 whose recording the crash suite withholds.
	crashListing = "L002"
	// Prices of the fake model in envFile, per million tokens.
	m2InPerMtok, m2OutPerMtok = 3, 15
)

// m2Env is the environment of a millstream process running the M2 fixture: its agent calls replayed from
// recordings, and the API key secret the pipeline declares (a replay never sends it).
func m2Env(recordings string) []string {
	return []string{agent.EnvHarnessMode + "=" + agent.ModeReplay, agent.EnvHarnessDir + "=" + recordings,
		"MILLSTREAM_SECRET_SCORER_KEY=sk-fake-0123456789"}
}

// m2Run returns the arguments of "run --once" on the M2 package pkg crawling day, under the request id.
func m2Run(pkg, request string, day int) []string {
	return []string{"--env-file", envFile, "run", "--once", "--request-id", request, "--lease-ttl", "1s",
		"--var", "day=" + strconv.Itoa(day), pkg}
}

// m2Counts computes the items per stream from score on for the listings that reach score, scored with the
// penalty: keep-good keeps those scoring at least m2MinScore, and every one kept is priced and reported.
func m2Counts(ls []listing.Listing, penalty int) map[string]int {
	good := 0
	for _, l := range ls {
		if pool.Score(l, penalty) >= m2MinScore {
			good++
		}
	}
	return map[string]int{"listings.scored": len(ls), "listings.shortlist": good, "listings.priced": good,
		"reports.done": good}
}

// m2DayCounts computes the items per stream of a manual run crawling day, with or without the dedupe step.
func m2DayCounts(day, penalty int, dedupe bool) map[string]int {
	ls := pool.Day(day)
	c := m2Counts(ls, penalty)
	c["trigger.adhoc"], c["listings.raw"] = 1, len(ls)
	if dedupe {
		c["listings.unique"] = len(ls)
	}
	return c
}

// scoredListing is the score step's output.
type scoredListing struct {
	listing.Scored
	Reason string `json:"reason"`
}

// checkScores checks every score output of the run in s: the listing as crawled, with the fake model's score
// under the penalty and a reason.
func checkScores(t *testing.T, s snapshot, runID string, penalty int) map[string]int {
	t.Helper()
	byID := map[string]listing.Listing{}
	for _, l := range pool.All() {
		byID[l.ID] = l
	}
	scores := map[string]int{}
	for id, it := range s.Items {
		if it.RunID != runID || it.Stream != "listings.scored" {
			continue
		}
		var got scoredListing
		if err := json.Unmarshal([]byte(it.Payload), &got); err != nil {
			t.Fatalf("item %s: %v", id, err)
		}
		l := byID[got.ID]
		if !jsonEqual(t, got.Listing, l) || got.Score != pool.Score(l, penalty) || got.Reason == "" ||
			it.Key != l.ID {
			t.Errorf("item %s (key %s) = %+v, want %+v scored %d with a reason", id, it.Key, got, l,
				pool.Score(l, penalty))
		}
		scores[got.ID] = got.Score
	}
	return scores
}

func jsonEqual(t *testing.T, a, b any) bool {
	t.Helper()
	ja, err1 := json.Marshal(a)
	jb, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	return string(ja) == string(jb)
}

// transcripts reads the transcript of every score delivery of the run in db, by delivery: each delivery done, with
// exactly one transcript.json attempt artifact, read from the artifact store next to the database.
func transcripts(t *testing.T, db, runID string) map[string]agent.Transcript {
	t.Helper()
	st, err := sqlite.Open(db, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	ds, err := st.Deliveries(t.Context(), store.DeliveryFilter{Pipeline: m2Pipeline})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]agent.Transcript{}
	for _, d := range ds {
		if d.RunID != runID || d.Step != "score" {
			continue
		}
		arts, err := st.AttemptArtifacts(t.Context(), m2Pipeline, d.ID)
		if err != nil || d.State != store.DeliveryDone || len(arts) != 1 || arts[0].Name != agent.TranscriptFile ||
			arts[0].Attempt != 1 {
			t.Fatalf("score delivery %s is %s with attempt artifacts %+v (%v)", d.ID, d.State, arts, err)
		}
		data, err := os.ReadFile(filepath.Join(filepath.Dir(db), "artifacts", m2Pipeline, arts[0].SHA256))
		if err != nil {
			t.Fatal(err)
		}
		var tr agent.Transcript
		if err := json.Unmarshal(data, &tr); err != nil {
			t.Fatalf("transcript of %s: %v", d.ID, err)
		}
		out[d.ID] = tr
	}
	return out
}

// TestM2Scoring is the first M2 exit criterion: the M2 fixture runs a day with its agent scorer, replayed from the
// committed recordings. Every agent delivery is done with its transcript kept as an attempt artifact, its cost is
// priced from the environment file and recorded on the run and as metrics, and the insights projector reports the
// step's cost.
func TestM2Scoring(t *testing.T) {
	t.Parallel()
	db := dbPath(t)
	runID := ids.ManualRun("day-1", "adhoc")
	r := mustExit(t, 0, db, m2Env(m2Recordings), m2Run(fixtureM2, "day-1", 1)...)
	want := m2DayCounts(1, lessonsV1Penalty, true)
	if !strings.Contains(r.stderr, "agent running without a sandbox") {
		t.Errorf("the unsandboxed agent was not logged:\n%s", lastLines(r.stderr, 8))
	}
	snap := readSnapshot(t, db)
	if got := snap.counts(); !maps.Equal(got, want) || want["listings.shortlist"] == 0 ||
		want["listings.shortlist"] == want["listings.scored"] {
		t.Fatalf("counts = %v, want %v with a shortlist neither empty nor everything", got, want)
	}
	checkScores(t, snap, runID, lessonsV1Penalty)

	trs := transcripts(t, db, runID)
	var cost float64
	for id, tr := range trs {
		price := (float64(tr.TokensIn)*m2InPerMtok + float64(tr.TokensOut)*m2OutPerMtok) / 1e6
		if tr.Harness != pipeline.HarnessAPI || tr.Model != m2Model || len(tr.Output) == 0 || tr.Error != "" ||
			!strings.Contains(tr.Prompt, fmt.Sprintf("lower its score by %d points", lessonsV1Penalty)) ||
			tr.CostUSD <= 0 || math.Abs(tr.CostUSD-price) > 1e-12 {
			t.Errorf("transcript of %s: %+v, want the committed lessons in its prompt and a cost of %g", id, tr, price)
		}
		cost += tr.CostUSD
	}
	if len(trs) != want["listings.scored"] {
		t.Fatalf("%d score deliveries, want %d", len(trs), want["listings.scored"])
	}

	st, err := sqlite.Open(db, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	run, err := st.Run(t.Context(), m2Pipeline, runID)
	if err != nil || math.Abs(run.CostUSD-cost) > 1e-9 {
		t.Errorf("run cost %g (%v), want the transcripts' %g", run.CostUSD, err, cost)
	}
	rollups, err := st.MetricRollups(t.Context(), store.MetricFilter{Pipeline: m2Pipeline, Name: agent.MetricCostUSD})
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	var sum float64
	for _, ro := range rollups {
		n, sum = n+ro.Count, sum+ro.Sum
		if ro.Labels["model"] != m2Model {
			t.Errorf("cost_usd rollup labels %v, want the model", ro.Labels)
		}
	}
	if n != int64(len(trs)) || math.Abs(sum-cost) > 1e-9 {
		t.Errorf("cost_usd rollups: %d attempts costing %g, want %d costing %g", n, sum, len(trs), cost)
	}
	insights, err := st.Insights(t.Context(), store.InsightFilter{Pipeline: m2Pipeline})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(insights, func(in store.Insight) bool { return in.Kind == "cost" && in.Scope == "score" }) {
		t.Errorf("insights %+v, want the score step's cost", insights)
	}
}

// TestM2DedupeReplay is the second M2 exit criterion: seven days are crawled and scored by the fixture without its
// dedupe step, then the fixture with dedupe is applied and the seven days' crawls are replayed through dedupe. The
// replay keeps each listing once, in its own generation, and scores, prices and reports those alone.
func TestM2DedupeReplay(t *testing.T) {
	t.Parallel()
	db := dbPath(t)
	env := m2Env(m2Recordings)
	const days = 7
	for d := 1; d <= days; d++ {
		mustExit(t, 0, db, env, m2Run(fixtureM2NoDedupe, fmt.Sprintf("day-%d", d), d)...)
	}
	// Applying the fixture with dedupe resumes the last day's settled run: nothing new fires.
	mustExit(t, 0, db, env, m2Run(fixtureM2, fmt.Sprintf("day-%d", days), days)...)
	r := mustExit(t, 0, db, env, "replay", "--pipeline", m2Pipeline, "--step", "dedupe", "--since", "30d",
		"--request-id", "dedupe")
	replayID := ids.ReplayRun("dedupe")
	if r.stdout != replayID+"\n" {
		t.Fatalf("replay printed %q, want %s", r.stdout, replayID)
	}
	mustExit(t, 0, db, env, m2Run(fixtureM2, fmt.Sprintf("day-%d", days), days)...)

	snap := readSnapshot(t, db)
	if len(snap.Runs) != days+1 {
		t.Fatalf("runs = %v, want the seven days and the replay", snap.Runs)
	}
	crawled, distinct := 0, map[string]listing.Listing{}
	for d := 1; d <= days; d++ {
		runID := ids.ManualRun(fmt.Sprintf("day-%d", d), "adhoc")
		if got, want := onlyRun(snap, runID).counts(), m2DayCounts(d, lessonsV1Penalty, false); !maps.Equal(got, want) ||
			snap.Runs[runID] != store.RunSucceeded || snap.Revisions[runID] != 1 {
			t.Errorf("day %d: %s on revision %d with counts %v, want succeeded on 1 with %v", d, snap.Runs[runID],
				snap.Revisions[runID], got, want)
		}
		for _, l := range pool.Day(d) {
			crawled++
			distinct[l.ID] = l
		}
	}
	duplicates := crawled - len(distinct)
	t.Logf("%d listings crawled over %d days, %d distinct, %d duplicates", crawled, days, len(distinct), duplicates)
	if duplicates == 0 || len(distinct) == pool.Size {
		t.Fatalf("the fixture's days do not overlap usefully: %d distinct of %d crawled", len(distinct), crawled)
	}

	replay := onlyRun(snap, replayID)
	want := m2Counts(slices.Collect(maps.Values(distinct)), lessonsV1Penalty)
	want["listings.unique"] = len(distinct)
	if got := replay.counts(); !maps.Equal(got, want) || snap.Runs[replayID] != store.RunSucceeded ||
		snap.Revisions[replayID] != 2 {
		t.Fatalf("replay: %s on revision %d with counts %v, want succeeded on 2 with %v", snap.Runs[replayID],
			snap.Revisions[replayID], got, want)
	}
	filtered, passed := 0, 0
	for id, d := range replay.Deliveries {
		switch {
		case d.Step == "crawl":
			t.Errorf("the replay crawled again: %s", id)
		case d.Step == "dedupe" && d.State == store.DeliveryFiltered:
			filtered++
		case d.Step == "dedupe":
			passed++
		}
	}
	if filtered != duplicates || passed != len(distinct) {
		t.Fatalf("dedupe filtered %d and passed %d, want %d duplicates filtered and %d passed", filtered, passed,
			duplicates, len(distinct))
	}
	keys := map[string]bool{}
	for _, it := range replay.Items {
		if it.Generation != 1 {
			t.Errorf("replay item on %s has generation %d, want 1", it.Stream, it.Generation)
		}
		if it.Stream == "listings.unique" {
			keys[it.Key] = true
		}
	}
	if !slices.Equal(slices.Sorted(maps.Keys(keys)), slices.Sorted(maps.Keys(distinct))) {
		t.Errorf("dedupe passed the listings %v, want each crawled listing once", slices.Sorted(maps.Keys(keys)))
	}
	checkScores(t, snap, replayID, lessonsV1Penalty)
}

// TestM2LessonsReplay is the third M2 exit criterion: a day is crawled and scored, the score step's lessons file is
// edited, and replaying the day through score changes the scores of the listings the edited rule concerns, from
// recordings keyed on the new prompt.
func TestM2LessonsReplay(t *testing.T) {
	t.Parallel()
	db := dbPath(t)
	env := m2Env(m2Recordings)
	const day = 3
	request := fmt.Sprintf("day-%d", day)
	dayRun := ids.ManualRun(request, "adhoc")
	mustExit(t, 0, db, env, m2Run(fixtureM2, request, day)...)
	mustExit(t, 0, db, env, m2Run(fixtureM2Lessons, request, day)...) // applies the edit as revision 2
	r := mustExit(t, 0, db, env, "replay", "--pipeline", m2Pipeline, "--step", "score", "--run", dayRun,
		"--request-id", "lessons")
	replayID := ids.ReplayRun("lessons")
	if r.stdout != replayID+"\n" {
		t.Fatalf("replay printed %q, want %s", r.stdout, replayID)
	}
	mustExit(t, 0, db, env, m2Run(fixtureM2Lessons, request, day)...)

	snap := readSnapshot(t, db)
	if got, want := onlyRun(snap, dayRun).counts(), m2DayCounts(day, lessonsV1Penalty, true); !maps.Equal(got, want) {
		t.Fatalf("day %d counts = %v, want %v", day, got, want)
	}
	want := m2Counts(pool.Day(day), lessonsV2Penalty)
	if got := onlyRun(snap, replayID).counts(); !maps.Equal(got, want) || snap.Revisions[replayID] != 2 {
		t.Fatalf("replay on revision %d: counts = %v, want %v on 2", snap.Revisions[replayID], got, want)
	}
	before := checkScores(t, snap, dayRun, lessonsV1Penalty)
	after := checkScores(t, snap, replayID, lessonsV2Penalty)
	changed, wantChanged := 0, 0
	for _, l := range pool.Day(day) {
		if pool.Score(l, lessonsV1Penalty) != pool.Score(l, lessonsV2Penalty) {
			wantChanged++
		}
		if before[l.ID] != after[l.ID] {
			changed++
		}
	}
	t.Logf("day %d: %d of %d scores changed; shortlist %d before, %d after", day, changed, len(before),
		m2DayCounts(day, lessonsV1Penalty, true)["listings.shortlist"], want["listings.shortlist"])
	if changed != wantChanged || changed == 0 ||
		want["listings.shortlist"] == m2DayCounts(day, lessonsV1Penalty, true)["listings.shortlist"] {
		t.Fatalf("%d scores changed, want %d, and a different shortlist", changed, wantChanged)
	}

	// The replay's answers come from the recordings of the edited prompt, none of the day's.
	pkg, err := pipeline.Load(fixtureM2Lessons)
	if err != nil {
		t.Fatal(err)
	}
	act, err := pipeline.Resolver{}.Resolve(pkg, "./actions/score")
	if err != nil {
		t.Fatal(err)
	}
	keysOf := func(runID string, penalty int) map[string]bool {
		t.Helper()
		keys := map[string]bool{}
		for id, tr := range transcripts(t, db, runID) {
			if !strings.Contains(tr.Prompt, fmt.Sprintf("lower its score by %d points", penalty)) {
				t.Errorf("transcript of %s: prompt without the penalty %d:\n%s", id, penalty, tr.Prompt)
			}
			key := agent.Key(pipeline.HarnessAPI, agent.Request{Model: tr.Model, Prompt: tr.Prompt, Schema: act.Output})
			if _, err := os.Stat(filepath.Join(m2Recordings, key+".json")); err != nil {
				t.Errorf("transcript of %s: no recording %s: %v", id, key, err)
			}
			keys[key] = true
		}
		return keys
	}
	old, edited := keysOf(dayRun, lessonsV1Penalty), keysOf(replayID, lessonsV2Penalty)
	if len(old) != len(before) || len(edited) != len(after) {
		t.Fatalf("%d and %d distinct recording keys, want one per listing", len(old), len(edited))
	}
	for k := range edited {
		if old[k] {
			t.Errorf("the replay used the day's recording %s", k)
		}
	}
}

// m2CrashUnits are the units "run --once" on the M2 fixture reaches, with the agent path through lease, done and
// fail; renew needs a slow crawl the M1 matrix covers.
var m2CrashUnits = []string{"applyRevision", "fire", "lease", "done", "fail", "expireLeases", "filter", "dedupe",
	"rollup", "recordInsights"}

// m2Clean is the M2 fixture's crash reference: day 1 run once with crashListing's recording withheld, so its score
// delivery dead-letters with its transcript.
var m2Clean reference

// m2CleanRun returns the M2 crash reference.
func m2CleanRun(t *testing.T) snapshot {
	t.Helper()
	m2Clean.once.Do(func() {
		m2Clean.failed = true
		db := dbPath(t)
		mustExit(t, 1, db, m2Env(m2CrashRecordings), m2Run(fixtureM2, crashRequest, 1)...)
		m2Clean.snap, m2Clean.failed = readSnapshot(t, db), t.Failed()
	})
	if m2Clean.failed {
		t.Fatal("the M2 crash reference failed")
	}
	return m2Clean.snap
}

// TestM2CrashAtFailpoints extends the crash matrix to the M2 fixture: each failpoint of the units its run reaches is
// armed in turn, the run rerun without, and the outcome must be the clean one, the agent deliveries' transcripts
// included: each kept once per attempt that reported, never counted twice.
func TestM2CrashAtFailpoints(t *testing.T) {
	t.Parallel()
	want := m2CleanRun(t)
	if len(want.DeadLetters) != 1 || len(want.AttemptArtifacts) != m2DayCounts(1, 0, true)["listings.scored"] {
		t.Fatalf("the M2 crash reference has dead letters %v and transcripts %v, want the one of %s and one per "+
			"score delivery", want.DeadLetters, want.AttemptArtifacts, crashListing)
	}
	for id, n := range want.AttemptArtifacts {
		if n != 1 {
			t.Fatalf("delivery %s keeps %d transcripts in the clean run", id, n)
		}
	}
	var (
		mu  sync.Mutex
		hit = map[string]bool{}
	)
	env := m2Env(m2CrashRecordings)
	t.Run("matrix", func(t *testing.T) {
		for _, unit := range m2CrashUnits {
			for _, phase := range phases {
				name := unit + "." + phase
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					db := dbPath(t)
					r := millstream(t, db, append(failpointEnv(name), env...), m2Run(fixtureM2, crashRequest, 1)...)
					switch {
					case r.code == failpoint.ExitCode:
					case r.code == 1 && strings.Contains(r.stdout, string(store.RunCompletedWithErrors)):
					default:
						t.Fatalf("with %s armed: %s", name, r)
					}
					mu.Lock()
					hit[name] = r.code == failpoint.ExitCode
					mu.Unlock()
					mustExit(t, 1, db, env, m2Run(fixtureM2, crashRequest, 1)...)
					wantSame(t, readSnapshot(t, db), want)
				})
			}
		}
	})
	if t.Failed() {
		return
	}
	for _, unit := range m2CrashUnits {
		for _, phase := range phases {
			if !hit[unit+"."+phase] {
				t.Errorf("failpoint %s.%s was never hit on the M2 fixture", unit, phase)
			}
		}
	}
}
