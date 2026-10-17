//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

const pipelineName = "m1-fixture"

// item is what a snapshot keeps of an item: everything but its timestamp. Artifacts is its artifact list as JSON.
type item struct {
	RunID, Stream, Step, Key string
	Generation               int64
	Parents                  string
	Payload                  string
	Artifacts                string
	ProducedBy               string
}

// delivery is what a snapshot keeps of a delivery: its run, step and state.
type delivery struct {
	RunID, Step string
	State       store.DeliveryState
}

// deadLetter is what a snapshot keeps of a dead letter: its run and state.
type deadLetter struct {
	RunID string
	State store.DeadLetterState
}

// snapshot is the outcome of the runs on a database, without the timestamps, attempts and lease details that
// legitimately differ between a crashed-and-resumed run and a clean one.
type snapshot struct {
	Runs        map[string]store.RunState
	Revisions   map[string]store.RevisionID // run id to revision
	Items       map[string]item
	Deliveries  map[string]delivery
	DeadLetters map[string]deadLetter
	// Refcounts is the reference count of each artifact items carry, by hash: an item counted twice after a crash
	// shows here.
	Refcounts map[string]int64
	// AttemptArtifacts is the number of attempt artifacts (agent transcripts) of each delivery that has some. Their
	// content holds timings, so they are compared by count, and each must be referenced exactly once per attempt
	// that keeps it: a transcript counted twice after a crash shows here or fails readSnapshot.
	AttemptArtifacts map[string]int
	// Audits are the ids of the fallback audits, every one ended: sampled deterministically, they are the same
	// after a crash, but an audit whose lease a crash cut ends failed rather than done, so states are not compared.
	Audits map[string]bool
}

// counts returns the number of items per stream.
func (s snapshot) counts() map[string]int {
	c := map[string]int{}
	for _, it := range s.Items {
		c[it.Stream]++
	}
	return c
}

// readSnapshot reads the database and checks the invariants every outcome must hold whatever crashed on the way:
// every run settled, every delivery terminal, item ids unique, and no two items of a run's generation with the same
// key on one stream (an output published twice).
func readSnapshot(t *testing.T, db string) snapshot {
	t.Helper()
	st := openStore(t, db)
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	s := snapshot{
		Runs: map[string]store.RunState{}, Revisions: map[string]store.RevisionID{}, Items: map[string]item{},
		Deliveries: map[string]delivery{}, DeadLetters: map[string]deadLetter{}, Refcounts: map[string]int64{},
		AttemptArtifacts: map[string]int{}, Audits: map[string]bool{},
	}
	runs, err := st.Runs(ctx, store.RunFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range runs {
		if r.SettledAt.IsZero() {
			t.Errorf("run %s is not settled: %s", r.ID, r.State)
		}
		s.Runs[r.ID], s.Revisions[r.ID] = r.State, r.Revision
	}
	dls, err := st.DeadLetters(ctx, store.DeadLetterFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, dl := range dls {
		s.DeadLetters[dl.ID] = deadLetter{RunID: dl.RunID, State: dl.State}
	}
	items, err := st.Items(ctx, store.ItemFilter{})
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]string{}
	for _, it := range items {
		if _, dup := s.Items[it.ID]; dup {
			t.Errorf("item id %s appears twice", it.ID)
		}
		payload := string(it.Payload)
		if _, ok := s.DeadLetters[it.ID]; ok {
			payload = withoutTimestamp(t, it.Payload)
		}
		artifacts, err := json.Marshal(it.Artifacts)
		if err != nil {
			t.Fatal(err)
		}
		s.Items[it.ID] = item{RunID: it.RunID, Stream: it.Stream, Step: it.Step, Key: it.Key,
			Generation: it.Generation, Parents: strings.Join(it.Parents, ","), Payload: payload,
			Artifacts: string(artifacts), ProducedBy: it.ProducedBy}
		if it.Key == "" {
			continue
		}
		k := fmt.Sprintf("%s/%d/%s/%s", it.RunID, it.Generation, it.Stream, it.Key)
		if other, dup := keys[k]; dup {
			t.Errorf("items %s and %s share run, generation, stream and key %s: an output was published twice",
				other, it.ID, k)
		}
		keys[k] = it.ID
	}
	arts, err := st.Artifacts(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range arts {
		s.Refcounts[a.SHA256] = a.Refcount
	}
	ds, err := st.Deliveries(ctx, store.DeliveryFilter{})
	if err != nil {
		t.Fatal(err)
	}
	attemptRefs := map[string]int64{}
	for _, d := range ds {
		switch d.State {
		case store.DeliveryDone, store.DeliveryFiltered, store.DeliveryDead, store.DeliveryCancelled:
		default:
			t.Errorf("delivery %s (%s) is %s, not terminal", d.ID, d.Step, d.State)
		}
		s.Deliveries[d.ID] = delivery{RunID: d.RunID, Step: d.Step, State: d.State}
		arts, err := st.AttemptArtifacts(ctx, d.Pipeline, d.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range arts {
			s.AttemptArtifacts[d.ID]++
			attemptRefs[a.SHA256]++
		}
	}
	for sha, n := range attemptRefs {
		if s.Refcounts[sha] != n {
			t.Errorf("attempt artifact %s has %d references, %d attempts keep it", sha, s.Refcounts[sha], n)
		}
		delete(s.Refcounts, sha)
	}
	audits, err := st.Audits(ctx, store.AuditFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range audits {
		if a.State == store.AuditPending || a.State == store.AuditLeased {
			t.Errorf("audit %s of delivery %s is %s after the run", a.ID, a.DeliveryID, a.State)
		}
		s.Audits[a.ID] = true
	}
	checkLedgers(t, st)
	return s
}

// rowQuerier is a store that runs a query behind its units (the store's QueryRow, on its read pool).
type rowQuerier interface {
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row
}

// checkLedgers checks the invariants of the tables the store API does not show whole: no reservation outlives its
// lease; what the day budgets were charged, the runs' costs and the deliveries' costs add up to the same; and the
// dedupe keys hold one row per key let through, the row naming the item of a done delivery of its step. The last
// assumes no dedupe window elapses during a test, so no key passes twice.
func checkLedgers(t *testing.T, st store.Store) {
	t.Helper()
	conn, ok := st.(rowQuerier)
	if !ok {
		t.Fatalf("%T has no QueryRow", st)
	}
	count := func(query string) float64 {
		t.Helper()
		var n float64
		if err := conn.QueryRow(t.Context(), query).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}
	if n := count(`SELECT COUNT(*) FROM deliveries WHERE reserved_usd != 0`); n != 0 {
		t.Errorf("%g deliveries keep a budget reservation after the runs settled", n)
	}
	if n := count(`SELECT COUNT(*) FROM audits WHERE reserved_usd != 0`); n != 0 {
		t.Errorf("%g audits keep a budget reservation after they ended", n)
	}
	// Audits are charged to the day, never to a run (spec section 11.5).
	days := count(`SELECT COALESCE(SUM(spent_usd), 0) FROM budget_days`)
	audits := count(`SELECT COALESCE(SUM(cost_usd), 0) FROM audits`)
	runs := count(`SELECT COALESCE(SUM(cost_usd), 0) FROM runs`)
	deliveries := count(`SELECT COALESCE(SUM(cost_usd), 0) FROM deliveries`)
	if math.Abs(days-audits-runs) > 1e-9 || math.Abs(runs-deliveries) > 1e-9 {
		t.Errorf("spend disagrees: budget days %g, audits %g, runs %g, deliveries %g", days, audits, runs, deliveries)
	}
	if n := count(`SELECT COUNT(*) FROM dedupe_keys k WHERE NOT EXISTS (SELECT 1 FROM deliveries d
		WHERE d.pipeline = k.pipeline AND d.step = k.step AND d.generation = k.generation AND d.item_id = k.first_item
		AND d.state = 'done')`); n != 0 {
		t.Errorf("%g dedupe keys name an item no done delivery of their step let through", n)
	}
	keys := count(`SELECT COUNT(*) FROM dedupe_keys`)
	passed := count(`SELECT COUNT(*) FROM deliveries d WHERE d.state = 'done' AND EXISTS (SELECT 1 FROM dedupe_keys k
		WHERE k.pipeline = d.pipeline AND k.step = d.step)`)
	if keys != passed {
		t.Errorf("%g dedupe keys for %g deliveries let through", keys, passed)
	}
}

// withoutTimestamp returns a dead letter's payload without the timestamp of the item it copies, which is when that
// item was first published and so differs between databases.
func withoutTimestamp(t *testing.T, payload []byte) string {
	t.Helper()
	var dl map[string]any
	if err := json.Unmarshal(payload, &dl); err != nil {
		t.Fatalf("dead letter payload: %v", err)
	}
	if it, ok := dl["item"].(map[string]any); ok {
		delete(it, "ts")
	}
	b, err := json.Marshal(dl)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// wantSame fails the test with the differences when got is not want.
func wantSame(t *testing.T, got, want snapshot) {
	t.Helper()
	diff := slices.Concat(
		diffMaps("run", got.Runs, want.Runs),
		diffMaps("run revision", got.Revisions, want.Revisions),
		diffMaps("item", got.Items, want.Items),
		diffMaps("delivery", got.Deliveries, want.Deliveries),
		diffMaps("dead letter", got.DeadLetters, want.DeadLetters),
		diffMaps("artifact refcount", got.Refcounts, want.Refcounts),
		diffMaps("attempt artifacts of delivery", got.AttemptArtifacts, want.AttemptArtifacts),
		diffMaps("audit", got.Audits, want.Audits),
	)
	if len(diff) > 0 {
		if len(diff) > 20 {
			diff = append(diff[:20], fmt.Sprintf("… and %d more", len(diff)-20))
		}
		t.Errorf("outcome differs from the clean run:\n  counts got %v\n  want %v\n%s", got.counts(), want.counts(),
			strings.Join(diff, "\n"))
	}
}

func diffMaps[V comparable](what string, got, want map[string]V) []string {
	var out []string
	for _, k := range slices.Sorted(maps.Keys(want)) {
		g, ok := got[k]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("  %s %s missing, want %v", what, k, want[k]))
		case g != want[k]:
			out = append(out, fmt.Sprintf("  %s %s = %v, want %v", what, k, g, want[k]))
		}
	}
	for _, k := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[k]; !ok {
			out = append(out, fmt.Sprintf("  %s %s unexpected: %v", what, k, got[k]))
		}
	}
	return out
}

// crashRequest is the request id of every run the crash suite fires, so every crashed database ends up with the
// same deterministic ids as the clean reference.
const crashRequest = "crash"

// runOnce returns the arguments of "run --once" on dir, with flags, under the crash request id. The 1 s lease makes a crashed
// run's leases expire quickly on the rerun, and makes the embedded node heartbeat (renew) during the run.
func runOnce(dir string, flags ...string) []string {
	return slices.Concat([]string{"run", "--once", "--request-id", crashRequest, "--lease-ttl", "1s"}, flags,
		[]string{dir})
}

// runBuiltins is runOnce of the builtins fixture with the pipeline importing its stream.
func runBuiltins(flags ...string) []string {
	return append(runOnce(fixtureBuiltins, flags...), fixtureImporter)
}

// reference is an outcome computed once per backend and shared by the tests that compare against it.
type reference struct {
	mu   sync.Mutex
	runs map[string]*referenceRun // by backend
}

type referenceRun struct {
	once   sync.Once
	snap   snapshot
	failed bool
}

// on returns the reference's run on b.
func (r *reference) on(b backend) *referenceRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.runs == nil {
		r.runs = map[string]*referenceRun{}
	}
	if r.runs[b.name] == nil {
		r.runs[b.name] = &referenceRun{}
	}
	return r.runs[b.name]
}

// compute runs fn once per backend, the first time the reference is asked for on it, and returns its outcome.
func (r *reference) compute(t *testing.T, b backend, what string, fn func(db string)) snapshot {
	t.Helper()
	run := r.on(b)
	run.once.Do(func() {
		run.failed = true
		db := b.newDB(t)
		fn(db)
		run.snap, run.failed = readSnapshot(t, db), t.Failed()
	})
	if run.failed {
		t.Fatalf("the %s reference on %s failed", what, b.name)
	}
	return run.snap
}

var (
	// clean is the fixture run once on v1, without a crash: its designed dead letter left open.
	clean reference
	// cleanBuiltins is the builtins fixture run once, without a crash.
	cleanBuiltins reference
	// replayed is clean followed by the dead-letter replay onto v2, without a crash.
	replayed reference
	// replayedStep is clean followed by a replay run of score over the run, without a crash.
	replayedStep reference
)

// cleanRun returns the clean reference on SQLite.
func cleanRun(t *testing.T) snapshot {
	t.Helper()
	return cleanRunOn(t, sqliteBackend)
}

// cleanRunOn returns the clean reference on b.
func cleanRunOn(t *testing.T, b backend) snapshot {
	t.Helper()
	return clean.compute(t, b, "clean", func(db string) { mustExit(t, 1, db, nil, runOnce(fixtureV1)...) })
}

// cleanBuiltinsRun returns the clean reference of the builtins fixture on SQLite.
func cleanBuiltinsRun(t *testing.T) snapshot {
	t.Helper()
	return cleanBuiltinsRunOn(t, sqliteBackend)
}

// cleanBuiltinsRunOn returns the clean reference of the builtins fixture on b.
func cleanBuiltinsRunOn(t *testing.T, b backend) snapshot {
	t.Helper()
	return cleanBuiltins.compute(t, b, "clean builtins", func(db string) {
		mustExit(t, 1, db, nil, runBuiltins()...)
	})
}

// replayedRunOn returns the replayed reference on b.
func replayedRunOn(t *testing.T, b backend) snapshot {
	t.Helper()
	return replayed.compute(t, b, "replayed", func(db string) {
		mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
		mustExit(t, 1, db, nil, runOnce(fixtureV2)...)
		mustExit(t, 0, db, nil, replayArgs()...)
		mustExit(t, 0, db, nil, runOnce(fixtureV2)...)
	})
}

// replayedStepRun returns the replayedStep reference on SQLite.
func replayedStepRun(t *testing.T) snapshot {
	t.Helper()
	return replayedStepRunOn(t, sqliteBackend)
}

// replayedStepRunOn returns the replayedStep reference on b.
func replayedStepRunOn(t *testing.T, b backend) snapshot {
	t.Helper()
	return replayedStep.compute(t, b, "replayed step", func(db string) {
		mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
		if r := mustExit(t, 0, db, nil, replayStepArgs()...); r.stdout != replayStepRun+"\n" {
			t.Errorf("replay printed %q, want the replay run id", r.stdout)
		}
		mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
	})
}

// replayStepRun is the id of the replay run replayStepArgs starts.
var replayStepRun = ids.ReplayRun(crashRequest)

// replayStepArgs replays score over the crash suite's run, under the crash request id.
func replayStepArgs() []string {
	return []string{"replay", "--pipeline", pipelineName, "--step", "score", "--run", crashRun,
		"--request-id", crashRequest}
}

// crashRun is the id of the run the crash suite fires.
var crashRun = ids.ManualRun(crashRequest, "adhoc")

func replayArgs() []string {
	return []string{"replay", "--from", "dlq", "--pipeline", pipelineName, "--run", crashRun, "--migrate"}
}
