package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

const pipelineName = "m1-fixture"

// item is what a snapshot keeps of an item: everything but its timestamp.
type item struct {
	RunID, Stream, Step, Key string
	Generation               int64
	Parents                  string
	Payload                  string
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
	st, err := sqlite.Open(db, sqlite.Options{})
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	s := snapshot{
		Runs: map[string]store.RunState{}, Revisions: map[string]store.RevisionID{}, Items: map[string]item{},
		Deliveries: map[string]delivery{}, DeadLetters: map[string]deadLetter{},
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
		s.Items[it.ID] = item{RunID: it.RunID, Stream: it.Stream, Step: it.Step, Key: it.Key,
			Generation: it.Generation, Parents: strings.Join(it.Parents, ","), Payload: payload}
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
	ds, err := st.Deliveries(ctx, store.DeliveryFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		switch d.State {
		case store.DeliveryDone, store.DeliveryFiltered, store.DeliveryDead, store.DeliveryCancelled:
		default:
			t.Errorf("delivery %s (%s) is %s, not terminal", d.ID, d.Step, d.State)
		}
		s.Deliveries[d.ID] = delivery{RunID: d.RunID, Step: d.Step, State: d.State}
	}
	return s
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

// reference is an outcome computed once and shared by the tests that compare against it.
type reference struct {
	once   sync.Once
	snap   snapshot
	failed bool
}

var (
	// clean is the fixture run once on v1, without a crash: its designed dead letter left open.
	clean reference
	// replayed is clean followed by the dead-letter replay onto v2, without a crash.
	replayed reference
)

// cleanRun returns the clean reference.
func cleanRun(t *testing.T) snapshot {
	t.Helper()
	clean.once.Do(func() {
		clean.failed = true
		db := dbPath(t)
		mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
		clean.snap, clean.failed = readSnapshot(t, db), t.Failed()
	})
	if clean.failed {
		t.Fatal("the clean reference run failed")
	}
	return clean.snap
}

// replayedRun returns the replayed reference.
func replayedRun(t *testing.T) snapshot {
	t.Helper()
	replayed.once.Do(func() {
		replayed.failed = true
		db := dbPath(t)
		mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
		mustExit(t, 1, db, nil, runOnce(fixtureV2)...)
		mustExit(t, 0, db, nil, replayArgs()...)
		mustExit(t, 0, db, nil, runOnce(fixtureV2)...)
		replayed.snap, replayed.failed = readSnapshot(t, db), t.Failed()
	})
	if replayed.failed {
		t.Fatal("the replayed reference run failed")
	}
	return replayed.snap
}

// crashRun is the id of the run the crash suite fires.
var crashRun = ids.ManualRun(crashRequest, "adhoc")

func replayArgs() []string {
	return []string{"replay", "--from", "dlq", "--pipeline", pipelineName, "--run", crashRun, "--migrate"}
}
