package engine

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/store"
)

// replayPipeline is a source feeding a scorer whose config the tests change between revisions.
const replayPipeline = `pipeline: p
vars: { label: v }
triggers:
  adhoc: { manual: {} }
steps:
  src: { action: ./actions/a, in: trigger.adhoc, out: raw }
  score: { action: ./actions/a, in: raw, out: scored, concurrency: 10, config: { rev: one } }
`

// srcItems fires run key, completes its src with outputs n, and returns the run id.
func (v *env) srcRun(key string, ns ...string) string {
	v.t.Helper()
	id := v.fire("p", "adhoc", key)
	var outs []store.Output
	for _, n := range ns {
		outs = append(outs, output("k"+n, `{"n":`+n+`}`))
	}
	v.emit(outs...)
	return id
}

// pullAll pulls every delivery a node may take now.
func (v *env) pullAll() []node.Assignment {
	v.t.Helper()
	as, err := v.e.Pull(v.ctx, "node-a", 100)
	if err != nil {
		v.t.Fatalf("Pull: %v", err)
	}
	return as
}

func (v *env) replay(req ReplayRequest) string {
	v.t.Helper()
	req.Pipeline = "p"
	id, created, err := v.e.Replay(v.ctx, req)
	if err != nil || !created {
		v.t.Fatalf("Replay %+v = %v, %v", req, created, err)
	}
	return id
}

func TestReplaySelection(t *testing.T) {
	v := newEnv(t, Options{})
	v.apply(newPackage(t, replayPipeline, "a"))
	r1 := v.srcRun("r1", "1", "7")
	v.clock.Advance(48 * time.Hour)
	v.srcRun("r2", "3", "9")
	v.pull(10) // the original score deliveries are leased, whatever the replays select

	count := func(id string) int { return len(v.byStep(id, "score")) }
	tests := []struct {
		name string
		req  ReplayRequest
		want int
	}{
		{"all", ReplayRequest{Step: "score", RequestID: "all"}, 4},
		{"one run", ReplayRequest{Step: "score", RunID: r1, RequestID: "run"}, 2},
		{"since", ReplayRequest{Step: "score", Since: 24 * time.Hour, RequestID: "since"}, 2},
		{"where", ReplayRequest{Step: "score", Where: "item.n > 5 && envelope.key != ''", RequestID: "where"}, 2},
		{"where with vars and run", ReplayRequest{Step: "score", RunID: r1,
			Where: "vars.label == 'v' && run.id.startsWith('run_replay_') && item.n == 7", RequestID: "vars"}, 1},
	}
	gens := map[int64]bool{0: true}
	for _, tt := range tests {
		id := v.replay(tt.req)
		if id != ids.ReplayRun(tt.req.RequestID) || count(id) != tt.want {
			t.Errorf("%s: run %s with %d deliveries, want %d", tt.name, id, count(id), tt.want)
		}
		r := v.run("p", id)
		if r.Kind != store.RunKindReplay || gens[r.Generation] || r.Vars["label"] != "v" {
			t.Errorf("%s: run %+v", tt.name, r)
		}
		gens[r.Generation] = true
	}
	if _, created, err := v.e.Replay(v.ctx, ReplayRequest{Pipeline: "p", Step: "score", RequestID: "all"}); err != nil ||
		created {
		t.Fatalf("the same request again = %v, %v", created, err)
	}
	for _, tt := range []struct {
		name string
		req  ReplayRequest
		want error
	}{
		{"nothing selected", ReplayRequest{Step: "score", Where: "item.n > 100"}, store.ErrInvalid},
		{"where does not compile", ReplayRequest{Step: "score", Where: "item.n >"}, store.ErrInvalid},
		{"where fails", ReplayRequest{Step: "score", Where: "item.missing > 1"}, store.ErrInvalid},
		{"unknown step", ReplayRequest{Step: "nope"}, store.ErrInvalid},
		{"unknown revision", ReplayRequest{Step: "score", Revision: 99}, store.ErrNotFound},
	} {
		tt.req.Pipeline = "p"
		if _, _, err := v.e.Replay(v.ctx, tt.req); !errors.Is(err, tt.want) {
			t.Errorf("%s: %v, want %v", tt.name, err, tt.want)
		}
	}
	if _, _, err := v.e.Replay(v.ctx, ReplayRequest{Pipeline: "nope", Step: "score"}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown pipeline: %v", err)
	}
}

// TestReplayRevisions: a replay runs on the active revision, or the one named; its deliveries run the step as that
// revision defines it.
func TestReplayRevisions(t *testing.T) {
	v := newEnv(t, Options{})
	rev1 := v.apply(newPackage(t, replayPipeline, "a"))
	run := v.srcRun("r1", "1")
	rev2 := v.apply(newPackage(t, strings.Replace(replayPipeline, "rev: one", "rev: two", 1), "a"))
	other := v.apply(newPackage(t, strings.Replace(replayPipeline, "pipeline: p", "pipeline: q", 1), "a"))
	configs := func(id string) []any {
		var out []any
		for _, l := range v.pullAll() {
			if l.Lease.RunID == id {
				out = append(out, l.Spec.Config["rev"])
			}
		}
		return out
	}
	active := v.replay(ReplayRequest{Step: "score", RunID: run, RequestID: "active"})
	if r := v.run("p", active); r.Revision != rev2 {
		t.Fatalf("replay on the active revision runs on %d, want %d", r.Revision, rev2)
	}
	if got := configs(active); !slices.Equal(got, []any{"two"}) {
		t.Fatalf("configs on the active revision = %v", got)
	}
	old := v.replay(ReplayRequest{Step: "score", RunID: run, Revision: rev1, RequestID: "old"})
	if r := v.run("p", old); r.Revision != rev1 {
		t.Fatalf("replay on revision 1 runs on %d", r.Revision)
	}
	if got := configs(old); !slices.Equal(got, []any{"one"}) {
		t.Fatalf("configs on revision 1 = %v", got)
	}
	if _, _, err := v.e.Replay(v.ctx, ReplayRequest{Pipeline: "p", Step: "score", Revision: other}); !errors.Is(err,
		store.ErrInvalid) {
		t.Fatalf("a revision of another pipeline: %v", err)
	}
}
