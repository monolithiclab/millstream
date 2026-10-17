package engine

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/store"
)

// builtinPipeline runs a source through a filter then a dedupe before a sink.
const builtinPipeline = `pipeline: p
vars: { min: 5 }
triggers:
  adhoc: { manual: {} }
steps:
  src: { action: ./actions/a, in: trigger.adhoc, out: raw }
  gate: { action: filter, in: raw, out: kept, config: { expr: "item.n >= vars.min" } }
  unique: { action: dedupe, in: kept, out: uniq, config: { key: "string(item.n)", window: 30d } }
  sink: { action: ./actions/a, in: uniq, concurrency: 5 }
`

// emit pulls the one src delivery due and completes it with outputs; whatever else the pull leased stays leased.
func (v *env) emit(outputs ...store.Output) {
	v.t.Helper()
	as, err := v.e.Pull(v.ctx, "node-a", 100)
	if err != nil {
		v.t.Fatal(err)
	}
	i := slices.IndexFunc(as, func(a node.Assignment) bool { return a.Lease.Step == "src" })
	if i < 0 {
		v.t.Fatalf("no src delivery in %v", as)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[i].Lease.Ref(), Attempt: 1, Outputs: outputs}); err != nil {
		v.t.Fatal(err)
	}
}

func output(key, body string) store.Output {
	return store.Output{Key: key, Payload: json.RawMessage(body)}
}

// byStep returns the run's deliveries of step, in creation order.
func (v *env) byStep(runID, step string) []store.Delivery {
	v.t.Helper()
	var out []store.Delivery
	for _, d := range v.deliveries(runID) {
		if d.Step == step {
			out = append(out, d)
		}
	}
	return out
}

func TestBuiltinsRunInTheEngine(t *testing.T) {
	v := newEnv(t, Options{})
	v.apply(newPackage(t, builtinPipeline, "a"))
	id := v.fire("p", "adhoc", "r1")
	v.emit(output("low", `{"n":1}`), output("a", `{"n":7}`), output("b", `{"n":7}`), output("bad", `{"n":"x"}`),
		output("c", `{"n":9}`))
	got := v.pull(10)
	if !slices.Equal(got, []string{"p/sink", "p/sink"}) {
		t.Fatalf("pull = %v: a node must receive only the sink deliveries", got)
	}
	gate := v.byStep(id, "gate")
	wantStates := []store.DeliveryState{store.DeliveryFiltered, store.DeliveryDone, store.DeliveryDone,
		store.DeliveryDead, store.DeliveryDone}
	for i, d := range gate {
		if d.State != wantStates[i] {
			t.Fatalf("gate delivery %d is %s, want %s", i, d.State, wantStates[i])
		}
	}
	if gate[0].FilterReason != store.FilterReasonExpr {
		t.Fatalf("filtered reason = %q", gate[0].FilterReason)
	}
	if gate[3].ErrorClass != ClassExpr || !strings.Contains(gate[3].ErrorMessage, "no such overload") ||
		gate[3].Attempt != 1 {
		t.Fatalf("CEL error delivery = %+v, want dead at once with class expr", gate[3])
	}
	unique := v.byStep(id, "unique")
	if len(unique) != 3 || unique[0].State != store.DeliveryDone || unique[1].State != store.DeliveryFiltered ||
		unique[1].FilterReason != store.FilterReasonDuplicate || unique[2].State != store.DeliveryDone {
		t.Fatalf("unique deliveries = %+v", unique)
	}
	kept, err := v.st.Items(v.ctx, store.ItemFilter{Pipeline: "p", Stream: "kept"})
	if err != nil || len(kept) != 3 || kept[0].Key != "a" || string(kept[0].Payload) != `{"n":7}` {
		t.Fatalf("kept = %+v, %v: the filter passes items on unchanged, keys included", kept, err)
	}
	if v.e.inflightCount() != 2 {
		t.Fatalf("in flight = %d, want the two sink leases only", v.e.inflightCount())
	}
	if logs := v.logs.String(); !strings.Contains(logs, `"state":"filtered"`) {
		t.Fatalf("no filtered delivery logged:\n%s", logs)
	}
}

// A backlog of builtins runs inside Pull, a lease at a time, while the leases already resolved for the node wait
// unrenewed for Pull to return. Pull returns
// what it has once a third of the lease has passed, so no lease it hands out has expired meanwhile: with a 1 s lease
// under load, a backlog left a lease to expire before its delivery started, each such expiry consuming an attempt
// (the failover suite saw one delivery lose all three, spec section 5.3).
func TestPullReturnsBeforeItsLeasesExpire(t *testing.T) {
	v := newEnv(t, Options{LeaseTTL: time.Second})
	v.apply(newPackage(t, builtinPipeline, "a"))
	v.fire("p", "adhoc", "r1")
	var outs []store.Output
	for n := range 6 {
		outs = append(outs, output(fmt.Sprint("k", n), fmt.Sprintf(`{"n":%d}`, 5+n)))
	}
	v.emit(outs...) // p's backlog: 6 filters, then 6 dedupes
	v.apply(newPackage(t, "pipeline: s\ntriggers: { adhoc: { manual: {} } }\nsteps:\n  job: { action: ./actions/a, in: trigger.adhoc }\n", "a"))
	v.fire("s", "adhoc", "r1")
	// Each look at the engine's clock takes 100 ms, the store's included: time passes as builtins run. The round-robin
	// leases s's job with p's first filter, then only p's builtins.
	now := v.e.opts.Now
	v.e.opts.Now = func() time.Time { v.clock.Advance(100 * time.Millisecond); return now() }
	as, err := v.e.Pull(v.ctx, "node-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if expired, err := v.st.ExpireLeases(v.ctx); err != nil || len(expired) != 0 {
		t.Fatalf("expired after the pull: %+v, %v; Pull held a lease past its expiry", expired, err)
	}
	if len(as) != 1 || as[0].Lease.Step != "job" {
		t.Fatalf("pulled %d assignments; want s's job alone", len(as))
	}
	if pending, err := v.st.Deliveries(v.ctx, store.DeliveryFilter{Pipeline: "p", State: store.DeliveryPending}); err != nil ||
		len(pending) == 0 {
		t.Fatalf("p's pending deliveries = %d, %v: Pull ran the whole backlog", len(pending), err)
	}
	// A remote node's pull leases within what is left of its wait: with none left, one pass of the loop.
	before, _ := v.st.Deliveries(v.ctx, store.DeliveryFilter{Pipeline: "p", State: store.DeliveryPending})
	v.e.opts.Now = now
	if as, err := v.e.PullWithin(v.ctx, "node-a", 2, 0); err != nil || len(as) != 0 {
		t.Fatalf("PullWithin = %v, %v", as, err)
	}
	after, _ := v.st.Deliveries(v.ctx, store.DeliveryFilter{Pipeline: "p", State: store.DeliveryPending})
	if len(before)-len(after) > 2 {
		t.Fatalf("PullWithin with no time left ran %d builtins, want one pass of at most 2",
			len(before)-len(after))
	}
}

// Builtin steps whose configuration fails fail their deliveries for good: the cases are the steps of one run.
func TestBuiltinConfigErrors(t *testing.T) {
	tests := []struct {
		step, def, class, message string
	}{
		{"filterexpr", `{ action: filter, in: trigger.adhoc, config: { expr: "item.n >" } }`,
			ClassExpr, "filter of step filterexpr"},
		{"dedupekey", `{ action: dedupe, in: trigger.adhoc, config: { key: "item.(" } }`,
			ClassExpr, "dedupe key of step dedupekey"},
		{"window", `{ action: dedupe, in: trigger.adhoc, config: { key: "item.n", window: soon } }`,
			ClassConfig, "dedupe window of step window"},
		{"keyfails", `{ action: dedupe, in: trigger.adhoc, config: { key: "item.missing.deeper" } }`,
			ClassExpr, "dedupe key of step keyfails"},
	}
	steps := ""
	for _, tt := range tests {
		steps += "  " + tt.step + ": " + tt.def + "\n"
	}
	v := newEnv(t, Options{})
	v.apply(newPackage(t, manualPipeline("p", steps)))
	id := v.fire("p", "adhoc", "r1")
	if got := v.pull(5); len(got) != 0 {
		t.Fatalf("pull = %v", got)
	}
	for _, tt := range tests {
		d := v.byStep(id, tt.step)[0]
		if d.State != store.DeliveryDead || d.ErrorClass != tt.class || !strings.Contains(d.ErrorMessage, tt.message) {
			t.Errorf("%s delivery = %+v", tt.step, d)
		}
	}
	if r := v.run("p", id); r.State != store.RunCompletedWithErrors {
		t.Fatalf("run = %s", r.State)
	}
}

// inflightCount returns how many deliveries the engine counts in flight.
func (e *Engine) inflightCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.inflight)
}
