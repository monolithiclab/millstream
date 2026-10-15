package engine

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

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

// emit pulls the one src delivery and completes it with outputs.
func (v *env) emit(outputs ...store.Output) {
	v.t.Helper()
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 || as[0].Lease.Step != "src" {
		v.t.Fatalf("pull src = %v, %v", as, err)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1, Outputs: outputs}); err != nil {
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
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(writePackage(t, builtinPipeline, "a"))
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

func TestBuiltinDedupeWindowWithTheEngineClock(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(writePackage(t, builtinPipeline, "a"))
	states := func(runID string) store.DeliveryState {
		v.t.Helper()
		v.emit(output("", `{"n":7}`))
		v.pull(10)
		return v.byStep(runID, "unique")[0].State
	}
	wantState := func(what string, got, want store.DeliveryState) {
		t.Helper()
		if got != want {
			t.Fatalf("%s: %s, want %s", what, got, want)
		}
	}
	wantState("first", states(v.fire("p", "adhoc", "r1")), store.DeliveryDone)
	v.clock.Advance(29 * 24 * time.Hour)
	wantState("29 days later", states(v.fire("p", "adhoc", "r2")), store.DeliveryFiltered)
	v.clock.Advance(24*time.Hour + time.Second)
	wantState("30 days and 1 second after the first", states(v.fire("p", "adhoc", "r3")), store.DeliveryDone)
}

func TestBuiltinConfigErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, step, class, message string
	}{
		{"filter expr does not compile", `{ action: filter, in: trigger.adhoc, config: { expr: "item.n >" } }`,
			ClassExpr, "filter of step s"},
		{"dedupe key does not compile", `{ action: dedupe, in: trigger.adhoc, config: { key: "item.(" } }`,
			ClassExpr, "dedupe key of step s"},
		{"dedupe window", `{ action: dedupe, in: trigger.adhoc, config: { key: "item.n", window: soon } }`,
			ClassConfig, "dedupe window of step s"},
		{"dedupe key fails", `{ action: dedupe, in: trigger.adhoc, config: { key: "item.missing.deeper" } }`,
			ClassExpr, "dedupe key of step s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newEnv(t, Options{})
			v.apply(writePackage(t, manualPipeline("p", "  s: "+tt.step+"\n")))
			id := v.fire("p", "adhoc", "r1")
			if got := v.pull(5); len(got) != 0 {
				t.Fatalf("pull = %v", got)
			}
			d := v.byStep(id, "s")[0]
			if d.State != store.DeliveryDead || d.ErrorClass != tt.class || !strings.Contains(d.ErrorMessage, tt.message) {
				t.Fatalf("delivery = %+v", d)
			}
			if r := v.run("p", id); r.State != store.RunCompletedWithErrors {
				t.Fatalf("run = %s", r.State)
			}
		})
	}
}

// inflightCount returns how many deliveries the engine counts in flight.
func (e *Engine) inflightCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.inflight)
}
