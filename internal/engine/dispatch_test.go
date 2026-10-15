package engine

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

func manualPipeline(name, steps string) string {
	return "pipeline: " + name + "\ntriggers:\n  adhoc: { manual: {} }\nsteps:\n" + steps
}

func fireN(v *env, pipelineName string, n int) []string {
	var out []string
	for i := range n {
		out = append(out, v.fire(pipelineName, "adhoc", "r"+string(rune('a'+i))))
	}
	return out
}

func TestAssignment(t *testing.T) {
	t.Parallel()
	environ := map[string]string{
		"PATH": "/usr/bin", "HOME": "/home/u", "MILLSTREAM_VAR_MAX_PRICE": "250000",
		"MILLSTREAM_SECRET_API_TOKEN": "tok", "OTHER": "never passed",
	}
	v := newEnv(t, Options{Env: func(k string) (string, bool) { s, ok := environ[k]; return s, ok }})
	pkg := writePackage(t, strings.Replace(varsPipeline, "config:", "timeout: 30s\n    config:", 1), "crawl")
	v.apply(pkg)
	id := v.fire("listings", "adhoc", "req1")
	as, err := v.e.Pull(v.ctx, "node-a", 5)
	if err != nil || len(as) != 1 {
		t.Fatalf("Pull = %v, %v", as, err)
	}
	a := as[0]
	wantConfig := map[string]any{"max": int64(250000), "token": "tok", "label": "Paris run adhoc"}
	for k, want := range wantConfig {
		if a.Spec.Config[k] != want {
			t.Fatalf("config = %v, want %v", a.Spec.Config, wantConfig)
		}
	}
	if !slices.Equal(a.Spec.Env, []string{"PATH=/usr/bin", "HOME=/home/u"}) {
		t.Fatalf("env = %v", a.Spec.Env)
	}
	if a.Spec.Action != "fake" || a.Spec.Version != "1.0.0" || !strings.HasPrefix(a.Spec.Digest, "sha256:") ||
		a.Spec.Lifecycle != pipeline.LifecycleLong || a.Spec.Dir != pkg.Dir+"/actions/crawl" ||
		a.Spec.Command[0] != pkg.Dir+"/actions/crawl/run" || a.Spec.Vars["maxPrice"] != int64(250000) {
		t.Fatalf("spec = %+v", a.Spec)
	}
	rev, err := v.st.Revision(v.ctx, a.Lease.Revision)
	if err != nil {
		t.Fatal(err)
	}
	m := a.ItemMsg
	if m.Delivery != a.Lease.DeliveryID || m.ID != ids.TriggerItem("adhoc", "req1") || m.Key != "req1" ||
		string(m.Payload) != "{}" || m.Run.ID != id || m.Run.Trigger != "adhoc" || m.Run.Revision != rev.PackageHash ||
		m.Attempt != 1 || m.IdempotencyKey != ids.Idempotency("crawl", m.ID, 0) || m.Artifacts == nil {
		t.Fatalf("item message = %+v", m)
	}
	if a.Timeout != 30*time.Second {
		t.Fatalf("timeout = %s", a.Timeout)
	}
}

func TestConcurrencyAndLimits(t *testing.T) {
	t.Parallel()
	t.Run("step concurrency", func(t *testing.T) {
		t.Parallel()
		v := newEnv(t, Options{})
		v.apply(writePackage(t, manualPipeline("p", `
  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 2 }
`), "a"))
		fireN(v, "p", 5)
		as, err := v.e.Pull(v.ctx, "node-a", 10)
		if err != nil || len(as) != 2 {
			t.Fatalf("first pull = %d, %v; want 2", len(as), err)
		}
		if got := v.pull(10); len(got) != 0 {
			t.Fatalf("pull at capacity = %v", got)
		}
		if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1}); err != nil {
			t.Fatal(err)
		}
		if got := v.pull(10); len(got) != 1 {
			t.Fatalf("pull after one done = %v, want 1", got)
		}
	})
	t.Run("pipeline limit", func(t *testing.T) {
		t.Parallel()
		v := newEnv(t, Options{})
		v.apply(writePackage(t, manualPipeline("p", `
  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 5 }
  b: { action: ./actions/a, in: trigger.adhoc, concurrency: 5 }
limits: { concurrency: 3 }
`), "a"))
		fireN(v, "p", 4)
		if got := v.pull(10); len(got) != 3 {
			t.Fatalf("pull = %v, want 3", got)
		}
		if got := v.pull(10); len(got) != 0 {
			t.Fatalf("pull at the limit = %v", got)
		}
	})
	t.Run("leases of a previous engine count until they expire", func(t *testing.T) {
		t.Parallel()
		v := newEnv(t, Options{LeaseTTL: time.Minute})
		pkg := writePackage(t, manualPipeline("p", `
  a: { action: ./actions/a, in: trigger.adhoc }
`), "a")
		v.apply(pkg)
		fireN(v, "p", 2)
		if got := v.pull(10); len(got) != 1 {
			t.Fatalf("pull = %v", got)
		}
		e2, err := New(Options{Store: v.st, Now: v.clock.Now})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e2.Apply(v.ctx, pkg); err != nil {
			t.Fatal(err)
		}
		if err := e2.resync(v.ctx); err != nil {
			t.Fatal(err)
		}
		if as, err := e2.Pull(v.ctx, "node-b", 10); err != nil || len(as) != 0 {
			t.Fatalf("pull with the old lease held = %d, %v", len(as), err)
		}
		v.clock.Advance(2 * time.Minute)
		e2.expire(v.ctx)
		if as, err := e2.Pull(v.ctx, "node-b", 10); err != nil || len(as) != 1 {
			t.Fatalf("pull after expiry = %d, %v", len(as), err)
		}
	})
}

func TestRateLimit(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(writePackage(t, manualPipeline("p", `
  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 10, rate: 2/min }
`), "a"))
	fireN(v, "p", 6)
	steps := []struct {
		advance time.Duration
		want    int
	}{
		{0, 2}, {0, 0}, {30 * time.Second, 1}, {10 * time.Second, 0}, {5 * time.Minute, 2},
	}
	for i, s := range steps {
		v.clock.Advance(s.advance)
		if got := v.pull(10); len(got) != s.want {
			t.Fatalf("step %d: pulled %d, want %d", i, len(got), s.want)
		}
	}
}

func TestRoundRobinAcrossPipelines(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	for _, name := range []string{"p1", "p2"} {
		v.apply(writePackage(t, manualPipeline(name, `
  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 10 }
`), "a"))
		fireN(v, name, 3)
	}
	for i := range 3 {
		got := v.pull(2)
		slices.Sort(got)
		if !slices.Equal(got, []string{"p1/a", "p2/a"}) {
			t.Fatalf("pull %d = %v, want one delivery of each pipeline", i, got)
		}
	}
}

func TestPauseAndResume(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(writePackage(t, manualPipeline("p", `
  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 10 }
  b: { action: ./actions/a, in: trigger.adhoc, concurrency: 10 }
`), "a"))
	fireN(v, "p", 2)
	if err := v.st.SetPaused(v.ctx, "p", "", true); err != nil {
		t.Fatal(err)
	}
	if got := v.pull(10); len(got) != 0 {
		t.Fatalf("paused pipeline pulled %v", got)
	}
	if err := v.st.SetPaused(v.ctx, "p", "", false); err != nil {
		t.Fatal(err)
	}
	if err := v.st.SetPaused(v.ctx, "p", "b", true); err != nil {
		t.Fatal(err)
	}
	if got := v.pull(10); !slices.Equal(got, []string{"p/a", "p/a"}) {
		t.Fatalf("pull with b paused = %v", got)
	}
	if err := v.st.SetPaused(v.ctx, "p", "b", false); err != nil {
		t.Fatal(err)
	}
	if got := v.pull(10); !slices.Equal(got, []string{"p/b", "p/b"}) {
		t.Fatalf("pull after resume = %v", got)
	}
}

func TestUndispatchableDeliveriesFail(t *testing.T) {
	t.Parallel()
	containerManifest := "name: c\nkind: container\nruntime: { container: { image: c:1 } }\n"
	agentManifest := "name: g\nkind: agent\noutput: { type: object }\nruntime: { agent: { harness: api, model: m } }\n"
	tests := []struct {
		name      string
		action    string
		files     map[string]string
		env       map[string]string
		class     string
		retryable bool
		message   string
		config    string
	}{
		{"builtin", "review", nil, nil, ClassUnsupported, false, "the review builtin arrives in M3", ""},
		{"registry", "lab/score@1", nil, nil, ClassUnsupported, false, "no registries are configured", ""},
		{"container without a runtime", "./actions/c", map[string]string{"actions/c/action.yaml": containerManifest},
			nil, ClassUnsupported, false, "container actions need Docker or Podman", ""},
		{"agent without a sandbox", "./actions/c", map[string]string{"actions/c/action.yaml": agentManifest,
			"actions/c/prompt.md": "Score."}, nil, ClassSandbox, false, "agent step s needs a container sandbox", ""},
		{"missing manifest", "./actions/none", nil, nil, ClassConfig, false, "no actions/none/action.yaml", ""},
		{"bad interpolation", "./actions/a", nil, nil, ClassConfig, false, "interpolate config of step s",
			`, config: { x: "${vars.nope}" }`},
		{"input schema", "./actions/a", map[string]string{"actions/a/action.yaml": fakeManifest +
			"input: { type: object, required: [url] }\n"}, nil, ClassInputSchema, false,
			"does not match the input schema of ./actions/a", ""},
		{"config schema", "./actions/a", map[string]string{"actions/a/action.yaml": fakeManifest +
			"config: { type: object, properties: { x: { type: integer } } }\n"}, nil, ClassConfig, false,
			"does not match the config schema of ./actions/a", `, config: { x: "${run.id}" }`},
		{"missing secret", "./actions/a", nil, map[string]string{"MILLSTREAM_SECRET_TOKEN": ""}, ClassConfig, false,
			"secret token is not set", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fireEnv := map[string]string{"MILLSTREAM_SECRET_TOKEN": "x"}
			e := newEnv(t, Options{Env: func(k string) (string, bool) { s, ok := fireEnv[k]; return s, ok }})
			config := tt.config
			src := "pipeline: p\nsecrets: [token]\ntriggers:\n  adhoc: { manual: {} }\nsteps:\n" +
				"  s: { action: \"" + tt.action + "\", in: trigger.adhoc, retry: { maxAttempts: 2 }" + config + " }\n"
			pkg := writePackage(t, src, "a")
			for name, body := range tt.files {
				pkg.Files[name] = []byte(body)
			}
			e.apply(pkg)
			id := e.fire("p", "adhoc", "k")
			if tt.env != nil {
				delete(fireEnv, "MILLSTREAM_SECRET_TOKEN") // set when firing, unset at dispatch
			}
			if got := e.pull(5); len(got) != 0 {
				t.Fatalf("assignments = %v", got)
			}
			d := e.deliveries(id)[0]
			wantState := store.DeliveryDead
			if tt.retryable {
				wantState = store.DeliveryPending
			}
			if d.State != wantState || d.ErrorClass != tt.class || !strings.Contains(d.ErrorMessage, tt.message) {
				t.Fatalf("delivery = %+v; want %s, class %s, message %q", d, wantState, tt.class, tt.message)
			}
			if r := e.run("p", id); r.State != store.RunCompletedWithErrors {
				t.Fatalf("run state = %s", r.State)
			}
		})
	}
}

func TestPackageNotLoadedRetries(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(writePackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "a"))
	id := v.fire("p", "adhoc", "k")
	e2, err := New(Options{Store: v.st, Now: v.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	if as, err := e2.Pull(v.ctx, "node-b", 5); err != nil || len(as) != 0 {
		t.Fatalf("Pull = %v, %v", as, err)
	}
	d := v.deliveries(id)[0]
	if d.State != store.DeliveryPending || d.ErrorClass != ClassEngine || !strings.Contains(d.ErrorMessage, "not loaded") {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestStubs(t *testing.T) {
	t.Parallel()
	src := manualPipeline("p", `
  crawl: { action: filter, in: trigger.adhoc, out: [listings] }
  score: { action: ./actions/a, in: listings, out: [scored] }
  report: { action: ./actions/a, in: scored }
`)
	v := newEnv(t, Options{Stubs: map[store.StepRef]Stub{
		{Pipeline: "p", Step: "crawl"}: {Outputs: []store.Output{
			{Key: "a", Payload: json.RawMessage(`{"n":1}`)}, {Key: "b", Payload: json.RawMessage(`{"n":2}`)},
		}},
		{Pipeline: "p", Step: "score"}:  {Outputs: []store.Output{{Payload: json.RawMessage(`{"score":1}`)}}},
		{Pipeline: "p", Step: "report"}: {Err: &exec.ActionError{Message: "boom", Retryable: false}},
	}})
	v.apply(writePackage(t, src, "a"))
	id := v.fire("p", "adhoc", "k")
	if got := v.pull(1); len(got) != 0 {
		t.Fatalf("stubbed pull returned %v", got)
	}
	r := v.run("p", id)
	if r.State != store.RunCompletedWithErrors {
		t.Fatalf("run = %s", r.State)
	}
	for stream, want := range map[string]int{"listings": 2, "scored": 2, "report.dlq": 2} {
		items, err := v.st.Items(v.ctx, store.ItemFilter{Pipeline: "p", Stream: stream})
		if err != nil || len(items) != want {
			t.Fatalf("%s: %d items (%v), want %d", stream, len(items), err, want)
		}
	}
}

func TestHeartbeatAndStaleReports(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{LeaseTTL: time.Minute})
	v.apply(writePackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "a"))
	fireN(v, "p", 2)
	as, err := v.e.Pull(v.ctx, "node-a", 5)
	if err != nil || len(as) != 1 {
		t.Fatalf("Pull = %v, %v", as, err)
	}
	ref := as[0].Lease.Ref()
	if lost, err := v.e.Heartbeat(v.ctx, "node-a", []store.LeaseRef{ref}); err != nil || len(lost) != 0 {
		t.Fatalf("Heartbeat = %v, %v", lost, err)
	}
	if lost, err := v.e.Heartbeat(v.ctx, "node-b", []store.LeaseRef{ref}); err != nil || len(lost) != 1 {
		t.Fatalf("Heartbeat of another owner = %v, %v; want the lease lost", lost, err)
	}
	// The lost lease no longer counts: the next delivery is leased although the first is still held in the store.
	if got := v.pull(5); len(got) != 1 {
		t.Fatalf("pull after a lost lease = %v", got)
	}
	stale := ref
	stale.FencingToken++
	err = v.e.Done(v.ctx, store.DoneRequest{LeaseRef: stale, Attempt: 1})
	if !errors.Is(err, store.ErrStaleLease) {
		t.Fatalf("Done with a stale token = %v", err)
	}
	err = v.e.Fail(v.ctx, store.FailRequest{LeaseRef: stale, Attempt: 1, Class: node.ClassAction})
	if !errors.Is(err, store.ErrStaleLease) {
		t.Fatalf("Fail with a stale token = %v", err)
	}
	if _, err := v.e.Heartbeat(v.ctx, "node-a", []store.LeaseRef{ref}); err != nil {
		t.Fatal(err)
	}
	if err := v.st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := v.e.Heartbeat(v.ctx, "node-a", []store.LeaseRef{ref}); err == nil {
		t.Fatal("Heartbeat on a closed store succeeded")
	}
	if _, err := v.e.Pull(v.ctx, "node-a", 1); err == nil {
		t.Fatal("Pull on a closed store succeeded")
	}
}

func TestInvalidRateIsNotLimited(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	pkg := writePackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 5 }\n"), "a")
	s := pkg.Pipeline.Steps["a"]
	s.Rate = "often"
	pkg.Pipeline.Steps["a"] = s
	v.apply(pkg)
	fireN(v, "p", 3)
	if got := v.pull(5); len(got) != 3 || !strings.Contains(v.logs.String(), "invalid step rate") {
		t.Fatalf("pull = %v; logs:\n%s", got, v.logs.String())
	}
}

// TestCancelWhileLeased: a run cancelled while its delivery is leased discards the outputs reported later and
// settles cancelled (review focus 5).
func TestCancelWhileLeased(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(writePackage(t, manualPipeline("p", `
  a: { action: ./actions/a, in: trigger.adhoc, out: [listings] }
  b: { action: ./actions/a, in: listings }
`), "a"))
	id := v.fire("p", "adhoc", "k")
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("Pull = %v, %v", as, err)
	}
	if err := v.st.Cancel(v.ctx, "p", id); err != nil {
		t.Fatal(err)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1, CostUSD: 0.25,
		Outputs: []store.Output{{Payload: json.RawMessage(`{}`)}}}); err != nil {
		t.Fatal(err)
	}
	r := v.run("p", id)
	if r.State != store.RunCancelled || r.SettledAt.IsZero() || r.CostUSD != 0.25 {
		t.Fatalf("run = %+v; want cancelled and settled with its cost", r)
	}
	if items, err := v.st.Items(v.ctx, store.ItemFilter{Pipeline: "p", Stream: "listings"}); err != nil || len(items) != 0 {
		t.Fatalf("listings = %v, %v; want the outputs discarded", items, err)
	}
	if got := v.pull(5); len(got) != 0 {
		t.Fatalf("pull after cancel = %v", got)
	}
}

// A step that only an older revision has keeps its concurrency and rate for the runs still on that revision: its
// slots come from the revision of its live runs.
func TestStepOfAnOlderRevisionKeepsItsLimits(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(writePackage(t, manualPipeline("p", `
  old: { action: ./actions/a, in: trigger.adhoc, concurrency: 1, rate: 2/h }
`), "a"))
	fireN(v, "p", 4)
	v.apply(writePackage(t, manualPipeline("p", "  new: { action: ./actions/a, in: trigger.adhoc }\n"), "a"))
	as, err := v.e.Pull(v.ctx, "node-a", 10)
	if err != nil || len(as) != 1 || as[0].Lease.Step != "old" {
		t.Fatalf("pull = %d, %v; want one delivery of old, its concurrency", len(as), err)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	as, err = v.e.Pull(v.ctx, "node-a", 10)
	if err != nil || len(as) != 1 {
		t.Fatalf("second pull = %d, %v; want one", len(as), err)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	if got := v.pull(10); len(got) != 0 {
		t.Fatalf("pull past the rate of 2/h = %v", got)
	}
}
