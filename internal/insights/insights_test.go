package insights

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

var update = flag.Bool("update", false, "rewrite the golden files")

// shop is the fixture pipeline: a crawl, a scorer that spends like an agent, and a step that needs a sandbox.
const shop = `
pipeline: shop
triggers: { adhoc: { manual: {} } }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.adhoc
    out: pages
  score:
    action: ./actions/score
    in: pages
    out: scored
    retry: { maxAttempts: 2, backoff: 1s, maxBackoff: 1s }
  publish:
    action: ./actions/publish
    in: scored
`

// now is the projector's instant in the fixture.
var now = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

func (c *clock) advance(d time.Duration) { c.set(c.Now().Add(d)) }

// sim drives a store through its units under a fake clock.
type sim struct {
	t     *testing.T
	ctx   context.Context
	st    *sqlite.Store
	clock *clock
	p     *pipeline.Pipeline
}

func newSim(t *testing.T, src string) *sim {
	t.Helper()
	c := &clock{t: now.Add(-30 * 24 * time.Hour)}
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: c.Now, Rand: func() float64 { return 0.5 }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	})
	p, err := pipeline.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	s := &sim{t: t, ctx: context.Background(), st: st, clock: c, p: p}
	if _, err := st.ApplyRevision(s.ctx, store.PackageRecord{Hash: fmt.Sprintf("%x", len(src)), Bundle: []byte(src),
		Pipeline: p}); err != nil {
		t.Fatal(err)
	}
	return s
}

func (s *sim) fire(runID string, at time.Time) {
	s.t.Helper()
	s.clock.set(at)
	if _, err := s.st.Fire(s.ctx, store.Firing{Pipeline: s.p.Name, Trigger: "adhoc", Key: runID, RunID: runID}); err != nil {
		s.t.Fatal(err)
	}
}

// lease leases every due delivery of step.
func (s *sim) lease(step string) []store.Lease {
	s.t.Helper()
	slots := map[store.StepRef]int{}
	for name := range s.p.Steps {
		if name != step {
			slots[store.StepRef{Pipeline: s.p.Name, Step: name}] = 0
		}
	}
	ls, err := s.st.Lease(s.ctx, store.LeaseRequest{Owner: "n", Max: 10, TTL: time.Hour, Slots: slots})
	if err != nil || len(ls) == 0 {
		s.t.Fatalf("lease %s: %v, %v", step, ls, err)
	}
	return ls
}

// agentMetrics are the metrics of one harness call.
func agentMetrics(schemaFailed bool) []store.Metric {
	m := map[string]string{"model": "small-model"}
	ms := []store.Metric{{Name: "tokens_in", Value: 100, Labels: m}, {Name: "tokens_out", Value: 20, Labels: m},
		{Name: "turns", Value: 2, Labels: m}, {Name: "cost_usd", Value: 0.02, Labels: m}}
	if schemaFailed {
		ms = append(ms, store.Metric{Name: "schema_validation_failures", Value: 1, Labels: m})
	}
	return ms
}

func (s *sim) done(l store.Lease, cost float64, ms []store.Metric, outputs ...store.Output) {
	s.t.Helper()
	if _, err := s.st.Done(s.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Outputs: outputs,
		CostUSD: cost, Metrics: ms}); err != nil {
		s.t.Fatal(err)
	}
}

func (s *sim) fail(l store.Lease, class, msg string, retryable bool, cost float64, ms []store.Metric) {
	s.t.Helper()
	if _, err := s.st.Fail(s.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Class: class,
		Message: msg, Retryable: retryable, CostUSD: cost, Metrics: ms}); err != nil {
		s.t.Fatal(err)
	}
}

func payload(i int) store.Output {
	return store.Output{Key: fmt.Sprintf("p%d", i), Payload: json.RawMessage(fmt.Sprintf(`{"page":%d}`, i))}
}

// quickRun is a baseline run: every step fast, the scorer at a cent per delivery.
func (s *sim) quickRun(id string, at time.Time) {
	s.fire(id, at)
	crawl := s.lease("crawl")[0]
	s.clock.advance(time.Second)
	s.done(crawl, 0, nil, payload(1), payload(2))
	scores := s.lease("score")
	s.clock.advance(time.Second)
	for _, l := range scores {
		s.done(l, 0.01, agentMetrics(false), store.Output{Payload: json.RawMessage(`{"score":1}`)})
	}
	for _, l := range s.lease("publish") {
		s.done(l, 0, nil)
	}
}

// slowRun is a window run: the scorer waits 10s for a slot and works 20s at two cents. failure says how its first
// delivery fails: "flaky" (a timeout, retried and recovered), "deterministic" (a parse error, dead) or "".
func (s *sim) slowRun(id string, at time.Time, failure string) {
	s.fire(id, at)
	crawl := s.lease("crawl")[0]
	s.clock.advance(time.Second)
	s.done(crawl, 0, nil, payload(1), payload(2))
	s.clock.advance(10 * time.Second)
	scores := s.lease("score")
	s.clock.advance(20 * time.Second)
	out := store.Output{Payload: json.RawMessage(`{"score":2}`)}
	switch failure {
	case "flaky":
		s.fail(scores[0], "action", "timeout after 30s calling https://api.example.test/v1/items/"+id, true, 0.02,
			agentMetrics(true))
	case "deterministic":
		s.fail(scores[0], "action", "parse /data/"+id+"/page-1.json: unexpected token at offset 17", false, 0.02,
			agentMetrics(false))
	default:
		s.done(scores[0], 0.02, agentMetrics(false), out)
	}
	s.done(scores[1], 0.02, agentMetrics(false), out)
	if failure == "flaky" {
		s.clock.advance(time.Second) // the backoff: 1s with a jitter of one half
		retry := s.lease("score")[0]
		s.clock.advance(2 * time.Second)
		s.done(retry, 0.02, agentMetrics(false), out)
	}
	for _, l := range s.lease("publish") {
		s.clock.advance(100 * time.Millisecond)
		s.fail(l, "sandbox", "agent step publish needs a container sandbox (a container runtime, agents.image)",
			false, 0, nil)
	}
}

// fixture builds the fixture's history: six quick runs in the baseline, then three slow ones in the window, with
// a flaky failure, a deterministic one and a step no runtime can take.
func fixture(t *testing.T) *sim {
	s := newSim(t, shop)
	for i := range 6 {
		s.quickRun(fmt.Sprintf("base%d", i), now.Add(-time.Duration(7-i)*24*time.Hour))
	}
	s.slowRun("slow1", now.Add(-3*time.Hour), "flaky")
	s.slowRun("slow2", now.Add(-2*time.Hour), "deterministic")
	s.slowRun("slow3", now.Add(-1*time.Hour), "")
	return s
}

// golden compares got with testdata/name, rewriting it with -update.
func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s differs (run go test -update to accept):\n%s", name, got)
	}
}

// goldenInsight is an insight as the golden file holds it.
type goldenInsight struct {
	ID       string          `json:"id"`
	Kind     string          `json:"kind"`
	Scope    string          `json:"scope"`
	Severity string          `json:"severity"`
	Window   [2]time.Time    `json:"window"`
	Summary  string          `json:"summary"`
	Evidence json.RawMessage `json:"evidence"`
}

// Golden insights over the fixture's history: a slowStep (the scorer holds the critical path, its p95 regressed),
// two failure clusters (flaky, recovered on retry; deterministic, dead), the scorer's cost (per output up threefold,
// a schema-validation failure) and the unschedulable publish step.
func TestGoldenInsights(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	d, err := Load(s.ctx, s.st, "shop", now)
	if err != nil {
		t.Fatal(err)
	}
	var out []goldenInsight
	for _, in := range Compute(d, Kinds) {
		out = append(out, goldenInsight{ID: in.ID, Kind: in.Kind, Scope: in.Scope, Severity: in.Severity,
			Window: [2]time.Time{in.WindowStart, in.WindowEnd}, Summary: Summary(in), Evidence: in.Evidence})
	}
	body, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "insights.json", append(body, '\n'))
}

// The projector records what it finds, updates it on the next pass, and resolves what clears: a day later, after
// a clean quick run, the failures and the unschedulable step are gone from the window.
func TestProjectLifecycle(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	if err := Project(s.ctx, s.st, s.p, now); err != nil {
		t.Fatal(err)
	}
	if err := Project(s.ctx, s.st, s.p, now.Add(time.Minute)); err != nil { // same findings: same rows
		t.Fatal(err)
	}
	ins, err := s.st.Insights(s.ctx, store.InsightFilter{Pipeline: "shop", State: store.InsightOpen})
	if err != nil || len(ins) != 6 {
		t.Fatalf("open insights = %d, %v; want 6", len(ins), err)
	}
	s.quickRun("later", now.Add(26*time.Hour))
	if err := Project(s.ctx, s.st, s.p, now.Add(27*time.Hour)); err != nil {
		t.Fatal(err)
	}
	states := map[string]store.InsightState{}
	all, err := s.st.Insights(s.ctx, store.InsightFilter{Pipeline: "shop"})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range all {
		states[in.Kind+"/"+in.Scope+"/"+string(in.State)] = in.State
	}
	for _, want := range []string{"failureCluster/score/resolved", "unschedulable/publish/resolved",
		"slowStep/score/resolved", "cost/score/open"} {
		if _, ok := states[want]; !ok {
			t.Errorf("no %s insight; have %v", want, states)
		}
	}
}

// insights.enabled and insights.disable decide which kinds the projector evaluates; with insights off it records
// nothing and leaves what it recorded alone.
func TestProjectKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		keys   string
		kinds  map[string]bool
		resets bool
	}{
		{"default", "", map[string]bool{"slowStep": true, "failureCluster": true, "cost": true,
			"unschedulable": true}, false},
		{"disable", "insights: { disable: [cost, slowStep, drift] }",
			map[string]bool{"failureCluster": true, "unschedulable": true}, false},
		{"off", "insights: { enabled: false, every: 1h }", map[string]bool{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := fixture(t)
			p, err := pipeline.Parse([]byte(shop + tt.keys + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			if err := Project(s.ctx, s.st, p, now); err != nil {
				t.Fatal(err)
			}
			ins, err := s.st.Insights(s.ctx, store.InsightFilter{Pipeline: "shop"})
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]bool{}
			for _, in := range ins {
				got[in.Kind] = true
			}
			if fmt.Sprint(got) != fmt.Sprint(tt.kinds) {
				t.Errorf("kinds recorded = %v, want %v", got, tt.kinds)
			}
		})
	}
	bad, err := pipeline.Parse([]byte(shop + "insights: { every: soon }\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Project(context.Background(), nil, bad, now); err == nil {
		t.Error("Project accepted an invalid insights key")
	}
}

// Golden run reports: a settled run against the median of the runs before it, and a running run with its ETA.
func TestGoldenReports(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	r, err := BuildReport(s.ctx, s.st, "shop", "slow2", now)
	if err != nil {
		t.Fatal(err)
	}
	var text bytes.Buffer
	if err := r.WriteText(&text, now); err != nil {
		t.Fatal(err)
	}
	golden(t, "report.txt", text.Bytes())
	body, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "report.json", append(body, '\n'))

	s.fire("running", now.Add(-5*time.Second))
	s.lease("crawl")
	r, err = BuildReport(s.ctx, s.st, "shop", "running", now)
	if err != nil {
		t.Fatal(err)
	}
	text.Reset()
	if err := r.WriteText(&text, now); err != nil {
		t.Fatal(err)
	}
	golden(t, "report_running.txt", text.Bytes())

	if _, err := BuildReport(s.ctx, s.st, "shop", "missing", now); err == nil {
		t.Error("a report of a missing run")
	}
}

// The first run of a pipeline has nothing to compare with.
func TestReportWithoutHistory(t *testing.T) {
	t.Parallel()
	s := newSim(t, shop)
	s.quickRun("only", now.Add(-time.Hour))
	r, err := BuildReport(s.ctx, s.st, "shop", "only", now)
	if err != nil {
		t.Fatal(err)
	}
	if r.Comparison != nil || r.ETA != nil || len(r.CriticalPath.Steps) != 3 || r.Items["scored"] != 2 {
		t.Fatalf("report = %+v", r)
	}
	var text bytes.Buffer
	if err := r.WriteText(&text, now); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(text.Bytes(), []byte("No earlier run to compare with.")) ||
		!bytes.Contains(text.Bytes(), []byte("Failures by cluster:\n  none")) {
		t.Errorf("report:\n%s", text.String())
	}
}
