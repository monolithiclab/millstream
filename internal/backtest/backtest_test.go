package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"
)

// seeded is a store holding the fixture's history (seedHistory), which tests only read, and historyHash its
// revision's package hash.
var (
	seeded      store.Store
	historyHash string
)

// revisionOnly is a store holding the fixture's revision and no history, and fixtureHash its package hash: built
// once, outside the budget.
var (
	revisionOnly store.Store
	fixtureHash  string
)

func TestMain(m *testing.M) {
	sqlitetest.Warm()
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		panic(err)
	}
	files := fixtureFiles()
	pkg, err := pipeline.FromFiles("fixture", files)
	if err != nil {
		panic(err)
	}
	bundle, _ := pipeline.EncodeBundle(files)
	if _, err := st.ApplyRevision(context.Background(), store.PackageRecord{Hash: pkg.Hash, Bundle: bundle,
		Pipeline: pkg.Pipeline}); err != nil {
		panic(err)
	}
	revisionOnly, fixtureHash = st, pkg.Hash
	if seeded, historyHash, err = seedHistory(); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = st.Close()
	_ = seeded.Close()
	os.Exit(code)
}

// TestPrepareRefusals: what Prepare refuses before reading anything, each with the error the API maps.
func TestPrepareRefusals(t *testing.T) {
	_, ok := edit(t, fixtureHash, fixtureFiles(), "bonus: 0", "bonus: 1")
	_, noBase := edit(t, "", fixtureFiles(), "bonus: 0", "bonus: 1")
	spent := &Ledger{}
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	spent.Charge(now, 5)
	req := Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: ok}
	for name, c := range map[string]struct {
		opts Options
		req  Request
		want error
	}{
		"production":   {Options{Mode: pipeline.ModeProduction}, req, ErrProduction},
		"no pipeline":  {Options{}, Request{BaseRevision: fixtureHash, Patch: ok}, store.ErrInvalid},
		"no patch":     {Options{}, Request{Pipeline: "bt", BaseRevision: fixtureHash}, store.ErrInvalid},
		"bad patch":    {Options{}, Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: []byte("x")}, patch.ErrPatch},
		"other base":   {Options{}, Request{Pipeline: "bt", BaseRevision: "ff" + fixtureHash[2:], Patch: ok}, store.ErrInvalid},
		"negative":     {Options{}, Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: ok, Sample: -1}, store.ErrInvalid},
		"bad source":   {Options{}, Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: ok, Source: "x"}, store.ErrInvalid},
		"budget spent": {Options{Budget: spent, Now: func() time.Time { return now }, Improve: pipeline.Improve{BudgetDayUSD: 5}}, req, ErrBudget},
	} {
		c.opts.Store = revisionOnly
		if _, err := New(c.opts).Prepare(c.req); !errors.Is(err, c.want) {
			t.Errorf("%s: Prepare = %v, want %v", name, err, c.want)
		}
	}
	if _, err := New(Options{Store: revisionOnly}).Prepare(Request{Pipeline: "bt", BaseRevision: fixtureHash,
		Patch: noBase}); err != nil {
		t.Errorf("a patch stating no base: %v", err)
	}
	if spent.Spent(now.Add(24*time.Hour)) != 0 || spent.Spent(now) != 5 {
		t.Error("the ledger does not start a new day")
	}
}

// TestRunRefusals: what Run refuses once it reads the revision: an unknown base, a patch that does not apply, one
// failing check, one touching an exec action's files, one changing no step, and history with nothing to replay.
func TestRunRefusals(t *testing.T) {
	files := fixtureFiles()
	_, unknown := edit(t, "", files, "bonus: 0", "bonus: 1")
	touched := fixtureFiles()
	touched["actions/crm/notes.txt"] = []byte("changed\n")
	exec, _ := patch.Format(fixtureHash, files, touched)
	stale := []byte("--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -1 +1 @@\n-pipeline: other\n+pipeline: bt\n")
	_, broken := edit(t, fixtureHash, files, "in: shortlist", "in: nowhere")
	lessons := fixtureFiles()
	lessons["lessons/unused-file.yaml"] = []byte("lessons: [x]\n")
	nothing, _ := patch.Format(fixtureHash, files, lessons)
	_, ok := edit(t, fixtureHash, files, "bonus: 0", "bonus: 1")
	b := New(Options{Store: revisionOnly, Launchers: launchers()})
	for name, c := range map[string]struct {
		req  Request
		want error
	}{
		"unknown base": {Request{Pipeline: "bt", BaseRevision: "ff" + fixtureHash[2:], Patch: unknown}, store.ErrNotFound},
		"stale":        {Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: stale}, patch.ErrPatch},
		"check":        {Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: broken}, store.ErrInvalid},
		"exec code":    {Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: exec}, ErrUntrusted},
		"no step":      {Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: nothing}, ErrNoHistory},
		"no history":   {Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: ok}, ErrNoHistory},
		"unknown step": {Request{Pipeline: "bt", BaseRevision: fixtureHash, Patch: ok, Steps: []string{"x"}}, store.ErrInvalid},
	} {
		if _, err := b.Run(context.Background(), c.req); !errors.Is(err, c.want) {
			t.Errorf("%s: Run = %v, want %v", name, err, c.want)
		}
	}
}

// TestChangedSteps: what makes a step changed, and what does not.
func TestChangedSteps(t *testing.T) {
	files := fixtureFiles()
	base, err := pipeline.FromFiles("base", files)
	if err != nil {
		t.Fatal(err)
	}
	info, err := resolveActions(base, pipeline.Resolver{})
	if err != nil {
		t.Fatal(err)
	}
	if !info.external["notify"] || len(info.execDirs) != 2 {
		t.Fatalf("actions = %+v", info)
	}
	for name, c := range map[string]struct {
		old, cur string
		touched  []string
		want     []string
	}{
		"config":      {"bonus: 0", "bonus: 1", nil, []string{"score"}},
		"variable":    {"minScore: 50", "minScore: 40", nil, []string{"keep"}},
		"concurrency": {"mode: enrich }", "mode: enrich }, concurrency: 4", nil, nil},
		"new step": {"  notify:", "  extra: { action: filter, in: shortlist, out: x, config: { expr: \"true\" } }\n  notify:",
			nil, []string{"extra"}},
		"removed": {"  enrich: { action: ./actions/fn, in: shortlist, out: enriched, config: { mode: enrich } }\n", "",
			nil, []string{"notify"}},
		"action dir": {"bonus: 0", "bonus: 0", []string{"actions/crm/notes.txt"}, []string{"notify"}},
	} {
		after := fixtureFiles()
		after[pipeline.FileName] = []byte(replaceOnce(t, fixturePipeline, c.old, c.cur))
		patched, err := pipeline.FromFiles("patched", after)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := changedSteps(base, patched, c.touched, nil, info, info)
		if err != nil || !slices.Equal(got, c.want) {
			t.Errorf("%s: changedSteps = %v, %v, want %v", name, got, err, c.want)
		}
	}
	if got := affected(base.Pipeline, base.Pipeline, []string{"score"}); len(got) != 4 || got["crawl"] {
		t.Errorf("affected = %v", got)
	}
}

func replaceOnce(t *testing.T, s, old, cur string) string {
	t.Helper()
	out, _ := edit(t, "", map[string][]byte{pipeline.FileName: []byte(s)}, old, cur)
	return string(out[pipeline.FileName])
}

// synthetic is a hand-built history: run r1 crawled two listings (a scored and approved after an edit, b scored
// and rejected), run r2's listing c lost its payload.
func synthetic() *history {
	h := newHistory(SourceLocal)
	h.runs = map[string]store.Run{"r1": {ID: "r1"}, "r2": {ID: "r2"}}
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	item := func(id, run, stream, step, payload string, parents ...string) store.Item {
		at = at.Add(time.Minute)
		return store.Item{ID: id, RunID: run, Stream: stream, Step: step, Key: id, Parents: parents, TS: at,
			Payload: json.RawMessage(payload)}
	}
	gone := item("c", "r2", "listings", "crawl", `null`, "t2")
	gone.Tombstone = store.TombstoneExpired
	items := []store.Item{
		item("t1", "r1", "trigger.adhoc", "trigger.adhoc", `{}`), item("t2", "r2", "trigger.adhoc", "trigger.adhoc", `{}`),
		item("a", "r1", "listings", "crawl", `{"id":"a"}`, "t1"), item("b", "r1", "listings", "crawl", `{"id":"b"}`, "t1"),
		gone,
		item("as", "r1", "scored", "score", `{"id":"a","score":1}`, "a"),
		item("bs", "r1", "scored", "score", `{"id":"b","score":2}`, "b"),
		item("cs", "r2", "scored", "score", `{"id":"c","score":3}`, "c"),
		item("ar", "r1", "approved", "check", `{"id":"a","score":5}`, "as"),
	}
	delivery := func(step, itemID, run string, state store.DeliveryState) store.Delivery {
		return store.Delivery{ID: ids.Delivery(step, itemID, 0), RunID: run, Step: step, ItemID: itemID, State: state,
			FinishedAt: at}
	}
	ds := []store.Delivery{
		delivery("score", "a", "r1", store.DeliveryDone), delivery("score", "b", "r1", store.DeliveryDone),
		delivery("score", "c", "r2", store.DeliveryDone),
		delivery("check", "as", "r1", store.DeliveryDone), delivery("check", "bs", "r1", store.DeliveryFiltered),
	}
	reviews := []store.Review{
		{DeliveryID: ids.Delivery("check", "as", 0), Decision: store.DecisionApprove,
			Patch: json.RawMessage(`[{"op":"replace","path":"/score","value":5}]`)},
		{DeliveryID: ids.Delivery("check", "bs", 0), Decision: store.DecisionReject, Patch: json.RawMessage(`[]`)},
	}
	h.add(items, ds, reviews)
	return h
}

// TestDraw: the sample is stratified by outcome, review decisions included, leaves out a lineage that lost its
// payload, fires each run once, and keeps the sampled lineages' items but not their siblings.
func TestDraw(t *testing.T) {
	h := synthetic()
	s := draw(h, map[string]bool{"score": true, "check": true}, 10)
	got := strata(s.sampled)
	if got[evidence.StratumDone] != 2 || got[evidence.StratumEdited] != 1 || got[evidence.StratumRejected] != 1 ||
		s.excluded != 1 || len(s.roots) != 1 || !s.fired["r1"] {
		t.Fatalf("sample = %+v, strata %v", s, got)
	}
	if !s.keep["t1"] || !s.keep["ar"] || s.keep["c"] {
		t.Errorf("keep = %v", s.keep)
	}
	one := draw(h, map[string]bool{"score": true}, 1)
	if len(one.sampled) != 1 || !one.keep["t1"] || one.keep["b"] == one.keep["a"] {
		t.Errorf("a sample of one keeps %v", one.keep)
	}
}

// TestMeasure: agreement with the historical outputs and with the reviewers, failures and costs, from a replay
// built by hand: score changed a's output, b now ends filtered before its review, and the patched score failed
// once.
func TestMeasure(t *testing.T) {
	h := synthetic()
	s := draw(h, map[string]bool{"score": true, "check": true}, 10)
	r := &replayed{deliveries: map[string]store.Delivery{}, items: map[string]store.Item{},
		children: map[string][]store.Item{}, decisions: map[string]decision{}}
	add := func(d store.Delivery, dec decision, outs ...store.Item) {
		d.ID = ids.Delivery(d.Step, d.ItemID, 0)
		r.deliveries[d.ID], r.decisions[d.ID] = d, dec
		for _, o := range outs {
			r.items[o.ID] = o
			r.children[d.ItemID] = append(r.children[d.ItemID], o)
		}
	}
	add(store.Delivery{Step: "score", ItemID: "a", RunID: "r1", State: store.DeliveryDone, CostUSD: 0.5,
		Work: 10 * time.Millisecond}, ran,
		store.Item{ID: "as", RunID: "r1", Step: "score", Key: "as", Payload: json.RawMessage(`{"id":"a","score":5}`)})
	add(store.Delivery{Step: "score", ItemID: "b", RunID: "r1", State: store.DeliveryDead, CostUSD: 0.25,
		Work: 30 * time.Millisecond}, ran)
	r.items["as"] = store.Item{ID: "as", Payload: json.RawMessage(`{"id":"a","score":5}`)}
	add(store.Delivery{Step: "check", ItemID: "as", RunID: "r1", State: store.DeliveryFiltered}, unrecorded)
	m := measure(h, s, r, "")
	if m.Compared != 4 || m.Agreed != 0 || m.NoLongerReached != 1 || m.Ran != 2 || m.Failed != 1 ||
		m.FailureRate != 0.5 || m.CostUSD != 0.75 || m.LatencyMs != 20 {
		t.Errorf("overall = %+v", m)
	}
	// a reaches its review with the edited score (agrees); b no longer reaches its own, which rejected it (agrees).
	if m.Reviewed != 2 || m.ReviewAgreed != 2 || m.ReviewAgreement != 1 {
		t.Errorf("reviews = %+v", m)
	}
	score := measure(h, s, r, "score")
	if score.Compared != 2 || score.Agreed != 0 || score.Reviewed != 0 {
		t.Errorf("score = %+v", score)
	}
	// Met again with its historical payload, the edited item needs its edit again.
	r.decisions[ids.Delivery("check", "as", 0)] = recorded
	if m := measure(h, s, r, ""); m.ReviewAgreed != 1 {
		t.Errorf("recorded review = %+v", m)
	}
}

// fakeRunner answers what it is given.
type fakeRunner struct {
	rep *Report
	err error
}

func (f fakeRunner) Run(context.Context, Request) (*Report, error) { return f.rep, f.err }

// TestJobs: a job starts running and ends done with its report or failed with its error; Start refuses at once
// what Prepare refuses.
func TestJobs(t *testing.T) {
	ok := NewJobs(context.Background(), fakeRunner{rep: &Report{Pipeline: "bt"}}, nil)
	job, err := ok.Start(Request{Pipeline: "bt"})
	if err != nil || job.State != JobRunning || job.ID == "" {
		t.Fatalf("Start = %+v, %v", job, err)
	}
	ok.Wait()
	if got, found := ok.Get(job.ID); !found || got.State != JobDone || got.Report.Pipeline != "bt" {
		t.Errorf("Get = %+v, %v", got, found)
	}
	failing := NewJobs(context.Background(), fakeRunner{err: ErrNoHistory}, nil)
	job, _ = failing.Start(Request{})
	failing.Wait()
	if got, _ := failing.Get(job.ID); got.State != JobFailed || !errors.Is(got.Err, ErrNoHistory) {
		t.Errorf("failed job = %+v", got)
	}
	if _, found := failing.Get("bt_nope"); found {
		t.Error("an unknown job was found")
	}
	prepared := NewJobs(context.Background(), New(Options{Store: revisionOnly}), nil)
	if _, err := prepared.Start(Request{}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("Start of an invalid request = %v", err)
	}
}

// TestConfigChangeSampled: a bonus on score, backtested on a sample of one: the base replay agrees with history and
// the patched one does not, and the external action never runs (TestConfigChange, under the e2e tag, replays it all).
func TestConfigChangeSampled(t *testing.T) {
	_, data := edit(t, historyHash, fixtureFiles(), "mode: score, bonus: 0", "mode: score, bonus: 15")
	rep, err := New(Options{Store: seeded, Launchers: launchers(), Timeout: time.Minute}).Run(context.Background(),
		Request{Pipeline: "bt", BaseRevision: historyHash, Patch: data, Sample: 1})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Sampled != 1 || rep.Runs != 1 || rep.Before.Agreement != 1 || rep.After.Agreement == 1 || crmCalls.Load() != 0 ||
		len(rep.Steps) != 1 || rep.Steps[0].Step != "score" {
		t.Errorf("report = %+v", rep)
	}
}
