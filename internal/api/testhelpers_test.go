package api

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/schema"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// The package's tests run serially: they share stores on the transpiled SQLite, whose allocator is one process-wide
// lock (AGENTS.md, Test suites), and each request takes microseconds.

var t0 = time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)

// artifactDir holds the fixtures' artifact stores, removed when the tests end.
var artifactDir string

func TestMain(m *testing.M) {
	flag.Parse()
	dir, err := os.MkdirTemp("", "millstream-api-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	artifactDir = dir
	// Built once per process, before the tests: the seeded store the others clone and the OpenAPI document.
	sharedFixture()
	theContract()
	code := m.Run()
	if code == 0 && flag.Lookup("test.run").Value.String() == "" {
		code = checkEveryRouteCalled()
	}
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// fakeClock is a clock tests advance by hand; its timers fire when their time comes.
type fakeClock struct {
	mu     sync.Mutex
	t      time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	c       chan time.Time
	at      time.Time
	stopped bool
	clock   *fakeClock
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: make(chan time.Time, 1), at: c.t.Add(d), clock: c}
	if d <= 0 {
		t.c <- c.t
		return t
	}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock on and fires the timers it reaches.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	c.timers = slices.DeleteFunc(c.timers, func(t *fakeTimer) bool {
		if t.stopped || t.at.After(c.t) {
			return t.stopped
		}
		t.c <- c.t
		return true
	})
}

// pending reports how many timers wait to fire.
func (c *fakeClock) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.stopped {
			n++
		}
	}
	return n
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := !t.stopped && slices.Contains(t.clock.timers, t)
	t.stopped = true
	return was
}

// fixtureSrc is the pipeline the fixtures run: a crawl, a score routing on a threshold, and a review of the good
// listings.
const fixtureSrc = `
pipeline: listings
description: Listings scored and reviewed
vars: { minScore: 70 }
triggers:
  adhoc: { manual: {} }
  daily: { cron: "0 6 * * *" }
steps:
  crawl: { action: ./actions/crawl, in: trigger.adhoc, out: [listings.raw] }
  score:
    action: ./actions/score
    in: listings.raw
    out:
      - { when: "item.score >= vars.minScore", out: listings.good }
      - { when: "item.score >= 0", out: listings.rest }
  check:
    action: review
    in: listings.good
    out: [listings.approved]
    config: { inbox: analysts, reasons: [overpriced] }
`

const nightlySrc = `
pipeline: nightly
triggers: { daily: { cron: "0 6 * * *" } }
steps: { crawl: { action: ./actions/crawl, in: trigger.daily } }
`

const lessonsSrc = `
rules:
  - { text: Price per square metre matters most., source: review/dl_1 }
examples:
  - { input: { price: 1 }, output: { score: 10 }, why: too cheap to be true }
`

var fixtureSteps = []string{"crawl", "score", "check"}

var reviewSpec = store.ReviewSpec{Inbox: "analysts", Editable: []string{"/score"}, Reasons: []string{"overpriced"}}

// fixture is a store holding a history the API reads: run r1 with a crawl that kept an artifact, a scored listing
// parked for review and one dead letter; run r2 with an auto-approved, sampled listing; an insight; the rollups.
type fixture struct {
	t     testing.TB
	st    *sqlite.Store
	clock *fakeClock
	arts  *artifacts.Store
	dir   string
	srv   *Server
	ctx   context.Context

	// ids of what the seed made
	crawlDelivery, reviewDelivery, sampleDelivery, deadDelivery string
	trigger, good, dead                                         store.Item
	transcript, photo, page                                     store.ArtifactRef
}

// sharedFixture is the read tests' fixture: seeded once, never changed. The other tests clone it.
var sharedFixture = sync.OnceValue(seedFixture)

// readFixture returns the shared fixture, for tests that change nothing.
func readFixture(t *testing.T) *fixture {
	f := sharedFixture()
	f.t = t
	return f
}

// newFixture returns a fixture of the test's own: a copy of the shared one's store, closed when the test ends.
// Cloning costs a tenth of seeding.
func newFixture(t testing.TB) *fixture {
	f := cloneFixture()
	f.t = t
	t.Cleanup(func() { _ = f.st.Close() })
	return f
}

// cloneFixture returns a copy of the shared fixture on a clone of its store, left open.
func cloneFixture() *fixture {
	base := sharedFixture()
	f := *base
	f.clock = &fakeClock{t: base.clock.Now()}
	st, err := base.st.Clone(sqlite.Options{Now: f.clock.Now, Rand: func() float64 { return 0.5 }})
	if err != nil {
		panic(err)
	}
	f.st = st
	f.srv = f.server()
	return &f
}

// scratchFixture is a clone the tests that change state in ways the others ignore share (their own runs, r1
// cancelled, its clock moved on): a clone's first requests prepare every statement again, so fewer clones run faster.
// A test that meets the scratch clone a second time (go test -count) starts a new one.
func scratchFixture(t testing.TB) *fixture {
	scratchState.mu.Lock()
	defer scratchState.mu.Unlock()
	if scratchState.f == nil || scratchState.used[t.Name()] {
		scratchState.f, scratchState.used = cloneFixture(), map[string]bool{}
	}
	scratchState.used[t.Name()] = true
	f := *scratchState.f
	f.t = t
	return &f
}

var scratchState struct {
	mu   sync.Mutex
	f    *fixture
	used map[string]bool
}

// seedFixture builds the shared fixture.
func seedFixture() *fixture {
	clock := &fakeClock{t: t0}
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: clock.Now, Rand: func() float64 { return 0.5 }})
	if err != nil {
		panic(err)
	}
	dir, err := os.MkdirTemp(artifactDir, "")
	if err != nil {
		panic(err)
	}
	f := &fixture{st: st, clock: clock, arts: artifacts.New(dir), dir: dir, ctx: context.Background()}
	f.seed()
	f.srv = f.server()
	return f
}

// server returns a server over the fixture's store, with an engine, its artifacts and the embedded node.
func (f *fixture) server() *Server {
	eng, err := engine.New(engine.Options{Store: f.st, Now: f.clock.Now})
	if err != nil {
		panic(err)
	}
	srv, err := New(Options{Store: f.st, Engine: eng, Artifacts: f.arts, Version: "v-test",
		Nodes: []Node{{ID: "embedded-1", Embedded: true, Version: "v-test", Slots: 4, Runtimes: []string{"exec"}}},
		User:  "tester", Clock: f.clock, NotificationFailures: func() int64 { return 2 }})
	if err != nil {
		panic(err)
	}
	return srv
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// lease leases the oldest due delivery of step.
func (f *fixture) lease(step string) store.Lease {
	slots := map[store.StepRef]int{}
	for _, s := range fixtureSteps {
		if s != step {
			slots[store.StepRef{Pipeline: "listings", Step: s}] = 0
		}
	}
	ls := must(f.st.Lease(f.ctx, store.LeaseRequest{Owner: "embedded-1", Max: 1, TTL: time.Minute, Slots: slots}))
	if len(ls) != 1 {
		panic("no delivery of " + step)
	}
	return ls[0]
}

func (f *fixture) done(l store.Lease, outs ...store.Output) {
	must(f.st.Done(f.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Outputs: outs}))
}

func (f *fixture) fire(runID string) {
	must(f.st.Fire(f.ctx, store.Firing{Pipeline: "listings", Trigger: "adhoc", Key: runID, RunID: runID,
		Vars: map[string]any{"minScore": int64(70)}, VarSources: map[string]string{"minScore": "default"},
		Payload: json.RawMessage(`{"city":"Lyon"}`)}))
}

// storePark parks a leased check delivery with the fixture's review spec, auto-approved and sampled with sample.
func storePark(l store.Lease, sample bool) store.ParkRequest {
	return store.ParkRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Spec: reviewSpec, Display: "Listing " + l.Item.Key,
		AutoApprove: sample, Sample: sample}
}

func out(key, body string) store.Output {
	return store.Output{Key: key, Payload: json.RawMessage(body)}
}

func (f *fixture) seed() {
	p := must(pipeline.Parse([]byte(fixtureSrc)))
	score := p.Steps["score"]
	score.Lessons = "lessons/score.yaml"
	p.Steps["score"] = score
	files := map[string][]byte{"pipeline.yaml": []byte(fixtureSrc), "lessons/score.yaml": []byte(lessonsSrc)}
	must(f.st.ApplyRevision(f.ctx, store.PackageRecord{Hash: "pkg1", Bundle: must(json.Marshal(files)), Pipeline: p}))

	f.fire("r1")
	f.clock.Advance(time.Second)
	f.photo = must(f.arts.Put("listings", "photo.png", bytes.NewReader([]byte("\x89PNG\r\n\x1a\nphoto"))))
	f.transcript = must(f.arts.Put("listings", "transcript.jsonl", strings.NewReader(`{"role":"user"}`+"\n")))
	f.page = must(f.arts.Put("listings", "page.html", strings.NewReader("<html><script>alert(1)</script></html>")))
	l := f.lease("crawl")
	f.crawlDelivery, f.trigger = l.DeliveryID, l.Item
	good := out("a", `{"price":100}`)
	good.Artifacts = []store.ArtifactRef{f.photo}
	must(f.st.Done(f.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt,
		Outputs: []store.Output{good, out("b", `{"price":5}`)}, Artifacts: []store.ArtifactRef{f.page, f.transcript},
		CostUSD: 0.25}))
	f.clock.Advance(time.Second)
	la := f.lease("score")
	f.good = la.Item
	f.done(la, out("", `{"price":100,"score":90}`))
	lb := f.lease("score")
	f.dead, f.deadDelivery = lb.Item, lb.DeliveryID
	must(f.st.Fail(f.ctx, store.FailRequest{LeaseRef: lb.Ref(), Attempt: lb.Attempt, Class: "nonRetryable",
		Message: "price 5 is below the floor", Retryable: false, Artifacts: []store.ArtifactRef{f.transcript}}))
	lc := f.lease("check")
	f.reviewDelivery = lc.DeliveryID
	must(f.st.Park(f.ctx, store.ParkRequest{LeaseRef: lc.Ref(), Attempt: lc.Attempt, Spec: reviewSpec,
		Display: "Listing a"}))

	f.clock.Advance(time.Minute)
	f.fire("r2")
	f.done(f.lease("crawl"), out("c", `{"price":200}`))
	f.done(f.lease("score"), out("", `{"price":200,"score":95}`))
	ls := f.lease("check")
	f.sampleDelivery = ls.DeliveryID
	must(f.st.Park(f.ctx, store.ParkRequest{LeaseRef: ls.Ref(), Attempt: ls.Attempt, Spec: reviewSpec,
		Display: "Listing c", AutoApprove: true, Sample: true}))

	// r3: two more open reviews; r4: one more sample. The inbox lists pages of them.
	f.clock.Advance(time.Minute)
	f.fire("r3")
	f.done(f.lease("crawl"), out("d", `{"price":300}`), out("e", `{"price":400}`))
	for range 2 {
		f.done(f.lease("score"), out("", `{"price":300,"score":99}`))
		must(f.st.Park(f.ctx, storePark(f.lease("check"), false)))
	}
	f.fire("r4")
	f.done(f.lease("crawl"), out("f", `{"price":500}`))
	f.done(f.lease("score"), out("", `{"price":500,"score":99}`))
	must(f.st.Park(f.ctx, storePark(f.lease("check"), true)))
	// A pipeline with a cron trigger only, and no package bundle.
	must(f.st.ApplyRevision(f.ctx, store.PackageRecord{Hash: "cron-only", Pipeline: must(parsePipeline(nightlySrc))}))

	if err := f.st.RecordInsights(f.ctx, store.InsightReport{Pipeline: "listings", Kinds: []string{"failureCluster"},
		Insights: []store.Insight{{Pipeline: "listings", ID: "in_1", Kind: "failureCluster", Scope: "score",
			WindowStart: t0, WindowEnd: t0.Add(time.Hour), Severity: "warning",
			Evidence: json.RawMessage(`{"count":1,"class":"nonRetryable","message":"price <n> is below the floor"}`)}},
	}); err != nil {
		panic(err)
	}
	must(f.st.Rollup(f.ctx))
}

// contract is the OpenAPI document, decoded, and the operations the tests called.
type contract struct {
	doc        map[string]any
	components any
	mu         sync.Mutex
	called     map[string]bool // "GET /pipelines/{pipeline}"
	// schemas is a document of the components and every JSON response's schema, under $defs by operation and status.
	schemas *schema.Document
}

// defKey is a response's key in the contract's $defs.
func defKey(method, path, status string) string {
	return schema.EscapeToken(method + " " + path + " " + status)
}

var theContract = sync.OnceValue(func() *contract {
	var doc map[string]any
	if err := yaml.Unmarshal(OpenAPI, &doc); err != nil {
		panic(err)
	}
	c := &contract{doc: doc, components: doc["components"], called: map[string]bool{}}
	defs := map[string]any{}
	for path, item := range doc["paths"].(map[string]any) {
		for method, op := range item.(map[string]any) {
			o, ok := op.(map[string]any) // not the path's parameters
			if !ok {
				continue
			}
			responses, _ := o["responses"].(map[string]any)
			for status := range responses {
				if s, ok := c.lookup("paths", path, method, "responses", status, "content", "application/json",
					"schema"); ok {
					defs[method+" "+path+" "+status] = s
				}
			}
		}
	}
	body, err := json.Marshal(map[string]any{"components": c.components, "$defs": defs})
	if err != nil {
		panic(err)
	}
	if c.schemas, err = schema.CompileDocument(body); err != nil {
		panic(err)
	}
	for key := range defs { // all at once, before the tests: the first to see a response would pay for its schema
		if _, err := c.schemas.Schema("/$defs/" + schema.EscapeToken(key)); err != nil {
			panic(err)
		}
	}
	return c
})

// lookup walks the document along keys, following a $ref met on the way.
func (c *contract) lookup(keys ...string) (any, bool) {
	var v any = c.doc
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		if ref, ok := m["$ref"].(string); ok {
			r, ok := c.lookup(strings.Split(strings.TrimPrefix(ref, "#/"), "/")...)
			if !ok {
				return nil, false
			}
			m = r.(map[string]any)
		}
		if v, ok = m[k]; !ok {
			return nil, false
		}
	}
	if m, ok := v.(map[string]any); ok {
		if ref, ok := m["$ref"].(string); ok {
			return c.lookup(strings.Split(strings.TrimPrefix(ref, "#/"), "/")...)
		}
	}
	return v, true
}

// validate checks a response against the document: the operation documents its status and content type, and a JSON
// body validates against the schema.
func (c *contract) validate(t testing.TB, method, path string, rec *httptest.ResponseRecorder) {
	t.Helper()
	status := fmt.Sprint(rec.Code)
	ct, _, _ := strings.Cut(rec.Header().Get("Content-Type"), ";")
	op := method + " " + path
	if path == "" { // the 404 of a path the API does not have, or a 405
		path, method = "/pipelines/{pipeline}", http.MethodGet
		if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("unrouted request answered %d", rec.Code)
		}
		status = "404"
	} else {
		c.mu.Lock()
		c.called[op] = true
		c.mu.Unlock()
	}
	resp, ok := c.lookup("paths", path, strings.ToLower(method), "responses", status)
	if !ok && rec.Code >= 500 {
		status = "default"
		resp, ok = c.lookup("paths", path, strings.ToLower(method), "responses", status)
	}
	if !ok {
		t.Errorf("%s: status %s is not documented (body %s)", op, status, rec.Body.String())
		return
	}
	content, _ := resp.(map[string]any)["content"].(map[string]any)
	if rec.Code == http.StatusNoContent || content == nil {
		if rec.Body.Len() > 0 {
			t.Errorf("%s: a %s answer documented without content has a body", op, status)
		}
		return
	}
	if strings.Contains(path, "/artifacts/") { // the artifact's own type, documented as any binary
		return
	}
	if _, ok := content[ct]; !ok {
		t.Errorf("%s %s: content type %q is not documented", op, status, ct)
		return
	}
	if ct != "application/json" {
		return
	}
	s, err := c.schemas.Schema("/$defs/" + defKey(strings.ToLower(method), path, status))
	if err != nil {
		t.Fatalf("%s %s: %v", op, status, err)
	}
	var v any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Errorf("%s %s: body is not JSON: %v", op, status, err)
		return
	}
	for _, viol := range s.Violations(v) {
		t.Errorf("%s %s: %s %s", op, status, viol.Pointer, viol.Message)
	}
}

// checkEveryRouteCalled fails the run when a route was not called by any test.
func checkEveryRouteCalled() int {
	c := theContract()
	var missed []string
	for _, rt := range routes {
		if op := rt.method + " " + rt.path; !c.called[op] {
			missed = append(missed, op)
		}
	}
	if len(missed) > 0 {
		fmt.Fprintf(os.Stderr, "routes no test calls:\n  %s\n", strings.Join(missed, "\n  "))
		return 1
	}
	return 0
}

// request is one call of the API.
type request struct {
	method, target string
	body           string
	header         map[string]string
}

// do serves req and checks the answer against the OpenAPI document.
func do(t testing.TB, h http.Handler, req request) *httptest.ResponseRecorder {
	t.Helper()
	var body io.Reader
	if req.body != "" {
		body = strings.NewReader(req.body)
	}
	r := httptest.NewRequest(cmpOr(req.method, http.MethodGet), req.target, body)
	if r.Method == http.MethodPost {
		r.Header.Set("Content-Type", "application/json")
	}
	for k, v := range req.header {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	path := ""
	if method, p, ok := strings.Cut(r.Pattern, " "); ok && strings.HasPrefix(p, Prefix) {
		path = strings.TrimPrefix(p, Prefix)
		if method != r.Method && (method != http.MethodGet || r.Method != http.MethodHead) {
			path = ""
		}
	}
	theContract().validate(t, r.Method, path, rec)
	return rec
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// get serves a GET and decodes a 200 answer into a value of type T.
func get[T any](t testing.TB, h http.Handler, target string) T {
	t.Helper()
	rec := do(t, h, request{target: target})
	var v T
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: %d %s", target, rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	return v
}

// wantError checks an error answer's status and code.
func wantError(t testing.TB, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var body errorBody
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if rec.Code != status || body.Error.Code != code {
		t.Errorf("answer %d %q, want %d %q: %s", rec.Code, body.Error.Code, status, code, rec.Body.String())
	}
}

func parsePipeline(src string) (*pipeline.Pipeline, error) { return pipeline.Parse([]byte(src)) }
