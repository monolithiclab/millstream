// Package storetest is the log store's conformance suite: the behaviour of every unit and read of store.Store,
// which the SQLite and Postgres stores must both show. Each test is an exported function taking the test and an
// Opener, listed in Tests by area; a store's package runs them, the SQLite store one area per package
// (internal/store/sqlstore/conformance/<area>, through RunArea), the Postgres store all of Tests under the e2e tag
// against a database of its own per test. Files named *_e2e.go hold the tests that only the e2e build runs, and
// scale.go sets how far the default build scales the long ones down.
package storetest

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// fixture is the pipeline most tests run: a crawl fanning out to two streams, a routed score with a gap for
// negative scores, a report, and an alert reading both dead-letter streams.
const fixture = `
pipeline: listings
vars: { minScore: 70 }
triggers:
  daily: { cron: "0 6 * * *" }
  adhoc: { manual: {} }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.daily
    out: [listings.raw, listings.audit]
    retry: { maxAttempts: 3, backoff: 10s, maxBackoff: 30s }
  audit:
    action: ./actions/audit
    in: listings.audit
  score:
    action: ./actions/score
    in: listings.raw
    out:
      - { when: "item.score >= vars.minScore", out: listings.good }
      - { when: "item.score >= 0", out: listings.rest }
  report:
    action: ./actions/report
    in: [listings.good, listings.rest]
  alert:
    action: ./actions/alert
    in: [crawl.dlq, score.dlq]
`

// fixtureFixed is the fixture with a catch-all route: replays with migrate route what the fixture left unrouted.
const fixtureFixed = `
pipeline: listings
vars: { minScore: 70 }
triggers:
  daily: { cron: "0 6 * * *" }
  adhoc: { manual: {} }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.daily
    out: [listings.raw, listings.audit]
    retry: { maxAttempts: 3, backoff: 10s, maxBackoff: 30s }
  audit:
    action: ./actions/audit
    in: listings.audit
  score:
    action: ./actions/score
    in: listings.raw
    out:
      - { when: "item.score >= vars.minScore", out: listings.good }
      - { when: "true", out: listings.rest }
  report:
    action: ./actions/report
    in: [listings.good, listings.rest]
  alert:
    action: ./actions/alert
    in: [crawl.dlq, score.dlq]
`

var t0 = time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)

// expire tombstones an item by hand, as retention does (spec section 9.3): TestRetain* cover the unit itself.
func (e *env) expire(pipelineName, id string) {
	e.t.Helper()
	if err := e.s.Exec(e.ctx, `UPDATE items SET payload = 'null', payload_hash = '', artifacts = '[]',
		tombstone = 'expired' WHERE pipeline = ? AND id = ?`, pipelineName, id); err != nil {
		e.t.Fatal(err)
	}
}

// fakeClock is a settable clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// The tests do not call t.Parallel. Run one at a time on SQLite, they take their store from the Memory pool warm,
// its statements prepared; run in parallel, each opens a cold connection and they contend on the transpiled
// SQLite's global allocator mutex (modernc libc's allocMu), so the package took longer in parallel than serially:
// 0.33 s against 0.45 s at -parallel 10. The concurrency tests race their own goroutines.

// Store is a store under test: the store's units and reads, and what a test needs to reach behind them.
type Store interface {
	store.Store
	// Exec runs a statement as a unit of its own: a test changing a record behind the units' back. The statement
	// is written with ? placeholders, as the units are.
	Exec(ctx context.Context, query string, args ...any) error
	// Unit runs fn as a unit of its own: exec runs a statement in the unit, and an error from fn rolls back what
	// it ran, alone.
	Unit(ctx context.Context, fn func(exec func(query string, args ...any) error) error) error
	// QueryRow runs a query on the read pool.
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row
}

// Options configures a store under test.
type Options struct {
	// Now is the store's clock in place of the database's; nil keeps the database clock.
	Now func() time.Time
	// Rand is the retry jitter.
	Rand func() float64
}

// Opener opens a new, empty store for a test, closed when the test ends. Closing it again is a no-op.
type Opener func(t *testing.T, opts Options) Store

// env is a store under test with its clock.
type env struct {
	t     *testing.T
	s     Store
	clock *fakeClock
	ctx   context.Context
}

// newEnv opens a store with a fake clock and a jitter of one half, and applies the given pipelines.
func newEnv(t *testing.T, open Opener, pipelines ...string) *env {
	t.Helper()
	clock := &fakeClock{t: t0}
	s := open(t, Options{Now: clock.Now, Rand: func() float64 { return 0.5 }})
	e := &env{t: t, s: s, clock: clock, ctx: context.Background()}
	for _, src := range pipelines {
		e.apply(src)
	}
	return e
}

// parsedPipelines caches parse: a parsed pipeline is read-only, so stores and tests share it.
var parsedPipelines sync.Map

func parsed(src string) (*pipeline.Pipeline, error) {
	f, _ := parsedPipelines.LoadOrStore(src, sync.OnceValues(func() (*pipeline.Pipeline, error) {
		return pipeline.Parse([]byte(src))
	}))
	return f.(func() (*pipeline.Pipeline, error))()
}

func parse(t *testing.T, src string) *pipeline.Pipeline {
	t.Helper()
	p, err := parsed(src)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return p
}

func hashOf(src string) string {
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:])
}

func (e *env) apply(src string) store.RevisionID {
	e.t.Helper()
	id, err := e.s.ApplyRevision(e.ctx, store.PackageRecord{Hash: hashOf(src), Bundle: []byte(src), Pipeline: parse(e.t, src)})
	if err != nil {
		e.t.Fatalf("ApplyRevision: %v", err)
	}
	return id
}

// fire fires the daily trigger of listings under run id runID.
func (e *env) fire(runID string) {
	e.t.Helper()
	e.firePipeline("listings", "daily", runID)
}

func (e *env) firePipeline(pipelineName, trigger, runID string) {
	e.t.Helper()
	created, err := e.s.Fire(e.ctx, store.Firing{
		Pipeline: pipelineName, Trigger: trigger, Key: runID, RunID: runID,
		Vars: map[string]any{"minScore": int64(70)}, VarSources: map[string]string{"minScore": "default"},
	})
	if err != nil || !created {
		e.t.Fatalf("Fire %s: created %v, %v", runID, created, err)
	}
}

// fireCron fires a cron trigger of listings for its scheduled time at.
func (e *env) fireCron(trigger string, at time.Time) {
	e.t.Helper()
	key := ids.CronKey(at)
	created, err := e.s.Fire(e.ctx, store.Firing{
		Pipeline: "listings", Trigger: trigger, Key: key, RunID: ids.CronRun(at, trigger), Source: store.SourceCron,
		ScheduledAt: at,
	})
	if err != nil || !created {
		e.t.Fatalf("Fire %s at %s: created %v, %v", trigger, key, created, err)
	}
}

func (e *env) lease(n int) []store.Lease {
	e.t.Helper()
	ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node1", Max: n, TTL: time.Minute})
	if err != nil {
		e.t.Fatalf("Lease: %v", err)
	}
	return ls
}

// fixtureSteps are the steps of fixture and fixtureFixed.
var fixtureSteps = []string{"crawl", "audit", "score", "report", "alert"}

// leaseStep leases the oldest due delivery of step only, failing when there is none.
func (e *env) leaseStep(step string) store.Lease {
	e.t.Helper()
	return e.leaseOnly("listings", fixtureSteps, step)
}

// leaseOnly leases the oldest due delivery of step in a pipeline, holding its other steps back.
func (e *env) leaseOnly(pipelineName string, steps []string, step string) store.Lease {
	e.t.Helper()
	slots := map[store.StepRef]int{}
	for _, s := range steps {
		if s != step {
			slots[store.StepRef{Pipeline: pipelineName, Step: s}] = 0
		}
	}
	ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node1", Max: 1, TTL: time.Minute, Slots: slots})
	if err != nil || len(ls) != 1 {
		e.t.Fatalf("lease %s: %v, %v", step, ls, err)
	}
	return ls[0]
}

func out(key, body string) store.Output {
	return store.Output{Key: key, Payload: json.RawMessage(body)}
}

func (e *env) done(l store.Lease, outputs ...store.Output) store.DeliveryState {
	e.t.Helper()
	state, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Outputs: outputs})
	if err != nil {
		e.t.Fatalf("Done %s: %v", l.Step, err)
	}
	return state
}

func (e *env) fail(l store.Lease, retryable bool) store.DeliveryState {
	e.t.Helper()
	state, err := e.s.Fail(e.ctx, store.FailRequest{
		LeaseRef: l.Ref(), Attempt: l.Attempt, Class: "test", Message: "boom", Retryable: retryable,
	})
	if err != nil {
		e.t.Fatalf("Fail %s: %v", l.Step, err)
	}
	return state
}

func (e *env) run(id string) store.Run {
	e.t.Helper()
	r, err := e.s.Run(e.ctx, "listings", id)
	if err != nil {
		e.t.Fatalf("Run: %v", err)
	}
	return r
}

func (e *env) items(stream string) []store.Item {
	e.t.Helper()
	items, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: "listings", Stream: stream})
	if err != nil {
		e.t.Fatalf("Items: %v", err)
	}
	return items
}

func (e *env) deliveries(runID string) []store.Delivery {
	e.t.Helper()
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: "listings", RunID: runID})
	if err != nil {
		e.t.Fatalf("Deliveries: %v", err)
	}
	return ds
}

func (e *env) deadLetters(runID string) []store.DeadLetter {
	e.t.Helper()
	dls, err := e.s.DeadLetters(e.ctx, store.DeadLetterFilter{Pipeline: "listings", RunID: runID})
	if err != nil {
		e.t.Fatalf("DeadLetters: %v", err)
	}
	return dls
}

func (e *env) events() []store.Event {
	e.t.Helper()
	evs, err := e.s.Events(e.ctx, 0, 100000)
	if err != nil {
		e.t.Fatalf("Events: %v", err)
	}
	return evs
}

func (e *env) eventKinds(kind string) []store.Event {
	e.t.Helper()
	var out []store.Event
	for _, ev := range e.events() {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

// drain runs every due delivery to done with no outputs until nothing is due.
func (e *env) drain() {
	e.t.Helper()
	for {
		ls := e.lease(100)
		if len(ls) == 0 {
			return
		}
		for _, l := range ls {
			e.done(l)
		}
	}
}

func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error %v, want %v", err, target)
	}
}

func wantState[S comparable](t *testing.T, what string, got, want S) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

// otherPipeline is the fixture under another pipeline name.
var otherPipeline = strings.Replace(fixture, "pipeline: listings", "pipeline: other", 1)

// itemsIn returns the items of one run on stream.
func (e *env) itemsIn(pipelineName, runID, stream string) []store.Item {
	e.t.Helper()
	items, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: pipelineName, RunID: runID, Stream: stream})
	if err != nil {
		e.t.Fatalf("Items: %v", err)
	}
	return items
}

// on returns e for the subtest t: subtests that run in turn share a store, each with runs of its own.
func (e *env) on(t *testing.T) *env {
	c := *e
	c.t = t
	return &c
}
