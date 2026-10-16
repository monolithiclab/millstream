package storetest

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// The pipelines of the import tests (spec section 4.12): src crawls listings twice over (crawl and recrawl) and
// scores them into good, routing negative scores nowhere; deal and audit import its listings, deal as approved and
// audit as seen; relay imports deal's approved again.
const (
	srcPipeline = `
pipeline: src
triggers:
  go: { manual: {} }
steps:
  crawl: { action: ./actions/crawl, in: trigger.go, out: listings, retry: { maxAttempts: 1 } }
  recrawl: { action: ./actions/crawl, in: trigger.go, out: listings }
  score:
    action: ./actions/score
    in: listings
    out: [{ when: "item.score >= 0", out: good }]
`
	srcFixed = `
pipeline: src
triggers:
  go: { manual: {} }
steps:
  crawl: { action: ./actions/crawl, in: trigger.go, out: listings, retry: { maxAttempts: 1 } }
  recrawl: { action: ./actions/crawl, in: trigger.go, out: listings }
  score:
    action: ./actions/score
    in: listings
    out: [{ when: "true", out: good }]
`
	dealPipeline = `
pipeline: deal
vars: { tag: x }
streams:
  approved: { pipeline: src, stream: listings }
  scored: { pipeline: src, stream: good }
steps:
  notify: { action: ./actions/notify, in: approved }
  file: { action: ./actions/file, in: scored }
`
	auditPipeline = `
pipeline: audit
streams:
  seen: { pipeline: src, stream: listings }
steps:
  log: { action: ./actions/log, in: seen }
`
	relayPipeline = `
pipeline: relay
streams:
  again: { pipeline: deal, stream: approved }
steps:
  pass: { action: ./actions/pass, in: again }
`
)

// importSteps are the steps of the import tests' pipelines.
var importSteps = map[string][]string{
	"src": {"crawl", "recrawl", "score"}, "deal": {"notify", "file"}, "audit": {"log"}, "relay": {"pass"},
}

// leaseIn leases the oldest due delivery of one step, holding every other step of the import pipelines back.
func (e *env) leaseIn(pipelineName, step string) store.Lease {
	e.t.Helper()
	slots := map[store.StepRef]int{}
	for p, steps := range importSteps {
		for _, s := range steps {
			if p != pipelineName || s != step {
				slots[store.StepRef{Pipeline: p, Step: s}] = 0
			}
		}
	}
	ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node1", Max: 1, TTL: time.Minute, Slots: slots})
	if err != nil || len(ls) != 1 {
		e.t.Fatalf("lease %s.%s: %v, %v", pipelineName, step, ls, err)
	}
	return ls[0]
}

// applyVars applies a pipeline with the variables of the runs its imports open.
func (e *env) applyVars(src string, vars map[string]any) store.RevisionID {
	e.t.Helper()
	id, err := e.s.ApplyRevision(e.ctx, store.PackageRecord{Hash: hashOf(src), Bundle: []byte(src),
		Pipeline: parse(e.t, src), Vars: vars, VarSources: map[string]string{"tag": "env"}})
	if err != nil {
		e.t.Fatalf("ApplyRevision: %v", err)
	}
	return id
}

// runIn returns a run of a pipeline, failing when it does not exist.
func (e *env) runIn(pipelineName, id string) store.Run {
	e.t.Helper()
	r, err := e.s.Run(e.ctx, pipelineName, id)
	if err != nil {
		e.t.Fatalf("Run %s %s: %v", pipelineName, id, err)
	}
	return r
}

// drainIn completes every due delivery of one step of a pipeline with no outputs.
func (e *env) drainIn(pipelineName, step string) {
	e.t.Helper()
	for {
		ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: pipelineName, Step: step,
			State: store.DeliveryPending})
		if err != nil {
			e.t.Fatal(err)
		}
		if len(ds) == 0 {
			return
		}
		e.done(e.leaseIn(pipelineName, step))
	}
}

func TestImportCopiesAndRuns(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline, auditPipeline)
	e.applyVars(dealPipeline, map[string]any{"tag": "y"})
	e.firePipeline("src", "go", "r1")
	e.done(e.leaseIn("src", "crawl"), out("a", `{"n":1}`), out("b", `{"n":2}`))
	listings := e.itemsIn("src", "r1", "listings")
	if len(listings) != 2 {
		t.Fatalf("listings = %+v", listings)
	}

	// One run in each importer for the producer run, holding a copy of each item.
	dealRun := ids.ImportRun("approved", "r1")
	r := e.runIn("deal", dealRun)
	if r.Kind != store.RunKindImport || r.Trigger != "approved" || r.TriggerKey != "r1" ||
		r.TriggerSource != store.SourceImport || r.Producer != "src" || r.Generation != 0 ||
		r.Vars["tag"] != "y" || r.VarSources["tag"] != "env" || r.State != store.RunRunning || r.Revision != 3 {
		t.Fatalf("import run = %+v", r)
	}
	copies := e.itemsIn("deal", dealRun, "approved")
	if len(copies) != 2 {
		t.Fatalf("copies = %+v", copies)
	}
	for i, c := range copies {
		p := listings[i]
		if c.ID != ids.ImportItem("approved", p.ID) || !slices.Equal(c.Parents, []string{p.ID}) ||
			c.ParentPipeline != "src" || c.Key != p.Key || string(c.Payload) != string(p.Payload) ||
			c.Step != "approved" || c.Generation != 0 {
			t.Fatalf("copy %d = %+v of %+v", i, c, p)
		}
		if d := e.deliveryIn("deal", dealRun, c.ID); d.Step != "notify" || d.State != store.DeliveryPending {
			t.Fatalf("delivery of copy %d = %+v", i, d)
		}
	}
	if seen := e.itemsIn("audit", ids.ImportRun("seen", "r1"), "seen"); len(seen) != 2 ||
		seen[0].ID != ids.ImportItem("seen", listings[0].ID) {
		t.Fatalf("audit copies = %+v", seen)
	}
	// A later output of the same producer run lands in the same consumer run.
	e.done(e.leaseIn("src", "recrawl"), out("c", `{"n":3}`))
	if copies := e.itemsIn("deal", dealRun, "approved"); len(copies) != 3 {
		t.Fatalf("copies after recrawl = %d", len(copies))
	}
	if runs, err := e.s.Runs(e.ctx, store.RunFilter{Producer: "src", ProducerRun: "r1"}); err != nil ||
		len(runs) != 2 {
		t.Fatalf("import runs of r1 = %+v, %v", runs, err)
	}

	// Lineage across pipelines: the copies of an item are found from the producer's side, never by id alone.
	kids, err := e.s.Items(e.ctx, store.ItemFilter{Parents: []string{listings[0].ID}, ParentPipeline: "src"})
	if err != nil || len(kids) != 2 {
		t.Fatalf("copies of %s = %+v, %v", listings[0].ID, kids, err)
	}
	if same, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: "deal", Parents: []string{listings[0].ID}}); err != nil ||
		len(same) != 0 {
		t.Fatalf("same-pipeline children = %+v, %v", same, err)
	}

	// The consumer run settles only once the producer run has: its own work done first, then the producer's.
	e.drainIn("deal", "notify")
	wantState(t, "deal run with src open", e.runIn("deal", dealRun).State, store.RunRunning)
	for range 3 {
		e.done(e.leaseIn("src", "score"))
	}
	if r := e.runIn("src", "r1"); r.SettledAt.IsZero() {
		t.Fatalf("src run = %+v", r)
	}
	if r := e.runIn("deal", dealRun); r.State != store.RunSucceeded || r.SettledAt.IsZero() {
		t.Fatalf("deal run after src settled = %+v", r)
	}
	wantState(t, "audit run, its work pending", e.runIn("audit", ids.ImportRun("seen", "r1")).State, store.RunRunning)
	e.drainIn("audit", "log")
	wantState(t, "audit run", e.runIn("audit", ids.ImportRun("seen", "r1")).State, store.RunSucceeded)
}

// deliveryIn returns the delivery of an item in a run of a pipeline.
func (e *env) deliveryIn(pipelineName, runID, itemID string) store.Delivery {
	e.t.Helper()
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: pipelineName, RunID: runID, ItemID: itemID})
	if err != nil || len(ds) != 1 {
		e.t.Fatalf("deliveries of %s: %+v, %v", itemID, ds, err)
	}
	return ds[0]
}

// TestImportPausedConsumerAndCancel covers spec section 4.12's "each pipeline stays its own" and review focus 3: a
// paused consumer still receives, as pending deliveries, and its run settles once resumed and drained after the
// producer run was cancelled; a cancelled producer delivery publishes nothing to import; a cancelled consumer run
// receives nothing more.
func TestImportPausedConsumerAndCancel(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline, dealPipeline, auditPipeline)
	if err := e.s.SetPaused(e.ctx, "deal", "", true); err != nil {
		t.Fatal(err)
	}
	e.firePipeline("src", "go", "r1")
	e.done(e.leaseIn("src", "crawl"), out("a", `{}`), out("b", `{}`))
	dealRun := ids.ImportRun("approved", "r1")
	if ds := e.deliveriesIn("deal", dealRun); len(ds) != 2 || ds[0].State != store.DeliveryPending {
		t.Fatalf("paused consumer's deliveries = %+v", ds)
	}
	if ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "n", Max: 10, TTL: time.Minute,
		Exclude: []store.StepRef{{Pipeline: "src", Step: "recrawl"}, {Pipeline: "src", Step: "score"},
			{Pipeline: "audit", Step: "log"}}}); err != nil || len(ls) != 0 {
		t.Fatalf("leased from a paused consumer: %+v, %v", ls, err)
	}
	// Cancelling the producer run stops its new items; the consumer run finishes with what it received.
	if err := e.s.Cancel(e.ctx, "src", "r1"); err != nil {
		t.Fatal(err)
	}
	wantState(t, "src run", e.runIn("src", "r1").State, store.RunCancelled)
	wantState(t, "paused deal run", e.runIn("deal", dealRun).State, store.RunRunning)
	if err := e.s.SetPaused(e.ctx, "deal", "", false); err != nil {
		t.Fatal(err)
	}
	e.drainIn("deal", "notify")
	if r := e.runIn("deal", dealRun); r.State != store.RunSucceeded {
		t.Fatalf("deal run = %+v", r)
	}
	if copies := e.itemsIn("deal", dealRun, "approved"); len(copies) != 2 {
		t.Fatalf("deal received %d items, want the 2 the producer run published", len(copies))
	}

	// A delivery leased when its run is cancelled publishes nothing, so imports nothing.
	e.firePipeline("src", "go", "r2")
	l := e.leaseIn("src", "crawl")
	if err := e.s.Cancel(e.ctx, "src", "r2"); err != nil {
		t.Fatal(err)
	}
	wantState(t, "cancelled crawl", e.done(l, out("c", `{}`)), store.DeliveryCancelled)
	if _, err := e.s.Run(e.ctx, "deal", ids.ImportRun("approved", "r2")); !strings.Contains(errText(err), "not found") {
		t.Fatalf("an import run for a cancelled producer delivery: %v", err)
	}

	// A consumer run cancelled receives nothing more; the other importers still do.
	e.firePipeline("src", "go", "r3")
	e.done(e.leaseIn("src", "crawl"), out("d", `{}`))
	r3 := ids.ImportRun("approved", "r3")
	if err := e.s.Cancel(e.ctx, "deal", r3); err != nil {
		t.Fatal(err)
	}
	e.done(e.leaseIn("src", "recrawl"), out("e", `{}`))
	if copies := e.itemsIn("deal", r3, "approved"); len(copies) != 1 {
		t.Fatalf("cancelled consumer run received %d items, want 1", len(copies))
	}
	if seen := e.itemsIn("audit", ids.ImportRun("seen", "r3"), "seen"); len(seen) != 2 {
		t.Fatalf("audit received %d items, want 2", len(seen))
	}
	if r := e.runIn("deal", r3); r.State != store.RunCancelled || r.SettledAt.IsZero() {
		t.Fatalf("cancelled consumer run, src still open = %+v", r)
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (e *env) deliveriesIn(pipelineName, runID string) []store.Delivery {
	e.t.Helper()
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: pipelineName, RunID: runID})
	if err != nil {
		e.t.Fatal(err)
	}
	return ds
}

// TestImportProducerReplay: a producer replay run publishes like any run, so the consumer, paused meanwhile, gets a
// run of its own for it, in a generation of its own, holding exactly the replay's items once, which settles once
// resumed (review focus 3).
func TestImportProducerReplay(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline, dealPipeline)
	e.firePipeline("src", "go", "r1")
	e.done(e.leaseIn("src", "crawl"), out("a", `{}`))
	trigger := e.itemsIn("src", "r1", "trigger.go")[0].ID
	if err := e.s.SetPaused(e.ctx, "deal", "", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.s.StartReplay(e.ctx, store.ReplayStart{Pipeline: "src", Step: "crawl", RunID: "rp1",
		Key: "rp1", Items: []string{trigger}}); err != nil {
		t.Fatal(err)
	}
	e.done(e.leaseIn("src", "crawl"), out("a", `{}`))
	replayed := e.itemsIn("src", "rp1", "listings")
	consumer := ids.ImportRun("approved", "rp1")
	r := e.runIn("deal", consumer)
	copies := e.itemsIn("deal", consumer, "approved")
	if r.Generation != 1 || r.TriggerKey != "rp1" || len(replayed) != 1 || len(copies) != 1 ||
		copies[0].ID != ids.ImportItem("approved", replayed[0].ID) || copies[0].Generation != 1 {
		t.Fatalf("consumer run %+v with %+v for %+v", r, copies, replayed)
	}
	if first := e.itemsIn("deal", ids.ImportRun("approved", "r1"), "approved"); len(first) != 1 ||
		first[0].ID == copies[0].ID {
		t.Fatalf("the replay's copies collide with the first run's: %+v", first)
	}
	e.done(e.leaseIn("src", "recrawl"))
	e.drainIn("src", "score")
	wantState(t, "paused consumer run, the replay settled", e.runIn("deal", consumer).State, store.RunRunning)
	if err := e.s.SetPaused(e.ctx, "deal", "", false); err != nil {
		t.Fatal(err)
	}
	e.drainIn("deal", "notify")
	wantState(t, "consumer run of the replay", e.runIn("deal", consumer).State, store.RunSucceeded)
	if got := e.itemsIn("deal", consumer, "approved"); len(got) != 1 {
		t.Fatalf("the consumer run of the replay holds %d items, want 1", len(got))
	}
}

// TestImportBackfill: replay --stream copies the producer's retained items on the stream to a consumer that
// imported it after they were published, in a replay run, and delivers again the copies it already holds.
func TestImportBackfill(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline, auditPipeline)
	e.firePipeline("src", "go", "r1")
	e.done(e.leaseIn("src", "crawl"), out("a", `{}`), out("b", `{}`))
	e.apply(dealPipeline) // deal imports listings from now on: r1's items are not copied
	e.done(e.leaseIn("src", "recrawl"), out("c", `{}`))
	listings := e.itemsIn("src", "r1", "listings")
	r1 := ids.ImportRun("approved", "r1")
	if copies := e.itemsIn("deal", r1, "approved"); len(copies) != 1 {
		t.Fatalf("deal copies before the back-fill = %+v", copies)
	}
	pids := []string{listings[0].ID, listings[1].ID, listings[2].ID}
	gen, created, err := e.s.StartReplay(e.ctx, store.ReplayStart{Pipeline: "deal", Stream: "approved", RunID: "bf",
		Key: "bf", Items: pids})
	if err != nil || !created || gen != 1 {
		t.Fatalf("back-fill = %d, %v, %v", gen, created, err)
	}
	if r := e.runIn("deal", "bf"); r.Kind != store.RunKindReplay || r.Generation != 1 {
		t.Fatalf("back-fill run = %+v", r)
	}
	if got := e.itemsIn("deal", "bf", "approved"); len(got) != 2 || got[0].ID != ids.ImportItem("approved", pids[0]) ||
		got[0].ParentPipeline != "src" || got[0].Generation != 1 {
		t.Fatalf("back-filled copies = %+v", got)
	}
	ds := e.deliveriesIn("deal", "bf")
	if len(ds) != 3 || ds[2].ItemID != ids.ImportItem("approved", pids[2]) || ds[2].Generation != 1 {
		t.Fatalf("back-fill deliveries = %+v", ds)
	}
	_, created, err = e.s.StartReplay(e.ctx, store.ReplayStart{Pipeline: "deal", Stream: "approved", RunID: "bf",
		Key: "bf", Items: pids})
	if err != nil || created {
		t.Fatalf("back-fill again = %v, %v", created, err)
	}
	e.drainIn("deal", "notify")
	wantState(t, "back-fill run", e.runIn("deal", "bf").State, store.RunSucceeded)

	trigger := e.itemsIn("src", "r1", "trigger.go")[0].ID
	if err := e.s.Exec(e.ctx, `UPDATE items SET payload = 'null', payload_hash = ? WHERE pipeline = 'src' AND id = ?`,
		tombstoneHash, pids[1]); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		req    store.ReplayStart
		target error
	}{
		{"not imported", store.ReplayStart{Stream: "notify", Items: pids[:1]}, store.ErrInvalid},
		{"another stream", store.ReplayStart{Stream: "approved", Items: []string{trigger}}, store.ErrInvalid},
		{"expired", store.ReplayStart{Stream: "approved", Items: pids[1:2]}, store.ErrConflict},
		{"missing", store.ReplayStart{Stream: "approved", Items: []string{"it_gone"}}, store.ErrNotFound},
		{"step and stream", store.ReplayStart{Stream: "approved", Step: "notify", Items: pids[:1]}, store.ErrInvalid},
	} {
		tc.req.Pipeline, tc.req.RunID, tc.req.Key = "deal", "bf-"+tc.name, "k"
		_, _, err := e.s.StartReplay(e.ctx, tc.req)
		if !errors.Is(err, tc.target) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.target)
		}
	}
}

// TestImportFollowsRollback: the streams a pipeline imports, and the variables of the runs they open, are its
// active revision's, a rollback's included (spec section 4.12): rolled back to a revision that imports, it receives
// again with that revision's variables; rolled forward to one that does not, it receives nothing.
func TestImportFollowsRollback(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline)
	importing := e.applyVars(dealPipeline, map[string]any{"tag": "first"})
	dropped := e.applyVars(strings.Replace(dealPipeline, "stream: listings", "stream: nothing", 1),
		map[string]any{"tag": "second"})
	if _, err := e.s.Rollback(e.ctx, "deal", 0); err != nil {
		t.Fatal(err)
	}
	e.firePipeline("src", "go", "r1")
	e.done(e.leaseIn("src", "crawl"), out("a", `{}`))
	if r := e.runIn("deal", ids.ImportRun("approved", "r1")); r.Revision != importing || r.Vars["tag"] != "first" {
		t.Fatalf("import run after the rollback = %+v", r)
	}
	if _, err := e.s.Rollback(e.ctx, "deal", dropped); err != nil {
		t.Fatal(err)
	}
	e.firePipeline("src", "go", "r2")
	e.done(e.leaseIn("src", "crawl"), out("b", `{}`))
	if _, err := e.s.Run(e.ctx, "deal", ids.ImportRun("approved", "r2")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("import run on a revision that does not import the stream: %v", err)
	}
}
