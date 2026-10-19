//go:build e2e

package storetest

import (
	"errors"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// Import variants the e2e build adds: reopening, chains and cycles, and following a producer's reviews. The default
// build covers the Stream import unit through the other import tests.

// TestImportReopens: a dead-letter replay that reopens the producer run reopens its import runs, which settle again
// with it; an unrouted output replayed publishes its copy into the import run it had let settle.
func TestImportReopens(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline, dealPipeline)
	e.firePipeline("src", "go", "r1")
	wantState(t, "crawl", e.fail(e.leaseIn("src", "crawl"), false), store.DeliveryDead)
	e.done(e.leaseIn("src", "recrawl"), out("a", `{}`), out("b", `{}`))
	e.done(e.leaseIn("src", "score"), out("", `{"score":1}`))
	e.done(e.leaseIn("src", "score"), out("", `{"score":-1}`)) // unrouted
	e.drainIn("deal", "notify")
	e.drainIn("deal", "file")
	approvedRun, scoredRun := ids.ImportRun("approved", "r1"), ids.ImportRun("scored", "r1")
	wantState(t, "src run", e.runIn("src", "r1").State, store.RunCompletedWithErrors)
	wantState(t, "deal approved run", e.runIn("deal", approvedRun).State, store.RunSucceeded)
	wantState(t, "deal scored run", e.runIn("deal", scoredRun).State, store.RunSucceeded)

	// The unrouted output, replayed on the fixed revision, lands in the scored run, reopened; the dead crawl,
	// replayed, reopens src and with it the approved run.
	e.apply(srcFixed)
	dls := e.openDeadLetters("src", "r1")
	if n, err := e.s.ReplayDeadLetters(e.ctx, "src", "r1", nil, true); err != nil || n != len(dls) || n != 2 {
		t.Fatalf("ReplayDeadLetters = %d, %v (open %d)", n, err, len(dls))
	}
	if copies := e.itemsIn("deal", scoredRun, "scored"); len(copies) != 2 {
		t.Fatalf("scored copies = %+v", copies)
	}
	wantState(t, "src reopened", e.runIn("src", "r1").State, store.RunRunning)
	wantState(t, "approved run reopened", e.runIn("deal", approvedRun).State, store.RunRunning)
	wantState(t, "scored run reopened", e.runIn("deal", scoredRun).State, store.RunRunning)
	reopened := 0
	for _, ev := range e.eventKinds(store.EventRunReopened) {
		if ev.Pipeline == "deal" {
			reopened++
		}
	}
	if reopened != 2 {
		t.Fatalf("%d runReopened events in deal, want 2", reopened)
	}
	e.done(e.leaseIn("src", "crawl"))
	e.drainIn("deal", "file")
	wantState(t, "src settled again", e.runIn("src", "r1").State, store.RunSucceeded)
	wantState(t, "approved run settled again", e.runIn("deal", approvedRun).State, store.RunSucceeded)
	wantState(t, "scored run settled again", e.runIn("deal", scoredRun).State, store.RunSucceeded)
}

// TestImportRevisionsAndChains: a revision that drops an import stops the copies; an imported stream imported again
// copies on down the chain; a cycle between pipelines, which check refuses, ends instead of copying for ever.
func TestImportRevisionsAndChains(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline, dealPipeline, relayPipeline)
	e.firePipeline("src", "go", "r1")
	e.done(e.leaseIn("src", "crawl"), out("a", `{}`))
	listing := e.itemsIn("src", "r1", "listings")[0]
	dealRun := ids.ImportRun("approved", "r1")
	relayed := e.itemsIn("relay", ids.ImportRun("again", dealRun), "again")
	if len(relayed) != 1 || relayed[0].ID != ids.ImportItem("again", ids.ImportItem("approved", listing.ID)) ||
		relayed[0].ParentPipeline != "deal" {
		t.Fatalf("relayed = %+v", relayed)
	}
	// relay's run waits for deal's, which waits for src's.
	e.drainIn("deal", "notify")
	e.drainIn("relay", "pass")
	wantState(t, "relay run", e.runIn("relay", ids.ImportRun("again", dealRun)).State, store.RunRunning)
	e.done(e.leaseIn("src", "recrawl"))
	e.done(e.leaseIn("src", "score"), out("", `{"score":1}`))
	e.drainIn("deal", "file")
	wantState(t, "relay run", e.runIn("relay", ids.ImportRun("again", dealRun)).State, store.RunSucceeded)

	// deal drops its imports: src's new items reach nobody.
	e.apply(strings.Replace(dealPipeline, "approved: { pipeline: src, stream: listings }",
		"approved: { pipeline: src, stream: nothing }", 1))
	e.firePipeline("src", "go", "r2")
	e.done(e.leaseIn("src", "crawl"), out("b", `{}`))
	if _, err := e.s.Run(e.ctx, "deal", ids.ImportRun("approved", "r2")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("import run after the import was dropped: %v", err)
	}

	// A cycle: src imports relay's again, which comes from src through deal.
	e.apply(dealPipeline)
	e.apply(strings.Replace(srcPipeline, "steps:", "streams:\n  back: { pipeline: relay, stream: again }\nsteps:\n"+
		"  echo: { action: ./actions/echo, in: back }", 1))
	e.firePipeline("src", "go", "r3")
	e.done(e.leaseIn("src", "crawl"), out("c", `{}`))
	if got := e.itemsIn("relay", ids.ImportRun("again", ids.ImportRun("approved", "r3")), "again"); len(got) != 1 {
		t.Fatalf("relay copies in the cycle = %+v", got)
	}
	if back := e.itemsIn("src", "", "back"); len(back) != 0 {
		t.Fatalf("the cycle copied back into src: %+v", back)
	}
}

// TestImportFollowsReviews: an import run with nothing of its own left, held open by a producer run that waits only
// on reviews, waits on them too, so run --once stops waiting on both; the decision settles both.
func TestImportFollowsReviews(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline, dealPipeline)
	e.firePipeline("src", "go", "r1")
	e.done(e.leaseIn("src", "crawl"))
	e.done(e.leaseIn("src", "recrawl"), out("a", `{}`))
	l := e.leaseIn("src", "score")
	res, err := e.s.Park(e.ctx, store.ParkRequest{LeaseRef: l.Ref(), Attempt: l.Attempt,
		Spec: store.ReviewSpec{Inbox: "analysts"}, Display: "a"})
	if err != nil || res.State != store.DeliveryAwaitingReview {
		t.Fatalf("Park = %+v, %v", res, err)
	}
	dealRun := ids.ImportRun("approved", "r1")
	wantState(t, "deal run with its own work", e.runIn("deal", dealRun).State, store.RunRunning)
	e.drainIn("deal", "notify")
	wantState(t, "src run", e.runIn("src", "r1").State, store.RunWaitingReview)
	wantState(t, "deal run held by src's review", e.runIn("deal", dealRun).State, store.RunWaitingReview)
	if _, err := e.s.DecideReview(e.ctx, store.ReviewDecisionRequest{Pipeline: "src", DeliveryID: l.DeliveryID,
		Version: 1, Decision: "reject", Reviewer: "ana"}); err != nil {
		t.Fatal(err)
	}
	wantState(t, "deal run", e.runIn("deal", dealRun).State, store.RunSucceeded)
}

func (e *env) openDeadLetters(pipelineName, runID string) []store.DeadLetter {
	e.t.Helper()
	dls, err := e.s.DeadLetters(e.ctx, store.DeadLetterFilter{Pipeline: pipelineName, RunID: runID,
		State: store.DeadLetterOpen})
	if err != nil {
		e.t.Fatal(err)
	}
	return dls
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
