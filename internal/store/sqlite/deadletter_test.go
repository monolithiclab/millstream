package sqlite

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// deadRun leaves run r1 completedWithErrors with one dead letter of kind dead on crawl.
func deadRun(t *testing.T) (*env, store.Lease) {
	t.Helper()
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.leaseStep("crawl")
	e.fail(l, false)
	e.drain() // alert
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
	return e, l
}

func TestReplayDead(t *testing.T) {
	e, l := deadRun(t)
	n, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, false)
	if err != nil || n != 1 {
		t.Fatalf("Replay = %d, %v", n, err)
	}
	r := e.run("r1")
	if r.State != store.RunRunning || !r.SettledAt.IsZero() {
		t.Fatalf("run = %+v; want reopened", r)
	}
	if dl := e.deadLetters("r1")[0]; dl.State != store.DeadLetterReplayed {
		t.Fatalf("dead letter %s", dl.State)
	}
	again := e.leaseStep("crawl")
	if again.DeliveryID != l.DeliveryID || again.Attempt != 1 || again.FencingToken != l.FencingToken+1 {
		t.Fatalf("replayed lease = %+v; want the same delivery, attempt 1", again)
	}
	e.done(again, out("a", `{"score":90}`))
	e.drain()
	wantState(t, "run", e.run("r1").State, store.RunSucceeded)
	wantState(t, "reopened events", len(e.eventKinds(store.EventRunReopened)), 1)
	wantState(t, "replayed events", len(e.eventKinds(store.EventDeadLetterReplayed)), 1)
}

// A delivery dead again after a replay publishes a new dead letter, under the next failure sequence, so readers of
// the dlq stream see the new failure; the replayed one stays replayed.
func TestReplayDeadAgainCreatesNewDeadLetter(t *testing.T) {
	e, l := deadRun(t)
	if _, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, false); err != nil {
		t.Fatal(err)
	}
	e.fail(e.leaseStep("crawl"), false)
	dls := e.deadLetters("r1")
	states := map[string]store.DeadLetterState{}
	for _, dl := range dls {
		states[dl.ID] = dl.State
	}
	first, second := ids.DeadLetter("crawl.dlq", l.DeliveryID, "", 0), ids.DeadLetter("crawl.dlq", l.DeliveryID, "", 1)
	if len(dls) != 2 || states[first] != store.DeadLetterReplayed || states[second] != store.DeadLetterOpen {
		t.Fatalf("dead letters = %+v; want %s replayed and %s open", dls, first, second)
	}
	wantState(t, "dlq items", len(e.items("crawl.dlq")), 2)
	e.drain() // alert reads the new dead letter
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
}

// unroutedRun leaves run r1 completedWithErrors with one unrouted dead letter on score.
func unroutedRun(t *testing.T) *env {
	t.Helper()
	e := newEnv(t, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{"score":1}`))
	e.done(e.leaseStep("score"), out("neg", `{"score":-3}`))
	e.drain()
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
	return e
}

func TestReplayUnroutedWithoutMigrateStaysOpen(t *testing.T) {
	e := unroutedRun(t)
	n, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, false)
	if err != nil || n != 0 {
		t.Fatalf("Replay = %d, %v; want nothing replayed", n, err)
	}
	if dl := e.deadLetters("r1")[0]; dl.State != store.DeadLetterOpen {
		t.Fatalf("dead letter %s", dl.State)
	}
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
}

func TestReplayUnroutedWithMigrate(t *testing.T) {
	e := unroutedRun(t)
	fixed := e.apply(fixtureFixed)
	dl := e.deadLetters("r1")[0]
	n, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", []string{dl.ID}, true)
	if err != nil || n != 1 {
		t.Fatalf("Replay = %d, %v", n, err)
	}
	r := e.run("r1")
	if r.Revision != fixed || r.State != store.RunRunning {
		t.Fatalf("run = %+v; want migrated to %d and running", r, fixed)
	}
	migrated := e.eventKinds(store.EventRevisionMigrated)
	if len(migrated) != 1 {
		t.Fatalf("%d revisionMigrated events", len(migrated))
	}
	var data struct{ From, To int64 }
	if err := json.Unmarshal(migrated[0].Data, &data); err != nil || data.From != 1 || data.To != int64(fixed) {
		t.Fatalf("migration event %s", migrated[0].Data)
	}
	rest := e.items("listings.rest")
	if len(rest) != 1 || rest[0].Key != "neg" || string(rest[0].Payload) != `{"score":-3}` {
		t.Fatalf("rest = %+v", rest)
	}
	e.drain() // report
	wantState(t, "run", e.run("r1").State, store.RunSucceeded)
	// Migrating to the revision the run already has records nothing more.
	if _, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, true); err != nil {
		t.Fatal(err)
	}
	wantState(t, "migrations", len(e.eventKinds(store.EventRevisionMigrated)), 1)
}

func TestReplayErrors(t *testing.T) {
	e, _ := deadRun(t)
	_, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", []string{"it_nope"}, false)
	wantErr(t, err, store.ErrNotFound)
	_, err = e.s.ReplayDeadLetters(e.ctx, "listings", "nope", nil, false)
	wantErr(t, err, store.ErrNotFound)

	c := newEnv(t, fixture)
	c.fire("r1")
	c.fail(c.leaseStep("crawl"), false)
	if err := c.s.Cancel(c.ctx, "listings", "r1"); err != nil {
		t.Fatal(err)
	}
	_, err = c.s.ReplayDeadLetters(c.ctx, "listings", "r1", nil, false)
	wantErr(t, err, store.ErrConflict)
}

func TestDiscard(t *testing.T) {
	e, _ := deadRun(t)
	dl := e.deadLetters("r1")[0]
	n, err := e.s.DiscardDeadLetters(e.ctx, "listings", []string{dl.ID})
	if err != nil || n != 1 {
		t.Fatalf("Discard = %d, %v", n, err)
	}
	if got := e.deadLetters("r1")[0]; got.State != store.DeadLetterDiscarded {
		t.Fatalf("dead letter %s", got.State)
	}
	// A discarded dead letter still counts against the run.
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
	_, err = e.s.DiscardDeadLetters(e.ctx, "listings", []string{dl.ID})
	wantErr(t, err, store.ErrNotFound)
	_, err = e.s.DiscardDeadLetters(e.ctx, "listings", nil)
	wantErr(t, err, store.ErrInvalid)
	if n, _ := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, false); n != 0 {
		t.Fatalf("replayed %d discarded dead letters", n)
	}
}

// A run whose dead letter is discarded before it settles settles completedWithErrors.
func TestDiscardBeforeSettlement(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.fail(e.leaseStep("crawl"), false)
	dl := e.deadLetters("r1")[0]
	if _, err := e.s.DiscardDeadLetters(e.ctx, "listings", []string{dl.ID}); err != nil {
		t.Fatal(err)
	}
	wantState(t, "run", e.run("r1").State, store.RunRunning) // alert still pending
	e.drain()
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
}

// Replaying into a revision that no longer has the dead delivery's step fails whole: the run keeps its revision.
func TestReplayMigrateToRevisionWithoutStep(t *testing.T) {
	e, _ := deadRun(t)
	e.apply(strings.Replace(fixture, "  crawl:\n", "  fetch:\n", 1))
	_, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, true)
	wantErr(t, err, store.ErrConflict)
	if r := e.run("r1"); r.Revision != 1 || r.State != store.RunCompletedWithErrors {
		t.Fatalf("run = %+v; want it untouched", r)
	}
	wantState(t, "migrations", len(e.eventKinds(store.EventRevisionMigrated)), 0)
}

// Migrating refuses a target revision that lacks the step of a delivery still live in the run: that delivery would
// have no step to run, route or dead-letter on. A target that keeps the step is accepted.
func TestReplayMigrateRefusedWhileLiveStepMissing(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{"score":1}`)) // audit and score deliveries, pending
	e.fail(e.leaseStep("score"), false)                   // a dead letter to replay; audit stays live
	e.apply(strings.Replace(fixture, "  audit:\n    action: ./actions/audit\n    in: listings.audit\n", "", 1))
	_, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, true)
	wantErr(t, err, store.ErrConflict)
	if !strings.Contains(err.Error(), "audit") {
		t.Errorf("error %v does not name the missing step", err)
	}
	if r := e.run("r1"); r.Revision != 1 {
		t.Fatalf("run moved to revision %d", r.Revision)
	}
	wantState(t, "migrations", len(e.eventKinds(store.EventRevisionMigrated)), 0)

	fixed := e.apply(fixtureFixed)
	if n, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, true); err != nil || n != 1 {
		t.Fatalf("Replay into a revision with every live step = %d, %v", n, err)
	}
	wantState(t, "revision", e.run("r1").Revision, fixed)
}

// moveRun points a run at another revision behind the units' back, as a run migrated before migrate checked live
// steps would be.
func moveRun(e *env, runID string, rev store.RevisionID) {
	e.t.Helper()
	err := e.s.submit(e.ctx, "test", func(w *wtx) error {
		_, err := w.exec(`UPDATE runs SET revision = ? WHERE pipeline = 'listings' AND id = ?`, rev, runID)
		return err
	})
	if err != nil {
		e.t.Fatal(err)
	}
}

// A delivery whose step its revision lacks ends dead on the default <step>.dlq, whether it fails or reports done,
// instead of staying leased forever behind an error.
func TestOrphanStepEndsDead(t *testing.T) {
	for _, report := range []string{"fail", "done"} {
		t.Run(report, func(t *testing.T) {
			e := newEnv(t, fixture)
			e.fire("r1")
			e.done(e.leaseStep("crawl"), out("a", `{"score":1}`))
			l := e.leaseStep("audit")
			moveRun(e, "r1", e.apply(strings.Replace(fixture, "  audit:\n", "  auditor:\n", 1)))
			var state store.DeliveryState
			var err error
			if report == "fail" {
				state, err = e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: 1, Class: "test",
					Message: "boom", Retryable: true})
			} else {
				state, err = e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1})
			}
			if err != nil || state != store.DeliveryDead {
				t.Fatalf("%s = %s, %v; want dead", report, state, err)
			}
			dls := e.deadLetters("r1")
			if len(dls) != 1 || dls[0].Stream != "audit.dlq" || dls[0].OriginStep != "audit" {
				t.Fatalf("dead letters = %+v; want one on audit.dlq", dls)
			}
		})
	}
}
