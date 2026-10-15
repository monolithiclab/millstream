package sqlite

import (
	"encoding/json"
	"strings"
	"testing"

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
	t.Parallel()
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

// A delivery dead again after a replay reopens its dead letter: the dead-letter id is a function of the delivery.
func TestReplayDeadAgainReopens(t *testing.T) {
	t.Parallel()
	e, _ := deadRun(t)
	if _, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, false); err != nil {
		t.Fatal(err)
	}
	e.fail(e.leaseStep("crawl"), false)
	dls := e.deadLetters("r1")
	if len(dls) != 1 || dls[0].State != store.DeadLetterOpen {
		t.Fatalf("dead letters = %+v; want the one reopened", dls)
	}
	wantState(t, "dlq items", len(e.items("crawl.dlq")), 1)
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	e, _ := deadRun(t)
	e.apply(strings.Replace(fixture, "  crawl:\n", "  fetch:\n", 1))
	_, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, true)
	wantErr(t, err, store.ErrConflict)
	if r := e.run("r1"); r.Revision != 1 || r.State != store.RunCompletedWithErrors {
		t.Fatalf("run = %+v; want it untouched", r)
	}
	wantState(t, "migrations", len(e.eventKinds(store.EventRevisionMigrated)), 0)
}
