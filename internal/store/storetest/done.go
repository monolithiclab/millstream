package storetest

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

func TestDoneStaleLease(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	l := e.lease(1)[0]
	wrongToken := l.Ref()
	wrongToken.FencingToken = 7
	tests := []struct {
		name    string
		ref     store.LeaseRef
		attempt int
		want    error
	}{
		{"wrong token", wrongToken, 1, store.ErrStaleLease},
		{"wrong attempt", l.Ref(), 2, store.ErrStaleLease},
		{"unknown delivery", store.LeaseRef{Pipeline: "listings", DeliveryID: "dl_x"}, 1, store.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: tt.ref, Attempt: tt.attempt})
			wantErr(t, err, tt.want)
			_, err = e.s.Fail(e.ctx, store.FailRequest{LeaseRef: tt.ref, Attempt: tt.attempt, Retryable: true})
			wantErr(t, err, tt.want)
		})
	}
	t.Run("already done", func(t *testing.T) {
		e := e.on(t) // r1's lease is held: r2's crawl is the one due
		e.fire("r2")
		l := e.lease(1)[0]
		e.done(l)
		_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1})
		wantErr(t, err, store.ErrStaleLease)
	})
}

func TestDoneInvalidOutput(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	l := e.lease(1)[0]
	_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1, Outputs: []store.Output{out("", "{")}})
	wantErr(t, err, store.ErrInvalid)
}

// Review focus 2: two outputs where one has no key, or two with the same key, fail as a non-retryable key-rule
// error, never collide.
func TestDoneKeyRule(t *testing.T, open Opener) {
	base := newEnv(t, open, fixture)
	for i, tc := range []struct {
		name    string
		outputs []store.Output
		message string
	}{
		{"missing key", []store.Output{out("a", `{"score":1}`), out("", `{"score":2}`)}, "output 1 of 2 has no key"},
		{"duplicate key", []store.Output{out("a", `{"score":1}`), out("b", `{}`), out("a", `{"score":1}`)},
			`outputs 0 and 2 share the key "a"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testKeyRule(base.on(t), fmt.Sprint("r", i+1), tc.outputs, tc.message)
		})
	}
}

// testKeyRule fires run runID on e, whose other runs are settled, and checks the key rule kills its crawl.
func testKeyRule(e *env, runID string, outputs []store.Output, message string) {
	t := e.t
	e.fire(runID)
	l := e.leaseStep("crawl")
	state := e.done(l, outputs...)
	wantState(t, "state", state, store.DeliveryDead)
	d := e.deliveries(runID)[0]
	if d.State != store.DeliveryDead || d.ErrorClass != store.ClassKeyRule || d.Attempt != 1 ||
		!strings.Contains(d.ErrorMessage, message) {
		t.Fatalf("delivery = %+v; want dead at once with class keyRule and %q", d, message)
	}
	if n := len(e.items("listings.raw")); n != 0 {
		t.Fatalf("%d outputs published", n)
	}
	dls := e.deadLetters(runID)
	if len(dls) != 1 || dls[0].Kind != store.DeadLetterDead || dls[0].Stream != "crawl.dlq" ||
		dls[0].ID != ids.DeadLetter("crawl.dlq", l.DeliveryID, "", 0) || dls[0].State != store.DeadLetterOpen {
		t.Fatalf("dead letters = %+v", dls)
	}
	dlItems := e.itemsIn("listings", runID, "crawl.dlq")
	if len(dlItems) != 1 || dlItems[0].Parents[0] != l.Item.ID {
		t.Fatalf("dead-letter items = %+v", dlItems)
	}
	var body struct {
		Kind, OriginStep, OriginDelivery, Error string
		Item                                    struct{ ID string }
	}
	if err := json.Unmarshal(dlItems[0].Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body.Kind != "dead" || body.OriginStep != "crawl" || body.OriginDelivery != l.DeliveryID ||
		body.Item.ID != l.Item.ID || !strings.HasPrefix(body.Error, "keyRule: ") {
		t.Fatalf("dead-letter payload = %s", dlItems[0].Payload)
	}
	// The alert step reads crawl.dlq and holds the run open until it is done.
	alert := e.leaseStep("alert")
	if r := e.run(runID); r.State != store.RunRunning {
		t.Fatalf("run %s before alert is done", r.State)
	}
	e.done(alert)
	wantState(t, "run", e.run(runID).State, store.RunCompletedWithErrors)
}

func TestDoneRouteEvaluationFails(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{}`))
	score := e.leaseStep("score")
	state := e.done(score, out("", `{"score":"high"}`))
	wantState(t, "state", state, store.DeliveryDead)
	for _, d := range e.deliveries("r1") {
		if d.Step == "score" && (d.ErrorClass != store.ClassRoute || d.State != store.DeliveryDead) {
			t.Fatalf("score delivery = %+v", d)
		}
	}
	if n := len(e.items("listings.good")) + len(e.items("listings.rest")); n != 0 {
		t.Fatalf("%d outputs published", n)
	}
}

func TestDoneRouteDoesNotCompile(t *testing.T, open Opener) {
	src := strings.Replace(fixture, `"item.score >= 0"`, `"item.score >="`, 1)
	e := newEnv(t, open, src)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{}`))
	// The first route does not match, so the one that does not compile is evaluated.
	state := e.done(e.leaseStep("score"), out("", `{"score":10}`))
	wantState(t, "state", state, store.DeliveryDead)
}

// redo runs a done delivery again: its lease is restored behind the units' back, as for a delivery whose Done
// committed but whose answer was lost, run again by an owner that still holds the same lease. Its outputs are
// published under the same item ids as the first time.
func (e *env) redo(l store.Lease, outputs ...store.Output) {
	e.t.Helper()
	if err := e.s.Exec(e.ctx, `UPDATE deliveries SET state = ? WHERE pipeline = ? AND id = ?`, store.DeliveryLeased,
		l.Pipeline, l.DeliveryID); err != nil {
		e.t.Fatal(err)
	}
	e.done(l, outputs...)
}

// The first commit of an item wins (spec section 5.2): publishing an existing item id again is a no-op with the same
// payload and records nondeterministicOutput with another.
func TestDoneFirstCommit(t *testing.T, open Opener) {
	base := newEnv(t, open, fixture)
	t.Run("same payload is a no-op", func(t *testing.T) {
		e := base.on(t)
		e.fire("r1")
		l := e.leaseStep("crawl")
		e.done(l, out("a", `{"score":1, "x":2}`))
		e.redo(l, out("a", `{"x":2,"score":1}`))
		if n := len(e.itemsIn("listings", "r1", "listings.raw")); n != 1 {
			t.Fatalf("%d items", n)
		}
		if n := len(e.eventKinds(store.EventNondeterministicOutput)); n != 0 {
			t.Fatalf("%d nondeterministicOutput events for an equal payload", n)
		}
	})
	t.Run("different payload keeps the first", func(t *testing.T) {
		e := base.on(t)
		e.fire("r2")
		l := e.leaseStep("crawl")
		e.done(l, out("a", `{"score":1}`))
		e.redo(l, out("a", `{"score":2}`))
		items := e.itemsIn("listings", "r2", "listings.raw")
		if len(items) != 1 || string(items[0].Payload) != `{"score":1}` {
			t.Fatalf("items = %+v", items)
		}
		evs := e.eventKinds(store.EventNondeterministicOutput)
		if len(evs) != 2 { // one per stream of the fan-out
			t.Fatalf("%d nondeterministicOutput events, want 2", len(evs))
		}
		if !strings.Contains(string(evs[0].Data), `"step":"crawl"`) {
			t.Errorf("event data %s", evs[0].Data)
		}
		var scoreDeliveries int
		for _, d := range e.deliveries("r2") {
			if d.Step == "score" {
				scoreDeliveries++
			}
		}
		wantState(t, "score deliveries", scoreDeliveries, 1)
	})
}
