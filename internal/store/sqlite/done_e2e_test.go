//go:build e2e

package sqlite

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"

	"github.com/monolithiclab/millstream/internal/store"
)

// Done variants the e2e build adds: the default build covers the same paths through the other Done tests.

func TestDoneTerminalStepDropsOutputs(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{"score":1}`))
	e.done(e.leaseStep("audit"), out("", `{"ignored":true}`))
	for _, l := range e.lease(10) {
		e.done(l)
	}
	wantState(t, "run", e.run("r1").State, store.RunSucceeded)
}

func TestDoneFanOutAndSettle(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	crawl := e.leaseStep("crawl")
	state, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: crawl.Ref(), Attempt: 1, CostUSD: 0.25, Outputs: []store.Output{
		out("a", `{"score": 80, "url": "https://example.com/a"}`),
		out("b", `{"url":"https://example.com/b","score":50}`),
	}})
	if err != nil || state != store.DeliveryDone {
		t.Fatalf("Done = %v, %v", state, err)
	}
	raw := e.items("listings.raw")
	if len(raw) != 2 || len(e.items("listings.audit")) != 2 {
		t.Fatalf("raw %d audit %d items, want 2 each", len(raw), len(e.items("listings.audit")))
	}
	a := raw[0]
	if a.ID != ids.Item("crawl", "listings.raw", crawl.Item.ID, 0, "a", 0) || a.Key != "a" || a.Step != "crawl" ||
		len(a.Parents) != 1 || a.Parents[0] != crawl.Item.ID || a.RunID != "r1" {
		t.Fatalf("item = %+v", a)
	}
	if string(a.Payload) != `{"score":80,"url":"https://example.com/a"}` {
		t.Errorf("payload not canonical: %s", a.Payload)
	}
	ds := e.deliveries("r1")
	steps := map[string]int{}
	for _, d := range ds {
		steps[d.Step]++
	}
	if steps["score"] != 2 || steps["audit"] != 2 || ds[0].State != store.DeliveryDone || ds[0].CostUSD != 0.25 {
		t.Fatalf("deliveries %+v", ds)
	}
	if r := e.run("r1"); r.State != store.RunRunning || r.CostUSD != 0.25 {
		t.Fatalf("run = %+v", r)
	}

	// Score routes 80 to good and 50 to rest; report and audit finish; the run succeeds.
	for _, l := range e.lease(10) {
		if l.Step == "score" {
			e.done(l, out("", string(l.Item.Payload)))
		} else {
			e.done(l)
		}
	}
	if len(e.items("listings.good")) != 1 || len(e.items("listings.rest")) != 1 {
		t.Fatalf("good %v rest %v", e.items("listings.good"), e.items("listings.rest"))
	}
	e.drain()
	r := e.run("r1")
	if r.State != store.RunSucceeded || r.SettledAt.IsZero() {
		t.Fatalf("run = %+v, want succeeded", r)
	}
	if n := len(e.eventKinds(store.EventRunSettled)); n != 1 {
		t.Fatalf("%d runSettled events", n)
	}
}

func TestDoneRouting(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{"score":1}`))
	score := e.leaseStep("score")
	state := e.done(score, out("hi", `{"score":90}`), out("mid", `{"score":10}`), out("neg", `{"score":-5}`))
	wantState(t, "state", state, store.DeliveryDone)
	good, rest := e.items("listings.good"), e.items("listings.rest")
	if len(good) != 1 || good[0].Key != "hi" || len(rest) != 1 || rest[0].Key != "mid" {
		t.Fatalf("good %+v rest %+v; want first match", good, rest)
	}
	dls := e.deadLetters("r1")
	outputID := ids.Item("score", "", score.Item.ID, 0, "neg", 2)
	if len(dls) != 1 || dls[0].Kind != store.DeadLetterUnrouted || dls[0].Stream != "score.dlq" ||
		dls[0].ID != ids.DeadLetter("score.dlq", score.DeliveryID, outputID, 0) || dls[0].OriginStep != "score" {
		t.Fatalf("dead letters = %+v", dls)
	}
	e.drain()
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
}
