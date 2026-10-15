package sqlite

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

func TestDoneFanOutAndSettle(t *testing.T) {
	t.Parallel()
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

func TestDoneStaleLease(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
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
			t.Parallel()
			_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: tt.ref, Attempt: tt.attempt})
			wantErr(t, err, tt.want)
			_, err = e.s.Fail(e.ctx, store.FailRequest{LeaseRef: tt.ref, Attempt: tt.attempt, Retryable: true})
			wantErr(t, err, tt.want)
		})
	}
	t.Run("already done", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, fixture)
		e.fire("r1")
		l := e.lease(1)[0]
		e.done(l)
		_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1})
		wantErr(t, err, store.ErrStaleLease)
	})
}

func TestDoneInvalidOutput(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.lease(1)[0]
	_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1, Outputs: []store.Output{out("", "{")}})
	wantErr(t, err, store.ErrInvalid)
}

// Review focus 2: two outputs where one has no key fail as a non-retryable protocol error, never collide.
func TestDoneKeyRule(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.leaseStep("crawl")
	state := e.done(l, out("a", `{"score":1}`), out("", `{"score":2}`))
	wantState(t, "state", state, store.DeliveryDead)
	d := e.deliveries("r1")[0]
	if d.State != store.DeliveryDead || d.ErrorClass != store.ClassKeyRule || d.Attempt != 1 {
		t.Fatalf("delivery = %+v; want dead at once with class keyRule", d)
	}
	if n := len(e.items("listings.raw")); n != 0 {
		t.Fatalf("%d outputs published", n)
	}
	dls := e.deadLetters("r1")
	if len(dls) != 1 || dls[0].Kind != store.DeadLetterDead || dls[0].Stream != "crawl.dlq" ||
		dls[0].ID != ids.DeadLetter("crawl.dlq", l.DeliveryID, "") || dls[0].State != store.DeadLetterOpen {
		t.Fatalf("dead letters = %+v", dls)
	}
	dlItems := e.items("crawl.dlq")
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
	if r := e.run("r1"); r.State != store.RunRunning {
		t.Fatalf("run %s before alert is done", r.State)
	}
	e.done(alert)
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
}

// The first commit of an item wins (spec section 5.2). Two outputs with the same key land on the same id.
func TestDoneFirstCommit(t *testing.T) {
	t.Parallel()
	t.Run("same payload is a no-op", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, fixture)
		e.fire("r1")
		e.done(e.leaseStep("crawl"), out("a", `{"score":1, "x":2}`), out("a", `{"x":2,"score":1}`))
		if n := len(e.items("listings.raw")); n != 1 {
			t.Fatalf("%d items", n)
		}
		if n := len(e.eventKinds(store.EventNondeterministicOutput)); n != 0 {
			t.Fatalf("%d nondeterministicOutput events for an equal payload", n)
		}
	})
	t.Run("different payload keeps the first", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, fixture)
		e.fire("r1")
		e.done(e.leaseStep("crawl"), out("a", `{"score":1}`), out("a", `{"score":2}`))
		items := e.items("listings.raw")
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
		for _, d := range e.deliveries("r1") {
			if d.Step == "score" {
				scoreDeliveries++
			}
		}
		wantState(t, "score deliveries", scoreDeliveries, 1)
	})
}

func TestDoneRouting(t *testing.T) {
	t.Parallel()
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
		dls[0].ID != ids.DeadLetter("score.dlq", score.DeliveryID, outputID) || dls[0].OriginStep != "score" {
		t.Fatalf("dead letters = %+v", dls)
	}
	e.drain()
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
}

func TestDoneRouteEvaluationFails(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
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

func TestDoneRouteDoesNotCompile(t *testing.T) {
	t.Parallel()
	src := strings.Replace(fixture, `"item.score >= 0"`, `"item.score >="`, 1)
	e := newEnv(t, src)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{}`))
	// The first route does not match, so the one that does not compile is evaluated.
	state := e.done(e.leaseStep("score"), out("", `{"score":10}`))
	wantState(t, "state", state, store.DeliveryDead)
}

func TestDoneTerminalStepDropsOutputs(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{"score":1}`))
	e.done(e.leaseStep("audit"), out("", `{"ignored":true}`))
	for _, l := range e.lease(10) {
		e.done(l)
	}
	wantState(t, "run", e.run("r1").State, store.RunSucceeded)
}
