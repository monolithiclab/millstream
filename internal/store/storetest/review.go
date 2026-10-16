package storetest

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// reviewFixture runs crawl's listings through a review step into a report.
const reviewFixture = `
pipeline: listings
triggers:
  daily: { cron: "0 6 * * *" }
steps:
  crawl: { action: ./actions/crawl, in: trigger.daily, out: listings.raw }
  gate: { action: review, in: listings.raw, out: listings.ok }
  report: { action: ./actions/report, in: listings.ok }
`

var reviewSteps = []string{"crawl", "gate", "report"}

// analysts is the review spec most tests park with: an editable price and notes, two reason codes, and an output
// schema requiring an integer price.
var analysts = store.ReviewSpec{Inbox: "analysts", Editable: []string{"/notes", "/price"},
	Reasons: []string{"overpriced", "stale"},
	Schemas: []json.RawMessage{json.RawMessage(`{"type":"object","properties":{"price":{"type":"integer"}},"required":["price"]}`)}}

// parkAll fires run r1, crawls the listings keyed by keys (each priced at 100), and parks every gate delivery with
// the spec specFor gives its key. It returns the delivery ids by key.
func (e *env) parkAll(specFor func(key string) store.ReviewSpec, keys ...string) map[string]string {
	e.t.Helper()
	e.fire("r1")
	outs := make([]store.Output, len(keys))
	for i, k := range keys {
		outs[i] = out(k, `{"city":"Lyon","price":100}`)
	}
	e.done(e.leaseGate("crawl"), outs...)
	ids := map[string]string{}
	for range keys {
		l := e.leaseGate("gate")
		res, err := e.s.Park(e.ctx, store.ParkRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Spec: specFor(l.Item.Key),
			Display: "listing " + l.Item.Key})
		if err != nil || res.State != store.DeliveryAwaitingReview {
			e.t.Fatalf("Park %s = %+v, %v", l.Item.Key, res, err)
		}
		ids[l.Item.Key] = l.DeliveryID
	}
	return ids
}

// leaseGate leases the oldest due delivery of a step of reviewFixture.
func (e *env) leaseGate(step string) store.Lease {
	e.t.Helper()
	return e.leaseOnly("listings", reviewSteps, step)
}

func (e *env) review(id string) store.Review {
	e.t.Helper()
	r, err := e.s.Review(e.ctx, "", id)
	if err != nil {
		e.t.Fatalf("Review %s: %v", id, err)
	}
	return r
}

func (e *env) decide(id, decision, patch, reason string) (store.DeliveryState, error) {
	return e.s.DecideReview(e.ctx, store.ReviewDecisionRequest{Pipeline: "listings", DeliveryID: id,
		Version: e.review(id).Version, Decision: decision, Patch: json.RawMessage(patch), Reviewer: "ana",
		Reason: reason})
}

func TestReviewDecisions(t *testing.T, open Opener) {
	e := newEnv(t, open, reviewFixture)
	ids := e.parkAll(func(string) store.ReviewSpec { return analysts }, "a", "b", "c", "d", "e")
	wantState(t, "run", e.run("r1").State, store.RunWaitingReview)
	if ls := e.lease(10); len(ls) != 0 {
		t.Fatalf("leased %v: a parked review holds no lease", ls)
	}
	all, err := e.s.Reviews(e.ctx, store.ReviewFilter{Inbox: "analysts"})
	if err != nil || len(all) != 5 || all[0].Display != "listing a" || all[0].Version != 1 ||
		string(all[0].Item.Payload) != `{"city":"Lyon","price":100}` || !all[0].Deadline.IsZero() {
		t.Fatalf("reviews = %+v, %v", all, err)
	}
	page, err := e.s.Reviews(e.ctx, store.ReviewFilter{Pipeline: "listings", Step: "gate", After: all[1].Seq, Limit: 2})
	if err != nil || len(page) != 2 || page[0].DeliveryID != all[2].DeliveryID {
		t.Fatalf("page = %+v, %v", page, err)
	}

	state, err := e.decide(ids["a"], store.DecisionApprove, "", "")
	wantState(t, "approve a", state, store.DeliveryDone)
	wantErr(t, err, nil)
	wantState(t, "run", e.run("r1").State, store.RunRunning) // the report delivery a's approval created

	state, err = e.decide(ids["b"], store.DecisionApprove,
		`[{"op":"replace","path":"/price","value":150},{"op":"add","path":"/notes","value":"checked"}]`, "")
	if err != nil || state != store.DeliveryDone {
		t.Fatalf("approve b with an edit = %s, %v", state, err)
	}
	okItems := e.items("listings.ok")
	if len(okItems) != 2 || string(okItems[0].Payload) != `{"city":"Lyon","price":100}` ||
		string(okItems[1].Payload) != `{"city":"Lyon","notes":"checked","price":150}` {
		t.Fatalf("approved items = %+v", okItems)
	}
	if b := e.review(ids["b"]); b.State != store.ReviewDecided || b.Reviewer != "ana" || string(b.Patch) !=
		`[{"op":"replace","path":"/price","value":150},{"op":"add","path":"/notes","value":"checked"}]` {
		t.Fatalf("review b = %+v", b)
	}

	for _, tt := range []struct{ decision, patch, reason, want string }{
		{store.DecisionApprove, `[{"op":"replace","path":"/city","value":"Paris"}]`, "", "/city, which is not editable"},
		{store.DecisionApprove, `[{"op":"replace","path":"/price","value":"cheap"}]`, "", "does not match the output schema"},
		{store.DecisionApprove, `[{"op":"remove","path":"/notes"}]`, "", "/notes does not exist"},
		{store.DecisionApprove, "", "nope", `reason code "nope" is not one of overpriced, stale`},
		{store.DecisionReject, "", "", "a rejection needs a reason code"},
	} {
		if _, err := e.decide(ids["c"], tt.decision, tt.patch, tt.reason); !strings.Contains(fmtErr(err), tt.want) {
			t.Errorf("%s %s %s: %v, want %q", tt.decision, tt.patch, tt.reason, err, tt.want)
		}
	}
	state, err = e.decide(ids["c"], store.DecisionReject, "", "overpriced")
	if err != nil || state != store.DeliveryFiltered {
		t.Fatalf("reject c = %s, %v", state, err)
	}
	for _, d := range e.deliveries("r1") {
		if d.ID == ids["c"] && d.FilterReason != "overpriced" {
			t.Fatalf("rejected delivery = %+v", d)
		}
	}
	_, err = e.decide(ids["a"], store.DecisionReject, "", "stale")
	if !strings.Contains(fmtErr(err), "already decided: approve by ana") {
		t.Fatalf("deciding twice: %v", err)
	}
	wantErr(t, err, store.ErrConflict)
	// The same decision sent again by its reviewer, its answer lost, succeeds again and changes nothing; the same
	// approval with another edit, or by someone else, is a conflict.
	decisions := len(e.eventKinds(store.EventReviewDecided))
	edit := `[{"op":"replace","path":"/price","value":150},{"op":"add","path":"/notes","value":"checked"}]`
	if state, err := e.decide(ids["b"], store.DecisionApprove, edit, ""); err != nil || state != store.DeliveryDone {
		t.Fatalf("approve b again = %s, %v", state, err)
	}
	if state, err := e.decide(ids["c"], store.DecisionReject, "", "overpriced"); err != nil ||
		state != store.DeliveryFiltered {
		t.Fatalf("reject c again = %s, %v", state, err)
	}
	_, err = e.decide(ids["b"], store.DecisionApprove, `[{"op":"replace","path":"/price","value":151}]`, "")
	wantErr(t, err, store.ErrConflict)
	_, err = e.s.DecideReview(e.ctx, store.ReviewDecisionRequest{Pipeline: "listings", DeliveryID: ids["a"],
		Version: e.review(ids["a"]).Version, Decision: store.DecisionApprove, Reviewer: "bob"})
	wantErr(t, err, store.ErrConflict)
	if n := len(e.eventKinds(store.EventReviewDecided)); n != decisions || len(e.items("listings.ok")) != 2 {
		t.Fatalf("a repeated decision recorded again: %d decisions, %d items", n, len(e.items("listings.ok")))
	}

	// Two reviewers decide d at once: one wins, the other conflicts, and d's item is published once.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	version := e.review(ids["d"]).Version
	for i, who := range []string{"ana", "bob"} {
		wg.Go(func() {
			_, errs[i] = e.s.DecideReview(e.ctx, store.ReviewDecisionRequest{Pipeline: "listings", DeliveryID: ids["d"],
				Version: version, Decision: store.DecisionApprove, Reviewer: who})
		})
	}
	wg.Wait()
	if (errs[0] == nil) == (errs[1] == nil) {
		t.Fatalf("concurrent decisions: %v, %v: want one success and one conflict", errs[0], errs[1])
	}
	wantState(t, "approved items", len(e.items("listings.ok")), 3)

	for _, req := range []store.ReviewDecisionRequest{
		{Pipeline: "listings", DeliveryID: ids["e"], Version: 9, Decision: store.DecisionApprove, Reviewer: "ana"},
		{Pipeline: "listings", DeliveryID: "dl_missing", Version: 1, Decision: store.DecisionApprove, Reviewer: "ana"},
	} {
		if _, err := e.s.DecideReview(e.ctx, req); err == nil {
			t.Errorf("decision %+v succeeded", req)
		}
	}
	for _, req := range []store.ReviewDecisionRequest{
		{Decision: "maybe", Reviewer: "ana"}, {Decision: store.DecisionApprove},
		{Decision: store.DecisionApprove, Reviewer: "rule"},
		{Decision: store.DecisionReject, Reviewer: "ana", Patch: json.RawMessage(`[]`)},
		{Decision: store.DecisionApprove, Reviewer: "ana", Patch: json.RawMessage(`{}`)},
	} {
		_, err := e.s.DecideReview(e.ctx, req)
		wantErr(t, err, store.ErrInvalid)
	}

	if err := e.s.Cancel(e.ctx, "listings", "r1"); err != nil {
		t.Fatal(err)
	}
	if r := e.review(ids["e"]); r.State != store.ReviewCancelled {
		t.Fatalf("review of a cancelled run = %s", r.State)
	}
	if _, err := e.decide(ids["e"], store.DecisionApprove, "", ""); !strings.Contains(fmtErr(err), "is cancelled") {
		t.Fatalf("deciding a cancelled review: %v", err)
	}
	if evs := e.eventKinds(store.EventReviewDecided); len(evs) != 4 ||
		!strings.Contains(string(evs[1].Data), `"edited":["/price","/notes"]`) ||
		!strings.Contains(string(evs[1].Data), `"reviewer":"person"`) || strings.Contains(string(evs[1].Data), `"ana"`) {
		t.Fatalf("decision events = %v", evs)
	}
}

func fmtErr(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// Each onTimeout applies once its deadline passes: reject, approve, escalate (then reject when the escalated
// review times out too), and a review without a timeout waits.
func TestReviewTimeouts(t *testing.T, open Opener) {
	e := newEnv(t, open, reviewFixture)
	specs := map[string]store.ReviewSpec{
		"r": {Inbox: "analysts", Timeout: time.Hour, OnTimeout: store.DecisionReject},
		"a": {Inbox: "analysts", Timeout: time.Hour, OnTimeout: store.DecisionApprove},
		"x": {Inbox: "analysts", Timeout: time.Hour, OnTimeout: "escalate", EscalateTo: "leads"},
		"n": {Inbox: "analysts"},
	}
	ids := e.parkAll(func(k string) store.ReviewSpec { return specs[k] }, "r", "a", "x", "n")
	timeouts := func() []store.TimedOutReview {
		t.Helper()
		ts, err := e.s.ReviewTimeouts(e.ctx)
		if err != nil {
			t.Fatal(err)
		}
		return ts
	}
	e.clock.Advance(59 * time.Minute)
	if ts := timeouts(); len(ts) != 0 {
		t.Fatalf("timed out early: %+v", ts)
	}
	e.clock.Advance(2 * time.Minute)
	got := map[string]string{}
	for _, to := range timeouts() {
		got[to.ItemKey] = to.Action + "/" + to.Inbox
	}
	if len(got) != 3 || got["r"] != "reject/analysts" || got["a"] != "approve/analysts" || got["x"] != "escalate/leads" {
		t.Fatalf("timeouts = %v", got)
	}
	if r := e.review(ids["r"]); r.State != store.ReviewTimedOut || r.Reviewer != "timeout" || r.Reason != "timeout" {
		t.Fatalf("review r = %+v", r)
	}
	if ok := e.items("listings.ok"); len(ok) != 1 || ok[0].Key != "a" {
		t.Fatalf("approved on timeout: %+v", ok)
	}
	if a := e.review(ids["a"]); a.Reason != "" || a.Decision != store.DecisionApprove {
		t.Fatalf("an approval on timeout carries no reason: %+v", a)
	}
	_, err := e.decide(ids["r"], store.DecisionApprove, "", "")
	if msg := fmtErr(err); !strings.Contains(msg, "timed out at 2026-10-15T07:01:00Z, applied reject") {
		t.Fatalf("late decision: %v", err)
	}
	x := e.review(ids["x"])
	if x.Spec.Inbox != "leads" || x.Version != 2 || !x.Deadline.Equal(t0.Add(2*time.Hour+time.Minute)) {
		t.Fatalf("escalated review = %+v", x)
	}
	_, err = e.s.DecideReview(e.ctx, store.ReviewDecisionRequest{Pipeline: "listings", DeliveryID: ids["x"],
		Version: 1, Decision: store.DecisionApprove, Reviewer: "ana"})
	if !strings.Contains(fmtErr(err), "moved to inbox leads") {
		t.Fatalf("decision on the version before escalation: %v", err)
	}
	if leads, err := e.s.Reviews(e.ctx, store.ReviewFilter{Inbox: "leads"}); err != nil || len(leads) != 1 {
		t.Fatalf("leads inbox = %+v, %v", leads, err)
	}
	e.clock.Advance(time.Hour)
	if ts := timeouts(); len(ts) != 1 || ts[0].Action != store.DecisionReject {
		t.Fatalf("second timeout of the escalated review = %+v", ts)
	}
	if ts := timeouts(); len(ts) != 0 {
		t.Fatalf("onTimeout applied twice: %+v", ts)
	}
	if n := e.review(ids["n"]); n.State != store.ReviewOpen {
		t.Fatalf("a review without a timeout = %s", n.State)
	}
	wantState(t, "escalations", len(e.eventKinds(store.EventReviewEscalated)), 1)
}

// An auto-approved item passes at once, recorded as approved by rule; sampled, it opens a review sample that never
// holds the run, is decided once, and expires unreviewed after the step's timeout.
func TestAutoApproveAndSamples(t *testing.T, open Opener) {
	e := newEnv(t, open, reviewFixture)
	e.fire("r1")
	e.done(e.leaseGate("crawl"), out("a", `{"price":1}`), out("b", `{"price":2}`), out("c", `{"price":3}`))
	spec := store.ReviewSpec{Inbox: "analysts", Timeout: time.Hour}
	for _, sample := range []bool{true, true, false} {
		l := e.leaseGate("gate")
		res, err := e.s.Park(e.ctx, store.ParkRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Spec: spec,
			AutoApprove: true, Sample: sample})
		if err != nil || res.State != store.DeliveryDone || !res.Deadline.IsZero() {
			t.Fatalf("auto-approve = %+v, %v", res, err)
		}
	}
	e.drain()
	wantState(t, "run", e.run("r1").State, store.RunSucceeded)
	if r, err := e.s.Reviews(e.ctx, store.ReviewFilter{State: store.ReviewDecided}); err != nil || len(r) != 3 ||
		r[0].Reviewer != "rule" || r[0].Decision != store.DecisionApprove {
		t.Fatalf("auto-approved reviews = %+v, %v", r, err)
	}
	samples, err := e.s.ReviewSamples(e.ctx, store.SampleFilter{Inbox: "analysts"})
	if err != nil || len(samples) != 2 || samples[0].Item.Key != "a" || !samples[0].Deadline.Equal(t0.Add(time.Hour)) {
		t.Fatalf("samples = %+v, %v", samples, err)
	}
	a, b := samples[0].DeliveryID, samples[1].DeliveryID
	decide := func(id, decision, reviewer string) error {
		return e.s.DecideSample(e.ctx, store.SampleDecisionRequest{Pipeline: "listings", DeliveryID: id,
			Decision: decision, Reviewer: reviewer, Reason: "fine"})
	}
	wantErr(t, decide(a, store.SampleAgree, "ana"), nil)
	if err := decide(a, store.SampleDisagree, "ana"); !strings.Contains(fmtErr(err), "already decided: agree by ana") {
		t.Fatalf("deciding a sample twice: %v", err)
	}
	wantErr(t, decide(a, "maybe", "ana"), store.ErrInvalid)
	wantErr(t, decide(a, store.SampleAgree, ""), store.ErrInvalid)
	wantErr(t, decide("dl_missing", store.SampleAgree, "ana"), store.ErrNotFound)
	e.clock.Advance(2 * time.Hour)
	if ts, err := e.s.ReviewTimeouts(e.ctx); err != nil || len(ts) != 1 || ts[0].Action != "unreviewed" {
		t.Fatalf("expired samples = %+v, %v", ts, err)
	}
	if err := decide(b, store.SampleAgree, "ana"); !strings.Contains(fmtErr(err), "expired unreviewed") {
		t.Fatalf("deciding an expired sample: %v", err)
	}
	if s, err := e.s.ReviewSample(e.ctx, "listings", a); err != nil || s.State != store.SampleDecided ||
		s.Decision != store.SampleAgree {
		t.Fatalf("decided sample = %+v, %v", s, err)
	}
	if _, err := e.s.ReviewSample(e.ctx, "", "dl_missing"); err == nil {
		t.Fatal("a missing sample was found")
	}
	if _, err := e.s.Park(e.ctx, store.ParkRequest{}); err == nil {
		t.Fatal("parking without an inbox succeeded")
	}
	wantState(t, "run after the samples", e.run("r1").State, store.RunSucceeded)
}

// A review that cannot time out is skipped and reported, its partial work rolled back: the others due time out.
func TestReviewTimeoutSkipsABrokenReview(t *testing.T, open Opener) {
	e := newEnv(t, open, reviewFixture)
	spec := store.ReviewSpec{Inbox: "analysts", Timeout: time.Hour, OnTimeout: store.DecisionApprove}
	ids := e.parkAll(func(string) store.ReviewSpec { return spec }, "a", "b")
	if err := e.s.Exec(e.ctx, `UPDATE reviews SET spec_json = 'not json' WHERE delivery_id = ?`, ids["a"]); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(2 * time.Hour)
	ts, err := e.s.ReviewTimeouts(e.ctx)
	if err != nil || len(ts) != 2 {
		t.Fatalf("timeouts = %+v, %v", ts, err)
	}
	byID := map[string]store.TimedOutReview{ts[0].DeliveryID: ts[0], ts[1].DeliveryID: ts[1]}
	if byID[ids["a"]].Err == nil || byID[ids["b"]].Err != nil || byID[ids["b"]].Action != store.DecisionApprove {
		t.Fatalf("timeouts = %+v", ts)
	}
	if b := e.review(ids["b"]); b.State != store.ReviewTimedOut {
		t.Fatalf("the healthy review = %s", b.State)
	}
}
