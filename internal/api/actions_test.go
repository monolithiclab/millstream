package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// The actions, one fixture of their own in sequence: each step leaves the state the next one acts on.
func TestActions(t *testing.T) {
	f := newFixture(t)
	h := f.srv
	post := func(target, body string, header map[string]string) *httptest.ResponseRecorder {
		t.Helper()
		return do(t, h, request{method: http.MethodPost, target: target, body: body, header: header})
	}

	// Pause and resume, the pipeline and a step.
	for _, target := range []string{"/pipelines/listings/pause", "/pipelines/listings/steps/score/pause"} {
		if rec := post("/api/v1"+target, "", nil); rec.Code != http.StatusNoContent {
			t.Fatalf("POST %s: %d %s", target, rec.Code, rec.Body.String())
		}
	}
	p := get[Pipeline](t, h, "/api/v1/pipelines/listings")
	if p.State != "paused" || !p.Steps[2].Paused {
		t.Errorf("paused pipeline = %+v", p)
	}
	for _, target := range []string{"/pipelines/listings/resume", "/pipelines/listings/steps/score/resume"} {
		if rec := post("/api/v1"+target, "", nil); rec.Code != http.StatusNoContent {
			t.Fatalf("POST %s: %d", target, rec.Code)
		}
	}
	if p := get[Pipeline](t, h, "/api/v1/pipelines/listings"); p.State != "active" || p.Steps[2].Paused {
		t.Errorf("resumed pipeline = %+v", p)
	}
	wantError(t, post("/api/v1/pipelines/listings/steps/nope/pause", "", nil), 404, codeNotFound)
	wantError(t, post("/api/v1/pipelines/nope/pause", "", nil), 404, codeNotFound)

	// Trigger: the first manual trigger by default, the same request id twice is one run.
	rec := post("/api/v1/pipelines/listings/runs", `{"requestId":"req1","payload":{"city":"Paris"}}`, nil)
	if rec.Code != http.StatusCreated || !strings.Contains(rec.Body.String(), `"trigger":"adhoc"`) {
		t.Fatalf("trigger: %d %s", rec.Code, rec.Body.String())
	}
	if again := post("/api/v1/pipelines/listings/runs", `{"requestId":"req1"}`, nil); again.Code != http.StatusOK {
		t.Errorf("trigger again: %d", again.Code)
	}
	if rec := post("/api/v1/pipelines/listings/runs", `{"trigger":"daily","payload":null}`, nil); rec.Code != 201 {
		t.Errorf("trigger daily by hand: %d %s", rec.Code, rec.Body.String())
	}
	wantError(t, post("/api/v1/pipelines/listings/runs", `{"trigger":"nope"}`, nil), 400, codeInvalid)
	wantError(t, post("/api/v1/pipelines/listings/runs", `{"bogus":1}`, nil), 400, codeInvalid)
	wantError(t, post("/api/v1/pipelines/listings/runs", `{} {}`, nil), 400, codeInvalid)
	wantError(t, post("/api/v1/pipelines/nope/runs", ``, nil), 404, codeNotFound)
	wantError(t, post("/api/v1/pipelines/nightly/runs", ``, nil), 400, codeInvalid) // no manual trigger

	// Cancel: a running run; a settled one is a conflict.
	runID := strings.Split(strings.Split(rec.Body.String(), `"id":"`)[1], `"`)[0]
	if rec := post("/api/v1/pipelines/listings/runs/"+runID+"/cancel", "", nil); rec.Code != http.StatusNoContent {
		t.Fatalf("cancel: %d %s", rec.Code, rec.Body.String())
	}
	wantError(t, post("/api/v1/pipelines/listings/runs/r2/cancel", "", nil), 409, codeConflict)
	wantError(t, post("/api/v1/pipelines/listings/runs/nope/cancel", "", nil), 404, codeNotFound)

	// Replay a step over r1's items.
	rec = post("/api/v1/pipelines/listings/replay", `{"step":"score","run":"r1","since":"7d","requestId":"rp1"}`, nil)
	if rec.Code != http.StatusCreated || !strings.Contains(rec.Body.String(), `"kind":"replay"`) {
		t.Fatalf("replay: %d %s", rec.Code, rec.Body.String())
	}
	wantError(t, post("/api/v1/pipelines/listings/replay", `{}`, nil), 400, codeInvalid)
	wantError(t, post("/api/v1/pipelines/listings/replay", `{"step":"score","since":"soon"}`, nil), 400, codeInvalid)
	wantError(t, post("/api/v1/pipelines/listings/replay", `{"step":"nope"}`, nil), 400, codeInvalid)

	// Review decisions: a stale version is a conflict, a rejection needs a reason code, an approval edits.
	review := "/api/v1/pipelines/listings/reviews/" + f.reviewDelivery + "/decision"
	wantError(t, post(review, `{"decision":"approve","version":7}`, nil), 409, codeConflict)
	wantError(t, post(review, `{"decision":"approve"}`, nil), 400, codeInvalid) // the version seen is required
	wantError(t, post(review, `{"decision":"reject","version":1}`, nil), 400, codeInvalid)
	wantError(t, post(review, `{"decision":"maybe","version":1}`, nil), 400, codeInvalid)
	wantError(t, post(review, `{"decision":"reject","version":1,"patch":[]}`, nil), 400, codeInvalid)
	wantError(t, post(review, `{"decision":"approve","version":1,"patch":{"op":"add"}}`, nil), 400, codeInvalid)
	wantError(t, post(review, `{"decision":"approve","version":1,"patch":[{"op":"add","path":"/price","value":1}]}`,
		nil), 400, codeInvalid)
	defer func(n int64) { maxBody = n }(maxBody)
	maxBody = 64
	wantError(t, post(review, `{"decision":"approve","text":"`+strings.Repeat("x", 64)+`"}`, nil), 413, codeTooLarge)
	maxBody = 1 << 20
	rec = post(review, `{"decision":"approve","version":1,"patch":[{"op":"replace","path":"/score","value":75}]}`,
		map[string]string{UserHeader: "ana"})
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"reviewer":"ana"`) ||
		!strings.Contains(rec.Body.String(), `"state":"decided"`) {
		t.Fatalf("approve: %d %s", rec.Code, rec.Body.String())
	}
	// Sent again by ana, its answer lost, the same approval succeeds again; another decision is a conflict.
	again := post(review, `{"decision":"approve","version":1,"patch":[{"op":"replace","path":"/score","value":75}]}`,
		map[string]string{UserHeader: "ana"})
	if again.Code != http.StatusOK || again.Body.String() != rec.Body.String() {
		t.Fatalf("approve again: %d %s", again.Code, again.Body.String())
	}
	wantError(t, post(review, `{"decision":"approve","version":1}`, nil), 409, codeConflict) // decided already
	wantError(t, post("/api/v1/pipelines/listings/reviews/nope/decision", `{"decision":"approve","version":1}`, nil),
		404, codeNotFound)

	// Sample decisions, decided once.
	sample := "/api/v1/pipelines/listings/samples/" + f.sampleDelivery + "/decision"
	wantError(t, post(sample, `{"decision":"maybe"}`, nil), 400, codeInvalid)
	rec = post(sample, `{"decision":"disagree","reason":"too generous"}`, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"reviewer":"tester"`) {
		t.Fatalf("sample: %d %s", rec.Code, rec.Body.String())
	}
	wantError(t, post(sample, `{"decision":"agree"}`, nil), 409, codeConflict)

	// Insights.
	for _, verb := range []string{"acknowledge", "mute"} {
		if rec := post("/api/v1/pipelines/listings/insights/in_1/"+verb, "", nil); rec.Code != http.StatusNoContent {
			t.Errorf("%s: %d %s", verb, rec.Code, rec.Body.String())
		}
	}
	if ins := get[[]Insight](t, h, "/api/v1/insights?state=muted"); len(ins) != 1 {
		t.Errorf("muted = %+v", ins)
	}
	wantError(t, post("/api/v1/pipelines/listings/insights/nope/mute", "", nil), 404, codeNotFound)

	// Dead letters: a selection names exactly one way, ids must be open ones.
	dl := "/api/v1/pipelines/listings/dead-letters/"
	groups := get[[]DeadLetterGroup](t, h, "/api/v1/pipelines/listings/dead-letters")
	for _, body := range []string{`{}`, `{"all":true,"ids":["x"]}`} {
		wantError(t, post(dl+"replay", body, nil), 400, codeInvalid)
	}
	wantError(t, post(dl+"replay", `{"ids":["nope"]}`, nil), 404, codeNotFound)
	wantError(t, post(dl+"replay", `{"fingerprint":"nope"}`, nil), 404, codeNotFound)
	wantError(t, post(dl+"discard", `{"all":true,"migrate":true}`, nil), 400, codeInvalid)
	wantError(t, post("/api/v1/pipelines/nope/dead-letters/discard", `{"all":true}`, nil), 404, codeNotFound)
	rec = post(dl+"replay", fmt.Sprintf(`{"fingerprint":%q}`, groups[0].Fingerprint), nil)
	if rec.Code != http.StatusOK || rec.Body.String() != `{"count":1}`+"\n" {
		t.Fatalf("replay dead letters: %d %s", rec.Code, rec.Body.String())
	}
	// The replayed delivery fails again, keeping a transcript of its own under the same name: a new dead letter,
	// discarded by id, then nothing is left for all.
	l := f.lease("score")
	second := must(f.arts.Put("listings", "transcript.jsonl", strings.NewReader(`{"role":"assistant"}`+"\n")))
	if l.DeliveryID != f.deadDelivery || l.Attempt != 2 {
		t.Fatalf("replayed lease = %+v: attempt numbers go on after a replay", l)
	}
	must(f.st.Fail(f.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Class: "nonRetryable",
		Message: "price 5 is below the floor", Artifacts: []store.ArtifactRef{second}}))
	groups = get[[]DeadLetterGroup](t, h, "/api/v1/pipelines/listings/dead-letters")
	rec = post(dl+"discard", fmt.Sprintf(`{"ids":[%q]}`, groups[0].DeadLetters[0].ID), nil)
	if rec.Code != http.StatusOK || rec.Body.String() != `{"count":1}`+"\n" {
		t.Fatalf("discard: %d %s", rec.Code, rec.Body.String())
	}
	// A run cancelled with an open dead letter discards it: replaying all never stops on a cancelled run.
	f.fire("cancelled")
	f.done(f.lease("crawl"), out("x", `{"price":1}`), out("y", `{"price":2}`))
	lc := f.lease("score") // y's score still pending: the run is running
	must(f.st.Fail(f.ctx, store.FailRequest{LeaseRef: lc.Ref(), Attempt: lc.Attempt, Class: "nonRetryable",
		Message: "price 1 is below the floor"}))
	if rec := post("/api/v1/pipelines/listings/runs/"+lc.RunID+"/cancel", "", nil); rec.Code != http.StatusNoContent {
		t.Fatalf("cancel: %d %s", rec.Code, rec.Body.String())
	}
	if rec := post(dl+"replay", `{"all":true}`, nil); rec.Body.String() != `{"count":0}`+"\n" {
		t.Errorf("replay all: %s", rec.Body.String())
	}
	// Each attempt keeps its own transcript, listed and served apart.
	d := get[Delivery](t, h, "/api/v1/pipelines/listings/deliveries/"+f.deadDelivery)
	if len(d.Attempts) != 2 || d.Attempts[0].Attempt != 1 || d.Attempts[1].Attempt != 2 ||
		len(d.Attempts[0].Artifacts) != 1 || len(d.Attempts[1].Artifacts) != 1 ||
		d.Attempts[0].Artifacts[0].SHA256 != f.transcript.SHA256 || d.Attempts[1].Artifacts[0].SHA256 != second.SHA256 {
		t.Errorf("attempts after a replay = %+v", d.Attempts)
	}
	for n, want := range map[int]string{1: `{"role":"user"}`, 2: `{"role":"assistant"}`} {
		rec := do(t, h, request{target: fmt.Sprintf("/api/v1/pipelines/listings/deliveries/%s/attempts/%d/artifacts/"+
			"transcript.jsonl", f.deadDelivery, n)})
		if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != want {
			t.Errorf("transcript of attempt %d: %d %q", n, rec.Code, rec.Body.String())
		}
	}
}

// Without an engine, the actions that need one are unavailable; without $USER, a decision names its reviewer.
func TestActionsUnavailable(t *testing.T) {
	f := readFixture(t)
	srv, err := New(Options{Store: f.st})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ target, body string }{
		{"/api/v1/pipelines/listings/runs", `{"trigger":"adhoc"}`},
		{"/api/v1/pipelines/listings/replay", `{"step":"score"}`},
	} {
		wantError(t, do(t, srv, request{method: http.MethodPost, target: tt.target, body: tt.body}), 503,
			codeUnavailable)
	}
	wantError(t, do(t, srv, request{method: http.MethodPost, body: `{"decision":"approve","version":1}`,
		target: "/api/v1/pipelines/listings/reviews/" + f.reviewDelivery + "/decision"}), 400, codeInvalid)
	wantError(t, do(t, srv, request{method: http.MethodPost, body: `{"decision":"agree"}`,
		target: "/api/v1/pipelines/listings/samples/" + f.sampleDelivery + "/decision"}), 400, codeInvalid)
	if _, err := New(Options{}); err == nil {
		t.Error("New without a store succeeded")
	}
}

// decisionError is the code and message of an error answer.
func decisionError(t *testing.T, rec *httptest.ResponseRecorder) errorDetail {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body %q: %v", rec.Body.String(), err)
	}
	return body.Error
}

// countItems counts the run's items on stream.
func countItems(t *testing.T, h http.Handler, run, stream string) int {
	t.Helper()
	return len(get[Page[Item]](t, h, "/api/v1/pipelines/listings/items?run="+run+"&stream="+stream).Items)
}

// Review focus 1: a review times out while a reviewer has it open. The reviewer's decision is a conflict that says
// when it timed out and what onTimeout applied, and onTimeout applied once: one item passed on, no second emit.
func TestReviewDecisionAfterTimeout(t *testing.T) {
	f := newFixture(t)
	h := f.srv
	// A listing parked with a one-hour timeout that approves (the seeded reviews have no timeout).
	f.fire("rt")
	f.done(f.lease("crawl"), out("t", `{"price":150}`))
	f.done(f.lease("score"), out("", `{"price":150,"score":80}`))
	l := f.lease("check")
	spec := reviewSpec
	spec.Timeout, spec.OnTimeout = time.Hour, store.DecisionApprove
	must(f.st.Park(f.ctx, store.ParkRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Spec: spec, Display: "Listing t"}))
	target := "/api/v1/pipelines/listings/reviews/" + l.DeliveryID

	// The reviewer opens it at version 1; its deadline passes and the Review timeout unit runs.
	opened := get[Review](t, h, target)
	if opened.State != "open" || opened.Deadline == nil {
		t.Fatalf("opened review = %+v", opened)
	}
	if n := countItems(t, h, "rt", "listings.approved"); n != 0 {
		t.Fatalf("approved before the timeout: %d", n)
	}
	f.clock.Advance(time.Hour + time.Minute)
	timedOutAt := f.clock.Now()
	ts := must(f.st.ReviewTimeouts(f.ctx))
	if len(ts) != 1 || ts[0].DeliveryID != l.DeliveryID || ts[0].Action != store.DecisionApprove {
		t.Fatalf("timeouts = %+v", ts)
	}

	// The reviewer's decision, on the version they saw, is a conflict naming the timeout.
	rec := do(t, h, request{method: http.MethodPost, target: target + "/decision",
		body:   fmt.Sprintf(`{"decision":"reject","reason":"overpriced","version":%d}`, opened.Version),
		header: map[string]string{UserHeader: "ana"}})
	wantError(t, rec, http.StatusConflict, codeConflict)
	want := fmt.Sprintf("review of delivery %s timed out at %s, applied approve", l.DeliveryID,
		timedOutAt.Format(time.RFC3339))
	if msg := decisionError(t, rec).Message; !strings.Contains(msg, want) {
		t.Errorf("message %q, want %q", msg, want)
	}

	// onTimeout applied once: the review records it, one item passed on, and the unit finds nothing more.
	rv := get[Review](t, h, target)
	if rv.State != "timedOut" || rv.Decision != store.DecisionApprove || rv.Reviewer != "timeout" {
		t.Errorf("timed-out review = %+v", rv)
	}
	if n := countItems(t, h, "rt", "listings.approved"); n != 1 {
		t.Errorf("approved after the timeout and the late decision: %d items, want 1", n)
	}
	if ts := must(f.st.ReviewTimeouts(f.ctx)); len(ts) != 0 {
		t.Errorf("timeouts again = %+v", ts)
	}
}

// Review focus 3: two reviewers approve the same review at once. One wins; the other's answer is a conflict naming
// the winner, and the item is passed on once.
func TestReviewDecisionConcurrent(t *testing.T) {
	f := newFixture(t)
	h := f.srv
	target := "/api/v1/pipelines/listings/reviews/" + f.reviewDelivery
	before := countItems(t, h, "r1", "listings.approved")

	reviewers := []string{"ana", "ben"}
	recs := make([]*httptest.ResponseRecorder, len(reviewers))
	var wg sync.WaitGroup
	for i, who := range reviewers {
		wg.Go(func() {
			recs[i] = do(t, h, request{method: http.MethodPost, target: target + "/decision",
				body: `{"decision":"approve","version":1}`, header: map[string]string{UserHeader: who}})
		})
	}
	wg.Wait()

	winner, loser := -1, -1
	for i, rec := range recs {
		switch rec.Code {
		case http.StatusOK:
			winner = i
		case http.StatusConflict:
			loser = i
		}
	}
	if winner < 0 || loser < 0 {
		t.Fatalf("answers %d %s / %d %s, want one 200 and one 409", recs[0].Code, recs[0].Body.String(),
			recs[1].Code, recs[1].Body.String())
	}
	wantError(t, recs[loser], http.StatusConflict, codeConflict)
	want := "already decided: approve by " + reviewers[winner]
	if msg := decisionError(t, recs[loser]).Message; !strings.Contains(msg, want) {
		t.Errorf("loser's message %q, want %q", msg, want)
	}
	if rv := get[Review](t, h, target); rv.State != "decided" || rv.Reviewer != reviewers[winner] {
		t.Errorf("decided review = %+v", rv)
	}
	if n := countItems(t, h, "r1", "listings.approved"); n != before+1 {
		t.Errorf("approved items %d, want %d: one emit", n, before+1)
	}
}
