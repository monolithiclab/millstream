package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	wantError(t, post(review, `{"decision":"reject"}`, nil), 400, codeInvalid)
	wantError(t, post(review, `{"decision":"maybe"}`, nil), 400, codeInvalid)
	wantError(t, post(review, `{"decision":"reject","patch":[]}`, nil), 400, codeInvalid)
	wantError(t, post(review, `{"decision":"approve","patch":{"op":"add"}}`, nil), 400, codeInvalid)
	wantError(t, post(review, `{"decision":"approve","patch":[{"op":"add","path":"/price","value":1}]}`, nil), 400,
		codeInvalid)
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
	wantError(t, post(review, `{"decision":"approve"}`, nil), 409, codeConflict) // decided already
	wantError(t, post("/api/v1/pipelines/listings/reviews/nope/decision", `{"decision":"approve"}`, nil), 404,
		codeNotFound)

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
	// The replayed delivery fails again: a new dead letter, discarded by id, then nothing is left for all.
	l := f.lease("score")
	must(f.st.Fail(f.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Class: "nonRetryable",
		Message: "price 5 is below the floor"}))
	groups = get[[]DeadLetterGroup](t, h, "/api/v1/pipelines/listings/dead-letters")
	rec = post(dl+"discard", fmt.Sprintf(`{"ids":[%q]}`, groups[0].DeadLetters[0].ID), nil)
	if rec.Code != http.StatusOK || rec.Body.String() != `{"count":1}`+"\n" {
		t.Fatalf("discard: %d %s", rec.Code, rec.Body.String())
	}
	if rec := post(dl+"replay", `{"all":true}`, nil); rec.Body.String() != `{"count":0}`+"\n" {
		t.Errorf("replay all: %s", rec.Body.String())
	}
	if d := get[Delivery](t, h, "/api/v1/pipelines/listings/deliveries/"+f.deadDelivery); len(d.Attempts) != 2 {
		t.Errorf("attempts after a replay = %+v", d.Attempts)
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
	wantError(t, do(t, srv, request{method: http.MethodPost, body: `{"decision":"approve"}`,
		target: "/api/v1/pipelines/listings/reviews/" + f.reviewDelivery + "/decision"}), 400, codeInvalid)
	wantError(t, do(t, srv, request{method: http.MethodPost, body: `{"decision":"agree"}`,
		target: "/api/v1/pipelines/listings/samples/" + f.sampleDelivery + "/decision"}), 400, codeInvalid)
	if _, err := New(Options{}); err == nil {
		t.Error("New without a store succeeded")
	}
}
