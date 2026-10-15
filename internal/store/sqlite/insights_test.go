package sqlite

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/store"
)

func finding(id, kind, severity string) store.Insight {
	return store.Insight{ID: id, Kind: kind, Scope: "score", Severity: severity, WindowStart: t0, WindowEnd: t0,
		Evidence: json.RawMessage(`{"count":1}`)}
}

func (e *env) record(kinds []string, found ...store.Insight) {
	e.t.Helper()
	if err := e.s.RecordInsights(e.ctx, store.InsightReport{Pipeline: "listings", Kinds: kinds, Insights: found}); err != nil {
		e.t.Fatalf("RecordInsights: %v", err)
	}
}

func (e *env) insightStates() map[string]store.InsightState {
	e.t.Helper()
	ins, err := e.s.Insights(e.ctx, store.InsightFilter{Pipeline: "listings"})
	if err != nil {
		e.t.Fatalf("Insights: %v", err)
	}
	out := map[string]store.InsightState{}
	for _, in := range ins {
		out[in.ID] = in.State
	}
	return out
}

// The insight lifecycle: opened when first found, updated by fingerprint, resolved when not found again for a kind
// that was evaluated, reopened when it recurs; an acknowledged insight is still resolved, a muted one never changes;
// a kind not evaluated is left alone.
func TestInsightLifecycle(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	kinds := []string{"failureCluster", "slowStep"}
	e.record(kinds, finding("a", "failureCluster", "warning"), finding("b", "failureCluster", "error"),
		finding("c", "slowStep", "info"))
	wantState(t, "opened", len(e.eventKinds(store.EventInsightOpened)), 3)

	e.clock.Advance(time.Minute)
	updated := finding("a", "failureCluster", "error")
	updated.Evidence = json.RawMessage(`{"count":9}`)
	if err := e.s.SetInsightState(e.ctx, "listings", "b", store.InsightAcknowledged); err != nil {
		t.Fatal(err)
	}
	if err := e.s.SetInsightState(e.ctx, "listings", "c", store.InsightMuted); err != nil {
		t.Fatal(err)
	}
	e.record(kinds, updated) // b and c not found
	states := e.insightStates()
	if states["a"] != store.InsightOpen || states["b"] != store.InsightResolved || states["c"] != store.InsightMuted {
		t.Fatalf("states = %v", states)
	}
	ins, err := e.s.Insights(e.ctx, store.InsightFilter{Pipeline: "listings", State: store.InsightOpen})
	if err != nil || len(ins) != 1 || ins[0].Severity != "error" || string(ins[0].Evidence) != `{"count":9}` ||
		!ins[0].OpenedAt.Equal(t0) || !ins[0].UpdatedAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("updated insight = %+v, %v", ins, err)
	}
	wantState(t, "resolved events", len(e.eventKinds(store.EventInsightResolved)), 1)

	e.record([]string{"slowStep"}) // failureCluster not evaluated: a stays open
	wantState(t, "a when its kind is not evaluated", e.insightStates()["a"], store.InsightOpen)

	e.record(kinds, finding("b", "failureCluster", "error"), finding("a", "failureCluster", "error"))
	wantState(t, "b recurring", e.insightStates()["b"], store.InsightOpen)
	wantState(t, "opened events", len(e.eventKinds(store.EventInsightOpened)), 4)
}

func TestInsightRequests(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	for _, r := range []store.InsightReport{
		{Kinds: []string{"cost"}},
		{Pipeline: "listings", Kinds: []string{"cost"}, Insights: []store.Insight{finding("a", "slowStep", "info")}},
		{Pipeline: "listings", Kinds: []string{"cost"}, Insights: []store.Insight{finding("", "cost", "info")}},
		{Pipeline: "listings", Kinds: []string{"cost"}, Insights: []store.Insight{finding("a", "cost", "info"),
			finding("a", "cost", "info")}},
		{Pipeline: "listings", Kinds: []string{"cost"}, Insights: []store.Insight{{ID: "a", Kind: "cost",
			Evidence: json.RawMessage(`{`)}}},
	} {
		wantErr(t, e.s.RecordInsights(e.ctx, r), store.ErrInvalid)
	}
	wantErr(t, e.s.SetInsightState(e.ctx, "listings", "a", store.InsightResolved), store.ErrInvalid)
	wantErr(t, e.s.SetInsightState(e.ctx, "listings", "missing", store.InsightMuted), store.ErrNotFound)
}

// A crash before an insights pass commits leaves no insight and no event.
func TestRecordInsightsFailpoint(t *testing.T) {
	e := newEnv(t, fixture)
	reset := failpoint.Set("recordInsights.beforeCommit")
	err := e.s.RecordInsights(e.ctx, store.InsightReport{Pipeline: "listings", Kinds: []string{"cost"},
		Insights: []store.Insight{finding("a", "cost", "info")}})
	reset()
	if !errors.As(err, new(failpoint.Crash)) {
		t.Fatalf("RecordInsights = %v, want the failpoint crash", err)
	}
	if len(e.insightStates()) != 0 || len(e.eventKinds(store.EventInsightOpened)) != 0 {
		t.Fatal("an insight survived a crash before commit")
	}
	e.record([]string{"cost"}, finding("a", "cost", "info"))
	reset = failpoint.Set("setInsightState.afterCommit")
	err = e.s.SetInsightState(e.ctx, "listings", "a", store.InsightMuted)
	reset()
	if !errors.As(err, new(failpoint.Crash)) {
		t.Fatalf("SetInsightState = %v, want the failpoint crash", err)
	}
	wantState(t, "muted after a crash after commit", e.insightStates()["a"], store.InsightMuted)
}
