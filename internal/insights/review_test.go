package insights

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// eventStore serves PipelineEvents from a list, for readReviews.
type eventStore struct {
	store.Store
	evs []store.Event
}

func (s eventStore) PipelineEvents(context.Context, store.EventFilter) ([]store.Event, error) {
	return s.evs, nil
}

// Golden reviewPattern over a day of decisions of one review step: people approve (some with edits), reject with
// reason codes or let reviews time out, the rule auto-approves, and the sampled-rule agreement falls under 90%, a
// warning. The scorer's cost insight gains its cost per approved item.
func TestGoldenReviewPattern(t *testing.T) {
	t.Parallel()
	at := func(h int) time.Time { return now.Add(-time.Duration(h) * time.Hour) }
	ev := func(h int, kind, data string) store.Event {
		return store.Event{TS: at(h), Kind: kind, Data: json.RawMessage(data)}
	}
	decided := func(h int, delivery, decision, reviewer, reason, edited string, ms int) store.Event {
		data := map[string]any{"deliveryId": delivery, "step": "gate", "decision": decision, "reviewer": reviewer,
			"timeToDecisionMs": ms}
		if reason != "" {
			data["reason"] = reason
		}
		if edited != "" {
			data["edited"] = []string{edited, "/notes"}
		}
		b, _ := json.Marshal(data)
		return store.Event{TS: at(h), Kind: store.EventReviewDecided, Data: b}
	}
	evs := []store.Event{
		decided(30, "d0", "approve", "person", "", "", 1000), // before the window
		decided(5, "d1", "approve", "person", "", "", 60000),
		decided(5, "d2", "approve", "person", "", "/price", 120000),
		decided(4, "d3", "approve", "person", "", "/price", 600000),
		decided(4, "d4", "reject", "person", "overpriced", "", 30000),
		decided(3, "d5", "reject", "person", "", "", 45000),
		decided(3, "d6", "reject", "timeout", "timeout", "", 3600000),
		decided(2, "d7", "approve", "rule", "", "", 0),
		decided(2, "d8", "approve", "rule", "", "", 0),
		ev(2, store.EventSampleDecided, `{"step":"gate","decision":"agree"}`),
		ev(1, store.EventSampleDecided, `{"step":"gate","decision":"disagree"}`),
		ev(1, store.EventSampleExpired, `{"step":"gate"}`),
	}
	d := &Data{Pipeline: "shop", Now: now}
	var err error
	if d.Decisions, d.Samples, err = readReviews(t.Context(), eventStore{evs: evs}, "shop", time.Time{}); err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		id := "d" + string(rune('0'+i))
		d.Deliveries = append(d.Deliveries, store.Delivery{ID: id, Step: "gate", ItemID: "it_" + id, CreatedAt: at(6)})
	}
	d.Attempts = []Attempt{{TS: at(6), Step: "score", Delivery: "s1", Outcome: store.EventDeliveryDone, Outputs: 8,
		CostUSD: 0.4, Metrics: []store.Metric{{Name: "turns", Value: 2, Labels: map[string]string{"model": "m"}}}}}
	var out []goldenInsight
	for _, in := range Compute(d, []string{pipeline.InsightReviewPattern, pipeline.InsightCost}) {
		out = append(out, goldenInsight{ID: in.ID, Kind: in.Kind, Scope: in.Scope, Severity: in.Severity,
			Window: [2]time.Time{in.WindowStart, in.WindowEnd}, Summary: Summary(in), Evidence: in.Evidence})
	}
	body, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "review_pattern.json", append(body, '\n'))
	if _, _, err := readReviews(t.Context(), eventStore{evs: []store.Event{ev(1, store.EventReviewDecided, `[`)}},
		"shop", time.Time{}); err == nil {
		t.Fatal("a malformed event was read")
	}
}
