package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// TestPairsOf: an agent step's pairs from history: the truth is its output, corrected by a person's edit at a review
// downstream where the review received the output's value unchanged; a rejected output, a gone input and an
// unfinished delivery make no pair; each pair carries its lineage root and the reviews behind its truth.
func TestPairsOf(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	item := func(id, step, key string, min int, payload string, parents ...string) store.Item {
		return store.Item{ID: id, RunID: "r", Step: step, Key: key, Stream: step + ".out", Parents: parents,
			TS: t0.Add(time.Duration(min) * time.Minute), Payload: json.RawMessage(payload)}
	}
	trigger := store.Item{ID: "t", RunID: "r", Stream: "trigger.adhoc", Payload: json.RawMessage(`{}`)}
	items := []store.Item{trigger,
		item("i1", "crawl", "A", 1, `{"id":"A"}`, "t"), item("o1", "score", "A", 2, `{"id":"A","score":50}`, "i1"),
		item("i2", "crawl", "B", 3, `{"id":"B"}`, "t"), item("o2", "score", "B", 4, `{"id":"B","score":90}`, "i2"),
		item("i3", "crawl", "C", 5, `{"id":"C"}`, "t"),
		item("i4", "crawl", "D", 6, `{"id":"D"}`, "t"), item("o4", "score", "D", 7, `{"id":"D","score":60}`, "i4"),
		item("x4", "enrich", "D", 8, `{"id":"D","score":60,"extra":1}`, "o4"),
		item("i5", "crawl", "E", 9, `{"id":"E"}`, "t"),
	}
	items[5].Tombstone = store.Tombstone("expired")
	ds := []store.Delivery{
		{ID: "d1", Step: "score", ItemID: "i1", RunID: "r", State: store.DeliveryDone, CostUSD: 0.01},
		{ID: "d2", Step: "score", ItemID: "i2", RunID: "r", State: store.DeliveryDone},
		{ID: "d3", Step: "score", ItemID: "i3", RunID: "r", State: store.DeliveryDone},
		{ID: "d4", Step: "score", ItemID: "i4", RunID: "r", State: store.DeliveryDone},
		{ID: "d5", Step: "score", ItemID: "i5", RunID: "r", State: store.DeliveryDead},
	}
	person := func(delivery, itemID, decision, patch string) store.Review {
		return store.Review{DeliveryID: delivery, Reviewer: review.ReviewerPerson, Decision: decision,
			Patch: json.RawMessage(patch), Item: store.Item{ID: itemID}}
	}
	reviews := []store.Review{
		person("dr1", "o1", store.DecisionApprove, `[{"op":"replace","path":"/score","value":30}]`),
		person("dr2", "o2", store.DecisionReject, `[]`),
		// Downstream, after enrich: /score came from score unchanged, /extra did not.
		person("dr4", "x4", store.DecisionApprove,
			`[{"op":"replace","path":"/score","value":40},{"op":"replace","path":"/extra","value":2}]`),
		{DeliveryID: "dr4r", Reviewer: "rule", Decision: store.DecisionReject, Item: store.Item{ID: "o4"}},
	}
	h := newHistory(SourceLocal)
	h.add(items, ds, reviews)
	ps := pairsOf(h, "score")
	if len(ps) != 2 || ps[0].Item != "i1" || ps[1].Item != "i4" {
		t.Fatalf("pairs %+v", ps)
	}
	if p := ps[0]; string(p.Outputs[0].Payload) != `{"id":"A","score":30}` || !p.Edited || p.Root != "t" ||
		!slices.Equal(p.Reviews, []string{"dr1"}) || p.CostUSD != 0.01 || p.Key != "A" || !p.At.Equal(t0.Add(time.Minute)) {
		t.Errorf("edited pair %+v (%s)", p, p.Outputs[0].Payload)
	}
	if p := ps[1]; string(p.Outputs[0].Payload) != `{"id":"D","score":40}` || !slices.Equal(p.Reviews, []string{"dr4"}) {
		t.Errorf("downstream edit %+v (%s)", p, p.Outputs[0].Payload)
	}
}

// agentScore resolves as the fixture does, but reports fn as an agent: the seeded history then has an agent step.
type agentScore struct{}

func (agentScore) Resolve(pkg *pipeline.Package, ref string) (*pipeline.Action, error) {
	a, err := pipeline.Resolver{}.Resolve(pkg, ref)
	if err == nil && ref == "./actions/fn" {
		c := *a
		c.Kind = pipeline.KindAgent
		a = &c
	}
	return a, err
}

// TestTraining: the training set of an agent step, refused for a step that is not an agent's, an unknown step, a
// production deployment, and a history whose entities were all first seen at once (the seeded days list the same
// keys); the holdout reads the pairs of the entities first seen since a boundary.
func TestTraining(t *testing.T) {
	ctx := context.Background()
	b := New(Options{Store: seeded, Launchers: launchers(), Resolver: agentScore{}})
	for name, c := range map[string]struct {
		b    *Backtester
		step string
		want error
	}{
		"not an agent": {New(Options{Store: seeded}), "score", store.ErrInvalid},
		"unknown step": {b, "nope", store.ErrNotFound},
		"production":   {New(Options{Store: seeded, Mode: pipeline.ModeProduction}), "score", ErrProduction},
		"no split":     {b, "score", distill.ErrNoSplit},
	} {
		if _, err := c.b.Training(ctx, "bt", c.step); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", name, err, c.want)
		}
	}
	all, err := b.Holdout(ctx, "bt", "score", time.Time{})
	if err != nil || len(all) != 6 {
		t.Fatalf("holdout from the start: %d pairs, %v", len(all), err)
	}
	if none, err := b.Holdout(ctx, "bt", "score", time.Now().Add(time.Hour)); err != nil || len(none) != 0 {
		t.Errorf("holdout from the future: %d pairs, %v", len(none), err)
	}
}

// TestStepLessons: a step's lessons reach the drafting agent without the examples derived from the holdout.
func TestStepLessons(t *testing.T) {
	files := fixtureFiles()
	files["lessons/score.yaml"] = []byte("rules:\n  - text: Keep it.\nexamples:\n" +
		"  - { input: { id: L1 }, output: { score: 1 }, source: review/dl_r1 }\n" +
		"  - { input: { id: L2 }, output: { score: 2 } }\n")
	pkg, err := pipeline.FromFiles("", files)
	if err != nil {
		t.Fatal(err)
	}
	holdout := []distill.Pair{{Item: "it_9", Reviews: []string{"dl_r1"}, Input: json.RawMessage(`{"id":"L9"}`)}}
	l, n, err := stepLessons(pkg, "score", holdout)
	if err != nil || n != 1 || len(l.Rules) != 1 || len(l.Examples) != 1 || l.Examples[0].Output == nil {
		t.Fatalf("lessons %+v, withheld %d, %v", l, n, err)
	}
	if l, n, err := stepLessons(pkg, "keep", holdout); err != nil || n != 0 || !l.Empty() {
		t.Errorf("no lessons file: %+v %d %v", l, n, err)
	}
	files["lessons/score.yaml"] = []byte("rules: [1]\n")
	if pkg, err = pipeline.FromFiles("", files); err == nil {
		if _, _, err := stepLessons(pkg, "score", nil); err == nil {
			t.Error("a broken lessons file was read")
		}
	}
}
