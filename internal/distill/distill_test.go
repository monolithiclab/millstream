package distill

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

var t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// pair is a pair of item it, key and root at hour h.
func pair(it, key, root string, h int) Pair {
	return Pair{Item: it, Key: key, Root: root, At: t0.Add(time.Duration(h) * time.Hour),
		Input: json.RawMessage(fmt.Sprintf(`{"id":%q,"n":%d}`, key, h))}
}

func items(ps []Pair) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Item
	}
	slices.Sort(out)
	return out
}

// TestSplitByTime: the most recent entities make up the holdout, the boundary is the oldest of them, and the holdout
// recomputed from the boundary is the split's, with the entities first seen since.
func TestSplitByTime(t *testing.T) {
	var ps []Pair
	for h := range 10 {
		ps = append(ps, pair(fmt.Sprintf("i%d", h), fmt.Sprintf("k%d", h), fmt.Sprintf("r%d", h), h))
	}
	s, err := SplitPairs(ps, HoldoutShare)
	if err != nil {
		t.Fatal(err)
	}
	if got := items(s.Holdout); !slices.Equal(got, []string{"i5", "i6", "i7", "i8", "i9"}) ||
		!s.HoldoutFrom.Equal(t0.Add(5*time.Hour)) || len(s.Train) != 5 {
		t.Fatalf("holdout %v from %s, train %d", got, s.HoldoutFrom, len(s.Train))
	}
	later := append(slices.Clone(ps), pair("i10", "k10", "r10", 10))
	if got := items(Holdout(later, s.HoldoutFrom)); !slices.Equal(got, []string{"i10", "i5", "i6", "i7", "i8", "i9"}) {
		t.Fatalf("recomputed holdout %v", got)
	}
}

// TestSplitByEntity: a key seen in training stays in training when it comes back later (review focus 2), so the
// holdout never holds an entity the drafting agent saw; the same holds for a pair recorded after the split.
func TestSplitByEntity(t *testing.T) {
	ps := []Pair{pair("a1", "A", "ra1", 0), pair("b", "B", "rb", 1), pair("c", "C", "rc", 2), pair("d", "D", "rd", 3),
		pair("a2", "A", "ra2", 4)}
	s, err := SplitPairs(ps, 0.4)
	if err != nil {
		t.Fatal(err)
	}
	if got := items(s.Train); !slices.Equal(got, []string{"a1", "a2", "b"}) {
		t.Fatalf("train %v, holdout %v", got, items(s.Holdout))
	}
	later := append(slices.Clone(ps), pair("b2", "B", "rb2", 9))
	for _, p := range Holdout(later, s.HoldoutFrom) {
		if p.Key == "A" || p.Key == "B" {
			t.Errorf("the training entity %s reached the holdout through %s", p.Key, p.Item)
		}
	}
}

// TestSplitRetryAndReplay: the retry of a holdout item (the same item again) and a dead-letter replay of it (another
// item of its lineage under another key) stay in the holdout with it (review focus 2).
func TestSplitRetryAndReplay(t *testing.T) {
	ps := []Pair{pair("x", "X", "rx", 0), pair("y", "Y", "ry", 1),
		pair("h", "H", "rh", 5),
		pair("x2", "X2", "rx2", 2),
		pair("h", "H", "rh", 6),         // a retry of h
		pair("h-replay", "H'", "rh", 7), // a replay of h's lineage, keyed differently
	}
	s, err := SplitPairs(ps, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	if got := items(s.Holdout); !slices.Equal(got, []string{"h", "h", "h-replay"}) {
		t.Fatalf("holdout %v", got)
	}
	// An older item of h's lineage makes the whole entity as old as it: it trains, replay and all.
	ps = append(ps, pair("old", "OLD", "rh", 0), pair("z1", "Z1", "", 3), pair("z2", "Z2", "", 4),
		pair("z3", "Z3", "", 8), pair("z4", "Z4", "", 9))
	if s, err = SplitPairs(ps, 0.4); err != nil {
		t.Fatal(err)
	}
	if got := items(s.Holdout); !slices.Equal(got, []string{"x2", "z1", "z2", "z3", "z4"}) {
		t.Fatalf("holdout %v", got)
	}
}

// TestSplitRefused: pairs all first seen at one instant, or a single entity, have nothing to split.
func TestSplitRefused(t *testing.T) {
	same := []Pair{pair("a", "A", "", 1), pair("b", "B", "", 1), pair("c", "C", "", 1)}
	one := []Pair{pair("a", "A", "r", 1), pair("b", "A", "r", 2)}
	for name, ps := range map[string][]Pair{"same instant": same, "one entity": one, "none": nil} {
		if _, err := SplitPairs(ps, HoldoutShare); !errors.Is(err, ErrNoSplit) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestWithheld: lesson examples derived from a holdout pair (its review, its item, or an excerpt of its input) are
// withheld from the drafting agent; others are kept (review focus 2).
func TestWithheld(t *testing.T) {
	holdout := []Pair{{Item: "it_h", Key: "H", Reviews: []string{"dl_review_h"},
		Input: json.RawMessage(`{"id":"H","city":"Lyon","price":1000,"rooms":2}`)}}
	examples := []pipeline.LessonExample{
		{Input: map[string]any{"id": "T"}, Source: "review/dl_review_t"},             // training: kept
		{Input: map[string]any{"city": "Paris"}, Source: "insight/in_1"},             // kept
		{Input: map[string]any{"x": 1}, Source: "review/dl_review_h"},                // the holdout's review
		{Input: map[string]any{"x": 2}, Source: "item/it_h"},                         // the holdout's item
		{Input: map[string]any{"city": "Lyon", "price": 1000}, Source: "proposal/p"}, // an excerpt of its input
		{Input: map[string]any{"city": "Lyon", "price": 999}},                        // not an excerpt
		{Input: map[string]any{}},                                                    // an empty excerpt matches nothing
	}
	if got := Withheld(examples, holdout); !slices.Equal(got, []int{2, 3, 4}) {
		t.Fatalf("withheld %v", got)
	}
}

// TestAgree: outputs agree by key (the input's when unkeyed) and canonical payload, in any order.
func TestAgree(t *testing.T) {
	truth := []Output{{Key: "A", Payload: json.RawMessage(`{"a":1,"b":[1,2]}`)}, {Key: "B", Payload: json.RawMessage(`2`)}}
	cases := map[string]struct {
		got  []Output
		want bool
	}{
		"reordered": {[]Output{{Key: "B", Payload: json.RawMessage(`2`)}, {Key: "A", Payload: json.RawMessage(`{"b":[1,2], "a":1}`)}}, true},
		"unkeyed":   {[]Output{{Payload: json.RawMessage(`{"b":[1,2],"a":1}`)}, {Key: "B", Payload: json.RawMessage(`2`)}}, true},
		"differs":   {[]Output{{Key: "A", Payload: json.RawMessage(`{"a":2,"b":[1,2]}`)}, {Key: "B", Payload: json.RawMessage(`2`)}}, false},
		"missing":   {[]Output{{Key: "A", Payload: json.RawMessage(`{"a":1,"b":[1,2]}`)}}, false},
	}
	for name, c := range cases {
		if got := Agree(truth, c.got, "A"); got != c.want {
			t.Errorf("%s: %v", name, got)
		}
	}
}

// results returns n results: covered ones agreeing but for wrong, and abstained ones, each item costing a cent.
func results(covered, wrong, abstained int) []Result {
	var out []Result
	for i := range covered {
		out = append(out, Result{Item: fmt.Sprintf("c%d", i), Agreed: i >= wrong, CostUSD: 0.01})
	}
	for i := range abstained {
		out = append(out, Result{Item: fmt.Sprintf("a%d", i), Abstained: true, CostUSD: 0.01})
	}
	return out
}

// TestEvaluate: the thresholds of spec section 11.5, at and around their limits.
func TestEvaluate(t *testing.T) {
	cases := map[string]struct {
		results  []Result
		failures []string
	}{
		"passes":            {results(250, 5, 50), nil},                  // 0.98 agreement, 0.83 coverage
		"agreement":         {results(250, 6, 0), []string{"agreement"}}, // 0.976
		"coverage":          {results(300, 0, 201), []string{"coverage"}},
		"too few covered":   {results(199, 0, 0), []string{"covered"}},
		"nothing covered":   {results(0, 0, 10), []string{"covered", "coverage", "agreement"}},
		"exactly the floor": {results(200, 4, 133), nil},
	}
	for name, c := range cases {
		v := Evaluate(c.results, Thresholds{})
		if v.Passed != (len(c.failures) == 0) || !slices.Equal(v.Failures, append([]string{}, c.failures...)) {
			t.Errorf("%s: %+v", name, v)
		}
	}
	v := Evaluate(results(250, 5, 50), Thresholds{MinAgreement: 0.99})
	if v.Passed || v.Covered != 250 || v.Agreed != 245 || len(v.Disagreements) != 5 ||
		fmt.Sprintf("%.2f %.2f", v.AgentCostSavedUSD, v.AgentCostUSD) != "2.50 3.00" || v.Thresholds.MinCovered != 200 {
		t.Errorf("custom thresholds: %+v", v)
	}
}

// TestSpec: a distilled action's spec file round-trips, and is strict.
func TestSpec(t *testing.T) {
	want := Spec{Step: "score", Agent: "./actions/score", HoldoutFrom: t0.Add(90 * time.Minute)}
	got, err := ParseSpec(want.Format())
	if err != nil || got.Step != want.Step || got.Agent != want.Agent || !got.HoldoutFrom.Equal(want.HoldoutFrom) {
		t.Fatalf("%+v, %v", got, err)
	}
	for _, bad := range []string{"step: s\nagent: a\n", "step: s\nagent: a\nholdoutFrom: 2026-10-01T00:00:00Z\nx: 1\n"} {
		if _, err := ParseSpec([]byte(bad)); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}
