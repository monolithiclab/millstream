package insights

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// history builds a Data by hand: items, deliveries, attempts and decisions of the window, no store.
type history struct{ *Data }

func newHistory(name string, steps map[string]Step) history {
	return history{&Data{Pipeline: name, Now: now, Items: map[string]store.Item{},
		Config: Config{DistillMinDeliveries: 10, Steps: steps}}}
}

func minutesAgo(m int) time.Time { return now.Add(-time.Duration(m) * time.Minute) }

func (h history) item(id, step, payload string, parents ...string) {
	h.Items[id] = store.Item{ID: id, Pipeline: h.Pipeline, Step: step, Parents: parents,
		Payload: json.RawMessage(payload)}
}

// deliver records a delivery of step for input item, and for a done one with a cost its attempt with turns.
func (h history) deliver(id, step, input string, state store.DeliveryState, cost, turns float64) {
	h.Deliveries = append(h.Deliveries, store.Delivery{ID: id, Pipeline: h.Pipeline, Step: step, ItemID: input,
		State: state, CostUSD: cost, CreatedAt: minutesAgo(60), FinishedAt: minutesAgo(59)})
	if state == store.DeliveryDone && cost > 0 {
		h.Attempts = append(h.Attempts, Attempt{TS: minutesAgo(59), Step: step, Delivery: id,
			Outcome: store.EventDeliveryDone, Outputs: 1, CostUSD: cost,
			Metrics: []store.Metric{{Name: agent.MetricTurns, Value: turns}}})
	}
}

func (h history) decide(step, delivery, item, decision, reason string, text bool, edited ...string) {
	h.Decisions = append(h.Decisions, Decision{TS: minutesAgo(30), Step: step, Delivery: delivery, Item: item,
		Decision: decision, Reviewer: "person", Reason: reason, Edited: edited, Text: text,
		TimeToDecision: time.Minute})
}

const scoreSchema = `{"type":"object","properties":{"city":{"type":"string"},"price":{"type":"number"},
"rooms":{"type":"integer","minimum":1,"maximum":20},"score":{"type":"integer","minimum":0,"maximum":5},
"label":{"$ref":"#/$defs/label"}},"$defs":{"label":{"enum":["ok","meh"]}}}`

// estate: crawl (code) feeds score (an agent), whose low scores the keep filter drops before the gate review.
// Six of twenty listings are dropped by the filter, the five in Lyon rejected (three with a text and no reason
// code), four approvals edit the score. score copies city and price from its input.
func estate() history {
	h := newHistory("estate", map[string]Step{"crawl": {}, "score": {Agent: true, Output: []byte(scoreSchema)},
		"keep": {Builtin: pipeline.BuiltinFilter}, "gate": {Builtin: pipeline.BuiltinReview}})
	for i := range 20 {
		n := strconv.Itoa(i)
		city, score := "Lille", 4
		if i >= 6 && i <= 10 {
			city = "Lyon"
		}
		if i < 6 {
			score = 1
		}
		listing := fmt.Sprintf(`"city":%q,"price":%d,"rooms":3`, city, 100000+i*10000)
		scored := fmt.Sprintf(`{%s,"score":%d,"label":"ok"}`, listing, score)
		h.item("t"+n, "trigger.adhoc", fmt.Sprintf(`{"id":%d}`, i))
		h.item("c"+n, "crawl", "{"+listing+"}", "t"+n)
		h.item("s"+n, "score", scored, "c"+n)
		h.deliver("dc"+n, "crawl", "t"+n, store.DeliveryDone, 0, 0)
		h.deliver("ds"+n, "score", "c"+n, store.DeliveryDone, 0.01, 1)
		if i < 6 {
			h.deliver("dk"+n, "keep", "s"+n, store.DeliveryFiltered, 0, 0)
			continue
		}
		h.deliver("dk"+n, "keep", "s"+n, store.DeliveryDone, 0, 0)
		h.item("k"+n, "keep", scored, "s"+n)
		switch {
		case i <= 7:
			h.deliver("dg"+n, "gate", "k"+n, store.DeliveryFiltered, 0, 0)
			h.decide("gate", "dg"+n, "k"+n, store.DecisionReject, "location", false)
		case i <= 10:
			h.deliver("dg"+n, "gate", "k"+n, store.DeliveryFiltered, 0, 0)
			h.decide("gate", "dg"+n, "k"+n, store.DecisionReject, "", true)
		case i <= 14:
			h.deliver("dg"+n, "gate", "k"+n, store.DeliveryDone, 0, 0)
			h.decide("gate", "dg"+n, "k"+n, store.DecisionApprove, "", false, "/score")
		default:
			h.deliver("dg"+n, "gate", "k"+n, store.DeliveryDone, 0, 0)
			h.decide("gate", "dg"+n, "k"+n, store.DecisionApprove, "", false)
		}
	}
	return h
}

// tidy: four agent steps. tag (bounded, its id copied from the input) is approved without edits twenty times; rate
// (bounded, unreviewed) costs the most; classify writes free prose; lookup uses a tool.
func tidy() history {
	enum := `{"type":"object","properties":{"grade":{"type":"string","enum":["a","b"]}}}`
	h := newHistory("tidy", map[string]Step{
		"tag":      {Agent: true, Output: []byte(`{"type":"object","properties":{"id":{"type":"integer"},"kind":{"enum":["flat","house"]}}}`)},
		"rate":     {Agent: true, Output: []byte(enum)},
		"classify": {Agent: true, Output: []byte(`{"type":"object","properties":{"summary":{"type":"string"},"kind":{"const":"x"}}}`)},
		"lookup":   {Agent: true, Tools: 1, Output: []byte(enum)},
		"gate":     {Builtin: pipeline.BuiltinReview},
	})
	for i := range 20 {
		n := strconv.Itoa(i)
		h.item("t"+n, "trigger.adhoc", fmt.Sprintf(`{"id":%d}`, i))
		h.item("g"+n, "tag", fmt.Sprintf(`{"id":%d,"kind":"flat"}`, i), "t"+n)
		h.deliver("dt"+n, "tag", "t"+n, store.DeliveryDone, 0.002, 1)
		h.deliver("dg"+n, "gate", "g"+n, store.DeliveryDone, 0, 0)
		h.decide("gate", "dg"+n, "g"+n, store.DecisionApprove, "", false)
		if i < 12 {
			h.deliver("dr"+n, "rate", "x"+n, store.DeliveryDone, 0.05, 1)
			h.deliver("dx"+n, "classify", "x"+n, store.DeliveryDone, 0.01, 2)
			h.deliver("dl"+n, "lookup", "x"+n, store.DeliveryDone, 0.01, 1)
		}
	}
	return h
}

// Golden waste, distillable and reviewPattern over the two histories: estate's score paid for eleven discarded
// listings (the filter dropping the most: move it upstream), its gate's decisions give a filter candidate on the
// city and the price, recurring score edits and uncovered reasons; tidy's rate and tag are distillable, ranked by
// saving, and its gate a near-universal approval.
func TestGoldenLearning(t *testing.T) {
	var out []goldenInsight
	for _, h := range []history{estate(), tidy()} {
		for _, in := range Compute(h.Data, []string{pipeline.InsightWaste, pipeline.InsightDistillable,
			pipeline.InsightReviewPattern}) {
			out = append(out, goldenInsight{ID: in.ID, Kind: in.Kind, Scope: in.Scope, Severity: in.Severity,
				Window: [2]time.Time{in.WindowStart, in.WindowEnd}, Summary: Summary(in), Evidence: in.Evidence})
		}
	}
	body, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "learning.json", append(body, '\n'))

	// The filter candidates keep what reviewers approved and drop the rejections, as the filter builtin evaluates
	// them.
	h := estate()
	for _, cond := range []string{`item.city != "Lyon"`, `item.price >= 210000.0`} {
		e, err := expr.CompileItem(cond)
		if err != nil {
			t.Fatal(err)
		}
		for id, want := range map[string]bool{"k6": false, "k15": true} {
			doc, _, _ := h.value(id, "")
			if got, err := e.Eval(doc, expr.Envelope{}, nil, expr.Run{}); err != nil || got != want {
				t.Errorf("%s on %s = %v, %v; want %v", cond, id, got, err, want)
			}
		}
	}
}

// Attribution walks the lineage upwards to the step whose output differs from its input on the field: builtins and
// copies pass through; a trigger's field, a field equal to one of two parents' and a lost parent are unattributed.
func TestAttribution(t *testing.T) {
	h := estate()
	h.item("j", "merge", `{"city":"Lille"}`, "c11", "c6")
	h.item("orphan", "score", `{"city":"Lille"}`, "gone")
	for _, tt := range []struct {
		id, pointer, want string
	}{
		{"k11", "/score", "score"}, // through the keep filter
		{"k11", "/city", "crawl"},  // score copies it
		{"s11", "/id", "crawl"},    // crawl dropped the trigger's field
		{"t11", "/id", ""},         // a trigger's
		{"j", "/city", ""},         // equal to a parent's
		{"orphan", "/city", ""},    // the parent is gone
	} {
		if got, ok := h.author(tt.id, tt.pointer); got != tt.want || ok != (tt.want != "") {
			t.Errorf("author(%s, %s) = %q, %v; want %q", tt.id, tt.pointer, got, ok, tt.want)
		}
	}
}

// itemStore serves Items from a map, for loadLineage: by ids, or the children of parents.
type itemStore struct {
	store.Store
	items map[string]store.Item
}

func (s itemStore) Items(_ context.Context, f store.ItemFilter) ([]store.Item, error) {
	var out []store.Item
	for _, it := range s.items {
		if slices.Contains(f.IDs, it.ID) || slices.ContainsFunc(it.Parents, func(p string) bool {
			return slices.Contains(f.Parents, p)
		}) {
			out = append(out, it)
		}
	}
	return out, nil
}

// loadLineage reads the inputs of what was discarded and decided with their ancestors, and the candidates' inputs
// with their outputs: here every item of the history but the outputs of keep that nobody decided.
func TestLoadLineage(t *testing.T) {
	h := estate()
	all := h.Items
	h.Items = nil
	if err := h.loadLineage(t.Context(), itemStore{items: all}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"s0", "c0", "t0", "k6", "s19", "c19", "t19"} {
		if _, ok := h.Items[id]; !ok {
			t.Errorf("%s not loaded", id)
		}
	}
	if len(h.Items) != len(all) {
		t.Errorf("loaded %d items of %d", len(h.Items), len(all))
	}
}

// The steps as the projector sees them: builtins by name, agents with their tools and output schema.
func TestStepsOf(t *testing.T) {
	p, err := pipeline.Parse([]byte(shop + "  gate: { action: review, in: scored }\n"))
	if err != nil {
		t.Fatal(err)
	}
	steps := StepsOf(p, func(ref string) *pipeline.Action {
		if ref != "./actions/score" {
			return nil
		}
		return &pipeline.Action{Output: []byte(`{}`), Agent: &pipeline.AgentRuntime{Tools: []string{"WebFetch"}}}
	})
	if s := steps["score"]; !s.Agent || s.Tools != 1 || string(s.Output) != "{}" || steps["gate"].Builtin != "review" ||
		steps["crawl"].Agent || StepsOf(p, nil)["score"].Agent {
		t.Fatalf("steps = %+v", steps)
	}
}

// A leaf is bounded by an enum, a const, a boolean, a number with both bounds, a formatted, patterned or short
// string, or anyOf of bounded leaves; arrays are walked into, and a remote $ref is unknown.
func TestBoundedFields(t *testing.T) {
	h := newHistory("s", nil)
	got := h.boundedFields("x", []byte(`{"type":"object","properties":{
		"flag":{"type":"boolean"},"when":{"type":["string","null"],"format":"date"},"code":{"type":"string","pattern":"^[A-Z]+$"},
		"tags":{"type":"array","items":{"type":"string","maxLength":8}},"either":{"anyOf":[{"const":1},{"type":"null"}]},
		"loose":{"anyOf":[{"type":"string"},{"enum":[1]}]},"count":{"type":"integer","minimum":0},
		"remote":{"$ref":"other.json"},"text":{"type":"string","maxLength":5000},"odd":true}}`))
	want := fields{Bounded: []string{"/code", "/either", "/flag", "/tags/*", "/when"}, Copied: []string{},
		Free: []string{"/count", "/loose", "/odd", "/remote", "/text"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("fields = %v\nwant     %v", got, want)
	}
	if f := h.boundedFields("x", []byte(`not json`)); len(f.Free)+len(f.Bounded) != 0 {
		t.Fatalf("fields of an unreadable schema = %v", f)
	}
}
