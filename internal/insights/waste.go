package insights

import (
	"cmp"
	"maps"
	"slices"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// wasteShare is the share of a step's cost in the window that, wasted, makes its waste insight a warning.
const wasteShare = 0.25

// discard is a step downstream that discarded items an agent step paid for, and how many.
type discard struct {
	Step  string `json:"step"`
	Kind  string `json:"kind"` // filter, dedupe or review
	Items int    `json:"items"`
}

// wastes finds, per step that spent, the items it paid for that a later filter, dedupe or reviewer discarded in the
// window (spec section 11.1): walking each discarded item's lineage upwards, every paid delivery that published an
// ancestor of it is charged its cost over its outputs, once per ancestor. At least minSamples discarded items make
// an insight, a warning when a quarter of the step's cost in the window or more was wasted; when a filter discarded
// the most, it suggests moving that filter upstream of the step.
func wastes(d *Data) []store.Insight {
	producers := map[[2]string]store.Delivery{} // by step and input item
	for _, dl := range d.Deliveries {
		producers[[2]string{dl.Step, dl.ItemID}] = dl
	}
	outputs := map[string]int{}
	for _, a := range d.Attempts {
		if a.Outcome == store.EventDeliveryDone {
			outputs[a.Delivery] = a.Outputs
		}
	}
	rejected := map[string]bool{}
	for _, r := range d.Decisions {
		if !r.Approved() {
			rejected[r.Delivery] = true
		}
	}
	type tally struct {
		items   int
		cost    float64
		charged map[string]bool // ancestors whose cost is counted
		by      map[string]*discard
		samples []string
	}
	byStep := map[string]*tally{}
	for _, dl := range d.Deliveries {
		if dl.State != store.DeliveryFiltered || !d.inWindow(dl.FinishedAt) {
			continue
		}
		kind := cmp.Or(d.Steps[dl.Step].Builtin, pipeline.BuiltinFilter)
		if rejected[dl.ID] {
			kind = pipeline.BuiltinReview
		}
		paid := map[string]bool{} // steps charged for this discard
		for _, it := range d.ancestors(dl.ItemID) {
			pd, ok := producer(producers, it)
			if !ok || pd.CostUSD <= 0 {
				continue
			}
			t := byStep[it.Step]
			if t == nil {
				t = &tally{charged: map[string]bool{}, by: map[string]*discard{}}
				byStep[it.Step] = t
			}
			if !t.charged[it.ID] {
				t.charged[it.ID] = true
				t.cost += pd.CostUSD / float64(max(outputs[pd.ID], 1))
			}
			if paid[it.Step] {
				continue
			}
			paid[it.Step] = true
			t.items++
			if t.by[dl.Step] == nil {
				t.by[dl.Step] = &discard{Step: dl.Step, Kind: kind}
			}
			t.by[dl.Step].Items++
			t.samples = appendSample(t.samples, dl.ItemID)
		}
	}
	spent := d.stepCosts()
	var out []store.Insight
	for _, step := range slices.Sorted(maps.Keys(byStep)) {
		t := byStep[step]
		if t.items < minSamples {
			continue
		}
		by := make([]discard, 0, len(t.by))
		for _, x := range t.by {
			by = append(by, *x)
		}
		slices.SortFunc(by, func(a, b discard) int { return cmp.Or(cmp.Compare(b.Items, a.Items), cmp.Compare(a.Step, b.Step)) })
		ev := map[string]any{"discardedItems": t.items, "wastedCostUsd": round6(t.cost),
			"costUsd": round6(spent[step]), "wasteShare": nil, "byStep": by, "suggestion": nil,
			"samples": nonNil(t.samples)}
		severity := SeverityInfo
		if spent[step] > 0 {
			share := min(t.cost/spent[step], 1)
			ev["wasteShare"] = round(share)
			if share >= wasteShare {
				severity = SeverityWarning
			}
		}
		if by[0].Kind == pipeline.BuiltinFilter {
			ev["suggestion"] = map[string]any{"kind": "topology", "moveUpstream": by[0].Step, "before": step}
		}
		out = append(out, d.insight(pipeline.InsightWaste, step, severity, ev, step))
	}
	return out
}

// ancestors returns item id and every ancestor of it held in the pipeline, each once.
func (d *Data) ancestors(id string) []store.Item {
	var out []store.Item
	seen := map[string]bool{}
	queue := []string{id}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		it, ok := d.Items[cur]
		if !ok || seen[cur] {
			continue
		}
		seen[cur] = true
		out = append(out, it)
		if it.ParentPipeline == "" {
			queue = append(queue, it.Parents...)
		}
	}
	return out
}

// producer returns the delivery that published item it: its step's delivery of one of its parents.
func producer(producers map[[2]string]store.Delivery, it store.Item) (store.Delivery, bool) {
	for _, p := range it.Parents {
		if dl, ok := producers[[2]string{it.Step, p}]; ok {
			return dl, true
		}
	}
	return store.Delivery{}, false
}
