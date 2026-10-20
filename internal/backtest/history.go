package backtest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// history is the past a backtest replays: local history or imported evidence, in the store's shapes.
type history struct {
	source  string
	bundles []string
	items   map[string]store.Item
	// children are the items by parent id.
	children map[string][]store.Item
	// deliveries are by id; only generation 0's are kept, those of trigger runs a replay fires again.
	deliveries map[string]store.Delivery
	// reviews are the decided and timed-out reviews, by delivery id.
	reviews map[string]store.Review
	// runs are the local runs by id (nil for evidence), revisions each run's package hash.
	runs      map[string]store.Run
	revisions map[string]string
}

// loadLocal reads the pipeline's own history.
func loadLocal(ctx context.Context, st store.Store, pipelineName string) (*history, error) {
	h := newHistory(SourceLocal)
	items, err := st.Items(ctx, store.ItemFilter{Pipeline: pipelineName})
	if err != nil {
		return nil, err
	}
	ds, err := st.Deliveries(ctx, store.DeliveryFilter{Pipeline: pipelineName})
	if err != nil {
		return nil, err
	}
	var reviews []store.Review
	for _, state := range []store.ReviewState{store.ReviewDecided, store.ReviewTimedOut} {
		rs, err := st.Reviews(ctx, store.ReviewFilter{Pipeline: pipelineName, State: state})
		if err != nil {
			return nil, err
		}
		reviews = append(reviews, rs...)
	}
	runs, err := st.Runs(ctx, store.RunFilter{Pipeline: pipelineName})
	if err != nil {
		return nil, err
	}
	revs, err := st.Revisions(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	hashes := map[store.RevisionID]string{}
	for _, r := range revs {
		hashes[r.ID] = r.PackageHash
	}
	h.runs = map[string]store.Run{}
	for _, r := range runs {
		if r.Kind == store.RunKindTrigger && r.Generation == 0 {
			h.runs[r.ID] = r
			h.revisions[r.ID] = hashes[r.Revision]
		}
	}
	h.add(items, ds, reviews)
	return h, nil
}

// loadEvidence reads the pipeline's imported evidence bundles that have not expired.
func loadEvidence(ctx context.Context, st store.Store, arts artifacts.Store, pipelineName string, now time.Time) (
	*history, error,
) {
	h := newHistory(SourceEvidence)
	if arts == nil {
		return h, nil
	}
	bundles, err := evidence.Load(ctx, st, arts, pipelineName, now)
	if err != nil {
		return nil, err
	}
	for _, b := range bundles {
		h.bundles = append(h.bundles, b.ID)
		items := b.StoreItems()
		for _, it := range items {
			h.revisions[it.RunID] = b.Revision.PackageHash
		}
		h.add(items, b.StoreDeliveries(), b.StoreReviews())
	}
	return h, nil
}

func newHistory(source string) *history {
	return &history{source: source, items: map[string]store.Item{}, children: map[string][]store.Item{},
		deliveries: map[string]store.Delivery{}, reviews: map[string]store.Review{}, revisions: map[string]string{}}
}

// add records items, deliveries and reviews, the first copy of each winning (bundles may overlap).
func (h *history) add(items []store.Item, ds []store.Delivery, reviews []store.Review) {
	for _, it := range items {
		if _, dup := h.items[it.ID]; dup {
			continue
		}
		h.items[it.ID] = it
		for _, p := range it.Parents {
			h.children[p] = append(h.children[p], it)
		}
	}
	for _, d := range ds {
		if _, dup := h.deliveries[d.ID]; !dup && d.Generation == 0 {
			h.deliveries[d.ID] = d
		}
	}
	for _, r := range reviews {
		if _, dup := h.reviews[r.DeliveryID]; !dup {
			h.reviews[r.DeliveryID] = r
		}
	}
}

// delivery returns the historical delivery of step on an item, the one a replay's delivery has the id of.
func (h *history) delivery(step, itemID string) (store.Delivery, bool) {
	d, ok := h.deliveries[ids.Delivery(step, itemID, 0)]
	return d, ok
}

// ended reports whether a delivery has an outcome a replay can take: done, filtered or dead.
func ended(d store.Delivery) bool {
	return d.State.Terminal() && d.State != store.DeliveryCancelled
}

// outputs returns what a delivery published: its step's children of its item in its run, one per key (an output
// routed to several streams is one output), in publication order.
func outputs(children map[string][]store.Item, d store.Delivery) []store.Output {
	var out []store.Output
	seen := map[string]bool{}
	for _, it := range children[d.ItemID] {
		if it.Step != d.Step || it.RunID != d.RunID || it.Generation != d.Generation {
			continue
		}
		k := outputKey(store.Output{Key: it.Key, Payload: it.Payload})
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, store.Output{Key: it.Key, Payload: it.Payload})
	}
	return out
}

// sameOutputs compares two deliveries' outputs as sets of keys and canonical payloads.
func sameOutputs(a, b []store.Output) bool {
	keys := func(os []store.Output) []string {
		out := make([]string, len(os))
		for i, o := range os {
			out[i] = outputKey(o)
		}
		slices.Sort(out)
		return out
	}
	return slices.Equal(keys(a), keys(b))
}

// outputKey identifies an output by its key and canonical payload.
func outputKey(o store.Output) string { return o.Key + "\x00" + distill.Canon(o.Payload) }

// root returns the trigger item an item descends from, false when its lineage leaves the history, crosses a
// tombstone (a payload replay cannot read, spec section 5.7) or holds no trigger item.
func (h *history) root(itemID string) (store.Item, bool) {
	for range 1000 { // a lineage is a DAG; the bound only guards against a corrupt one
		it, ok := h.items[itemID]
		if !ok || it.Tombstone != "" {
			return store.Item{}, false
		}
		if strings.HasPrefix(it.Stream, pipeline.TriggerStreamPrefix) {
			return it, len(it.Parents) == 0
		}
		if len(it.Parents) == 0 {
			return store.Item{}, false
		}
		itemID = it.Parents[0]
	}
	return store.Item{}, false
}

// lineage returns the ids of the items, their ancestors and their descendants.
func (h *history) lineage(itemIDs []string) map[string]bool {
	out, seenUp, seenDown := map[string]bool{}, map[string]bool{}, map[string]bool{}
	var up, down func(string)
	up = func(id string) {
		if seenUp[id] {
			return
		}
		seenUp[id], out[id] = true, true
		for _, p := range h.items[id].Parents {
			up(p)
		}
	}
	down = func(id string) {
		if seenDown[id] {
			return
		}
		seenDown[id], out[id] = true, true
		for _, c := range h.children[id] {
			down(c.ID)
		}
	}
	for _, id := range itemIDs {
		up(id)
		down(id)
	}
	return out
}

// firing is a historical run as a replay fires it again.
type firing struct {
	runID, trigger, key string
	payload             json.RawMessage
	vars                map[string]any
	sources             map[string]string
}

// firing returns the run a trigger item opened, with its variables for p: the run's own, except those it took
// from a default p changes, which take p's (a patch moving a threshold kept in a variable). Evidence carries no run:
// its variables resolve against p as a firing's would.
func (h *history) firing(trigger store.Item, p *pipeline.Pipeline, resolve func(*pipeline.Pipeline) (
	pipeline.Resolved, error),
) (firing, error) {
	f := firing{runID: trigger.RunID, trigger: strings.TrimPrefix(trigger.Stream, pipeline.TriggerStreamPrefix),
		key: trigger.Key, payload: trigger.Payload}
	run, local := h.runs[trigger.RunID]
	if !local {
		res, err := resolve(p)
		if err != nil {
			return f, fmt.Errorf("the variables of run %s: %w", trigger.RunID, err)
		}
		f.vars, f.sources = res.Vars, res.Sources
		return f, nil
	}
	f.vars, f.sources = map[string]any{}, map[string]string{}
	for k, v := range run.Vars {
		f.vars[k], f.sources[k] = v, run.VarSources[k]
	}
	for name, def := range p.Vars {
		if src, ok := f.sources[name]; (!ok || src == pipeline.SourceDefault) && def != nil {
			f.vars[name], f.sources[name] = def, pipeline.SourceDefault
		}
	}
	return f, nil
}
