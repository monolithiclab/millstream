package insights

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// Step is what the projector knows of a step of the pipeline's active revision (StepsOf).
type Step struct {
	// Builtin is filter, dedupe or review for a builtin step, empty for an action.
	Builtin string
	// Agent is true for an agent action, and Tools counts its tools and MCP servers.
	Agent bool
	Tools int
	// Output is the action's output JSON Schema, nil when it declares none.
	Output []byte
}

// StepsOf describes p's steps; action resolves an action reference, returning nil when it cannot, and may be nil
// (actions are then of unknown kind).
func StepsOf(p *pipeline.Pipeline, action func(ref string) *pipeline.Action) map[string]Step {
	out := make(map[string]Step, len(p.Steps))
	for name, s := range p.Steps {
		if pipeline.IsBuiltin(s.Action) {
			out[name] = Step{Builtin: s.Action}
			continue
		}
		var st Step
		if action != nil {
			if a := action(s.Action); a != nil {
				st.Output = a.Output
				if a.Agent != nil {
					st.Agent, st.Tools = true, len(a.Agent.Tools)+len(a.Agent.MCP)
				}
			}
		}
		out[name] = st
	}
	return out
}

// Defaults of the improve settings the projector reads (spec section 14).
const (
	// DefaultDistillMinDeliveries is improve.distill.minDeliveries' default.
	DefaultDistillMinDeliveries = 500
	// DefaultFilterMaxFalseDrop is improve.filter.maxFalseDrop's default.
	DefaultFilterMaxFalseDrop = 0.01
)

// Config is what a pass reads besides the store: the steps of the active revision, the steps no registered node
// can take now (as the engine finds them, spec section 5.10), and improve.distill.minDeliveries and
// improve.filter.maxFalseDrop (zero: their defaults).
type Config struct {
	Steps                map[string]Step
	Pending              []PendingStep
	DistillMinDeliveries int
	FilterMaxFalseDrop   float64
}

// Lineage bounds.
const (
	// copySamples is how many of a distillation candidate's deliveries are read to tell the output fields it copies
	// from its input.
	copySamples = 50
	// idChunk is how many ids one read names.
	idChunk = 500
	// maxDepth bounds a walk up a lineage: deeper than any pipeline.
	maxDepth = 64
)

// loadLineage reads the items the lineage walks need into d.Items: the inputs of the deliveries discarded and the
// items decided in the window, with their ancestors in the pipeline, and a sample of each distillation candidate's
// inputs and outputs.
func (d *Data) loadLineage(ctx context.Context, st store.Store) error {
	d.Items = map[string]store.Item{}
	var seeds, inputs []string
	for _, dl := range d.Deliveries {
		if dl.State == store.DeliveryFiltered && d.inWindow(dl.FinishedAt) {
			seeds = append(seeds, dl.ItemID)
		}
	}
	for _, r := range d.Decisions {
		if r.Item != "" && r.Reviewer == review.ReviewerPerson && d.inWindow(r.TS) {
			seeds = append(seeds, r.Item)
		}
	}
	candidates := d.candidates()
	sampled := map[string]int{}
	for _, dl := range d.Deliveries {
		if dl.State == store.DeliveryDone && slices.Contains(candidates, dl.Step) && d.inWindow(dl.CreatedAt) &&
			sampled[dl.Step] < copySamples {
			sampled[dl.Step]++
			inputs = append(inputs, dl.ItemID)
		}
	}
	if err := d.fetch(ctx, st, append(seeds, inputs...), nil); err != nil {
		return err
	}
	if err := d.fetch(ctx, st, inputs, func(it store.Item) bool {
		return slices.Contains(candidates, it.Step)
	}); err != nil {
		return err
	}
	frontier, reached := seeds, map[string]bool{}
	for range maxDepth {
		var next, missing []string
		for _, id := range frontier {
			it, ok := d.Items[id]
			if !ok || it.ParentPipeline != "" {
				continue
			}
			for _, p := range it.Parents {
				if reached[p] {
					continue
				}
				reached[p] = true
				next = append(next, p)
				if _, ok := d.Items[p]; !ok {
					missing = append(missing, p)
				}
			}
		}
		if len(next) == 0 {
			return nil
		}
		if err := d.fetch(ctx, st, missing, nil); err != nil {
			return err
		}
		frontier = next
	}
	return nil
}

// fetch reads items of the pipeline into d.Items: those with ids, or with children, the children of ids that keep
// passes.
func (d *Data) fetch(ctx context.Context, st store.Store, ids []string, children func(store.Item) bool) error {
	for chunk := range slices.Chunk(ids, idChunk) {
		f := store.ItemFilter{Pipeline: d.Pipeline, IDs: chunk}
		if children != nil {
			f = store.ItemFilter{Pipeline: d.Pipeline, Parents: chunk}
		}
		items, err := st.Items(ctx, f)
		if err != nil {
			return fmt.Errorf("lineage: %w", err)
		}
		for _, it := range items {
			if children == nil || children(it) {
				d.Items[it.ID] = it
			}
		}
	}
	return nil
}

// candidates are the agent steps with at least improve.distill.minDeliveries deliveries created in the window: the
// steps distillable evaluates further.
func (d *Data) candidates() []string {
	n := map[string]int{}
	for _, dl := range d.Deliveries {
		if d.Steps[dl.Step].Agent && d.inWindow(dl.CreatedAt) {
			n[dl.Step]++
		}
	}
	var out []string
	for step, c := range n {
		if c >= d.DistillMinDeliveries {
			out = append(out, step)
		}
	}
	slices.Sort(out)
	return out
}

// value returns the value at pointer p of item id's payload, and whether the item is held with its payload and has
// one there (present).
func (d *Data) value(id, p string) (v any, held, present bool) {
	it, ok := d.Items[id]
	if !ok || it.Tombstone != "" {
		return nil, false, false
	}
	if d.payloads == nil {
		d.payloads = map[string]any{}
	}
	doc, ok := d.payloads[id]
	if !ok {
		if err := json.Unmarshal(it.Payload, &doc); err != nil {
			doc = nil
		}
		d.payloads[id] = doc
	}
	v, err := review.Get(doc, p)
	return v, true, err == nil
}

// differs reports whether item a and item b differ at pointer p, and whether both are held, so comparable.
func (d *Data) differs(a, b, p string) (diff, held bool) {
	av, aHeld, aOK := d.value(a, p)
	bv, bHeld, bOK := d.value(b, p)
	if !aHeld || !bHeld {
		return false, false
	}
	return aOK != bOK || !reflect.DeepEqual(av, bv), true
}

// author attributes the value at pointer p of item id (spec section 11.6): walking its lineage upwards, the
// nearest step whose output differs from its input there. Builtins pass items through and are never blamed, nor is
// an action whose output equals its input there. ok is false when no single step changed the value: it came from a
// trigger, from another pipeline, from several parents it equals one of, or from an item no longer held.
func (d *Data) author(id, p string) (step string, ok bool) {
	for range maxDepth {
		it, found := d.Items[id]
		if !found || it.ParentPipeline != "" || len(it.Parents) == 0 {
			return "", false
		}
		builtin := d.Steps[it.Step].Builtin != ""
		if len(it.Parents) > 1 {
			if builtin {
				return "", false
			}
			for _, parent := range it.Parents {
				if diff, held := d.differs(id, parent, p); !diff || !held {
					return "", false
				}
			}
			return it.Step, true
		}
		parent := it.Parents[0]
		if !builtin {
			diff, held := d.differs(id, parent, p)
			switch {
			case !held:
				return "", false
			case diff:
				return it.Step, true
			}
		}
		id = parent
	}
	return "", false
}

// lineage returns item id and its ancestors held, nearest first, following first parents.
func (d *Data) lineage(id string) []store.Item {
	var out []store.Item
	for range maxDepth {
		it, ok := d.Items[id]
		if !ok {
			break
		}
		out = append(out, it)
		if len(it.Parents) == 0 || it.ParentPipeline != "" {
			break
		}
		id = it.Parents[0]
	}
	return out
}

// producedBy reports whether step published item id or one of its ancestors on its first-parent lineage.
func (d *Data) producedBy(id, step string) bool {
	return slices.ContainsFunc(d.lineage(id), func(it store.Item) bool { return it.Step == step })
}
