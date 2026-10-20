package backtest

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// The distill proposal's history (spec section 11.5): the pairs of an agent step, their split, and the holdout the
// engine validates a distilled action on. The backtester serves them because it reads history as backtests do:
// local history, else the imported evidence.

// Training is what the drafting agent may see of an agent step's history: the training pairs of its split, and the
// step's lessons with the examples derived from the holdout withheld. The holdout itself is never served.
type Training struct {
	Pipeline string
	Step     string
	// Agent is the step's action, Source where the pairs come from (local or evidence).
	Agent  string
	Source string
	// HoldoutFrom is the split's boundary: the holdout is the entities first seen at or after it.
	HoldoutFrom time.Time
	// Pairs counts every pair, Holdout those of the holdout.
	Pairs   int
	Holdout int
	Train   []distill.Pair
	// Lessons are the step's lessons on the active revision but for Withheld examples.
	Lessons  pipeline.Lessons
	Withheld int
}

// Training splits an agent step's pairs and returns the training side (GET .../steps/{step}/distill-training).
func (b *Backtester) Training(ctx context.Context, pipelineName, step string) (Training, error) {
	t := Training{Pipeline: pipelineName, Step: step}
	if b.opts.Mode == pipeline.ModeProduction {
		return t, ErrProduction
	}
	pkg, err := b.activePackage(ctx, pipelineName)
	if err != nil {
		return t, err
	}
	st, ok := pkg.Pipeline.Steps[step]
	if !ok {
		return t, fmt.Errorf("pipeline %s has no step %s: %w", pipelineName, step, store.ErrNotFound)
	}
	act, err := b.opts.Resolver.Resolve(pkg, st.Action)
	if err != nil || act.Kind != pipeline.KindAgent {
		return t, fmt.Errorf("step %s of %s does not run an agent: only an agent step is distilled: %w", step,
			pipelineName, store.ErrInvalid)
	}
	t.Agent = st.Action
	pairs, source, err := b.pairs(ctx, pipelineName, step)
	if err != nil {
		return t, err
	}
	split, err := distill.SplitPairs(pairs, distill.HoldoutShare)
	if err != nil {
		return t, fmt.Errorf("%w: %d pairs of step %s: %w", err, len(pairs), step, store.ErrInvalid)
	}
	t.Source, t.HoldoutFrom, t.Pairs, t.Holdout, t.Train = source, split.HoldoutFrom, len(pairs),
		len(split.Holdout), split.Train
	t.Lessons, t.Withheld, err = stepLessons(pkg, step, split.Holdout)
	return t, err
}

// stepLessons returns a step's lessons without the examples derived from the holdout, and how many it withheld.
func stepLessons(pkg *pipeline.Package, step string, holdout []distill.Pair) (pipeline.Lessons, int, error) {
	var out pipeline.Lessons
	file := cmp.Or(pkg.Pipeline.Steps[step].Lessons, "lessons/"+step+".yaml")
	data, ok := pkg.Files[file]
	if !ok {
		return out, 0, nil
	}
	l, err := pipeline.ParseLessons(data)
	if err != nil {
		return out, 0, fmt.Errorf("%s: %w", file, err)
	}
	withheld := distill.Withheld(l.Examples, holdout)
	for i, ex := range l.Examples {
		if !slices.Contains(withheld, i) {
			out.Examples = append(out.Examples, ex)
		}
	}
	out.Rules = l.Rules
	return out, len(withheld), nil
}

// Holdout returns an agent step's holdout pairs: those of the entities first seen at or after from (the boundary a
// distill proposal records), read from history now. It implements proposals.HoldoutSource.
func (b *Backtester) Holdout(ctx context.Context, pipelineName, step string, from time.Time) ([]distill.Pair, error) {
	pairs, _, err := b.pairs(ctx, pipelineName, step)
	if err != nil {
		return nil, err
	}
	return distill.Holdout(pairs, from), nil
}

// pairs reads an agent step's pairs from local history, else from the imported evidence.
func (b *Backtester) pairs(ctx context.Context, pipelineName, step string) ([]distill.Pair, string, error) {
	h, err := loadLocal(ctx, b.opts.Store, pipelineName)
	if err != nil {
		return nil, "", err
	}
	if ps := pairsOf(h, step); len(ps) > 0 {
		return ps, SourceLocal, nil
	}
	if h, err = loadEvidence(ctx, b.opts.Store, b.opts.Artifacts, pipelineName, b.opts.Now()); err != nil {
		return nil, "", err
	}
	return pairsOf(h, step), SourceEvidence, nil
}

// activePackage reads a pipeline's active revision's package, in memory.
func (b *Backtester) activePackage(ctx context.Context, pipelineName string) (*pipeline.Package, error) {
	id, err := b.opts.Store.ActiveRevision(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	rev, err := b.opts.Store.Revision(ctx, id)
	if err != nil {
		return nil, err
	}
	bundle, err := b.opts.Store.Package(ctx, rev.PackageHash)
	if err != nil {
		return nil, err
	}
	files, err := pipeline.DecodeBundle(bundle)
	if err != nil {
		return nil, err
	}
	return pipeline.FromFiles("", files)
}

// maxReviewDepth bounds how far below an output a review is looked for.
const maxReviewDepth = 16

// pairsOf builds an agent step's pairs from history, oldest input first: each generation-0 delivery of the step
// that ended done on an input whose payload is kept, with its outputs as the truth, corrected by what people edited
// in them at a review downstream (the values the review received unchanged from the output). An output a person
// rejected has no truth, and its pair is left out, as is one whose output is gone.
func pairsOf(h *history, step string) []distill.Pair {
	reviewed := map[string][]store.Review{} // the decisions of people, by the item decided
	for _, r := range h.reviews {
		if r.Reviewer == review.ReviewerPerson {
			reviewed[r.Item.ID] = append(reviewed[r.Item.ID], r)
		}
	}
	var out []distill.Pair
	for _, d := range h.deliveries {
		if d.Step != step || d.State != store.DeliveryDone {
			continue
		}
		in, ok := h.items[d.ItemID]
		if !ok || in.Tombstone != "" {
			continue
		}
		p := distill.Pair{Item: in.ID, Key: in.Key, At: in.TS, Input: in.Payload, CostUSD: d.CostUSD,
			Outputs: []distill.Output{}}
		if root, ok := h.root(in.ID); ok {
			p.Root = root.ID
		}
		if h.truth(d, &p, reviewed) {
			out = append(out, p)
		}
	}
	slices.SortFunc(out, func(a, b distill.Pair) int { return cmp.Or(a.At.Compare(b.At), cmp.Compare(a.Item, b.Item)) })
	return out
}

// truth sets a pair's outputs from its delivery's, with the people's edits applied; false when it has none.
func (h *history) truth(d store.Delivery, p *distill.Pair, reviewed map[string][]store.Review) bool {
	seen := map[string]bool{}
	for _, o := range h.children[d.ItemID] {
		if o.Step != d.Step || o.RunID != d.RunID || o.Generation != d.Generation || seen[o.Key] {
			continue
		}
		seen[o.Key] = true
		if o.Tombstone != "" {
			return false
		}
		var doc any
		if json.Unmarshal(o.Payload, &doc) != nil {
			return false
		}
		for _, r := range h.reviewsBelow(o.ID, reviewed) {
			if r.Decision == store.DecisionReject {
				return false
			}
			if edited, ok := h.edit(doc, r); ok {
				doc, p.Edited, p.Reviews = edited, true, append(p.Reviews, r.DeliveryID)
			}
		}
		payload, err := json.Marshal(doc)
		if err != nil {
			return false
		}
		p.Outputs = append(p.Outputs, distill.Output{Key: o.Key, Payload: payload})
	}
	return len(p.Outputs) > 0
}

// reviewsBelow returns the people's decisions on an item and its descendants, nearest first.
func (h *history) reviewsBelow(id string, reviewed map[string][]store.Review) []store.Review {
	var out []store.Review
	level, seen := []string{id}, map[string]bool{id: true}
	for depth := 0; len(level) > 0 && depth < maxReviewDepth; depth++ {
		var next []string
		for _, it := range level {
			out = append(out, reviewed[it]...)
			for _, c := range h.children[it] {
				if !seen[c.ID] {
					seen[c.ID] = true
					next = append(next, c.ID)
				}
			}
		}
		level = next
	}
	return out
}

// edit applies to an output's payload the operations of a review's edit at pointers where the reviewed item held
// the output's value unchanged: the edits the output was corrected by (spec section 11.6's attribution).
func (h *history) edit(doc any, r store.Review) (any, bool) {
	if len(r.Patch) == 0 {
		return doc, false
	}
	ops, err := review.ParsePatch(r.Patch)
	if err != nil || len(ops) == 0 {
		return doc, false
	}
	var item any
	if it, ok := h.items[r.Item.ID]; !ok || json.Unmarshal(it.Payload, &item) != nil {
		return doc, false
	}
	var carried []review.Op
	for _, op := range ops {
		if op.Op != "add" && op.Op != "replace" && op.Op != "remove" {
			continue
		}
		a, errA := review.Get(doc, op.Path)
		b, errB := review.Get(item, op.Path)
		if (errA == nil) == (errB == nil) && reflect.DeepEqual(a, b) {
			carried = append(carried, op)
		}
	}
	if len(carried) == 0 {
		return doc, false
	}
	out, err := review.Apply(doc, carried)
	if err != nil {
		return doc, false
	}
	return out, true
}
