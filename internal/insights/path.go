package insights

import (
	"context"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// PathStep is one delivery on a run's critical path, with its latency split into queue wait, work and the rest:
// the time a review step's delivery waited on people (Review), any other's retry backoffs (Backoff).
type PathStep struct {
	Step     string
	Delivery string
	Item     string
	Latency  time.Duration
	Wait     time.Duration
	Work     time.Duration
	Backoff  time.Duration
	Review   time.Duration
}

// Path is a run's critical path: the chain of deliveries along lineage whose latencies add up the most, from the
// trigger item to the last output (spec section 11.1).
type Path struct {
	Steps []PathStep
	Total time.Duration
}

// CriticalPath returns the critical path through a run's deliveries and items: each delivery's producer is the
// delivery that published its item (the item's step on its parent, in the item's generation). A delivery not yet
// terminal counts its latency up to now. Ties go to the delivery created last, the deeper chain. The deliveries of the
// steps in review wait on people for the rest of their latency.
func CriticalPath(ds []store.Delivery, items []store.Item, review map[string]bool, now time.Time) Path {
	byID := make(map[string]int, len(ds))
	for i, d := range ds {
		byID[d.ID] = i
	}
	producer := make(map[string]string, len(items)) // item id → the delivery that published it
	for _, it := range items {
		if len(it.Parents) != 1 {
			continue // a trigger item has none
		}
		if id := ids.Delivery(it.Step, it.Parents[0], it.Generation); hasKey(byID, id) {
			producer[it.ID] = id
		}
	}
	memo := make([]time.Duration, len(ds))
	done := make([]bool, len(ds))
	var longest func(i int) time.Duration
	longest = func(i int) time.Duration {
		if done[i] {
			return memo[i]
		}
		total := ds[i].Latency(now)
		if p, ok := producer[ds[i].ItemID]; ok && p != ds[i].ID {
			total += longest(byID[p])
		}
		memo[i], done[i] = total, true
		return total
	}
	end := -1
	for i := range ds {
		if end < 0 || longest(i) >= longest(end) {
			end = i
		}
	}
	if end < 0 {
		return Path{}
	}
	var steps []PathStep
	for i := end; ; {
		d := ds[i]
		lat, wait, work := d.Latency(now), d.WaitAt(now), d.WorkAt(now)
		s := PathStep{Step: d.Step, Delivery: d.ID, Item: d.ItemID, Latency: lat, Wait: wait, Work: work}
		if rest := max(lat-wait-work, 0); review[d.Step] {
			s.Review = rest
		} else {
			s.Backoff = rest
		}
		steps = append(steps, s)
		p, ok := producer[d.ItemID]
		if !ok || p == d.ID {
			break
		}
		i = byID[p]
	}
	slices.Reverse(steps)
	return Path{Steps: steps, Total: longest(end)}
}

// reviewSteps returns the review steps of a run's revision.
func reviewSteps(ctx context.Context, st store.Store, id store.RevisionID) (map[string]bool, error) {
	rev, err := st.Revision(ctx, id)
	if err != nil {
		return nil, err
	}
	p, err := rev.Decode()
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for name, s := range p.Steps {
		if s.Action == pipeline.BuiltinReview {
			out[name] = true
		}
	}
	return out, nil
}

func hasKey[K comparable, V any](m map[K]V, k K) bool {
	_, ok := m[k]
	return ok
}
