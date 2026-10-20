package backtest

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// measure compares a replay with the history: over the sample when step is empty, else over step's deliveries on
// the sampled lineages.
func measure(h *history, s sample, r *replayed, step string) Metrics {
	var m Metrics
	var latency time.Duration
	lin := newReviewed(h, s, r)
	// Agreement with the historical outputs, split by whether a review lies below.
	compare := func(hd store.Delivery, agreed bool) {
		m.Compared++
		if agreed {
			m.Agreed++
		}
		if revs := lin.below(hd.ItemID); len(revs) == 0 {
			m.UnreviewedCompared++
			if agreed {
				m.UnreviewedAgreed++
			}
		} else {
			m.ReviewedCompared++
			if agreed {
				m.ReviewedAgreed++
			} else if lin.met(revs) {
				m.TowardReviewers++
			}
		}
	}
	if step == "" {
		for _, sd := range s.sampled {
			hd := h.deliveries[sd.Delivery]
			d, ok := r.deliveries[hd.ID]
			if !ok {
				m.NoLongerReached++
			}
			compare(hd, ok && alike(h, r, hd, d))
		}
	} else {
		for _, hd := range h.deliveries {
			if hd.Step != step || !ended(hd) || !s.keep[hd.ItemID] {
				continue
			}
			if _, ok := r.deliveries[hd.ID]; !ok && s.fired[hd.RunID] {
				m.NoLongerReached++
			}
		}
	}
	for _, d := range r.deliveries {
		if step != "" && d.Step != step {
			continue
		}
		dec := r.decisions[d.ID]
		if dec == offSample {
			continue
		}
		hd, inHistory := h.deliveries[d.ID]
		if !inHistory || !ended(hd) {
			if dec != unrecorded {
				m.NewlyReached++
			}
		} else if step != "" && dec != unrecorded {
			compare(hd, alike(h, r, hd, d))
		}
		if dec == ran && d.State != store.DeliveryPending && d.State != store.DeliveryLeased {
			m.Ran++
			m.CostUSD += d.CostUSD
			latency += d.Wait + d.Work
			if d.State == store.DeliveryDead {
				m.Failed++
			}
		}
	}
	// Agreement with the reviewers.
	for id, rv := range h.reviews {
		hd, ok := h.deliveries[id]
		if !ok || !s.keep[hd.ItemID] || step != "" && hd.Step != step || !s.fired[hd.RunID] {
			continue
		}
		m.Reviewed++
		if rv.Decision != store.DecisionReject {
			m.Approved++
			if _, reached := r.deliveries[hd.ID]; !reached {
				m.ApprovalsDropped++
			}
		}
		if lin.verdict[id].agrees {
			m.ReviewAgreed++
		}
	}
	m.Agreement = ratio(m.Agreed, m.Compared)
	m.UnreviewedAgreement = ratio(m.UnreviewedAgreed, m.UnreviewedCompared)
	m.ReviewedAgreement = ratio(m.ReviewedAgreed, m.ReviewedCompared)
	m.TowardReviewersShare = ratio(m.TowardReviewers, m.ReviewedCompared)
	m.ReviewAgreement = ratio(m.ReviewAgreed, m.Reviewed)
	m.FailureRate = ratio(m.Failed, m.Ran)
	if m.Ran > 0 {
		m.LatencyMs = float64(latency.Milliseconds()) / float64(m.Ran)
	}
	return m
}

// reviewed finds the decided reviews below a delivery's item, on the sampled lineages, and whether a replay meets
// them: each review's verdict is worked out once.
type reviewed struct {
	h       *history
	byItem  map[string][]string // the reviews' delivery ids by the item each review received
	verdict map[string]verdict  // by review delivery id
	cache   map[string][]string
}

// verdict is a replay against one review: whether it agrees, and whether the review edited or rejected the item.
type verdict struct{ agrees, corrective bool }

func newReviewed(h *history, s sample, r *replayed) *reviewed {
	l := &reviewed{h: h, byItem: map[string][]string{}, verdict: map[string]verdict{}, cache: map[string][]string{}}
	for id, rv := range h.reviews {
		hd, ok := h.deliveries[id]
		if !ok || !s.keep[hd.ItemID] || !s.fired[hd.RunID] {
			continue
		}
		l.byItem[hd.ItemID] = append(l.byItem[hd.ItemID], id)
		ops, err := review.ParsePatch(rv.Patch)
		l.verdict[id] = verdict{agrees: reviewAgrees(h, r, rv, hd),
			corrective: rv.Decision == store.DecisionReject || (err == nil && len(ops) > 0)}
	}
	return l
}

// below returns the reviews of an item and its descendants: the reviews its delivery's outputs lead to.
func (l *reviewed) below(itemID string) []string {
	if len(l.byItem) == 0 {
		return nil
	}
	out, ok := l.cache[itemID]
	if !ok {
		out = below(l.h, itemID, l.byItem)
		l.cache[itemID] = out
	}
	return out
}

// met reports whether the replay moved toward the reviewers' truth on these reviews: one at least edited or rejected
// the item, and the replay agrees with every one (the edited values reached, the rejected item no longer reaching
// its review, the approved one still reaching it).
func (l *reviewed) met(revs []string) bool {
	corrective := false
	for _, id := range revs {
		v := l.verdict[id]
		if !v.agrees {
			return false
		}
		corrective = corrective || v.corrective
	}
	return corrective
}

// ratio is a / b, 0 when b is 0.
func ratio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// alike reports whether a replay's delivery ended as its historical counterpart did: the same state, and for a done
// delivery the same outputs.
func alike(h *history, r *replayed, hd, d store.Delivery) bool {
	if hd.State != d.State {
		return false
	}
	return hd.State != store.DeliveryDone || sameOutputs(outputs(h.children, hd), outputs(r.children, d))
}

// reviewAgrees reports whether a replay agrees with a review's decision, taking the reviewer's output as the
// truth. An item the review rejected agrees by no longer reaching it; one it approved, by still reaching it; one it
// edited, by reaching it with the edited values at every pointer the edit wrote. An item reaching the review with
// its historical payload meets the same decision again: it agrees only when that was a plain approval.
func reviewAgrees(h *history, r *replayed, rv store.Review, hd store.Delivery) bool {
	d, reached := r.deliveries[hd.ID]
	if reached && (d.State == store.DeliveryDead || d.State == store.DeliveryCancelled) {
		return false
	}
	reject := rv.Decision == store.DecisionReject
	if !reached || reject {
		return !reached && reject
	}
	ops, err := review.ParsePatch(rv.Patch)
	if err != nil || len(ops) == 0 {
		return true // approved, and still reaching its review
	}
	if r.decisions[d.ID] == recorded {
		return false // the same input needs the same edit
	}
	truth := outputs(h.children, hd)
	if len(truth) == 0 {
		return false
	}
	want, got := decode(truth[0].Payload), decode(r.items[d.ItemID].Payload)
	for _, p := range review.Pointers(ops) {
		w, werr := review.Get(want, p)
		g, gerr := review.Get(got, p)
		if (werr == nil) != (gerr == nil) || !reflect.DeepEqual(w, g) {
			return false
		}
	}
	return true
}

func decode(raw json.RawMessage) any {
	v, _ := pipeline.DecodeValue(string(raw))
	return v
}

// strata counts the sample by stratum.
func strata(sampled []evidence.Sampled) map[string]int {
	out := map[string]int{}
	for _, s := range sampled {
		out[s.Stratum]++
	}
	return out
}
