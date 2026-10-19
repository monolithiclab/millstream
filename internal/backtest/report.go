package backtest

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// measure compares a replay with the history: over the sample when step is empty, else over step's deliveries on
// the sampled lineages.
func measure(h *history, s sample, r *replayed, step string) Metrics {
	var m Metrics
	var latency time.Duration
	// Agreement with the historical outputs.
	if step == "" {
		for _, sd := range s.sampled {
			hd := h.deliveries[sd.Delivery]
			m.Compared++
			d, ok := r.deliveries[hd.ID]
			switch {
			case !ok:
				m.NoLongerReached++
			case alike(h, r, hd, d):
				m.Agreed++
			}
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
			m.Compared++
			if alike(h, r, hd, d) {
				m.Agreed++
			}
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
		if reviewAgrees(h, r, rv, hd) {
			m.ReviewAgreed++
		}
	}
	m.Agreement = ratio(m.Agreed, m.Compared)
	m.ReviewAgreement = ratio(m.ReviewAgreed, m.Reviewed)
	m.FailureRate = ratio(m.Failed, m.Ran)
	if m.Ran > 0 {
		m.LatencyMs = float64(latency.Milliseconds()) / float64(m.Ran)
	}
	return m
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
	var v any
	_ = json.Unmarshal(raw, &v)
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
