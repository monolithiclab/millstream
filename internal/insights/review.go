package insights

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// Review thresholds.
const (
	// minAgreement is the sampled-rule agreement under which a reviewPattern is a warning: the autoApprove rule
	// approves what people would not.
	minAgreement = 0.9
	// maxHotSpots is how many of the most edited pointers a reviewPattern lists.
	maxHotSpots = 10
)

// Decision is a review decision, read from its reviewDecided event: by a person, the autoApprove rule or onTimeout.
type Decision struct {
	TS       time.Time
	Step     string
	Delivery string
	// Decision is approve or reject; Reviewer is rule, timeout or person (events never name the person).
	Decision string
	Reviewer string
	Reason   string
	Edited   []string
	// TimeToDecision is from parked to decided.
	TimeToDecision time.Duration
}

// Approved reports whether the decision passed the item on.
func (d Decision) Approved() bool { return d.Decision == store.DecisionApprove }

// SampleOutcome is a review sample's end, read from its sampleDecided or sampleExpired event: agree, disagree or
// unreviewed.
type SampleOutcome struct {
	TS        time.Time
	Step      string
	Agreement string
}

// readReviews returns a pipeline's review decisions and sample outcomes recorded at or after since.
func readReviews(ctx context.Context, st store.Store, pipelineName string, since time.Time) ([]Decision,
	[]SampleOutcome, error,
) {
	evs, err := st.PipelineEvents(ctx, store.EventFilter{Pipeline: pipelineName, Since: since,
		Kinds: []string{store.EventReviewDecided, store.EventSampleDecided, store.EventSampleExpired}})
	if err != nil {
		return nil, nil, err
	}
	var ds []Decision
	var ss []SampleOutcome
	for _, ev := range evs {
		var d struct {
			DeliveryID string   `json:"deliveryId"`
			Step       string   `json:"step"`
			Decision   string   `json:"decision"`
			Reviewer   string   `json:"reviewer"`
			Reason     string   `json:"reason"`
			Edited     []string `json:"edited"`
			DecisionMS int64    `json:"timeToDecisionMs"`
		}
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			return nil, nil, fmt.Errorf("event %d: %w", ev.Seq, err)
		}
		switch ev.Kind {
		case store.EventReviewDecided:
			ds = append(ds, Decision{TS: ev.TS, Step: d.Step, Delivery: d.DeliveryID, Decision: d.Decision,
				Reviewer: d.Reviewer, Reason: d.Reason, Edited: d.Edited,
				TimeToDecision: time.Duration(d.DecisionMS) * time.Millisecond})
		case store.EventSampleDecided:
			ss = append(ss, SampleOutcome{TS: ev.TS, Step: d.Step, Agreement: d.Decision})
		default:
			ss = append(ss, SampleOutcome{TS: ev.TS, Step: d.Step, Agreement: string(store.SampleUnreviewed)})
		}
	}
	return ds, ss, nil
}

func (d *Data) inWindow(t time.Time) bool { return !t.Before(d.windowStart()) && !t.After(d.Now) }

// approvals counts the items the pipeline's review steps approved in the window, whoever approved them: the
// denominator of the cost per approved item.
func (d *Data) approvals() int {
	n := 0
	for _, r := range d.Decisions {
		if r.Approved() && d.inWindow(r.TS) {
			n++
		}
	}
	return n
}

// reviewStats is what reviewPatterns adds up for one review step over the window.
type reviewStats struct {
	approved, rejected, autoApproved, timedOut int
	byReason                                   map[string]int
	edited                                     map[string]int
	editedApprovals                            int
	times                                      []time.Duration
	agreement                                  map[string]int
	samples                                    []string
}

// reviewPatterns reports, per review step with decisions or samples in the window: decisions by outcome and
// rejections by reason code, the pointers reviewers edit most, time to decision, sampled-rule agreement (a warning
// under 90%), and the intervention rate: items a person decided over items reaching the step.
func reviewPatterns(d *Data) []store.Insight {
	items, reached := map[string]string{}, map[string]int{}
	for _, dl := range d.Deliveries {
		items[dl.ID] = dl.ItemID
		if d.inWindow(dl.CreatedAt) {
			reached[dl.Step]++
		}
	}
	steps := map[string]*reviewStats{}
	stats := func(step string) *reviewStats {
		s := steps[step]
		if s == nil {
			s = &reviewStats{byReason: map[string]int{}, edited: map[string]int{}, agreement: map[string]int{}}
			steps[step] = s
		}
		return s
	}
	for _, r := range d.Decisions {
		if !d.inWindow(r.TS) {
			continue
		}
		s := stats(r.Step)
		switch {
		case r.Reviewer == review.ReviewerRule:
			s.autoApproved++
		case r.Reviewer == review.ReviewerTimeout:
			s.timedOut++
		case r.Approved():
			s.approved++
		default:
			s.rejected++
		}
		if !r.Approved() {
			s.byReason[cmp.Or(r.Reason, store.FilterReasonRejected)]++
		}
		if r.Reviewer != review.ReviewerPerson {
			continue
		}
		s.times = append(s.times, r.TimeToDecision)
		if r.Approved() && len(r.Edited) > 0 {
			s.editedApprovals++
		}
		for _, p := range r.Edited {
			s.edited[p]++
		}
		if (!r.Approved() || len(r.Edited) > 0) && items[r.Delivery] != "" {
			s.samples = appendSample(s.samples, items[r.Delivery])
		}
	}
	for _, o := range d.Samples {
		if d.inWindow(o.TS) {
			stats(o.Step).agreement[o.Agreement]++
		}
	}
	var out []store.Insight
	for _, step := range slices.Sorted(maps.Keys(steps)) {
		s := steps[step]
		decisions := s.approved + s.rejected + s.autoApproved + s.timedOut
		ev := map[string]any{
			"decisions": map[string]int{"approved": s.approved, "rejected": s.rejected, "autoApproved": s.autoApproved,
				"timedOut": s.timedOut},
			"rejectionsByReason": s.byReason, "rejectionRate": nil, "autoApprovalRate": nil, "editRate": nil,
			"editHotSpots": hotSpots(s.edited), "timeToDecisionP50Seconds": nil, "timeToDecisionP95Seconds": nil,
			"samplesReviewed": s.agreement, "sampledAgreement": nil, "interventionRate": nil,
			"samples": nonNil(s.samples),
		}
		if decisions > 0 {
			rejected := 0
			for _, n := range s.byReason {
				rejected += n
			}
			ev["rejectionRate"] = round(float64(rejected) / float64(decisions))
			ev["autoApprovalRate"] = round(float64(s.autoApproved) / float64(decisions))
		}
		if s.approved > 0 { // a person's approvals
			ev["editRate"] = round(float64(s.editedApprovals) / float64(s.approved))
		}
		if len(s.times) > 0 {
			ev["timeToDecisionP50Seconds"] = seconds(percentile(s.times, 0.5))
			ev["timeToDecisionP95Seconds"] = seconds(percentile(s.times, 0.95))
		}
		if n := max(reached[step], decisions); n > 0 {
			ev["interventionRate"] = round(float64(s.approved+s.rejected) / float64(n))
		}
		severity := SeverityInfo
		if agree, disagree := s.agreement[store.SampleAgree], s.agreement[store.SampleDisagree]; agree+disagree > 0 {
			share := float64(agree) / float64(agree+disagree)
			ev["sampledAgreement"] = round(share)
			if share < minAgreement {
				severity = SeverityWarning
			}
		}
		out = append(out, d.insight(pipeline.InsightReviewPattern, step, severity, ev, step))
	}
	return out
}

// hotSpot is a pointer reviewers edited, with how many decisions edited it.
type hotSpot struct {
	Pointer string `json:"pointer"`
	Edits   int    `json:"edits"`
}

// hotSpots returns the most edited pointers, most edits first, at most maxHotSpots.
func hotSpots(edited map[string]int) []hotSpot {
	out := make([]hotSpot, 0, len(edited))
	for p, n := range edited {
		out = append(out, hotSpot{Pointer: p, Edits: n})
	}
	slices.SortFunc(out, func(a, b hotSpot) int {
		return cmp.Or(cmp.Compare(b.Edits, a.Edits), cmp.Compare(a.Pointer, b.Pointer))
	})
	return out[:min(len(out), maxHotSpots)]
}
