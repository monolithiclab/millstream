package insights

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Drift from fallback audits (spec sections 11.1 and 11.5): when a step's fallback disagrees with its primary on
// more than DriftDisagreement of the audits that ended in the window, the data has drifted away from the rules.
const (
	// DriftDisagreement is the share of disagreeing audits above which a step's drift is a warning.
	DriftDisagreement = 0.05
	// driftMinAudits is how many audits with an agreement a step needs in the window to be judged.
	driftMinAudits = minSamples
)

// AuditOutcome is the end of one fallback audit, read from its auditEnded event.
type AuditOutcome struct {
	TS     time.Time
	Step   string
	ItemID string
	// State is the audit's: done, failed or cancelled. Agreement is set once done: 1 or 0.
	State     string
	Agreement *float64
}

// readAudits returns the audits of a pipeline that ended at or after since.
func readAudits(ctx context.Context, st store.Store, pipelineName string, since time.Time) ([]AuditOutcome, error) {
	evs, err := st.PipelineEvents(ctx, store.EventFilter{
		Pipeline: pipelineName, Kinds: []string{store.EventAuditEnded}, Since: since,
	})
	if err != nil {
		return nil, err
	}
	out := make([]AuditOutcome, 0, len(evs))
	for _, ev := range evs {
		var d struct {
			Step      string   `json:"step"`
			ItemID    string   `json:"itemId"`
			State     string   `json:"state"`
			Agreement *float64 `json:"agreement"`
		}
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			return nil, fmt.Errorf("event %d: %w", ev.Seq, err)
		}
		out = append(out, AuditOutcome{TS: ev.TS, Step: d.Step, ItemID: d.ItemID, State: d.State,
			Agreement: d.Agreement})
	}
	return out, nil
}

// drifts finds, per step, the audits of the window: a warning when the fallback disagreed with the primary on more
// than DriftDisagreement of those with an agreement (at least driftMinAudits), with the disagreeing items as
// samples. A step under the threshold is not an insight.
func drifts(d *Data) []store.Insight {
	type tally struct {
		agreed, disagreed, failed int
		samples                   []string
	}
	steps := map[string]*tally{}
	for _, a := range d.Audits {
		if !d.inWindow(a.TS) {
			continue
		}
		t := steps[a.Step]
		if t == nil {
			t = &tally{}
			steps[a.Step] = t
		}
		switch {
		case a.Agreement == nil:
			t.failed++
		case *a.Agreement == 1:
			t.agreed++
		default:
			t.disagreed++
			t.samples = appendSample(t.samples, a.ItemID)
		}
	}
	var out []store.Insight
	for _, step := range slices.Sorted(maps.Keys(steps)) {
		t := steps[step]
		judged := t.agreed + t.disagreed
		if judged < driftMinAudits {
			continue
		}
		share := float64(t.disagreed) / float64(judged)
		if share <= DriftDisagreement {
			continue
		}
		out = append(out, d.insight(pipeline.InsightDrift, step, SeverityWarning, map[string]any{
			"source": "audits", "audits": judged + t.failed, "agreed": t.agreed, "disagreed": t.disagreed,
			"failed": t.failed, "agreement": round(1 - share), "disagreementShare": round(share),
			"threshold": DriftDisagreement, "samples": nonNil(t.samples),
		}, step, "audits"))
	}
	return out
}
