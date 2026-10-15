package insights

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Attempt is the end of one attempt of a delivery, read from its event (spec section 10.1): done, retry, dead,
// filtered or cancelled.
type Attempt struct {
	TS       time.Time
	RunID    string
	Step     string
	Delivery string
	Attempt  int
	// Outcome is the event kind: store.EventDeliveryDone, EventDeliveryRetry, EventDeliveryDead,
	// EventDeliveryFiltered or EventDeliveryCancelled.
	Outcome string
	// Class and Error are a failed attempt's error class and message, the message at most 512 bytes.
	Class string
	Error string
	// Outputs is how many outputs a done attempt published.
	Outputs int
	CostUSD float64
	Metrics []store.Metric
}

// Failed reports whether the attempt failed: retried or dead.
func (a Attempt) Failed() bool {
	return a.Outcome == store.EventDeliveryRetry || a.Outcome == store.EventDeliveryDead
}

// attemptKinds are the events that end an attempt.
var attemptKinds = []string{store.EventDeliveryDone, store.EventDeliveryRetry, store.EventDeliveryDead,
	store.EventDeliveryFiltered, store.EventDeliveryCancelled}

// published is an item a run published, read from its itemPublished event.
type published struct {
	RunID, Stream string
}

// readAttempts returns the attempts of a pipeline that ended at or after since, and the items it published since.
func readAttempts(ctx context.Context, st store.Store, pipelineName string, since time.Time) ([]Attempt,
	[]published, error,
) {
	evs, err := st.PipelineEvents(ctx, store.EventFilter{
		Pipeline: pipelineName, Kinds: append([]string{store.EventItemPublished}, attemptKinds...), Since: since,
	})
	if err != nil {
		return nil, nil, err
	}
	var as []Attempt
	var items []published
	for _, ev := range evs {
		var d struct {
			DeliveryID string         `json:"deliveryId"`
			Step       string         `json:"step"`
			Stream     string         `json:"stream"`
			Attempt    int            `json:"attempt"`
			Class      string         `json:"class"`
			Error      string         `json:"error"`
			Outputs    int            `json:"outputs"`
			CostUSD    float64        `json:"costUsd"`
			Metrics    []store.Metric `json:"metrics"`
		}
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			return nil, nil, fmt.Errorf("event %d: %w", ev.Seq, err)
		}
		if ev.Kind == store.EventItemPublished {
			items = append(items, published{RunID: ev.RunID, Stream: d.Stream})
			continue
		}
		as = append(as, Attempt{
			TS: ev.TS, RunID: ev.RunID, Step: d.Step, Delivery: d.DeliveryID, Attempt: d.Attempt, Outcome: ev.Kind,
			Class: d.Class, Error: d.Error, Outputs: d.Outputs, CostUSD: d.CostUSD, Metrics: d.Metrics,
		})
	}
	return as, items, nil
}
