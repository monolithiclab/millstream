package sqlite

import (
	"encoding/json"
	"fmt"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// newItem is an item to publish.
type newItem struct {
	id, stream, step, key string
	parents               []string
	payload               payload
}

// publish inserts an item under the first-commit rule (spec section 5.2) and creates the deliveries of every step
// of rev reading its stream. An existing item with the same payload hash is a no-op; with a different one, the
// existing item is kept and a nondeterministicOutput event recorded.
func (w *wtx) publish(rev *revision, run *runRow, it newItem) error {
	parents := it.parents
	if parents == nil {
		parents = []string{}
	}
	parentsJSON, err := json.Marshal(parents)
	if err != nil {
		return fmt.Errorf("encode parents: %w", err)
	}
	hash := hashPayload(it.payload.raw)
	n, err := w.affected(`INSERT INTO items
		(pipeline, id, run_id, stream, step, key, parents, generation, ts, payload, payload_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		run.pipeline, it.id, run.id, it.stream, it.step, it.key, string(parentsJSON), run.generation, w.ts(),
		string(it.payload.raw), hash)
	if err != nil {
		return err
	}
	if n == 1 {
		err = w.event(run.pipeline, run.id, store.EventItemPublished,
			map[string]any{"itemId": it.id, "stream": it.stream, "step": it.step})
	} else {
		err = w.firstCommit(run, it, hash)
	}
	if err != nil {
		return err
	}
	return w.createDeliveries(rev, run, it.stream, it.id)
}

func (w *wtx) firstCommit(run *runRow, it newItem, hash string) error {
	var existing string
	if err := w.queryRow(`SELECT payload_hash FROM items WHERE pipeline = ? AND id = ?`, run.pipeline, it.id).
		Scan(&existing); err != nil {
		return notFound(err, "item "+it.id)
	}
	if existing == hash {
		return nil
	}
	return w.event(run.pipeline, run.id, store.EventNondeterministicOutput,
		map[string]any{"itemId": it.id, "stream": it.stream, "step": it.step})
}

// createDeliveries creates a pending delivery of item for every step of rev reading stream, unique on (step,
// item, generation).
func (w *wtx) createDeliveries(rev *revision, run *runRow, stream, itemID string) error {
	for _, step := range rev.consumers[stream] {
		_, err := w.exec(`INSERT INTO deliveries
			(pipeline, id, run_id, step, item_id, generation, state, attempt, next_attempt_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?) ON CONFLICT DO NOTHING`,
			run.pipeline, ids.Delivery(step, itemID, run.generation), run.id, step, itemID, run.generation,
			store.DeliveryPending, w.ts())
		if err != nil {
			return err
		}
	}
	return nil
}

// deadLetter publishes a dead letter (spec section 5.6) on the step's dlq stream: an item whose payload carries
// the kind, origin and error, with item as the dead input or the unrouted output. outputID is empty for a dead
// delivery. A dead letter that exists already (the same delivery dead again after a replay) is reopened.
func (w *wtx) deadLetter(rev *revision, run *runRow, step pipeline.Step, kind store.DeadLetterKind,
	deliveryID, outputID, parent, errText string, item any,
) error {
	stream := step.DLQ
	id := ids.DeadLetter(stream, deliveryID, outputID)
	n, err := w.affected(`UPDATE dead_letters SET state = ?, error = ? WHERE pipeline = ? AND id = ?`,
		store.DeadLetterOpen, errText, run.pipeline, id)
	if err != nil {
		return err
	}
	if n == 0 {
		body, err := json.Marshal(map[string]any{
			"kind": kind, "originStep": step.Name, "originDelivery": deliveryID, "error": errText, "item": item,
		})
		if err != nil {
			return fmt.Errorf("encode dead letter: %w", err)
		}
		pl, err := canonical(body)
		if err != nil {
			return err
		}
		err = w.publish(rev, run, newItem{id: id, stream: stream, step: step.Name, parents: []string{parent},
			payload: pl})
		if err != nil {
			return err
		}
		_, err = w.exec(`INSERT INTO dead_letters
			(pipeline, id, run_id, origin_delivery, origin_step, kind, state, stream, error, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			run.pipeline, id, run.id, deliveryID, step.Name, kind, store.DeadLetterOpen, stream, errText, w.ts())
		if err != nil {
			return err
		}
	}
	return w.event(run.pipeline, run.id, store.EventDeadLetter, map[string]any{
		"deadLetterId": id, "kind": kind, "stream": stream, "originDelivery": deliveryID, "reopened": n > 0,
	})
}

// settle settles a run once every delivery it owns is terminal (spec section 5.4): succeeded without open or
// discarded dead letters, completedWithErrors with some, and a cancelled run stays cancelled.
func (w *wtx) settle(pipelineName, runID string) error {
	run, err := w.run(pipelineName, runID)
	if err != nil || run.settled {
		return err
	}
	var live int
	if err := w.queryRow(`SELECT COUNT(*) FROM deliveries WHERE pipeline = ? AND run_id = ?
		AND state NOT IN (?, ?, ?, ?)`, pipelineName, runID,
		store.DeliveryDone, store.DeliveryFiltered, store.DeliveryDead, store.DeliveryCancelled).Scan(&live); err != nil {
		return fmt.Errorf("count live deliveries: %w", err)
	}
	if live > 0 {
		return nil
	}
	state := run.state
	if state != store.RunCancelled {
		var errs int
		if err := w.queryRow(`SELECT COUNT(*) FROM dead_letters WHERE pipeline = ? AND run_id = ? AND state IN (?, ?)`,
			pipelineName, runID, store.DeadLetterOpen, store.DeadLetterDiscarded).Scan(&errs); err != nil {
			return fmt.Errorf("count dead letters: %w", err)
		}
		state = store.RunSucceeded
		if errs > 0 {
			state = store.RunCompletedWithErrors
		}
	}
	if _, err := w.exec(`UPDATE runs SET state = ?, settled_at = ? WHERE pipeline = ? AND id = ?`,
		state, w.ts(), pipelineName, runID); err != nil {
		return err
	}
	return w.event(pipelineName, runID, store.EventRunSettled, map[string]any{"state": state})
}

// addCost adds a delivery's reported cost to the delivery and its run.
func (w *wtx) addCost(d *deliveryRow, cost float64) error {
	if cost == 0 {
		return nil
	}
	if _, err := w.exec(`UPDATE deliveries SET cost_usd = cost_usd + ? WHERE pipeline = ? AND id = ?`,
		cost, d.pipeline, d.id); err != nil {
		return err
	}
	_, err := w.exec(`UPDATE runs SET cost_usd = cost_usd + ? WHERE pipeline = ? AND id = ?`,
		cost, d.pipeline, d.runID)
	return err
}
