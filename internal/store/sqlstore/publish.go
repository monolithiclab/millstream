package sqlstore

import (
	"cmp"
	"encoding/json"
	"fmt"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// newItem is an item to publish.
type newItem struct {
	id, stream, step, key string
	// parentPipeline is the pipeline of the parents of a copy published through an import, empty otherwise.
	parentPipeline string
	parents        []string
	payload        payload
	artifacts      []store.ArtifactRef
}

// publish inserts an item under the first-commit rule (spec section 5.2) and creates the deliveries of every step
// of rev reading its stream. An existing item with the same payload hash is a no-op; with a different one, the
// existing item is kept and a nondeterministicOutput event recorded. A new item on a stream other pipelines import
// is copied to each of them in the same transaction (the Stream import unit, spec section 4.12); an existing one
// was copied when it was inserted.
func (w *wtx) publish(rev *revision, run *runRow, it newItem) error {
	parents := it.parents
	if parents == nil {
		parents = []string{}
	}
	parentsJSON, err := json.Marshal(parents)
	if err != nil {
		return fmt.Errorf("encode parents: %w", err)
	}
	artifacts := it.artifacts
	if artifacts == nil {
		artifacts = []store.ArtifactRef{}
	}
	artifactsJSON, err := json.Marshal(artifacts)
	if err != nil {
		return fmt.Errorf("encode artifacts: %w", err)
	}
	hash := hashPayload(it.payload.raw)
	n, err := w.affected(`INSERT INTO items
		(pipeline, id, run_id, stream, step, key, parents, generation, ts, payload, payload_hash, artifacts,
		parent_pipeline) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		run.pipeline, it.id, run.id, it.stream, it.step, it.key, string(parentsJSON), run.generation, w.ts(),
		string(it.payload.raw), hash, string(artifactsJSON), it.parentPipeline)
	if err != nil {
		return err
	}
	if n == 1 {
		if err := w.countArtifacts(run.pipeline, artifacts); err != nil {
			return err
		}
		if err := w.linkParents(run.pipeline, it); err != nil {
			return err
		}
		err = w.event(run.pipeline, run.id, store.EventItemPublished,
			map[string]any{"itemId": it.id, "stream": it.stream, "step": it.step})
	} else {
		var tombstoned bool
		if tombstoned, err = w.firstCommit(run, it, hash); err == nil && tombstoned {
			return nil // its payload is gone: there is nothing to deliver (spec section 9.3)
		}
	}
	if err != nil {
		return err
	}
	if err := w.createDeliveries(rev, run, it.stream, it.id); err != nil {
		return err
	}
	if n == 1 {
		return w.importCopies(run, it)
	}
	return nil
}

// countArtifacts adds one reference to each artifact a newly inserted item carries, creating its entry on the first.
// An existing item republished by a retry is not counted again; artifacts are not part of the payload hash, so
// under the first-commit rule the first attempt's artifacts are the ones kept.
func (w *wtx) countArtifacts(pipelineName string, refs []store.ArtifactRef) error {
	for _, a := range refs {
		if _, err := w.exec(`INSERT INTO artifacts (pipeline, sha256, size, type, refcount, created_at)
			VALUES (?, ?, ?, ?, 1, ?) ON CONFLICT (pipeline, sha256) DO UPDATE SET refcount = artifacts.refcount + 1`,
			pipelineName, a.SHA256, a.Size, a.Type, w.ts()); err != nil {
			return fmt.Errorf("count artifact %s: %w", a.SHA256, err)
		}
	}
	return nil
}

// firstCommit applies the first-commit rule to an item published again (spec section 5.2): the same payload is a
// no-op, another one records a nondeterministicOutput event. It reports whether the existing item is tombstoned
// (spec section 9.3), which a back-fill finding a copy the consumer let expire or erased may meet: nothing is
// compared or delivered then.
func (w *wtx) firstCommit(run *runRow, it newItem, hash string) (tombstoned bool, err error) {
	var existing, tombstone string
	if err := w.queryRow(`SELECT payload_hash, tombstone FROM items WHERE pipeline = ? AND id = ?`, run.pipeline,
		it.id).Scan(&existing, &tombstone); err != nil {
		return false, notFound(err, "item "+it.id)
	}
	if tombstone != "" {
		return true, nil
	}
	if existing == hash {
		return false, nil
	}
	return false, w.event(run.pipeline, run.id, store.EventNondeterministicOutput,
		map[string]any{"itemId": it.id, "stream": it.stream, "step": it.step})
}

// createDeliveries creates a pending delivery of item for every step of rev reading stream, unique on (step,
// item, generation).
func (w *wtx) createDeliveries(rev *revision, run *runRow, stream, itemID string) error {
	for _, step := range rev.consumers[stream] {
		_, err := w.exec(`INSERT INTO deliveries
			(pipeline, id, run_id, step, item_id, generation, state, attempt, next_attempt_at, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?) ON CONFLICT DO NOTHING`,
			run.pipeline, ids.Delivery(step, itemID, run.generation), run.id, step, itemID, run.generation,
			store.DeliveryPending, w.ts(), w.ts())
		if err != nil {
			return err
		}
	}
	return nil
}

// deadLetter publishes a dead letter (spec section 5.6) on the step's dlq stream: an item whose payload carries
// the kind, origin and error, with item as the dead input or the unrouted output. outputID is empty for a dead
// delivery. Its id includes the number of the origin delivery's earlier dead letters, so a delivery that fails
// again after a replay publishes a new dead letter and the replayed one stays replayed.
func (w *wtx) deadLetter(rev *revision, run *runRow, step pipeline.Step, kind store.DeadLetterKind,
	deliveryID, outputID, parent, errText string, item any,
) error {
	stream := step.DLQ
	var seq int
	if err := w.queryRow(`SELECT COUNT(*) FROM dead_letters WHERE pipeline = ? AND origin_delivery = ?`,
		run.pipeline, deliveryID).Scan(&seq); err != nil {
		return fmt.Errorf("count dead letters of %s: %w", deliveryID, err)
	}
	id := ids.DeadLetter(stream, deliveryID, outputID, seq)
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
	err = w.publish(rev, run, newItem{id: id, stream: stream, step: step.Name, parents: []string{parent}, payload: pl})
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
	return w.event(run.pipeline, run.id, store.EventDeadLetter, map[string]any{
		"deadLetterId": id, "kind": kind, "stream": stream, "originDelivery": deliveryID, "originStep": step.Name,
		"seq": seq,
	})
}

// notTerminal is the condition on a delivery's state that it is not terminal (spec section 5.4); its arguments
// are terminalStates.
const notTerminal = "state NOT IN (?, ?, ?, ?)"

var terminalStates = []any{store.DeliveryDone, store.DeliveryFiltered, store.DeliveryDead, store.DeliveryCancelled}

// settle settles a run once every delivery it owns is terminal and, for an import run that was not cancelled, its
// producer run is settled (spec section 5.4): succeeded without open or discarded dead letters, completedWithErrors
// with some, and a cancelled run stays cancelled. A run not yet settled is waitingReview while only parked reviews
// remain, running otherwise. Settling a run settles the import runs it was holding open.
func (w *wtx) settle(pipelineName, runID string) error {
	run, err := w.run(pipelineName, runID)
	if err != nil || run.settled {
		return err
	}
	var live, parked int
	if err := w.queryRow(`SELECT COUNT(*), COALESCE(SUM(CASE WHEN state = ? THEN 1 ELSE 0 END), 0) FROM deliveries
		WHERE pipeline = ? AND run_id = ? AND `+notTerminal,
		append([]any{store.DeliveryAwaitingReview, pipelineName, runID}, terminalStates...)...).
		Scan(&live, &parked); err != nil {
		return fmt.Errorf("count live deliveries: %w", err)
	}
	if live > 0 {
		return w.waitingReview(run, live == parked)
	}
	state := run.state
	if state != store.RunCancelled {
		// An import run stays open while its producer run is: more items may arrive (spec section 4.12).
		open, onlyReviews, err := w.producerHolds(run)
		if err != nil {
			return err
		}
		if open {
			return w.waitingReview(run, onlyReviews)
		}
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
	var cost float64
	if err := w.queryRow(`UPDATE runs SET state = ?, settled_at = ? WHERE pipeline = ? AND id = ? RETURNING cost_usd`,
		state, w.ts(), pipelineName, runID).Scan(&cost); err != nil {
		return fmt.Errorf("settle run %s: %w", runID, err)
	}
	if err := w.event(pipelineName, runID, store.EventRunSettled, map[string]any{
		"state": state, "durationMs": millis(w.now.Sub(run.startedAt)), "costUsd": cost,
	}); err != nil {
		return err
	}
	return w.settleConsumers(run)
}

// waitingReview moves a live run between running and waitingReview: waitingReview while only parked reviews remain
// (spec section 5.4), or, for an import run with nothing of its own left, while its producer run waits only on
// reviews. A cancelled run stays cancelled.
func (w *wtx) waitingReview(run *runRow, onlyReviews bool) error {
	want := store.RunRunning
	if onlyReviews {
		want = store.RunWaitingReview
	}
	if run.state == want || run.state == store.RunCancelled {
		return nil
	}
	if _, err := w.exec(`UPDATE runs SET state = ? WHERE pipeline = ? AND id = ?`, want, run.pipeline, run.id); err != nil {
		return err
	}
	run.state = want
	return w.settleConsumers(run) // the import runs it holds open, with nothing of their own, follow it
}

// addCost charges a delivery's attempt its cost, on the delivery, its run and its pipeline's UTC day, and clears
// the attempt's budget reservation (spec section 5.9).
func (w *wtx) addCost(d *deliveryRow, cost float64) error {
	if d.reserved != 0 {
		if _, err := w.exec(`UPDATE deliveries SET reserved_usd = 0 WHERE pipeline = ? AND id = ?`,
			d.pipeline, d.id); err != nil {
			return err
		}
		d.reserved = 0
	}
	if cost == 0 {
		return nil
	}
	if _, err := w.exec(`UPDATE deliveries SET cost_usd = cost_usd + ? WHERE pipeline = ? AND id = ?`,
		cost, d.pipeline, d.id); err != nil {
		return err
	}
	if _, err := w.exec(`UPDATE runs SET cost_usd = cost_usd + ? WHERE pipeline = ? AND id = ?`,
		cost, d.pipeline, d.runID); err != nil {
		return err
	}
	_, err := w.exec(`INSERT INTO budget_days (pipeline, day, spent_usd) VALUES (?, ?, ?)
		ON CONFLICT (pipeline, day) DO UPDATE SET spent_usd = budget_days.spent_usd + excluded.spent_usd`,
		d.pipeline, w.day(), cost)
	return err
}

// linkParents records the lineage's edges of an item just inserted in pipelineName (item_parents), its parents in its
// own pipeline, or a copy's in the producer's.
func (w *wtx) linkParents(pipelineName string, it newItem) error {
	if len(it.parents) == 0 {
		return nil
	}
	if _, err := w.exec(`INSERT INTO item_parents (pipeline, parent_id, child_pipeline, child_id)
		SELECT CAST(? AS TEXT), value, CAST(? AS TEXT), CAST(? AS TEXT) FROM (`+w.s.d.JSONParam(false)+`) l
		WHERE true ON CONFLICT DO NOTHING`, cmp.Or(it.parentPipeline, pipelineName), pipelineName, it.id,
		jsonList(it.parents)); err != nil {
		return fmt.Errorf("link item %s to its parents: %w", it.id, err)
	}
	return nil
}
