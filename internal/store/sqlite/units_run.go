package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/store"
)

// Cancel is the Cancel unit (spec section 5.11): the run is cancelled and its pending deliveries, retries
// included, with it. Leased deliveries finish later, and Done or Fail cancels them. Its open dead letters are
// discarded: a cancelled run is never replayed, so they could only be left open for ever. Cancelling a cancelled
// run is a no-op; a settled run cannot be cancelled.
func (s *Store) Cancel(ctx context.Context, pipelineName, runID string) error {
	return s.submit(ctx, unitCancel, func(w *wtx) error {
		run, err := w.run(pipelineName, runID)
		if err != nil {
			return err
		}
		switch {
		case run.state == store.RunCancelled:
			return nil
		case run.settled:
			return fmt.Errorf("run %s is %s: %w", runID, run.state, store.ErrConflict)
		}
		if _, err := w.exec(`UPDATE runs SET state = ? WHERE pipeline = ? AND id = ?`,
			store.RunCancelled, pipelineName, runID); err != nil {
			return err
		}
		n, err := w.affected(`UPDATE deliveries SET state = ?, finished_at = ?
			WHERE pipeline = ? AND run_id = ? AND state IN (?, ?)`,
			store.DeliveryCancelled, w.ts(), pipelineName, runID, store.DeliveryPending, store.DeliveryAwaitingReview)
		if err != nil {
			return err
		}
		if err := w.event(pipelineName, runID, store.EventRunCancelled, map[string]any{"cancelledDeliveries": n}); err != nil {
			return err
		}
		if err := w.cancelReviews(pipelineName, runID); err != nil {
			return err
		}
		if err := w.discardOpenDeadLetters(pipelineName, runID); err != nil {
			return err
		}
		return w.settle(pipelineName, runID)
	})
}

// discardOpenDeadLetters discards the open dead letters of a run being cancelled, each with its event.
func (w *wtx) discardOpenDeadLetters(pipelineName, runID string) error {
	letters, err := w.openDeadLetters(pipelineName, runID, nil)
	if err != nil {
		return err
	}
	for _, dl := range letters {
		if _, err := w.exec(`UPDATE dead_letters SET state = ? WHERE pipeline = ? AND id = ?`,
			store.DeadLetterDiscarded, pipelineName, dl.id); err != nil {
			return err
		}
		if err := w.event(pipelineName, runID, store.EventDeadLetterDiscarded,
			map[string]any{"deadLetterId": dl.id, "reason": "runCancelled"}); err != nil {
			return err
		}
	}
	return nil
}

// deadLetterRow is a dead letter the replay unit acts on.
type deadLetterRow struct {
	id, originDelivery string
	kind               store.DeadLetterKind
}

// ReplayDeadLetters is the Dead-letter replay unit (spec section 5.6). With migrate, the run moves to the
// pipeline's active revision first, recorded as revisionMigrated. A dead letter of kind dead sends its delivery
// back to pending with its retry budget restarted; an unrouted one routes its stored output again and publishes it, or stays
// open if it still matches no route. A run that had settled is reopened.
func (s *Store) ReplayDeadLetters(ctx context.Context, pipelineName, runID string, ids []string, migrate bool) (int,
	error,
) {
	var count int
	err := s.submit(ctx, unitReplay, func(w *wtx) error {
		var err error
		count, err = w.replay(pipelineName, runID, ids, migrate)
		return err
	})
	return count, err
}

func (w *wtx) replay(pipelineName, runID string, ids []string, migrate bool) (int, error) {
	run, err := w.run(pipelineName, runID)
	if err != nil {
		return 0, err
	}
	if run.state == store.RunCancelled {
		return 0, fmt.Errorf("run %s is cancelled: %w", runID, store.ErrConflict)
	}
	if migrate {
		if err := w.migrate(run); err != nil {
			return 0, err
		}
	}
	rev, err := w.revision(run.revision)
	if err != nil {
		return 0, err
	}
	letters, err := w.openDeadLetters(pipelineName, runID, ids)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, dl := range letters {
		replayed, err := w.replayOne(rev, run, dl)
		if err != nil {
			return 0, err
		}
		if !replayed {
			continue
		}
		count++
		if _, err := w.exec(`UPDATE dead_letters SET state = ? WHERE pipeline = ? AND id = ?`,
			store.DeadLetterReplayed, pipelineName, dl.id); err != nil {
			return 0, err
		}
		if err := w.event(pipelineName, runID, store.EventDeadLetterReplayed,
			map[string]any{"deadLetterId": dl.id, "kind": dl.kind, "revision": run.revision}); err != nil {
			return 0, err
		}
	}
	if count > 0 && run.settled {
		if _, err := w.exec(`UPDATE runs SET state = ?, settled_at = NULL WHERE pipeline = ? AND id = ?`,
			store.RunRunning, pipelineName, runID); err != nil {
			return 0, err
		}
		if err := w.event(pipelineName, runID, store.EventRunReopened, map[string]any{"from": run.state}); err != nil {
			return 0, err
		}
	}
	return count, w.settle(pipelineName, runID)
}

// migrate moves the run to its pipeline's active revision. It refuses, with ErrConflict, a revision that lacks the
// step of a delivery still live in the run (spec section 5.7): that delivery would be left with no step to run.
func (w *wtx) migrate(run *runRow) error {
	var active int64
	if err := w.queryRow(`SELECT active_revision FROM pipelines WHERE id = ?`, run.pipeline).Scan(&active); err != nil {
		return notFound(err, "pipeline "+run.pipeline)
	}
	if store.RevisionID(active) == run.revision {
		return nil
	}
	target, err := w.revision(store.RevisionID(active))
	if err != nil {
		return err
	}
	live, err := w.liveSteps(run)
	if err != nil {
		return err
	}
	var missing []string
	for _, step := range live {
		if _, ok := target.p.Steps[step]; !ok {
			missing = append(missing, step)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("revision %d has no step %s, which live deliveries of run %s need: let them settle first: %w",
			active, strings.Join(missing, ", "), run.id, store.ErrConflict)
	}
	if _, err := w.exec(`UPDATE runs SET revision = ? WHERE pipeline = ? AND id = ?`,
		active, run.pipeline, run.id); err != nil {
		return err
	}
	if err := w.event(run.pipeline, run.id, store.EventRevisionMigrated,
		map[string]any{"from": run.revision, "to": active}); err != nil {
		return err
	}
	run.revision = store.RevisionID(active)
	return nil
}

// liveSteps returns the steps of the run's deliveries that are not terminal, sorted.
func (w *wtx) liveSteps(run *runRow) ([]string, error) {
	rows, err := w.query(`SELECT DISTINCT step FROM deliveries WHERE pipeline = ? AND run_id = ? AND `+notTerminal+
		` ORDER BY step`, append([]any{run.pipeline, run.id}, terminalStates...)...)
	if err != nil {
		return nil, err
	}
	var steps []string
	for rows.Next() {
		var step string
		if err := rows.Scan(&step); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan live step: %w", err)
		}
		steps = append(steps, step)
	}
	return steps, closeRows(rows)
}

// openDeadLetters returns the run's open dead letters, all of them or those named; naming one that is not an open
// dead letter of the run is an error.
func (w *wtx) openDeadLetters(pipelineName, runID string, ids []string) ([]deadLetterRow, error) {
	rows, err := w.query(`SELECT id, origin_delivery, kind FROM dead_letters
		WHERE pipeline = ? AND run_id = ? AND state = ? ORDER BY created_at, id`,
		pipelineName, runID, store.DeadLetterOpen)
	if err != nil {
		return nil, err
	}
	var all []deadLetterRow
	for rows.Next() {
		var dl deadLetterRow
		if err := rows.Scan(&dl.id, &dl.originDelivery, &dl.kind); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		all = append(all, dl)
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return all, nil
	}
	var picked []deadLetterRow
	var missing []string
	for _, id := range ids {
		i := slices.IndexFunc(all, func(dl deadLetterRow) bool { return dl.id == id })
		if i < 0 {
			missing = append(missing, id)
			continue
		}
		picked = append(picked, all[i])
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("no open dead letter %s in run %s: %w", strings.Join(missing, ", "), runID,
			store.ErrNotFound)
	}
	return picked, nil
}

// replayOne replays one dead letter and reports whether it was replayed.
func (w *wtx) replayOne(rev *revision, run *runRow, dl deadLetterRow) (bool, error) {
	d, err := w.delivery(run.pipeline, dl.originDelivery)
	if err != nil {
		return false, err
	}
	step, err := rev.step(d.step)
	if err != nil {
		return false, err
	}
	if dl.kind == store.DeadLetterDead {
		// The replayed delivery starts afresh: its timings count from the replay, and its retry budget from its next
		// attempt, numbered on from the dead one (attempt numbers are never reused).
		n, err := w.affected(`UPDATE deliveries SET state = ?, attempt_base = attempt, attempt = attempt + 1,
			next_attempt_at = ?, created_at = ?,
			leased_at = '', finished_at = '', wait_ms = 0, work_ms = 0
			WHERE pipeline = ? AND id = ? AND state = ?`,
			store.DeliveryPending, w.ts(), w.ts(), d.pipeline, d.id, store.DeliveryDead)
		if err != nil {
			return false, err
		}
		if n == 0 {
			return false, fmt.Errorf("delivery %s of dead letter %s is %s, not dead: %w", d.id, dl.id, d.state,
				store.ErrConflict)
		}
		return true, nil
	}
	var body string
	if err := w.queryRow(`SELECT payload FROM items WHERE pipeline = ? AND id = ?`, run.pipeline, dl.id).
		Scan(&body); err != nil {
		return false, notFound(err, "dead letter item "+dl.id)
	}
	var letter struct {
		Item struct {
			Key       string              `json:"key"`
			Index     int                 `json:"index"`
			Payload   json.RawMessage     `json:"payload"`
			Artifacts []store.ArtifactRef `json:"artifacts"`
		} `json:"item"`
	}
	if err := json.Unmarshal([]byte(body), &letter); err != nil {
		return false, fmt.Errorf("decode dead letter %s: %w", dl.id, err)
	}
	o, err := canonical(letter.Item.Payload)
	if err != nil {
		return false, fmt.Errorf("dead letter %s: %w", dl.id, err)
	}
	r, err := rev.route(step, o.value, run.vars, run.exprRun(rev))
	if err != nil {
		return false, fmt.Errorf("route dead letter %s: %w", dl.id, err)
	}
	if r.unrouted {
		return false, nil
	}
	out := store.Output{Key: letter.Item.Key, Artifacts: letter.Item.Artifacts}
	return true, w.publishOutput(rev, run, step, d, out, letter.Item.Index, o, r)
}

// DiscardDeadLetters is the Dead-letter discard unit (spec section 5.11): the open dead letters named become
// discarded, a loss accepted and still counted in their runs' outcome.
func (s *Store) DiscardDeadLetters(ctx context.Context, pipelineName string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, fmt.Errorf("discard dead letters: no ids: %w", store.ErrInvalid)
	}
	var count int
	err := s.submit(ctx, unitDiscard, func(w *wtx) error {
		for _, id := range ids {
			var runID string
			err := w.queryRow(`UPDATE dead_letters SET state = ? WHERE pipeline = ? AND id = ? AND state = ?
				RETURNING run_id`, store.DeadLetterDiscarded, pipelineName, id, store.DeadLetterOpen).Scan(&runID)
			if err != nil {
				return notFound(err, "open dead letter "+id)
			}
			count++
			if err := w.event(pipelineName, runID, store.EventDeadLetterDiscarded,
				map[string]any{"deadLetterId": id}); err != nil {
				return err
			}
			if err := w.settle(pipelineName, runID); err != nil {
				return err
			}
		}
		return nil
	})
	return count, err
}
