package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// tombstoneHash is the payload hash of an item whose payload has expired (spec section 9.3): retention, from M4,
// replaces the payload with null and its hash with this marker, which no SHA-256 hex digest can equal. A replay
// refuses such an item rather than replaying it from its tombstone (spec section 5.7).
const tombstoneHash = "tombstone"

// StartReplay is the Start replay unit (spec sections 5.7 and 5.11): a run of kind replay on the revision named (the
// active one for zero), the pipeline's next generation, and a pending delivery of the step for each selected item,
// in that generation. A run already started under the same id is returned as it is, with created false, so a
// request retried after a crash starts nothing new. Every selected item must be on one of the step's input streams
// and still hold its payload.
func (s *Store) StartReplay(ctx context.Context, r store.ReplayStart) (int64, bool, error) {
	if r.Pipeline == "" || r.Step == "" || r.RunID == "" || r.Key == "" {
		return 0, false, fmt.Errorf("start replay: pipeline, step, run id and key are required: %w", store.ErrInvalid)
	}
	vars, err := json.Marshal(nonNil(r.Vars))
	if err != nil {
		return 0, false, fmt.Errorf("start replay: encode vars: %w", err)
	}
	sources, err := json.Marshal(nonNil(r.VarSources))
	if err != nil {
		return 0, false, fmt.Errorf("start replay: encode var sources: %w", err)
	}
	var (
		generation int64
		created    bool
	)
	err = s.submit(ctx, unitStartReplay, func(w *wtx) error {
		var kind store.RunKind
		err := w.queryRow(`SELECT kind, generation FROM runs WHERE pipeline = ? AND id = ?`, r.Pipeline, r.RunID).
			Scan(&kind, &generation)
		switch {
		case err == nil && kind != store.RunKindReplay:
			return fmt.Errorf("run %s exists and is not a replay run: %w", r.RunID, store.ErrConflict)
		case err == nil:
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("read run %s: %w", r.RunID, err)
		}
		if len(r.Items) == 0 {
			return fmt.Errorf("replay of %s.%s selects no item: %w", r.Pipeline, r.Step, store.ErrInvalid)
		}
		revID := r.Revision
		if revID == 0 {
			if err := w.queryRow(`SELECT active_revision FROM pipelines WHERE id = ?`, r.Pipeline).Scan(&revID); err != nil {
				return notFound(err, "pipeline "+r.Pipeline)
			}
		}
		rev, err := w.revision(revID)
		if err != nil {
			return err
		}
		if rev.p.Name != r.Pipeline {
			return fmt.Errorf("revision %d belongs to %s: %w", revID, rev.p.Name, store.ErrInvalid)
		}
		step, err := rev.step(r.Step)
		if err != nil {
			return err
		}
		for _, id := range r.Items {
			if err := w.replayable(r.Pipeline, id, step.In); err != nil {
				return err
			}
		}
		if err := w.queryRow(`INSERT INTO generations (pipeline, next) VALUES (?, 2)
			ON CONFLICT (pipeline) DO UPDATE SET next = next + 1 RETURNING next - 1`, r.Pipeline).
			Scan(&generation); err != nil {
			return fmt.Errorf("allocate a generation: %w", err)
		}
		if _, err := w.exec(`INSERT INTO runs (pipeline, id, revision, trigger, trigger_key, trigger_source, kind,
			generation, vars, var_sources, state, started_at) VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.Pipeline, r.RunID, revID, r.Key, store.SourceManual, store.RunKindReplay, generation, string(vars),
			string(sources), store.RunRunning, w.ts()); err != nil {
			return err
		}
		if err := w.event(r.Pipeline, r.RunID, store.EventRunStarted, map[string]any{
			"kind": store.RunKindReplay, "key": r.Key, "step": r.Step, "revision": revID, "generation": generation,
			"items": len(r.Items),
		}); err != nil {
			return err
		}
		for _, id := range r.Items {
			if _, err := w.exec(`INSERT INTO deliveries
				(pipeline, id, run_id, step, item_id, generation, state, attempt, next_attempt_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?) ON CONFLICT DO NOTHING`,
				r.Pipeline, ids.Delivery(r.Step, id, generation), r.RunID, r.Step, id, generation,
				store.DeliveryPending, w.ts()); err != nil {
				return err
			}
		}
		created = true
		return w.settle(r.Pipeline, r.RunID)
	})
	return generation, created, err
}

// replayable checks that an item exists on one of the streams in and still holds its payload.
func (w *wtx) replayable(pipelineName, id string, in []string) error {
	var stream, hash string
	if err := w.queryRow(`SELECT stream, payload_hash FROM items WHERE pipeline = ? AND id = ?`, pipelineName, id).
		Scan(&stream, &hash); err != nil {
		return notFound(err, "item "+id)
	}
	switch {
	case !slices.Contains(in, stream):
		return fmt.Errorf("item %s is on %s, which the step does not read: %w", id, stream, store.ErrInvalid)
	case hash == tombstoneHash:
		return fmt.Errorf("item %s has expired: its payload is a tombstone and cannot be replayed: %w", id,
			store.ErrConflict)
	}
	return nil
}
