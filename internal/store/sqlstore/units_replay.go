package sqlstore

import (
	"cmp"
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
	if r.Pipeline == "" || (r.Step == "") == (r.Stream == "") || r.RunID == "" || r.Key == "" {
		return 0, false, fmt.Errorf("start replay: pipeline, a step or a stream, run id and key are required: %w",
			store.ErrInvalid)
	}
	vars, sources, err := encodeVars(r.Vars, r.VarSources)
	if err != nil {
		return 0, false, fmt.Errorf("start replay: %w", err)
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
			return fmt.Errorf("replay of %s.%s selects no item: %w", r.Pipeline, cmp.Or(r.Step, r.Stream), store.ErrInvalid)
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
		var copies []newItem
		if r.Stream != "" {
			if copies, err = w.backfill(rev, r); err != nil {
				return err
			}
		} else {
			step, err := rev.step(r.Step)
			if err != nil {
				return err
			}
			for _, id := range r.Items {
				if err := w.replayable(r.Pipeline, id, step.In); err != nil {
					return err
				}
			}
		}
		if generation, err = w.nextGeneration(r.Pipeline); err != nil {
			return err
		}
		if _, err := w.exec(`INSERT INTO runs (pipeline, id, revision, trigger, trigger_key, trigger_source, kind,
			generation, vars, var_sources, state, started_at) VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.Pipeline, r.RunID, revID, r.Key, store.SourceManual, store.RunKindReplay, generation, vars,
			sources, store.RunRunning, w.ts()); err != nil {
			return err
		}
		data := map[string]any{"kind": store.RunKindReplay, "key": r.Key, "revision": revID, "generation": generation,
			"items": len(r.Items)}
		if r.Stream != "" {
			data["stream"] = r.Stream
		} else {
			data["step"] = r.Step
		}
		if err := w.event(r.Pipeline, r.RunID, store.EventRunStarted, data); err != nil {
			return err
		}
		if err := w.replayDeliveries(rev, r, generation, copies); err != nil {
			return err
		}
		created = true
		return w.settle(r.Pipeline, r.RunID)
	})
	return generation, created, err
}

// replayDeliveries creates the replay run's work: for a step, a delivery of each item selected; for an imported
// stream, its copies, published in the run, which creates the deliveries of every step reading the stream.
func (w *wtx) replayDeliveries(rev *revision, r store.ReplayStart, generation int64, copies []newItem) error {
	if r.Stream != "" {
		run, err := w.run(r.Pipeline, r.RunID)
		if err != nil {
			return err
		}
		for _, c := range copies {
			if err := w.publish(rev, run, c); err != nil {
				return err
			}
		}
		return nil
	}
	for _, id := range r.Items {
		if _, err := w.exec(`INSERT INTO deliveries
			(pipeline, id, run_id, step, item_id, generation, state, attempt, next_attempt_at, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?) ON CONFLICT DO NOTHING`,
			r.Pipeline, ids.Delivery(r.Step, id, generation), r.RunID, r.Step, id, generation,
			store.DeliveryPending, w.ts(), w.ts()); err != nil {
			return err
		}
	}
	return nil
}

// backfill returns the copies a back-fill of an imported stream publishes (spec section 4.12): one per producer
// item selected, under the id the import gave it, so a copy the consumer already holds is found rather than copied
// again. Each producer item must be on the producer's stream and still hold its payload.
func (w *wtx) backfill(rev *revision, r store.ReplayStart) ([]newItem, error) {
	imp := rev.p.Streams[r.Stream].Import
	if imp == nil {
		return nil, fmt.Errorf("pipeline %s does not import a stream %q: %w", r.Pipeline, r.Stream, store.ErrInvalid)
	}
	copies := make([]newItem, 0, len(r.Items))
	for _, id := range r.Items {
		var stream, key, body, hash, artifacts string
		if err := w.queryRow(`SELECT stream, key, payload, payload_hash, artifacts FROM items
			WHERE pipeline = ? AND id = ?`, imp.Pipeline, id).Scan(&stream, &key, &body, &hash, &artifacts); err != nil {
			return nil, notFound(err, "item "+id+" of pipeline "+imp.Pipeline)
		}
		switch {
		case stream != imp.Stream:
			return nil, fmt.Errorf("item %s is on %s, not on %s.%s: %w", id, stream, imp.Pipeline, imp.Stream,
				store.ErrInvalid)
		case hash == tombstoneHash:
			return nil, fmt.Errorf("item %s has expired: its payload is a tombstone and cannot be replayed: %w", id,
				store.ErrConflict)
		}
		pl, err := canonical(json.RawMessage(body))
		if err != nil {
			return nil, fmt.Errorf("item %s: %w", id, err)
		}
		producerItem := newItem{id: id, key: key, payload: pl}
		if err := json.Unmarshal([]byte(artifacts), &producerItem.artifacts); err != nil {
			return nil, fmt.Errorf("item %s artifacts: %w", id, err)
		}
		copies = append(copies, copyOf(r.Stream, imp.Pipeline, producerItem))
	}
	return copies, nil
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
