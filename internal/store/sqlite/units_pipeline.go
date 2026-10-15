package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// ApplyRevision stores the package, makes its revision active and starts the steps the pipeline marks paused
// paused, unless their pause state was already set.
func (s *Store) ApplyRevision(ctx context.Context, pkg store.PackageRecord) (store.RevisionID, error) {
	if pkg.Pipeline == nil || pkg.Pipeline.Name == "" || pkg.Hash == "" {
		return 0, fmt.Errorf("apply revision: a pipeline with a name and a package hash: %w", store.ErrInvalid)
	}
	data, err := json.Marshal(pkg.Pipeline)
	if err != nil {
		return 0, fmt.Errorf("apply revision: encode pipeline: %w", err)
	}
	name := pkg.Pipeline.Name
	bundle := pkg.Bundle
	if bundle == nil {
		bundle = []byte{}
	}
	var id store.RevisionID
	err = s.submit(ctx, unitApplyRevision, func(w *wtx) error {
		var activeHash string
		err := w.queryRow(`SELECT p.active_revision, r.package_hash FROM pipelines p
			JOIN revisions r ON r.id = p.active_revision WHERE p.id = ?`, name).Scan(&id, &activeHash)
		switch {
		case err == nil && activeHash == pkg.Hash:
			return nil
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("read active revision: %w", err)
		}
		if _, err := w.exec(`INSERT INTO packages (hash, bundle, created_at) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
			pkg.Hash, bundle, w.ts()); err != nil {
			return err
		}
		if _, err := w.exec(`UPDATE revisions SET state = 'superseded' WHERE pipeline = ? AND state = 'active'`,
			name); err != nil {
			return err
		}
		if err := w.queryRow(`INSERT INTO revisions (pipeline, package_hash, pipeline_json, applied_at, state)
			VALUES (?, ?, ?, ?, 'active') RETURNING id`, name, pkg.Hash, string(data), w.ts()).Scan(&id); err != nil {
			return fmt.Errorf("insert revision: %w", err)
		}
		if _, err := w.exec(`INSERT INTO pipelines (id, active_revision, state) VALUES (?, ?, 'active')
			ON CONFLICT (id) DO UPDATE SET active_revision = excluded.active_revision`, name, id); err != nil {
			return err
		}
		for stepName, step := range pkg.Pipeline.Steps {
			if !step.Paused {
				continue
			}
			if _, err := w.exec(`INSERT INTO steps_state (pipeline, step, paused) VALUES (?, ?, 1)
				ON CONFLICT DO NOTHING`, name, stepName); err != nil {
				return err
			}
		}
		return w.event(name, "", store.EventRevisionApplied, map[string]any{"revision": id, "packageHash": pkg.Hash})
	})
	return id, err
}

// Fire is the Trigger unit (spec section 5.11).
func (s *Store) Fire(ctx context.Context, f store.Firing) (bool, error) {
	if f.Pipeline == "" || f.Trigger == "" || f.Key == "" || f.RunID == "" {
		return false, fmt.Errorf("fire: pipeline, trigger, key and run id are required: %w", store.ErrInvalid)
	}
	pl, err := canonical(f.Payload)
	if err != nil {
		return false, fmt.Errorf("fire: %w", err)
	}
	vars, err := json.Marshal(nonNil(f.Vars))
	if err != nil {
		return false, fmt.Errorf("fire: encode vars: %w", err)
	}
	sources, err := json.Marshal(nonNil(f.VarSources))
	if err != nil {
		return false, fmt.Errorf("fire: encode var sources: %w", err)
	}
	kind := f.Kind
	if kind == "" {
		kind = store.RunKindTrigger
	}
	var created bool
	err = s.submit(ctx, unitFire, func(w *wtx) error {
		var exists int
		err := w.queryRow(`SELECT COUNT(*) FROM runs WHERE pipeline = ? AND id = ?`, f.Pipeline, f.RunID).Scan(&exists)
		if err != nil || exists > 0 {
			return err
		}
		revID := f.Revision
		if revID == 0 {
			if err := w.queryRow(`SELECT active_revision FROM pipelines WHERE id = ?`, f.Pipeline).Scan(&revID); err != nil {
				return notFound(err, "pipeline "+f.Pipeline)
			}
		}
		rev, err := w.revision(revID)
		if err != nil {
			return err
		}
		if rev.p.Name != f.Pipeline {
			return fmt.Errorf("revision %d belongs to %s: %w", revID, rev.p.Name, store.ErrInvalid)
		}
		if _, ok := rev.p.Triggers[f.Trigger]; !ok {
			return fmt.Errorf("pipeline %s has no trigger %q: %w", f.Pipeline, f.Trigger, store.ErrInvalid)
		}
		if _, err := w.exec(`INSERT INTO runs (pipeline, id, revision, trigger, trigger_key, kind, generation, vars,
			var_sources, state, started_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.Pipeline, f.RunID, revID, f.Trigger, f.Key, kind, f.Generation, string(vars), string(sources),
			store.RunRunning, w.ts()); err != nil {
			return err
		}
		run := &runRow{pipeline: f.Pipeline, id: f.RunID, generation: f.Generation}
		stream := pipeline.TriggerStreamPrefix + f.Trigger
		if err := w.event(f.Pipeline, f.RunID, store.EventRunStarted, map[string]any{
			"trigger": f.Trigger, "key": f.Key, "revision": revID, "kind": kind, "generation": f.Generation,
		}); err != nil {
			return err
		}
		err = w.publish(rev, run, newItem{
			id: ids.TriggerItem(f.Trigger, f.Key), stream: stream, step: stream, key: f.Key, payload: pl,
		})
		if err != nil {
			return err
		}
		created = true
		return w.settle(f.Pipeline, f.RunID)
	})
	return created, err
}

// SetPaused pauses or resumes a step, or the whole pipeline when step is empty.
func (s *Store) SetPaused(ctx context.Context, pipelineName, step string, paused bool) error {
	state, kind := "active", store.EventResumed
	if paused {
		state, kind = "paused", store.EventPaused
	}
	return s.submit(ctx, unitSetPaused, func(w *wtx) error {
		var exists int
		if err := w.queryRow(`SELECT COUNT(*) FROM pipelines WHERE id = ?`, pipelineName).Scan(&exists); err != nil {
			return fmt.Errorf("read pipeline: %w", err)
		}
		if exists == 0 {
			return fmt.Errorf("pipeline %s: %w", pipelineName, store.ErrNotFound)
		}
		var err error
		if step == "" {
			_, err = w.exec(`UPDATE pipelines SET state = ? WHERE id = ?`, state, pipelineName)
		} else {
			_, err = w.exec(`INSERT INTO steps_state (pipeline, step, paused) VALUES (?, ?, ?)
				ON CONFLICT DO UPDATE SET paused = excluded.paused`, pipelineName, step, paused)
		}
		if err != nil {
			return err
		}
		return w.event(pipelineName, "", kind, map[string]any{"step": step})
	})
}

func nonNil[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}
	return m
}
