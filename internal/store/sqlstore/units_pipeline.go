package sqlstore

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
		// Numbered after the last revision, as events are: a revision rolled back gives its number back, where a
		// Postgres sequence would leave a gap and number the next one differently after a crash.
		rev, err := w.insertKey(`INSERT INTO revisions (id, pipeline, package_hash, pipeline_json, applied_at, message,
			state) VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM revisions), ?, ?, ?, ?, ?, 'active')`, "id",
			name, pkg.Hash, string(data), w.ts(), pkg.Message)
		if err != nil {
			return fmt.Errorf("insert revision: %w", err)
		}
		id = store.RevisionID(rev)
		if _, err := w.exec(`INSERT INTO pipelines (id, active_revision, state) VALUES (?, ?, 'active')
			ON CONFLICT (id) DO UPDATE SET active_revision = excluded.active_revision`, name, id); err != nil {
			return err
		}
		for stepName, step := range pkg.Pipeline.Steps {
			if !step.Paused {
				continue
			}
			if _, err := w.exec(`INSERT INTO steps_state (pipeline, step, paused) VALUES (?, ?, TRUE)
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
	source, scheduledAt := f.Source, any(nil)
	switch source {
	case "", store.SourceManual:
		source = store.SourceManual
	case store.SourceCron:
		if f.ScheduledAt.IsZero() {
			return false, fmt.Errorf("fire: a cron firing needs its scheduled time: %w", store.ErrInvalid)
		}
		scheduledAt = formatTime(f.ScheduledAt)
	default:
		return false, fmt.Errorf("fire: unknown trigger source %q: %w", source, store.ErrInvalid)
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
		state, err := w.pipelineState(f.Pipeline)
		if err != nil {
			return err
		}
		if state == "disabled" {
			return fmt.Errorf("pipeline %s is disabled: its triggers do not fire until it is resumed: %w", f.Pipeline,
				store.ErrConflict)
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
		if _, err := w.exec(`INSERT INTO runs (pipeline, id, revision, trigger, trigger_key, trigger_source,
			scheduled_at, kind, generation, vars, var_sources, state, started_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.Pipeline, f.RunID, revID, f.Trigger, f.Key, source, scheduledAt, kind, f.Generation, string(vars),
			string(sources), store.RunRunning, w.ts()); err != nil {
			return err
		}
		run := &runRow{pipeline: f.Pipeline, id: f.RunID, generation: f.Generation}
		stream := pipeline.TriggerStreamPrefix + f.Trigger
		if err := w.event(f.Pipeline, f.RunID, store.EventRunStarted, map[string]any{
			"trigger": f.Trigger, "key": f.Key, "source": source, "revision": revID, "kind": kind,
			"generation": f.Generation,
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

// SetPaused pauses or resumes a step, or the whole pipeline when step is empty. Resuming the pipeline also resumes
// the steps a budget paused, and enables a disabled pipeline; a step a user paused stays paused until resumed by
// name. A disabled pipeline is not paused: it is resumed first.
func (s *Store) SetPaused(ctx context.Context, pipelineName, step string, paused bool) error {
	state, kind := "active", store.EventResumed
	if paused {
		state, kind = "paused", store.EventPaused
	}
	return s.submit(ctx, unitSetPaused, func(w *wtx) error {
		current, err := w.pipelineState(pipelineName)
		if err != nil {
			return err
		}
		if paused && step == "" && current == "disabled" {
			return fmt.Errorf("pipeline %s is disabled: resume it before pausing it: %w", pipelineName, store.ErrConflict)
		}
		switch {
		case step != "":
			_, err = w.exec(`INSERT INTO steps_state (pipeline, step, paused, cause) VALUES (?, ?, ?, '')
				ON CONFLICT (pipeline, step) DO UPDATE SET paused = excluded.paused, cause = ''`, pipelineName, step, paused)
		case paused:
			_, err = w.exec(`UPDATE pipelines SET state = ? WHERE id = ?`, state, pipelineName)
		default: // resuming the pipeline also lifts the pauses a budget made (spec section 7.5)
			if _, err = w.exec(`UPDATE pipelines SET state = ? WHERE id = ?`, state, pipelineName); err == nil {
				_, err = w.exec(`UPDATE steps_state SET paused = FALSE, cause = '' WHERE pipeline = ? AND cause != ''`,
					pipelineName)
			}
		}
		if err != nil {
			return err
		}
		return w.event(pipelineName, "", kind, map[string]any{"step": step})
	})
}

// pipelineState reads a pipeline's state: active, paused or disabled.
func (w *wtx) pipelineState(pipelineName string) (string, error) {
	var state string
	if err := w.queryRow(`SELECT state FROM pipelines WHERE id = ?`, pipelineName).Scan(&state); err != nil {
		return "", notFound(err, "pipeline "+pipelineName)
	}
	return state, nil
}

// Disable is the Disable unit: the pipeline's triggers stop firing and its deliveries wait until it is resumed.
func (s *Store) Disable(ctx context.Context, pipelineName string) error {
	return s.submit(ctx, unitDisable, func(w *wtx) error {
		state, err := w.pipelineState(pipelineName)
		if err != nil || state == "disabled" {
			return err
		}
		if _, err := w.exec(`UPDATE pipelines SET state = 'disabled' WHERE id = ?`, pipelineName); err != nil {
			return err
		}
		return w.event(pipelineName, "", store.EventDisabled, map[string]any{})
	})
}

// Rollback is the Rollback unit: it makes an earlier revision of the pipeline its active one again. Runs keep the
// revision they started on; the runs started from now on take the one reactivated.
func (s *Store) Rollback(ctx context.Context, pipelineName string, to store.RevisionID) (store.RevisionID, error) {
	var id store.RevisionID
	err := s.submit(ctx, unitRollback, func(w *wtx) error {
		var active store.RevisionID
		if err := w.queryRow(`SELECT active_revision FROM pipelines WHERE id = ?`, pipelineName).Scan(&active); err != nil {
			return notFound(err, "pipeline "+pipelineName)
		}
		id = to
		if id == 0 {
			err := w.queryRow(`SELECT id FROM revisions WHERE pipeline = ? AND id < ? ORDER BY id DESC LIMIT 1`,
				pipelineName, active).Scan(&id)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("pipeline %s has no revision older than the active one, %d: %w", pipelineName, active,
					store.ErrConflict)
			}
			if err != nil {
				return err
			}
		}
		var one int
		if err := w.queryRow(`SELECT 1 FROM revisions WHERE id = ? AND pipeline = ?`, id, pipelineName).
			Scan(&one); err != nil {
			return notFound(err, fmt.Sprintf("revision %d of %s", id, pipelineName))
		}
		if id == active {
			return fmt.Errorf("revision %d is already the active revision of %s: %w", id, pipelineName, store.ErrConflict)
		}
		if _, err := w.exec(`UPDATE revisions SET state = 'superseded' WHERE pipeline = ? AND state = 'active'`,
			pipelineName); err != nil {
			return err
		}
		if _, err := w.exec(`UPDATE revisions SET state = 'active' WHERE id = ?`, id); err != nil {
			return err
		}
		if _, err := w.exec(`UPDATE pipelines SET active_revision = ? WHERE id = ?`, id, pipelineName); err != nil {
			return err
		}
		return w.event(pipelineName, "", store.EventRolledBack, map[string]any{"revision": id, "from": active})
	})
	return id, err
}

func nonNil[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}
	return m
}
