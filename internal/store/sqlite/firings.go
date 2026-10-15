package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/monolithiclab/millstream/internal/store"
)

// SkipFirings records a firingSkipped event per key, in one unit. The trigger must exist in the pipeline's active
// revision.
func (s *Store) SkipFirings(ctx context.Context, pipelineName, trigger string, keys []string) error {
	if pipelineName == "" || trigger == "" {
		return fmt.Errorf("skip firings: pipeline and trigger are required: %w", store.ErrInvalid)
	}
	if len(keys) == 0 {
		return nil
	}
	return s.submit(ctx, unitSkipFirings, func(w *wtx) error {
		var revID store.RevisionID
		if err := w.queryRow(`SELECT active_revision FROM pipelines WHERE id = ?`, pipelineName).Scan(&revID); err != nil {
			return notFound(err, "pipeline "+pipelineName)
		}
		rev, err := w.revision(revID)
		if err != nil {
			return err
		}
		if _, ok := rev.p.Triggers[trigger]; !ok {
			return fmt.Errorf("pipeline %s has no trigger %q: %w", pipelineName, trigger, store.ErrInvalid)
		}
		for _, key := range keys {
			if err := w.event(pipelineName, "", store.EventFiringSkipped,
				map[string]any{"trigger": trigger, "key": key}); err != nil {
				return err
			}
		}
		return nil
	})
}

// LastFiring returns the greatest key among the trigger's runs and its firingSkipped events. Skipped firings are
// read from the event log: they are rare (catch-up none) and read once per trigger when an engine starts.
func (s *Store) LastFiring(ctx context.Context, pipelineName, trigger string) (string, error) {
	var key sql.NullString
	err := s.read.QueryRowContext(ctx, `SELECT MAX(k) FROM (
		SELECT trigger_key AS k FROM runs WHERE pipeline = ? AND trigger = ? AND kind = ?
		UNION ALL
		SELECT json_extract(data, '$.key') FROM events
			WHERE kind = ? AND pipeline = ? AND json_extract(data, '$.trigger') = ?)`,
		pipelineName, trigger, store.RunKindTrigger, store.EventFiringSkipped, pipelineName, trigger).Scan(&key)
	if err != nil {
		return "", fmt.Errorf("last firing of %s.%s: %w", pipelineName, trigger, err)
	}
	return key.String, nil
}

// Pipelines returns every pipeline's state, by name.
func (s *Store) Pipelines(ctx context.Context) ([]store.PipelineState, error) {
	names, err := scan(ctx, s.read, `SELECT id FROM pipelines ORDER BY id`, nil, func(rows *sql.Rows) (string, error) {
		var name string
		err := rows.Scan(&name)
		return name, err
	})
	if err != nil {
		return nil, fmt.Errorf("pipelines: %w", err)
	}
	out := make([]store.PipelineState, 0, len(names))
	for _, name := range names {
		ps, err := s.PipelineState(ctx, name)
		if err != nil {
			return nil, err
		}
		out = append(out, ps)
	}
	return out, nil
}
