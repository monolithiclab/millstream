package sqlstore

import (
	"context"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// SkipFirings records the skipped scheduled times, each with a firingSkipped event, in one unit. The trigger must
// exist in the pipeline's active revision; a time already recorded is a no-op.
func (s *Store) SkipFirings(ctx context.Context, pipelineName, trigger string, times []time.Time) error {
	if pipelineName == "" || trigger == "" {
		return fmt.Errorf("skip firings: pipeline and trigger are required: %w", store.ErrInvalid)
	}
	if len(times) == 0 {
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
		for _, at := range times {
			n, err := w.affected(`INSERT INTO skipped_firings (pipeline, trigger, scheduled_at) VALUES (?, ?, ?)
				ON CONFLICT DO NOTHING`, pipelineName, trigger, formatTime(at))
			if err != nil {
				return err
			}
			if n == 0 {
				continue
			}
			if err := w.event(pipelineName, "", store.EventFiringSkipped,
				map[string]any{"trigger": trigger, "key": ids.CronKey(at)}); err != nil {
				return err
			}
		}
		return nil
	})
}

// LastFiring returns the latest scheduled time among the trigger's cron runs and skipped firings.
func (s *Store) LastFiring(ctx context.Context, pipelineName, trigger string) (time.Time, error) {
	var at TimeCol
	err := s.read.QueryRowContext(ctx, `SELECT MAX(t) FROM (
		SELECT scheduled_at AS t FROM runs WHERE pipeline = ? AND trigger = ? AND trigger_source = ?
		UNION ALL
		SELECT scheduled_at FROM skipped_firings WHERE pipeline = ? AND trigger = ?) AS f`,
		pipelineName, trigger, store.SourceCron, pipelineName, trigger).Scan(&at)
	if err != nil {
		return time.Time{}, fmt.Errorf("last firing of %s.%s: %w", pipelineName, trigger, err)
	}
	return at.Time, nil
}

// Pipelines returns every pipeline's state, by name.
func (s *Store) Pipelines(ctx context.Context) ([]store.PipelineState, error) {
	return s.pipelineStates(ctx, "")
}
