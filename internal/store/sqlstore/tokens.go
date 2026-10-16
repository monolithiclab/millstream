package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/monolithiclab/millstream/internal/store"
)

// CreateTriggerTokens is the Trigger tokens unit of an apply: a token hash for each trigger that has none.
func (s *Store) CreateTriggerTokens(ctx context.Context, pipelineName string, hashes map[string]string) ([]string,
	error,
) {
	if len(hashes) == 0 {
		return nil, nil
	}
	var created []string
	err := s.submit(ctx, unitTriggerTokens, func(w *wtx) error {
		created = nil // the unit fills the result only when it applies
		for _, trigger := range slices.Sorted(maps.Keys(hashes)) {
			n, err := w.affected(`INSERT INTO trigger_tokens (pipeline, trigger, hash, created_at) VALUES (?, ?, ?, ?)
				ON CONFLICT (pipeline, trigger) DO NOTHING`, pipelineName, trigger, hashes[trigger], w.ts())
			if err != nil {
				return err
			}
			if n == 0 {
				continue
			}
			created = append(created, trigger)
			if err := w.event(pipelineName, "", store.EventTriggerTokenSet, map[string]any{"trigger": trigger,
				"rotated": false}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create trigger tokens of %s: %w", pipelineName, err)
	}
	return created, nil
}

// RotateTriggerToken is the Trigger tokens unit of a rotation: the webhook trigger's token hash is replaced.
func (s *Store) RotateTriggerToken(ctx context.Context, pipelineName, trigger, hash string) error {
	err := s.submit(ctx, unitTriggerTokens, func(w *wtx) error {
		var revID store.RevisionID
		if err := w.queryRow(`SELECT active_revision FROM pipelines WHERE id = ?`, pipelineName).Scan(&revID); err != nil {
			return notFound(err, "pipeline "+pipelineName)
		}
		rev, err := w.revision(revID)
		if err != nil {
			return err
		}
		t, ok := rev.p.Triggers[trigger]
		switch {
		case !ok:
			return fmt.Errorf("pipeline %s has no trigger %q: %w", pipelineName, trigger, store.ErrNotFound)
		case t.Webhook == nil:
			return fmt.Errorf("trigger %s of %s is not a webhook trigger: only those have a token: %w", trigger,
				pipelineName, store.ErrInvalid)
		}
		if _, err := w.exec(`INSERT INTO trigger_tokens (pipeline, trigger, hash, created_at) VALUES (?, ?, ?, ?)
			ON CONFLICT (pipeline, trigger) DO UPDATE SET hash = excluded.hash, created_at = excluded.created_at`,
			pipelineName, trigger, hash, w.ts()); err != nil {
			return err
		}
		return w.event(pipelineName, "", store.EventTriggerTokenSet, map[string]any{"trigger": trigger, "rotated": true})
	})
	if err != nil {
		return fmt.Errorf("rotate the token of %s.%s: %w", pipelineName, trigger, err)
	}
	return nil
}

// TriggerTokenHash returns a webhook trigger's token hash.
func (s *Store) TriggerTokenHash(ctx context.Context, pipelineName, trigger string) (string, error) {
	var hash string
	err := s.read.QueryRowContext(ctx, `SELECT hash FROM trigger_tokens WHERE pipeline = ? AND trigger = ?`,
		pipelineName, trigger).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("the token of %s.%s: %w", pipelineName, trigger, store.ErrNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("the token of %s.%s: %w", pipelineName, trigger, err)
	}
	return hash, nil
}
