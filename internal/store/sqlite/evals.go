package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/monolithiclab/millstream/internal/store"
)

// RecordEval stores the scores of an eval run in one unit (spec section 7.4).
func (s *Store) RecordEval(ctx context.Context, results []store.EvalResult) error {
	for _, r := range results {
		if r.Action == "" || r.Case == "" || math.IsNaN(r.Score) || r.Score < 0 || r.Score > 1 {
			return fmt.Errorf("record eval: an action, a case and a score from 0 to 1: %w", store.ErrInvalid)
		}
	}
	return s.submit(ctx, unitRecordEval, func(w *wtx) error {
		for _, r := range results {
			if _, err := w.exec(`INSERT INTO eval_results (action, action_digest, case_name, score, at)
				VALUES (?, ?, ?, ?, ?)`, r.Action, r.ActionDigest, r.Case, r.Score, w.ts()); err != nil {
				return err
			}
		}
		return nil
	})
}

// EvalResults returns an action's recorded scores, newest first.
func (s *Store) EvalResults(ctx context.Context, action string) ([]store.EvalResult, error) {
	out, err := scan(ctx, s.read, `SELECT action, action_digest, case_name, score, at FROM eval_results
		WHERE action = ? ORDER BY at DESC, rowid DESC`, []any{action}, func(rows *sql.Rows) (store.EvalResult, error) {
		var r store.EvalResult
		var at string
		if err := rows.Scan(&r.Action, &r.ActionDigest, &r.Case, &r.Score, &at); err != nil {
			return r, fmt.Errorf("scan eval result: %w", err)
		}
		var err error
		r.At, err = parseTime(at)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("eval results: %w", err)
	}
	return out, nil
}
