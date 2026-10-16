package sqlstore

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
	var w where
	w.eq("action", action)
	out, err := scan(ctx, s.read, `SELECT action, action_digest, case_name, score, at FROM eval_results`+w.String()+
		` ORDER BY at DESC, rowid DESC`, w.args, func(rows *sql.Rows) (store.EvalResult, error) {
		var r store.EvalResult
		var at TimeCol
		if err := rows.Scan(&r.Action, &r.ActionDigest, &r.Case, &r.Score, &at); err != nil {
			return r, fmt.Errorf("scan eval result: %w", err)
		}
		r.At = at.Time
		return r, nil
	})
	if err != nil {
		return nil, fmt.Errorf("eval results: %w", err)
	}
	return out, nil
}
