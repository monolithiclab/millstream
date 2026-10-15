package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// RecordInsights applies one pass of the insights projector: see store.Store.
func (s *Store) RecordInsights(ctx context.Context, r store.InsightReport) error {
	if r.Pipeline == "" {
		return fmt.Errorf("record insights: no pipeline: %w", store.ErrInvalid)
	}
	seen := map[string]bool{}
	for _, in := range r.Insights {
		if in.ID == "" || in.Kind == "" || !slices.Contains(r.Kinds, in.Kind) || seen[in.ID] {
			return fmt.Errorf("record insights: insight %q of kind %q: want a distinct id of an evaluated kind: %w",
				in.ID, in.Kind, store.ErrInvalid)
		}
		if !json.Valid(in.Evidence) {
			return fmt.Errorf("record insights: insight %s: evidence is not JSON: %w", in.ID, store.ErrInvalid)
		}
		seen[in.ID] = true
	}
	return s.submit(ctx, unitRecordInsights, func(w *wtx) error {
		for _, in := range r.Insights {
			if err := w.recordInsight(r.Pipeline, in); err != nil {
				return err
			}
		}
		return w.resolveInsights(r, seen)
	})
}

// recordInsight opens an insight found for the first time, reopens a resolved one, and updates the figures of the
// others, keeping their state.
func (w *wtx) recordInsight(pipelineName string, in store.Insight) error {
	var state store.InsightState
	err := w.queryRow(`SELECT state FROM insights WHERE pipeline = ? AND id = ?`, pipelineName, in.ID).Scan(&state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := w.exec(`INSERT INTO insights (pipeline, id, kind, scope, window_start, window_end, severity,
			evidence_json, state, opened_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			pipelineName, in.ID, in.Kind, in.Scope, formatTime(in.WindowStart), formatTime(in.WindowEnd),
			in.Severity, string(in.Evidence), store.InsightOpen, w.ts(), w.ts()); err != nil {
			return err
		}
		return w.insightEvent(pipelineName, store.EventInsightOpened, in)
	case err != nil:
		return fmt.Errorf("read insight %s: %w", in.ID, err)
	}
	next := state
	if state == store.InsightResolved {
		next = store.InsightOpen
	}
	if _, err := w.exec(`UPDATE insights SET scope = ?, window_start = ?, window_end = ?, severity = ?,
		evidence_json = ?, state = ?, updated_at = ? WHERE pipeline = ? AND id = ?`,
		in.Scope, formatTime(in.WindowStart), formatTime(in.WindowEnd), in.Severity, string(in.Evidence), next, w.ts(),
		pipelineName, in.ID); err != nil {
		return err
	}
	if next != state {
		return w.insightEvent(pipelineName, store.EventInsightOpened, in)
	}
	return nil
}

// resolveInsights resolves the open and acknowledged insights of the evaluated kinds that the pass did not find.
func (w *wtx) resolveInsights(r store.InsightReport, found map[string]bool) error {
	if len(r.Kinds) == 0 {
		return nil
	}
	args := []any{r.Pipeline, store.InsightOpen, store.InsightAcknowledged}
	for _, k := range r.Kinds {
		args = append(args, k)
	}
	rows, err := w.query(`SELECT id, kind, scope, severity FROM insights WHERE pipeline = ? AND state IN (?, ?)
		AND kind IN (?`+strings.Repeat(", ?", len(r.Kinds)-1)+`) ORDER BY id`, args...)
	if err != nil {
		return err
	}
	var gone []store.Insight
	for rows.Next() {
		var in store.Insight
		if err := rows.Scan(&in.ID, &in.Kind, &in.Scope, &in.Severity); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan insight: %w", err)
		}
		if !found[in.ID] {
			gone = append(gone, in)
		}
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	for _, in := range gone {
		if _, err := w.exec(`UPDATE insights SET state = ?, updated_at = ? WHERE pipeline = ? AND id = ?`,
			store.InsightResolved, w.ts(), r.Pipeline, in.ID); err != nil {
			return err
		}
		if err := w.insightEvent(r.Pipeline, store.EventInsightResolved, in); err != nil {
			return err
		}
	}
	return nil
}

func (w *wtx) insightEvent(pipelineName, kind string, in store.Insight) error {
	return w.event(pipelineName, "", kind, map[string]any{
		"insightId": in.ID, "kind": in.Kind, "scope": in.Scope, "severity": in.Severity,
	})
}

// SetInsightState sets an insight's state as a user asks: acknowledged, muted, or open again.
func (s *Store) SetInsightState(ctx context.Context, pipelineName, id string, state store.InsightState) error {
	switch state {
	case store.InsightAcknowledged, store.InsightMuted, store.InsightOpen:
	default:
		return fmt.Errorf("set insight state: %q is not acknowledged, muted or open: %w", state, store.ErrInvalid)
	}
	return s.submit(ctx, unitSetInsightState, func(w *wtx) error {
		n, err := w.affected(`UPDATE insights SET state = ?, updated_at = ? WHERE pipeline = ? AND id = ?`,
			state, w.ts(), pipelineName, id)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("insight %s of %s: %w", id, pipelineName, store.ErrNotFound)
		}
		return nil
	})
}

// Insights returns the insights f selects, by pipeline then most recently updated first.
func (s *Store) Insights(ctx context.Context, f store.InsightFilter) ([]store.Insight, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("state", string(f.State))
	out, err := scan(ctx, s.read, `SELECT pipeline, id, kind, scope, window_start, window_end, severity, evidence_json,
		state, opened_at, updated_at FROM insights`+w.String()+` ORDER BY pipeline, updated_at DESC, id`, w.args,
		func(rows *sql.Rows) (store.Insight, error) {
			var in store.Insight
			var start, end, evidence, opened, updated string
			if err := rows.Scan(&in.Pipeline, &in.ID, &in.Kind, &in.Scope, &start, &end, &in.Severity, &evidence,
				&in.State, &opened, &updated); err != nil {
				return in, fmt.Errorf("scan insight: %w", err)
			}
			in.Evidence = json.RawMessage(evidence)
			return in, parseTimes(map[*time.Time]string{&in.WindowStart: start, &in.WindowEnd: end,
				&in.OpenedAt: opened, &in.UpdatedAt: updated})
		})
	if err != nil {
		return nil, fmt.Errorf("insights: %w", err)
	}
	return out, nil
}

// parseTimes parses each stored time into its destination.
func parseTimes(fields map[*time.Time]string) error {
	for dst, s := range fields {
		t, err := parseTime(s)
		if err != nil {
			return err
		}
		*dst = t
	}
	return nil
}
