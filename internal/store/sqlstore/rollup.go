package sqlstore

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/metrics"
	"github.com/monolithiclab/millstream/internal/store"
)

// rollupCursor is the projections row holding the last event the rollup read.
const rollupCursor = "rollup"

// rollupKey identifies one rollup row.
type rollupKey struct {
	pipeline, name, labels, minute string
}

// rollupAgg is a rollup row's increment.
type rollupAgg struct {
	count         int64
	sum, min, max float64
}

func (a *rollupAgg) add(v float64) {
	if a.count == 0 {
		a.min, a.max = v, v
	}
	a.count++
	a.sum += v
	a.min, a.max = min(a.min, v), max(a.max, v)
}

// Rollup is the rollup projector's unit: see store.Store.
func (s *Store) Rollup(ctx context.Context) (int, error) {
	var n int
	err := s.submit(ctx, unitRollup, func(w *wtx) error {
		var err error
		n, err = w.rollup()
		return err
	})
	return n, err
}

func (w *wtx) rollup() (int, error) {
	var cursor int64
	if err := w.queryRow(`SELECT seq FROM projections WHERE name = ?`, rollupCursor).Scan(&cursor); err != nil &&
		!errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("read rollup cursor: %w", err)
	}
	rows, err := w.query(`SELECT seq, ts, pipeline, run_id, kind, data FROM events WHERE seq > ? ORDER BY seq`+limitParam,
		cursor, store.RollupBatch)
	if err != nil {
		return 0, err
	}
	acc := map[rollupKey]*rollupAgg{}
	n := 0
	for rows.Next() {
		var ev store.Event
		var ts TimeCol
		var data string
		if err := rows.Scan(&ev.Seq, &ts, &ev.Pipeline, &ev.RunID, &ev.Kind, &data); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan event: %w", err)
		}
		ev.TS, ev.Data = ts.Time, json.RawMessage(data)
		for _, p := range metrics.Points(ev) {
			if err := addPoint(acc, ev.Pipeline, metrics.Minute(ev.TS), p); err != nil {
				_ = rows.Close()
				return 0, err
			}
		}
		cursor = ev.Seq
		n++
	}
	if err := closeRows(rows); err != nil {
		return 0, err
	}
	if err := w.sampleQueues(acc); err != nil {
		return 0, err
	}
	for _, k := range slices.SortedFunc(maps.Keys(acc), compareRollupKeys) {
		a := acc[k]
		if _, err := w.exec(`INSERT INTO metric_rollups (pipeline, name, labels_json, minute, count, sum, min, max)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (pipeline, name, labels_json, minute) DO UPDATE SET
			count = metric_rollups.count + excluded.count, sum = metric_rollups.sum + excluded.sum,
			min = CASE WHEN excluded.min < metric_rollups.min THEN excluded.min ELSE metric_rollups.min END,
			max = CASE WHEN excluded.max > metric_rollups.max THEN excluded.max ELSE metric_rollups.max END`,
			k.pipeline, k.name, k.labels, k.minute, a.count, a.sum, a.min, a.max); err != nil {
			return 0, fmt.Errorf("roll up %s: %w", k.name, err)
		}
	}
	_, err = w.exec(`INSERT INTO projections (name, seq) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET seq = excluded.seq`,
		rollupCursor, cursor)
	return n, err
}

func compareRollupKeys(a, b rollupKey) int {
	return cmp.Or(strings.Compare(a.pipeline, b.pipeline), strings.Compare(a.name, b.name),
		strings.Compare(a.labels, b.labels), strings.Compare(a.minute, b.minute))
}

func addPoint(acc map[rollupKey]*rollupAgg, pipelineName string, minute time.Time, p metrics.Point) error {
	labels := p.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	body, err := json.Marshal(labels) // map keys marshal sorted: one encoding per label set
	if err != nil {
		return fmt.Errorf("encode labels of %s: %w", p.Name, err)
	}
	k := rollupKey{pipeline: pipelineName, name: p.Name, labels: string(body), minute: formatTime(minute)}
	a := acc[k]
	if a == nil {
		a = &rollupAgg{}
		acc[k] = a
	}
	a.add(p.Value)
	return nil
}

// sampleQueues adds a sample of the queue depth and the oldest pending age of every step of the active pipelines'
// active revisions, and of every other step with due deliveries, at the batch instant: zero for a step whose queue
// is empty, so a drained queue shows as drained.
func (w *wtx) sampleQueues(acc map[rollupKey]*rollupAgg) error {
	type queue struct {
		depth  int64
		oldest TimeCol
	}
	queues := map[store.StepRef]queue{}
	rows, err := w.query(`SELECT id, active_revision FROM pipelines WHERE state = 'active'`)
	if err != nil {
		return err
	}
	active := map[string]store.RevisionID{}
	for rows.Next() {
		var name string
		var rev store.RevisionID
		if err := rows.Scan(&name, &rev); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan pipeline: %w", err)
		}
		active[name] = rev
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	for name, id := range active {
		rev, err := w.revision(id)
		if err != nil {
			return err
		}
		for step := range rev.p.Steps {
			queues[store.StepRef{Pipeline: name, Step: step}] = queue{}
		}
	}
	rows, err = w.query(`SELECT pipeline, step, COUNT(*), MIN(next_attempt_at) FROM deliveries
		WHERE state = ? AND next_attempt_at <= ? GROUP BY pipeline, step`, store.DeliveryPending, w.ts())
	if err != nil {
		return err
	}
	for rows.Next() {
		var ref store.StepRef
		var q queue
		if err := rows.Scan(&ref.Pipeline, &ref.Step, &q.depth, &q.oldest); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan queue: %w", err)
		}
		queues[ref] = q
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	minute := metrics.Minute(w.now)
	for ref, q := range queues {
		age := 0.0
		if q.depth > 0 {
			age = float64(millis(w.now.Sub(q.oldest.Time))) / 1000
		}
		labels := map[string]string{"step": ref.Step}
		if err := addPoint(acc, ref.Pipeline, minute, metrics.Point{Name: metrics.QueueDepth, Labels: labels,
			Value: float64(q.depth)}); err != nil {
			return err
		}
		if err := addPoint(acc, ref.Pipeline, minute, metrics.Point{Name: metrics.OldestPending, Labels: labels,
			Value: age}); err != nil {
			return err
		}
	}
	return nil
}

// MetricRollups returns the rollups f selects, by pipeline, name, labels and minute.
func (s *Store) MetricRollups(ctx context.Context, f store.MetricFilter) ([]store.MetricRollup, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("name", f.Name)
	w.since("minute", f.Since)
	out, err := scan(ctx, s.read, `SELECT pipeline, name, labels_json, minute, count, sum, min, max FROM metric_rollups`+
		w.String()+` ORDER BY pipeline, name, labels_json, minute`, w.args,
		func(rows *sql.Rows) (store.MetricRollup, error) {
			var r store.MetricRollup
			var labels string
			var minute TimeCol
			if err := rows.Scan(&r.Pipeline, &r.Name, &labels, &minute, &r.Count, &r.Sum, &r.Min, &r.Max); err != nil {
				return r, fmt.Errorf("scan rollup: %w", err)
			}
			if err := json.Unmarshal([]byte(labels), &r.Labels); err != nil {
				return r, fmt.Errorf("rollup %s labels: %w", r.Name, err)
			}
			if len(r.Labels) == 0 {
				r.Labels = nil
			}
			r.Minute = minute.Time
			return r, nil
		})
	if err != nil {
		return nil, fmt.Errorf("metric rollups: %w", err)
	}
	return out, nil
}
