package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// where builds a WHERE clause from the non-empty filters, in order.
type where struct {
	conds []string
	args  []any
}

func (w *where) eq(column string, v string) {
	if v != "" {
		w.conds = append(w.conds, column+" = ?")
		w.args = append(w.args, v)
	}
}

func (w *where) since(column string, t time.Time) {
	if !t.IsZero() {
		w.conds = append(w.conds, column+" >= ?")
		w.args = append(w.args, formatTime(t))
	}
}

func (w *where) String() string {
	if len(w.conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(w.conds, " AND ")
}

// scan runs a read query and scans every row with fn.
func scan[T any](ctx context.Context, db *readDB, query string, args []any, fn func(*sql.Rows) (T, error)) ([]T,
	error,
) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	var out []T
	for rows.Next() {
		v, err := fn(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		out = append(out, v)
	}
	return out, closeRows(rows)
}

func closeRows(rows *sql.Rows) error {
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close rows: %w", err)
	}
	return nil
}

const runColumns = `pipeline, id, revision, trigger, trigger_key, trigger_source, COALESCE(scheduled_at, ''), kind,
	generation, vars, var_sources, state, started_at, COALESCE(settled_at, ''), cost_usd`

func scanRun(rows interface{ Scan(...any) error }) (store.Run, error) {
	var (
		r                                 store.Run
		vars, sources, started, settledAt string
		scheduledAt                       string
	)
	if err := rows.Scan(&r.Pipeline, &r.ID, &r.Revision, &r.Trigger, &r.TriggerKey, &r.TriggerSource, &scheduledAt,
		&r.Kind, &r.Generation, &vars, &sources, &r.State, &started, &settledAt, &r.CostUSD); err != nil {
		return r, fmt.Errorf("scan run: %w", err)
	}
	v, err := pipeline.DecodeValue(vars)
	if err != nil {
		return r, fmt.Errorf("run %s vars: %w", r.ID, err)
	}
	r.Vars, _ = v.(map[string]any)
	if err := json.Unmarshal([]byte(sources), &r.VarSources); err != nil {
		return r, fmt.Errorf("run %s var sources: %w", r.ID, err)
	}
	if r.StartedAt, err = parseTime(started); err != nil {
		return r, err
	}
	if r.ScheduledAt, err = parseTime(scheduledAt); err != nil {
		return r, err
	}
	r.SettledAt, err = parseTime(settledAt)
	return r, err
}

// Runs returns runs, newest first.
func (s *Store) Runs(ctx context.Context, f store.RunFilter) ([]store.Run, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("state", string(f.State))
	q := `SELECT ` + runColumns + ` FROM runs` + w.String() + ` ORDER BY started_at DESC, rowid DESC`
	if f.Limit > 0 {
		q += ` LIMIT ?`
		w.args = append(w.args, f.Limit)
	}
	runs, err := scan(ctx, s.read, q, w.args, func(r *sql.Rows) (store.Run, error) { return scanRun(r) })
	if err != nil {
		return nil, fmt.Errorf("runs: %w", err)
	}
	return runs, nil
}

// Run returns one run.
func (s *Store) Run(ctx context.Context, pipelineName, id string) (store.Run, error) {
	r, err := scanRun(s.read.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE pipeline = ? AND id = ?`,
		pipelineName, id))
	if err != nil {
		return r, notFound(err, "run "+id)
	}
	return r, nil
}

// fillItem decodes an item's stored parents, timestamp and artifacts.
func fillItem(it *store.Item, parents, ts, artifacts string) error {
	if err := json.Unmarshal([]byte(parents), &it.Parents); err != nil {
		return fmt.Errorf("item %s parents: %w", it.ID, err)
	}
	if err := json.Unmarshal([]byte(artifacts), &it.Artifacts); err != nil {
		return fmt.Errorf("item %s artifacts: %w", it.ID, err)
	}
	var err error
	it.TS, err = parseTime(ts)
	return err
}

// Items returns items in insertion order.
func (s *Store) Items(ctx context.Context, f store.ItemFilter) ([]store.Item, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("stream", f.Stream)
	w.eq("run_id", f.RunID)
	w.since("ts", f.Since)
	items, err := scan(ctx, s.read, `SELECT pipeline, id, run_id, stream, step, key, parents, generation, ts, payload,
		artifacts FROM items`+w.String()+` ORDER BY rowid`, w.args, func(rows *sql.Rows) (store.Item, error) {
		var it store.Item
		var parents, ts, body, artifacts string
		if err := rows.Scan(&it.Pipeline, &it.ID, &it.RunID, &it.Stream, &it.Step, &it.Key, &parents,
			&it.Generation, &ts, &body, &artifacts); err != nil {
			return it, fmt.Errorf("scan item: %w", err)
		}
		it.Payload = json.RawMessage(body)
		return it, fillItem(&it, parents, ts, artifacts)
	})
	if err != nil {
		return nil, fmt.Errorf("items: %w", err)
	}
	return items, nil
}

// Deliveries returns deliveries in creation order.
func (s *Store) Deliveries(ctx context.Context, f store.DeliveryFilter) ([]store.Delivery, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("run_id", f.RunID)
	w.eq("state", string(f.State))
	w.since("created_at", f.Since)
	ds, err := scan(ctx, s.read, `SELECT pipeline, id, run_id, step, item_id, generation, state, attempt, lease_owner,
		fencing_token, lease_expires_at, next_attempt_at, error_class, error_message, filter_reason, cost_usd,
		reserved_usd, created_at, leased_at, finished_at, wait_ms, work_ms FROM deliveries`+w.String()+` ORDER BY rowid`,
		w.args, func(rows *sql.Rows) (store.Delivery, error) {
			var d store.Delivery
			var expires, next, created, leased, finished string
			var wait, work int64
			if err := rows.Scan(&d.Pipeline, &d.ID, &d.RunID, &d.Step, &d.ItemID, &d.Generation, &d.State, &d.Attempt,
				&d.LeaseOwner, &d.FencingToken, &expires, &next, &d.ErrorClass, &d.ErrorMessage, &d.FilterReason,
				&d.CostUSD, &d.ReservedUSD, &created, &leased, &finished, &wait, &work); err != nil {
				return d, fmt.Errorf("scan delivery: %w", err)
			}
			d.Wait, d.Work = time.Duration(wait)*time.Millisecond, time.Duration(work)*time.Millisecond
			return d, parseTimes(map[*time.Time]string{&d.LeaseExpiresAt: expires, &d.NextAttemptAt: next,
				&d.CreatedAt: created, &d.LeasedAt: leased, &d.FinishedAt: finished})
		})
	if err != nil {
		return nil, fmt.Errorf("deliveries: %w", err)
	}
	return ds, nil
}

// DeadLetters returns dead letters in creation order.
func (s *Store) DeadLetters(ctx context.Context, f store.DeadLetterFilter) ([]store.DeadLetter, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("run_id", f.RunID)
	w.eq("state", string(f.State))
	dls, err := scan(ctx, s.read, `SELECT pipeline, id, run_id, origin_delivery, origin_step, kind, state, stream,
		error, created_at FROM dead_letters`+w.String()+` ORDER BY rowid`, w.args,
		func(rows *sql.Rows) (store.DeadLetter, error) {
			var dl store.DeadLetter
			var created string
			if err := rows.Scan(&dl.Pipeline, &dl.ID, &dl.RunID, &dl.OriginDelivery, &dl.OriginStep, &dl.Kind,
				&dl.State, &dl.Stream, &dl.Error, &created); err != nil {
				return dl, fmt.Errorf("scan dead letter: %w", err)
			}
			var err error
			dl.CreatedAt, err = parseTime(created)
			return dl, err
		})
	if err != nil {
		return nil, fmt.Errorf("dead letters: %w", err)
	}
	return dls, nil
}

// Events returns up to limit events after afterSeq, in sequence order; limit <= 0 means 1000.
func (s *Store) Events(ctx context.Context, afterSeq int64, limit int) ([]store.Event, error) {
	if limit <= 0 {
		limit = 1000
	}
	evs, err := scan(ctx, s.read, `SELECT seq, ts, pipeline, run_id, kind, data FROM events WHERE seq > ?
		ORDER BY seq LIMIT ?`, []any{afterSeq, limit}, scanEvent)
	if err != nil {
		return nil, fmt.Errorf("events: %w", err)
	}
	return evs, nil
}

// PipelineEvents returns the events f selects, in sequence order.
func (s *Store) PipelineEvents(ctx context.Context, f store.EventFilter) ([]store.Event, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	if len(f.Kinds) > 0 {
		w.conds = append(w.conds, "kind IN (?"+strings.Repeat(", ?", len(f.Kinds)-1)+")")
		for _, k := range f.Kinds {
			w.args = append(w.args, k)
		}
	}
	w.since("ts", f.Since)
	evs, err := scan(ctx, s.read, `SELECT seq, ts, pipeline, run_id, kind, data FROM events`+w.String()+
		` ORDER BY seq`, w.args, scanEvent)
	if err != nil {
		return nil, fmt.Errorf("pipeline events: %w", err)
	}
	return evs, nil
}

func scanEvent(rows *sql.Rows) (store.Event, error) {
	var e store.Event
	var ts, data string
	if err := rows.Scan(&e.Seq, &ts, &e.Pipeline, &e.RunID, &e.Kind, &data); err != nil {
		return e, fmt.Errorf("scan event: %w", err)
	}
	e.Data = json.RawMessage(data)
	var err error
	e.TS, err = parseTime(ts)
	return e, err
}

// ActiveRevision returns the pipeline's active revision.
// OpenRevisions returns the revisions of the unsettled runs, ascending.
func (s *Store) OpenRevisions(ctx context.Context) ([]store.RevisionID, error) {
	ids, err := scan(ctx, s.read, `SELECT DISTINCT revision FROM runs WHERE settled_at IS NULL ORDER BY revision`, nil,
		func(rows *sql.Rows) (store.RevisionID, error) {
			var id store.RevisionID
			err := rows.Scan(&id)
			return id, err
		})
	if err != nil {
		return nil, fmt.Errorf("open revisions: %w", err)
	}
	return ids, nil
}

func (s *Store) ActiveRevision(ctx context.Context, pipelineName string) (store.RevisionID, error) {
	var id store.RevisionID
	if err := s.read.QueryRowContext(ctx, `SELECT active_revision FROM pipelines WHERE id = ?`, pipelineName).
		Scan(&id); err != nil {
		return 0, notFound(err, "pipeline "+pipelineName)
	}
	return id, nil
}

// Revision returns a revision with its stored pipeline.
func (s *Store) Revision(ctx context.Context, id store.RevisionID) (store.Revision, error) {
	var r store.Revision
	var data, appliedAt string
	err := s.read.QueryRowContext(ctx,
		`SELECT id, pipeline, package_hash, pipeline_json, applied_at FROM revisions WHERE id = ?`, id).
		Scan(&r.ID, &r.Pipeline, &r.PackageHash, &data, &appliedAt)
	if err != nil {
		return r, notFound(err, fmt.Sprintf("revision %d", id))
	}
	r.PipelineJSON = []byte(data)
	r.AppliedAt, err = parseTime(appliedAt)
	return r, err
}

// PipelineState returns the pipeline's active revision and pause state.
func (s *Store) PipelineState(ctx context.Context, pipelineName string) (store.PipelineState, error) {
	ps := store.PipelineState{Name: pipelineName}
	var state string
	if err := s.read.QueryRowContext(ctx, `SELECT active_revision, state FROM pipelines WHERE id = ?`, pipelineName).
		Scan(&ps.ActiveRevision, &state); err != nil {
		return ps, notFound(err, "pipeline "+pipelineName)
	}
	ps.Paused = state == "paused"
	type pausedStep struct{ step, cause string }
	steps, err := scan(ctx, s.read, `SELECT step, cause FROM steps_state WHERE pipeline = ? AND paused = 1
		ORDER BY step`, []any{pipelineName}, func(rows *sql.Rows) (pausedStep, error) {
		var p pausedStep
		err := rows.Scan(&p.step, &p.cause)
		return p, err
	})
	if err != nil {
		return ps, fmt.Errorf("pipeline state %s: %w", pipelineName, err)
	}
	for _, p := range steps {
		ps.PausedSteps = append(ps.PausedSteps, p.step)
		if p.cause != "" {
			if ps.PauseCauses == nil {
				ps.PauseCauses = map[string]string{}
			}
			ps.PauseCauses[p.step] = p.cause
		}
	}
	return ps, nil
}

// Artifacts returns the artifact store entries of a pipeline, or of every pipeline, by pipeline and hash.
func (s *Store) Artifacts(ctx context.Context, pipelineName string) ([]store.Artifact, error) {
	var w where
	w.eq("pipeline", pipelineName)
	out, err := scan(ctx, s.read, `SELECT pipeline, sha256, size, type, refcount FROM artifacts`+w.String()+
		` ORDER BY pipeline, sha256`, w.args, func(rows *sql.Rows) (store.Artifact, error) {
		var a store.Artifact
		err := rows.Scan(&a.Pipeline, &a.SHA256, &a.Size, &a.Type, &a.Refcount)
		return a, err
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: %w", err)
	}
	return out, nil
}

// AttemptArtifacts implements store.Store.
func (s *Store) AttemptArtifacts(ctx context.Context, pipelineName, deliveryID string) ([]store.AttemptArtifact,
	error,
) {
	out, err := scan(ctx, s.read, `SELECT attempt, name, sha256, size, type FROM attempt_artifacts
		WHERE pipeline = ? AND delivery_id = ? ORDER BY attempt, name`, []any{pipelineName, deliveryID},
		func(rows *sql.Rows) (store.AttemptArtifact, error) {
			var a store.AttemptArtifact
			err := rows.Scan(&a.Attempt, &a.Name, &a.SHA256, &a.Size, &a.Type)
			return a, err
		})
	if err != nil {
		return nil, fmt.Errorf("attempt artifacts: %w", err)
	}
	return out, nil
}
