package sqlstore

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

// in keeps the rows whose column is one of vs, when there are any.
func (w *where) in(column string, vs []string) {
	if len(vs) == 0 {
		return
	}
	w.conds = append(w.conds, column+" IN (?"+strings.Repeat(", ?", len(vs)-1)+")")
	for _, v := range vs {
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
func scan[T any](ctx context.Context, db *ReadDB, query string, args []any, fn func(*sql.Rows) (T, error)) ([]T,
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

const runColumns = `rowid, pipeline, id, revision, trigger, trigger_key, trigger_source, scheduled_at, kind,
	generation, vars, var_sources, state, started_at, settled_at, cost_usd`

func scanRun(rows interface{ Scan(...any) error }) (store.Run, error) {
	var (
		r                               store.Run
		vars, sources                   string
		started, settledAt, scheduledAt TimeCol
	)
	if err := rows.Scan(&r.Seq, &r.Pipeline, &r.ID, &r.Revision, &r.Trigger, &r.TriggerKey, &r.TriggerSource, &scheduledAt,
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
	r.StartedAt, r.ScheduledAt, r.SettledAt = started.Time, scheduledAt.Time, settledAt.Time
	return r, nil
}

// Runs returns runs, newest first.
func (s *Store) Runs(ctx context.Context, f store.RunFilter) ([]store.Run, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("state", string(f.State))
	if f.Unsettled {
		w.conds = append(w.conds, "settled_at IS NULL")
	}
	if f.After.Seq > 0 {
		w.conds = append(w.conds, "(started_at, rowid) < (?, ?)")
		w.args = append(w.args, formatTime(f.After.StartedAt), f.After.Seq)
	}
	q := `SELECT ` + runColumns + ` FROM runs` + w.String() + ` ORDER BY started_at DESC, rowid DESC`
	if f.Limit > 0 {
		q += limitParam
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
func fillItem(it *store.Item, parents string, ts TimeCol, artifacts string) error {
	if err := json.Unmarshal([]byte(parents), &it.Parents); err != nil {
		return fmt.Errorf("item %s parents: %w", it.ID, err)
	}
	if err := json.Unmarshal([]byte(artifacts), &it.Artifacts); err != nil {
		return fmt.Errorf("item %s artifacts: %w", it.ID, err)
	}
	it.TS = ts.Time
	return nil
}

// Items returns items in insertion order.
func (s *Store) Items(ctx context.Context, f store.ItemFilter) ([]store.Item, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("stream", f.Stream)
	w.eq("run_id", f.RunID)
	w.in("id", f.IDs)
	if len(f.Parents) > 0 {
		w.conds = append(w.conds, "EXISTS (SELECT 1 FROM "+s.d.JSONEach("parents")+" WHERE value IN (?"+
			strings.Repeat(", ?", len(f.Parents)-1)+"))")
		for _, id := range f.Parents {
			w.args = append(w.args, id)
		}
	}
	w.since("ts", f.Since)
	if f.After > 0 {
		w.conds = append(w.conds, "rowid > ?")
		w.args = append(w.args, f.After)
	}
	q := `SELECT rowid, pipeline, id, run_id, stream, step, key, parents, generation, ts, payload, artifacts FROM items` +
		w.String() + ` ORDER BY rowid`
	if f.Limit > 0 {
		q += limitParam
		w.args = append(w.args, f.Limit)
	}
	items, err := scan(ctx, s.read, q, w.args, func(rows *sql.Rows) (store.Item, error) {
		var it store.Item
		var parents, body, artifacts string
		var ts TimeCol
		if err := rows.Scan(&it.Seq, &it.Pipeline, &it.ID, &it.RunID, &it.Stream, &it.Step, &it.Key, &parents,
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
	w.eq("id", f.ID)
	w.eq("run_id", f.RunID)
	w.eq("step", f.Step)
	w.eq("item_id", f.ItemID)
	w.eq("state", string(f.State))
	w.since("created_at", f.Since)
	ds, err := scan(ctx, s.read, `SELECT pipeline, id, run_id, step, item_id, generation, state, attempt, lease_owner,
		fencing_token, lease_expires_at, next_attempt_at, error_class, error_message, filter_reason, cost_usd,
		reserved_usd, created_at, leased_at, finished_at, wait_ms, work_ms FROM deliveries`+w.String()+` ORDER BY rowid`,
		w.args, func(rows *sql.Rows) (store.Delivery, error) {
			var d store.Delivery
			var expires, next, created, leased, finished TimeCol
			var wait, work int64
			if err := rows.Scan(&d.Pipeline, &d.ID, &d.RunID, &d.Step, &d.ItemID, &d.Generation, &d.State, &d.Attempt,
				&d.LeaseOwner, &d.FencingToken, &expires, &next, &d.ErrorClass, &d.ErrorMessage, &d.FilterReason,
				&d.CostUSD, &d.ReservedUSD, &created, &leased, &finished, &wait, &work); err != nil {
				return d, fmt.Errorf("scan delivery: %w", err)
			}
			d.Wait, d.Work = time.Duration(wait)*time.Millisecond, time.Duration(work)*time.Millisecond
			d.LeaseExpiresAt, d.NextAttemptAt, d.CreatedAt = expires.Time, next.Time, created.Time
			d.LeasedAt, d.FinishedAt = leased.Time, finished.Time
			return d, nil
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
			var created TimeCol
			if err := rows.Scan(&dl.Pipeline, &dl.ID, &dl.RunID, &dl.OriginDelivery, &dl.OriginStep, &dl.Kind,
				&dl.State, &dl.Stream, &dl.Error, &created); err != nil {
				return dl, fmt.Errorf("scan dead letter: %w", err)
			}
			dl.CreatedAt = created.Time
			return dl, nil
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
		ORDER BY seq`+limitParam, []any{afterSeq, limit}, scanEvent)
	if err != nil {
		return nil, fmt.Errorf("events: %w", err)
	}
	return evs, nil
}

// PipelineEvents returns the events f selects, in sequence order.
func (s *Store) PipelineEvents(ctx context.Context, f store.EventFilter) ([]store.Event, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("run_id", f.RunID)
	w.in("kind", f.Kinds)
	w.since("ts", f.Since)
	evs, err := scan(ctx, s.read, `SELECT seq, ts, pipeline, run_id, kind, data FROM events`+w.String()+
		` ORDER BY seq`, w.args, scanEvent)
	if err != nil {
		return nil, fmt.Errorf("pipeline events: %w", err)
	}
	return evs, nil
}

// LastEventSeq returns the last event's sequence number, zero when there is none.
func (s *Store) LastEventSeq(ctx context.Context) (int64, error) {
	var seq int64
	if err := s.read.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM events`).Scan(&seq); err != nil {
		return 0, fmt.Errorf("last event: %w", err)
	}
	return seq, nil
}

func scanEvent(rows *sql.Rows) (store.Event, error) {
	var e store.Event
	var ts TimeCol
	var data string
	if err := rows.Scan(&e.Seq, &ts, &e.Pipeline, &e.RunID, &e.Kind, &data); err != nil {
		return e, fmt.Errorf("scan event: %w", err)
	}
	e.Data, e.TS = json.RawMessage(data), ts.Time
	return e, nil
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
	var data string
	var appliedAt TimeCol
	err := s.read.QueryRowContext(ctx,
		`SELECT id, pipeline, package_hash, pipeline_json, applied_at FROM revisions WHERE id = ?`, id).
		Scan(&r.ID, &r.Pipeline, &r.PackageHash, &data, &appliedAt)
	if err != nil {
		return r, notFound(err, fmt.Sprintf("revision %d", id))
	}
	r.PipelineJSON, r.AppliedAt = []byte(data), appliedAt.Time
	return r, nil
}

// Revisions returns a pipeline's revisions, newest first.
func (s *Store) Revisions(ctx context.Context, pipelineName string) ([]store.Revision, error) {
	revs, err := scan(ctx, s.read, `SELECT id, pipeline, package_hash, pipeline_json, applied_at FROM revisions
		WHERE pipeline = ? ORDER BY id DESC`, []any{pipelineName}, func(rows *sql.Rows) (store.Revision, error) {
		var r store.Revision
		var data string
		var appliedAt TimeCol
		if err := rows.Scan(&r.ID, &r.Pipeline, &r.PackageHash, &data, &appliedAt); err != nil {
			return r, fmt.Errorf("scan revision: %w", err)
		}
		r.PipelineJSON, r.AppliedAt = []byte(data), appliedAt.Time
		return r, nil
	})
	if err != nil {
		return nil, fmt.Errorf("revisions of %s: %w", pipelineName, err)
	}
	return revs, nil
}

// Package returns the bundle of the package applied under hash.
func (s *Store) Package(ctx context.Context, hash string) ([]byte, error) {
	var bundle []byte
	if err := s.read.QueryRowContext(ctx, `SELECT bundle FROM packages WHERE hash = ?`, hash).Scan(&bundle); err != nil {
		return nil, notFound(err, "package "+hash)
	}
	return bundle, nil
}

// StreamCounts returns how many items each stream of a pipeline, or of one of its runs, holds.
func (s *Store) StreamCounts(ctx context.Context, pipelineName, runID string) (map[string]int64, error) {
	type count struct {
		stream string
		n      int64
	}
	var w where
	w.eq("pipeline", pipelineName)
	w.eq("run_id", runID)
	counts, err := scan(ctx, s.read, `SELECT stream, COUNT(*) FROM items`+w.String()+` GROUP BY stream`, w.args,
		func(rows *sql.Rows) (count, error) {
			var c count
			err := rows.Scan(&c.stream, &c.n)
			return c, err
		})
	if err != nil {
		return nil, fmt.Errorf("stream counts of %s: %w", pipelineName, err)
	}
	out := make(map[string]int64, len(counts))
	for _, c := range counts {
		out[c.stream] = c.n
	}
	return out, nil
}

// StepStats returns each step's deliveries by state, and those finished since since.
func (s *Store) StepStats(ctx context.Context, pipelineName string, since time.Time) ([]store.StepStats, error) {
	type row struct {
		step, state       string
		created, finished TimeCol
	}
	rows, err := scan(ctx, s.read, `SELECT step, state, created_at, finished_at FROM deliveries
		WHERE pipeline = ? AND (state IN (?, ?, ?) OR finished_at >= ?) ORDER BY step`,
		[]any{pipelineName, store.DeliveryPending, store.DeliveryLeased, store.DeliveryAwaitingReview, formatTime(since)},
		func(rows *sql.Rows) (row, error) {
			var r row
			err := rows.Scan(&r.step, &r.state, &r.created, &r.finished)
			return r, err
		})
	if err != nil {
		return nil, fmt.Errorf("step stats of %s: %w", pipelineName, err)
	}
	var out []store.StepStats
	for _, r := range rows {
		if len(out) == 0 || out[len(out)-1].Step != r.step {
			out = append(out, store.StepStats{Step: r.step})
		}
		st := &out[len(out)-1]
		switch store.DeliveryState(r.state) {
		case store.DeliveryPending:
			st.Pending++
		case store.DeliveryLeased:
			st.Leased++
		case store.DeliveryAwaitingReview:
			st.AwaitingReview++
		default:
			created, finished := r.created.Time, r.finished.Time
			st.Finished++
			if store.DeliveryState(r.state) == store.DeliveryDead {
				st.Dead++
			}
			st.Latencies = append(st.Latencies, max(finished.Sub(created), 0))
		}
	}
	return out, nil
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
	steps, err := scan(ctx, s.read, `SELECT step, cause FROM steps_state WHERE pipeline = ? AND paused = TRUE
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
