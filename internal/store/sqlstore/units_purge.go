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

	"github.com/monolithiclab/millstream/internal/store"
)

// A purge (spec section 9.3) is three units. StartPurge records it and fences its lineage in one transaction, so no
// new work can reach the lineage once it commits: every pending or parked delivery of a fenced item is cancelled,
// a leased one is cancelled when it reports (discards), and an item republished onto a tombstoned one creates no
// delivery (firstCommit). ErasePurge then erases the fenced items a batch at a time, recording each one erased in
// purge_items, so a crash resumes where it stopped. FinishPurge closes it once the engine deleted the artifact
// files no surviving item references. No unit records the key or item named.

// lineageChunk bounds the item ids one lineage query looks up.
const lineageChunk = 200

// itemRef names an item.
type itemRef struct{ pipeline, id string }

// StartPurge is the StartPurge unit: see store.Store.
func (s *Store) StartPurge(ctx context.Context, req store.PurgeRequest) (store.Purge, bool, error) {
	if req.ID == "" || (req.Key == "") == (req.Item == "") {
		return store.Purge{}, false, fmt.Errorf("start purge: an id and exactly one of a key and an item are required: %w",
			store.ErrInvalid)
	}
	var (
		p       store.Purge
		created bool
	)
	err := s.submit(ctx, unitStartPurge, func(w *wtx) error {
		var err error
		p, created, err = w.startPurge(req)
		return err
	})
	return p, created, err
}

func (w *wtx) startPurge(req store.PurgeRequest) (store.Purge, bool, error) {
	p, err := w.purge(req.ID)
	switch {
	case err == nil:
		return p, false, nil
	case !errors.Is(err, store.ErrNotFound):
		return p, false, err
	}
	roots, err := w.purgeRoots(req)
	if err != nil {
		return p, false, err
	}
	lineage, err := w.lineage(roots)
	if err != nil {
		return p, false, err
	}
	p = store.Purge{ID: req.ID, Scope: store.PurgeKey, Pipeline: req.Pipeline, State: store.PurgeErasing,
		Items: int64(len(lineage)), Pipelines: []string{}, RequestedAt: w.now}
	if req.Item != "" {
		p.Scope = store.PurgeItem
	}
	byPipeline := map[string][]string{}
	for _, it := range lineage {
		byPipeline[it.pipeline] = append(byPipeline[it.pipeline], it.id)
	}
	p.Pipelines = slices.Sorted(maps.Keys(byPipeline))
	if len(lineage) == 0 {
		p.State, p.FinishedAt = store.PurgeDone, w.now
	}
	pipelines, _ := json.Marshal(p.Pipelines)
	if _, err := w.exec(`INSERT INTO purges (id, scope, pipeline, state, items, pipelines, requested_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, p.ID, p.Scope, p.Pipeline, p.State, p.Items, string(pipelines), w.ts(),
		nullTime(p.FinishedAt)); err != nil {
		return p, false, err
	}
	runs := map[itemRef]bool{} // the runs whose deliveries or dead letters the fence ended, to settle
	for _, name := range p.Pipelines {
		ids := byPipeline[name]
		for chunk := range slices.Chunk(ids, lineageChunk) {
			if err := w.fence(p.ID, name, chunk, runs); err != nil {
				return p, false, err
			}
		}
		if err := w.event(name, "", store.EventPurgeStarted,
			map[string]any{"purgeId": p.ID, "scope": p.Scope, "items": len(ids)}); err != nil {
			return p, false, err
		}
	}
	for _, r := range slices.SortedFunc(maps.Keys(runs), compareRefs) {
		if err := w.settle(r.pipeline, r.id); err != nil {
			return p, false, err
		}
	}
	return p, true, nil
}

func compareRefs(a, b itemRef) int {
	return cmp.Or(strings.Compare(a.pipeline, b.pipeline), strings.Compare(a.id, b.id))
}

// purgeRoots returns the items a purge names: those with its key, or its item, in its pipeline or in every one, but
// those already erased.
func (w *wtx) purgeRoots(req store.PurgeRequest) ([]itemRef, error) {
	var w2 where
	if req.Item != "" {
		w2.eq("id", req.Item)
	} else {
		w2.eq("key", req.Key)
		w2.conds = append(w2.conds, "key <> ''") // the partial index items_key holds keyed items only
	}
	w2.eq("pipeline", req.Pipeline)
	w2.conds = append(w2.conds, "tombstone <> ?")
	w2.args = append(w2.args, store.TombstoneErased)
	return queryAll(w, scanRef, `SELECT pipeline, id FROM items`+w2.String()+` ORDER BY pipeline, id`, w2.args...)
}

// queryAll runs a query in the unit's transaction and scans every row with fn.
func queryAll[T any](w *wtx, fn func(*sql.Rows) (T, error), query string, args ...any) ([]T, error) {
	rows, err := w.query(query, args...)
	if err != nil {
		return nil, err
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

// scanRef scans a row of a pipeline and an id.
func scanRef(rows *sql.Rows) (itemRef, error) {
	var r itemRef
	return r, rows.Scan(&r.pipeline, &r.id)
}

// scanPair scans a row of two text columns.
func scanPair(rows *sql.Rows) ([2]string, error) {
	var p [2]string
	return p, rows.Scan(&p[0], &p[1])
}

// lineage returns roots and every descendant of theirs, breadth first: the items whose parents hold one of them in
// the same pipeline, and the copies imports published of them in other pipelines (spec section 4.12), whose
// parents are in the pipeline named by parent_pipeline. Dead letters are descendants too: a dead letter's parent is
// its item.
func (w *wtx) lineage(roots []itemRef) ([]itemRef, error) {
	seen := map[itemRef]bool{}
	var out []itemRef
	frontier := roots
	for len(frontier) > 0 {
		byPipeline := map[string][]string{}
		for _, r := range frontier {
			if !seen[r] {
				seen[r] = true
				out = append(out, r)
				byPipeline[r.pipeline] = append(byPipeline[r.pipeline], r.id)
			}
		}
		frontier = nil
		for _, name := range slices.Sorted(maps.Keys(byPipeline)) {
			for chunk := range slices.Chunk(byPipeline[name], lineageChunk) {
				children, err := queryAll(w, scanRef, `SELECT pipeline, id FROM items
					WHERE ((pipeline = ? AND parent_pipeline = '') OR (parent_pipeline <> '' AND parent_pipeline = ?))
					AND EXISTS (SELECT 1 FROM `+w.s.d.JSONEach("parents")+` WHERE value IN (`+w.s.d.JSONParam(false)+`))
					ORDER BY pipeline, id`, name, name, jsonList(chunk))
				if err != nil {
					return nil, err
				}
				frontier = append(frontier, children...)
			}
		}
	}
	return out, nil
}

// fence fences items of a pipeline for purge id: listed in purge_items, tombstoned erased (their payloads are no
// longer served, nor replayed), their pending and parked deliveries cancelled with their open reviews, their open
// review samples unreviewed and their open dead letters discarded. The runs touched are added to runs.
func (w *wtx) fence(id, pipelineName string, ids []string, runs map[itemRef]bool) error {
	list, in := jsonList(ids), ` IN (`+w.s.d.JSONParam(false)+`)`
	touched, err := queryAll(w, scanRef, `SELECT pipeline, run_id FROM deliveries WHERE pipeline = ? AND item_id`+in+`
		AND state IN (?, ?) UNION SELECT pipeline, run_id FROM dead_letters WHERE pipeline = ? AND id`+in+`
		AND state = ?`, pipelineName, list, store.DeliveryPending, store.DeliveryAwaitingReview, pipelineName, list,
		store.DeadLetterOpen)
	if err != nil {
		return err
	}
	for _, r := range touched {
		runs[r] = true
	}
	letters, err := queryAll(w, scanPair, `SELECT id, run_id FROM dead_letters WHERE pipeline = ? AND id`+in+`
		AND state = ? ORDER BY id`, pipelineName, list, store.DeadLetterOpen)
	if err != nil {
		return err
	}
	for _, st := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO purge_items (purge_id, pipeline, item_id) SELECT CAST(? AS TEXT), CAST(? AS TEXT), value
			FROM (` + w.s.d.JSONParam(false) + `) l`, []any{id, pipelineName, list}},
		{`UPDATE items SET tombstone = ? WHERE pipeline = ? AND id` + in, []any{store.TombstoneErased, pipelineName, list}},
		{`UPDATE deliveries SET state = ?, finished_at = ? WHERE pipeline = ? AND item_id` + in + ` AND state IN (?, ?)`,
			[]any{store.DeliveryCancelled, w.ts(), pipelineName, list, store.DeliveryPending, store.DeliveryAwaitingReview}},
		{`UPDATE reviews SET state = ?, decided_at = ? WHERE pipeline = ? AND item_id` + in + ` AND state = ?`,
			[]any{store.ReviewCancelled, w.ts(), pipelineName, list, store.ReviewOpen}},
		{`UPDATE review_samples SET state = ? WHERE pipeline = ? AND item_id` + in + ` AND state = ?`,
			[]any{store.SampleUnreviewed, pipelineName, list, store.SampleOpen}},
		{`UPDATE dead_letters SET state = ? WHERE pipeline = ? AND id` + in + ` AND state = ?`,
			[]any{store.DeadLetterDiscarded, pipelineName, list, store.DeadLetterOpen}},
	} {
		if _, err := w.exec(st.query, st.args...); err != nil {
			return err
		}
	}
	for _, dl := range letters {
		if err := w.event(pipelineName, dl[1], store.EventDeadLetterDiscarded,
			map[string]any{"deadLetterId": dl[0], "reason": "purged"}); err != nil {
			return err
		}
	}
	return nil
}

// ErasePurge is the ErasePurge unit: see store.Store.
func (s *Store) ErasePurge(ctx context.Context, id string) (store.Purge, error) {
	var p store.Purge
	err := s.submit(ctx, unitErasePurge, func(w *wtx) error {
		var err error
		p, err = w.erasePurge(id)
		return err
	})
	return p, err
}

func (w *wtx) erasePurge(id string) (store.Purge, error) {
	p, err := w.purge(id)
	if err != nil || p.State != store.PurgeErasing {
		return p, err
	}
	batch, err := queryAll(w, scanRef, `SELECT pipeline, item_id FROM purge_items WHERE purge_id = ? AND erased = FALSE
		ORDER BY pipeline, item_id`+limitParam, id, store.PurgeBatch)
	if err != nil {
		return p, err
	}
	for _, it := range batch {
		if err := w.erase(it); err != nil {
			return p, err
		}
	}
	if len(batch) > 0 {
		if _, err := w.exec(`UPDATE purge_items SET erased = TRUE WHERE purge_id = ? AND erased = FALSE
			AND (pipeline, item_id) IN (SELECT pipeline, item_id FROM purge_items WHERE purge_id = ? AND erased = FALSE
			ORDER BY pipeline, item_id`+limitParam+`)`, id, id, len(batch)); err != nil {
			return p, err
		}
	}
	p.Erased += int64(len(batch))
	if len(batch) < store.PurgeBatch {
		p.State = store.PurgeSweeping
	}
	_, err = w.exec(`UPDATE purges SET erased = ?, state = ? WHERE id = ?`, p.Erased, p.State, id)
	return p, err
}

// erase erases an item a purge fenced, and what its deliveries hold of it: the payload, key and artifact
// references; each delivery's attempt artifacts (transcripts), error message, review rendering, edit and free text,
// sample reason, and the error its failure events quote; its dead letter's error; the dedupe keys it let through.
func (w *wtx) erase(it itemRef) error {
	var artifacts string
	if err := w.queryRow(`SELECT artifacts FROM items WHERE pipeline = ? AND id = ?`, it.pipeline, it.id).
		Scan(&artifacts); err != nil {
		return notFound(err, "item "+it.id)
	}
	if err := w.tombstone(it.pipeline, it.id, artifacts, store.TombstoneErased); err != nil {
		return err
	}
	deliveries, err := queryAll(w, scanPair, `SELECT id, run_id FROM deliveries WHERE pipeline = ? AND item_id = ?
		ORDER BY id`, it.pipeline, it.id)
	if err != nil {
		return err
	}
	for _, d := range deliveries {
		if err := w.eraseDelivery(it.pipeline, d[0], d[1]); err != nil {
			return err
		}
	}
	if _, err := w.exec(`UPDATE dead_letters SET error = '' WHERE pipeline = ? AND id = ?`, it.pipeline, it.id); err != nil {
		return err
	}
	_, err = w.exec(`DELETE FROM dedupe_keys WHERE pipeline = ? AND first_item = ?`, it.pipeline, it.id)
	return err
}

// failureKinds are the events that quote an attempt's error message.
var failureKinds = []any{store.EventDeliveryRetry, store.EventDeliveryDead}

func (w *wtx) eraseDelivery(pipelineName, deliveryID, runID string) error {
	arts, err := queryAll(w, scanAttemptArtifact, `SELECT pipeline, delivery_id, attempt, name, sha256
		FROM attempt_artifacts WHERE pipeline = ? AND delivery_id = ? ORDER BY attempt, name`, pipelineName, deliveryID)
	if err != nil {
		return err
	}
	for _, a := range arts {
		if err := w.deleteAttemptArtifact(a); err != nil {
			return err
		}
	}
	for _, q := range []string{
		`UPDATE deliveries SET error_message = '' WHERE pipeline = ? AND id = ?`,
		`UPDATE reviews SET display = '', patch = '[]', reason_text = '' WHERE pipeline = ? AND delivery_id = ?`,
		`UPDATE review_samples SET display = '', reason = '' WHERE pipeline = ? AND delivery_id = ?`,
	} {
		if _, err := w.exec(q, pipelineName, deliveryID); err != nil {
			return err
		}
	}
	return w.scrubEvents(pipelineName, deliveryID, runID)
}

// scrubEvents drops the error message the failure events of a delivery quote.
func (w *wtx) scrubEvents(pipelineName, deliveryID, runID string) error {
	type event struct {
		seq  int64
		data map[string]any
	}
	evs, err := queryAll(w, func(rows *sql.Rows) (event, error) {
		var ev event
		var body string
		if err := rows.Scan(&ev.seq, &body); err != nil {
			return ev, err
		}
		return ev, json.Unmarshal([]byte(body), &ev.data)
	}, `SELECT seq, data FROM events WHERE pipeline = ? AND run_id = ? AND kind IN (?, ?) ORDER BY seq`,
		append([]any{pipelineName, runID}, failureKinds...)...)
	if err != nil {
		return fmt.Errorf("read failure events: %w", err)
	}
	for _, ev := range evs {
		if ev.data["deliveryId"] != deliveryID || ev.data["error"] == nil {
			continue
		}
		delete(ev.data, "error")
		b, _ := json.Marshal(ev.data) // decoded from JSON: always encodes
		if _, err := w.exec(`UPDATE events SET data = ? WHERE seq = ?`, string(b), ev.seq); err != nil {
			return err
		}
	}
	return nil
}

// FinishPurge is the FinishPurge unit: see store.Store.
func (s *Store) FinishPurge(ctx context.Context, id string) (store.Purge, error) {
	var p store.Purge
	err := s.submit(ctx, unitFinishPurge, func(w *wtx) error {
		var err error
		if p, err = w.purge(id); err != nil || p.State != store.PurgeSweeping {
			return err
		}
		p.State, p.FinishedAt = store.PurgeDone, w.now
		if _, err := w.exec(`UPDATE purges SET state = ?, finished_at = ? WHERE id = ?`, p.State, w.ts(), id); err != nil {
			return err
		}
		counts, err := w.purgeCounts(id)
		if err != nil {
			return err
		}
		for _, name := range p.Pipelines {
			if err := w.event(name, "", store.EventPurgeCompleted,
				map[string]any{"purgeId": id, "items": counts[name]}); err != nil {
				return err
			}
		}
		return nil
	})
	return p, err
}

// purgeCounts returns how many items a purge fenced in each pipeline.
func (w *wtx) purgeCounts(id string) (map[string]int, error) {
	type count struct {
		pipeline string
		n        int
	}
	rows, err := queryAll(w, func(rows *sql.Rows) (count, error) {
		var c count
		return c, rows.Scan(&c.pipeline, &c.n)
	}, `SELECT pipeline, COUNT(*) FROM purge_items WHERE purge_id = ? GROUP BY pipeline`, id)
	counts := map[string]int{}
	for _, c := range rows {
		counts[c.pipeline] = c.n
	}
	return counts, err
}

const purgeColumns = `id, scope, pipeline, state, items, erased, pipelines, requested_at, finished_at`

func scanPurge(row interface{ Scan(...any) error }) (store.Purge, error) {
	var (
		p                   store.Purge
		pipelines           string
		requested, finished TimeCol
	)
	if err := row.Scan(&p.ID, &p.Scope, &p.Pipeline, &p.State, &p.Items, &p.Erased, &pipelines, &requested,
		&finished); err != nil {
		return p, err
	}
	if err := json.Unmarshal([]byte(pipelines), &p.Pipelines); err != nil {
		return p, fmt.Errorf("purge %s pipelines: %w", p.ID, err)
	}
	p.RequestedAt, p.FinishedAt = requested.Time, finished.Time
	return p, nil
}

// purge reads a purge in the unit's transaction.
func (w *wtx) purge(id string) (store.Purge, error) {
	p, err := scanPurge(w.queryRow(`SELECT `+purgeColumns+` FROM purges WHERE id = ?`, id))
	if err != nil {
		return p, notFound(err, "purge "+id)
	}
	return p, nil
}

// Purge returns a purge.
func (s *Store) Purge(ctx context.Context, id string) (store.Purge, error) {
	p, err := scanPurge(s.read.QueryRowContext(ctx, `SELECT `+purgeColumns+` FROM purges WHERE id = ?`, id))
	if err != nil {
		return p, notFound(err, "purge "+id)
	}
	return p, nil
}

// Purges returns the purges not yet done, oldest first.
func (s *Store) Purges(ctx context.Context) ([]store.Purge, error) {
	out, err := scan(ctx, s.read, `SELECT `+purgeColumns+` FROM purges WHERE state <> ? ORDER BY requested_at, id`,
		[]any{store.PurgeDone}, func(rows *sql.Rows) (store.Purge, error) { return scanPurge(rows) })
	if err != nil {
		return nil, fmt.Errorf("purges: %w", err)
	}
	return out, nil
}
