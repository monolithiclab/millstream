package sqlstore

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Retain is the retention unit (spec section 9.3): one pass over the streams with a retention, the events, the
// attempt artifacts (transcripts) and the metric rollups, each bounded by the request's limit. Ages count back from
// the batch's instant, the store clock.
func (s *Store) Retain(ctx context.Context, req store.RetentionRequest) (store.RetentionResult, error) {
	for _, sr := range req.Streams {
		if sr.Pipeline == "" || sr.Stream == "" || sr.Age <= 0 {
			return store.RetentionResult{}, fmt.Errorf("retain: a stream retention needs a pipeline, a stream and a "+
				"positive age: %w", store.ErrInvalid)
		}
	}
	if req.Events < 0 || req.Transcripts < 0 || req.Rollups < 0 || req.Limit < 0 {
		return store.RetentionResult{}, fmt.Errorf("retain: negative age or limit: %w", store.ErrInvalid)
	}
	var res store.RetentionResult
	err := s.submit(ctx, unitRetain, func(w *wtx) error {
		var err error
		res, err = w.retain(req, cmp.Or(req.Limit, store.RetentionBatch))
		return err
	})
	return res, err
}

func (w *wtx) retain(req store.RetentionRequest, limit int) (store.RetentionResult, error) {
	var res store.RetentionResult
	for _, sr := range req.Streams {
		n, err := w.expireItems(sr, limit-res.Items)
		if err != nil {
			return res, err
		}
		if res.Items += n; res.Items >= limit {
			break
		}
	}
	var err error
	if req.Events > 0 {
		if res.Events, err = w.deleteEvents(w.now.Add(-req.Events), limit); err != nil {
			return res, err
		}
	}
	if req.Transcripts > 0 {
		if res.Transcripts, err = w.deleteTranscripts(w.now.Add(-req.Transcripts), limit); err != nil {
			return res, err
		}
	}
	if req.Rollups > 0 {
		if res.Rollups, err = w.deleteRollups(w.now.Add(-req.Rollups), limit); err != nil {
			return res, err
		}
	}
	res.More = res.Items >= limit || res.Events >= limit || res.Transcripts >= limit || res.Rollups >= limit
	return res, nil
}

// keepAlive is the condition, on items i, that unfinished work needs an item's payload (spec section 9.3): a
// delivery of it that is not terminal (a parked review's included), an open dead letter that is the item or whose
// dead delivery it is the input of (a replay sends it again), an open review sample of it. Its arguments are
// keepAliveArgs.
const keepAlive = `(EXISTS (SELECT 1 FROM deliveries d WHERE d.pipeline = i.pipeline AND d.item_id = i.id
		AND d.state NOT IN (?, ?, ?, ?))
	OR EXISTS (SELECT 1 FROM dead_letters l WHERE l.pipeline = i.pipeline AND l.id = i.id AND l.state = ?)
	OR EXISTS (SELECT 1 FROM deliveries d JOIN dead_letters l ON l.pipeline = d.pipeline AND l.origin_delivery = d.id
		WHERE d.pipeline = i.pipeline AND d.item_id = i.id AND l.kind = ? AND l.state = ?)
	OR EXISTS (SELECT 1 FROM review_samples rs WHERE rs.pipeline = i.pipeline AND rs.item_id = i.id AND rs.state = ?))`

var keepAliveArgs = append(append([]any{}, terminalStates...), store.DeadLetterOpen, store.DeadLetterDead,
	store.DeadLetterOpen, store.SampleOpen)

// expireItems tombstones up to n items of a stream published before its retention, oldest first, but those
// unfinished work needs: payload null, no artifacts (their references dropped), the envelope and lineage kept. It
// records an itemsExpired event when it expired any.
func (w *wtx) expireItems(sr store.StreamRetention, n int) (int, error) {
	if n <= 0 {
		return 0, nil
	}
	items, err := queryAll(w, scanPair, `SELECT i.id, i.artifacts FROM items i
		WHERE i.pipeline = ? AND i.stream = ? AND i.tombstone = '' AND i.ts < ? AND NOT `+keepAlive+`
		ORDER BY i.ts`+limitParam,
		append(append([]any{sr.Pipeline, sr.Stream, formatTime(w.now.Add(-sr.Age))}, keepAliveArgs...), n)...)
	if err != nil {
		return 0, err
	}
	for _, e := range items {
		if err := w.tombstone(sr.Pipeline, e[0], e[1], store.TombstoneExpired); err != nil {
			return 0, err
		}
	}
	if len(items) == 0 {
		return 0, nil
	}
	return len(items), w.event(sr.Pipeline, "", store.EventItemsExpired,
		map[string]any{"stream": sr.Stream, "items": len(items)})
}

// tombstone replaces an item's payload with null and drops its artifact references, which artifacts is the stored
// list of; an erased item loses its key too (spec section 9.3).
func (w *wtx) tombstone(pipelineName, id, artifacts string, why store.Tombstone) error {
	var refs []store.ArtifactRef
	if err := json.Unmarshal([]byte(artifacts), &refs); err != nil {
		return fmt.Errorf("item %s artifacts: %w", id, err)
	}
	for _, a := range refs {
		if err := w.unref(pipelineName, a.SHA256); err != nil {
			return err
		}
	}
	key := "key" // an expired item keeps its key
	if why == store.TombstoneErased {
		key = "''"
	}
	_, err := w.exec(`UPDATE items SET payload = 'null', payload_hash = '', artifacts = '[]', key = `+key+`,
		tombstone = ? WHERE pipeline = ? AND id = ?`, why, pipelineName, id)
	return err
}

// unref drops one reference to an artifact: at zero, the artifact sweep deletes its file, then its entry.
func (w *wtx) unref(pipelineName, sha string) error {
	if _, err := w.exec(`UPDATE artifacts SET refcount = refcount - 1 WHERE pipeline = ? AND sha256 = ?`,
		pipelineName, sha); err != nil {
		return fmt.Errorf("unreference artifact %s: %w", sha, err)
	}
	return nil
}

// deleteEvents deletes up to n events recorded before cutoff, oldest first, that every projection has read (a
// store whose rollup never ran keeps them all), never the last one: the next event's sequence number follows it.
func (w *wtx) deleteEvents(cutoff time.Time, n int) (int, error) {
	var read, last int64
	if err := w.queryRow(`SELECT COALESCE((SELECT MIN(seq) FROM projections), 0),
		COALESCE((SELECT MAX(seq) FROM events), 0)`).Scan(&read, &last); err != nil {
		return 0, fmt.Errorf("read projection cursors: %w", err)
	}
	n, err := w.affectedInt(`DELETE FROM events WHERE seq IN (SELECT seq FROM events
		WHERE ts < ? AND seq <= ? AND seq < ? ORDER BY seq`+limitParam+`)`, formatTime(cutoff), read, last, n)
	if err != nil {
		return 0, fmt.Errorf("delete events: %w", err)
	}
	return n, nil
}

// deleteRollups deletes up to n metric rollups of minutes before cutoff.
func (w *wtx) deleteRollups(cutoff time.Time, n int) (int, error) {
	m, err := w.affectedInt(`DELETE FROM metric_rollups WHERE (pipeline, name, labels_json, minute) IN
		(SELECT pipeline, name, labels_json, minute FROM metric_rollups WHERE minute < ?`+limitParam+`)`,
		formatTime(cutoff), n)
	if err != nil {
		return 0, fmt.Errorf("delete rollups: %w", err)
	}
	return m, nil
}

// deleteTranscripts deletes up to n attempt artifacts kept before cutoff, oldest first, each dropping its
// reference, but those of a delivery not terminal or with an open dead letter: a replay or an operator still needs
// them.
func (w *wtx) deleteTranscripts(cutoff time.Time, n int) (int, error) {
	arts, err := queryAll(w, scanAttemptArtifact, `SELECT a.pipeline, a.delivery_id, a.attempt, a.name, a.sha256
		FROM attempt_artifacts a
		JOIN deliveries d ON d.pipeline = a.pipeline AND d.id = a.delivery_id
		WHERE a.created_at < ? AND d.state IN (?, ?, ?, ?)
			AND NOT EXISTS (SELECT 1 FROM dead_letters l WHERE l.pipeline = a.pipeline
				AND l.origin_delivery = a.delivery_id AND l.state = ?)
		ORDER BY a.created_at`+limitParam,
		append(append([]any{formatTime(cutoff)}, terminalStates...), store.DeadLetterOpen, n)...)
	if err != nil {
		return 0, err
	}
	for _, a := range arts {
		if err := w.deleteAttemptArtifact(a); err != nil {
			return 0, err
		}
	}
	return len(arts), nil
}

// attemptArtifact is a row of attempt_artifacts.
type attemptArtifact struct {
	pipeline, deliveryID, name, sha string
	attempt                         int
}

func scanAttemptArtifact(rows *sql.Rows) (attemptArtifact, error) {
	var a attemptArtifact
	return a, rows.Scan(&a.pipeline, &a.deliveryID, &a.attempt, &a.name, &a.sha)
}

func (w *wtx) deleteAttemptArtifact(a attemptArtifact) error {
	if _, err := w.exec(`DELETE FROM attempt_artifacts WHERE pipeline = ? AND delivery_id = ? AND attempt = ?
		AND name = ?`, a.pipeline, a.deliveryID, a.attempt, a.name); err != nil {
		return err
	}
	return w.unref(a.pipeline, a.sha)
}

// affectedInt is affected as an int.
func (w *wtx) affectedInt(query string, args ...any) (int, error) {
	n, err := w.affected(query, args...)
	return int(n), err
}

// DeleteArtifacts is the artifact sweep's unit: the entries named whose reference count is zero, once their files
// are gone. An entry referenced again since keeps its row.
func (s *Store) DeleteArtifacts(ctx context.Context, keys []store.ArtifactKey) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	var n int
	err := s.submit(ctx, unitDeleteArtifacts, func(w *wtx) error {
		n = 0
		for _, k := range keys {
			m, err := w.affectedInt(`DELETE FROM artifacts WHERE pipeline = ? AND sha256 = ? AND refcount <= 0`,
				k.Pipeline, k.SHA256)
			if err != nil {
				return err
			}
			n += m
		}
		return nil
	})
	return n, err
}
