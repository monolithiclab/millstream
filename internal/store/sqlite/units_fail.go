package sqlite

import (
	"context"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Fail is the Fail unit (spec section 5.11): fencing check, then a retry after backoff while attempts remain for
// a retryable error, or dead with its dead letter.
func (s *Store) Fail(ctx context.Context, f store.FailRequest) (store.DeliveryState, error) {
	var state store.DeliveryState
	err := s.submit(ctx, unitFail, func(w *wtx) error {
		d, err := w.leased(f.LeaseRef, f.Attempt)
		if err != nil {
			return err
		}
		run, err := w.run(d.pipeline, d.runID)
		if err != nil {
			return err
		}
		if run.state == store.RunCancelled {
			state, err = w.cancelDelivery(d, f.CostUSD)
			return err
		}
		rev, err := w.revision(run.revision)
		if err != nil {
			return err
		}
		step, _ := rev.stepOrOrphan(d.step)
		state, err = w.fail(d, run, rev, step, failure{
			class: f.Class, message: f.Message, retryable: f.Retryable, cost: f.CostUSD,
		})
		return err
	})
	return state, err
}

// failure is why an attempt failed.
type failure struct {
	class, message string
	retryable      bool
	cost           float64
}

// fail ends a failed attempt of a leased delivery: pending after backoff when the error is retryable and attempts
// remain, dead with a dead letter otherwise.
func (w *wtx) fail(d *deliveryRow, run *runRow, rev *revision, step pipeline.Step, f failure) (store.DeliveryState,
	error,
) {
	msg := truncate(f.message, store.MaxErrorMessage)
	if err := w.addCost(d, f.cost); err != nil {
		return "", err
	}
	retry := step.Retry
	if f.retryable && d.attempt < retry.MaxAttempts {
		next := w.now.Add(backoff(retry, d.attempt, w.s.opts.Rand()))
		if _, err := w.exec(`UPDATE deliveries SET state = ?, attempt = attempt + 1, lease_owner = '',
			lease_expires_at = '', next_attempt_at = ?, error_class = ?, error_message = ?
			WHERE pipeline = ? AND id = ?`,
			store.DeliveryPending, formatTime(next), f.class, msg, d.pipeline, d.id); err != nil {
			return "", err
		}
		return store.DeliveryPending, w.event(d.pipeline, d.runID, store.EventDeliveryRetry, map[string]any{
			"deliveryId": d.id, "step": d.step, "attempt": d.attempt, "class": f.class,
			"nextAttemptAt": formatTime(next),
		})
	}
	if _, err := w.exec(`UPDATE deliveries SET state = ?, lease_owner = '', lease_expires_at = '', error_class = ?,
		error_message = ? WHERE pipeline = ? AND id = ?`, store.DeliveryDead, f.class, msg, d.pipeline, d.id); err != nil {
		return "", err
	}
	if err := w.event(d.pipeline, d.runID, store.EventDeliveryDead, map[string]any{
		"deliveryId": d.id, "step": d.step, "attempt": d.attempt, "class": f.class,
	}); err != nil {
		return "", err
	}
	input, err := w.itemEnvelope(d.pipeline, d.itemID)
	if err != nil {
		return "", err
	}
	errText := msg
	if f.class != "" {
		errText = f.class + ": " + msg
	}
	if err := w.deadLetter(rev, run, step, store.DeadLetterDead, d.id, "", d.itemID, errText, input); err != nil {
		return "", err
	}
	return store.DeliveryDead, w.settle(d.pipeline, d.runID)
}

// itemEnvelope reads an item as the envelope a dead letter carries (spec section 5.2).
func (w *wtx) itemEnvelope(pipelineName, id string) (map[string]any, error) {
	var stream, step, key, parents, ts, body string
	var generation int64
	if err := w.queryRow(`SELECT stream, step, key, parents, generation, ts, payload FROM items
		WHERE pipeline = ? AND id = ?`, pipelineName, id).
		Scan(&stream, &step, &key, &parents, &generation, &ts, &body); err != nil {
		return nil, notFound(err, "item "+id)
	}
	return map[string]any{
		"id": id, "stream": stream, "step": step, "key": key, "parents": json.RawMessage(parents),
		"generation": generation, "ts": ts, "payload": json.RawMessage(body),
	}, nil
}

// backoff is the delay before the retry following attempt: exponential from r.Backoff, capped at r.MaxBackoff,
// with full jitter (a uniform fraction of the capped delay, so retries of many deliveries spread out).
func backoff(r pipeline.Retry, attempt int, jitter float64) time.Duration {
	d := r.Backoff
	for i := 1; i < attempt && d < r.MaxBackoff; i++ {
		d *= 2
	}
	d = min(d, r.MaxBackoff)
	return time.Duration(jitter * float64(d))
}

// truncate cuts s to at most n bytes without splitting a UTF-8 sequence.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
