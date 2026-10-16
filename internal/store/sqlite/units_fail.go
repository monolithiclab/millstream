package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Fail is the Fail unit (spec section 5.11): fencing check, then a retry after backoff while attempts remain for
// a retryable error, or dead with its dead letter.
func (s *Store) Fail(ctx context.Context, f store.FailRequest) (store.DeliveryState, error) {
	if err := validAttemptArtifacts(f.Artifacts); err != nil {
		return "", fmt.Errorf("fail: %w", err)
	}
	if err := store.ValidMetrics(f.Metrics); err != nil {
		return "", fmt.Errorf("fail: %w", err)
	}
	var state store.DeliveryState
	err := s.submit(ctx, unitFail, func(w *wtx) error {
		d, err := w.leased(f.LeaseRef, f.Attempt)
		if err != nil {
			return err
		}
		if err := w.keepAttemptArtifacts(d, f.Artifacts); err != nil {
			return err
		}
		run, err := w.run(d.pipeline, d.runID)
		if err != nil {
			return err
		}
		cost := f.CostUSD
		if f.Unreported {
			cost = max(cost, d.reserved) // unreported spend is charged in full (spec section 5.9)
		}
		if run.state == store.RunCancelled {
			state, err = w.cancelDelivery(d, cost, f.Metrics)
			return err
		}
		rev, err := w.revision(run.revision)
		if err != nil {
			return err
		}
		step, _ := rev.stepOrOrphan(d.step)
		state, err = w.fail(d, run, rev, step, failure{
			class: f.Class, message: f.Message, retryable: f.Retryable,
			attemptEnd: attemptEnd{cost: cost, metrics: f.Metrics},
		})
		return err
	})
	return state, err
}

// failure is why an attempt failed, with what the attempt's end records.
type failure struct {
	class, message string
	retryable      bool
	attemptEnd
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
	tries := d.attempt - d.attemptBase // since the last dead-letter replay
	retried := f.retryable && tries < retry.MaxAttempts
	data, err := w.endAttempt(d, !retried, f.attemptEnd)
	if err != nil {
		return "", err
	}
	data["class"], data["error"] = f.class, eventError(msg)
	if retried {
		next := w.now.Add(backoff(retry, tries, w.s.opts.Rand()))
		if _, err := w.exec(`UPDATE deliveries SET state = ?, attempt = attempt + 1, lease_owner = '',
			lease_expires_at = '', next_attempt_at = ?, error_class = ?, error_message = ?
			WHERE pipeline = ? AND id = ?`,
			store.DeliveryPending, formatTime(next), f.class, msg, d.pipeline, d.id); err != nil {
			return "", err
		}
		data["nextAttemptAt"] = formatTime(next)
		return store.DeliveryPending, w.event(d.pipeline, d.runID, store.EventDeliveryRetry, data)
	}
	if _, err := w.exec(`UPDATE deliveries SET state = ?, lease_owner = '', lease_expires_at = '', error_class = ?,
		error_message = ? WHERE pipeline = ? AND id = ?`, store.DeliveryDead, f.class, msg, d.pipeline, d.id); err != nil {
		return "", err
	}
	if err := w.event(d.pipeline, d.runID, store.EventDeliveryDead, data); err != nil {
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

// maxEventError is the most bytes of an error message a failed attempt's event keeps: enough for the
// failureCluster insight to fingerprint it (spec section 11.1); the delivery keeps up to MaxErrorMessage.
const maxEventError = 512

func eventError(msg string) string { return truncate(msg, maxEventError) }

// itemEnvelope reads an item as the envelope a dead letter carries (spec section 5.2).
func (w *wtx) itemEnvelope(pipelineName, id string) (map[string]any, error) {
	var stream, step, key, parents, ts, body, artifacts string
	var generation int64
	if err := w.queryRow(`SELECT stream, step, key, parents, generation, ts, payload, artifacts FROM items
		WHERE pipeline = ? AND id = ?`, pipelineName, id).
		Scan(&stream, &step, &key, &parents, &generation, &ts, &body, &artifacts); err != nil {
		return nil, notFound(err, "item "+id)
	}
	return map[string]any{
		"id": id, "stream": stream, "step": step, "key": key, "parents": json.RawMessage(parents),
		"generation": generation, "ts": ts, "payload": json.RawMessage(body), "artifacts": json.RawMessage(artifacts),
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
