package sqlstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// deliveryRow is the part of a delivery the units read.
type deliveryRow struct {
	pipeline, id, runID, step, itemID string
	generation                        int64
	state                             store.DeliveryState
	attempt                           int
	attemptBase                       int // the attempt before the last dead-letter replay: retries count from it
	owner                             string
	token                             int64
	reserved                          float64 // the budget reservation of a leased agent delivery
	createdAt, leasedAt               time.Time
	erased                            bool // a purge fenced its item (spec section 9.3)
}

func (w *wtx) delivery(pipelineName, id string) (*deliveryRow, error) {
	d := &deliveryRow{pipeline: pipelineName, id: id}
	var created, leased TimeCol
	var tombstone string
	err := w.queryRow(`SELECT d.run_id, d.step, d.item_id, d.generation, d.state, d.attempt, d.attempt_base,
		d.lease_owner, d.fencing_token, d.reserved_usd, d.created_at, d.leased_at, i.tombstone
		FROM deliveries d JOIN items i ON i.pipeline = d.pipeline AND i.id = d.item_id
		WHERE d.pipeline = ? AND d.id = ?`, pipelineName, id).
		Scan(&d.runID, &d.step, &d.itemID, &d.generation, &d.state, &d.attempt, &d.attemptBase, &d.owner, &d.token,
			&d.reserved, &created, &leased, &tombstone)
	if err != nil {
		return nil, notFound(err, "delivery "+id)
	}
	d.createdAt, d.leasedAt = created.Time, leased.Time
	d.erased = store.Tombstone(tombstone) == store.TombstoneErased
	return d, nil
}

// discards reports whether the report of d, a leased delivery of run, is discarded and d cancelled instead: its run
// was cancelled (spec section 5.3), or a purge fenced its item (spec section 9.3), so nothing it produced may be
// published.
func discards(d *deliveryRow, run *runRow) bool {
	return run.state == store.RunCancelled || d.erased
}

// millis is d in whole milliseconds, never negative: the unit of the delivery timings.
func millis(d time.Duration) int64 { return max(d.Milliseconds(), 0) }

// attemptEnd is what the end of a leased attempt records in its event (spec section 10.2): the attempt's work
// time, its cost and the action's metrics, and, when the delivery became terminal, its latency.
type attemptEnd struct {
	cost    float64
	metrics []store.Metric
}

// endAttempt ends d's leased attempt at the batch instant: it adds the attempt's work time to the delivery and,
// when terminal, marks it finished. It returns the fields the attempt's event carries. A delivery whose leasedAt is
// zero (a review decided after its attempt parked) records no work.
func (w *wtx) endAttempt(d *deliveryRow, terminal bool, a attemptEnd) (map[string]any, error) {
	work := int64(0)
	if !d.leasedAt.IsZero() {
		work = millis(w.now.Sub(d.leasedAt))
	}
	var finished any // NULL while not terminal
	if terminal {
		finished = w.ts()
	}
	if _, err := w.exec(`UPDATE deliveries SET work_ms = work_ms + ?, finished_at = ? WHERE pipeline = ? AND id = ?`,
		work, finished, d.pipeline, d.id); err != nil {
		return nil, err
	}
	data := map[string]any{"deliveryId": d.id, "itemId": d.itemID, "step": d.step, "attempt": d.attempt}
	if !d.leasedAt.IsZero() { // a parked review's attempt ended when it parked: its decision adds no work
		data["workMs"] = work
	}
	if terminal {
		data["latencyMs"] = millis(w.now.Sub(d.createdAt))
	}
	if a.cost != 0 {
		data["costUsd"] = a.cost
	}
	if len(a.metrics) > 0 {
		data["metrics"] = a.metrics
	}
	return data, nil
}

// leased returns the delivery a Done or Fail names, refusing it unless it is still leased under the same fencing
// token and attempt.
func (w *wtx) leased(ref store.LeaseRef, attempt int) (*deliveryRow, error) {
	return w.leasedBy(ref, attempt, "")
}

// leasedBy is leased, the lease held by owner too when it is set.
func (w *wtx) leasedBy(ref store.LeaseRef, attempt int, owner string) (*deliveryRow, error) {
	d, err := w.delivery(ref.Pipeline, ref.DeliveryID)
	if err != nil {
		return nil, err
	}
	if owner != "" && d.owner != owner {
		return nil, fmt.Errorf("delivery %s is not leased to %s: %w", d.id, owner, store.ErrStaleLease)
	}
	if d.state != store.DeliveryLeased || d.token != ref.FencingToken || d.attempt != attempt {
		return nil, fmt.Errorf("delivery %s is %s with token %d attempt %d, not token %d attempt %d: %w",
			d.id, d.state, d.token, d.attempt, ref.FencingToken, attempt, store.ErrStaleLease)
	}
	return d, nil
}

// runRow is the part of a run the units read.
type runRow struct {
	pipeline, id, trigger string
	// producer and producerRun name the producer run of an import run (spec section 4.12), its trigger key; producer
	// is empty for other runs.
	producer, producerRun string
	// consumers counts the import runs the run opened in other pipelines.
	consumers  int
	revision   store.RevisionID
	generation int64
	state      store.RunState
	startedAt  time.Time
	settled    bool
	vars       map[string]any
}

func (w *wtx) run(pipelineName, id string) (*runRow, error) {
	r := &runRow{pipeline: pipelineName, id: id}
	var vars string
	var startedAt, settledAt TimeCol
	err := w.queryRow(`SELECT trigger, revision, generation, state, started_at, settled_at, vars, producer, trigger_key, consumers
		FROM runs WHERE pipeline = ? AND id = ?`, pipelineName, id).
		Scan(&r.trigger, &r.revision, &r.generation, &r.state, &startedAt, &settledAt, &vars, &r.producer, &r.producerRun, &r.consumers)
	if err != nil {
		return nil, notFound(err, "run "+id)
	}
	r.settled, r.startedAt = !settledAt.IsZero(), startedAt.Time
	v, err := pipeline.DecodeValue(vars)
	if err != nil {
		return nil, fmt.Errorf("run %s vars: %w", id, err)
	}
	r.vars, _ = v.(map[string]any)
	return r, nil
}

// exprRun is the run as route expressions see it; the revision is the package hash, as actions see it.
func (r *runRow) exprRun(rev *revision) expr.Run {
	return expr.Run{ID: r.id, Trigger: r.trigger, StartedAt: r.startedAt, Revision: rev.hash}
}

// payload is a JSON document in canonical form, with its decoded value for expressions.
type payload struct {
	raw   []byte
	value any
}

// canonical decodes a JSON document and re-encodes it with sorted keys, no insignificant space and normalised
// numbers, so two encodings of one value store and hash the same. nil means an empty object.
func canonical(raw json.RawMessage) (payload, error) {
	if raw == nil {
		raw = json.RawMessage("{}")
	}
	v, err := pipeline.DecodeValue(string(raw))
	if err != nil {
		return payload{}, fmt.Errorf("payload is not JSON: %w: %w", store.ErrInvalid, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return payload{}, fmt.Errorf("encode payload: %w", err)
	}
	return payload{raw: b, value: v}, nil
}

func hashPayload(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
