package sqlstore

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Fallback and audits (spec section 11.5). A delivery whose action abstains, or fails without retry when the step's
// fallback.when lists error, goes back to pending with its retry budget restarted, flagged to run the step's
// fallback action (deliveries.fallback); its outputs then record producedBy fallback. A delivery its primary covered
// is sampled for an audit at the step's auditRate: the audit is a row of its own, leased by the Lease unit after the
// deliveries like one of them but never part of its run, ended by EndAudit once, never retried.

// producedBy returns what the envelope of d's outputs records: primary or fallback on a step with a fallback, empty
// on any other step.
func producedBy(step pipeline.Step, d *deliveryRow) string {
	switch {
	case step.Fallback == nil:
		return ""
	case d.fallback:
		return store.ProducedByFallback
	}
	return store.ProducedByPrimary
}

// fallsBack reports whether d, abstaining or failing for cause, goes to its step's fallback action: the step has one
// whose when lists cause, and d is not running it already.
func (w *wtx) fallsBack(d *deliveryRow, step pipeline.Step, cause string) bool {
	return step.Fallback != nil && !d.fallback && slices.Contains(step.Fallback.When, cause)
}

// Abstain is the Abstain unit (spec section 5.11): see store.Store.
func (s *Store) Abstain(ctx context.Context, f store.FailRequest) (store.DeliveryState, error) {
	if err := validAttemptArtifacts(f.Artifacts); err != nil {
		return "", fmt.Errorf("abstain: %w", err)
	}
	if err := store.ValidMetrics(f.Metrics); err != nil {
		return "", fmt.Errorf("abstain: %w", err)
	}
	var state store.DeliveryState
	err := s.submit(ctx, unitAbstain, func(w *wtx) error {
		d, err := w.leased(f.LeaseRef, f.Attempt, f.Owner)
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
			cost = max(cost, d.reserved)
		}
		end := attemptEnd{cost: cost, metrics: f.Metrics}
		if discards(d, run) {
			state, err = w.cancelDelivery(d, cost, f.Metrics)
			return err
		}
		rev, err := w.revision(run.revision)
		if err != nil {
			return err
		}
		step, _ := rev.stepOrOrphan(d.step)
		reason := truncate(f.Message, store.MaxErrorMessage)
		if w.fallsBack(d, step, store.FallbackOnAbstain) {
			state, err = w.handOff(d, store.FallbackOnAbstain, store.ClassAbstain, reason, end)
			return err
		}
		msg := "the action abstained and the step has no fallback for it"
		if d.fallback {
			msg = "the fallback action abstained"
		}
		if reason != "" {
			msg += ": " + reason
		}
		state, err = w.fail(d, run, rev, step, failure{class: store.ClassAbstain, message: msg, attemptEnd: end})
		return err
	})
	return state, err
}

// handOff ends d's attempt and hands d to its step's fallback action (spec section 11.5): pending at once, its retry
// budget restarted from the next attempt (numbered on, never reused), its last error kept as why. Its outputs, once
// done, keep the ids the primary's would have had.
func (w *wtx) handOff(d *deliveryRow, cause, class, msg string, end attemptEnd) (store.DeliveryState, error) {
	if err := w.addCost(d, end.cost); err != nil {
		return "", err
	}
	data, err := w.endAttempt(d, false, end)
	if err != nil {
		return "", err
	}
	if _, err := w.exec(`UPDATE deliveries SET state = ?, fallback = TRUE, attempt_base = attempt,
		attempt = attempt + 1, lease_owner = '', lease_expires_at = NULL, next_attempt_at = ?, error_class = ?,
		error_message = ? WHERE pipeline = ? AND id = ?`,
		store.DeliveryPending, w.ts(), class, msg, d.pipeline, d.id); err != nil {
		return "", err
	}
	data["cause"], data["class"], data["error"] = cause, class, eventError(msg)
	return store.DeliveryPending, w.event(d.pipeline, d.runID, store.EventDeliveryFallback, data)
}

// auditOutput is one output as an audit compares it.
type auditOutput struct {
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

// auditOutputs encodes outputs, their payloads canonical, as an audit stores and compares them: a JSON array
// sorted by key, then payload, so the same outputs in another order compare equal.
func auditOutputs(outs []store.Output, payloads []payload) string {
	list := make([]auditOutput, len(outs))
	for i, o := range outs {
		list[i] = auditOutput{Key: o.Key, Payload: payloads[i].raw}
	}
	slices.SortFunc(list, func(a, b auditOutput) int {
		return cmp.Or(cmp.Compare(a.Key, b.Key), bytes.Compare(a.Payload, b.Payload))
	})
	b, _ := json.Marshal(list) // canonical payloads always encode
	return string(b)
}

// sampleAudit records the audit of d, a delivery its step's primary action just covered with outputs, when the
// step audits and d falls in its sample (spec section 11.5). The sample is deterministic per delivery.
func (w *wtx) sampleAudit(d *deliveryRow, step pipeline.Step, outs []store.Output, payloads []payload) error {
	if step.Fallback == nil || step.Fallback.AuditRate <= 0 || d.fallback {
		return nil
	}
	id := ids.Audit(d.id)
	if !ids.Sampled(id, step.Fallback.AuditRate) {
		return nil
	}
	_, err := w.exec(`INSERT INTO audits (pipeline, id, delivery_id, run_id, step, item_id, state, primary_output,
		created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		d.pipeline, id, d.id, d.runID, d.step, d.itemID, store.AuditPending, auditOutputs(outs, payloads), w.ts())
	return err
}

// leaseAudits leases due audits with what the pass has left (spec section 11.5): after the deliveries, so audits
// only take capacity the deliveries leave, within the steps' slots and the pipelines', past paused steps and
// pipelines. An agent fallback reserves its maxCostUsd against the day budget only, an audit being no part of a run;
// an audit the day budget has no room for is cancelled, as is one whose item expired or was erased.
func (w *wtx) leaseAudits(p *leasePass, expires time.Time) ([]store.Lease, error) {
	steps, err := queryAll(w, scanRef, `SELECT DISTINCT a.pipeline, a.step FROM audits a
		JOIN pipelines p ON p.id = a.pipeline AND p.state = 'active'
		LEFT JOIN steps_state ss ON ss.pipeline = a.pipeline AND ss.step = a.step
		WHERE a.state = ? AND COALESCE(ss.paused, FALSE) = FALSE ORDER BY a.pipeline, a.step`, store.AuditPending)
	if err != nil {
		return nil, err
	}
	var leases []store.Lease
	for _, s := range steps {
		ref := store.StepRef{Pipeline: s.pipeline, Step: s.id}
		n := p.req.Max
		if slots, ok := p.req.Slots[ref]; ok {
			n = min(n, slots)
		}
		if slots, ok := p.req.PipelineSlots[ref.Pipeline]; ok {
			n = min(n, slots)
		}
		if n <= 0 || !leasable(p.req, ref) {
			continue
		}
		cs, err := w.dueAudits(ref, n)
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			l := c.lease
			if c.tombstone != "" {
				if err := w.cancelAudit(l.Pipeline, l.DeliveryID, "the audited item is "+c.tombstone); err != nil {
					return nil, err
				}
				continue
			}
			reserve, agent := p.req.Reserve[store.AgentStep{StepRef: ref, Revision: l.Revision, Fallback: true}]
			if agent {
				scope, err := w.budgetAllows(l, reserve, p.req.Reserve, p.paused)
				if err != nil {
					return nil, err
				}
				if scope != "" {
					if err := w.cancelAudit(l.Pipeline, l.DeliveryID, "budget: the day budget leaves no room for the audit"); err != nil {
						return nil, err
					}
					continue
				}
			}
			if err := w.queryRow(`UPDATE audits SET state = ?, lease_owner = ?, fencing_token = fencing_token + 1,
				lease_expires_at = ?, reserved_usd = ? WHERE pipeline = ? AND id = ? RETURNING fencing_token`,
				store.AuditLeased, p.req.Owner, formatTime(expires), reserve, l.Pipeline, l.DeliveryID).
				Scan(&l.FencingToken); err != nil {
				return nil, fmt.Errorf("lease audit %s: %w", l.DeliveryID, err)
			}
			l.ExpiresAt = expires
			leases = append(leases, l)
			p.take(ref)
		}
	}
	return leases, nil
}

// auditCandidate is a pending audit Lease may pick, with its item's tombstone.
type auditCandidate struct {
	lease     store.Lease
	tombstone string
}

// dueAudits returns the step's n oldest pending audits, as the leases they become.
func (w *wtx) dueAudits(ref store.StepRef, n int) ([]auditCandidate, error) {
	rows, err := w.query(`SELECT a.id, a.run_id, r.revision, d.generation,
		i.id, i.run_id, i.stream, i.step, i.key, i.parents, i.generation, i.ts, i.payload, i.artifacts, i.tombstone
		FROM audits a
		JOIN runs r ON r.pipeline = a.pipeline AND r.id = a.run_id
		JOIN deliveries d ON d.pipeline = a.pipeline AND d.id = a.delivery_id
		JOIN items i ON i.pipeline = a.pipeline AND i.id = a.item_id
		WHERE a.pipeline = ? AND a.step = ? AND a.state = ? ORDER BY a.rowid`+limitParam,
		ref.Pipeline, ref.Step, store.AuditPending, n)
	if err != nil {
		return nil, err
	}
	var out []auditCandidate
	for rows.Next() {
		c := auditCandidate{lease: store.Lease{Pipeline: ref.Pipeline, Step: ref.Step, Attempt: 1, Fallback: true,
			Audit: true}}
		l := &c.lease
		var parents, body, artifacts string
		var ts TimeCol
		if err := rows.Scan(&l.DeliveryID, &l.RunID, &l.Revision, &l.Generation, &l.Item.ID, &l.Item.RunID,
			&l.Item.Stream, &l.Item.Step, &l.Item.Key, &parents, &l.Item.Generation, &ts, &body, &artifacts,
			&c.tombstone); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan due audit: %w", err)
		}
		l.Item.Pipeline = ref.Pipeline
		l.Item.Payload = json.RawMessage(body)
		if err := fillItem(&l.Item, parents, ts, artifacts); err != nil {
			_ = rows.Close()
			return nil, err
		}
		out = append(out, c)
	}
	return out, closeRows(rows)
}

// cancelAudit cancels a pending audit, saying why.
func (w *wtx) cancelAudit(pipelineName, id, why string) error {
	_, err := w.exec(`UPDATE audits SET state = ?, error = ?, finished_at = ? WHERE pipeline = ? AND id = ?`,
		store.AuditCancelled, why, w.ts(), pipelineName, id)
	return err
}

// dropAudits drops what the audits of an item hold of it once it is tombstoned (spec section 9.3): their outputs and
// errors, and a pending audit is cancelled. A leased one ends as EndAudit decides.
func (w *wtx) dropAudits(pipelineName, itemID string, why store.Tombstone) error {
	if _, err := w.exec(`UPDATE audits SET primary_output = 'null', fallback_output = 'null', error = ''
		WHERE pipeline = ? AND item_id = ?`, pipelineName, itemID); err != nil {
		return err
	}
	_, err := w.exec(`UPDATE audits SET state = ?, error = ?, finished_at = ? WHERE pipeline = ? AND item_id = ?
		AND state = ?`, store.AuditCancelled, "the audited item is "+string(why), w.ts(), pipelineName, itemID,
		store.AuditPending)
	return err
}

// auditRow is the part of an audit the units read.
type auditRow struct {
	pipeline, id, deliveryID, step, itemID, owner, primary string
	state                                                  store.AuditState
	token                                                  int64
	reserved                                               float64
	tombstone                                              string
}

func (w *wtx) audit(pipelineName, id string) (*auditRow, error) {
	a := &auditRow{pipeline: pipelineName, id: id}
	err := w.queryRow(`SELECT a.delivery_id, a.step, a.item_id, a.lease_owner, a.primary_output, a.state,
		a.fencing_token, a.reserved_usd, i.tombstone
		FROM audits a JOIN items i ON i.pipeline = a.pipeline AND i.id = a.item_id
		WHERE a.pipeline = ? AND a.id = ?`, pipelineName, id).
		Scan(&a.deliveryID, &a.step, &a.itemID, &a.owner, &a.primary, &a.state, &a.token, &a.reserved, &a.tombstone)
	if err != nil {
		return nil, notFound(err, "audit "+id)
	}
	return a, nil
}

// EndAudit ends a leased audit: see store.Store.
func (s *Store) EndAudit(ctx context.Context, req store.AuditEnd) error {
	payloads := make([]payload, len(req.Outputs))
	if req.Class == "" {
		for i, o := range req.Outputs {
			var err error
			if payloads[i], err = canonical(o.Payload); err != nil {
				return fmt.Errorf("end audit: output %d: %w", i, err)
			}
		}
	}
	if err := store.ValidMetrics(req.Metrics); err != nil {
		return fmt.Errorf("end audit: %w", err)
	}
	return s.submit(ctx, unitEndAudit, func(w *wtx) error {
		a, err := w.audit(req.Pipeline, req.DeliveryID)
		if err != nil {
			return err
		}
		if (req.Owner != "" && a.owner != req.Owner) || a.state != store.AuditLeased || a.token != req.FencingToken {
			return fmt.Errorf("audit %s is %s with token %d, not leased with token %d: %w", a.id, a.state, a.token,
				req.FencingToken, store.ErrStaleLease)
		}
		cost := req.CostUSD
		if req.Unreported {
			cost = max(cost, a.reserved)
		}
		switch {
		case a.tombstone != "":
			return w.endAudit(a, store.AuditCancelled, "null", nil, "", "the audited item is "+a.tombstone, cost)
		case req.Class != "":
			return w.endAudit(a, store.AuditFailed, "null", nil, req.Class, req.Message, cost)
		}
		fallback := auditOutputs(req.Outputs, payloads)
		agreement := 0.0
		if fallback == a.primary {
			agreement = 1
		}
		return w.endAudit(a, store.AuditDone, fallback, &agreement, "", "", cost)
	})
}

// endAudit ends a leased audit in state, with the fallback's outputs, their agreement and its error (class and
// message, stored as "class: message"): its reservation released and its cost charged to the pipeline's day (an audit
// is no part of a run), with an auditEnded event that names the error's class, never its message.
func (w *wtx) endAudit(a *auditRow, state store.AuditState, fallback string, agreement *float64, class, msg string,
	cost float64,
) error {
	errText := msg
	if class != "" {
		errText = class + ": " + msg
	}
	errText = truncate(errText, store.MaxErrorMessage)
	if _, err := w.exec(`UPDATE audits SET state = ?, lease_owner = '', lease_expires_at = NULL, reserved_usd = 0,
		cost_usd = ?, fallback_output = ?, agreement = ?, error = ?, finished_at = ? WHERE pipeline = ? AND id = ?`,
		state, cost, fallback, agreement, errText, w.ts(), a.pipeline, a.id); err != nil {
		return err
	}
	if cost != 0 {
		if err := w.chargeDay(a.pipeline, cost); err != nil {
			return err
		}
	}
	data := map[string]any{"auditId": a.id, "deliveryId": a.deliveryID, "itemId": a.itemID, "step": a.step,
		"state": state}
	if agreement != nil {
		data["agreement"] = *agreement
	}
	if cost != 0 {
		data["costUsd"] = cost
	}
	if class != "" {
		data["class"] = class
	}
	return w.event(a.pipeline, "", store.EventAuditEnded, data)
}

// expireAudits ends, as failed (class leaseExpired), the leases of the leased audits cond selects, their
// reservations charged in full, and returns them with the fencing token each was held under. reason is the
// error's message, the expiry's own when empty.
func (w *wtx) expireAudits(cond string, args []any, reason string) ([]store.LeaseRef, error) {
	refs, err := queryAll(w, func(rows *sql.Rows) (store.LeaseRef, error) {
		var r store.LeaseRef
		err := rows.Scan(&r.Pipeline, &r.DeliveryID)
		return r, err
	}, `SELECT pipeline, id FROM audits WHERE state = ? AND `+cond+` ORDER BY pipeline, id`,
		append([]any{store.AuditLeased}, args...)...)
	if err != nil {
		return nil, err
	}
	for i, ref := range refs {
		a, err := w.audit(ref.Pipeline, ref.DeliveryID)
		if err != nil {
			return nil, err
		}
		msg := cmp.Or(reason, fmt.Sprintf("the lease held by %s expired before it reported", a.owner))
		if err := w.endAudit(a, store.AuditFailed, "null", nil, store.ClassLeaseExpired, msg, a.reserved); err != nil {
			return nil, err
		}
		refs[i].Step, refs[i].FencingToken = a.step, a.token
	}
	return refs, nil
}

// Audits returns audits in creation order.
func (s *Store) Audits(ctx context.Context, f store.AuditFilter) ([]store.Audit, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("id", f.ID)
	w.eq("item_id", f.ItemID)
	w.eq("delivery_id", f.DeliveryID)
	w.eq("run_id", f.RunID)
	w.eq("state", string(f.State))
	as, err := scan(ctx, s.read, `SELECT pipeline, id, delivery_id, run_id, step, item_id, state, lease_owner,
		fencing_token, primary_output, fallback_output, agreement, cost_usd, error, created_at, finished_at FROM audits`+w.String()+` ORDER BY rowid`,
		w.args, func(rows *sql.Rows) (store.Audit, error) {
			var a store.Audit
			var primary, fallback string
			var agreement sql.NullFloat64
			var created, finished TimeCol
			if err := rows.Scan(&a.Pipeline, &a.ID, &a.DeliveryID, &a.RunID, &a.Step, &a.ItemID, &a.State,
				&a.LeaseOwner, &a.FencingToken, &primary,
				&fallback, &agreement, &a.CostUSD, &a.Error, &created, &finished); err != nil {
				return a, fmt.Errorf("scan audit: %w", err)
			}
			a.Primary, a.Fallback = json.RawMessage(primary), json.RawMessage(fallback)
			if agreement.Valid {
				a.Agreement = &agreement.Float64
			}
			a.CreatedAt, a.FinishedAt = created.Time, finished.Time
			return a, nil
		})
	if err != nil {
		return nil, fmt.Errorf("audits: %w", err)
	}
	return as, nil
}
