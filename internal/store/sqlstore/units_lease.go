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
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// candidate is a pending delivery Lease may pick, with what the lease returns.
type candidate struct {
	rowid int64
	due   time.Time // next_attempt_at: when the attempt became due, which its queue wait counts from
	lease store.Lease
}

// Lease is the Lease unit (spec section 5.11). The engine's in-memory checks (concurrency, rate, limits) arrive as
// req.Slots and req.Exclude; paused pipelines and steps are skipped from the store's state. An agent delivery (its step
// and revision in req.Reserve) is leased with its budget reservation, only while the budgets allow it (budgetAllows).
func (s *Store) Lease(ctx context.Context, req store.LeaseRequest) ([]store.Lease, error) {
	if req.Owner == "" || req.TTL <= 0 {
		return nil, fmt.Errorf("lease: an owner and a positive ttl are required: %w", store.ErrInvalid)
	}
	if req.Max <= 0 {
		return nil, nil
	}
	var leases []store.Lease
	err := s.submit(ctx, unitLease, func(w *wtx) error {
		var err error
		leases, err = w.lease(req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return leases, nil
}

// leasePass is what one pass of the Lease unit has left to lease, and what a budget held back. A denial holds for
// the rest of the lease, spend only growing within it: a run budget holds back every delivery of the run's step, a
// day budget (or a pipeline a budget paused) every delivery of the step of that revision, whose maxCostUsd the
// reservation is. The next passes exclude them in the due query, so held-back deliveries are read once.
type leasePass struct {
	req        store.LeaseRequest
	deniedRuns map[store.StepRef][]string           // run ids per step
	deniedRevs map[store.StepRef][]store.RevisionID // revisions per step
	paused     map[string]bool                      // pipelines a budget paused during this lease
}

// deny records that the budget scope (store.BudgetRun or store.BudgetDay) held back l, a delivery of ref.
func (p *leasePass) deny(ref store.StepRef, l store.Lease, scope string) {
	if scope == store.BudgetRun {
		p.deniedRuns[ref] = append(p.deniedRuns[ref], l.RunID)
	} else {
		p.deniedRevs[ref] = append(p.deniedRevs[ref], l.Revision)
	}
}

// take counts one lease of ref against what the pass has left: its Max, the step's slots and the pipeline's.
func (p *leasePass) take(ref store.StepRef) {
	p.req.Max--
	if _, ok := p.req.Slots[ref]; ok {
		p.req.Slots[ref]--
	}
	if _, ok := p.req.PipelineSlots[ref.Pipeline]; ok {
		p.req.PipelineSlots[ref.Pipeline]--
	}
}

// heldBack reports whether a budget already held back a delivery like l, of ref, during this lease.
func (p *leasePass) heldBack(ref store.StepRef, l store.Lease) bool {
	return slices.Contains(p.deniedRuns[ref], l.RunID) || slices.Contains(p.deniedRevs[ref], l.Revision)
}

// lease leases due deliveries in passes: a delivery a budget holds back does not use its step's slot, so the next
// pass looks past it, excluding what the budgets denied, until a pass is denied nothing or nothing is left to lease.
// Every pass but the last adds a denial, so the passes are bounded by the runs and revisions held back, not by
// their deliveries.
func (w *wtx) lease(req store.LeaseRequest) ([]store.Lease, error) {
	p := &leasePass{req: req, deniedRuns: map[store.StepRef][]string{},
		deniedRevs: map[store.StepRef][]store.RevisionID{}, paused: map[string]bool{}}
	if req.Slots != nil {
		p.req.Slots = maps.Clone(req.Slots)
	}
	if req.PipelineSlots != nil {
		p.req.PipelineSlots = maps.Clone(req.PipelineSlots)
	}
	var leases []store.Lease
	expires := w.now.Add(req.TTL)
	for p.req.Max > 0 {
		queues, err := w.dueQueues(p)
		if err != nil {
			return nil, err
		}
		if hook := w.s.leasePassHook; hook != nil {
			hook(queues)
		}
		skipped := false
		for _, c := range w.s.roundRobin(queues, p.req.Max) {
			l := c.lease
			ref := store.StepRef{Pipeline: l.Pipeline, Step: l.Step}
			reserve, agent := req.Reserve[store.AgentStep{StepRef: ref, Revision: l.Revision, Fallback: l.Fallback}]
			if agent {
				if p.heldBack(ref, l) {
					skipped = true
					continue
				}
				scope, err := w.budgetAllows(l, reserve, req.Reserve, p.paused)
				if err != nil {
					return nil, err
				}
				if scope != "" { // stays pending until the budget allows it, once a user resumed the step
					p.deny(ref, l, scope)
					skipped = true
					continue
				}
			}
			wait := millis(w.now.Sub(c.due))
			if err := w.queryRow(`UPDATE deliveries SET state = ?, lease_owner = ?, fencing_token = fencing_token + 1,
				lease_expires_at = ?, reserved_usd = ?, leased_at = ?, wait_ms = wait_ms + ?
				WHERE pipeline = ? AND id = ? RETURNING fencing_token`,
				store.DeliveryLeased, req.Owner, formatTime(expires), reserve, w.ts(), wait, l.Pipeline, l.DeliveryID).
				Scan(&l.FencingToken); err != nil {
				return nil, fmt.Errorf("lease %s: %w", l.DeliveryID, err)
			}
			l.ExpiresAt = expires
			data := map[string]any{
				"deliveryId": l.DeliveryID, "itemId": l.Item.ID, "step": l.Step, "attempt": l.Attempt, "owner": req.Owner,
				"fencingToken": l.FencingToken, "waitMs": wait,
			}
			if reserve > 0 {
				data["reservedUsd"] = reserve
			}
			if err := w.event(l.Pipeline, l.RunID, store.EventDeliveryLeased, data); err != nil {
				return nil, err
			}
			leases = append(leases, l)
			p.take(ref)
		}
		if !skipped {
			break
		}
	}
	if p.req.Max > 0 {
		audits, err := w.leaseAudits(p, expires)
		if err != nil {
			return nil, err
		}
		leases = append(leases, audits...)
	}
	return leases, nil
}

// dueQueues returns, per pipeline, the due pending deliveries the pass may pick, oldest first, at most the step's
// slots and the pass's Max per step, past the runs and revisions a budget denied.
func (w *wtx) dueQueues(p *leasePass) (map[string][]candidate, error) {
	req := p.req
	rows, err := w.query(`SELECT DISTINCT d.pipeline, d.step FROM deliveries d
		JOIN pipelines p ON p.id = d.pipeline AND p.state = 'active'
		LEFT JOIN steps_state ss ON ss.pipeline = d.pipeline AND ss.step = d.step
		WHERE d.state = ? AND d.next_attempt_at <= ? AND COALESCE(ss.paused, FALSE) = FALSE
		ORDER BY d.pipeline, d.step`, store.DeliveryPending, w.ts())
	if err != nil {
		return nil, err
	}
	var steps []store.StepRef
	for rows.Next() {
		var ref store.StepRef
		if err := rows.Scan(&ref.Pipeline, &ref.Step); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan due step: %w", err)
		}
		steps = append(steps, ref)
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	queues := map[string][]candidate{}
	for _, ref := range steps {
		if excluded(req.Exclude, ref) {
			continue
		}
		n := req.Max
		if slots, ok := req.Slots[ref]; ok {
			n = min(n, slots)
		}
		if n <= 0 {
			continue
		}
		cs, err := w.dueDeliveries(ref, n, p.deniedRuns[ref], p.deniedRevs[ref])
		if err != nil {
			return nil, err
		}
		queues[ref.Pipeline] = append(queues[ref.Pipeline], cs...)
	}
	for name, q := range queues {
		slices.SortFunc(q, func(a, b candidate) int { return cmp.Compare(a.rowid, b.rowid) })
		if slots, ok := req.PipelineSlots[name]; ok {
			queues[name] = q[:min(len(q), max(slots, 0))]
		}
	}
	return queues, nil
}

func excluded(exclude []store.StepRef, ref store.StepRef) bool {
	for _, e := range exclude {
		if e.Pipeline == ref.Pipeline && (e.Step == "" || e.Step == ref.Step) {
			return true
		}
	}
	return false
}

// dueDeliveries returns the step's n oldest due deliveries, but those of the runs and revisions given.
func (w *wtx) dueDeliveries(ref store.StepRef, n int, runs []string, revs []store.RevisionID) ([]candidate, error) {
	rows, err := w.query(`SELECT d.rowid, d.next_attempt_at, d.id, d.run_id, r.revision, d.generation, d.attempt,
		d.fallback, i.id, i.run_id, i.stream, i.step, i.key, i.parents, i.generation, i.ts, i.payload, i.artifacts
		FROM deliveries d
		JOIN runs r ON r.pipeline = d.pipeline AND r.id = d.run_id
		JOIN items i ON i.pipeline = d.pipeline AND i.id = d.item_id
		WHERE d.pipeline = ? AND d.step = ? AND d.state = ? AND d.next_attempt_at <= ?
			AND d.run_id NOT IN (`+w.s.d.JSONParam(false)+`) AND r.revision NOT IN (`+w.s.d.JSONParam(true)+`)
		ORDER BY d.rowid`+limitParam, ref.Pipeline, ref.Step, store.DeliveryPending, w.ts(), jsonList(runs),
		jsonList(revs), n)
	if err != nil {
		return nil, err
	}
	var out []candidate
	for rows.Next() {
		c := candidate{lease: store.Lease{Pipeline: ref.Pipeline, Step: ref.Step}}
		l := &c.lease
		var parents, body, artifacts string
		var due, ts TimeCol
		if err := rows.Scan(&c.rowid, &due, &l.DeliveryID, &l.RunID, &l.Revision, &l.Generation, &l.Attempt,
			&l.Fallback, &l.Item.ID, &l.Item.RunID, &l.Item.Stream, &l.Item.Step, &l.Item.Key, &parents, &l.Item.Generation,
			&ts, &body, &artifacts); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan due delivery: %w", err)
		}
		l.Item.Pipeline = ref.Pipeline
		l.Item.Payload = json.RawMessage(body)
		c.due = due.Time
		if err := fillItem(&l.Item, parents, ts, artifacts); err != nil {
			_ = rows.Close()
			return nil, err
		}
		out = append(out, c)
	}
	return out, closeRows(rows)
}

// jsonList encodes vs as a JSON array for Dialect.JSONParam, [] when empty: SQLite's json_each of null yields a
// row, which NOT IN would compare as unknown and so exclude everything.
func jsonList[T any](vs []T) string {
	if len(vs) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(vs) // run ids and revision ids always encode
	return string(b)
}

// roundRobin takes up to n candidates, one pipeline at a time in name order, carrying on after the pipeline served
// last, by this call or an earlier one (spec section 4.10). Only pipelines with candidates take part, so one at its
// limit, or out of rate tokens, drops out of the rotation without handing its turn to the next one.
func (s *Store) roundRobin(queues map[string][]candidate, n int) []candidate {
	names := make([]string, 0, len(queues))
	for name, q := range queues {
		if len(q) > 0 {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	start := max(slices.IndexFunc(names, func(name string) bool { return name > s.rrLast }), 0)
	names = append(names[start:], names[:start]...)
	var out []candidate
	for len(out) < n {
		took := false
		for _, name := range names {
			if q := queues[name]; len(q) > 0 && len(out) < n {
				out = append(out, q[0])
				queues[name] = q[1:]
				s.rrLast, took = name, true
			}
		}
		if !took {
			break
		}
	}
	return out
}

// Renew extends the leases owner still holds under the same fencing token and returns the ones it lost, and records
// that the owner, a remote node, was heard from; a revoked one is ErrRevoked. Renewals record no event: they change no state, only an expiry.
func (s *Store) Renew(ctx context.Context, owner string, refs []store.LeaseRef, ttl time.Duration) ([]store.LeaseRef,
	error,
) {
	if ttl <= 0 {
		return nil, fmt.Errorf("renew: a positive ttl is required: %w", store.ErrInvalid)
	}
	var lost []store.LeaseRef
	err := s.submit(ctx, unitRenew, func(w *wtx) error {
		expires := formatTime(w.now.Add(ttl))
		var state store.NodeState
		switch err := w.queryRow(`SELECT state FROM nodes WHERE id = ?`, owner).Scan(&state); {
		case errors.Is(err, sql.ErrNoRows): // the embedded node: no row
		case err != nil:
			return fmt.Errorf("renew: node %s: %w", owner, err)
		case state == store.NodeRevoked:
			return fmt.Errorf("renew: node %s: %w", owner, store.ErrRevoked)
		default:
			if _, err := w.exec(`UPDATE nodes SET last_heartbeat = ? WHERE id = ?`, w.ts(), owner); err != nil {
				return err
			}
		}
		for _, ref := range refs {
			table, state := "deliveries", string(store.DeliveryLeased)
			if ids.IsAudit(ref.DeliveryID) {
				table, state = "audits", string(store.AuditLeased)
			}
			n, err := w.affected(`UPDATE `+table+` SET lease_expires_at = ? WHERE pipeline = ? AND id = ?
				AND state = ? AND lease_owner = ? AND fencing_token = ?`,
				expires, ref.Pipeline, ref.DeliveryID, state, owner, ref.FencingToken)
			if err != nil {
				return err
			}
			if n == 0 {
				lost = append(lost, ref)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lost, nil
}

// ExpireLeases ends every lease that expired by the store clock and returns them. An expired lease consumes its
// attempt (spec section 5.3): the delivery is retried after backoff while attempts remain, and is dead with its dead
// letter (class leaseExpired) once they are exhausted. A delivery of a cancelled run is cancelled instead, since
// nothing will run it again.
func (s *Store) ExpireLeases(ctx context.Context) ([]store.LeaseRef, error) {
	var expired []store.LeaseRef
	err := s.submit(ctx, unitExpireLeases, func(w *wtx) error {
		var err error
		if expired, err = w.expireWhere(`lease_expires_at < ?`, []any{w.ts()}, ""); err != nil {
			return err
		}
		audits, err := w.expireAudits(`lease_expires_at < ?`, []any{w.ts()}, "")
		expired = append(expired, audits...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return expired, nil
}

// expireWhere ends, as expired, the leases of the leased deliveries cond selects (cond's arguments args), and returns
// them with the fencing token each was held under. reason is the attempt's error message, the expiry's own when
// empty.
func (w *wtx) expireWhere(cond string, args []any, reason string) ([]store.LeaseRef, error) {
	rows, err := w.query(`SELECT id, pipeline FROM deliveries WHERE state = ? AND `+cond+` ORDER BY rowid`,
		append([]any{store.DeliveryLeased}, args...)...)
	if err != nil {
		return nil, err
	}
	var refs []store.LeaseRef
	for rows.Next() {
		var ref store.LeaseRef
		if err := rows.Scan(&ref.DeliveryID, &ref.Pipeline); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan expired lease: %w", err)
		}
		refs = append(refs, ref)
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	expired := make([]store.LeaseRef, 0, len(refs))
	for _, ref := range refs {
		d, err := w.delivery(ref.Pipeline, ref.DeliveryID)
		if err != nil {
			return nil, err
		}
		if err := w.expire(d, reason); err != nil {
			return nil, err
		}
		expired = append(expired, store.LeaseRef{
			Pipeline: d.pipeline, DeliveryID: d.id, Step: d.step, FencingToken: d.token,
		})
	}
	return expired, nil
}

// expire ends the expired lease of d as a failed attempt, or cancels d when its run is cancelled or a purge fenced its
// item. Its budget
// reservation is charged in full: the owner may have spent it without reporting (spec section 5.9).
func (w *wtx) expire(d *deliveryRow, reason string) error {
	run, err := w.run(d.pipeline, d.runID)
	if err != nil {
		return err
	}
	if err := w.event(d.pipeline, d.runID, store.EventLeaseExpired, map[string]any{
		"deliveryId": d.id, "itemId": d.itemID, "step": d.step, "attempt": d.attempt, "owner": d.owner,
		"chargedUsd": d.reserved,
	}); err != nil {
		return err
	}
	if discards(d, run) {
		_, err := w.cancelDelivery(d, d.reserved, nil)
		return err
	}
	rev, err := w.revision(run.revision)
	if err != nil {
		return err
	}
	step, _ := rev.stepOrOrphan(d.step)
	_, err = w.fail(d, run, rev, step, failure{
		class: store.ClassLeaseExpired, retryable: true, attemptEnd: attemptEnd{cost: d.reserved},
		message: cmp.Or(reason, fmt.Sprintf("the lease held by %s expired before it reported", d.owner)),
	})
	return err
}
