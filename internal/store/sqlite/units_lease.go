package sqlite

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// candidate is a pending delivery Lease may pick, with what the lease returns.
type candidate struct {
	rowid int64
	lease store.Lease
}

// Lease is the Lease unit (spec section 5.11). The engine's in-memory checks (concurrency, rate, limits) arrive
// as req.Slots and req.Exclude; paused pipelines and steps are skipped from the store's state.
func (s *Store) Lease(ctx context.Context, req store.LeaseRequest) ([]store.Lease, error) {
	if req.Owner == "" || req.TTL <= 0 {
		return nil, fmt.Errorf("lease: an owner and a positive ttl are required: %w", store.ErrInvalid)
	}
	if req.Max <= 0 {
		return nil, nil
	}
	var leases []store.Lease
	err := s.submit(ctx, unitLease, func(w *wtx) error {
		queues, err := w.dueQueues(req)
		if err != nil {
			return err
		}
		expires := w.now.Add(req.TTL)
		for _, c := range w.s.roundRobin(queues, req.Max) {
			l := c.lease
			if err := w.queryRow(`UPDATE deliveries SET state = ?, lease_owner = ?, fencing_token = fencing_token + 1,
				lease_expires_at = ? WHERE pipeline = ? AND id = ? RETURNING fencing_token`,
				store.DeliveryLeased, req.Owner, formatTime(expires), l.Pipeline, l.DeliveryID).
				Scan(&l.FencingToken); err != nil {
				return fmt.Errorf("lease %s: %w", l.DeliveryID, err)
			}
			l.ExpiresAt = expires
			if err := w.event(l.Pipeline, l.RunID, store.EventDeliveryLeased, map[string]any{
				"deliveryId": l.DeliveryID, "step": l.Step, "attempt": l.Attempt, "owner": req.Owner,
				"fencingToken": l.FencingToken,
			}); err != nil {
				return err
			}
			leases = append(leases, l)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return leases, nil
}

// dueQueues returns, per pipeline, the due pending deliveries Lease may pick, oldest first, at most the step's
// slots and req.Max per step.
func (w *wtx) dueQueues(req store.LeaseRequest) (map[string][]candidate, error) {
	rows, err := w.query(`SELECT DISTINCT d.pipeline, d.step FROM deliveries d
		JOIN pipelines p ON p.id = d.pipeline AND p.state = 'active'
		LEFT JOIN steps_state ss ON ss.pipeline = d.pipeline AND ss.step = d.step
		WHERE d.state = ? AND d.next_attempt_at <= ? AND COALESCE(ss.paused, 0) = 0
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
		cs, err := w.dueDeliveries(ref, n)
		if err != nil {
			return nil, err
		}
		queues[ref.Pipeline] = append(queues[ref.Pipeline], cs...)
	}
	for _, q := range queues {
		slices.SortFunc(q, func(a, b candidate) int { return cmp.Compare(a.rowid, b.rowid) })
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

func (w *wtx) dueDeliveries(ref store.StepRef, n int) ([]candidate, error) {
	rows, err := w.query(`SELECT d.rowid, d.id, d.run_id, r.revision, d.generation, d.attempt,
		i.id, i.run_id, i.stream, i.step, i.key, i.parents, i.generation, i.ts, i.payload
		FROM deliveries d
		JOIN runs r ON r.pipeline = d.pipeline AND r.id = d.run_id
		JOIN items i ON i.pipeline = d.pipeline AND i.id = d.item_id
		WHERE d.pipeline = ? AND d.step = ? AND d.state = ? AND d.next_attempt_at <= ?
		ORDER BY d.rowid LIMIT ?`, ref.Pipeline, ref.Step, store.DeliveryPending, w.ts(), n)
	if err != nil {
		return nil, err
	}
	var out []candidate
	for rows.Next() {
		c := candidate{lease: store.Lease{Pipeline: ref.Pipeline, Step: ref.Step}}
		l := &c.lease
		var parents, ts, body string
		if err := rows.Scan(&c.rowid, &l.DeliveryID, &l.RunID, &l.Revision, &l.Generation, &l.Attempt,
			&l.Item.ID, &l.Item.RunID, &l.Item.Stream, &l.Item.Step, &l.Item.Key, &parents, &l.Item.Generation,
			&ts, &body); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan due delivery: %w", err)
		}
		l.Item.Pipeline = ref.Pipeline
		l.Item.Payload = json.RawMessage(body)
		if err := fillItem(&l.Item, parents, ts); err != nil {
			_ = rows.Close()
			return nil, err
		}
		out = append(out, c)
	}
	return out, closeRows(rows)
}

// roundRobin takes up to n candidates, one pipeline at a time, starting one pipeline further at every call so no
// pipeline is always served first (spec section 4.10).
func (s *Store) roundRobin(queues map[string][]candidate, n int) []candidate {
	names := make([]string, 0, len(queues))
	for name := range queues {
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil
	}
	slices.Sort(names)
	start := s.rr % len(names)
	s.rr++
	names = append(names[start:], names[:start]...)
	var out []candidate
	for len(out) < n {
		took := false
		for _, name := range names {
			if q := queues[name]; len(q) > 0 && len(out) < n {
				out = append(out, q[0])
				queues[name] = q[1:]
				took = true
			}
		}
		if !took {
			break
		}
	}
	return out
}

// Renew extends the leases owner still holds under the same fencing token and returns the ones it lost. Renewals
// record no event: they change no state, only an expiry.
func (s *Store) Renew(ctx context.Context, owner string, refs []store.LeaseRef, ttl time.Duration) ([]store.LeaseRef,
	error,
) {
	if ttl <= 0 {
		return nil, fmt.Errorf("renew: a positive ttl is required: %w", store.ErrInvalid)
	}
	var lost []store.LeaseRef
	err := s.submit(ctx, unitRenew, func(w *wtx) error {
		expires := formatTime(w.now.Add(ttl))
		for _, ref := range refs {
			n, err := w.affected(`UPDATE deliveries SET lease_expires_at = ? WHERE pipeline = ? AND id = ?
				AND state = ? AND lease_owner = ? AND fencing_token = ?`,
				expires, ref.Pipeline, ref.DeliveryID, store.DeliveryLeased, owner, ref.FencingToken)
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

// ExpireLeases returns every lease that expired by the store clock to pending, with its attempt unchanged. A
// delivery of a cancelled run is cancelled instead, since nothing will run it again.
func (s *Store) ExpireLeases(ctx context.Context) (int, error) {
	var count int
	err := s.submit(ctx, unitExpireLeases, func(w *wtx) error {
		type expired struct {
			d         deliveryRow
			cancelled bool
		}
		rows, err := w.query(`SELECT d.pipeline, d.id, d.run_id, d.step, d.attempt, d.lease_owner, r.state = ?
			FROM deliveries d JOIN runs r ON r.pipeline = d.pipeline AND r.id = d.run_id
			WHERE d.state = ? AND d.lease_expires_at < ? ORDER BY d.rowid`,
			store.RunCancelled, store.DeliveryLeased, w.ts())
		if err != nil {
			return err
		}
		var list []expired
		for rows.Next() {
			var e expired
			if err := rows.Scan(&e.d.pipeline, &e.d.id, &e.d.runID, &e.d.step, &e.d.attempt, &e.d.owner,
				&e.cancelled); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan expired lease: %w", err)
			}
			list = append(list, e)
		}
		if err := closeRows(rows); err != nil {
			return err
		}
		for _, e := range list {
			state := store.DeliveryPending
			if e.cancelled {
				state = store.DeliveryCancelled
			}
			if _, err := w.exec(`UPDATE deliveries SET state = ?, lease_owner = '', lease_expires_at = '',
				next_attempt_at = ? WHERE pipeline = ? AND id = ?`, state, w.ts(), e.d.pipeline, e.d.id); err != nil {
				return err
			}
			if err := w.event(e.d.pipeline, e.d.runID, store.EventLeaseExpired, map[string]any{
				"deliveryId": e.d.id, "step": e.d.step, "attempt": e.d.attempt, "owner": e.d.owner, "state": state,
			}); err != nil {
				return err
			}
			if e.cancelled {
				if err := w.settle(e.d.pipeline, e.d.runID); err != nil {
					return err
				}
			}
		}
		count = len(list)
		return nil
	})
	return count, err
}
