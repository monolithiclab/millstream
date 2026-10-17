package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/monolithiclab/millstream/internal/store"
)

// Filter is the Filter unit: fencing check, then the delivery is filtered with its reason and its run settled. A
// delivery of a cancelled run is cancelled instead.
func (s *Store) Filter(ctx context.Context, f store.FilterRequest) (store.DeliveryState, error) {
	var state store.DeliveryState
	err := s.submit(ctx, unitFilter, func(w *wtx) error {
		d, _, cancelled, err := w.leasedLive(f.LeaseRef, f.Attempt)
		if err != nil || cancelled {
			state = store.DeliveryCancelled
			return err
		}
		state, err = w.filter(d, f.Reason)
		return err
	})
	return state, err
}

// Dedupe is the Dedupe unit (spec section 5.11): fencing check; the key is looked up for the delivery's pipeline,
// step and generation; absent, or let through longer ago than the window, it passes: the input item is published
// unchanged as by Done, and the key records it with the batch instant; otherwise the delivery is filtered as a
// duplicate. A delivery of a cancelled run is cancelled instead.
func (s *Store) Dedupe(ctx context.Context, req store.DedupeRequest) (store.DeliveryState, error) {
	if req.Window < 0 {
		return "", fmt.Errorf("dedupe: negative window: %w", store.ErrInvalid)
	}
	var state store.DeliveryState
	err := s.submit(ctx, unitDedupe, func(w *wtx) error {
		d, run, cancelled, err := w.leasedLive(req.LeaseRef, req.Attempt)
		if err != nil || cancelled {
			state = store.DeliveryCancelled
			return err
		}
		var seenAt TimeCol
		err = w.queryRow(`SELECT seen_at FROM dedupe_keys WHERE pipeline = ? AND step = ? AND generation = ? AND key = ?`,
			d.pipeline, d.step, d.generation, req.Key).Scan(&seenAt)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return fmt.Errorf("read dedupe key: %w", err)
		case req.Window == 0 || !seenAt.Before(w.now.Add(-req.Window)):
			state, err = w.filter(d, store.FilterReasonDuplicate)
			return err
		}
		out, o, err := w.passItem(d)
		if err != nil {
			return err
		}
		if state, err = w.emit(d, run, []store.Output{out}, []payload{o}, attemptEnd{}); err != nil ||
			state != store.DeliveryDone {
			return err
		}
		_, err = w.exec(`INSERT INTO dedupe_keys (pipeline, step, generation, key, first_item, seen_at)
			VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (pipeline, step, generation, key) DO UPDATE SET
			first_item = excluded.first_item, seen_at = excluded.seen_at`,
			d.pipeline, d.step, d.generation, req.Key, d.itemID, w.ts())
		return err
	})
	return state, err
}

// leasedLive returns the delivery a builtin unit names, and its run, refused unless still leased under the same fencing token and
// attempt. A delivery of a cancelled run or of an item a purge fenced is cancelled on the spot, and cancelled is true.
func (w *wtx) leasedLive(ref store.LeaseRef, attempt int) (d *deliveryRow, run *runRow, cancelled bool, err error) {
	if d, err = w.leased(ref, attempt, ""); err != nil {
		return nil, nil, false, err
	}
	if run, err = w.run(d.pipeline, d.runID); err != nil {
		return nil, nil, false, err
	}
	if discards(d, run) {
		_, err = w.cancelDelivery(d, 0, nil)
		return nil, nil, true, err
	}
	return d, run, false, nil
}

// filter marks a leased delivery filtered with its reason and settles its run.
func (w *wtx) filter(d *deliveryRow, reason string) (store.DeliveryState, error) {
	if _, err := w.exec(`UPDATE deliveries SET state = ?, lease_owner = '', lease_expires_at = NULL, filter_reason = ?
		WHERE pipeline = ? AND id = ?`, store.DeliveryFiltered, reason, d.pipeline, d.id); err != nil {
		return "", err
	}
	data, err := w.endAttempt(d, true, attemptEnd{})
	if err != nil {
		return "", err
	}
	data["reason"] = reason
	if err := w.event(d.pipeline, d.runID, store.EventDeliveryFiltered, data); err != nil {
		return "", err
	}
	return store.DeliveryFiltered, w.settle(d.pipeline, d.runID)
}
