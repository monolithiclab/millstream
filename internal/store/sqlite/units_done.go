package sqlite

import (
	"context"
	"fmt"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// unroutedError is the error an unrouted dead letter carries.
const unroutedError = "no route matched the output"

// Done is the Done unit (spec section 5.11): fencing check, key rule, routing, first-commit publication,
// consumer deliveries, done, settlement. A delivery of a cancelled run is cancelled with its cost instead, and
// its outputs are discarded.
func (s *Store) Done(ctx context.Context, d store.DoneRequest) (store.DeliveryState, error) {
	outputs := make([]payload, len(d.Outputs))
	for i, o := range d.Outputs {
		var err error
		if outputs[i], err = canonical(o.Payload); err != nil {
			return "", fmt.Errorf("done: output %d: %w", i, err)
		}
	}
	var state store.DeliveryState
	err := s.submit(ctx, unitDone, func(w *wtx) error {
		var err error
		state, err = w.done(d, outputs)
		return err
	})
	return state, err
}

func (w *wtx) done(req store.DoneRequest, outputs []payload) (store.DeliveryState, error) {
	d, err := w.leased(req.LeaseRef, req.Attempt)
	if err != nil {
		return "", err
	}
	run, err := w.run(d.pipeline, d.runID)
	if err != nil {
		return "", err
	}
	if run.state == store.RunCancelled {
		return w.cancelDelivery(d, req.CostUSD)
	}
	rev, err := w.revision(run.revision)
	if err != nil {
		return "", err
	}
	step, err := rev.step(d.step)
	if err != nil {
		return "", err
	}
	if msg := keyRule(req.Outputs); msg != "" {
		return w.fail(d, run, rev, step, failure{class: store.ClassKeyRule, message: msg, cost: req.CostUSD})
	}
	routes := make([]routing, len(outputs))
	for i, o := range outputs {
		if routes[i], err = rev.route(step, o.value, run.vars, run.exprRun(rev)); err != nil {
			return w.fail(d, run, rev, step, failure{class: store.ClassRoute, message: err.Error(), cost: req.CostUSD})
		}
	}
	for i, o := range outputs {
		if err := w.publishOutput(rev, run, step, d, req.Outputs[i].Key, i, o, routes[i]); err != nil {
			return "", err
		}
	}
	if _, err := w.exec(`UPDATE deliveries SET state = ?, lease_owner = '', lease_expires_at = ''
		WHERE pipeline = ? AND id = ?`, store.DeliveryDone, d.pipeline, d.id); err != nil {
		return "", err
	}
	if err := w.addCost(d, req.CostUSD); err != nil {
		return "", err
	}
	if err := w.event(d.pipeline, d.runID, store.EventDeliveryDone, map[string]any{
		"deliveryId": d.id, "step": d.step, "attempt": d.attempt, "outputs": len(outputs),
	}); err != nil {
		return "", err
	}
	return store.DeliveryDone, w.settle(d.pipeline, d.runID)
}

// keyRule returns why the outputs break the key rule (spec section 5.2), or "" when they keep it: several
// outputs must all be keyed, with distinct keys, since an output's index is only stable for a single output and
// two outputs under one key would land on one item id.
func keyRule(outputs []store.Output) string {
	if len(outputs) < 2 {
		return ""
	}
	seen := make(map[string]int, len(outputs))
	for i, o := range outputs {
		if o.Key == "" {
			return fmt.Sprintf("output %d of %d has no key: a delivery with several outputs must key every one",
				i, len(outputs))
		}
		if j, dup := seen[o.Key]; dup {
			return fmt.Sprintf("outputs %d and %d share the key %q: the keys of a delivery's outputs must be distinct",
				j, i, o.Key)
		}
		seen[o.Key] = i
	}
	return ""
}

// publishOutput publishes output index of delivery d where its routing sends it, or as an unrouted dead letter.
func (w *wtx) publishOutput(rev *revision, run *runRow, step pipeline.Step, d *deliveryRow, key string, index int,
	o payload, r routing,
) error {
	if r.unrouted {
		outputID := ids.Item(step.Name, "", d.itemID, d.generation, key, index)
		item := map[string]any{"id": outputID, "key": key, "index": index, "payload": o.value}
		return w.deadLetter(rev, run, step, store.DeadLetterUnrouted, d.id, outputID, d.itemID, unroutedError, item)
	}
	for _, stream := range r.streams {
		err := w.publish(rev, run, newItem{
			id: ids.Item(step.Name, stream, d.itemID, d.generation, key, index), stream: stream, step: step.Name,
			key: key, parents: []string{d.itemID}, payload: o,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// cancelDelivery ends a delivery of a cancelled run: no outputs, its cost recorded (spec section 5.3).
func (w *wtx) cancelDelivery(d *deliveryRow, cost float64) (store.DeliveryState, error) {
	if _, err := w.exec(`UPDATE deliveries SET state = ?, lease_owner = '', lease_expires_at = ''
		WHERE pipeline = ? AND id = ?`, store.DeliveryCancelled, d.pipeline, d.id); err != nil {
		return "", err
	}
	if err := w.addCost(d, cost); err != nil {
		return "", err
	}
	if err := w.event(d.pipeline, d.runID, store.EventDeliveryCancelled,
		map[string]any{"deliveryId": d.id, "step": d.step, "attempt": d.attempt}); err != nil {
		return "", err
	}
	return store.DeliveryCancelled, w.settle(d.pipeline, d.runID)
}
