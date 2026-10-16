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
		for _, a := range o.Artifacts {
			if err := store.ValidArtifact(a); err != nil {
				return "", fmt.Errorf("done: output %d: %w", i, err)
			}
		}
	}
	if err := validAttemptArtifacts(d.Artifacts); err != nil {
		return "", fmt.Errorf("done: %w", err)
	}
	if err := store.ValidMetrics(d.Metrics); err != nil {
		return "", fmt.Errorf("done: %w", err)
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
	if err := w.keepAttemptArtifacts(d, req.Artifacts); err != nil {
		return "", err
	}
	run, err := w.run(d.pipeline, d.runID)
	if err != nil {
		return "", err
	}
	if run.state == store.RunCancelled {
		return w.cancelDelivery(d, req.CostUSD, req.Metrics)
	}
	return w.emit(d, run, req.Outputs, outputs, attemptEnd{cost: req.CostUSD, metrics: req.Metrics})
}

// emit ends delivery d of a live run with its outputs (spec section 5.11, Done): key rule, routing, publication, done,
// cost and settlement; a key rule or routing failure fails the attempt instead. The caller checked d's state: a
// leased delivery reporting, or a review approved.
func (w *wtx) emit(d *deliveryRow, run *runRow, outs []store.Output, outputs []payload, end attemptEnd) (
	store.DeliveryState, error,
) {
	rev, err := w.revision(run.revision)
	if err != nil {
		return "", err
	}
	step, ok := rev.stepOrOrphan(d.step)
	if !ok {
		return w.fail(d, run, rev, step, failure{class: store.ClassMissingStep, attemptEnd: end,
			message: fmt.Sprintf("step %q is not in revision %s: its outputs have nowhere to go", d.step, rev.hash)})
	}
	if msg := keyRule(outs); msg != "" {
		return w.fail(d, run, rev, step, failure{class: store.ClassKeyRule, message: msg, attemptEnd: end})
	}
	routes := make([]routing, len(outputs))
	for i, o := range outputs {
		if routes[i], err = rev.route(step, o.value, run.vars, run.exprRun(rev)); err != nil {
			return w.fail(d, run, rev, step, failure{class: store.ClassRoute, message: err.Error(), attemptEnd: end})
		}
	}
	for i, o := range outputs {
		if err := w.publishOutput(rev, run, step, d, outs[i], i, o, routes[i]); err != nil {
			return "", err
		}
	}
	if _, err := w.exec(`UPDATE deliveries SET state = ?, lease_owner = '', lease_expires_at = ''
		WHERE pipeline = ? AND id = ?`, store.DeliveryDone, d.pipeline, d.id); err != nil {
		return "", err
	}
	if err := w.addCost(d, end.cost); err != nil {
		return "", err
	}
	data, err := w.endAttempt(d, true, end)
	if err != nil {
		return "", err
	}
	data["outputs"] = len(outputs)
	if err := w.event(d.pipeline, d.runID, store.EventDeliveryDone, data); err != nil {
		return "", err
	}
	return store.DeliveryDone, w.settle(d.pipeline, d.runID)
}

// validAttemptArtifacts checks an attempt's artifacts: valid references with distinct names.
func validAttemptArtifacts(refs []store.ArtifactRef) error {
	seen := make(map[string]bool, len(refs))
	for _, a := range refs {
		if err := store.ValidArtifact(a); err != nil {
			return fmt.Errorf("attempt artifact: %w", err)
		}
		if seen[a.Name] {
			return fmt.Errorf("attempt artifact %s: named twice: %w", a.Name, store.ErrInvalid)
		}
		seen[a.Name] = true
	}
	return nil
}

// keepAttemptArtifacts records the artifacts of the leased delivery's current attempt, each one reference.
func (w *wtx) keepAttemptArtifacts(d *deliveryRow, refs []store.ArtifactRef) error {
	for _, a := range refs {
		if _, err := w.exec(`INSERT INTO attempt_artifacts
			(pipeline, delivery_id, attempt, name, sha256, size, type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			d.pipeline, d.id, d.attempt, a.Name, a.SHA256, a.Size, a.Type, w.ts()); err != nil {
			return fmt.Errorf("keep attempt artifact %s: %w", a.Name, err)
		}
	}
	return w.countArtifacts(d.pipeline, refs)
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

// publishOutput publishes output index of delivery d, whose payload is o, where its routing sends it, or as an
// unrouted dead letter that keeps its artifacts for a replay.
func (w *wtx) publishOutput(rev *revision, run *runRow, step pipeline.Step, d *deliveryRow, out store.Output,
	index int, o payload, r routing,
) error {
	key := out.Key
	if r.unrouted {
		outputID := ids.Item(step.Name, "", d.itemID, d.generation, key, index)
		item := map[string]any{"id": outputID, "key": key, "index": index, "payload": o.value}
		if len(out.Artifacts) > 0 {
			item["artifacts"] = out.Artifacts
		}
		return w.deadLetter(rev, run, step, store.DeadLetterUnrouted, d.id, outputID, d.itemID, unroutedError, item)
	}
	for _, stream := range r.streams {
		err := w.publish(rev, run, newItem{
			id: ids.Item(step.Name, stream, d.itemID, d.generation, key, index), stream: stream, step: step.Name,
			key: key, parents: []string{d.itemID}, payload: o, artifacts: out.Artifacts,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// cancelDelivery ends a leased delivery of a cancelled run: no outputs, its cost and metrics recorded (spec section
// 5.3).
func (w *wtx) cancelDelivery(d *deliveryRow, cost float64, metrics []store.Metric) (store.DeliveryState, error) {
	if _, err := w.exec(`UPDATE deliveries SET state = ?, lease_owner = '', lease_expires_at = ''
		WHERE pipeline = ? AND id = ?`, store.DeliveryCancelled, d.pipeline, d.id); err != nil {
		return "", err
	}
	if err := w.addCost(d, cost); err != nil {
		return "", err
	}
	data, err := w.endAttempt(d, true, attemptEnd{cost: cost, metrics: metrics})
	if err != nil {
		return "", err
	}
	if err := w.event(d.pipeline, d.runID, store.EventDeliveryCancelled, data); err != nil {
		return "", err
	}
	return store.DeliveryCancelled, w.settle(d.pipeline, d.runID)
}
