package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/schema"
	"github.com/monolithiclab/millstream/internal/store"
)

// Park is the Park unit: see store.Store. A parked delivery holds no lease and no slot: only its review row and its
// deadline remain, and its run is waitingReview once nothing else is live.
func (s *Store) Park(ctx context.Context, req store.ParkRequest) (store.ParkResult, error) {
	if req.Spec.Inbox == "" {
		return store.ParkResult{}, fmt.Errorf("park: no inbox: %w", store.ErrInvalid)
	}
	spec, err := json.Marshal(req.Spec)
	if err != nil {
		return store.ParkResult{}, fmt.Errorf("park: encode the review spec: %w", err)
	}
	var res store.ParkResult
	err = s.submit(ctx, unitPark, func(w *wtx) error {
		res = store.ParkResult{}
		d, run, cancelled, err := w.leasedLive(req.LeaseRef, req.Attempt)
		if err != nil || cancelled {
			res.State = store.DeliveryCancelled
			return err
		}
		r := newReview{d: d, inbox: req.Spec.Inbox, spec: string(spec), display: req.Display}
		if req.AutoApprove {
			res.State, err = w.autoApprove(d, run, r, req)
			return err
		}
		data, err := w.endAttempt(d, false, attemptEnd{})
		if err != nil {
			return err
		}
		// The attempt ended here: a decision later adds no work time to it (endAttempt with no leasedAt).
		if _, err := w.exec(`UPDATE deliveries SET state = ?, lease_owner = '', lease_expires_at = NULL,
			leased_at = NULL WHERE pipeline = ? AND id = ?`, store.DeliveryAwaitingReview, d.pipeline, d.id); err != nil {
			return err
		}
		deadline := w.deadline(req.Spec.Timeout)
		r.state, r.deadline = store.ReviewOpen, deadline
		if err := w.upsertReview(r); err != nil {
			return err
		}
		data["inbox"] = req.Spec.Inbox
		if !deadline.IsZero() {
			data["deadline"] = formatTime(deadline)
			res.Deadline = deadline
		}
		if err := w.event(d.pipeline, d.runID, store.EventReviewParked, data); err != nil {
			return err
		}
		res.State = store.DeliveryAwaitingReview
		return w.settle(d.pipeline, d.runID)
	})
	return res, err
}

// newReview is a review row to write when a delivery parks or is auto-approved.
type newReview struct {
	d                    *deliveryRow
	inbox, spec, display string
	state                store.ReviewState
	decision, reviewer   string
	deadline, when       time.Time // when: the decision's instant, zero while open
}

// upsertReview writes a review. A delivery parked again after a dead-letter replay starts its review afresh, at a
// new version.
func (w *wtx) upsertReview(r newReview) error {
	d := r.d
	_, err := w.exec(`INSERT INTO reviews (pipeline, delivery_id, run_id, step, item_id, inbox, state, version, spec_json,
		display, parked_at, deadline, decision, reviewer, decided_at) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (pipeline, delivery_id) DO UPDATE SET inbox = excluded.inbox, state = excluded.state,
		version = reviews.version + 1,
		spec_json = excluded.spec_json, display = excluded.display, parked_at = excluded.parked_at,
		deadline = excluded.deadline, decision = excluded.decision, reviewer = excluded.reviewer, reason = '',
		reason_text = '', patch = '[]', decided_at = excluded.decided_at`,
		d.pipeline, d.id, d.runID, d.step, d.itemID, r.inbox, r.state, r.spec, r.display, w.ts(), nullTime(r.deadline),
		r.decision, r.reviewer, nullTime(r.when))
	return err
}

// autoApprove passes an auto-approved item on as Done does, records the rule's approval and, when sampled, opens a
// review sample.
func (w *wtx) autoApprove(d *deliveryRow, run *runRow, r newReview, req store.ParkRequest) (store.DeliveryState,
	error,
) {
	out, o, err := w.passItem(d)
	if err != nil {
		return "", err
	}
	state, err := w.emit(d, run, []store.Output{out}, []payload{o}, attemptEnd{})
	if err != nil || state != store.DeliveryDone {
		return state, err
	}
	r.state, r.decision, r.reviewer, r.when = store.ReviewDecided, store.DecisionApprove, review.ReviewerRule, w.now
	if err := w.upsertReview(r); err != nil {
		return "", err
	}
	if err := w.event(d.pipeline, d.runID, store.EventReviewDecided, map[string]any{
		"deliveryId": d.id, "itemId": d.itemID, "step": d.step, "inbox": req.Spec.Inbox,
		"decision": store.DecisionApprove, "reviewer": review.ReviewerRule, "timeToDecisionMs": 0,
	}); err != nil {
		return "", err
	}
	if !req.Sample {
		return state, nil
	}
	if _, err := w.exec(`INSERT INTO review_samples (pipeline, delivery_id, run_id, step, item_id, inbox, state, display,
		created_at, deadline) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		d.pipeline, d.id, d.runID, d.step, d.itemID, req.Spec.Inbox, store.SampleOpen, req.Display, w.ts(),
		nullTime(w.deadline(req.Spec.Timeout))); err != nil {
		return "", err
	}
	return state, w.event(d.pipeline, d.runID, store.EventReviewSampled,
		map[string]any{"deliveryId": d.id, "itemId": d.itemID, "step": d.step, "inbox": req.Spec.Inbox})
}

// deadline is the batch instant plus timeout, zero without a timeout.
func (w *wtx) deadline(timeout time.Duration) time.Time {
	if timeout <= 0 {
		return time.Time{}
	}
	return w.now.Add(timeout)
}

// passItem returns delivery d's input item as the output that passes it on unchanged: key, payload, artifacts.
func (w *wtx) passItem(d *deliveryRow) (store.Output, payload, error) {
	var key, body, artifacts string
	if err := w.queryRow(`SELECT key, payload, artifacts FROM items WHERE pipeline = ? AND id = ?`, d.pipeline,
		d.itemID).Scan(&key, &body, &artifacts); err != nil {
		return store.Output{}, payload{}, notFound(err, "item "+d.itemID)
	}
	o, err := canonical([]byte(body))
	if err != nil {
		return store.Output{}, payload{}, err
	}
	out := store.Output{Key: key, Payload: o.raw}
	if err := json.Unmarshal([]byte(artifacts), &out.Artifacts); err != nil {
		return store.Output{}, payload{}, fmt.Errorf("item %s artifacts: %w", d.itemID, err)
	}
	return out, o, nil
}

// reviewRow is a review as the units read it.
type reviewRow struct {
	pipeline, deliveryID, runID, step, inbox string
	itemID                                   string
	state                                    store.ReviewState
	version                                  int64
	spec                                     store.ReviewSpec
	display                                  string
	parkedAt, deadline, decidedAt            time.Time
	decision, reviewer                       string
}

func (w *wtx) review(pipelineName, deliveryID string) (*reviewRow, error) {
	r := &reviewRow{pipeline: pipelineName, deliveryID: deliveryID}
	var spec string
	var parked, deadline, decided TimeCol
	err := w.queryRow(`SELECT run_id, step, item_id, inbox, state, version, spec_json, display, parked_at, deadline,
		decision, reviewer, decided_at FROM reviews WHERE pipeline = ? AND delivery_id = ?`, pipelineName, deliveryID).
		Scan(&r.runID, &r.step, &r.itemID, &r.inbox, &r.state, &r.version, &spec, &r.display, &parked, &deadline,
			&r.decision, &r.reviewer, &decided)
	if err != nil {
		return nil, notFound(err, "review of delivery "+deliveryID)
	}
	if err := json.Unmarshal([]byte(spec), &r.spec); err != nil {
		return nil, fmt.Errorf("review of delivery %s: %w", deliveryID, err)
	}
	r.parkedAt, r.deadline, r.decidedAt = parked.Time, deadline.Time, decided.Time
	return r, nil
}

// decided records the end of review r: its state, decision, who decided, why, and the edit's diff.
func (w *wtx) decided(r *reviewRow, state store.ReviewState, decision, reviewer, reason, text string,
	patch []review.Op,
) error {
	if patch == nil {
		patch = []review.Op{}
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("encode the edit: %w", err)
	}
	_, err = w.exec(`UPDATE reviews SET state = ?, decision = ?, reviewer = ?, reason = ?, reason_text = ?, patch = ?,
		decided_at = ? WHERE pipeline = ? AND delivery_id = ?`,
		state, decision, reviewer, reason, text, string(body), w.ts(), r.pipeline, r.deliveryID)
	if err != nil {
		return err
	}
	data := map[string]any{
		"deliveryId": r.deliveryID, "itemId": r.itemID, "step": r.step, "inbox": r.inbox, "decision": decision,
		"reviewer":         reviewerKind(reviewer),
		"timeToDecisionMs": millis(w.now.Sub(r.parkedAt)),
	}
	if reason != "" {
		data["reason"] = reason
	}
	if len(patch) > 0 {
		data["edited"] = review.Pointers(patch)
	}
	if text != "" { // whether a reason text came with it, never the text (spec section 11.6)
		data["text"] = true
	}
	return w.event(r.pipeline, r.runID, store.EventReviewDecided, data)
}

// reviewerKind is what an event records of a reviewer: rule, timeout, or person. Events feed metrics and insights,
// which never see reviewer identities (spec section 11.6); the review row keeps who decided.
func reviewerKind(reviewer string) string {
	if reviewer == review.ReviewerRule || reviewer == review.ReviewerTimeout {
		return reviewer
	}
	return review.ReviewerPerson
}

// DecideReview is the Review decision unit: see store.Store.
func (s *Store) DecideReview(ctx context.Context, req store.ReviewDecisionRequest) (store.DeliveryState, error) {
	var ops []review.Op
	switch {
	case req.Decision != store.DecisionApprove && req.Decision != store.DecisionReject:
		return "", fmt.Errorf("decide review: decision %q: want approve or reject: %w", req.Decision, store.ErrInvalid)
	case req.Reviewer == "" || req.Reviewer == review.ReviewerRule || req.Reviewer == review.ReviewerTimeout:
		return "", fmt.Errorf("decide review: reviewer %q: name the person deciding: %w", req.Reviewer, store.ErrInvalid)
	case len(req.Patch) > 0 && req.Decision == store.DecisionReject:
		return "", fmt.Errorf("decide review: a rejection carries no edit: %w", store.ErrInvalid)
	case len(req.Patch) > 0:
		var err error
		if ops, err = review.ParsePatch(req.Patch); err != nil {
			return "", fmt.Errorf("decide review: %w: %w", err, store.ErrInvalid)
		}
	}
	var state store.DeliveryState
	err := s.submit(ctx, unitReviewDecision, func(w *wtx) error {
		r, err := w.review(req.Pipeline, req.DeliveryID)
		if err != nil {
			return err
		}
		if again, current, err := w.repeated(r, req, ops); err != nil || again {
			state = current
			return err
		}
		if err := r.open(req.Version); err != nil {
			return err
		}
		if err := checkReason(r.spec, req.Decision, req.Reason); err != nil {
			return err
		}
		d, run, err := w.parked(r)
		if err != nil {
			return err
		}
		var patch []review.Op
		if req.Decision == store.DecisionApprove {
			out, o, err := w.passItem(d)
			if err != nil {
				return err
			}
			if len(ops) > 0 {
				if out, o, patch, err = edit(r.spec, out, o, ops); err != nil {
					return err
				}
			}
			state, err = w.emit(d, run, []store.Output{out}, []payload{o}, attemptEnd{})
			if err != nil {
				return err
			}
		} else {
			reason := req.Reason
			if reason == "" {
				reason = store.FilterReasonRejected
			}
			if state, err = w.filter(d, reason); err != nil {
				return err
			}
		}
		return w.decided(r, store.ReviewDecided, req.Decision, req.Reviewer, req.Reason, req.Text, patch)
	})
	return state, err
}

// repeated reports whether req is the decision review r already holds, sent again by the same reviewer after its
// answer was lost (same version, decision, reason and edit): it then succeeds again, changing nothing, and state is
// the delivery's.
func (w *wtx) repeated(r *reviewRow, req store.ReviewDecisionRequest, ops []review.Op) (bool, store.DeliveryState,
	error,
) {
	if r.state != store.ReviewDecided || r.version != req.Version || r.decision != req.Decision ||
		r.reviewer != req.Reviewer {
		return false, "", nil
	}
	var reason, stored, item string
	var state store.DeliveryState
	err := w.queryRow(`SELECT rv.reason, rv.patch, d.state, i.payload FROM reviews rv
		JOIN deliveries d ON d.pipeline = rv.pipeline AND d.id = rv.delivery_id
		JOIN items i ON i.pipeline = rv.pipeline AND i.id = rv.item_id
		WHERE rv.pipeline = ? AND rv.delivery_id = ?`, r.pipeline, r.deliveryID).Scan(&reason, &stored, &state, &item)
	if err != nil {
		return false, "", notFound(err, "review of delivery "+r.deliveryID)
	}
	if reason != req.Reason {
		return false, "", nil
	}
	diff := []review.Op{}
	if len(ops) > 0 {
		o, err := canonical([]byte(item))
		if err != nil {
			return false, "", err
		}
		edited, err := review.Apply(o.value, ops)
		if err != nil {
			return false, "", nil // not the edit that was applied
		}
		if d := review.Diff(o.value, edited); d != nil {
			diff = d
		}
	}
	body, err := json.Marshal(diff)
	if err != nil {
		return false, "", err
	}
	return string(body) == stored, state, nil
}

// open checks that a reviewer seeing version may still decide the review.
func (r *reviewRow) open(version int64) error {
	switch r.state {
	case store.ReviewOpen:
	case store.ReviewTimedOut:
		return fmt.Errorf("review of delivery %s timed out at %s, applied %s: %w", r.deliveryID,
			r.decidedAt.Format(time.RFC3339), r.decision, store.ErrConflict)
	case store.ReviewDecided:
		return fmt.Errorf("review of delivery %s was already decided: %s by %s at %s: %w", r.deliveryID, r.decision,
			r.reviewer, r.decidedAt.Format(time.RFC3339), store.ErrConflict)
	default:
		return fmt.Errorf("review of delivery %s is %s: %w", r.deliveryID, r.state, store.ErrConflict)
	}
	if version != r.version {
		return fmt.Errorf("review of delivery %s is at version %d, not %d: it moved to inbox %s since: %w",
			r.deliveryID, r.version, version, r.inbox, store.ErrConflict)
	}
	return nil
}

// checkReason checks a decision's reason code against the step's: a rejection needs one when the step lists codes,
// and a code given must be one of them.
func checkReason(spec store.ReviewSpec, decision, reason string) error {
	if len(spec.Reasons) == 0 {
		return nil
	}
	switch {
	case reason == "" && decision == store.DecisionReject:
		return fmt.Errorf("a rejection needs a reason code: one of %s: %w", strings.Join(spec.Reasons, ", "),
			store.ErrInvalid)
	case reason != "" && !slices.Contains(spec.Reasons, reason):
		return fmt.Errorf("reason code %q is not one of %s: %w", reason, strings.Join(spec.Reasons, ", "),
			store.ErrInvalid)
	}
	return nil
}

// parked returns the delivery and run of an open review. Park cleared its leasedAt: deciding it adds no work time.
func (w *wtx) parked(r *reviewRow) (*deliveryRow, *runRow, error) {
	d, err := w.delivery(r.pipeline, r.deliveryID)
	if err != nil {
		return nil, nil, err
	}
	if d.state != store.DeliveryAwaitingReview {
		return nil, nil, fmt.Errorf("delivery %s is %s, not awaiting review: %w", d.id, d.state, store.ErrConflict)
	}
	run, err := w.run(d.pipeline, d.runID)
	return d, run, err
}

// edit applies a reviewer's patch to the item passed on: within the editable pointers, and accepted by one of the
// output schemas reaching the step. It returns the edited output and the edit's diff.
func edit(spec store.ReviewSpec, out store.Output, o payload, ops []review.Op) (store.Output, payload,
	[]review.Op, error,
) {
	if err := review.Editable(ops, spec.Editable); err != nil {
		return out, o, nil, fmt.Errorf("%w: %w", err, store.ErrInvalid)
	}
	edited, err := review.Apply(o.value, ops)
	if err != nil {
		return out, o, nil, fmt.Errorf("%w: %w", err, store.ErrInvalid)
	}
	if err := matchesOne(spec.Schemas, edited); err != nil {
		return out, o, nil, err
	}
	body, err := json.Marshal(edited)
	if err != nil {
		return out, o, nil, fmt.Errorf("encode the edited payload: %w", err)
	}
	e, err := canonical(body)
	if err != nil {
		return out, o, nil, err
	}
	out.Payload = e.raw
	return out, e, review.Diff(o.value, edited), nil
}

// matchesOne checks v against the schemas: any one accepting it will do, and none means anything goes.
func matchesOne(schemas []json.RawMessage, v any) error {
	var errs []error
	for _, doc := range schemas {
		sc, err := schema.Compile(doc)
		if err != nil {
			return fmt.Errorf("compile an output schema of the step: %w", err)
		}
		if err := sc.Validate(v); err != nil {
			errs = append(errs, err)
			continue
		}
		return nil
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("the edited payload does not match the output schema: %w: %w", errors.Join(errs...),
		store.ErrInvalid)
}

// DecideSample is the Sample decision unit: see store.Store. It never emits and never touches the run.
func (s *Store) DecideSample(ctx context.Context, req store.SampleDecisionRequest) error {
	switch {
	case req.Decision != store.SampleAgree && req.Decision != store.SampleDisagree:
		return fmt.Errorf("decide sample: decision %q: want agree or disagree: %w", req.Decision, store.ErrInvalid)
	case req.Reviewer == "":
		return fmt.Errorf("decide sample: name the person deciding: %w", store.ErrInvalid)
	}
	return s.submit(ctx, unitSampleDecision, func(w *wtx) error {
		var runID, step, inbox, decision, reviewer string
		var decided TimeCol
		var state store.SampleState
		var itemID, reason string
		if err := w.queryRow(`SELECT run_id, step, item_id, inbox, state, decision, reviewer, reason, decided_at
			FROM review_samples WHERE pipeline = ? AND delivery_id = ?`, req.Pipeline, req.DeliveryID).
			Scan(&runID, &step, &itemID, &inbox, &state, &decision, &reviewer, &reason, &decided); err != nil {
			return notFound(err, "review sample of delivery "+req.DeliveryID)
		}
		switch {
		case state == store.SampleOpen:
		case state == store.SampleDecided && decision == req.Decision && reviewer == req.Reviewer &&
			reason == req.Reason:
			return nil // the same decision sent again, its answer lost: it stands
		case state == store.SampleDecided:
			return fmt.Errorf("review sample of delivery %s was already decided: %s by %s at %s: %w", req.DeliveryID,
				decision, reviewer, decided.stored(), store.ErrConflict)
		default:
			return fmt.Errorf("review sample of delivery %s expired unreviewed at %s: %w", req.DeliveryID,
				decided.stored(), store.ErrConflict)
		}
		if _, err := w.exec(`UPDATE review_samples SET state = ?, decision = ?, reviewer = ?, reason = ?, decided_at = ?
			WHERE pipeline = ? AND delivery_id = ?`, store.SampleDecided, req.Decision, req.Reviewer, req.Reason, w.ts(),
			req.Pipeline, req.DeliveryID); err != nil {
			return err
		}
		return w.event(req.Pipeline, runID, store.EventSampleDecided, map[string]any{"deliveryId": req.DeliveryID,
			"itemId": itemID, "step": step, "inbox": inbox, "decision": req.Decision})
	})
}

// ReviewTimeouts is the Review timeout unit: see store.Store. Each review it ends leaves the open state in the same
// transaction, so onTimeout applies once however often the engine's ticker runs it.
func (s *Store) ReviewTimeouts(ctx context.Context) ([]store.TimedOutReview, error) {
	if due, err := s.deadlinesDue(ctx); err != nil || !due {
		return nil, err
	}
	var out []store.TimedOutReview
	err := s.submit(ctx, unitReviewTimeout, func(w *wtx) error {
		out = nil // filled only when the unit applies
		due, err := w.overdue(`SELECT pipeline, delivery_id FROM reviews WHERE state = ? AND deadline <= ?
			ORDER BY deadline, pipeline, delivery_id`, store.ReviewOpen)
		if err != nil {
			return err
		}
		for _, k := range due {
			// A review that cannot time out (its item no longer passes, a broken spec) is skipped, its partial work
			// rolled back, rather than holding back every other review due.
			var t store.TimedOutReview
			err, abort := savepoint(w, "review", func(w *wtx) error {
				var err error
				t, err = w.timeOut(k.Pipeline, k.ID)
				return err
			})
			if abort != nil {
				return abort
			}
			if err != nil {
				t = store.TimedOutReview{Pipeline: k.Pipeline, DeliveryID: k.ID, Err: err}
			}
			out = append(out, t)
		}
		samples, err := w.overdue(`SELECT pipeline, delivery_id FROM review_samples WHERE state = ? AND deadline <= ?
			ORDER BY deadline, pipeline, delivery_id`, store.SampleOpen)
		if err != nil {
			return err
		}
		for _, k := range samples {
			var runID, step, itemID, inbox string
			if err := w.queryRow(`UPDATE review_samples SET state = ?, decided_at = ? WHERE pipeline = ? AND delivery_id = ?
				RETURNING run_id, step, item_id, inbox`, store.SampleUnreviewed, w.ts(), k.Pipeline, k.ID).
				Scan(&runID, &step, &itemID, &inbox); err != nil {
				return fmt.Errorf("expire review sample %s: %w", k.ID, err)
			}
			if err := w.event(k.Pipeline, runID, store.EventSampleExpired,
				map[string]any{"deliveryId": k.ID, "itemId": itemID, "step": step, "inbox": inbox}); err != nil {
				return err
			}
			out = append(out, store.TimedOutReview{Pipeline: k.Pipeline, DeliveryID: k.ID, RunID: runID, Step: step,
				Inbox: inbox, Action: string(store.SampleUnreviewed)})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// deadlinesDue reports, from the read pool, whether an open review or sample may be past its deadline, so the engine's
// every tick costs a write transaction only when there is something to time out. It compares with the store clock
// read now, the database's unless Options.Now replaces it; the unit compares again with its batch instant.
func (s *Store) deadlinesDue(ctx context.Context) (bool, error) {
	now, args := s.d.Now, []any{store.ReviewOpen, store.SampleOpen}
	if s.opts.Now != nil {
		now, args = "?", []any{store.ReviewOpen, formatTime(s.opts.Now()), store.SampleOpen, formatTime(s.opts.Now())}
	}
	var due bool
	err := s.read.Row(ctx, `SELECT EXISTS (SELECT 1 FROM reviews WHERE state = ? AND deadline <= `+now+`)
		OR EXISTS (SELECT 1 FROM review_samples WHERE state = ? AND deadline <= `+now+`)`, args...).Scan(&due)
	if err != nil {
		return false, fmt.Errorf("review timeouts: %w", err)
	}
	return due, nil
}

// overdue returns the keys a deadline query selects in state at the batch instant.
func (w *wtx) overdue(query string, state any) ([]store.DeliveryKey, error) {
	rows, err := w.query(query, state, w.ts())
	if err != nil {
		return nil, err
	}
	var keys []store.DeliveryKey
	for rows.Next() {
		var k store.DeliveryKey
		if err := rows.Scan(&k.Pipeline, &k.ID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan overdue review: %w", err)
		}
		keys = append(keys, k)
	}
	return keys, closeRows(rows)
}

// timeOut applies an overdue review's onTimeout: escalate moves it to the other inbox with a fresh deadline (an
// escalated review timing out again is rejected); approve passes the item on; reject filters it with reason timeout.
func (w *wtx) timeOut(pipelineName, deliveryID string) (store.TimedOutReview, error) {
	r, err := w.review(pipelineName, deliveryID)
	if err != nil {
		return store.TimedOutReview{}, err
	}
	d, run, err := w.parked(r)
	if err != nil {
		return store.TimedOutReview{}, err
	}
	out, o, err := w.passItem(d)
	if err != nil {
		return store.TimedOutReview{}, err
	}
	t := store.TimedOutReview{Pipeline: r.pipeline, DeliveryID: r.deliveryID, RunID: r.runID, Step: r.step,
		Revision: run.revision, Inbox: r.inbox, ItemKey: out.Key, Display: r.display, Notify: r.spec.Notify,
		NotifySecret: r.spec.NotifySecret}
	action := r.spec.OnTimeout
	if action == pipeline.OnTimeoutEscalate && r.inbox == r.spec.EscalateTo {
		action = pipeline.OnTimeoutReject // escalated once already
	}
	switch action {
	case pipeline.OnTimeoutEscalate:
		deadline := w.deadline(r.spec.Timeout)
		if _, err := w.exec(`UPDATE reviews SET inbox = ?, version = version + 1, deadline = ?
			WHERE pipeline = ? AND delivery_id = ?`, r.spec.EscalateTo, nullTime(deadline), r.pipeline,
			r.deliveryID); err != nil {
			return t, err
		}
		t.Action, t.Inbox, t.Deadline = pipeline.OnTimeoutEscalate, r.spec.EscalateTo, deadline
		data := map[string]any{
			"deliveryId": r.deliveryID, "itemId": r.itemID, "step": r.step, "from": r.inbox, "inbox": r.spec.EscalateTo,
		}
		if !deadline.IsZero() {
			data["deadline"] = formatTime(deadline)
		}
		return t, w.event(r.pipeline, r.runID, store.EventReviewEscalated, data)
	case pipeline.OnTimeoutApprove:
		if _, err := w.emit(d, run, []store.Output{out}, []payload{o}, attemptEnd{}); err != nil {
			return t, err
		}
	default:
		action = pipeline.OnTimeoutReject
		if _, err := w.filter(d, store.FilterReasonTimeout); err != nil {
			return t, err
		}
	}
	t.Action = action
	reason := "" // an approval needs no reason; a rejection is filtered with reason timeout
	if action == pipeline.OnTimeoutReject {
		reason = store.FilterReasonTimeout
	}
	return t, w.decided(r, store.ReviewTimedOut, action, review.ReviewerTimeout, reason, "", nil)
}

// cancelReviews marks the open reviews of a cancelled run cancelled: their deliveries were cancelled with it.
func (w *wtx) cancelReviews(pipelineName, runID string) error {
	_, err := w.exec(`UPDATE reviews SET state = ?, decided_at = ? WHERE pipeline = ? AND run_id = ? AND state = ?`,
		store.ReviewCancelled, w.ts(), pipelineName, runID, store.ReviewOpen)
	return err
}

// reviewColumns are the columns scanReview reads, from reviews r joined with items i.
const reviewColumns = `r.rowid, r.pipeline, r.delivery_id, r.run_id, r.step, r.state, r.version, r.spec_json, r.display,
	r.parked_at, r.deadline, r.decision, r.reviewer, r.reason, r.reason_text, r.patch, r.decided_at, r.inbox,
	i.id, i.run_id, i.stream, i.step, i.key, i.parents, i.generation, i.ts, i.payload, i.artifacts, i.tombstone`

func scanReview(rows interface{ Scan(...any) error }) (store.Review, error) {
	var (
		r                             store.Review
		spec, patch, inbox            string
		parents, body, artifacts      string
		parked, deadline, decided, ts TimeCol
	)
	if err := rows.Scan(&r.Seq, &r.Pipeline, &r.DeliveryID, &r.RunID, &r.Step, &r.State, &r.Version, &spec, &r.Display,
		&parked, &deadline, &r.Decision, &r.Reviewer, &r.Reason, &r.Text, &patch, &decided, &inbox,
		&r.Item.ID, &r.Item.RunID, &r.Item.Stream, &r.Item.Step, &r.Item.Key, &parents, &r.Item.Generation, &ts, &body,
		&artifacts, &r.Item.Tombstone); err != nil {
		return r, fmt.Errorf("scan review: %w", err)
	}
	if err := json.Unmarshal([]byte(spec), &r.Spec); err != nil {
		return r, fmt.Errorf("review of delivery %s: %w", r.DeliveryID, err)
	}
	r.Spec.Inbox = inbox // where it waits now, escalated or not
	r.Patch = json.RawMessage(patch)
	r.Item.Pipeline, r.Item.Payload = r.Pipeline, json.RawMessage(body)
	r.ParkedAt, r.Deadline, r.DecidedAt = parked.Time, deadline.Time, decided.Time
	return r, fillItem(&r.Item, parents, ts, artifacts)
}

// Reviews returns the reviews f selects in Seq order, with their items.
func (s *Store) Reviews(ctx context.Context, f store.ReviewFilter) ([]store.Review, error) {
	if f.State == "" {
		f.State = store.ReviewOpen
	}
	q, args := pageQuery(`SELECT `+reviewColumns+` FROM reviews r JOIN items i ON i.pipeline = r.pipeline AND
		i.id = r.item_id`, f.Pipeline, f.Inbox, f.Step, string(f.State), f.After, f.Limit)
	out, err := scan(ctx, s.read, q, args, func(rows *sql.Rows) (store.Review, error) { return scanReview(rows) })
	if err != nil {
		return nil, fmt.Errorf("reviews: %w", err)
	}
	return out, nil
}

// Review returns one review with its item: see store.Store.
func (s *Store) Review(ctx context.Context, pipelineName, deliveryID string) (store.Review, error) {
	var w where
	w.eq("r.pipeline", pipelineName)
	w.eq("r.delivery_id", deliveryID)
	out, err := scan(ctx, s.read, `SELECT `+reviewColumns+` FROM reviews r JOIN items i ON i.pipeline = r.pipeline AND
		i.id = r.item_id`+w.String()+` LIMIT 2`, w.args, func(rows *sql.Rows) (store.Review, error) {
		return scanReview(rows)
	})
	if err != nil {
		return store.Review{}, fmt.Errorf("review of delivery %s: %w", deliveryID, err)
	}
	return one(out, "review", deliveryID)
}

// one returns the single record found for a delivery looked up in every pipeline.
func one[T any](found []T, what, deliveryID string) (T, error) {
	var zero T
	switch len(found) {
	case 0:
		return zero, fmt.Errorf("%s of delivery %s: %w", what, deliveryID, store.ErrNotFound)
	case 1:
		return found[0], nil
	}
	return zero, fmt.Errorf("%s of delivery %s: several pipelines have one, name the pipeline: %w", what, deliveryID,
		store.ErrConflict)
}

// pageQuery adds to a reviews or samples query (aliased r) the filters, the cursor after Seq and the limit, in Seq
// order.
func pageQuery(base, pipelineName, inbox, step, state string, after int64, limit int) (string, []any) {
	var w where
	w.eq("r.pipeline", pipelineName)
	w.eq("r.inbox", inbox)
	w.eq("r.step", step)
	w.eq("r.state", state)
	if after > 0 {
		w.conds = append(w.conds, "r.rowid > ?")
		w.args = append(w.args, after)
	}
	q := base + w.String() + ` ORDER BY r.rowid`
	if limit > 0 {
		q += limitParam
		w.args = append(w.args, limit)
	}
	return q, w.args
}

// sampleColumns are the columns scanSample reads, from review_samples r joined with items i.
const sampleColumns = `r.rowid, r.pipeline, r.delivery_id, r.run_id, r.step, r.inbox, r.state, r.display, r.created_at,
	r.deadline, r.decision, r.reviewer, r.reason, r.decided_at,
	i.id, i.run_id, i.stream, i.step, i.key, i.parents, i.generation, i.ts, i.payload, i.artifacts, i.tombstone`

func scanSample(rows interface{ Scan(...any) error }) (store.ReviewSample, error) {
	var (
		r                              store.ReviewSample
		parents, body, artifacts       string
		created, deadline, decided, ts TimeCol
	)
	if err := rows.Scan(&r.Seq, &r.Pipeline, &r.DeliveryID, &r.RunID, &r.Step, &r.Inbox, &r.State, &r.Display, &created,
		&deadline, &r.Decision, &r.Reviewer, &r.Reason, &decided,
		&r.Item.ID, &r.Item.RunID, &r.Item.Stream, &r.Item.Step, &r.Item.Key, &parents, &r.Item.Generation, &ts, &body,
		&artifacts, &r.Item.Tombstone); err != nil {
		return r, fmt.Errorf("scan review sample: %w", err)
	}
	r.Item.Pipeline, r.Item.Payload = r.Pipeline, json.RawMessage(body)
	r.CreatedAt, r.Deadline, r.DecidedAt = created.Time, deadline.Time, decided.Time
	return r, fillItem(&r.Item, parents, ts, artifacts)
}

// ReviewSamples returns the review samples f selects in Seq order, with their items.
func (s *Store) ReviewSamples(ctx context.Context, f store.SampleFilter) ([]store.ReviewSample, error) {
	if f.State == "" {
		f.State = store.SampleOpen
	}
	q, args := pageQuery(`SELECT `+sampleColumns+` FROM review_samples r JOIN items i ON i.pipeline = r.pipeline AND
		i.id = r.item_id`, f.Pipeline, f.Inbox, f.Step, string(f.State), f.After, f.Limit)
	out, err := scan(ctx, s.read, q, args, func(rows *sql.Rows) (store.ReviewSample, error) { return scanSample(rows) })
	if err != nil {
		return nil, fmt.Errorf("review samples: %w", err)
	}
	return out, nil
}

// ReviewSample returns one review sample with its item: see store.Store.
func (s *Store) ReviewSample(ctx context.Context, pipelineName, deliveryID string) (store.ReviewSample, error) {
	var w where
	w.eq("r.pipeline", pipelineName)
	w.eq("r.delivery_id", deliveryID)
	out, err := scan(ctx, s.read, `SELECT `+sampleColumns+` FROM review_samples r JOIN items i ON
		i.pipeline = r.pipeline AND i.id = r.item_id`+w.String()+` LIMIT 2`, w.args,
		func(rows *sql.Rows) (store.ReviewSample, error) { return scanSample(rows) })
	if err != nil {
		return store.ReviewSample{}, fmt.Errorf("review sample of delivery %s: %w", deliveryID, err)
	}
	return one(out, "review sample", deliveryID)
}
