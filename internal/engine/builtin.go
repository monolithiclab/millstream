package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// ClassExpr is the error class of a delivery whose builtin expression (filter's expr, dedupe's key) fails to compile
// or evaluate for its item. Deterministic, so never retried.
const ClassExpr = "expr"

// builtinKey identifies a builtin step of a revision.
type builtinKey struct{ hash, step string }

// builtin is a builtin step's config, compiled once per revision, or the dispatch error it compiles to.
type builtin struct {
	name   string
	filter *expr.ItemExpr
	key    *expr.KeyExpr
	window time.Duration // zero: a key holds back its duplicates for ever
	review *pipeline.Review
	// spec is what a parked review keeps of the review step's config, its output schemas included.
	spec store.ReviewSpec
	err  error
}

// runBuiltin executes a leased delivery of a builtin step inside the engine (spec section 4.7) and reports whether
// the step is a builtin; a node never receives one. filter passes the item on unchanged, through the Done unit so
// routing applies, or filters the delivery; dedupe hands the item's key to the Dedupe unit; review parks it through
// the Park unit, or passes it at once when autoApprove holds (spec section 8). An expression that fails for the item
// ends the delivery dead (class expr). It returns false, leaving the lease to assign, when the
// revision cannot be read or the step is not a builtin.
func (e *Engine) runBuiltin(ctx context.Context, l store.Lease) bool {
	rev, err := e.revision(ctx, l.Revision)
	if err != nil {
		return false
	}
	step, ok := rev.p.Steps[l.Step]
	if !ok || !pipeline.IsBuiltin(step.Action) {
		return false
	}
	b, err := e.builtin(rev, step)
	if err != nil {
		e.failDispatch(ctx, l, err)
		return true
	}
	if b.err != nil {
		e.failDispatch(ctx, l, b.err)
		return true
	}
	run, err := e.st.Run(ctx, l.Pipeline, l.RunID)
	if err != nil {
		e.failDispatch(ctx, l, dispatchErr(ClassEngine, true, "read run %s: %w", l.RunID, err))
		return true
	}
	item, err := pipeline.DecodeValue(string(l.Item.Payload))
	if err != nil {
		e.failDispatch(ctx, l, dispatchErr(ClassExpr, false, "item %s: %w", l.Item.ID, err))
		return true
	}
	env := expr.Envelope{ID: l.Item.ID, Key: l.Item.Key, TS: l.Item.TS, Attempt: l.Attempt, Step: l.Step}
	runInfo := expr.Run{ID: run.ID, Trigger: run.Trigger, StartedAt: run.StartedAt, Revision: rev.hash}
	ref := l.Ref()
	switch b.name {
	case pipeline.BuiltinFilter:
		pass, evalErr := b.filter.Eval(item, env, run.Vars, runInfo)
		switch {
		case evalErr != nil:
			e.failDispatch(ctx, l, dispatchErr(ClassExpr, false, "filter of step %s: %w", l.Step, evalErr))
		case pass:
			err = e.done(ctx, store.DoneRequest{LeaseRef: ref, Attempt: l.Attempt,
				Outputs: []store.Output{{Key: l.Item.Key, Payload: l.Item.Payload}}})
		default:
			err = e.complete(ref, func() (store.DeliveryState, error) {
				return e.st.Filter(ctx, store.FilterRequest{LeaseRef: ref, Attempt: l.Attempt,
					Reason: store.FilterReasonExpr})
			})
		}
	case pipeline.BuiltinDedupe:
		key, evalErr := b.key.Eval(item, env, run.Vars, runInfo)
		if evalErr != nil {
			e.failDispatch(ctx, l, dispatchErr(ClassExpr, false, "dedupe key of step %s: %w", l.Step, evalErr))
			break
		}
		err = e.complete(ref, func() (store.DeliveryState, error) {
			return e.st.Dedupe(ctx, store.DedupeRequest{LeaseRef: ref, Attempt: l.Attempt, Key: key, Window: b.window})
		})
	case pipeline.BuiltinReview:
		err = e.park(ctx, l, b, item, env, run.Vars, runInfo)
	}
	if err != nil {
		e.leaseLog(l).Error("complete builtin delivery", "error", err.Error())
	}
	return true
}

// park applies the Park unit to a review delivery: auto-approved (and maybe sampled) when autoApprove holds,
// parked in its inbox otherwise, with the step's webhook told after the commit.
func (e *Engine) park(ctx context.Context, l store.Lease, b *builtin, item any, env expr.Envelope,
	vars map[string]any, runInfo expr.Run,
) error {
	auto := false
	if b.review.AutoApprove != nil {
		var err error
		if auto, err = b.review.AutoApprove.Eval(item, env, vars, runInfo); err != nil {
			e.failDispatch(ctx, l, dispatchErr(ClassExpr, false, "autoApprove of step %s: %w", l.Step, err))
			return nil
		}
	}
	ref := l.Ref()
	req := store.ParkRequest{LeaseRef: ref, Attempt: l.Attempt, Spec: b.spec, AutoApprove: auto,
		Sample:  auto && review.Sampled(l.Item.ID, b.review.SampleRate),
		Display: b.review.Render(item, l.Item.Key, l.Item.ID, l.Step)}
	var res store.ParkResult
	err := e.complete(ref, func() (store.DeliveryState, error) {
		var err error
		res, err = e.st.Park(ctx, req)
		return res.State, err
	})
	if err == nil && res.State == store.DeliveryAwaitingReview {
		e.notify(b.spec.Notify, b.spec.NotifySecret, review.Notification{Pipeline: l.Pipeline, Step: l.Step,
			Inbox: b.spec.Inbox, DeliveryID: l.DeliveryID, ItemKey: l.Item.Key, Display: req.Display,
			Deadline: res.Deadline})
	}
	return err
}

// notify posts a notification to a review step's webhook in the background, when it has one: the URL itself, or
// the value of the secret holding it, read now (spec section 4.5).
func (e *Engine) notify(webhook, secret string, n review.Notification) {
	if (webhook == "" && secret == "") || e.opts.Notifier == nil {
		return
	}
	if secret != "" {
		webhook, _ = e.secrets(n.Pipeline)(secret) // an unresolved secret posts nowhere; the notifier says so
	}
	n.URL, n.Secret = webhook, secret
	e.opts.Notifier.Notify(n)
}

// complete applies a builtin's store unit to a delivery in flight, counting it out of flight like Done.
func (e *Engine) complete(ref store.LeaseRef, unit func() (store.DeliveryState, error)) error {
	state, err := unit()
	f := e.settleFlight(ref, err)
	if err != nil {
		return fmt.Errorf("builtin %s: %w", ref.DeliveryID, err)
	}
	e.flightLog(ref, f).Info("delivery done", "state", string(state))
	return nil
}

// builtin compiles a builtin step's config, once per revision and step. The config is not interpolated: its
// strings are CEL expressions, durations and templates, which check validates. A review step also needs the output
// schemas reaching it, from its revision's package: until that package is loaded, the delivery fails as retryable
// and nothing is cached.
func (e *Engine) builtin(rev *revision, step pipeline.Step) (*builtin, error) {
	k := builtinKey{rev.hash, step.Name}
	e.mu.Lock()
	b := e.builtins[k]
	e.mu.Unlock()
	if b != nil {
		return b, nil
	}
	b = compileBuiltin(step)
	if b.review != nil && b.err == nil {
		schemas, err := e.reviewSchemas(rev, step)
		if err != nil {
			return nil, err
		}
		b.spec.Schemas = schemas
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.builtins[k] = b
	return b, nil
}

// reviewSchemas returns the output schemas of what reaches a review step (pipeline.Origins), one of which an edited
// payload must match.
func (e *Engine) reviewSchemas(rev *revision, step pipeline.Step) ([]json.RawMessage, error) {
	e.mu.Lock()
	pkg := e.packages[rev.hash]
	e.mu.Unlock()
	if pkg == nil {
		return nil, dispatchErr(ClassEngine, true,
			"the package of revision %d (%s) is not loaded in this engine: run it with that package", rev.id, rev.hash)
	}
	action := func(name string) *pipeline.Action {
		s, ok := rev.p.Steps[name]
		if !ok {
			return nil
		}
		if pipeline.IsBuiltin(s.Action) {
			return &pipeline.Action{Ref: s.Action, Builtin: true, Name: s.Action}
		}
		ra := e.action(rev, s.Action)
		if ra.err != nil {
			return nil
		}
		return ra.a
	}
	schemas := []json.RawMessage{}
	for _, in := range step.In {
		for _, o := range pipeline.Origins(pkg, action, in, nil) {
			schemas = append(schemas, o.Schema)
		}
	}
	return schemas, nil
}

func compileBuiltin(step pipeline.Step) *builtin {
	b := &builtin{name: step.Action}
	str := func(key string) string { s, _ := step.Config[key].(string); return s }
	var err error
	switch step.Action {
	case pipeline.BuiltinFilter:
		if b.filter, err = expr.CompileItem(str("expr")); err != nil {
			b.err = dispatchErr(ClassExpr, false, "filter of step %s: %w", step.Name, err)
		}
	case pipeline.BuiltinDedupe:
		if b.key, err = expr.CompileKey(str("key")); err != nil {
			b.err = dispatchErr(ClassExpr, false, "dedupe key of step %s: %w", step.Name, err)
			break
		}
		if w := str("window"); w != "" {
			if b.window, err = pipeline.ParseDuration(w); err != nil {
				b.err = dispatchErr(ClassConfig, false, "dedupe window of step %s: %w", step.Name, err)
			}
		}
	case pipeline.BuiltinReview:
		r, errs := pipeline.ParseReview(step.Name, step.Config)
		if len(errs) > 0 {
			b.err = dispatchErr(ClassConfig, false, "review config of step %s: %w", step.Name, errs[0])
			break
		}
		b.review = r
		b.spec = store.ReviewSpec{Inbox: r.Inbox, Timeout: r.Timeout, OnTimeout: r.OnTimeout, EscalateTo: r.EscalateTo,
			Editable: r.Editable, Reasons: r.Reasons, Notify: r.Notify, NotifySecret: r.NotifySecret}
	}
	return b
}

// reviewTimeouts applies the Review timeout unit, and tells the webhook of each review escalated to another inbox.
func (e *Engine) reviewTimeouts(ctx context.Context) {
	ts, err := e.st.ReviewTimeouts(ctx)
	if err != nil {
		if ctx.Err() == nil {
			e.log.Error("review timeouts", "error", err.Error())
		}
		return
	}
	for _, t := range ts {
		if t.Err != nil {
			e.log.Error("review timeout failed: the review stays open", "pipeline", t.Pipeline,
				"deliveryId", t.DeliveryID, "error", t.Err.Error())
			continue
		}
		e.log.Info("review timed out", "pipeline", t.Pipeline, "runId", t.RunID, "step", t.Step,
			"deliveryId", t.DeliveryID, "applied", t.Action, "inbox", t.Inbox)
		if t.Action == pipeline.OnTimeoutEscalate {
			e.notify(t.Notify, t.NotifySecret, review.Notification{Pipeline: t.Pipeline, Step: t.Step, Inbox: t.Inbox,
				DeliveryID: t.DeliveryID, ItemKey: t.ItemKey, Display: t.Display, Escalated: true, Deadline: t.Deadline})
		}
	}
}
