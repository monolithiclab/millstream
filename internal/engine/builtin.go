package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/pipeline"
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
	err    error
}

// runBuiltin executes a leased delivery of a builtin step inside the engine (spec section 4.7) and reports whether
// the step is a builtin; a node never receives one. filter passes the item on unchanged, through the Done unit so
// routing applies, or filters the delivery; dedupe hands the item's key to the Dedupe unit. An expression that
// fails for the item ends the delivery dead (class expr). It returns false, leaving the lease to assign, when the
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
	b := e.builtin(rev, step)
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
			err = e.Done(ctx, store.DoneRequest{LeaseRef: ref, Attempt: l.Attempt,
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
	}
	if err != nil {
		e.leaseLog(l).Error("complete builtin delivery", "error", err.Error())
	}
	return true
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
// strings are CEL expressions and a duration, which check validates.
func (e *Engine) builtin(rev *revision, step pipeline.Step) *builtin {
	k := builtinKey{rev.hash, step.Name}
	e.mu.Lock()
	defer e.mu.Unlock()
	if b := e.builtins[k]; b != nil {
		return b
	}
	b := compileBuiltin(step)
	e.builtins[k] = b
	return b
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
	default:
		b.err = dispatchErr(ClassUnsupported, false, "the %s builtin arrives in M3: this build cannot run it",
			step.Action)
	}
	return b
}
