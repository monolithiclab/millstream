package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// ReplayRequest asks for a replay run of a step over past items (spec section 5.7). The selection is every item on
// the step's input streams, in every generation, narrowed by Since, RunID and Where.
type ReplayRequest struct {
	Pipeline string
	Step     string
	// Revision is the revision the replay runs on; zero means the pipeline's active one.
	Revision store.RevisionID
	// Since keeps the items published within this duration of now; zero keeps them all.
	Since time.Duration
	// RunID keeps the items of one run.
	RunID string
	// Where keeps the items for which this CEL expression over item, envelope, vars and run holds.
	Where string
	// RequestID names the replay run (ids.ReplayRun), so retrying the same request starts nothing new; generated
	// when empty.
	RequestID string
}

// Replay starts a replay run: it resolves the run's variables against the revision as a firing does, selects the
// items (reads only: items are immutable, so the selection cannot go stale), and applies the Start replay unit,
// which takes the run's generation. It returns the run id, and created false when the request already started it.
func (e *Engine) Replay(ctx context.Context, req ReplayRequest) (string, bool, error) {
	fail := func(err error) (string, bool, error) {
		return "", false, fmt.Errorf("replay %s.%s: %w", req.Pipeline, req.Step, err)
	}
	key := req.RequestID
	if key == "" {
		key = NewRequestID(e.opts.Now())
	}
	runID := ids.ReplayRun(key)
	revID := req.Revision
	if revID == 0 {
		var err error
		if revID, err = e.st.ActiveRevision(ctx, req.Pipeline); err != nil {
			return fail(err)
		}
	}
	rev, err := e.revision(ctx, revID)
	if err != nil {
		return fail(err)
	}
	if rev.p.Name != req.Pipeline {
		return fail(fmt.Errorf("revision %d belongs to %s: %w", revID, rev.p.Name, store.ErrInvalid))
	}
	step, ok := rev.p.Steps[req.Step]
	if !ok {
		return fail(fmt.Errorf("revision %d has no step %q: %w", revID, req.Step, store.ErrInvalid))
	}
	res, err := pipeline.ResolveVarsWith(rev.p, e.declaredFlags(rev.p), e.opts.Env, e.secrets(rev.p.Name))
	if err != nil {
		return fail(fmt.Errorf("%w: %w", ErrFiringConfig, err))
	}
	now := e.opts.Now()
	selected, err := e.selectItems(ctx, req, step, res.Vars,
		expr.Run{ID: runID, StartedAt: now, Revision: rev.hash}, now)
	if err != nil {
		return fail(err)
	}
	_, created, err := e.st.StartReplay(ctx, store.ReplayStart{
		Pipeline: req.Pipeline, Step: req.Step, RunID: runID, Key: key, Revision: revID, Vars: res.Vars,
		VarSources: res.Sources, Items: selected,
	})
	if err != nil {
		return fail(err)
	}
	e.log.Info("replay started", "pipeline", req.Pipeline, "runId", runID, "step", req.Step,
		"revision", int64(revID), "items", len(selected), "created", created)
	return runID, created, nil
}

// selectItems returns the ids of the items on step's input streams that the request keeps.
func (e *Engine) selectItems(ctx context.Context, req ReplayRequest, step pipeline.Step, vars map[string]any,
	run expr.Run, now time.Time,
) ([]string, error) {
	var where *expr.ItemExpr
	if req.Where != "" {
		var err error
		if where, err = expr.CompileItem(req.Where); err != nil {
			return nil, fmt.Errorf("--where: %w: %w", err, store.ErrInvalid)
		}
	}
	f := store.ItemFilter{Pipeline: req.Pipeline, RunID: req.RunID}
	if req.Since > 0 {
		f.Since = now.Add(-req.Since)
	}
	var selected []string
	for _, stream := range step.In {
		f.Stream = stream
		items, err := e.st.Items(ctx, f)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			if where != nil {
				v, err := pipeline.DecodeValue(string(it.Payload))
				if err != nil {
					return nil, fmt.Errorf("item %s: %w", it.ID, err)
				}
				ok, err := where.Eval(v, expr.Envelope{ID: it.ID, Key: it.Key, TS: it.TS, Step: req.Step}, vars, run)
				if err != nil {
					return nil, fmt.Errorf("--where on item %s: %w: %w", it.ID, err, store.ErrInvalid)
				}
				if !ok {
					continue
				}
			}
			selected = append(selected, it.ID)
		}
	}
	return selected, nil
}
