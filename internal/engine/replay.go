package engine

import (
	"cmp"
	"context"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// ReplayRequest asks for a replay run of a step over past items (spec section 5.7). The selection is every item on
// the step's input streams, in every generation, narrowed by Since, RunID and Where. With Stream instead of Step,
// it back-fills an imported stream (spec section 4.12): the selection is the producer's items on its stream, those
// whose payload has not expired, narrowed by Since and Where.
type ReplayRequest struct {
	Pipeline string
	Step     string
	Stream   string
	// Revision is the revision the replay runs on; zero means the pipeline's active one.
	Revision store.RevisionID
	// Since keeps the items published within this duration of now; zero keeps them all.
	Since time.Duration
	// RunID keeps the items of one run; not with Stream.
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
		return "", false, fmt.Errorf("replay %s.%s: %w", req.Pipeline, cmp.Or(req.Step, req.Stream), err)
	}
	switch {
	case (req.Step == "") == (req.Stream == ""):
		return fail(fmt.Errorf("a step or an imported stream, not both: %w", store.ErrInvalid))
	case req.Stream != "" && req.RunID != "":
		return fail(fmt.Errorf("a back-fill selects by --since and --where, not by run: %w", store.ErrInvalid))
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
	sel := selection{pipeline: req.Pipeline, streams: rev.p.Steps[req.Step].In, step: req.Step}
	if req.Stream != "" {
		imp := rev.p.Streams[req.Stream].Import
		if imp == nil {
			return fail(fmt.Errorf("revision %d does not import a stream %q: %w", revID, req.Stream, store.ErrInvalid))
		}
		sel = selection{pipeline: imp.Pipeline, streams: []string{imp.Stream}, retained: true}
	} else if _, ok := rev.p.Steps[req.Step]; !ok {
		return fail(fmt.Errorf("revision %d has no step %q: %w", revID, req.Step, store.ErrInvalid))
	}
	res, err := pipeline.ResolveVarsWith(rev.p, e.declaredFlags(rev.p), e.opts.Env, e.secrets(rev.p.Name))
	if err != nil {
		return fail(fmt.Errorf("%w: %w", ErrFiringConfig, err))
	}
	now := e.opts.Now()
	selected, err := e.selectItems(ctx, req, sel, res.Vars,
		expr.Run{ID: runID, StartedAt: now, Revision: rev.hash}, now)
	if err != nil {
		return fail(err)
	}
	_, created, err := e.st.StartReplay(ctx, store.ReplayStart{
		Pipeline: req.Pipeline, Step: req.Step, Stream: req.Stream, RunID: runID, Key: key, Revision: revID, Vars: res.Vars,
		VarSources: res.Sources, Items: selected,
	})
	if err != nil {
		return fail(err)
	}
	e.log.Info("replay started", "pipeline", req.Pipeline, "runId", runID, "step", req.Step, "stream", req.Stream,
		"revision", int64(revID), "items", len(selected), "created", created)
	return runID, created, nil
}

// selection is where a replay's items are: streams of a pipeline, read by a step (whose name the envelope of --where
// carries) or imported (retained: the items whose payload expired are left out).
type selection struct {
	pipeline string
	streams  []string
	step     string
	retained bool
}

// selectItems returns the ids of the items on the selection's streams that the request keeps.
func (e *Engine) selectItems(ctx context.Context, req ReplayRequest, sel selection, vars map[string]any,
	run expr.Run, now time.Time,
) ([]string, error) {
	var where *expr.ItemExpr
	if req.Where != "" {
		var err error
		if where, err = expr.CompileItem(req.Where); err != nil {
			return nil, fmt.Errorf("--where: %w: %w", err, store.ErrInvalid)
		}
	}
	f := store.ItemFilter{Pipeline: sel.pipeline, RunID: req.RunID}
	if req.Since > 0 {
		f.Since = now.Add(-req.Since)
	}
	var selected []string
	for _, stream := range sel.streams {
		f.Stream = stream
		items, err := e.st.Items(ctx, f)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			if sel.retained && it.Tombstone != "" {
				continue // its payload is gone: retention or a purge left only its envelope (spec section 9.3)
			}
			if where != nil {
				v, err := pipeline.DecodeValue(string(it.Payload))
				if err != nil {
					return nil, fmt.Errorf("item %s: %w", it.ID, err)
				}
				ok, err := where.Eval(v, expr.Envelope{ID: it.ID, Key: it.Key, TS: it.TS, Step: sel.step}, vars, run)
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
