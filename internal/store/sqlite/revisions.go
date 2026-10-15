package sqlite

import (
	"fmt"
	"slices"
	"sync"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// revision is a revision's pipeline with what the units derive from it: the steps reading each stream and the
// compiled route expressions. It is immutable once built.
type revision struct {
	hash      string
	p         *pipeline.Pipeline
	consumers map[string][]string // stream → steps reading it, sorted
	routes    map[string][]route  // step → its route objects, compiled
}

type route struct {
	out  string
	when *expr.ItemExpr
	err  error // a when that does not compile fails every output it is evaluated for
}

func newRevision(hash string, data []byte) (*revision, error) {
	p, err := pipeline.UnmarshalPipeline(data)
	if err != nil {
		return nil, err
	}
	r := &revision{hash: hash, p: p, consumers: map[string][]string{}, routes: map[string][]route{}}
	for name, step := range p.Steps {
		for _, in := range step.In {
			r.consumers[in] = append(r.consumers[in], name)
		}
		for _, ro := range step.Out.Routes {
			when, err := expr.CompileRoute(ro.When)
			r.routes[name] = append(r.routes[name], route{out: ro.Out, when: when, err: err})
		}
	}
	for _, steps := range r.consumers {
		slices.Sort(steps)
	}
	return r, nil
}

// revision loads revision id through the unit's transaction, so a revision applied earlier in the same batch is
// visible. Revisions are cached by package hash: a revision's pipeline is a function of its package, so an entry
// is never stale, even for a revision row a rolled-back batch inserted and a later batch inserted again under the
// same id.
func (w *wtx) revision(id store.RevisionID) (*revision, error) {
	var hash, data string
	if err := w.queryRow(`SELECT package_hash, pipeline_json FROM revisions WHERE id = ?`, id).
		Scan(&hash, &data); err != nil {
		return nil, notFound(err, fmt.Sprintf("revision %d", id))
	}
	if r := w.s.revs[hash]; r != nil {
		return r, nil
	}
	key := hash + "\x00" + data
	if r, ok := revisions.Load(key); ok {
		w.s.revs[hash] = r.(*revision)
		return r.(*revision), nil
	}
	r, err := newRevision(hash, []byte(data))
	if err != nil {
		return nil, fmt.Errorf("revision %d: %w", id, err)
	}
	revisions.Store(key, r)
	w.s.revs[hash] = r
	return r, nil
}

// revisions caches the revisions every store of the process built, by package hash and pipeline: compiling a
// pipeline's routes costs more than most units, and the stores of a process (tests, millstream test) apply the same
// few pipelines over and over.
var revisions sync.Map

// step returns a step of the revision, or an error naming it when the revision has no such step.
func (r *revision) step(name string) (pipeline.Step, error) {
	s, ok := r.p.Steps[name]
	if !ok {
		return s, fmt.Errorf("step %q is not in revision %s: %w", name, r.hash, store.ErrConflict)
	}
	return s, nil
}

// stepOrOrphan returns a step of the revision and true, or, for a step the revision lacks, an orphan and false: a
// step with no retries whose dead letters go to the default <step>.dlq. Migrate refuses to create orphans, but a
// delivery that has one still ends dead rather than staying leased behind an error.
func (r *revision) stepOrOrphan(name string) (pipeline.Step, bool) {
	if s, ok := r.p.Steps[name]; ok {
		return s, true
	}
	return pipeline.Step{Name: name, DLQ: pipeline.DefaultDLQ(name)}, false
}

// routing is where one output goes: the streams it publishes to, or unrouted when route objects matched none.
type routing struct {
	streams  []string
	unrouted bool
}

// route decides where an output of step goes (spec section 4.3): every stream of a stream list, the first route
// object whose when holds, or unrouted. A when that fails to evaluate is an error, and deterministic.
func (r *revision) route(step pipeline.Step, payload any, vars map[string]any, run expr.Run) (routing, error) {
	if len(step.Out.Routes) == 0 {
		return routing{streams: step.Out.Streams}, nil
	}
	for _, ro := range r.routes[step.Name] {
		if ro.err != nil {
			return routing{}, ro.err
		}
		ok, err := ro.when.Eval(payload, expr.Envelope{}, vars, run)
		if err != nil {
			return routing{}, err
		}
		if ok {
			return routing{streams: []string{ro.out}}, nil
		}
	}
	return routing{unrouted: true}, nil
}
