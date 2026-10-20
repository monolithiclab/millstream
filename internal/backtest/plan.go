package backtest

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// actionInfo is what a package's actions tell a backtest: resolveActions resolves each step's actions (its own and its fallback) to their directories in pkg, and reports the
// steps whose action declares effects: external, and the directories of exec actions.
type actionInfo struct {
	dirs     map[string][]string // step -> action directories, slash-separated, with a trailing slash
	external map[string]bool
	review   map[string]bool // the review steps
	execDirs []string
}

func resolveActions(pkg *pipeline.Package, r pipeline.ActionResolver) (actionInfo, error) {
	info := actionInfo{dirs: map[string][]string{}, external: map[string]bool{}, review: map[string]bool{}}
	for name, s := range pkg.Pipeline.Steps {
		info.review[name] = s.Action == pipeline.BuiltinReview
		refs := []string{s.Action}
		if s.Fallback != nil {
			refs = append(refs, s.Fallback.Action)
		}
		for i, ref := range refs {
			if pipeline.IsBuiltin(ref) {
				continue
			}
			a, err := r.Resolve(pkg, ref)
			if err != nil {
				return info, fmt.Errorf("step %s: %w", name, err)
			}
			if a.Effects == pipeline.EffectsExternal && i == 0 {
				info.external[name] = true
			}
			if a.Dir == "" {
				continue
			}
			dir := strings.TrimSuffix(a.Dir, "/") + "/"
			info.dirs[name] = append(info.dirs[name], dir)
			if a.Kind == pipeline.KindExec && !slices.Contains(info.execDirs, dir) {
				info.execDirs = append(info.execDirs, dir)
			}
		}
	}
	return info, nil
}

// varRef finds the variables an expression or a config names.
var varRef = regexp.MustCompile(`\bvars\.([A-Za-z_][A-Za-z0-9_]*)`)

// changedSteps returns the steps of patched the patch changes, sorted: new steps, steps whose definition moved
// (action, inputs, outputs, config, retry, timeout, fallback, lessons; not concurrency, rate, placement or pause,
// which change no output), steps whose action directory or lessons file it touches, steps naming a variable whose
// default it changes, and steps reading a stream a removed step produced. extra steps are added, each a step of
// patched.
func changedSteps(base, patched *pipeline.Package, touched, extra []string, baseInfo, patchedInfo actionInfo) (
	[]string, error,
) {
	changed := map[string]bool{}
	var vars []string
	for name, def := range patched.Pipeline.Vars {
		if old, ok := base.Pipeline.Vars[name]; !ok || !reflect.DeepEqual(old, def) {
			vars = append(vars, name)
		}
	}
	removedOut := map[string]bool{}
	for name, s := range base.Pipeline.Steps {
		if _, ok := patched.Pipeline.Steps[name]; !ok {
			for _, o := range outStreams(s) {
				removedOut[o] = true
			}
		}
	}
	for name, s := range patched.Pipeline.Steps {
		old, ok := base.Pipeline.Steps[name]
		dirs := slices.Concat(baseInfo.dirs[name], patchedInfo.dirs[name])
		switch {
		case !ok || !reflect.DeepEqual(definition(old), definition(s)):
			changed[name] = true
		case slices.ContainsFunc(touched, func(p string) bool {
			return p == s.Lessons || slices.ContainsFunc(dirs, func(dir string) bool { return strings.HasPrefix(p, dir) })
		}):
			changed[name] = true
		case slices.ContainsFunc(s.In, func(in string) bool { return removedOut[in] }):
			changed[name] = true
		case len(vars) > 0:
			raw, _ := json.Marshal([]any{s.Config, s.Out})
			for _, m := range varRef.FindAllStringSubmatch(string(raw), -1) {
				if slices.Contains(vars, m[1]) {
					changed[name] = true
				}
			}
		}
	}
	for _, name := range extra {
		if _, ok := patched.Pipeline.Steps[name]; !ok {
			return nil, fmt.Errorf("steps: the patched pipeline has no step %q: %w", name, store.ErrInvalid)
		}
		changed[name] = true
	}
	return slices.Sorted(maps.Keys(changed)), nil
}

// definition is what of a step changes its outputs.
func definition(s pipeline.Step) pipeline.Step {
	s.Concurrency, s.Rate, s.Placement, s.Paused = 0, "", nil, false
	return s
}

// outStreams are the streams a step publishes to: its outputs' and its dead letters'.
func outStreams(s pipeline.Step) []string {
	return append(s.Out.Targets(), cmp.Or(s.DLQ, pipeline.DefaultDLQ(s.Name)))
}

// affected returns the steps whose past deliveries a change may alter: the changed steps, the steps that read
// their inputs in the base (those a new step now stands before), and everything downstream of them in either
// pipeline.
func affected(base, patched *pipeline.Pipeline, changed []string) map[string]bool {
	out := map[string]bool{}
	var walk func(p *pipeline.Pipeline, step string)
	walk = func(p *pipeline.Pipeline, step string) {
		s, ok := p.Steps[step]
		if !ok {
			return
		}
		out[step] = true
		for _, stream := range outStreams(s) {
			for name, c := range p.Steps {
				if slices.Contains(c.In, stream) && !out[name] {
					walk(p, name)
				}
			}
		}
	}
	for _, name := range changed {
		for _, p := range []*pipeline.Pipeline{base, patched} {
			walk(p, name)
		}
		if s, ok := patched.Steps[name]; ok {
			for cname, c := range base.Steps {
				if slices.ContainsFunc(c.In, func(in string) bool { return slices.Contains(s.In, in) }) {
					walk(base, cname)
					walk(patched, cname)
				}
			}
		}
	}
	return out
}

// sample is what a backtest replays: the sampled deliveries, the historical runs they belong to, and the
// historical items on their lineages, which the replays keep.
type sample struct {
	sampled []evidence.Sampled
	roots   []store.Item    // the trigger items of the runs to fire again, in run order
	fired   map[string]bool // their run ids
	keep    map[string]bool
	// excluded counts the deliveries of the population left out because their lineage cannot be replayed.
	excluded int
}

// draw samples n of the history's deliveries at the affected steps that ended and whose lineage replays from a
// trigger item.
func draw(h *history, steps map[string]bool, n int) sample {
	var pop []store.Delivery
	s := sample{}
	roots := map[string]store.Item{}
	for _, d := range h.deliveries {
		if !steps[d.Step] || !ended(d) {
			continue
		}
		root, ok := h.root(d.ItemID)
		if _, run := h.runs[root.RunID]; !ok || h.source == SourceLocal && !run {
			s.excluded++
			continue
		}
		pop = append(pop, d)
		roots[d.ID] = root
	}
	s.sampled = evidence.Stratify(pop, h.reviews, n)
	var inputs []string
	s.fired = map[string]bool{}
	for _, sd := range s.sampled {
		d := h.deliveries[sd.Delivery]
		inputs = append(inputs, d.ItemID)
		if r := roots[d.ID]; !s.fired[r.RunID] {
			s.fired[r.RunID] = true
			s.roots = append(s.roots, r)
		}
	}
	slices.SortFunc(s.roots, func(a, b store.Item) int { return a.TS.Compare(b.TS) })
	s.keep = h.lineage(inputs)
	return s
}
