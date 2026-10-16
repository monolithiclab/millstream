package pipeline

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/schema"
)

// InstanceOptions say how CheckInstance judges the imports between packages.
type InstanceOptions struct {
	// Changed names the pipelines being checked or applied; the others are the revisions already active in the
	// instance. Nil means every package given is changed (millstream check a/ b/, run a/ b/).
	Changed []string
	// Complete says the packages are every pipeline of the instance (apply): an import from a pipeline that is not
	// among them is an error. Otherwise it is a warning that the import is not checked.
	Complete bool
	// Force turns the refusals of a producer revision (an imported stream it no longer writes, an importer's
	// contract it breaks) into warnings (spec section 4.12). Cycles and an importer's own mistakes stay errors.
	Force bool
}

// CheckInstance validates what links the pipelines of an instance (spec sections 4.8 and 4.12): each import names a
// pipeline and a stream that exist, the consumer's steps reading an imported stream (directly or through builtins)
// accept what the producer publishes on it (section 4.11, across the two packages), and imports form no cycle
// between pipelines. It returns each pipeline's findings by name, located in that pipeline's file and sorted as
// Check sorts them; a pipeline without findings is absent.
//
// A finding a producer revision causes, while the consumer is not among the changed pipelines, is reported on the
// producer (path "pipeline") and is a warning under Force; any other finding is the consumer's. Check, run on each
// package alone, covers everything else, so CheckInstance never repeats its findings.
func CheckInstance(pkgs []*Package, actions ActionResolver, opts InstanceOptions) map[string][]Diagnostic {
	ic := &instanceChecker{byName: map[string]*Package{}, actions: actions, opts: opts,
		resolved: map[*Package]map[string]*Action{}, out: map[string][]Diagnostic{}}
	for _, pkg := range pkgs {
		name := pkg.Pipeline.Name
		if _, dup := ic.byName[name]; dup {
			ic.add(name, SeverityError, "pipeline", "pipeline %s is given twice", name)
			continue
		}
		ic.byName[name] = pkg
	}
	ic.names = slices.Sorted(maps.Keys(ic.byName))
	for _, name := range ic.names {
		ic.checkImports(ic.byName[name])
	}
	ic.checkCycles()
	for name, diags := range ic.out {
		ic.out[name] = SortDiagnostics(diags)
	}
	return ic.out
}

type instanceChecker struct {
	byName   map[string]*Package
	names    []string
	actions  ActionResolver
	opts     InstanceOptions
	resolved map[*Package]map[string]*Action // each package's resolved step actions, nil where unresolved
	out      map[string][]Diagnostic
}

func (ic *instanceChecker) add(pipelineName, severity, at, format string, args ...any) {
	ic.out[pipelineName] = append(ic.out[pipelineName],
		Diagnostic{Severity: severity, Path: at, Message: fmt.Sprintf(format, args...)})
}

// changed reports whether the pipeline is among those being checked or applied.
func (ic *instanceChecker) changed(name string) bool {
	return ic.opts.Changed == nil || slices.Contains(ic.opts.Changed, name)
}

// onProducer reports whether a finding about what producer publishes to consumer goes on the producer: when only the
// producer changed. It returns the severity it has there, a warning under Force.
func (ic *instanceChecker) onProducer(producer, consumer string) (bool, string) {
	if !ic.changed(producer) || ic.changed(consumer) {
		return false, ""
	}
	if ic.opts.Force {
		return true, SeverityWarning
	}
	return true, SeverityError
}

// action returns the resolved action of a step of pkg, nil when it does not resolve (Check reports why).
func (ic *instanceChecker) action(pkg *Package, step string) *Action {
	m := ic.resolved[pkg]
	if m == nil {
		m = map[string]*Action{}
		for name, s := range pkg.Pipeline.Steps {
			if a, err := ic.actions.Resolve(pkg, s.Action); err == nil {
				m[name] = a
			}
		}
		ic.resolved[pkg] = m
	}
	return m[step]
}

// checkImports checks each stream pkg imports, and its steps' inputs against what the producers publish.
func (ic *instanceChecker) checkImports(pkg *Package) {
	consumer := pkg.Pipeline
	imports := consumer.Imports()
	if len(imports) == 0 {
		return
	}
	for _, name := range imports {
		imp, at := consumer.Streams[name].Import, joinPath("streams", name)
		producer := ic.byName[imp.Pipeline]
		switch {
		case producer == nil && ic.opts.Complete:
			ic.add(consumer.Name, SeverityError, at, "pipeline %s does not exist", imp.Pipeline)
		case producer == nil:
			ic.add(consumer.Name, SeverityWarning, at, "pipeline %s is not among the packages checked: the import of "+
				"its stream %s is not checked", imp.Pipeline, imp.Stream)
		case !producer.Pipeline.HasStream(imp.Stream):
			if onProducer, severity := ic.onProducer(imp.Pipeline, consumer.Name); onProducer {
				ic.add(imp.Pipeline, severity, "pipeline", "pipeline %s imports stream %s as %s, and this revision "+
					"no longer writes it", consumer.Name, imp.Stream, name)
			} else {
				ic.add(consumer.Name, SeverityError, at, "pipeline %s has no stream %s", imp.Pipeline, imp.Stream)
			}
		}
	}
	ic.checkContracts(pkg)
}

// checkContracts checks every step of pkg with an input schema against the schemas reaching each stream it reads
// from another pipeline, through imported streams (spec section 4.12).
func (ic *instanceChecker) checkContracts(pkg *Package) {
	consumer := pkg.Pipeline
	for _, stepName := range slices.Sorted(maps.Keys(consumer.Steps)) {
		a := ic.action(pkg, stepName)
		if a == nil || a.Input == nil {
			continue
		}
		for i, in := range consumer.Steps[stepName].In {
			at := indexPath(joinPath(joinPath("steps", stepName), "in"), i)
			for _, o := range ic.origins(pkg, in, map[string]bool{}) {
				if o.Pipeline == "" {
					continue // the package's own: Check's
				}
				for _, f := range schema.Compatible(o.Schema, a.Input) {
					where := ""
					if f.Pointer != "" {
						where = " at " + f.Pointer
					}
					msg := fmt.Sprintf("%s of pipeline %s on stream %s against the input schema of %s%s: %s", o.From,
						o.Pipeline, in, a.Ref, where, f.Message)
					if onProducer, severity := ic.onProducer(o.Pipeline, consumer.Name); onProducer &&
						f.Severity == SeverityError {
						ic.add(o.Pipeline, severity, "pipeline", "breaks the contract of pipeline %s, step %s: %s",
							consumer.Name, stepName, msg)
					} else {
						ic.add(consumer.Name, f.Severity, at, "%s", msg)
					}
				}
			}
		}
	}
}

// origins returns the schemas reaching stream in pkg, following its imports into the producers' packages; seen
// holds the pipeline and stream pairs already followed, so a cycle (reported by checkCycles) ends.
func (ic *instanceChecker) origins(pkg *Package, stream string, seen map[string]bool) []Origin {
	imported := func(name string) []Origin {
		imp := pkg.Pipeline.Streams[name].Import
		producer, key := ic.byName[imp.Pipeline], imp.Pipeline+"\x00"+imp.Stream
		if producer == nil || seen[key] {
			return nil
		}
		seen[key] = true
		out := ic.origins(producer, imp.Stream, seen)
		for i := range out {
			if out[i].Pipeline == "" {
				out[i].Pipeline = imp.Pipeline
			}
		}
		return out
	}
	return originsVia(pkg, func(step string) *Action { return ic.action(pkg, step) }, stream, nil, imported)
}

// checkCycles fails on every cycle between pipelines through imported streams, once per strongly connected
// component, on its first pipeline's import from another pipeline of the cycle.
func (ic *instanceChecker) checkCycles() {
	g := map[string][]string{}
	for _, name := range ic.names {
		p := ic.byName[name].Pipeline
		for _, s := range p.Imports() {
			producer := p.Streams[s].Import.Pipeline
			if _, ok := ic.byName[producer]; ok && !slices.Contains(g[producer], name) {
				g[producer] = append(g[producer], name)
			}
		}
	}
	for _, next := range g {
		slices.Sort(next)
	}
	for _, comp := range components(ic.names, g) {
		first := ic.byName[comp[0]].Pipeline
		at := "streams"
		for _, s := range first.Imports() {
			if slices.Contains(comp, first.Streams[s].Import.Pipeline) {
				at = joinPath("streams", s)
				break
			}
		}
		ic.add(first.Name, SeverityError, at, "cycle between pipelines through imported streams: %s",
			strings.Join(append(comp, comp[0]), " -> "))
	}
}

// HasStream reports whether p has the stream: a trigger's stream, an imported stream, or one a step writes,
// dead-letter streams included.
func (p *Pipeline) HasStream(name string) bool {
	if trigger, ok := strings.CutPrefix(name, TriggerStreamPrefix); ok {
		_, exists := p.Triggers[trigger]
		return exists
	}
	if p.Streams[name].Import != nil {
		return true
	}
	for _, s := range p.Steps {
		if s.DLQ == name || slices.Contains(s.Out.Targets(), name) {
			return true
		}
	}
	return false
}
