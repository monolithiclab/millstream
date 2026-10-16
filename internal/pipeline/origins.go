package pipeline

import (
	"cmp"
	"slices"
	"strings"
)

// Origin is a schema the items on a stream are known to satisfy, and what produced them.
type Origin struct {
	From   string
	Schema []byte
}

// Origins returns the schemas of what reaches stream in pkg: each producing action's output schema, a trigger's input
// schema, and, through builtins, which pass items through unchanged, the origins of what they read. action returns a
// step's resolved action, nil when it did not resolve; report receives a trigger input schema that cannot be read,
// located by its key path, when report is not nil. Each origin is listed once, however many paths lead from it, sorted by From.
func Origins(pkg *Package, action func(step string) *Action, stream string, report func(at string, err error)) []Origin {
	producers, _ := pkg.Pipeline.streamIndex()
	return origins(pkg, producers, action, stream, report, map[string]bool{})
}

func origins(pkg *Package, producers map[string][]string, action func(string) *Action, stream string,
	report func(string, error), visiting map[string]bool,
) []Origin {
	if visiting[stream] {
		return nil
	}
	visiting[stream] = true
	defer delete(visiting, stream)
	p := pkg.Pipeline
	if trigger, ok := strings.CutPrefix(stream, TriggerStreamPrefix); ok {
		t := p.Triggers[trigger]
		if t.InputSchema == nil {
			return nil
		}
		doc, err := schemaDoc(pkg, ".", t.InputSchema)
		if err != nil {
			if report != nil {
				report(joinPath(joinPath("triggers", trigger), "inputSchema"), err)
			}
			return nil
		}
		return []Origin{{From: "trigger " + trigger, Schema: doc}}
	}
	var out []Origin
	for _, w := range producers[stream] {
		s, a := p.Steps[w], action(w)
		if a == nil || !slices.Contains(s.Out.Targets(), stream) {
			continue // unresolved, or the stream is w's dead-letter stream, whose payload is not w's output
		}
		if !a.Builtin {
			if a.Output != nil {
				out = append(out, Origin{From: "output of step " + w, Schema: a.Output})
			}
			continue
		}
		for _, in := range s.In {
			out = append(out, origins(pkg, producers, action, in, report, visiting)...)
		}
	}
	slices.SortFunc(out, func(a, b Origin) int { return cmp.Compare(a.From, b.From) })
	return slices.CompactFunc(out, func(a, b Origin) bool { return a.From == b.From })
}
