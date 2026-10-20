package proposals

import (
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// The path rules let config and topology proposals change pipeline.yaml; what they may change in it is checked on
// the parsed files (spec section 11.7). A config proposal changes a step's retry, timeout or concurrency, a
// threshold (a filter's config, a numeric variable's default) or a review's autoApprove and reasons, and nothing
// else. Neither kind adds a trigger, changes one, or reads a secret a step did not read already.

// reviewConfigKeys are the keys of a review step's config a config proposal may change.
var reviewConfigKeys = []string{"autoApprove", "reasons"}

// semantic checks the pipeline change of a config or topology proposal.
func semantic(kind store.ProposalKind, base, patched *pipeline.Pipeline) error {
	if kind != store.ProposalConfig && kind != store.ProposalTopology {
		return nil
	}
	refuse := func(format string, args ...any) error {
		return fmt.Errorf("%w: a %s proposal may not %s", patch.ErrPatch, kind, fmt.Sprintf(format, args...))
	}
	if !reflect.DeepEqual(base.Triggers, patched.Triggers) {
		return refuse("add or change a trigger")
	}
	if extra := slices.DeleteFunc(slices.Clone(patched.Secrets), func(s string) bool {
		return slices.Contains(base.Secrets, s)
	}); len(extra) > 0 {
		return refuse("declare the secret %s", extra[0])
	}
	for _, name := range slices.Sorted(maps.Keys(patched.Steps)) {
		was := secretRefs(base.Steps[name]) // a new step's base is the zero step, reading no secret
		for ref := range secretRefs(patched.Steps[name]) {
			if !was[ref] {
				return refuse("make step %s read the secret %s", name, ref)
			}
		}
	}
	if kind == store.ProposalTopology {
		return nil
	}
	if err := configOnly(base, patched); err != nil {
		return refuse("%s: only a step's retry, timeout or concurrency, a filter's config, a numeric variable's "+
			"default, and a review's autoApprove or reasons", err)
	}
	return nil
}

// configOnly reports the first change a config proposal may not make.
func configOnly(base, patched *pipeline.Pipeline) error {
	b, p := *base, *patched
	b.Steps, p.Steps, b.Vars, p.Vars = nil, nil, nil, nil
	if !reflect.DeepEqual(b, p) {
		return fmt.Errorf("change the pipeline's settings")
	}
	if !slices.Equal(slices.Sorted(maps.Keys(base.Vars)), slices.Sorted(maps.Keys(patched.Vars))) {
		return fmt.Errorf("add or remove a variable")
	}
	for name, v := range patched.Vars {
		if was := base.Vars[name]; !reflect.DeepEqual(was, v) && (!isNumber(was) || !isNumber(v)) {
			return fmt.Errorf("change the variable %s", name)
		}
	}
	if !slices.Equal(slices.Sorted(maps.Keys(base.Steps)), slices.Sorted(maps.Keys(patched.Steps))) {
		return fmt.Errorf("add or remove a step")
	}
	for name, s := range patched.Steps {
		was := base.Steps[name]
		s.Retry, s.Timeout, s.Concurrency = was.Retry, was.Timeout, was.Concurrency
		switch s.Action {
		case pipeline.BuiltinFilter:
			if was.Action == s.Action {
				s.Config = was.Config
			}
		case pipeline.BuiltinReview:
			if was.Action == s.Action {
				cfg := maps.Clone(s.Config)
				for _, k := range reviewConfigKeys {
					delete(cfg, k)
					if v, ok := was.Config[k]; ok {
						cfg[k] = v
					}
				}
				s.Config = cfg
			}
		}
		if !reflect.DeepEqual(was, s) {
			return fmt.Errorf("change step %s", name)
		}
	}
	return nil
}

func isNumber(v any) bool {
	switch v.(type) {
	case int64, float64:
		return true
	}
	return false
}

// secretRefs returns the secrets a step's config reads: the names its interpolations name, "*" for one that may
// read any, and a review's notify secret.
func secretRefs(s pipeline.Step) map[string]bool {
	out := map[string]bool{}
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			refs, err := expr.TemplateRefs(x)
			if err != nil {
				return // check reports it
			}
			for _, name := range refs.Secrets {
				out[name] = true
			}
			if refs.AllSecrets {
				out["*"] = true
			}
		case map[string]any:
			for _, c := range x {
				walk(c)
			}
		case []any:
			for _, c := range x {
				walk(c)
			}
		}
	}
	walk(s.Config)
	if s.Action == pipeline.BuiltinReview {
		if r, _ := pipeline.ParseReview(s.Name, s.Config); r != nil && r.NotifySecret != "" {
			out[r.NotifySecret] = true
		}
	}
	return out
}
