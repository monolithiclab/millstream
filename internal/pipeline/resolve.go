package pipeline

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/ids"
)

// Sources of a resolved variable, highest precedence first.
const (
	SourceFlag    = "flag"
	SourceEnv     = "env"
	SourceDefault = "default"
)

// Resolved is a pipeline's variables and secrets resolved for one run.
type Resolved struct {
	Vars map[string]any
	// Sources records where each variable came from: flag, env or default.
	Sources map[string]string
	Secrets map[string]string
}

// VarEnv is the environment variable overriding variable name: MILLSTREAM_VAR_<UPPER_SNAKE>.
func VarEnv(name string) string { return "MILLSTREAM_VAR_" + ids.UpperSnake(name) }

// SecretEnv is the environment variable a secret resolves from without an environment file:
// MILLSTREAM_SECRET_<UPPER_SNAKE>.
func SecretEnv(name string) string { return "MILLSTREAM_SECRET_" + ids.UpperSnake(name) }

// SecretValues returns the values of the secrets named, by name, read from env (SecretEnv); an unset one, or an
// empty name, is left out.
func SecretValues(names []string, env func(string) (string, bool)) map[string]string {
	secrets := map[string]string{}
	for _, name := range names {
		if name == "" {
			continue
		}
		if v, ok := env(SecretEnv(name)); ok {
			secrets[name] = v
		}
	}
	return secrets
}

// ResolveVars resolves p's variables (spec section 4.4) and secrets (section 4.5). Each variable takes, highest
// first, its --var flag, its MILLSTREAM_VAR_ environment variable, then its default. A value is parsed as JSON
// unless the default is a string, and must have the default's type; a variable with a null default is required
// and takes any JSON value, or the raw text when it is not JSON. Every problem is reported in one error, and the
// returned Resolved holds everything that did resolve, so callers such as `millstream vars` can still show it.
func ResolveVars(p *Pipeline, flags map[string]string, env func(string) (string, bool)) (Resolved, error) {
	return ResolveVarsWith(p, flags, env, EnvSecrets(env)(p.Name))
}

// ResolveVarsWith is ResolveVars with the pipeline's secrets resolved through secrets: the environment file's
// mapping (Environment.SecretLookup).
func ResolveVarsWith(p *Pipeline, flags map[string]string, env func(string) (string, bool), secrets SecretLookup,
) (Resolved, error) {
	r := Resolved{Vars: map[string]any{}, Sources: map[string]string{}, Secrets: map[string]string{}}
	var problems []string
	for _, name := range slices.Sorted(maps.Keys(flags)) {
		if _, ok := p.Vars[name]; !ok {
			problems = append(problems, fmt.Sprintf("--var %s: pipeline %s declares no such variable", name, p.Name))
		}
	}
	for _, name := range slices.Sorted(maps.Keys(p.Vars)) {
		def := p.Vars[name]
		raw, set := flags[name]
		source := SourceFlag
		if !set {
			raw, set = env(VarEnv(name))
			source = SourceEnv
		}
		if !set {
			if def == nil {
				problems = append(problems, fmt.Sprintf("variable %s is required: set --var %s=… or %s", name, name,
					VarEnv(name)))
				continue
			}
			r.Vars[name], r.Sources[name] = def, SourceDefault
			continue
		}
		v, err := convertVar(raw, def)
		if err != nil {
			problems = append(problems, fmt.Sprintf("variable %s from %s: %v", name, source, err))
			continue
		}
		r.Vars[name], r.Sources[name] = v, source
	}
	for _, name := range p.Secrets {
		v, err := secrets(name)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		r.Secrets[name] = v
	}
	if len(problems) > 0 {
		return r, fmt.Errorf("resolve pipeline %s: %s", p.Name, strings.Join(problems, "; "))
	}
	return r, nil
}

// convertVar parses raw as the type of def.
func convertVar(raw string, def any) (any, error) {
	if _, ok := def.(string); ok {
		return raw, nil
	}
	v, err := DecodeValue(raw)
	if err != nil {
		if def == nil {
			return raw, nil
		}
		return nil, fmt.Errorf("not valid JSON: %w", err)
	}
	if def == nil {
		return v, nil
	}
	want := typeName(def)
	if want == "number" {
		if i, ok := v.(int64); ok {
			v = float64(i)
		}
	}
	if got := typeName(v); got != want {
		return nil, fmt.Errorf("want %s like the default, got %s", want, got)
	}
	return v, nil
}

// typeName names the type of a variable value for matching and messages.
func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "boolean"
	case int64:
		return "integer"
	case float64:
		return "number"
	case []any:
		return "list"
	case map[string]any:
		return "map"
	default:
		return fmt.Sprintf("%T", v)
	}
}
