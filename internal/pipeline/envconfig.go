package pipeline

import (
	"cmp"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// VarPrefix starts the name of every variable that sets an environment key (MILLSTREAM_UI_BASE_URL).
const VarPrefix = "MILLSTREAM_"

// Duration is a positive duration in the environment file's syntax: Go's (30s, 1h30m) or whole days (90d).
type Duration time.Duration

// Set parses s as ParseDuration does.
func (d *Duration) Set(s string) error {
	v, err := ParseDuration(s)
	*d = Duration(v)
	return err
}

// UnmarshalYAML reads a scalar duration.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return errors.New("must be a duration such as 30s or 90d")
	}
	return d.Set(n.Value)
}

// Setting is one leaf of the environment: its file key, its variable, its default (in the file's syntax, empty
// without one), an example value for one without, its one-line doc, and the field holding its value.
type Setting struct {
	Key, Var, Default, Example, Doc string
	Value                           reflect.Value
}

// Settings returns e's leaves in declaration order: every field of a section struct that is not a section itself.
func Settings(e *Environment) []Setting {
	var out []Setting
	var walk func(v reflect.Value, key, name string)
	walk = func(v reflect.Value, key, name string) {
		for i := range v.NumField() {
			f := v.Type().Field(i)
			k, n := joinPath(key, f.Tag.Get("yaml")), name+"_"+f.Tag.Get("env")
			if f.Type.Kind() == reflect.Struct {
				walk(v.Field(i), k, n)
				continue
			}
			out = append(out, Setting{Key: k, Var: VarPrefix + n[1:], Default: f.Tag.Get("default"),
				Example: f.Tag.Get("example"), Doc: f.Tag.Get("doc"), Value: v.Field(i)})
		}
	}
	walk(reflect.ValueOf(e).Elem(), "", "")
	return out
}

// Defaults returns the environment with every default tag applied, and nothing else.
func Defaults() *Environment {
	e := &Environment{}
	for _, s := range Settings(e) {
		if s.Default != "" {
			if err := setText(s, s.Default); err != nil {
				panic(fmt.Sprintf("%s: default %q: %v", s.Key, s.Default, err)) // a tag of this file is wrong
			}
		}
	}
	return e
}

// applyVariables sets every leaf whose variable lookup holds a non-empty value. names are the variables of the
// process (NAME or NAME=value): one that names a section (MILLSTREAM_UI) or a map (MILLSTREAM_REGISTRIES), or falls
// under one (MILLSTREAM_UI_BASEURL, MILLSTREAM_REGISTRIES_LAB), but names no leaf is an error, so a misspelt setting
// is never ignored; the others (MILLSTREAM_SERVER, MILLSTREAM_SECRET_<NAME>) are not settings and are left alone.
func applyVariables(e *Environment, lookup func(string) (string, bool), names []string) error {
	known := map[string]bool{}
	squeezed := map[string]string{}
	var scopes []string // the sections, and the maps settable only whole: no variable but a setting is under one
	t := reflect.TypeFor[Environment]()
	for i := range t.NumField() {
		if f := t.Field(i); f.Type.Kind() == reflect.Struct {
			scopes = append(scopes, VarPrefix+f.Tag.Get("env"))
		}
	}
	for _, s := range Settings(e) {
		known[s.Var], squeezed[strings.ReplaceAll(s.Var, "_", "")] = true, s.Var
		if s.Value.Kind() == reflect.Map {
			scopes = append(scopes, s.Var)
		}
		if v, ok := lookup(s.Var); ok && v != "" {
			if err := setText(s, v); err != nil {
				return fmt.Errorf("%s: %w", s.Var, err)
			}
		}
	}
	for _, kv := range names {
		name, _, _ := strings.Cut(kv, "=")
		if _, set := lookup(name); known[name] || !set {
			continue
		}
		for _, p := range scopes {
			if name == p || strings.HasPrefix(name, p+"_") {
				err := fmt.Errorf("%s: no such setting", name)
				if like, ok := squeezed[strings.ReplaceAll(name, "_", "")]; ok {
					err = fmt.Errorf("%w; did you mean %s?", err, like)
				}
				return err
			}
		}
	}
	return nil
}

// setText sets s from a variable's (or a default tag's) text: a secret source becomes { env: <s.Var> }, the
// variable holding the secret itself; a list or a map is YAML flow syntax ({ lab: ghcr.io/x }), replacing the
// whole value; any other leaf is a scalar.
func setText(s Setting, text string) error {
	n := &yaml.Node{Kind: yaml.ScalarNode, Value: text}
	switch s.Value.Kind() {
	case reflect.Pointer:
		s.Value.Set(reflect.ValueOf(&SecretSource{Env: s.Var}))
		return nil
	case reflect.Map, reflect.Slice:
		var doc yaml.Node
		if err := yaml.Unmarshal([]byte(text), &doc); err != nil || len(doc.Content) == 0 {
			return fmt.Errorf("%s: want YAML flow syntax, such as { key: value } or [a, b]", s.Key)
		}
		n = doc.Content[0]
	case reflect.String:
		n.Tag = "!!str"
	}
	var pe *ParseError
	err := decode(n, s.Value, s.Key)
	if errors.As(err, &pe) {
		return fmt.Errorf("%s: %s", pe.Path, pe.Msg)
	}
	return err
}

// decode reads n into v strictly, as the environment file holds it: a mapping key no field's yaml tag names is an
// error, and every error is located by its key path. A null leaves v as it is.
func decode(n *yaml.Node, v reflect.Value, path string) error {
	if isNull(n) {
		return nil
	}
	n = deref(n)
	seen := map[string]bool{}
	for i := 0; n.Kind == yaml.MappingNode && i < len(n.Content); i += 2 {
		if k := n.Content[i]; seen[k.Value] {
			return errAt(k, joinPath(path, k.Value), "duplicate key")
		}
		seen[n.Content[i].Value] = true
	}
	if u, ok := v.Addr().Interface().(yaml.Unmarshaler); ok {
		if err := u.UnmarshalYAML(n); err != nil {
			return errAt(n, path, "%v", err)
		}
		return nil
	}
	switch v.Kind() {
	case reflect.Pointer:
		p := reflect.New(v.Type().Elem())
		if err := decode(n, p.Elem(), path); err != nil {
			return err
		}
		v.Set(p)
	case reflect.Struct:
		if n.Kind != yaml.MappingNode {
			if v.Type() == reflect.TypeFor[SecretSource]() {
				return errAt(n, path, "want { env: NAME } or { file: path }: a secret is never written in the "+
					"environment file")
			}
			return errAt(n, path, "must be a mapping")
		}
	keys:
		for i := 0; i < len(n.Content); i += 2 {
			k, at := n.Content[i], joinPath(path, n.Content[i].Value)
			for j := range v.NumField() {
				if v.Type().Field(j).Tag.Get("yaml") == k.Value {
					if err := decode(n.Content[i+1], v.Field(j), at); err != nil {
						return err
					}
					continue keys
				}
			}
			return errAt(k, at, "unknown key")
		}
		for j := range v.NumField() {
			if f := v.Type().Field(j); f.Tag.Get("required") != "" && !seen[f.Tag.Get("yaml")] {
				return errAt(n, joinPath(path, f.Tag.Get("yaml")), "is required")
			}
		}
	case reflect.Map:
		if n.Kind != yaml.MappingNode {
			return errAt(n, path, "must be a mapping")
		}
		m := reflect.MakeMapWithSize(v.Type(), len(n.Content)/2)
		for i := 0; i < len(n.Content); i += 2 {
			elem := reflect.New(v.Type().Elem()).Elem()
			if err := decode(n.Content[i+1], elem, joinPath(path, n.Content[i].Value)); err != nil {
				return err
			}
			m.SetMapIndex(reflect.ValueOf(n.Content[i].Value), elem)
		}
		v.Set(m)
	case reflect.Slice:
		if n.Kind != yaml.SequenceNode {
			return errAt(n, path, "must be a list")
		}
		s := reflect.MakeSlice(v.Type(), len(n.Content), len(n.Content))
		for i, c := range n.Content {
			if err := decode(c, s.Index(i), indexPath(path, i)); err != nil {
				return err
			}
		}
		v.Set(s)
	default:
		if n.Kind != yaml.ScalarNode || v.Kind() == reflect.Int && n.ShortTag() != "!!int" ||
			n.Decode(v.Addr().Interface()) != nil {
			return errAt(n, path, "must be %s", scalarKinds[v.Kind()])
		}
	}
	return nil
}

// scalarKinds names what a scalar leaf of each kind holds, for decode's errors.
var scalarKinds = map[reflect.Kind]string{reflect.Bool: "true or false", reflect.Int: "an integer",
	reflect.Float64: "a number", reflect.String: "a string"}

// fileWidth is the column the generated file's comments wrap at.
const fileWidth = 120

// DefaultFile is the environment file `millstream init environment` writes: every setting commented out at its
// default (at an example when it has none), grouped by section, each key under its doc and followed by its variable.
func DefaultFile() []byte {
	var b strings.Builder
	b.WriteString("## millstream.yaml: the environment file (spec section 14).\n##\n" +
		"## Every key is commented out at its default, or at an example value when it has none (marked \"example\"):\n" +
		"## uncomment and edit the ones to change. The MILLSTREAM_* variable named on each key overrides this file,\n" +
		"## and a flag overrides both.\n")
	var open []string // the sections the last line sits in
	for _, s := range Settings(Defaults()) {
		path := strings.Split(s.Key, ".")
		sections, leaf := path[:len(path)-1], path[len(path)-1]
		common := 0
		for common < len(open) && common < len(sections) && open[common] == sections[common] {
			common++
		}
		b.WriteByte('\n')
		for i := common; i < len(sections); i++ {
			fmt.Fprintf(&b, "# %s%s:\n", strings.Repeat("  ", i), sections[i])
		}
		open = sections
		indent := strings.Repeat("  ", len(sections))
		line := "## " + indent
		for _, w := range strings.Fields(s.Doc) {
			if len(line)+len(w) >= fileWidth && strings.TrimSpace(line) != "##" {
				b.WriteString(line + "\n")
				line = "## " + indent
			} else if line != "## "+indent {
				line += " "
			}
			line += w
		}
		b.WriteString(line + "\n")
		note := ""
		if s.Default == "" {
			note = ", example"
		}
		fmt.Fprintf(&b, "# %s%s: %s   # %s%s\n", indent, leaf, cmp.Or(s.Default, s.Example), s.Var, note)
	}
	return []byte(b.String())
}
