package pipeline

import (
	"fmt"
	"math"
	"slices"
	"time"

	"go.yaml.in/yaml/v3"
)

// ParseError is a pipeline file error located by its key path (steps.score.config) and line.
type ParseError struct {
	Path string
	Line int
	Msg  string
}

func (e *ParseError) Error() string {
	if e.Path == "" {
		return fmt.Sprintf("line %d: %s", e.Line, e.Msg)
	}
	return fmt.Sprintf("line %d: %s: %s", e.Line, e.Path, e.Msg)
}

func errAt(n *yaml.Node, path, format string, args ...any) error {
	return &ParseError{Path: path, Line: n.Line, Msg: fmt.Sprintf(format, args...)}
}

// joinPath appends a mapping key to a key path.
func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// indexPath appends a sequence index to a key path.
func indexPath(path string, i int) string { return fmt.Sprintf("%s[%d]", path, i) }

// deref follows YAML aliases to the node they name.
func deref(n *yaml.Node) *yaml.Node {
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	return n
}

// isNull reports whether a value is absent (nil) or an explicit YAML null: both mean "not set".
func isNull(n *yaml.Node) bool {
	if n == nil {
		return true
	}
	n = deref(n)
	return n.Kind == yaml.ScalarNode && n.Tag == "!!null"
}

// fields is a YAML mapping read for one struct: its entries in file order, with the path they sit at.
type fields struct {
	path string
	node *yaml.Node
	keys []*yaml.Node
	vals map[string]*yaml.Node
}

// mapping reads n as a mapping with string keys, refusing duplicate keys.
func mapping(n *yaml.Node, path string) (*fields, error) {
	n = deref(n)
	if n.Kind != yaml.MappingNode {
		return nil, errAt(n, path, "must be a mapping")
	}
	f := &fields{path: path, node: n, vals: make(map[string]*yaml.Node, len(n.Content)/2)}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := deref(n.Content[i]), n.Content[i+1]
		if k.Kind != yaml.ScalarNode || k.Tag == "!!null" {
			return nil, errAt(k, path, "keys must be strings")
		}
		if _, dup := f.vals[k.Value]; dup {
			return nil, errAt(k, joinPath(path, k.Value), "duplicate key")
		}
		f.keys = append(f.keys, k)
		f.vals[k.Value] = v
	}
	return f, nil
}

// allow fails on the first key, in file order, that is not in allowed.
func (f *fields) allow(allowed ...string) error {
	for _, k := range f.keys {
		if !slices.Contains(allowed, k.Value) {
			return errAt(k, joinPath(f.path, k.Value), "unknown key")
		}
	}
	return nil
}

// get returns the value of key, or nil when it is absent.
func (f *fields) get(key string) *yaml.Node { return f.vals[key] }

// sub returns the key path of key.
func (f *fields) sub(key string) string { return joinPath(f.path, key) }

// require returns the value of key, failing when it is absent or null.
func (f *fields) require(key string) (*yaml.Node, error) {
	v := f.vals[key]
	if isNull(v) {
		return nil, errAt(f.node, f.sub(key), "is required")
	}
	return v, nil
}

// requiredStr reads key as a required, non-empty string.
func (f *fields) requiredStr(key string) (string, error) {
	var s string
	err := required(f, key, &s, nonEmptyStr)
	return s, err
}

// required reads key into dst with read, failing when it is absent or null.
func required[T any](f *fields, key string, dst *T, read func(*yaml.Node, string) (T, error)) error {
	v, err := f.require(key)
	if err != nil {
		return err
	}
	got, err := read(v, f.sub(key))
	if err != nil {
		return err
	}
	*dst = got
	return nil
}

// optional reads key into dst with read when it is set; an absent or null key leaves dst at its default.
func optional[T any](f *fields, key string, dst *T, read func(*yaml.Node, string) (T, error)) error {
	v := f.vals[key]
	if isNull(v) {
		return nil
	}
	got, err := read(v, f.sub(key))
	if err != nil {
		return err
	}
	*dst = got
	return nil
}

// str reads a non-null scalar as a string.
func str(n *yaml.Node, path string) (string, error) {
	n = deref(n)
	if n.Kind != yaml.ScalarNode || n.Tag == "!!null" {
		return "", errAt(n, path, "must be a string")
	}
	return n.Value, nil
}

// nonEmptyStr reads a scalar that must not be empty.
func nonEmptyStr(n *yaml.Node, path string) (string, error) {
	s, err := str(n, path)
	if err == nil && s == "" {
		err = errAt(n, path, "must not be empty")
	}
	return s, err
}

// strList reads a string or a non-empty list of strings.
func strList(n *yaml.Node, path string) ([]string, error) {
	n = deref(n)
	if n.Kind == yaml.ScalarNode {
		s, err := nonEmptyStr(n, path)
		if err != nil {
			return nil, err
		}
		return []string{s}, nil
	}
	list, err := strSeq(n, path)
	if err == nil && len(list) == 0 {
		err = errAt(n, path, "must not be empty")
	}
	return list, err
}

// strSeq reads a list of non-empty strings.
func strSeq(n *yaml.Node, path string) ([]string, error) {
	n = deref(n)
	if n.Kind != yaml.SequenceNode {
		return nil, errAt(n, path, "must be a list of strings")
	}
	out := make([]string, 0, len(n.Content))
	for i, c := range n.Content {
		s, err := nonEmptyStr(c, indexPath(path, i))
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// positiveInt reads an integer of at least 1.
func positiveInt(n *yaml.Node, path string) (int, error) {
	n = deref(n)
	var v int
	if n.Kind != yaml.ScalarNode || n.Tag != "!!int" || n.Decode(&v) != nil || v < 1 {
		return 0, errAt(n, path, "must be a positive integer")
	}
	return v, nil
}

// boolean reads true or false.
func boolean(n *yaml.Node, path string) (bool, error) {
	n = deref(n)
	var v bool
	if n.Kind != yaml.ScalarNode || n.Tag != "!!bool" || n.Decode(&v) != nil {
		return false, errAt(n, path, "must be true or false")
	}
	return v, nil
}

// nonNegativeNumber reads an integer or a float of at least 0.
func nonNegativeNumber(n *yaml.Node, path string) (float64, error) {
	n = deref(n)
	var v float64
	if n.Kind != yaml.ScalarNode || (n.Tag != "!!int" && n.Tag != "!!float") || n.Decode(&v) != nil ||
		v < 0 || math.IsInf(v, 0) || math.IsNaN(v) {
		return 0, errAt(n, path, "must be a non-negative number")
	}
	return v, nil
}

// duration reads a duration such as 10s, 72h or 90d.
func duration(n *yaml.Node, path string) (time.Duration, error) {
	s, err := str(n, path)
	if err != nil {
		return 0, err
	}
	d, err := ParseDuration(s)
	if err != nil {
		return 0, errAt(n, path, "%v", err)
	}
	return d, nil
}

// value decodes any node into plain Go values: map[string]any, []any, string, bool, int64, float64 or nil.
func value(n *yaml.Node, path string) (any, error) {
	n = deref(n)
	switch n.Kind {
	case yaml.MappingNode:
		f, err := mapping(n, path)
		if err != nil {
			return nil, err
		}
		m := make(map[string]any, len(f.keys))
		for _, k := range f.keys {
			v, err := value(f.vals[k.Value], f.sub(k.Value))
			if err != nil {
				return nil, err
			}
			m[k.Value] = v
		}
		return m, nil
	case yaml.SequenceNode:
		list := make([]any, 0, len(n.Content))
		for i, c := range n.Content {
			v, err := value(c, indexPath(path, i))
			if err != nil {
				return nil, err
			}
			list = append(list, v)
		}
		return list, nil
	default:
		return scalarValue(n, path)
	}
}

func scalarValue(n *yaml.Node, path string) (any, error) {
	switch n.Tag {
	case "!!null":
		return nil, nil
	case "!!bool":
		var b bool
		if err := n.Decode(&b); err != nil {
			return nil, errAt(n, path, "%v", err)
		}
		return b, nil
	case "!!int":
		var i int64
		if err := n.Decode(&i); err != nil {
			return nil, errAt(n, path, "integer out of range")
		}
		return i, nil
	case "!!float":
		var f float64
		if err := n.Decode(&f); err != nil {
			return nil, errAt(n, path, "%v", err)
		}
		return f, nil
	case "!!str":
		return n.Value, nil
	default:
		return nil, errAt(n, path, "unsupported YAML value of type %s", n.Tag)
	}
}
