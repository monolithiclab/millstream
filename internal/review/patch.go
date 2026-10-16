package review

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/monolithiclab/millstream/internal/schema"
)

// Op is one operation of a JSON Patch (RFC 6902).
type Op struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	From  string `json:"from,omitempty"`
	Value any    `json:"value,omitempty"`
}

// MarshalJSON encodes the operation with its value whenever the op takes one, even a zero value.
func (o Op) MarshalJSON() ([]byte, error) {
	type plain struct {
		Op    string `json:"op"`
		Path  string `json:"path"`
		From  string `json:"from,omitempty"`
		Value *any   `json:"value,omitempty"`
	}
	p := plain{Op: o.Op, Path: o.Path, From: o.From}
	if o.Op == "add" || o.Op == "replace" || o.Op == "test" {
		p.Value = &o.Value
	}
	return json.Marshal(p)
}

// ErrPatch marks a patch that cannot be applied, or that edits outside the editable pointers.
var ErrPatch = errors.New("invalid edit")

// ParsePatch decodes a JSON Patch document: an array of operations.
func ParsePatch(data []byte) ([]Op, error) {
	var ops []Op
	if err := json.Unmarshal(data, &ops); err != nil {
		return nil, fmt.Errorf("a JSON Patch is an array of operations: %w: %w", ErrPatch, err)
	}
	return ops, nil
}

// Editable checks that every pointer an operation writes or reads from is one of the editable pointers or below
// one: /notes allows /notes and /notes/0, never /notesExtra.
func Editable(ops []Op, editable []string) error {
	for i, o := range ops {
		paths := []string{o.Path}
		if o.Op == "move" || o.Op == "copy" {
			paths = append(paths, o.From)
		}
		for _, p := range paths {
			if !under(p, editable) {
				return fmt.Errorf("operation %d edits %s, which is not editable (editable: %s): %w", i, p,
					strings.Join(editable, ", "), ErrPatch)
			}
		}
	}
	return nil
}

func under(p string, editable []string) bool {
	return slices.ContainsFunc(editable, func(e string) bool { return p == e || strings.HasPrefix(p, e+"/") })
}

// Apply applies a JSON Patch to doc, a decoded JSON value, and returns the result; doc is not changed. It supports
// add, remove, replace, move, copy and test.
func Apply(doc any, ops []Op) (any, error) {
	out := clone(doc)
	for i, o := range ops {
		var err error
		switch o.Op {
		case "add":
			out, err = set(out, o.Path, clone(o.Value), true)
		case "remove":
			out, _, err = remove(out, o.Path)
		case "replace":
			if _, err = get(out, o.Path); err == nil {
				out, err = set(out, o.Path, clone(o.Value), false)
			}
		case "move", "copy":
			var v any
			if v, err = get(out, o.From); err != nil {
				break
			}
			if o.Op == "move" {
				if strings.HasPrefix(o.Path, o.From+"/") {
					err = errors.New("cannot move a value into itself")
					break
				}
				if out, _, err = remove(out, o.From); err != nil {
					break
				}
			}
			out, err = set(out, o.Path, clone(v), true)
		case "test":
			var v any
			if v, err = get(out, o.Path); err == nil && !reflect.DeepEqual(normal(v), normal(o.Value)) {
				err = fmt.Errorf("%s does not hold the tested value", o.Path)
			}
		default:
			err = fmt.Errorf("unknown op %q", o.Op)
		}
		if err != nil {
			return nil, fmt.Errorf("operation %d (%s %s): %w: %w", i, o.Op, o.Path, ErrPatch, err)
		}
	}
	return out, nil
}

// Diff returns a JSON Patch turning a into b: objects compared member by member, any other differing value
// replaced whole. Members are visited in sorted order, so a diff is stable.
func Diff(a, b any) []Op {
	return diff("", normal(a), normal(b), nil)
}

func diff(at string, a, b any, ops []Op) []Op {
	ma, aok := a.(map[string]any)
	mb, bok := b.(map[string]any)
	if !aok || !bok {
		if !reflect.DeepEqual(a, b) {
			ops = append(ops, Op{Op: "replace", Path: at, Value: b})
		}
		return ops
	}
	for _, k := range slices.Sorted(maps.Keys(ma)) {
		p := at + "/" + schema.EscapeToken(k)
		if vb, ok := mb[k]; ok {
			ops = diff(p, ma[k], vb, ops)
		} else {
			ops = append(ops, Op{Op: "remove", Path: p})
		}
	}
	for _, k := range slices.Sorted(maps.Keys(mb)) {
		if _, ok := ma[k]; !ok {
			ops = append(ops, Op{Op: "add", Path: at + "/" + schema.EscapeToken(k), Value: mb[k]})
		}
	}
	return ops
}

// Pointers returns the pointers a patch writes, in order, without repeats.
func Pointers(ops []Op) []string {
	var out []string
	for _, o := range ops {
		if !slices.Contains(out, o.Path) {
			out = append(out, o.Path)
		}
	}
	return out
}

// tokens splits a JSON pointer into its unescaped reference tokens; "" is the whole document.
func tokens(p string) ([]string, error) {
	if p == "" {
		return nil, nil
	}
	if !strings.HasPrefix(p, "/") {
		return nil, fmt.Errorf("%q is not a JSON pointer", p)
	}
	ts := strings.Split(p[1:], "/")
	for i, t := range ts {
		ts[i] = schema.UnescapeToken(t)
	}
	return ts, nil
}

func get(doc any, p string) (any, error) {
	ts, err := tokens(p)
	if err != nil {
		return nil, err
	}
	for _, t := range ts {
		switch c := doc.(type) {
		case map[string]any:
			v, ok := c[t]
			if !ok {
				return nil, fmt.Errorf("%s does not exist", p)
			}
			doc = v
		case []any:
			i, err := index(t, len(c), false)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", p, err)
			}
			doc = c[i]
		default:
			return nil, fmt.Errorf("%s does not exist", p)
		}
	}
	return doc, nil
}

// set writes v at p, whose parent must exist: into an object member, or into an array at an index (inserted when
// insert is set, "-" appending) or replacing one.
func set(doc any, p string, v any, insert bool) (any, error) {
	ts, err := tokens(p)
	if err != nil {
		return nil, err
	}
	if len(ts) == 0 {
		return v, nil
	}
	parent, err := get(doc, pointer(ts[:len(ts)-1]))
	if err != nil {
		return nil, err
	}
	last := ts[len(ts)-1]
	switch c := parent.(type) {
	case map[string]any:
		c[last] = v
	case []any:
		i, err := index(last, len(c), insert)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if insert {
			c = slices.Insert(c, i, v)
		} else {
			c[i] = v
		}
		return replaceAt(doc, ts[:len(ts)-1], c)
	default:
		return nil, fmt.Errorf("the parent of %s is not an object or an array", p)
	}
	return doc, nil
}

func remove(doc any, p string) (any, any, error) {
	ts, err := tokens(p)
	if err != nil {
		return nil, nil, err
	}
	if len(ts) == 0 {
		return nil, nil, errors.New("cannot remove the whole document")
	}
	parent, err := get(doc, pointer(ts[:len(ts)-1]))
	if err != nil {
		return nil, nil, err
	}
	last := ts[len(ts)-1]
	switch c := parent.(type) {
	case map[string]any:
		v, ok := c[last]
		if !ok {
			return nil, nil, fmt.Errorf("%s does not exist", p)
		}
		delete(c, last)
		return doc, v, nil
	case []any:
		i, err := index(last, len(c), false)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", p, err)
		}
		v := c[i]
		out, err := replaceAt(doc, ts[:len(ts)-1], slices.Delete(c, i, i+1))
		return out, v, err
	}
	return nil, nil, fmt.Errorf("%s does not exist", p)
}

// replaceAt puts the array arr, resized, back at the pointer tokens ts.
func replaceAt(doc any, ts []string, arr []any) (any, error) {
	if len(ts) == 0 {
		return arr, nil
	}
	return set(doc, pointer(ts), arr, false)
}

func pointer(ts []string) string {
	var b strings.Builder
	for _, t := range ts {
		b.WriteString("/" + schema.EscapeToken(t))
	}
	return b.String()
}

// index parses an array index token: within the array, or up to its length (and "-") when inserting.
func index(t string, n int, insert bool) (int, error) {
	if insert && t == "-" {
		return n, nil
	}
	i, err := strconv.Atoi(t)
	if err != nil || i < 0 || (t != "0" && strings.HasPrefix(t, "0")) {
		return 0, fmt.Errorf("%q is not an array index", t)
	}
	if i > n || (!insert && i == n) {
		return 0, fmt.Errorf("index %d is out of range", i)
	}
	return i, nil
}

// clone deep-copies a decoded JSON value, so patches never write into the caller's document.
func clone(v any) any {
	switch c := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(c))
		for k, e := range c {
			m[k] = clone(e)
		}
		return m
	case []any:
		s := make([]any, len(c))
		for i, e := range c {
			s[i] = clone(e)
		}
		return s
	}
	return v
}

// normal re-decodes a value through JSON, so numbers compare alike whatever decoded them.
func normal(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if json.Unmarshal(b, &out) != nil {
		return v
	}
	return out
}
