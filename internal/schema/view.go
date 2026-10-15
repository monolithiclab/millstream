package schema

import (
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"strings"
)

// Keywords outside the checked subset (spec section 4.11): a schema using one is not statically checked below
// that point. The spec names anyOf, oneOf, not, if/then/else, patternProperties and dependentSchemas; the other
// applicators and conditional keywords here change which values are valid just as much, so they are treated alike.
var outsideKeywords = map[string]bool{
	"anyOf": true, "oneOf": true, "not": true, "if": true, "then": true, "else": true,
	"patternProperties": true, "dependentSchemas": true,
	"prefixItems": true, "contains": true, "unevaluatedItems": true, "unevaluatedProperties": true,
	"propertyNames": true, "dependentRequired": true, "additionalItems": true, "dependencies": true,
	"$dynamicRef": true, "$recursiveRef": true,
}

// jsonTypes are the JSON Schema type names.
var jsonTypes = map[string]bool{
	"string": true, "number": true, "integer": true, "boolean": true, "object": true, "array": true, "null": true,
}

// ref is a schema node and its JSON pointer in its document.
type ref struct {
	node any
	ptr  string
}

// view is the checked subset of one position in a schema, with allOf members and $ref targets merged in.
// Every other keyword (format, pattern, numeric and length bounds, additionalProperties, annotations) is enforced
// at runtime only and is ignored here.
type view struct {
	ptrs     []string        // locations merged into the view, the position's own first
	types    map[string]bool // nil: type unconstrained
	enum     []any           // const folds into a one-value enum
	hasEnum  bool
	props    map[string][]ref
	required map[string]bool
	items    []ref // nil: no items keyword
	never    bool  // a false schema somewhere in the merge: no value is valid
	outside  []string
	broken   []string // problems that make the schema itself invalid, such as a $ref that does not resolve
}

// newView builds the view of the schemas at refs, all describing the same position; refs is never empty.
func newView(root any, refs ...ref) *view {
	v := &view{props: map[string][]ref{}, required: map[string]bool{}}
	seen := map[string]bool{}
	for _, r := range refs {
		v.merge(root, r, seen)
	}
	return v
}

// loc is the view's own location, used to place findings.
func (v *view) loc() string { return v.ptrs[0] }

// key identifies the view for the recursion guard.
func (v *view) key() string { return strings.Join(v.ptrs, "\x00") }

func (v *view) merge(root any, r ref, seen map[string]bool) {
	if seen[r.ptr] {
		return // a $ref or allOf cycle: this node is already merged
	}
	seen[r.ptr] = true
	v.ptrs = append(v.ptrs, r.ptr)
	switch n := r.node.(type) {
	case bool:
		v.never = v.never || !n
		return
	case map[string]any:
		for _, kw := range slices.Sorted(maps.Keys(n)) {
			v.keyword(root, r.ptr, kw, n[kw], seen)
		}
	default:
		v.broken = append(v.broken, fmt.Sprintf("%s: a schema must be an object or a boolean", pointerOrRoot(r.ptr)))
	}
}

// keyword merges one keyword of the schema object at ptr.
func (v *view) keyword(root any, ptr, kw string, val any, seen map[string]bool) {
	at := ptr + "/" + escape(kw)
	switch {
	case outsideKeywords[kw]:
		v.outside = append(v.outside, at)
	case kw == "type":
		v.mergeType(at, val)
	case kw == "enum":
		list, ok := val.([]any)
		if !ok {
			v.broken = append(v.broken, at+": enum must be an array")
			return
		}
		v.mergeEnum(list)
	case kw == "const":
		v.mergeEnum([]any{val})
	case kw == "properties":
		props, _ := val.(map[string]any)
		for name, sub := range props {
			v.props[name] = append(v.props[name], ref{sub, at + "/" + escape(name)})
		}
	case kw == "required":
		list, _ := val.([]any)
		for _, name := range list {
			if s, ok := name.(string); ok {
				v.required[s] = true
			}
		}
	case kw == "items":
		if _, tuple := val.([]any); tuple {
			v.outside = append(v.outside, at)
			return
		}
		v.items = append(v.items, ref{val, at})
	case kw == "allOf":
		list, _ := val.([]any)
		for i, sub := range list {
			v.merge(root, ref{sub, at + "/" + strconv.Itoa(i)}, seen)
		}
	case kw == "$ref":
		s, _ := val.(string)
		target, tptr, err := resolve(root, s)
		if err != nil {
			v.broken = append(v.broken, fmt.Sprintf("%s: %v", at, err))
			return
		}
		v.merge(root, ref{target, tptr}, seen)
	}
}

// mergeType intersects the view's types with a type keyword in the subset: one type, or one type and null.
func (v *view) mergeType(at string, val any) {
	var names []string
	switch t := val.(type) {
	case string:
		names = []string{t}
	case []any:
		for _, e := range t {
			s, _ := e.(string)
			names = append(names, s)
		}
	}
	nonNull := 0
	for _, n := range names {
		if !jsonTypes[n] {
			v.broken = append(v.broken, fmt.Sprintf("%s: unknown type %q", at, n))
			return
		}
		if n != "null" {
			nonNull++
		}
	}
	if len(names) == 0 || nonNull > 1 {
		v.outside = append(v.outside, at) // several non-null types: outside the subset
		return
	}
	set := map[string]bool{}
	for _, n := range names {
		set[n] = true
	}
	if v.types == nil {
		v.types = set
		return
	}
	merged := map[string]bool{}
	for t := range v.types {
		switch {
		case set[t]:
			merged[t] = true
		case (t == "number" && set["integer"]) || (t == "integer" && set["number"]):
			merged["integer"] = true
		}
	}
	v.types = merged
}

// mergeEnum intersects the view's allowed values with list.
func (v *view) mergeEnum(list []any) {
	if !v.hasEnum {
		v.enum, v.hasEnum = list, true
		return
	}
	var kept []any
	for _, e := range v.enum {
		if containsValue(list, e) {
			kept = append(kept, e)
		}
	}
	v.enum = kept
}

// effectiveTypes is the set of types the view admits: its type keyword, or else the types of its enum values;
// nil when neither constrains it.
func (v *view) effectiveTypes() map[string]bool {
	if v.types != nil || !v.hasEnum {
		return v.types
	}
	set := map[string]bool{}
	for _, e := range v.enum {
		set[valueType(e)] = true
	}
	return set
}

// valueType is the JSON Schema type of a decoded value; integral numbers are integers.
func valueType(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case json.Number:
		if r, ok := new(big.Rat).SetString(string(t)); ok && r.IsInt() {
			return "integer"
		}
		return "number"
	case []any:
		return "array"
	default:
		return "object"
	}
}

// containsValue reports whether list holds a value equal to v under JSON equality (1 equals 1.0).
func containsValue(list []any, v any) bool {
	k := canonical(v)
	for _, e := range list {
		if canonical(e) == k {
			return true
		}
	}
	return false
}

// canonical renders a decoded JSON value so that JSON-equal values render equally.
func canonical(v any) string {
	switch t := v.(type) {
	case json.Number:
		if r, ok := new(big.Rat).SetString(string(t)); ok {
			return "n:" + r.RatString()
		}
		return "n:" + string(t)
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = canonical(e)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case map[string]any:
		keys := slices.Sorted(maps.Keys(t))
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = strconv.Quote(k) + ":" + canonical(t[k])
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		b, _ := json.Marshal(t) // nil, bool and string always marshal
		return string(b)
	}
}
