package schema

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Keywords whose value is a schema, a list of schemas, or a map of names to schemas: the places a walk descends.
var (
	schemaKeywords = []string{"items", "additionalProperties", "not", "if", "then", "else", "contains",
		"propertyNames", "unevaluatedItems", "unevaluatedProperties", "additionalItems", "contentSchema"}
	schemaListKeywords = []string{"allOf", "anyOf", "oneOf", "prefixItems", "items"}
	schemaMapKeywords  = []string{"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
)

// walk calls fn for every schema object in the document, depth first, with its JSON pointer.
func walk(node any, ptr string, fn func(obj map[string]any, ptr string) error) error {
	obj, ok := node.(map[string]any)
	if !ok {
		return nil
	}
	if err := fn(obj, ptr); err != nil {
		return err
	}
	for _, kw := range schemaKeywords {
		if sub, ok := obj[kw].(map[string]any); ok {
			if err := walk(sub, ptr+"/"+EscapeToken(kw), fn); err != nil {
				return err
			}
		}
	}
	for _, kw := range schemaListKeywords {
		list, _ := obj[kw].([]any)
		for i, sub := range list {
			if err := walk(sub, ptr+"/"+EscapeToken(kw)+"/"+strconv.Itoa(i), fn); err != nil {
				return err
			}
		}
	}
	for _, kw := range schemaMapKeywords {
		m, _ := obj[kw].(map[string]any)
		for name, sub := range m {
			if err := walk(sub, ptr+"/"+EscapeToken(kw)+"/"+EscapeToken(name), fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// EscapeToken escapes one JSON pointer reference token: ~ as ~0, / as ~1.
func EscapeToken(token string) string { return pointerEscaper.Replace(token) }

// UnescapeToken reverses EscapeToken.
func UnescapeToken(token string) string { return pointerUnescaper.Replace(token) }

var (
	pointerEscaper   = strings.NewReplacer("~", "~0", "/", "~1")
	pointerUnescaper = strings.NewReplacer("~1", "/", "~0", "~")
)

// pointerOrRoot names the empty pointer as the document root in messages.
func pointerOrRoot(ptr string) string {
	if ptr == "" {
		return "the root"
	}
	return ptr
}

// Resolve follows a same-document $ref ("#" or "#/...") from root to its target.
func Resolve(root any, ref string) (any, error) {
	target, _, err := resolve(root, ref)
	return target, err
}

// resolve follows a same-document $ref ("#" or "#/...") from root, returning the target and its JSON pointer.
func resolve(root any, ref string) (any, string, error) {
	if !isLocalRef(ref) {
		return nil, "", fmt.Errorf("$ref %q is not a same-document reference", ref)
	}
	frag, err := url.PathUnescape(ref[1:])
	if err != nil {
		return nil, "", fmt.Errorf("$ref %q: %w", ref, err)
	}
	node := root
	if frag == "" {
		return node, "", nil
	}
	for _, tok := range strings.Split(frag[1:], "/") {
		tok = UnescapeToken(tok)
		switch t := node.(type) {
		case map[string]any:
			next, ok := t[tok]
			if !ok {
				return nil, "", fmt.Errorf("$ref %q does not resolve", ref)
			}
			node = next
		case []any:
			i, err := strconv.Atoi(tok)
			if err != nil || i < 0 || i >= len(t) {
				return nil, "", fmt.Errorf("$ref %q does not resolve", ref)
			}
			node = t[i]
		default:
			return nil, "", fmt.Errorf("$ref %q does not resolve", ref)
		}
	}
	return node, frag, nil
}
