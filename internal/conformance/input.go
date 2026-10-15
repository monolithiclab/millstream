package conformance

import "encoding/json"

// MinimalInput returns a small value valid against common input schemas, for an action without a fixture input:
// an object of its required properties, each minimal in turn; a string's first enum value or ""; a number's minimum
// or 0; false; an empty array; null. A missing or unreadable schema gives {}.
func MinimalInput(schema json.RawMessage) json.RawMessage {
	var s map[string]any
	if json.Unmarshal(schema, &s) != nil {
		return json.RawMessage(`{}`)
	}
	b, err := json.Marshal(minimal(s, 0))
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return b
}

// maxDepth bounds the nesting minimal follows, against recursive schemas.
const maxDepth = 8

func minimal(s map[string]any, depth int) any {
	if c, ok := s["const"]; ok {
		return c
	}
	if e, ok := s["enum"].([]any); ok && len(e) > 0 {
		return e[0]
	}
	typ, _ := s["type"].(string)
	if types, ok := s["type"].([]any); ok && len(types) > 0 {
		typ, _ = types[0].(string)
	}
	switch typ {
	case "string":
		return ""
	case "integer", "number":
		if m, ok := s["minimum"].(float64); ok {
			return m
		}
		return 0
	case "boolean":
		return false
	case "array":
		return []any{}
	case "null":
		return nil
	}
	obj := map[string]any{}
	if depth >= maxDepth {
		return obj
	}
	props, _ := s["properties"].(map[string]any)
	required, _ := s["required"].([]any)
	for _, r := range required {
		name, _ := r.(string)
		p, _ := props[name].(map[string]any)
		obj[name] = minimal(p, depth+1)
	}
	return obj
}
