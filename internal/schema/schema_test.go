package schema

import (
	"encoding/json"
	"strings"
	"testing"
)

const listingSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "url": {"type": "string", "format": "uri"},
    "price": {"type": "integer", "minimum": 0},
    "city": {"$ref": "#/$defs/city"}
  },
  "required": ["url", "price"],
  "$defs": {"city": {"enum": ["Lyon", "Paris"]}}
}`

func TestCompileAndValidate(t *testing.T) {
	t.Parallel()
	s, err := Compile([]byte(listingSchema))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	tests := []struct {
		name    string
		doc     string
		wantErr string
	}{
		{name: "valid", doc: `{"url": "https://example.com/1", "price": 350000, "city": "Lyon"}`},
		{name: "missing required", doc: `{"url": "https://example.com/1"}`, wantErr: "price"},
		{name: "wrong type", doc: `{"url": "https://example.com/1", "price": "cheap"}`, wantErr: "price"},
		{name: "bound enforced", doc: `{"url": "https://example.com/1", "price": -1}`, wantErr: "minimum"},
		{name: "format asserted", doc: `{"url": "not a uri", "price": 1}`, wantErr: "uri"},
		{name: "local ref followed", doc: `{"url": "https://example.com/1", "price": 1, "city": "Nantes"}`, wantErr: "city"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var v any
			if err := json.Unmarshal([]byte(tt.doc), &v); err != nil {
				t.Fatal(err)
			}
			err := s.Validate(v)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestCompileRefusesNonLocalRefs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, doc, want string
	}{
		{"remote", `{"$ref": "https://example.com/s.json"}`, `$ref "https://example.com/s.json" at the root is refused`},
		{"relative file", `{"properties": {"a": {"$ref": "other.json#/x"}}}`, `$ref "other.json#/x" at /properties/a is refused`},
		{"anchor", `{"$ref": "#city", "$defs": {"c": {"$anchor": "city"}}}`, `$ref "#city" at the root is refused`},
		{"in allOf", `{"allOf": [{"$ref": "x.json"}]}`, "at /allOf/0 is refused"},
		{"in items", `{"items": {"$ref": "x.json"}}`, "at /items is refused"},
		{"in $defs with escaping", `{"$defs": {"a/b": {"$ref": "x.json"}}}`, "at /$defs/a~1b is refused"},
		{"dynamic ref", `{"$dynamicRef": "https://example.com/meta"}`, "$dynamicRef"},
		{"unknown metaschema", `{"$schema": "https://example.com/meta"}`, "loading https://example.com/meta is refused"},
		{"not JSON", `{`, "schema is not valid JSON"},
		{"invalid schema", `{"type": 3}`, "compile schema"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Compile([]byte(tt.doc))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Compile error = %v, want it to contain %q", err, tt.want)
			}
		})
	}
	// A property named $ref is a name, not a reference.
	if _, err := Compile([]byte(`{"properties": {"$ref": {"type": "string"}}, "$defs": {"x": {"$ref": "#"}}}`)); err != nil {
		t.Errorf("Compile refused a property named $ref: %v", err)
	}
}
