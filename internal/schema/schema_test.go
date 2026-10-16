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

// A document compiles once: the same schema serves every caller.
func TestCompileCache(t *testing.T) {
	t.Parallel()
	doc := []byte(`{"type": "string", "title": "cached"}`)
	a, errA := Compile(doc)
	b, errB := Compile(doc)
	if errA != nil || errB != nil || a != b {
		t.Fatalf("schemas %p %p, errors %v %v: want one", a, b, errA, errB)
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

func TestViolations(t *testing.T) {
	t.Parallel()
	s, err := Compile([]byte(`{
	  "type": "object",
	  "properties": {
	    "filters": {"type": "object", "properties": {"max": {"type": "integer"}, "a/b": {"type": "string"}}},
	    "tags": {"type": "array", "items": {"type": "string"}}
	  },
	  "required": ["filters"],
	  "additionalProperties": false
	}`))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		v    any
		want []string // pointer|message fragment
	}{
		{"valid with Go numbers", map[string]any{"filters": map[string]any{"max": int64(3)}, "tags": []any{"x"}}, nil},
		{"missing required at root", map[string]any{}, []string{"|filters"}},
		{"not a JSON value", make(chan int), []string{"|invalid jsonType"}},
		{"nested leaves", map[string]any{
			"filters": map[string]any{"max": "x", "a/b": 1.5},
			"tags":    []any{"ok", true},
			"extra":   1,
		}, []string{"|extra", "/filters/a~1b|string", "/filters/max|integer", "/tags/1|string"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s.Violations(tt.v)
			if len(got) != len(tt.want) {
				t.Fatalf("got %+v, want %v", got, tt.want)
			}
			for i, w := range tt.want {
				ptr, frag, _ := strings.Cut(w, "|")
				if got[i].Pointer != ptr || !strings.Contains(got[i].Message, frag) {
					t.Fatalf("violation %d = %+v, want %s", i, got[i], w)
				}
			}
		})
	}
}

func TestDocument(t *testing.T) {
	d, err := CompileDocument([]byte(`{"defs": {"n": {"type": "integer"}, "list": {"type": "array",
		"items": {"$ref": "#/defs/n"}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	s, err := d.Schema("/defs/list")
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := d.Schema("/defs/list"); again != s {
		t.Error("a schema compiled twice")
	}
	if s.Validate([]any{float64(1)}) != nil || s.Validate([]any{"x"}) == nil {
		t.Error("the schema does not validate as written")
	}
	if _, err := d.Schema("/defs/nope"); err == nil {
		t.Error("a missing pointer compiled")
	}
	if _, err := CompileDocument([]byte(`{"$ref": "https://example.com/s.json"}`)); err == nil {
		t.Error("a remote reference was accepted")
	}
}
