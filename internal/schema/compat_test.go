package schema

import (
	"fmt"
	"strings"
	"testing"
)

// want is an expected finding: severity, exact pointer, and a substring of the message.
type want struct {
	severity, pointer, message string
}

func obj(props, required string) string {
	return fmt.Sprintf(`{"type": "object", "properties": {%s}, "required": [%s]}`, props, required)
}

const treeNode = `"$defs": {"node": {"type": "object", "properties": {
  "value": {"type": "%s"},
  "children": {"type": "array", "items": {"$ref": "#/$defs/node"}}
}, "required": ["value"]}}`

func tree(valueType string) string {
	return `{"$ref": "#/$defs/node", ` + fmt.Sprintf(treeNode, valueType) + `}`
}

func TestCompatible(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		producer, consumer string
		want               []want
	}{
		{
			name:     "identical",
			producer: obj(`"url": {"type": "string"}`, `"url"`),
			consumer: obj(`"url": {"type": "string"}`, `"url"`),
		},
		{
			name:     "extra producer properties allowed",
			producer: obj(`"url": {"type": "string"}, "extra": {"type": "boolean"}`, `"url", "extra"`),
			consumer: obj(`"url": {"type": "string"}`, `"url"`),
		},
		{
			name:     "optional producer property against required consumer",
			producer: obj(`"price": {"type": "integer"}`, ``),
			consumer: obj(`"price": {"type": "integer"}`, `"price"`),
			want:     []want{{SeverityError, "/properties/price", `requires property "price", which the producer declares but does not require`}},
		},
		{
			name:     "undeclared producer property",
			producer: obj(`"url": {"type": "string"}`, `"url"`),
			consumer: obj(`"price": {"type": "integer"}`, `"price"`),
			want:     []want{{SeverityError, "/properties/price", `requires property "price", which the producer does not declare`}},
		},
		{
			name:     "required without a schema is unprovable",
			producer: `{"type": "object", "required": ["price"]}`,
			consumer: obj(`"price": {"type": "integer"}`, `"price"`),
			want:     []want{{SeverityWarning, "/properties/price", "cannot prove the type"}},
		},
		{
			name:     "optional consumer property still type-checked",
			producer: obj(`"note": {"type": "integer"}`, ``),
			consumer: obj(`"note": {"type": "string"}`, ``),
			want:     []want{{SeverityError, "/properties/note", "the producer may emit integer, the consumer accepts string"}},
		},
		{name: "integer is a number", producer: `{"type": "integer"}`, consumer: `{"type": "number"}`},
		{
			name:     "number is not an integer",
			producer: `{"type": "number"}`, consumer: `{"type": "integer"}`,
			want: []want{{SeverityError, "", "the producer may emit number, the consumer accepts integer"}},
		},
		{
			name:     "type mismatch does not recurse",
			producer: `{"type": "string"}`, consumer: obj(`"a": {"type": "string"}`, `"a"`),
			want: []want{{SeverityError, "", "the producer may emit string, the consumer accepts object"}},
		},
		{
			name:     "null where the consumer does not allow it",
			producer: `{"type": ["string", "null"]}`, consumer: `{"type": "string"}`,
			want: []want{{SeverityError, "", "the producer may emit null, which the consumer does not allow"}},
		},
		{name: "null where the consumer allows it", producer: `{"type": ["null", "string"]}`, consumer: `{"type": ["string", "null"]}`},
		{name: "non-null producer into nullable consumer", producer: `{"type": "string"}`, consumer: `{"type": ["string", "null"]}`},
		{
			name:     "unconstrained producer type",
			producer: `{}`, consumer: `{"type": "string"}`,
			want: []want{{SeverityWarning, "", "cannot prove the type: the producer does not constrain it, the consumer accepts string"}},
		},
		{name: "unconstrained consumer", producer: `{"type": "string"}`, consumer: `true`},
		{name: "enum subset", producer: `{"enum": ["a", "b"]}`, consumer: `{"type": "string", "enum": ["a", "b", "c"]}`},
		{
			name:     "enum not a subset",
			producer: `{"type": "string", "enum": ["a", "z"]}`, consumer: `{"enum": ["a", "b"]}`,
			want: []want{{SeverityError, "", `the producer may emit ["z"], not in the consumer's enum ["a", "b"]`}},
		},
		{name: "const in enum", producer: `{"const": "a"}`, consumer: `{"enum": ["a", "b"]}`},
		{
			name:     "const against const",
			producer: `{"const": "a"}`, consumer: `{"const": "b"}`,
			want: []want{{SeverityError, "", `the producer may emit ["a"], not in the consumer's enum ["b"]`}},
		},
		{
			name:     "structured enum equality",
			producer: `{"enum": [{"a": 1, "b": [true, null]}]}`,
			consumer: `{"enum": [{"b": [true, null], "a": 1.0}, "x"]}`,
		},
		{
			name:     "enum value types",
			producer: `{"enum": [true, null, [1], {"a": 1}, 1.5, "s"]}`, consumer: `{"type": "string"}`,
			want: []want{
				{SeverityError, "", "the producer may emit array"},
				{SeverityError, "", "the producer may emit boolean"},
				{SeverityError, "", "the producer may emit null"},
				{SeverityError, "", "the producer may emit number"},
				{SeverityError, "", "the producer may emit object"},
			},
		},
		{name: "numeric enum equality", producer: `{"enum": [1, 2.0]}`, consumer: `{"type": "number", "enum": [1.0, 2, 3]}`},
		{
			name:     "enum types against consumer type",
			producer: `{"enum": [1, "x"]}`, consumer: `{"type": "integer"}`,
			want: []want{{SeverityError, "", "the producer may emit string, the consumer accepts integer"}},
		},
		{
			name:     "unconstrained producer against enum",
			producer: `{"type": "string"}`, consumer: `{"enum": ["a", "b"]}`,
			want: []want{{SeverityWarning, "", `cannot prove the value is one of the consumer's enum ["a", "b"]`}},
		},
		{
			name:     "allOf merged on both sides",
			producer: `{"allOf": [` + obj(`"a": {"type": "string"}`, `"a"`) + `, {"properties": {"b": {"type": "integer"}}, "required": ["b"]}]}`,
			consumer: `{"type": "object", "allOf": [{"required": ["a"]}, {"required": ["b"], "properties": {"b": {"type": "number"}}}]}`,
		},
		{
			name:     "allOf merged, missing requirement",
			producer: `{"allOf": [` + obj(`"a": {"type": "string"}`, `"a"`) + `, {"properties": {"b": {"type": "integer"}}}]}`,
			consumer: `{"allOf": [{"type": "object", "required": ["a", "b"]}]}`,
			want:     []want{{SeverityError, "/properties/b", `which the producer declares but does not require`}},
		},
		{
			name:     "allOf intersects types and enums",
			producer: `{"allOf": [{"type": "number"}, {"type": "integer"}, {"enum": [1, 2, 3]}, {"enum": [2, 3, 4]}]}`,
			consumer: `{"type": "integer", "enum": [2, 3]}`,
		},
		{
			name:     "allOf property schemas merged",
			producer: `{"type": "object", "required": ["n"], "allOf": [{"properties": {"n": {"type": "number"}}}, {"properties": {"n": {"type": "integer"}}}]}`,
			consumer: obj(`"n": {"type": "integer"}`, `"n"`),
		},
		{name: "local $ref recursion, compatible", producer: tree("integer"), consumer: tree("number")},
		{
			name:     "local $ref recursion, incompatible",
			producer: tree("string"), consumer: tree("integer"),
			want: []want{{SeverityError, "/$defs/node/properties/value", "the producer may emit string, the consumer accepts integer"}},
		},
		{
			name:     "$ref cycle without structure terminates",
			producer: `{"$ref": "#/$defs/a", "$defs": {"a": {"$ref": "#/$defs/b"}, "b": {"$ref": "#/$defs/a", "type": "string"}}}`,
			consumer: `{"type": "string"}`,
		},
		{
			name:     "nested objects",
			producer: obj(`"address": `+obj(`"city": {"type": "string"}, "zip": {"type": "string"}`, `"zip"`), `"address"`),
			consumer: obj(`"address": `+obj(`"city": {"type": "string"}, "zip": {"type": "integer"}`, `"city", "zip"`), `"address"`),
			want: []want{
				{SeverityError, "/properties/address/properties/city", `requires property "city", which the producer declares but does not require`},
				{SeverityError, "/properties/address/properties/zip", "the producer may emit string, the consumer accepts integer"},
			},
		},
		{name: "arrays compatible", producer: `{"type": "array", "items": {"type": "integer"}}`, consumer: `{"type": "array", "items": {"type": "number"}}`},
		{
			name:     "array items incompatible",
			producer: obj(`"tags": {"type": "array", "items": {"type": ["string", "null"]}}`, `"tags"`),
			consumer: obj(`"tags": {"type": "array", "items": {"type": "string"}}`, `"tags"`),
			want:     []want{{SeverityError, "/properties/tags/items", "the producer may emit null"}},
		},
		{
			name:     "array of objects",
			producer: `{"type": "array", "items": ` + obj(`"id": {"type": "string"}`, ``) + `}`,
			consumer: `{"type": "array", "items": ` + obj(`"id": {"type": "string"}`, `"id"`) + `}`,
			want:     []want{{SeverityError, "/items/properties/id", "declares but does not require"}},
		},
		{
			name:     "producer items unconstrained",
			producer: `{"type": "array"}`, consumer: `{"type": "array", "items": {"type": "string"}}`,
			want: []want{{SeverityWarning, "/items", "cannot prove the type"}},
		},
		{name: "producer false emits nothing", producer: `false`, consumer: `{"type": "string"}`},
		{
			name:     "consumer false accepts nothing",
			producer: `{"type": "string"}`, consumer: obj(`"a": false`, ``),
			want: []want{{SeverityError, "", "the producer may emit string, the consumer accepts object"}},
		},
		{
			name:     "consumer false property",
			producer: obj(`"a": {"type": "string"}`, ``), consumer: obj(`"a": false`, ``),
			want: []want{{SeverityError, "/properties/a", "the consumer accepts no value here"}},
		},
		{
			name:     "several non-null types are outside the subset",
			producer: `{"type": ["string", "integer"]}`, consumer: `{"type": "string"}`,
			want: []want{{SeverityWarning, "/type", "producer schema: type is outside the checked subset"}},
		},
		{
			name:     "tuple items are outside the subset",
			producer: `{"type": "array", "items": [{"type": "string"}]}`, consumer: `{"type": "array", "items": {"type": "string"}}`,
			want: []want{{SeverityWarning, "/items", "producer schema: items is outside the checked subset"}},
		},
		{
			name:     "format and bounds are runtime only",
			producer: `{"type": "string"}`, consumer: `{"type": "string", "format": "uri", "pattern": "^h", "maxLength": 3, "title": "t"}`,
		},
		{
			name:     "unresolvable $ref",
			producer: `{"$ref": "#/$defs/missing"}`, consumer: `{"type": "string"}`,
			want: []want{{SeverityError, "", `producer schema: /$ref: $ref "#/$defs/missing" does not resolve`}},
		},
		{
			name:     "$ref into an array and through escapes",
			producer: `{"$ref": "#/$defs/a~1b/allOf/0", "$defs": {"a/b": {"allOf": [{"type": "integer"}]}}}`,
			consumer: `{"type": "number"}`,
		},
		{
			name:     "$ref with a bad index",
			producer: `{"$ref": "#/$defs/a/allOf/9", "$defs": {"a": {"allOf": [true]}}}`, consumer: `true`,
			want: []want{{SeverityError, "", "does not resolve"}},
		},
		{
			name:     "$ref through a scalar",
			producer: `{"$ref": "#/$defs/a/type/x", "$defs": {"a": {"type": "string"}}}`, consumer: `true`,
			want: []want{{SeverityError, "", "does not resolve"}},
		},
		{
			name:     "$ref bad escape",
			producer: `{"$ref": "#/%zz"}`, consumer: `true`,
			want: []want{{SeverityError, "", "invalid URL escape"}},
		},
		{
			name:     "invalid keyword values",
			producer: `{"enum": "a", "type": "text"}`, consumer: `{"type": "string"}`,
			want: []want{
				{SeverityError, "", "producer schema: /enum: enum must be an array"},
				{SeverityError, "", `producer schema: /type: unknown type "text"`},
			},
		},
		{
			name:     "a schema that is not an object",
			producer: `{"properties": {"a": 3}, "required": ["a"], "type": "object"}`, consumer: obj(`"a": {"type": "string"}`, `"a"`),
			want: []want{{SeverityError, "/properties/a", "producer schema: /properties/a: a schema must be an object or a boolean"}},
		},
		{
			name:     "remote $ref refused in the producer",
			producer: `{"$ref": "https://example.com/s.json"}`, consumer: `true`,
			want: []want{{SeverityError, "", `producer schema: $ref "https://example.com/s.json" at the root is refused`}},
		},
		{
			name:     "remote $ref refused in the consumer",
			producer: `true`, consumer: `{"properties": {"a": {"$ref": "s.json#/a"}}}`,
			want: []want{{SeverityError, "", `consumer schema: $ref "s.json#/a" at /properties/a is refused`}},
		},
		{
			name:     "unreadable producer",
			producer: `{`, consumer: `true`,
			want: []want{{SeverityError, "", "producer schema:"}},
		},
		{
			name:     "unreadable consumer",
			producer: `true`, consumer: `[`,
			want: []want{{SeverityError, "", "consumer schema:"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertFindings(t, Compatible([]byte(tt.producer), []byte(tt.consumer)), tt.want)
		})
	}
}

// Every construct outside the subset is reported as not statically checked, with its pointer, on either side,
// and stops the check of that subtree only.
func TestCompatibleOutsideSubset(t *testing.T) {
	t.Parallel()
	keywords := map[string]string{
		"anyOf":             `"anyOf": [{"type": "string"}, {"type": "integer"}]`,
		"oneOf":             `"oneOf": [{"type": "string"}]`,
		"not":               `"not": {"type": "null"}`,
		"if":                `"if": {"minLength": 1}`,
		"then":              `"then": {"maxLength": 3}`,
		"else":              `"else": {"maxLength": 9}`,
		"patternProperties": `"patternProperties": {"^x": {"type": "string"}}`,
		"dependentSchemas":  `"dependentSchemas": {"a": {"required": ["b"]}}`,
	}
	for kw, fragment := range keywords {
		for _, side := range []string{"producer", "consumer"} {
			t.Run(side+" "+kw, func(t *testing.T) {
				t.Parallel()
				weird := `{"type": "object", "properties": {"x": {` + fragment + `}, "y": {"type": "string"}}, "required": ["x", "y"]}`
				plain := `{"type": "object", "properties": {"x": {"type": "string"}, "y": {"type": "integer"}}, "required": ["x", "y"]}`
				producer, consumer := weird, plain
				if side == "consumer" {
					producer, consumer = plain, weird
				}
				// y still fails on one side or the other: the rest of the schema is checked.
				yMsg := "the producer may emit string, the consumer accepts integer"
				if side == "consumer" {
					yMsg = "the producer may emit integer, the consumer accepts string"
				}
				assertFindings(t, Compatible([]byte(producer), []byte(consumer)), []want{
					{SeverityWarning, "/properties/x/" + kw, side + " schema: " + kw + " is outside the checked subset; this value is not statically checked"},
					{SeverityError, "/properties/y", yMsg},
				})
			})
		}
	}
}

func assertFindings(t *testing.T, got []Finding, wants []want) {
	t.Helper()
	if len(got) != len(wants) {
		t.Fatalf("got %d findings, want %d:\n%s", len(got), len(wants), formatFindings(got))
	}
	for i, w := range wants {
		f := got[i]
		if f.Severity != w.severity || f.Pointer != w.pointer || !strings.Contains(f.Message, w.message) {
			t.Errorf("finding %d = %+v, want %s at %q containing %q", i, f, w.severity, w.pointer, w.message)
		}
	}
}

func formatFindings(fs []Finding) string {
	var b strings.Builder
	for _, f := range fs {
		fmt.Fprintf(&b, "  %s %q %s\n", f.Severity, f.Pointer, f.Message)
	}
	return b.String()
}
