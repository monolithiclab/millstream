package conformance

import (
	"encoding/json"
	"testing"
)

func TestMinimalInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		schema, want string
	}{
		{``, `{}`},
		{`[1]`, `{}`},
		{`{}`, `{}`},
		{`{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer","minimum":3},` +
			`"c":{"type":["boolean","null"]},"d":{"type":"array"},"e":{"enum":["x","y"]},"f":{"const":7},` +
			`"g":{"type":"null"},"h":{"type":"number"},"i":{"type":"object"},"j":{"type":"string"}},` +
			`"required":["a","b","c","d","e","f","g","h","i"]}`,
			`{"a":"","b":3,"c":false,"d":[],"e":"x","f":7,"g":null,"h":0,"i":{}}`},
		{`{"type":"string"}`, `""`},
		{`{"$defs":{"n":{"type":"object","properties":{"n":{"$ref":"#/$defs/n"}},"required":["n"]}},` +
			`"type":"object","properties":{"n":{"type":"object","required":["n"]}},"required":["n"]}`,
			`{"n":{"n":{}}}`},
	}
	for _, tt := range tests {
		got := MinimalInput(json.RawMessage(tt.schema))
		if string(got) != tt.want {
			t.Errorf("MinimalInput(%s) = %s, want %s", tt.schema, got, tt.want)
		}
	}
}
