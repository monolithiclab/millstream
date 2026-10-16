package expr

import (
	"reflect"
	"strings"
	"testing"
)

func TestTemplateRefs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    Refs
		wantErr string
	}{
		{"plain string", "Lyon", Refs{}, ""},
		{"one var", "${vars.city}", Refs{HasExpr: true, Vars: []string{"city"}}, ""},
		{"index form", `${vars["max"] + 1}`, Refs{HasExpr: true, Vars: []string{"max"}}, ""},
		{
			"secrets and vars spliced, deduplicated",
			"${secrets.token}:${vars.b}${vars.a}${vars.b}",
			Refs{HasExpr: true, Vars: []string{"a", "b"}, Secrets: []string{"token"}}, "",
		},
		{"whole vars map", "${vars}", Refs{HasExpr: true, AllVars: true}, ""},
		{"computed secret key", "${secrets[vars.which]}", Refs{HasExpr: true, Vars: []string{"which"}, AllSecrets: true}, ""},
		{"run only", "${run.id}", Refs{HasExpr: true}, ""},
		{"nested field", "${vars.filters.city}", Refs{HasExpr: true, Vars: []string{"filters"}}, ""},
		{"compile error", "${vars.city +}", Refs{}, "compile"},
		{"item is not available", "${item.x}", Refs{}, "undeclared reference"},
		{"unterminated", "${vars.city", Refs{}, "unterminated"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TemplateRefs(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestCompileKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		src     string
		item    any
		want    string
		wantErr string
	}{
		{"string concat", "item.addr + ':' + string(item.m2)", map[string]any{"addr": "1 rue", "m2": int64(40)}, "1 rue:40", ""},
		{"integer key", "item.m2", map[string]any{"m2": int64(40)}, "40", ""},
		{"list key as JSON", "[item.a, envelope.step]", map[string]any{"a": "x"}, `["x","dedupe"]`, ""},
		{"secrets refused", "secrets.token", nil, "", "secrets are only available"},
		{"compile error", "item.", nil, "", "compile"},
		{"runtime error", "item.missing", map[string]any{}, "", "evaluate"},
		{"unsupported result", "type(item)", map[string]any{}, "", "unsupported result type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k, err := CompileKey(tt.src)
			if err == nil {
				if k.String() != tt.src {
					t.Fatalf("String() = %q", k.String())
				}
				var got string
				got, err = k.Eval(tt.item, Envelope{Step: "dedupe"}, nil, Run{})
				if err == nil && got != tt.want {
					t.Fatalf("got %q, want %q", got, tt.want)
				}
			}
			if tt.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
