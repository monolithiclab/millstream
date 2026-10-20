package pipeline

import (
	"reflect"
	"testing"
)

// envOf returns an environment lookup over m.
func envOf(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestVarAndSecretEnvNames(t *testing.T) {
	t.Parallel()
	if got := VarEnv("minScore"); got != "MILLSTREAM_VAR_MIN_SCORE" {
		t.Errorf("VarEnv = %s", got)
	}
	if got := SecretEnv("portalToken"); got != "MILLSTREAM_SECRET_PORTAL_TOKEN" {
		t.Errorf("SecretEnv = %s", got)
	}
}

func TestResolveVars(t *testing.T) {
	t.Parallel()
	p := &Pipeline{Name: "p", Vars: map[string]any{
		"city":     "Lyon",
		"maxPrice": int64(400000),
		"ratio":    0.5,
		"enabled":  false,
		"tags":     []any{"a"},
		"limits":   map[string]any{"a": int64(1)},
		"region":   nil,
	}}
	tests := []struct {
		name        string
		flags, env  map[string]string
		wantVar     string
		want        any
		wantSource  string
		wantErr     string
		wantPartial bool
	}{
		{name: "default", env: map[string]string{"MILLSTREAM_VAR_REGION": "x"}, wantVar: "city", want: "Lyon", wantSource: SourceDefault},
		{name: "env over default", env: map[string]string{"MILLSTREAM_VAR_CITY": "Paris", "MILLSTREAM_VAR_REGION": "x"}, wantVar: "city", want: "Paris", wantSource: SourceEnv},
		{name: "flag over env", flags: map[string]string{"city": "Nantes", "region": "x"}, env: map[string]string{"MILLSTREAM_VAR_CITY": "Paris"}, wantVar: "city", want: "Nantes", wantSource: SourceFlag},
		{name: "string default is not JSON-parsed", flags: map[string]string{"city": `"quoted"`, "region": "x"}, wantVar: "city", want: `"quoted"`, wantSource: SourceFlag},
		{name: "integer", flags: map[string]string{"maxPrice": "350000", "region": "x"}, wantVar: "maxPrice", want: int64(350000), wantSource: SourceFlag},
		{name: "integer to number", flags: map[string]string{"ratio": "2", "region": "x"}, wantVar: "ratio", want: 2.0, wantSource: SourceFlag},
		{name: "number", env: map[string]string{"MILLSTREAM_VAR_RATIO": "0.75", "MILLSTREAM_VAR_REGION": "x"}, wantVar: "ratio", want: 0.75, wantSource: SourceEnv},
		{name: "boolean", flags: map[string]string{"enabled": "true", "region": "x"}, wantVar: "enabled", want: true, wantSource: SourceFlag},
		{name: "list", flags: map[string]string{"tags": `["x", 2]`, "region": "x"}, wantVar: "tags", want: []any{"x", int64(2)}, wantSource: SourceFlag},
		{name: "map", flags: map[string]string{"limits": `{"b": 2.5, "c": [1]}`, "region": "x"}, wantVar: "limits", want: map[string]any{"b": 2.5, "c": []any{int64(1)}}, wantSource: SourceFlag},
		{name: "required takes JSON", flags: map[string]string{"region": "42"}, wantVar: "region", want: int64(42), wantSource: SourceFlag},
		{name: "required takes raw text", env: map[string]string{"MILLSTREAM_VAR_REGION": "Rhône"}, wantVar: "region", want: "Rhône", wantSource: SourceEnv},
		{name: "big integer is a number", flags: map[string]string{"ratio": "1e400", "region": "x"}, wantErr: "variable ratio from flag: not valid JSON"},
		{name: "required missing", wantErr: "variable region is required: set --var region=… or MILLSTREAM_VAR_REGION", wantPartial: true},
		{name: "integer refuses float", flags: map[string]string{"maxPrice": "1.5", "region": "x"}, wantErr: "variable maxPrice from flag: want integer like the default, got number", wantPartial: true},
		{name: "integer refuses string", env: map[string]string{"MILLSTREAM_VAR_MAX_PRICE": `"cheap"`, "MILLSTREAM_VAR_REGION": "x"}, wantErr: "variable maxPrice from env: want integer like the default, got string", wantPartial: true},
		{name: "boolean refuses text", flags: map[string]string{"enabled": "yes", "region": "x"}, wantErr: "variable enabled from flag: not valid JSON", wantPartial: true},
		{name: "list refuses map", flags: map[string]string{"tags": "{}", "region": "x"}, wantErr: "want list like the default, got map", wantPartial: true},
		{name: "trailing data", flags: map[string]string{"maxPrice": "1 2", "region": "x"}, wantErr: "trailing data", wantPartial: true},
		{name: "null refused for typed", flags: map[string]string{"maxPrice": "null", "region": "x"}, wantErr: "got null", wantPartial: true},
		{name: "unknown flag", flags: map[string]string{"cty": "x", "region": "x"}, wantErr: "--var cty: pipeline p declares no such variable", wantPartial: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := ResolveVars(p, tt.flags, envOf(tt.env))
			if tt.wantErr != "" {
				assertErr(t, err, tt.wantErr)
				if tt.wantPartial && r.Vars["city"] == nil {
					t.Errorf("partial result lost resolved variables: %v", r.Vars)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveVars: %v", err)
			}
			if got := r.Vars[tt.wantVar]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("%s = %#v, want %#v", tt.wantVar, got, tt.want)
			}
			if got := r.Sources[tt.wantVar]; got != tt.wantSource {
				t.Errorf("source of %s = %s, want %s", tt.wantVar, got, tt.wantSource)
			}
			if len(r.Vars) != len(p.Vars) || len(r.Sources) != len(p.Vars) {
				t.Errorf("resolved %d vars and %d sources, want %d", len(r.Vars), len(r.Sources), len(p.Vars))
			}
		})
	}
}

func TestResolveSecrets(t *testing.T) {
	t.Parallel()
	p := &Pipeline{Name: "p", Vars: map[string]any{}, Secrets: []string{"portalToken", "apiKey"}}
	r, err := ResolveVars(p, nil, envOf(map[string]string{
		"MILLSTREAM_SECRET_PORTAL_TOKEN": "s3cret",
		"MILLSTREAM_SECRET_API_KEY":      "",
		"MILLSTREAM_VAR_PORTAL_TOKEN":    "not this one",
	}))
	if err != nil {
		t.Fatalf("ResolveVars: %v", err)
	}
	if want := map[string]string{"portalToken": "s3cret", "apiKey": ""}; !reflect.DeepEqual(r.Secrets, want) {
		t.Errorf("secrets = %v, want %v", r.Secrets, want)
	}

	// An unset secret is left out, not a problem: only a dispatch needs its value. An unmapped one is.
	r, err = ResolveVars(p, nil, envOf(map[string]string{"MILLSTREAM_SECRET_API_KEY": "k"}))
	if want := map[string]string{"apiKey": "k"}; err != nil || !reflect.DeepEqual(r.Secrets, want) {
		t.Errorf("partial secrets = %v, %v; want %v", r.Secrets, err, want)
	}
	env := &Environment{Secrets: map[string]map[string]SecretSource{"p": {"apiKey": {Env: "KEY"}}}}
	_, err = ResolveVarsWith(p, nil, envOf(nil), env.SecretLookup("p", envOf(nil)))
	assertErr(t, err, "secret portalToken is not mapped for pipeline p")
}

func TestResolveReportsEveryProblem(t *testing.T) {
	t.Parallel()
	p := &Pipeline{Name: "p", Vars: map[string]any{"a": nil, "b": nil}, Secrets: []string{"c"}}
	grant := &Environment{Secrets: map[string]map[string]SecretSource{"p": {}}}
	_, err := ResolveVarsWith(p, nil, envOf(nil), grant.SecretLookup("p", envOf(nil)))
	for _, want := range []string{"variable a is required", "variable b is required", "secret c is not mapped"} {
		assertErr(t, err, want)
	}
}

func TestSecretValues(t *testing.T) {
	t.Parallel()
	env := map[string]string{"MILLSTREAM_SECRET_API_TOKEN": "t0ken", "MILLSTREAM_SECRET_EMPTY": ""}
	got := SecretValues([]string{"apiToken", "empty", "unset", ""}, func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	})
	if len(got) != 2 || got["apiToken"] != "t0ken" || got["empty"] != "" {
		t.Fatalf("SecretValues = %v", got)
	}
}
