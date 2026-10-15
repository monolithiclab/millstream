package expr

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

var startedAt = time.Date(2026, 10, 14, 6, 0, 0, 0, time.UTC)

func testCtx() InterpCtx {
	return InterpCtx{
		Vars: map[string]any{
			"city": "Lyon", "maxPrice": int64(400000), "ratio": 0.5, "on": true,
			"tags": []any{"a", "b"}, "big": make([]any, 200),
		},
		Run:     Run{ID: "run_x_daily", Trigger: "daily", StartedAt: startedAt, Revision: "abc"},
		Secrets: map[string]string{"portalToken": "s3cret"},
	}
}

func TestInterpolate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"typed integer", "${vars.maxPrice}", int64(400000)},
		{"typed with spaces", "${ vars.maxPrice }", int64(400000)},
		{"typed double", "${vars.ratio * 2.0}", 1.0},
		{"typed bool", "${vars.on}", true},
		{"typed list", "${vars.tags}", []any{"a", "b"}},
		{"typed map literal", "${ {'k': vars.city} }", map[string]any{"k": "Lyon"}},
		{"typed null", "${null}", nil},
		{"typed uint", "${1u}", uint64(1)},
		{"typed timestamp", "${run.startedAt}", startedAt},
		{"typed bytes", "${b'hi'}", []byte("hi")},
		{"secret", "${secrets.portalToken}", "s3cret"},
		{"run fields", "${run.id}/${run.trigger}/${run.revision}", "run_x_daily/daily/abc"},
		{"splice", "under ${vars.maxPrice} in ${vars.city}", "under 400000 in Lyon"},
		{"splice double", "r=${vars.ratio}", "r=0.5"},
		{"splice bool and null", "${vars.on}/${null}", "true/null"},
		{"splice uint", "n=${2u}", "n=2"},
		{"splice list as JSON", "tags=${vars.tags}", `tags=["a","b"]`},
		{"splice timestamp", "at ${run.startedAt}", "at 2026-10-14T06:00:00Z"},
		{"splice duration", "d=${duration('90s')}", "d=1m30s"},
		{"braces in a string literal", "${'}{' + vars.city}!", "}{Lyon!"},
		{"escaped quote in a string literal", `${'it\'s ' + vars.city}`, "it's Lyon"},
		{"no expression", "plain $ text {x}", "plain $ text {x}"},
		{"non-string kept", int64(3), int64(3)},
		{
			"nested maps and lists",
			map[string]any{"filters": map[string]any{"city": "${vars.city}", "max": "${vars.maxPrice}"}, "list": []any{"${vars.on}", 1.5}},
			map[string]any{"filters": map[string]any{"city": "Lyon", "max": int64(400000)}, "list": []any{true, 1.5}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Interpolate(map[string]any{"v": tt.in}, testCtx())
			if err != nil {
				t.Fatalf("Interpolate: %v", err)
			}
			if !reflect.DeepEqual(got["v"], tt.want) {
				t.Errorf("got %#v (%T), want %#v (%T)", got["v"], got["v"], tt.want, tt.want)
			}
		})
	}
}

func TestInterpolateDoesNotMutate(t *testing.T) {
	t.Parallel()
	in := map[string]any{"a": map[string]any{"b": "${vars.city}"}, "l": []any{"${vars.city}"}}
	if _, err := Interpolate(in, testCtx()); err != nil {
		t.Fatal(err)
	}
	if in["a"].(map[string]any)["b"] != "${vars.city}" || in["l"].([]any)[0] != "${vars.city}" {
		t.Errorf("input mutated: %v", in)
	}
	got, err := Interpolate(nil, InterpCtx{})
	if err != nil || got != nil {
		t.Errorf("Interpolate(nil) = %v, %v", got, err)
	}
	got, err = Interpolate(map[string]any{"x": "${size(vars)}"}, InterpCtx{})
	if err != nil || got["x"] != int64(0) {
		t.Errorf("nil vars are an empty map: %v, %v", got, err)
	}
}

func TestInterpolateErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in, want string
	}{
		{"compile error carries expression and path", "${vars.city +}", `config filters[1].v: compile "vars.city +"`},
		{"unknown variable", "${item.x}", `compile "item.x": ERROR`},
		{"runtime error", "${vars.nope}", `config filters[1].v: evaluate "vars.nope": no such key: nope`},
		{"runtime error when splicing", "x ${vars.nope}", `evaluate "vars.nope"`},
		{"unterminated", "a ${vars.city", "unterminated ${ at offset 2"},
		{"unterminated string", "${'}", "unterminated ${ at offset 0"},
		{"empty", "a ${ } b", "empty expression at offset 2"},
		{"cost limit", "${vars.big.all(x, vars.big.all(y, x == y))}", "CEL cost limit exceeded"},
		{"unsplicable", "x ${ {1: 2} }", "only string keys are supported"},
		{"unsupported type", "${type(1)}", "unsupported result type"},
		{"unsupported nested", "${[type(1)]}", "unsupported result type"},
		{"unsupported in map", "${ {'a': type(1)} }", "unsupported result type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := map[string]any{"filters": []any{"ok", map[string]any{"v": tt.in}}}
			_, err := Interpolate(cfg, testCtx())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.want)
			}
		})
	}
	_, err := Interpolate(map[string]any{"v": "${vars.big.all(x, vars.big.all(y, x == y))}"}, testCtx())
	if !errors.Is(err, ErrCostLimit) {
		t.Errorf("cost limit error %v is not ErrCostLimit", err)
	}
}

func TestItemExpr(t *testing.T) {
	t.Parallel()
	item := map[string]any{"score": int64(82), "city": "Lyon", "list": make([]any, 200)}
	env := Envelope{ID: "it_1", Key: "k1", TS: startedAt, Attempt: 2, Step: "score"}
	vars := map[string]any{"minScore": int64(70)}
	run := Run{ID: "run_1", Trigger: "daily"}
	tests := []struct {
		name    string
		expr    string
		want    bool
		wantErr string
	}{
		{name: "threshold true", expr: "item.score >= vars.minScore", want: true},
		{name: "threshold false", expr: "item.score < vars.minScore", want: false},
		{name: "envelope", expr: "envelope.key == 'k1' && envelope.attempt == 2 && envelope.step == 'score' && envelope.id == 'it_1'", want: true},
		{name: "envelope ts", expr: "envelope.ts == timestamp('2026-10-14T06:00:00Z')", want: true},
		{name: "run", expr: "run.trigger == 'daily'", want: true},
		{name: "dyn non-bool at runtime", expr: "item.city", wantErr: `evaluate "item.city": want a bool, got string`},
		{name: "missing field", expr: "item.nope > 1", wantErr: "no such key: nope"},
		{name: "cost limit", expr: "item.list.all(x, item.list.all(y, x == y))", wantErr: "CEL cost limit exceeded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e, err := CompileItem(tt.expr)
			if err != nil {
				t.Fatalf("CompileItem: %v", err)
			}
			if e.String() != tt.expr {
				t.Errorf("String() = %q", e.String())
			}
			got, err := e.Eval(item, env, vars, run)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Errorf("Eval = %v, %v; want %v", got, err, tt.want)
			}
		})
	}
	e, _ := CompileItem("item.list.all(x, item.list.all(y, x == y))")
	if _, err := e.Eval(item, env, vars, run); !errors.Is(err, ErrCostLimit) {
		t.Errorf("cost limit error %v is not ErrCostLimit", err)
	}
	e, _ = CompileItem("size(vars) == 0")
	if ok, err := e.Eval(nil, Envelope{}, nil, Run{}); !ok || err != nil {
		t.Errorf("nil vars are an empty map: %v, %v", ok, err)
	}
}

func TestCompileErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		compile func(string) (*ItemExpr, error)
		expr    string
		want    string
	}{
		{"syntax error carries expression", CompileItem, "item.score >=", `compile "item.score >="`},
		{"secrets refused in item expressions", CompileItem, "secrets.portalToken == 'x'", `compile "secrets.portalToken == 'x'": secrets are only available in config interpolation`},
		{"secrets refused in routes", CompileRoute, "has(secrets.x)", "secrets are only available in config interpolation"},
		{"non-bool result", CompileItem, "'yes'", `compile "'yes'": must evaluate to a bool, not string`},
		{"non-bool route", CompileRoute, "1 + 2", "must evaluate to a bool, not int"},
		{"unknown variable", CompileItem, "foo > 1", "undeclared reference to 'foo'"},
		{"no envelope in routes", CompileRoute, "envelope.key == 'x'", "undeclared reference to 'envelope'"},
		{"type error", CompileItem, "1 == 'a'", "no matching overload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := tt.compile(tt.expr)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestCompileRoute(t *testing.T) {
	t.Parallel()
	e, err := CompileRoute("item.city == 'Lyon' && vars.on && run.id != ''")
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.Eval(map[string]any{"city": "Lyon"}, Envelope{}, map[string]any{"on": true}, Run{ID: "r"})
	if err != nil || !got {
		t.Errorf("Eval = %v, %v", got, err)
	}
	got, err = e.Eval(map[string]any{"city": "Paris"}, Envelope{}, map[string]any{"on": true}, Run{ID: "r"})
	if err != nil || got {
		t.Errorf("Eval = %v, %v", got, err)
	}
}

// The environments are built lazily; compiling from many goroutines at once must be race-free.
func TestConcurrentCompile(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	errs := make(chan error, 48)
	for range 16 {
		wg.Go(func() {
			if _, err := CompileItem("item.x > 1"); err != nil {
				errs <- err
			}
			if _, err := CompileRoute("item.x > 1"); err != nil {
				errs <- err
			}
			if _, err := Interpolate(map[string]any{"a": "${vars.x}"}, InterpCtx{Vars: map[string]any{"x": 1}}); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
