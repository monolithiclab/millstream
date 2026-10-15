package expr

import (
	"fmt"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
)

// ItemExpr is a compiled boolean expression over one item: a filter or a route object's when.
type ItemExpr struct {
	src string
	prg cel.Program
}

// CompileItem compiles a per-item boolean expression over item, envelope, vars and run.
func CompileItem(expr string) (*ItemExpr, error) {
	return compileBool(itemEnv, expr)
}

// CompileRoute compiles a route object's when: a boolean expression over the output item, vars and run (spec
// section 4.3). Routes are evaluated before the output has an envelope, so envelope is not available.
func CompileRoute(when string) (*ItemExpr, error) {
	return compileBool(routeEnv, when)
}

func compileBool(envFn func() (*cel.Env, error), src string) (*ItemExpr, error) {
	ast, prg, err := compileItemEnv(envFn, src)
	if err != nil {
		return nil, err
	}
	if t := ast.OutputType(); !t.IsExactType(cel.BoolType) && !t.IsExactType(cel.DynType) {
		return nil, fmt.Errorf("compile %q: must evaluate to a bool, not %s", src, t)
	}
	return &ItemExpr{src: src, prg: prg}, nil
}

// compileItemEnv compiles src in a per-item environment, refusing any reference to secrets.
func compileItemEnv(envFn func() (*cel.Env, error), src string) (*cel.Ast, cel.Program, error) {
	ast, prg, err := compile(envFn, src)
	if err != nil {
		return nil, nil, err
	}
	for _, ref := range ast.NativeRep().ReferenceMap() {
		if ref.Name == varSecrets {
			return nil, nil, fmt.Errorf("compile %q: secrets are only available in config interpolation", src)
		}
	}
	return ast, prg, nil
}

// String returns the expression's source.
func (e *ItemExpr) String() string { return e.src }

// Eval evaluates the expression for one item. A route expression ignores env. A non-boolean result, a runtime
// error or a cost limit breach (ErrCostLimit) is an error.
func (e *ItemExpr) Eval(item any, env Envelope, vars map[string]any, run Run) (bool, error) {
	out, err := evalItem(e.src, e.prg, item, env, vars, run)
	if err != nil {
		return false, err
	}
	b, ok := out.(types.Bool)
	if !ok {
		return false, fmt.Errorf("evaluate %q: want a bool, got %s", e.src, out.Type())
	}
	return bool(b), nil
}

// KeyExpr is a compiled per-item expression computing a key, such as dedupe's key (spec section 4.7).
type KeyExpr struct {
	src string
	prg cel.Program
}

// CompileKey compiles a per-item key expression over item, envelope, vars and run. Its result may have any type;
// Eval formats it as interpolation splices a value.
func CompileKey(expr string) (*KeyExpr, error) {
	_, prg, err := compileItemEnv(itemEnv, expr)
	if err != nil {
		return nil, err
	}
	return &KeyExpr{src: expr, prg: prg}, nil
}

// String returns the expression's source.
func (e *KeyExpr) String() string { return e.src }

// Eval computes the key of one item: strings as they are, other scalars as text, lists and maps as JSON.
func (e *KeyExpr) Eval(item any, env Envelope, vars map[string]any, run Run) (string, error) {
	out, err := evalItem(e.src, e.prg, item, env, vars, run)
	if err != nil {
		return "", err
	}
	v, err := native(out)
	if err != nil {
		return "", fmt.Errorf("evaluate %q: %w", e.src, err)
	}
	return splice(v)
}

func evalItem(src string, prg cel.Program, item any, env Envelope, vars map[string]any, run Run) (ref.Val, error) {
	out, _, err := prg.Eval(map[string]any{
		varItem:     item,
		varEnvelope: env.value(),
		varVars:     orEmpty(vars),
		varRun:      run.value(),
	})
	if err != nil {
		return nil, evalErr(src, err)
	}
	return out, nil
}
