package expr

import (
	"fmt"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
)

// Names of the variables eval assertions see.
const (
	varOutput = "output"
	varInput  = "input"
)

var assertEnv = newEnv(cel.Variable(varOutput, cel.DynType), cel.Variable(varInput, cel.DynType))

// Assertion is a compiled eval assertion (spec section 7.4): a boolean expression over an action's output, the
// input it was given and the case's vars.
type Assertion struct{ ItemExpr }

// CompileAssertion compiles an eval assertion over output, input and vars.
func CompileAssertion(src string) (*Assertion, error) {
	e, err := compileBool(assertEnv, src)
	if err != nil {
		return nil, err
	}
	return &Assertion{*e}, nil
}

// Eval evaluates the assertion. A non-boolean result, a runtime error or a cost limit breach is an error.
func (a *Assertion) Eval(output, input any, vars map[string]any) (bool, error) {
	out, _, err := a.prg.Eval(map[string]any{
		varOutput: output, varInput: input, varVars: orEmpty(vars), varRun: map[string]any{},
	})
	if err != nil {
		return false, evalErr(a.src, err)
	}
	b, ok := out.(types.Bool)
	if !ok {
		return false, fmt.Errorf("evaluate %q: want a bool, got %s", a.src, out.Type())
	}
	return bool(b), nil
}
