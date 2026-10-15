// Package expr is Millstream's CEL environment (spec section 4.4): config interpolation, evaluated once per run over
// vars, run and secrets, and item expressions (filters, route objects), evaluated per item over item, envelope,
// vars and run. Secrets exist only in interpolation, so no per-item expression can read or leak one. Every program
// runs under a cost limit; exceeding it is ErrCostLimit, which the engine treats as non-retryable.
package expr

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/interpreter"
)

// CostLimit is the most CEL cost units one evaluation may spend.
const CostLimit = 10_000

// ErrCostLimit is returned, wrapped, when an evaluation exceeds CostLimit.
var ErrCostLimit = errors.New("CEL cost limit exceeded")

// Names of the variables expressions see.
const (
	varVars     = "vars"
	varRun      = "run"
	varSecrets  = "secrets"
	varItem     = "item"
	varEnvelope = "envelope"
)

// Run is the run an expression evaluates in, exposed as run: {id, trigger, startedAt, revision}.
type Run struct {
	ID        string
	Trigger   string
	StartedAt time.Time
	Revision  string
}

func (r Run) value() map[string]any {
	return map[string]any{"id": r.ID, "trigger": r.Trigger, "startedAt": r.StartedAt, "revision": r.Revision}
}

// Envelope is the item envelope, exposed to item expressions as envelope: {id, key, ts, attempt, step}.
type Envelope struct {
	ID      string
	Key     string
	TS      time.Time
	Attempt int
	Step    string
}

func (e Envelope) value() map[string]any {
	return map[string]any{"id": e.ID, "key": e.Key, "ts": e.TS, "attempt": int64(e.Attempt), "step": e.Step}
}

var dynMap = cel.MapType(cel.StringType, cel.DynType)

// newEnv declares vars, run and secrets plus extra. Item environments declare secrets too, only so that a
// reference is reported as misuse (compileBool) rather than as an unknown name.
func newEnv(extra ...cel.EnvOption) func() (*cel.Env, error) {
	return sync.OnceValues(func() (*cel.Env, error) {
		return cel.NewEnv(append([]cel.EnvOption{
			cel.Variable(varVars, dynMap),
			cel.Variable(varRun, dynMap),
			cel.Variable(varSecrets, cel.MapType(cel.StringType, cel.StringType)),
		}, extra...)...)
	})
}

// The environments are built once, on first use; cel.Env is safe for concurrent use.
var (
	interpEnv = newEnv()
	itemEnv   = newEnv(cel.Variable(varItem, cel.DynType), cel.Variable(varEnvelope, dynMap))
	routeEnv  = newEnv(cel.Variable(varItem, cel.DynType))
)

// compile parses, type-checks and plans src in env.
func compile(envFn func() (*cel.Env, error), src string) (*cel.Ast, cel.Program, error) {
	env, err := envFn()
	if err != nil {
		return nil, nil, fmt.Errorf("build CEL environment: %w", err)
	}
	ast, iss := env.Compile(src)
	if iss.Err() != nil {
		return nil, nil, fmt.Errorf("compile %q: %s", src, strings.ReplaceAll(iss.Err().Error(), "\n", " "))
	}
	prg, err := env.Program(ast, cel.CostLimit(CostLimit))
	if err != nil {
		return nil, nil, fmt.Errorf("plan %q: %w", src, err)
	}
	return ast, prg, nil
}

// evalErr wraps an evaluation error, marking a cost limit breach with ErrCostLimit.
func evalErr(src string, err error) error {
	var cancelled interpreter.EvalCancelledError
	if errors.As(err, &cancelled) && cancelled.Cause == interpreter.CostLimitExceeded {
		return fmt.Errorf("evaluate %q: %w", src, ErrCostLimit)
	}
	return fmt.Errorf("evaluate %q: %w", src, err)
}

// orEmpty returns m, or an empty map when m is nil, so expressions see an empty map rather than null.
func orEmpty[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}
	return m
}
