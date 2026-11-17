package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Harness drives one LLM harness (spec section 7.1): it runs a rendered prompt to completion and reports what the
// model answered and what it cost. A harness never sees the action protocol.
type Harness interface {
	Run(ctx context.Context, req Request) (Response, error)
}

// Request is one harness run.
type Request struct {
	Model  string
	Prompt string
	// Schema is the action's output schema, for harnesses with native structured output.
	Schema json.RawMessage
	// Tools is the manifest's tool allowlist: the harness offers these and nothing else.
	Tools []string
	// MaxTurns bounds the run's turns; MaxCostUSD its cost, zero meaning no bound of the action's own.
	MaxTurns   int
	MaxCostUSD float64
	// Repairs are the attempt's earlier answers that failed the output schema, in order, each with the feedback the
	// adapter sends back on it (spec section 7.1): empty for the first turn, the harness answers the last feedback.
	Repairs []Repair
}

// Repair is a rejected answer and the feedback on it: the follow-up turn holding the validator's errors.
type Repair struct {
	Answer   string
	Feedback string
}

// Response is what a harness run produced.
type Response struct {
	// Text is the model's final answer; Structured its native structured output, when the harness has one.
	Text       string          `json:"text"`
	Structured json.RawMessage `json:"structured,omitempty"`
	TokensIn   int64           `json:"tokensIn"`
	TokensOut  int64           `json:"tokensOut"`
	// CostUSD is the cost the harness reported, nil when it reports none: the adapter then prices the tokens.
	CostUSD *float64 `json:"costUsd,omitempty"`
	Turns   int      `json:"turns"`
	// Transcript is every message of the run: the prompt, tool calls and results, the answers.
	Transcript []Turn `json:"transcript"`
}

// Turn is one message of a harness run: who sent it, its text, and the harness's own record of it when it has one
// (a stream-json line, an HTTP response body).
type Turn struct {
	Role string          `json:"role"`
	Text string          `json:"text,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// Error is a failed delivery as the adapter reports it: retryable unless the failure is deterministic (a missing
// recording, an exhausted budget, a refused request).
type Error struct {
	Msg       string
	Retryable bool
	// Schema marks an output that is not JSON or does not match the output schema: a schema-validation failure.
	Schema bool
}

func (e *Error) Error() string { return e.Msg }

// permanent is a non-retryable Error.
func permanent(format string, args ...any) error {
	return &Error{Msg: fmt.Sprintf(format, args...)}
}

// overBudget is the non-retryable Error of an attempt stopped by, or over, the action's maxCostUsd (spec section
// 7.5): its message starts with "budget:".
func overBudget(format string, args ...any) error {
	return &Error{Msg: "budget: " + fmt.Sprintf(format, args...)}
}

// schemaFailure is the retryable Error of an output that is not JSON or does not match the output schema.
func schemaFailure(format string, args ...any) error {
	return &Error{Msg: fmt.Sprintf(format, args...), Retryable: true, Schema: true}
}

// retryable is a retryable Error.
func retryable(format string, args ...any) error {
	return &Error{Msg: fmt.Sprintf(format, args...), Retryable: true}
}

// isRetryable reports whether err should be retried: an Error says so; anything else (a network failure, a
// harness that crashed) is retried.
func isRetryable(err error) bool {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Retryable
	}
	return true
}
