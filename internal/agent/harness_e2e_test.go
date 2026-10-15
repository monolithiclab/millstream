//go:build e2e

package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestClaudeCode: the CLI, a script run as a process, gets the prompt on stdin and the arguments of Args; the
// stream-json result's structured output, usage, cost and turns come back, with every message in the transcript.
func TestClaudeCode(t *testing.T) {
	t.Parallel()
	bin, dir := fakeClaude(t, claudeStream, 0)
	h := ClaudeCode{Bin: bin, Env: []string{"PATH=/usr/bin:/bin", "ANTHROPIC_API_KEY=sk"}, Dir: dir}
	resp, err := h.Run(t.Context(), Request{Model: "claude-sonnet-5-5", Prompt: "Score {it}.", Tools: []string{"WebFetch",
		"Read"}, MaxTurns: 7, MaxCostUSD: 0.5, Schema: json.RawMessage(`{"type":"object"}` + "\n")})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out, _ := Extract(resp); string(out) != `{"score":9}` || resp.Text != "Done." || resp.TokensIn != 160 ||
		resp.TokensOut != 40 || resp.CostUSD == nil || *resp.CostUSD != 0.042 || resp.Turns != 3 || len(resp.Transcript) != 5 ||
		resp.Transcript[0].Text != "Score {it}." || resp.Transcript[2].Role != "assistant" {
		t.Fatalf("response %+v", resp)
	}
	args, _ := os.ReadFile(filepath.Join(dir, "args")) // #nosec G304 -- the test's own file
	stdin, _ := os.ReadFile(filepath.Join(dir, "stdin"))
	if string(args) != claudeArgs || string(stdin) != "Score {it}." {
		t.Fatalf("args:\n%s\nstdin %q", args, stdin)
	}
}

// TestClaudeCodeFailures: a budget stop is a non-retryable error keeping its cost; other failures are retryable.
func TestClaudeCodeFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		out       string
		status    int
		retryable bool
		message   string
	}{
		{"budget", `{"type":"result","subtype":"error_max_budget_usd","is_error":true,"total_cost_usd":0.51}`, 1, false,
			"maxCostUsd"},
		{"max turns", `{"type":"result","subtype":"error_max_turns","is_error":true,"num_turns":7}`, 1, true,
			"error_max_turns"},
		{"no result", `{"type":"system"}`, 0, true, "printed no result"},
		{"crash", ``, 3, true, "exit status 3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bin, _ := fakeClaude(t, tt.out, tt.status)
			resp, err := ClaudeCode{Bin: bin}.Run(t.Context(), Request{Model: "m", MaxTurns: 7, MaxCostUSD: 0.5})
			if err == nil || isRetryable(err) != tt.retryable || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("err = %v, want retryable %v with %q", err, tt.retryable, tt.message)
			}
			if tt.name == "budget" && (resp.CostUSD == nil || *resp.CostUSD != 0.51) {
				t.Fatalf("the cost of a stopped run is lost: %+v", resp)
			}
		})
	}
}
