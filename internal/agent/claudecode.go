package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	osexec "os/exec"
	"strconv"
	"strings"
)

// maxHarnessOutput bounds what the adapter reads from a harness: a run's whole stream-json output, or an HTTP
// response body.
const maxHarnessOutput = 64 << 20

// ClaudeCode is the claude-code harness: Claude Code in headless mode (claude -p), with the manifest's tools as its
// only tools, no MCP servers, no session saved, and the output schema as its native structured output. The prompt
// goes on stdin, never on the command line. The output is stream-json, so the transcript holds every message,
// tool calls included; its last line is the result Claude Code prints with --output-format json.
type ClaudeCode struct {
	// Bin is the claude CLI.
	Bin string
	// Env is the CLI's whole environment: PATH, HOME and its configuration, and the API key when the manifest
	// names one, never the pipeline's other secrets (spec section 15.4).
	Env []string
	// Dir is the CLI's working directory.
	Dir string
	// cli runs the CLI; nil runs it as a process. Tests answer in-process through it.
	cli runner
}

// runner runs a CLI invocation as cmd describes it (arguments, stdin, environment, output writers) and waits for it.
type runner interface {
	Run(cmd *osexec.Cmd) error
}

// Args are the CLI arguments of req.
func (c ClaudeCode) Args(req Request) []string {
	tools := strings.Join(req.Tools, ",")
	args := []string{"--print", "--output-format", "stream-json", "--verbose", "--model", req.Model,
		"--max-turns", strconv.Itoa(req.MaxTurns), "--tools", tools, "--strict-mcp-config", "--no-session-persistence"}
	if tools != "" {
		args = append(args, "--allowedTools", tools)
	}
	if len(req.Schema) > 0 {
		args = append(args, "--json-schema", string(bytes.TrimSpace(req.Schema)))
	}
	if req.MaxCostUSD > 0 {
		args = append(args, "--max-budget-usd", strconv.FormatFloat(req.MaxCostUSD, 'f', -1, 64))
	}
	return args
}

// claudeResult is the result line of Claude Code's output. Every field is optional: a missing one reads as zero.
type claudeResult struct {
	Type             string          `json:"type"`
	Subtype          string          `json:"subtype"`
	IsError          bool            `json:"is_error"`
	Result           string          `json:"result"`
	StructuredOutput json.RawMessage `json:"structured_output"`
	NumTurns         int             `json:"num_turns"`
	TotalCostUSD     *float64        `json:"total_cost_usd"`
	Usage            struct {
		InputTokens              int64 `json:"input_tokens"`
		OutputTokens             int64 `json:"output_tokens"`
		CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
		CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
	} `json:"usage"`
	Errors []string `json:"errors"`
}

// run runs the CLI invocation cmd and waits for it.
func (c ClaudeCode) run(cmd *osexec.Cmd) error {
	if c.cli != nil {
		return c.cli.Run(cmd)
	}
	return cmd.Run()
}

// Run implements Harness.
func (c ClaudeCode) Run(ctx context.Context, req Request) (Response, error) {
	cmd := osexec.CommandContext(ctx, c.Bin, c.Args(req)...) // #nosec G204 -- the claude CLI, with arguments we build
	cmd.Env, cmd.Dir = c.Env, c.Dir
	cmd.Stdin = strings.NewReader(req.Prompt)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{w: &stdout, n: maxHarnessOutput}
	cmd.Stderr = &limitedWriter{w: &stderr, n: 64 << 10}
	runErr := c.run(cmd)

	resp := Response{Transcript: []Turn{{Role: "user", Text: req.Prompt}}}
	var result *claudeResult
	sc := bufio.NewScanner(&stdout)
	sc.Buffer(make([]byte, 0, 64<<10), maxHarnessOutput)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		var head struct {
			Type string `json:"type"`
		}
		if len(line) == 0 || json.Unmarshal(line, &head) != nil {
			continue // not a message: Claude Code prints nothing else, but a stray line must not fail the run
		}
		data := json.RawMessage(bytes.Clone(line))
		resp.Transcript = append(resp.Transcript, Turn{Role: head.Type, Data: data})
		if head.Type == "result" {
			result = &claudeResult{}
			_ = json.Unmarshal(data, result) // it decoded once already
		}
	}
	if result == nil {
		msg := strings.TrimSpace(stderr.String())
		if runErr != nil {
			return resp, retryable("claude-code: %v: %s", runErr, msg)
		}
		return resp, retryable("claude-code printed no result: %s", msg)
	}
	resp.Text, resp.Structured, resp.Turns = result.Result, result.StructuredOutput, result.NumTurns
	resp.TokensIn = result.Usage.InputTokens + result.Usage.CacheCreationInputTokens + result.Usage.CacheReadInputTokens
	resp.TokensOut = result.Usage.OutputTokens
	resp.CostUSD = result.TotalCostUSD
	if string(resp.Structured) == "null" {
		resp.Structured = nil
	}
	if result.IsError || strings.HasPrefix(result.Subtype, "error") {
		detail := strings.Join(append(result.Errors, result.Result), "; ")
		if strings.Contains(result.Subtype, "budget") {
			return resp, overBudget("claude-code stopped at maxCostUsd %g (%s): %s", req.MaxCostUSD, result.Subtype,
				detail)
		}
		return resp, retryable("claude-code failed (%s): %s", result.Subtype, detail)
	}
	return resp, nil
}

// limitedWriter keeps the first n bytes written to it and drops the rest, so a runaway child cannot exhaust memory.
type limitedWriter struct {
	w *bytes.Buffer
	n int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	k := min(len(p), l.n)
	l.w.Write(p[:k])
	l.n -= k
	return len(p), nil
}
