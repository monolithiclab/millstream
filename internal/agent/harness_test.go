package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/agent/agenttest"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
)

func TestExtract(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		resp Response
		want string // "" means an error
	}{
		{"native", Response{Text: "```json\n{\"a\": 2}\n```", Structured: json.RawMessage(`{"a": 1}`)}, `{"a": 1}`},
		{"native invalid", Response{Structured: json.RawMessage(`{a}`)}, ""},
		{"last fenced block", Response{Text: "First:\n```json\n{\"a\": 1}\n```\nFinal:\n```json\n{\"a\": 2}\n```\n"}, `{"a": 2}`},
		{"fence with spaces", Response{Text: "```json  \r\n [1, 2] \r\n```"}, `[1, 2]`},
		{"other fences ignored", Response{Text: "```python\nprint(1)\n```\n```json\n\"x\"\n```"}, `"x"`},
		{"whole text", Response{Text: "  {\"a\": 3}\n"}, `{"a": 3}`},
		{"prose", Response{Text: "I think 7."}, ""},
		{"empty", Response{}, ""},
		{"invalid fenced block", Response{Text: "```json\n{a: 1}\n```"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Extract(tt.resp)
			if tt.want == "" {
				if err == nil || !isRetryable(err) {
					t.Fatalf("Extract = %s, %v; want a retryable error", got, err)
				}
				return
			}
			if err != nil || string(got) != tt.want {
				t.Fatalf("Extract = %s, %v; want %s", got, err, tt.want)
			}
		})
	}
}

func TestRender(t *testing.T) {
	t.Parallel()
	tmpl, err := ParsePrompt("p", "{{ .Payload.title }} in {{ .Vars.city }} ({{ .Config.mode }})")
	if err != nil {
		t.Fatal(err)
	}
	got, err := Render(tmpl, PromptData{Payload: map[string]any{"title": "Flat"}, Vars: map[string]any{"city": "Lyon"},
		Config: map[string]any{"mode": "fast"}})
	if err != nil || got != "Flat in Lyon (fast)" {
		t.Fatalf("Render = %q, %v", got, err)
	}
	if _, err := ParsePrompt("p", "{{ .Payload"); err == nil {
		t.Fatal("a bad template parsed")
	}
}

// TestRecorder: replay answers from the recording keyed by harness, model, prompt and schema; a change to any of
// them misses, with a non-retryable error naming the key; record stores what the harness answered.
func TestRecorder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	calls := 0
	live := &fakeHarness{fn: func(req Request) (Response, error) {
		calls++
		return Response{Text: "answer to " + req.Prompt, TokensIn: 3}, nil
	}}
	req := Request{Model: "m", Prompt: "hello", Schema: json.RawMessage(`{"type":"object"}`), MaxTurns: 4}
	rec := Recorder{Harness: live, Name: pipeline.HarnessAPI, Mode: ModeRecord, Dir: filepath.Join(dir, "rec")}
	if _, err := rec.Run(t.Context(), req); err != nil {
		t.Fatalf("record: %v", err)
	}
	key := Key(pipeline.HarnessAPI, req)
	if _, err := os.Stat(filepath.Join(dir, "rec", key+".json")); err != nil {
		t.Fatalf("recording: %v", err)
	}

	rec.Mode = ModeReplay
	resp, err := rec.Run(t.Context(), Request{Model: "m", Prompt: "hello", Schema: req.Schema, MaxTurns: 9})
	if err != nil || resp.Text != "answer to hello" || resp.TokensIn != 3 || calls != 1 {
		t.Fatalf("replay = %+v, %v (calls %d)", resp, err, calls)
	}
	for name, miss := range map[string]Request{
		"prompt": {Model: "m", Prompt: "hello!", Schema: req.Schema},
		"model":  {Model: "n", Prompt: "hello", Schema: req.Schema},
		"schema": {Model: "m", Prompt: "hello", Schema: json.RawMessage(`{}`)},
	} {
		_, err := rec.Run(t.Context(), miss)
		if err == nil || isRetryable(err) || !strings.Contains(err.Error(), Key(pipeline.HarnessAPI, miss)) {
			t.Fatalf("a changed %s replayed: %v", name, err)
		}
	}
	if Key(pipeline.HarnessClaudeCode, req) == key {
		t.Fatal("the harness is not part of the key")
	}
	if err := os.WriteFile(filepath.Join(dir, "rec", Key("x", req)+".json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (Recorder{Name: "x", Mode: ModeReplay, Dir: filepath.Join(dir, "rec")}).Run(t.Context(), req); err == nil {
		t.Fatal("a corrupt recording replayed")
	}

	rec.Mode = ModeLive
	if _, err := rec.Run(t.Context(), req); err != nil || calls != 2 {
		t.Fatalf("live: %v, calls %d", err, calls)
	}
}

// fakeAPI stands in for a model provider, in process: its Client hands every request to a handler that answers with
// status and body and records the request.
type fakeAPI struct {
	URL     string
	h       http.Handler
	mu      sync.Mutex
	paths   []string
	headers []http.Header
	bodies  []map[string]any
}

func newFakeAPI(t *testing.T, status int, body string) *fakeAPI {
	t.Helper()
	f := &fakeAPI{URL: agenttest.URL}
	f.h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var b map[string]any
		_ = json.Unmarshal(raw, &b)
		f.mu.Lock()
		f.paths, f.headers, f.bodies = append(f.paths, r.URL.Path), append(f.headers, r.Header.Clone()), append(f.bodies, b)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
	return f
}

// Client returns a client whose requests reach the fake.
func (f *fakeAPI) Client() *http.Client { return agenttest.Client(f.h) }

const (
	anthropicBody = `{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"Score:\n` +
		"```json\\n{\\\"score\\\": 7}\\n```" + `"}],"usage":{"input_tokens":120,"output_tokens":30}}`
	openAIBody = `{"id":"c1","choices":[{"index":0,"message":{"role":"assistant","content":"` +
		"```json\\n{\\\"score\\\": 4}\\n```" + `"}}],"usage":{"prompt_tokens":80,"completion_tokens":20}}`
)

// TestAPIAnthropic: the Messages API request carries the key, the version, the model, max_tokens and the prompt
// with the schema instruction; the answer's text and usage come back, with the raw response in the transcript.
func TestAPIAnthropic(t *testing.T) {
	t.Parallel()
	srv := newFakeAPI(t, http.StatusOK, anthropicBody)
	h := API{Provider: pipeline.ProviderAnthropic, BaseURL: srv.URL + "/", Client: srv.Client(), APIKey: "sk-ant"}
	resp, err := h.Run(t.Context(), Request{Model: "claude-sonnet-5-5", Prompt: "Score it.",
		Schema: json.RawMessage(`{"type":"object"}`)})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out, _ := Extract(resp); string(out) != `{"score": 7}` || resp.TokensIn != 120 || resp.TokensOut != 30 ||
		resp.CostUSD != nil || resp.Turns != 1 || len(resp.Transcript) != 2 || !json.Valid(resp.Transcript[1].Data) {
		t.Fatalf("response %+v", resp)
	}
	hd, body := srv.headers[0], srv.bodies[0]
	msgs, _ := body["messages"].([]any)
	first, _ := msgs[0].(map[string]any)
	content, _ := first["content"].(string)
	if srv.paths[0] != "/v1/messages" || hd.Get("X-Api-Key") != "sk-ant" || hd.Get("Anthropic-Version") != AnthropicVersion ||
		body["model"] != "claude-sonnet-5-5" || body["max_tokens"] != float64(DefaultMaxTokens) ||
		!strings.HasPrefix(content, "Score it.") || !strings.Contains(content, `{"type":"object"}`) {
		t.Fatalf("request %s %v %v", srv.paths[0], hd, body)
	}
}

func TestAPIOpenAI(t *testing.T) {
	t.Parallel()
	srv := newFakeAPI(t, http.StatusOK, openAIBody)
	h := API{Provider: pipeline.ProviderOpenAI, BaseURL: srv.URL + "/v1", Client: srv.Client(), APIKey: "sk-oai"}
	resp, err := h.Run(t.Context(), Request{Model: "mistral-large", Prompt: "Score it."})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out, _ := Extract(resp); string(out) != `{"score": 4}` || resp.TokensIn != 80 || resp.TokensOut != 20 {
		t.Fatalf("response %+v", resp)
	}
	if srv.paths[0] != "/v1/chat/completions" || srv.headers[0].Get("Authorization") != "Bearer sk-oai" ||
		srv.bodies[0]["max_tokens"] != float64(DefaultMaxTokens) || srv.bodies[0]["model"] != "mistral-large" {
		t.Fatalf("request %s %v %v", srv.paths[0], srv.headers[0], srv.bodies[0])
	}
}

// TestAPIErrors: rate limits, server errors, transport failures and undecodable answers are retried; refusals,
// tools and a missing key are not.
func TestAPIErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		status    int
		body      string
		provider  string
		retryable bool
	}{
		{"rate limited", http.StatusTooManyRequests, `{}`, pipeline.ProviderAnthropic, true},
		{"overloaded", 529, `{}`, pipeline.ProviderAnthropic, true},
		{"bad request", http.StatusBadRequest, `{"error":"bad"}`, pipeline.ProviderAnthropic, false},
		{"unauthorized", http.StatusUnauthorized, `{}`, pipeline.ProviderOpenAI, false},
		{"not JSON", http.StatusOK, `<html>`, pipeline.ProviderAnthropic, true},
		{"wrong shape", http.StatusOK, `{"content": 3}`, pipeline.ProviderAnthropic, true},
		{"no choices", http.StatusOK, `{"choices": []}`, pipeline.ProviderOpenAI, true},
		{"openai wrong shape", http.StatusOK, `{"choices": 1}`, pipeline.ProviderOpenAI, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := newFakeAPI(t, tt.status, tt.body)
			h := API{Provider: tt.provider, BaseURL: srv.URL, Client: srv.Client(), APIKey: "k"}
			_, err := h.Run(t.Context(), Request{Model: "m"})
			if err == nil || isRetryable(err) != tt.retryable {
				t.Fatalf("err = %v, want retryable %v", err, tt.retryable)
			}
		})
	}
	// Nothing listens on port 1: the default client's connection is refused.
	refused := API{BaseURL: "http://127.0.0.1:1", APIKey: "k"}
	if _, err := refused.Run(t.Context(), Request{}); err == nil || !isRetryable(err) {
		t.Fatalf("refused connection: %v", err)
	}
	if _, err := (API{}).Run(t.Context(), Request{}); err == nil || isRetryable(err) {
		t.Fatalf("no key: %v", err)
	}
	_, err := API{APIKey: "k"}.Run(t.Context(), Request{Tools: []string{"WebFetch"}})
	if err == nil || isRetryable(err) || !strings.Contains(err.Error(), "api harness tools arrive later") {
		t.Fatalf("tools: %v", err)
	}
}

// fakeClaude writes a fake claude CLI that saves its arguments and stdin under dir and prints out, then exits with
// status.
func fakeClaude(t *testing.T, out string, status int) (bin, dir string) {
	t.Helper()
	dir = t.TempDir()
	bin = filepath.Join(dir, "claude")
	script := "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\n' \"$a\"; done > " + dir + "/args\ncat > " + dir + "/stdin\n" +
		"cat <<'EOF'\n" + out + "\nEOF\nexit " + string(rune('0'+status)) + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil { // #nosec G306 -- an executable test script
		t.Fatal(err)
	}
	return bin, dir
}

const claudeStream = `{"type":"system","subtype":"init","tools":["WebFetch"]}
{"type":"assistant","message":{"content":[{"type":"tool_use","name":"WebFetch","input":{"url":"https://example.com"}}]}}
{"type":"user","message":{"content":[{"type":"tool_result","content":"a page"}]}}
not a message
{"type":"result","subtype":"success","is_error":false,"num_turns":3,"result":"Done.","structured_output":{"score":9},"total_cost_usd":0.042,"usage":{"input_tokens":100,"cache_read_input_tokens":50,"cache_creation_input_tokens":10,"output_tokens":40}}`

// TestClaudeCodeArgs: the CLI gets an explicit allowlist, the schema and the budget; without tools, an empty --tools
// disables them all and nothing is allowed. Running the CLI is the e2e build's TestClaudeCode.
func TestClaudeCodeArgs(t *testing.T) {
	t.Parallel()
	h := ClaudeCode{}
	got := strings.Join(h.Args(Request{Model: "claude-sonnet-5-5", Tools: []string{"WebFetch", "Read"}, MaxTurns: 7,
		MaxCostUSD: 0.5, Schema: json.RawMessage(`{"type":"object"}` + "\n")}), "\n") + "\n"
	if got != claudeArgs {
		t.Fatalf("args:\n%s", got)
	}
	if got := strings.Join(h.Args(Request{Model: "m", MaxTurns: 1}), " "); !strings.Contains(got, "--tools  --strict") ||
		strings.Contains(got, "allowedTools") || strings.Contains(got, "json-schema") || strings.Contains(got, "budget") {
		t.Fatalf("args without tools: %s", got)
	}
}

// claudeArgs are the CLI arguments of the request of TestClaudeCodeArgs and TestClaudeCode, one per line.
const claudeArgs = "--print\n--output-format\nstream-json\n--verbose\n--model\nclaude-sonnet-5-5\n--max-turns\n7\n--tools\n" +
	"WebFetch,Read\n--strict-mcp-config\n--no-session-persistence\n--allowedTools\nWebFetch,Read\n--json-schema\n" +
	"{\"type\":\"object\"}\n--max-budget-usd\n0.5\n"

// TestDefaultHarness: the manifest's harness is built with its API key; claude-code is found on the adapter's PATH
// and receives only its own variables, USER included (its keychain login on macOS), and the key.
func TestDefaultHarness(t *testing.T) {
	t.Parallel()
	bin, dir := fakeClaude(t, claudeStream, 0)
	env := map[string]string{"PATH": "/nonexistent:" + dir, "HOME": "/home/u", "USER": "u", "MILLSTREAM_SECRET_X": "s",
		"MILLSTREAM_WORKDIR": "/w"}
	a := &adapter{opts: Options{Env: func(k string) (string, bool) { v, ok := env[k]; return v, ok }}, workdir: "/w"}
	h := a.defaultHarness(&pipeline.AgentRuntime{Harness: pipeline.HarnessClaudeCode}, "sk")
	c, ok := h.(ClaudeCode)
	if !ok || c.Bin != bin || c.Dir != "/w" || strings.Join(c.Env, " ") != "PATH=/nonexistent:"+dir+" HOME=/home/u USER=u ANTHROPIC_API_KEY=sk" {
		t.Fatalf("claude-code harness %+v", h)
	}
	h = a.defaultHarness(&pipeline.AgentRuntime{Harness: pipeline.HarnessAPI, Provider: pipeline.ProviderOpenAI,
		BaseURL: "https://x/v1"}, "k")
	if api, ok := h.(API); !ok || api.APIKey != "k" || api.BaseURL != "https://x/v1" || api.Provider != pipeline.ProviderOpenAI {
		t.Fatalf("api harness %+v", h)
	}
	env["PATH"] = "/nonexistent"
	if c := a.defaultHarness(&pipeline.AgentRuntime{Harness: pipeline.HarnessClaudeCode}, "").(ClaudeCode); c.Bin != "claude" {
		t.Fatalf("claude-code without claude on PATH: %+v", c)
	}
}

// TestAPIMaxCost: an api attempt that costs more than maxCostUsd, priced from its usage, is a non-retryable budget
// error with its metrics still reported; one within it is emitted.
func TestAPIMaxCost(t *testing.T) {
	t.Parallel()
	srv := newFakeAPI(t, http.StatusOK, anthropicBody) // 120 tokens in, 30 out: $0.00081 at 3 and 15 per Mtok
	for _, tt := range []struct {
		limit string
		over  bool
	}{{"0.0005", true}, {"0.01", false}} {
		t.Run(tt.limit, func(t *testing.T) {
			t.Parallel()
			manifest := strings.Replace(testManifest, "apiKeySecret: apiToken",
				"apiKeySecret: apiToken, baseUrl: \""+srv.URL+"\", maxCostUsd: "+tt.limit, 1)
			s := newSession(t, memPackage(t, manifest, testPrompt), nil)
			s.opts.Price = &pipeline.Price{InPerMtok: 3, OutPerMtok: 15}
			s.opts.Client = srv.Client()
			msgs := s.run(`{"url":"https://example.com/1"}`)
			var cost float64
			for _, m := range msgs {
				if mt, ok := m.(protocol.Metric); ok && mt.Name == MetricCostUSD {
					cost = mt.Value
				}
			}
			if cost < 0.00080 || cost > 0.00082 {
				t.Fatalf("cost_usd = %v, messages %+v", cost, msgs)
			}
			last := msgs[len(msgs)-1]
			e, isErr := last.(protocol.Error)
			if isErr != tt.over {
				t.Fatalf("last message = %+v", last)
			}
			if tt.over && (e.Retryable || !strings.HasPrefix(e.Message, "budget: ") ||
				!strings.Contains(e.Message, "maxCostUsd")) {
				t.Fatalf("error = %+v", e)
			}
		})
	}
}

// fakeCLI answers a ClaudeCode invocation in-process, like fakeClaude's script: it records the arguments and stdin,
// prints out and fails with status when it is not zero.
type fakeCLI struct {
	out    string
	status int
	args   []string
	stdin  string
}

func (f *fakeCLI) Run(cmd *osexec.Cmd) error {
	f.args = cmd.Args[1:]
	b, err := io.ReadAll(cmd.Stdin)
	if err != nil {
		return err
	}
	f.stdin = string(b)
	if _, err := io.WriteString(cmd.Stdout, f.out); err != nil {
		return err
	}
	if f.status != 0 {
		return fmt.Errorf("exit status %d", f.status)
	}
	return nil
}

// TestClaudeCodeInProcess: TestClaudeCode (harness_e2e_test.go) with the CLI answered in-process.
func TestClaudeCodeInProcess(t *testing.T) {
	t.Parallel()
	cli := &fakeCLI{out: claudeStream}
	h := ClaudeCode{Bin: "claude", Env: []string{"ANTHROPIC_API_KEY=sk"}, cli: cli}
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
	if got := strings.Join(cli.args, "\n") + "\n"; got != claudeArgs || cli.stdin != "Score {it}." {
		t.Fatalf("args:\n%s\nstdin %q", got, cli.stdin)
	}
}

// TestClaudeCodeFailuresInProcess: TestClaudeCodeFailures (harness_e2e_test.go) with the CLI answered in-process.
func TestClaudeCodeFailuresInProcess(t *testing.T) {
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
			h := ClaudeCode{Bin: "claude", cli: &fakeCLI{out: tt.out, status: tt.status}}
			resp, err := h.Run(t.Context(), Request{Model: "m", MaxTurns: 7, MaxCostUSD: 0.5})
			if err == nil || isRetryable(err) != tt.retryable || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("err = %v, want retryable %v with %q", err, tt.retryable, tt.message)
			}
			if tt.name == "budget" && (resp.CostUSD == nil || *resp.CostUSD != 0.51) {
				t.Fatalf("the cost of a stopped run is lost: %+v", resp)
			}
		})
	}
}
