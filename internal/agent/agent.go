// Package agent is the agent adapter (spec section 7): the action process of agent steps, run by the engine as
// `millstream agent`. It speaks the action protocol (internal/protocol) on its stdin and stdout and drives a
// harness on the other side, so the engine never knows which model ran.
//
// For each item it renders the action's prompt template with the payload, config, vars and the step's lessons (from
// hello), runs the harness with the manifest's tools and limits, extracts the structured output (native when the
// harness has it, else the last fenced JSON block) and validates it against the output schema; a mismatch is a
// retryable error. It emits the output with the metrics tokens_in, tokens_out, cost_usd, turns and duration_ms
// (labelled with the model), and keeps the attempt's transcript as the artifact transcript.json, on failure as on
// success, with every secret value redacted.
//
// Harness calls go through a record/replay layer (Recorder), so tests and evals never reach a model. Secret values
// reach the adapter through its environment (MILLSTREAM_SECRET_<NAME>), for redaction and for the API key the
// manifest names; the harness receives the API key only.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/schema"
)

// TranscriptFile is the name, and the path under the working directory, of an attempt's transcript.
const TranscriptFile = "transcript.json"

// Metric names (spec section 7.1).
const (
	MetricTokensIn   = "tokens_in"
	MetricTokensOut  = "tokens_out"
	MetricCostUSD    = protocol.MetricCostUSD
	MetricTurns      = "turns"
	MetricDurationMS = "duration_ms"
	// MetricSchemaFailures is 1 for an attempt whose output is not JSON or does not match the output schema.
	MetricSchemaFailures = "schema_validation_failures"
)

// Options configure the adapter. Package and Action are required.
type Options struct {
	// Package is the package directory and Action the step's reference to the agent action in it (./actions/x).
	Package string
	Action  string
	// Loaded is the package at Package when the caller has loaded it already; nil loads it.
	Loaded *pipeline.Package
	// Secrets are the names of the pipeline's secrets; their values are read from MILLSTREAM_SECRET_<NAME>.
	Secrets []string
	// Price is the model's price from the environment file, nil when it has none.
	Price *pipeline.Price
	// Env looks up the adapter's environment variables.
	Env func(string) (string, bool)
	// Logger receives the adapter's logs (stderr, which the node relays); nil discards them.
	Logger *slog.Logger
	// NewHarness builds the harness of an action from its runtime and API key; nil means the driver the manifest
	// names. The record/replay layer wraps it either way.
	NewHarness func(rt *pipeline.AgentRuntime, apiKey string) Harness
}

// Run serves the protocol on in and out until shutdown or the end of input. A problem with the action itself (its
// manifest, prompt or harness setup) does not stop the process: every item is answered with it as a non-retryable
// error, so it reaches the step's dead letters with its reason.
func Run(ctx context.Context, in io.Reader, out io.Writer, opts Options) error {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Env == nil {
		opts.Env = func(string) (string, bool) { return "", false }
	}
	r, w := protocol.NewReader(in), protocol.NewWriter(out)
	m, err := r.Read()
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	hello, ok := m.(protocol.Hello)
	if !ok {
		return fmt.Errorf("%w: first message is %s, want hello", protocol.ErrProtocol, m.Type())
	}
	a := newAdapter(opts, hello)
	if a.setupErr != nil {
		opts.Logger.Error("agent action cannot run", "error", a.setupErr.Error())
	}
	if err := w.Write(protocol.Ready{}); err != nil {
		return err
	}
	for {
		m, err := r.Read()
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		}
		switch m := m.(type) {
		case protocol.Shutdown:
			return nil
		case protocol.Item:
			if err := a.handle(ctx, w, m); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unexpected %s message", protocol.ErrProtocol, m.Type())
		}
	}
}

// adapter is the state of one process: the action, set up once from hello.
type adapter struct {
	opts     Options
	hello    protocol.Hello
	setupErr error

	agent   *pipeline.AgentRuntime
	schema  json.RawMessage
	output  *schema.Schema
	prompt  *template.Template
	lessons pipeline.Lessons
	harness Harness
	redact  *redact.Redactor
	workdir string
}

func newAdapter(opts Options, hello protocol.Hello) *adapter {
	a := &adapter{opts: opts, hello: hello}
	secrets := map[string]string{}
	for _, name := range opts.Secrets {
		if v, ok := opts.Env(pipeline.SecretEnv(name)); ok {
			secrets[name] = v
		}
	}
	a.redact = redact.New(secrets)
	a.workdir, _ = opts.Env("MILLSTREAM_WORKDIR")
	a.setupErr = a.setup(secrets)
	return a
}

// setup resolves the action in its package and prepares its prompt, output schema and harness.
func (a *adapter) setup(secrets map[string]string) error {
	pkg := a.opts.Loaded
	if pkg == nil {
		var err error
		if pkg, err = pipeline.Load(a.opts.Package); err != nil {
			return err
		}
	}
	act, err := pipeline.Resolver{}.Resolve(pkg, a.opts.Action)
	if err != nil {
		return err
	}
	if act.Kind != pipeline.KindAgent {
		return fmt.Errorf("action %s is of kind %s, not agent", a.opts.Action, act.Kind)
	}
	a.agent, a.schema = act.Agent, act.Output
	if len(a.hello.Lessons) > 0 {
		if err := json.Unmarshal(a.hello.Lessons, &a.lessons); err != nil {
			return fmt.Errorf("hello lessons: %w", err)
		}
	}
	if a.output, err = schema.Compile(act.Output); err != nil {
		return fmt.Errorf("output schema: %w", err)
	}
	if a.prompt, err = ParsePrompt(act.Agent.Prompt, string(pkg.Files[act.Agent.Prompt])); err != nil {
		return err
	}
	mode, dir, err := harnessMode(a.opts.Env)
	if err != nil {
		return err
	}
	var apiKey string
	if name := act.Agent.APIKeySecret; name != "" {
		v, ok := secrets[name]
		if !ok && mode != ModeReplay { // a replay never calls the harness
			return fmt.Errorf("runtime.agent.apiKeySecret: secret %s is not one of the pipeline's secrets, or is "+
				"not set", name)
		}
		apiKey = v
	}
	newHarness := a.opts.NewHarness
	if newHarness == nil {
		newHarness = a.defaultHarness
	}
	a.harness = Recorder{Harness: newHarness(act.Agent, apiKey), Name: act.Agent.Harness, Mode: mode, Dir: dir}
	return nil
}

// ClaudeEnv are the variables of the adapter's environment the claude CLI receives, besides its API key: USER too,
// since on macOS the CLI reads its login from the user's keychain by account name and reports "Not logged in"
// without it.
var ClaudeEnv = []string{"PATH", "HOME", "TMPDIR", "USER", "CLAUDE_CONFIG_DIR", "XDG_CONFIG_HOME"}

// defaultHarness is the driver the manifest names.
func (a *adapter) defaultHarness(rt *pipeline.AgentRuntime, apiKey string) Harness {
	if rt.Harness == pipeline.HarnessAPI {
		return API{Provider: rt.Provider, BaseURL: rt.BaseURL, APIKey: apiKey}
	}
	path, _ := a.opts.Env("PATH")
	bin, ok := exec.LookPath("claude", path)
	if !ok {
		bin = "claude" // running it reports the error
	}
	c := ClaudeCode{Bin: bin, Dir: a.workdir}
	for _, k := range ClaudeEnv {
		if v, ok := a.opts.Env(k); ok {
			c.Env = append(c.Env, k+"="+v)
		}
	}
	if apiKey != "" {
		c.Env = append(c.Env, "ANTHROPIC_API_KEY="+apiKey)
	}
	return c
}

// Transcript is the record of one attempt, stored as its transcript.json artifact (spec section 7.2).
type Transcript struct {
	Delivery   string          `json:"delivery"`
	Item       string          `json:"item"`
	Attempt    int             `json:"attempt"`
	Harness    string          `json:"harness"`
	Model      string          `json:"model"`
	Prompt     string          `json:"prompt"`
	Turns      []Turn          `json:"turns"`
	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
	TokensIn   int64           `json:"tokensIn"`
	TokensOut  int64           `json:"tokensOut"`
	CostUSD    float64         `json:"costUsd"`
	NumTurns   int             `json:"numTurns"`
	DurationMS int64           `json:"durationMs"`
}

// handle answers one item: the output and done, or an error, each with the attempt's transcript.
func (a *adapter) handle(ctx context.Context, w *protocol.Writer, it protocol.Item) error {
	if a.setupErr != nil {
		return w.Write(protocol.Error{Delivery: it.Delivery, Message: a.setupErr.Error()})
	}
	start := time.Now()
	t := Transcript{Delivery: it.Delivery, Item: it.ID, Attempt: it.Attempt, Harness: a.agent.Harness,
		Model: a.agent.Model}
	output, err := a.attempt(ctx, it, &t)
	t.DurationMS = time.Since(start).Milliseconds()
	if err != nil {
		t.Error = err.Error()
	}
	var arts []protocol.Artifact
	if werr := a.writeTranscript(t); werr != nil {
		a.opts.Logger.Warn("transcript not kept", "error", werr.Error())
	} else {
		arts = []protocol.Artifact{{Name: TranscriptFile, Path: TranscriptFile}}
	}
	if t.Prompt != "" { // the harness ran: its metrics are due
		e, ok := errors.AsType[*Error](err)
		if err := a.metrics(w, it.Delivery, t, ok && e.Schema); err != nil {
			return err
		}
	}
	if err != nil {
		msg := err.Error()
		if arts != nil {
			msg += " (the attempt's transcript is its " + TranscriptFile + " artifact)"
		}
		return w.Write(protocol.Error{Delivery: it.Delivery, Message: msg, Retryable: isRetryable(err),
			Artifacts: arts})
	}
	if err := w.Write(protocol.Emit{Delivery: it.Delivery, Key: it.Key, Payload: output}); err != nil {
		return err
	}
	return w.Write(protocol.Done{Delivery: it.Delivery, Artifacts: arts})
}

// attempt renders the prompt, runs the harness and validates its output, recording each step in t: its prompt is
// set once the harness runs.
func (a *adapter) attempt(ctx context.Context, it protocol.Item, t *Transcript) (json.RawMessage, error) {
	var payload any
	if err := json.Unmarshal(it.Payload, &payload); err != nil {
		return nil, permanent("decode payload: %v", err)
	}
	prompt, err := Render(a.prompt, PromptData{Payload: payload, Config: a.hello.Config, Vars: a.hello.Vars,
		Lessons: a.lessons})
	if err != nil {
		return nil, permanent("%v", err) // a template that fails on this payload fails on every retry
	}
	t.Prompt = prompt
	resp, err := a.harness.Run(ctx, Request{
		Model: a.agent.Model, Prompt: prompt, Schema: a.schema, Tools: a.agent.Tools,
		MaxTurns: a.agent.MaxTurns, MaxCostUSD: a.agent.MaxCostUSD,
	})
	t.Turns, t.TokensIn, t.TokensOut, t.NumTurns = resp.Transcript, resp.TokensIn, resp.TokensOut, resp.Turns
	t.CostUSD = a.cost(resp)
	if limit := a.agent.MaxCostUSD; limit > 0 && t.CostUSD > limit {
		// Whatever the harness answered, the attempt spent more than one delivery may: no retry.
		return nil, overBudget("the attempt cost $%g, over the action's maxCostUsd of $%g", t.CostUSD, limit)
	}
	if err != nil {
		return nil, err
	}
	out, err := Extract(resp)
	if err != nil {
		return nil, err
	}
	t.Output = out
	var v any
	if err := json.Unmarshal(out, &v); err != nil {
		return nil, schemaFailure("decode output: %v", err)
	}
	if err := a.output.Validate(v); err != nil {
		return nil, schemaFailure("the output does not match the output schema: %v", err)
	}
	return out, nil
}

// cost is the cost the harness reported, else the tokens at the environment file's price, else 0 with a warning.
func (a *adapter) cost(resp Response) float64 {
	switch {
	case resp.CostUSD != nil:
		return *resp.CostUSD
	case a.opts.Price != nil:
		return (float64(resp.TokensIn)*a.opts.Price.InPerMtok + float64(resp.TokensOut)*a.opts.Price.OutPerMtok) / 1e6
	}
	if resp.TokensIn > 0 || resp.TokensOut > 0 {
		a.opts.Logger.Warn("cost unknown: the harness reports none and the environment file has no price for "+
			"the model; cost_usd is 0", "model", a.agent.Model)
	}
	return 0
}

// metrics sends the attempt's metrics, labelled with the model; schemaFailed adds a schema-validation failure.
func (a *adapter) metrics(w *protocol.Writer, delivery string, t Transcript, schemaFailed bool) error {
	labels := map[string]string{"model": t.Model}
	type metric struct {
		name  string
		value float64
	}
	ms := []metric{
		{MetricTokensIn, float64(t.TokensIn)}, {MetricTokensOut, float64(t.TokensOut)}, {MetricCostUSD, t.CostUSD},
		{MetricTurns, float64(t.NumTurns)}, {MetricDurationMS, float64(t.DurationMS)},
	}
	if schemaFailed {
		ms = append(ms, metric{MetricSchemaFailures, 1})
	}
	for _, m := range ms {
		if err := w.Write(protocol.Metric{Delivery: delivery, Name: m.name, Value: m.value, Labels: labels}); err != nil {
			return err
		}
	}
	return nil
}

// writeTranscript writes t as transcript.json in the working directory, every string in it, the harness's own
// records included, with its secret values redacted. Redacting decoded strings catches a value whatever escaping
// the harness used in its JSON.
func (a *adapter) writeTranscript(t Transcript) error {
	if a.workdir == "" {
		return errors.New("MILLSTREAM_WORKDIR is not set")
	}
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // numbers come back as written
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return err
	}
	data, err := json.MarshalIndent(a.redactJSON(doc), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(a.workdir, TranscriptFile), append(data, '\n'), 0o600)
}

// redactJSON redacts every string of a decoded JSON value, object keys included.
func (a *adapter) redactJSON(v any) any {
	switch v := v.(type) {
	case string:
		return a.redact.String(v)
	case []any:
		for i, e := range v {
			v[i] = a.redactJSON(e)
		}
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, e := range v {
			out[a.redact.String(k)] = a.redactJSON(e)
		}
		return out
	}
	return v
}
