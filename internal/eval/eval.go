// Package eval runs the evals of an action (spec section 7.4): the cases under evals/*.yaml in its directory, each an
// input run through the action and scored by CEL assertions over the output and, optionally, an LLM judge applying a
// rubric. Agent actions run in-process through the agent adapter, exec and container actions through the exec
// runtime, the way a node runs them; harness calls, the judge's included, go through the agent package's
// record/replay layer, so an eval replays deterministically in CI and never reaches a model there.
//
// A case's score is the mean of its parts: the share of its assertions that hold, and the judge's score from 0 to 1.
// A case whose action fails, or whose judge cannot answer, scores 0 and is reported with its error.
package eval

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/schema"
	"github.com/monolithiclab/millstream/internal/store"

	"go.yaml.in/yaml/v3"
)

// Dir is the directory of an action holding its eval cases, and RecordingsDir the default recordings directory
// under it.
const (
	Dir           = "evals"
	RecordingsDir = "evals/recordings"
)

// Case is one eval case: evals/<file>.yaml.
type Case struct {
	// Name defaults to the file name without its extension.
	Name string `yaml:"name"`
	// Input is the item's payload; Config and Vars are the hello's, as a step would resolve them.
	Input  any            `yaml:"input"`
	Config map[string]any `yaml:"config"`
	Vars   map[string]any `yaml:"vars"`
	// Assert are CEL expressions over output, input and vars that must hold.
	Assert []string `yaml:"assert"`
	Judge  *Judge   `yaml:"judge"`

	asserts []*expr.Assertion
}

// Judge asks a model to score the output against a rubric, through the api harness.
type Judge struct {
	Rubric string `yaml:"rubric"`
	Model  string `yaml:"model"`
	// Provider is anthropic (the default) or openai, BaseURL its endpoint (required for openai), and APIKeySecret the
	// name of the pipeline secret holding the API key, read from MILLSTREAM_SECRET_<NAME>.
	Provider     string `yaml:"provider"`
	BaseURL      string `yaml:"baseUrl"`
	APIKeySecret string `yaml:"apiKeySecret"`
}

// Load reads the eval cases of the action directory dir, by file name. Every case is checked: a name of its own,
// assertions that compile, at least one assertion or a judge, and a judge with a rubric and a model.
func Load(dir string) ([]*Case, error) {
	files, err := filepath.Glob(filepath.Join(dir, Dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	return parseCases(files, os.ReadFile)
}

// parseCases parses the eval files, in that order, reading each with read.
func parseCases(files []string, read func(string) ([]byte, error)) ([]*Case, error) {
	var cases []*Case
	seen := map[string]string{}
	for _, f := range files {
		c, err := loadCase(f, read)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		if other, dup := seen[c.Name]; dup {
			return nil, fmt.Errorf("%s: case %q is also named by %s", f, c.Name, other)
		}
		seen[c.Name] = f
		cases = append(cases, c)
	}
	return cases, nil
}

func loadCase(path string, read func(string) ([]byte, error)) (*Case, error) {
	data, err := read(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	c := &Case{}
	if err := dec.Decode(c); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if c.Name == "" {
		c.Name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	if c.Input == nil {
		c.Input = map[string]any{}
	}
	if c.Config == nil {
		c.Config = map[string]any{}
	}
	if c.Vars == nil {
		c.Vars = map[string]any{}
	}
	// As JSON values, numbers int64 when integral: what the action receives and what CEL compares.
	if c.Input, err = jsonValue(c.Input); err != nil {
		return nil, fmt.Errorf("input: %w", err)
	}
	for key, m := range map[string]*map[string]any{"config": &c.Config, "vars": &c.Vars} {
		v, err := jsonValue(*m)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		*m, _ = v.(map[string]any)
	}
	if len(c.Assert) == 0 && c.Judge == nil {
		return nil, errors.New("a case needs assertions (assert) or a judge")
	}
	for i, src := range c.Assert {
		a, err := expr.CompileAssertion(src)
		if err != nil {
			return nil, fmt.Errorf("assert[%d]: %w", i, err)
		}
		c.asserts = append(c.asserts, a)
	}
	if j := c.Judge; j != nil {
		switch {
		case j.Rubric == "" || j.Model == "":
			return nil, errors.New("judge: a rubric and a model are required")
		case j.Provider == "":
			j.Provider = pipeline.ProviderAnthropic
		case j.Provider != pipeline.ProviderAnthropic && j.Provider != pipeline.ProviderOpenAI:
			return nil, fmt.Errorf("judge.provider: must be %s or %s", pipeline.ProviderAnthropic, pipeline.ProviderOpenAI)
		}
		if j.Provider == pipeline.ProviderOpenAI && j.BaseURL == "" {
			return nil, errors.New("judge.baseUrl: the openai provider needs the endpoint's base URL")
		}
	}
	return c, nil
}

// Options say what to run the cases with. Package, Action and Ref are required.
type Options struct {
	Package *pipeline.Package
	// Action is the resolved action, an agent or exec one, and Ref the reference that resolved it (./actions/x).
	Action *pipeline.Action
	Ref    string
	// Mode is the harness mode (agent.ModeReplay, ModeRecord or ModeLive) and Dir the recordings directory, for the
	// agent's calls and the judge's.
	Mode, Dir string
	// Env looks up the environment: PATH, HOME and TMPDIR for exec actions, and the secrets
	// (MILLSTREAM_SECRET_<NAME>) for API keys.
	Env func(string) (string, bool)
	// Price is the agent model's price from the environment file, nil when it has none.
	Price *pipeline.Price
	// Logger receives the action's logs; nil discards them.
	Logger *slog.Logger
	// Launchers start exec and container processes by kind, as exec.Options says; nil runs exec actions only.
	Launchers map[string]exec.Launcher
	// Client sends the api harness's requests, the agent action's and the judge's; nil means http.DefaultClient.
	Client *http.Client
}

// Result is a case's outcome.
type Result struct {
	Case string
	// Passed of Total assertions held; Failed lists the others, each with why when it did not evaluate.
	Passed, Total int
	Failed        []string
	// Judge is the judge's verdict, nil without a judge or when it failed.
	Judge *Verdict
	// Err is why the case could not be scored: the action or the judge failed.
	Err string
	// CostUSD is what the action reported spending (its cost_usd metrics).
	CostUSD float64
	Score   float64
}

// Verdict is a judge's answer.
type Verdict struct {
	Score  float64 `json:"score"`
	Reason string  `json:"reason"`
}

// Passes reports whether the case passed: every assertion held and nothing failed. A judge's score never fails a
// case: it is a score, not a check.
func (r Result) Passes() bool { return r.Err == "" && r.Passed == r.Total }

// Run runs one case and scores it.
func Run(ctx context.Context, opts Options, c *Case) Result {
	res := Result{Case: c.Name, Total: len(c.asserts)}
	out, err := runAction(ctx, opts, c, &res)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	for _, a := range c.asserts {
		ok, err := a.Eval(out, c.Input, c.Vars)
		switch {
		case err != nil:
			res.Failed = append(res.Failed, fmt.Sprintf("%s: %v", a, err))
		case ok:
			res.Passed++
		default:
			res.Failed = append(res.Failed, a.String())
		}
	}
	var sum, parts float64 // Load ensures a case has assertions or a judge
	if res.Total > 0 {
		sum, parts = float64(res.Passed)/float64(res.Total), 1
	}
	if c.Judge != nil {
		v, err := judge(ctx, opts, c, out)
		if err != nil {
			res.Err = fmt.Sprintf("judge: %v", err)
			return res
		}
		res.Judge = v
		sum, parts = sum+v.Score, parts+1
	}
	res.Score = sum / parts
	return res
}

// jsonValue converts a value read from YAML to the values a JSON document decodes to, numbers as int64 when integral,
// as CEL sees payloads in the engine.
func jsonValue(v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return pipeline.DecodeValue(string(raw))
}

// runAction runs the case's input through the action and returns its output as CEL sees it: the payload of its one
// output, the list of their payloads when it emitted several, null when none.
func runAction(ctx context.Context, opts Options, c *Case, res *Result) (any, error) {
	r, err := Invoke(ctx, opts, c.Name, c.Input, c.Config, c.Vars)
	if err != nil {
		return nil, err
	}
	for _, m := range r.Metrics {
		if m.Name == protocol.MetricCostUSD {
			res.CostUSD += m.Value
		}
	}
	if r.Err != nil {
		return nil, fmt.Errorf("the action failed: %s", r.Err.Message)
	}
	outs := make([]any, len(r.Outputs))
	for i, o := range r.Outputs {
		if outs[i], err = pipeline.DecodeValue(string(o.Payload)); err != nil {
			return nil, fmt.Errorf("output %d: %w", i, err)
		}
	}
	switch len(outs) {
	case 0:
		return nil, nil
	case 1:
		return outs[0], nil
	}
	return outs, nil
}

// Invoke runs one input through the action the way a node would, as the item of a delivery named after name, with
// the hello's config and vars: an agent action in-process through the adapter, an exec or container action as a
// process started by the launcher of its kind. The result holds the action's outputs and metrics, or its own error;
// err is a failure around the action (it could not start, it broke the protocol).
func Invoke(ctx context.Context, opts Options, name string, input any, config, vars map[string]any) (exec.Result,
	error,
) {
	payload, err := json.Marshal(input)
	if err != nil {
		return exec.Result{}, fmt.Errorf("input: %w", err)
	}
	item := protocol.Item{
		Delivery: "eval", ID: "eval-" + name, Key: name, Payload: payload, Artifacts: []protocol.Artifact{},
		Run:     protocol.Run{ID: "eval", Trigger: "eval", StartedAt: time.Now().UTC(), Revision: opts.Package.Hash},
		Attempt: 1, IdempotencyKey: "eval-" + name,
	}
	switch opts.Action.Kind {
	case pipeline.KindAgent:
		return runAgent(ctx, opts, config, vars, item)
	case pipeline.KindExec, pipeline.KindContainer:
		return runProcess(ctx, opts, config, vars, item)
	}
	return exec.Result{}, fmt.Errorf("action %s is of kind %s: only agent, exec and container actions run here",
		opts.Ref, opts.Action.Kind)
}

// ProcessSpec is the process an exec or container action runs as, the way a node runs it: an exec action in its
// directory with PATH, HOME and TMPDIR only, a container from its pinned image with none of them.
func ProcessSpec(opts Options, config, vars map[string]any) exec.Spec {
	a := opts.Action
	spec := exec.Spec{
		Action: a.Name, Version: a.Version, Kind: a.Kind, Digest: opts.Package.ActionDigest(a.Dir),
		Command: slices.Clone(a.Command), Lifecycle: a.Lifecycle, Config: config, Vars: vars,
	}
	if a.Kind == pipeline.KindContainer {
		spec.Image, spec.Network = a.Pin, a.Network
		return spec
	}
	spec.Dir = filepath.Join(opts.Package.Dir, filepath.FromSlash(a.Dir))
	for _, k := range []string{"PATH", "HOME", "TMPDIR"} {
		if v, ok := opts.Env(k); ok {
			spec.Env = append(spec.Env, k+"="+v)
		}
	}
	return spec
}

// runProcess runs an exec or container action as a node would.
func runProcess(ctx context.Context, opts Options, config, vars map[string]any, item protocol.Item) (exec.Result,
	error,
) {
	p, err := exec.Start(ctx, ProcessSpec(opts, config, vars), exec.Options{Logger: opts.Logger,
		Launchers: opts.Launchers})
	if err != nil {
		return exec.Result{}, err
	}
	defer func() { _ = p.Close() }()
	return p.Deliver(ctx, exec.Delivery{Item: item, Timeout: pipeline.DefaultTimeout, Log: opts.Logger})
}

// runAgent runs an agent action through the adapter in this process, in a working directory of its own, with the
// harness mode and recordings directory of opts.
func runAgent(ctx context.Context, opts Options, config, vars map[string]any, item protocol.Item) (exec.Result,
	error,
) {
	workdir, err := os.MkdirTemp("", "millstream-eval-")
	if err != nil {
		return exec.Result{}, err
	}
	defer func() { _ = os.RemoveAll(workdir) }()
	overlay := map[string]string{"MILLSTREAM_WORKDIR": workdir, agent.EnvHarnessMode: opts.Mode,
		agent.EnvHarnessDir: opts.Dir}
	var in, out bytes.Buffer
	w := protocol.NewWriter(&in)
	for _, m := range []protocol.Message{
		protocol.Hello{Protocol: protocol.Version, Action: opts.Action.Name, Config: config, Vars: vars},
		item, protocol.Shutdown{},
	} {
		if err := w.Write(m); err != nil {
			return exec.Result{}, err
		}
	}
	err = agent.Run(ctx, &in, &out, agent.Options{
		Package: opts.Package.Dir, Loaded: opts.Package, Action: opts.Ref, Secrets: opts.Package.Pipeline.Secrets,
		Price: opts.Price, Logger: opts.Logger, Client: opts.Client,
		Env: func(k string) (string, bool) {
			if v, ok := overlay[k]; ok {
				return v, v != ""
			}
			return opts.Env(k)
		},
	})
	if err != nil {
		return exec.Result{}, err
	}
	var r exec.Result
	rd := protocol.NewReader(&out)
	for {
		m, err := rd.Read()
		if errors.Is(err, io.EOF) {
			return r, nil
		}
		if err != nil {
			return r, err
		}
		switch m := m.(type) {
		case protocol.Emit:
			r.Outputs = append(r.Outputs, store.Output{Key: m.Key, Payload: m.Payload})
		case protocol.Metric:
			r.Metrics = append(r.Metrics, m)
		case protocol.Error:
			r.Err = &exec.ActionError{Message: m.Message, Retryable: m.Retryable}
		}
	}
}

// judgeSchema is the answer a judge gives.
var judgeSchema = json.RawMessage(`{"type":"object","properties":{"score":{"type":"number","minimum":0,"maximum":1},` +
	`"reason":{"type":"string"}},"required":["score","reason"]}`)

// judgePrompt asks for a verdict on an output; the api harness appends the schema of the answer.
const judgePrompt = `You are grading the output of an automated step against a rubric.

Rubric:

%s

The step's input:

` + "```json\n%s\n```" + `

Its output:

` + "```json\n%s\n```" + `

Score how well the output meets the rubric, from 0 (not at all) to 1 (fully), and give the reason in one sentence.`

// judge asks the case's judge for its verdict on output, through the api harness behind the recorder.
// secretsRedactor redacts the package's secrets and the extra ones, as set in the environment, out of what the
// judge records: its prompt carries the case's input and the action's output.
func secretsRedactor(opts Options, extra ...string) *redact.Redactor {
	secrets := map[string]string{}
	for _, name := range append(slices.Clone(opts.Package.Pipeline.Secrets), extra...) {
		if name == "" {
			continue
		}
		if v, ok := opts.Env(pipeline.SecretEnv(name)); ok {
			secrets[name] = v
		}
	}
	return redact.New(secrets)
}

func judge(ctx context.Context, opts Options, c *Case, output any) (*Verdict, error) {
	j := c.Judge
	in, err := json.MarshalIndent(c.Input, "", "  ")
	if err != nil {
		return nil, err
	}
	out, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return nil, err
	}
	var key string
	if j.APIKeySecret != "" {
		key, _ = opts.Env(pipeline.SecretEnv(j.APIKeySecret))
	}
	h := agent.Recorder{
		Harness: agent.API{Provider: j.Provider, BaseURL: j.BaseURL, APIKey: key, Client: opts.Client},
		Name:    pipeline.HarnessAPI, Mode: opts.Mode, Dir: opts.Dir, Redact: secretsRedactor(opts, j.APIKeySecret),
	}
	resp, err := h.Run(ctx, agent.Request{
		Model: j.Model, Prompt: fmt.Sprintf(judgePrompt, strings.TrimSpace(j.Rubric), in, out), Schema: judgeSchema,
	})
	if err != nil {
		return nil, err
	}
	raw, err := agent.Extract(resp)
	if err != nil {
		return nil, err
	}
	s, err := schema.Compile(judgeSchema)
	if err != nil {
		return nil, err
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	if err := s.Validate(doc); err != nil {
		return nil, fmt.Errorf("the verdict does not match {score, reason}: %w", err)
	}
	var v Verdict
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return &v, nil
}
