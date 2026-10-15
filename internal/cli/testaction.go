package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/monolithiclab/millstream/internal/conformance"
	"github.com/monolithiclab/millstream/internal/eval"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// TestActionCmd is "millstream test action".
type TestActionCmd struct {
	Dir         string        `arg:"" type:"path" help:"The action directory, holding tests/*.yaml."`
	Package     string        `type:"path" help:"The package directory (default: the nearest directory above the action holding pipeline.yaml; without one, the action runs alone, unless it is an agent)."`
	HarnessMode string        `name:"harness-mode" enum:"replay,record,live" default:"replay" help:"Harness mode of an agent action's calls: replay, record or live."`
	HarnessDir  string        `name:"harness-dir" type:"path" help:"Recordings directory (default: <action>/tests/recordings)."`
	Conformance bool          `help:"Run the protocol conformance suite instead of the fixtures (exec and container actions)."`
	Timeout     time.Duration `default:"5s" help:"How long each conformance check waits for the action."`
}

// actionTestsDir is the directory of an action holding its fixtures, and actionRecordingsDir the default recordings
// directory of an agent's harness calls under it.
const (
	actionTestsDir      = "tests"
	actionRecordingsDir = "tests/recordings"
)

// actionTest is one tests/*.yaml file of an action.
type actionTest struct {
	Name   string         `yaml:"name"`
	Input  any            `yaml:"input"`
	Config map[string]any `yaml:"config"`
	Vars   map[string]any `yaml:"vars"`
	Expect struct {
		Outputs *[]struct {
			Key     *string `yaml:"key"`
			Payload any     `yaml:"payload"`
		} `yaml:"outputs"`
		Error *struct {
			Retryable bool `yaml:"retryable"`
		} `yaml:"error"`
	} `yaml:"expect"`
}

// Run runs each fixture through the action, in file order, and compares what it answered with the expectation:
// its outputs, in emitted order, by JSON equality of their payloads and by key where the fixture gives one, or an
// error with its retryability. It prints PASS or FAIL per fixture, with a diff on a mismatch, and fails when any
// fixture fails. With --conformance it runs the protocol conformance suite instead.
func (c TestActionCmd) Run(rt *Runtime) error {
	opts, err := actionOptions(rt, c.Dir, c.Package, true)
	if err != nil {
		return err
	}
	opts.Mode, opts.Dir = c.HarnessMode, harnessDir(c.HarnessMode, c.HarnessDir, c.Dir, actionRecordingsDir)
	tests, err := loadActionTests(c.Dir)
	if err != nil {
		return err
	}
	if c.Conformance {
		return c.conformance(rt, opts, tests)
	}
	if len(tests) == 0 {
		return usageError(fmt.Sprintf("no test case in %s", filepath.Join(c.Dir, actionTestsDir, "*.yaml")))
	}
	failed := 0
	for _, tc := range tests {
		r, err := eval.Invoke(rt.Ctx, opts, tc.Name, tc.Input, tc.Config, tc.Vars)
		var problem string
		if err != nil {
			problem = "  " + oneLine(err.Error(), 1000) + "\n"
		} else {
			problem = tc.compare(r)
		}
		if problem != "" {
			failed++
			fmt.Fprintf(rt.IO.Out, "FAIL %s\n%s", tc.Name, problem)
			continue
		}
		fmt.Fprintf(rt.IO.Out, "PASS %s\n", tc.Name)
	}
	fmt.Fprintf(rt.IO.Err, "%s: %d passed, %d failed\n", opts.Action.Name, len(tests)-failed, failed)
	if failed > 0 {
		return errFailed
	}
	return nil
}

// loadActionTests reads the fixtures of the action directory dir, by file name; a fixture's name defaults to its
// file name without the extension.
func loadActionTests(dir string) ([]actionTest, error) {
	files, err := filepath.Glob(filepath.Join(dir, actionTestsDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	tests := make([]actionTest, 0, len(files))
	for _, f := range files {
		data, err := os.ReadFile(f) // #nosec G304 -- a fixture of the action the user names
		if err != nil {
			return nil, err
		}
		var tc actionTest
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(&tc); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		if (tc.Expect.Outputs == nil) == (tc.Expect.Error == nil) {
			return nil, fmt.Errorf("%s: expect needs outputs or error, not both", f)
		}
		if tc.Name == "" {
			tc.Name = strings.TrimSuffix(filepath.Base(f), filepath.Ext(f))
		}
		if tc.Input == nil {
			tc.Input = map[string]any{}
		}
		tests = append(tests, tc)
	}
	return tests, nil
}

// compare returns what differs between the fixture's expectation and the action's answer, indented, or "".
func (tc actionTest) compare(r exec.Result) string {
	if e := tc.Expect.Error; e != nil {
		switch {
		case r.Err == nil:
			return fmt.Sprintf("  want an error (retryable %t), got done with %d output(s)\n", e.Retryable, len(r.Outputs))
		case r.Err.Retryable != e.Retryable:
			return fmt.Sprintf("  want an error with retryable %t, got retryable %t: %s\n", e.Retryable, r.Err.Retryable,
				oneLine(r.Err.Message, 1000))
		}
		return ""
	}
	if r.Err != nil {
		return fmt.Sprintf("  want done, got an error (retryable %t): %s\n", r.Err.Retryable, oneLine(r.Err.Message, 1000))
	}
	type output struct {
		Key     *string `json:"key,omitempty"`
		Payload any     `json:"payload"`
	}
	want := make([]output, len(*tc.Expect.Outputs))
	for i, o := range *tc.Expect.Outputs {
		want[i] = output{Key: o.Key, Payload: jsonRoundTrip(o.Payload)}
	}
	got := make([]output, len(r.Outputs))
	for i, o := range r.Outputs {
		var v any
		if err := json.Unmarshal(o.Payload, &v); err != nil {
			return fmt.Sprintf("  output %d: %v\n", i, err)
		}
		got[i] = output{Payload: v}
		if i < len(want) && want[i].Key != nil { // compare keys only where the fixture gives them
			got[i].Key = &o.Key
		}
	}
	if reflect.DeepEqual(want, got) {
		return ""
	}
	return lineDiff(prettyJSON(want), prettyJSON(got))
}

// jsonRoundTrip turns a value decoded from YAML into what decoding its JSON gives, so it compares with an output.
func jsonRoundTrip(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if json.Unmarshal(b, &out) != nil {
		return v
	}
	return out
}

func prettyJSON(v any) []string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return []string{err.Error()}
	}
	return strings.Split(string(b), "\n")
}

// lineDiff renders a line diff of want and got: "-" for lines only expected, "+" for lines only produced, from
// their longest common subsequence.
func lineDiff(want, got []string) string {
	n, m := len(want), len(got)
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if want[i] == got[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	var b strings.Builder
	b.WriteString("  outputs differ (- want, + got):\n")
	i, j := 0, 0
	for i < n || j < m {
		switch {
		case i < n && j < m && want[i] == got[j]:
			fmt.Fprintf(&b, "      %s\n", want[i])
			i, j = i+1, j+1
		case i < n && (j == m || lcs[i+1][j] >= lcs[i][j+1]):
			fmt.Fprintf(&b, "    - %s\n", want[i])
			i++
		default:
			fmt.Fprintf(&b, "    + %s\n", got[j])
			j++
		}
	}
	return b.String()
}

// conformance runs the protocol conformance suite against the action, with the first fixture's input, config and
// vars when it has fixtures, else an input made from its input schema.
func (c TestActionCmd) conformance(rt *Runtime, opts eval.Options, tests []actionTest) error {
	a := opts.Action
	if a.Kind != pipeline.KindExec && a.Kind != pipeline.KindContainer {
		return usageError(fmt.Sprintf("action %s is of kind %s: the conformance suite runs exec and container actions",
			c.Dir, a.Kind))
	}
	input := conformance.MinimalInput(a.Input)
	var config, vars map[string]any
	if len(tests) > 0 {
		b, err := json.Marshal(tests[0].Input)
		if err != nil {
			return fmt.Errorf("%s: input: %w", tests[0].Name, err)
		}
		input, config, vars = b, tests[0].Config, tests[0].Vars
	}
	if config == nil {
		config = map[string]any{}
	}
	if vars == nil {
		vars = map[string]any{}
	}
	launcher, ok := opts.Launchers[a.Kind]
	if !ok {
		return fmt.Errorf("action %s is of kind %s: no runtime for it here", c.Dir, a.Kind)
	}
	results := conformance.Run(rt.Ctx, conformance.Target{
		Spec: eval.ProcessSpec(opts, config, vars), Launcher: launcher, Input: input, ConfigSchema: a.Config,
		Timeout: c.Timeout,
	})
	for _, r := range results {
		fmt.Fprintf(rt.IO.Out, "%s %s: %s\n", r.Status, r.Name, oneLine(r.Reason, 1000))
	}
	if conformance.Failed(results) {
		fmt.Fprintf(rt.IO.Err, "%s: not conformant\n", a.Name)
		return errFailed
	}
	fmt.Fprintf(rt.IO.Err, "%s: conformant\n", a.Name)
	return nil
}
