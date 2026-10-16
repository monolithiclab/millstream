package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// TestCmd is "millstream test".
type TestCmd struct {
	Pipeline TestPipelineCmd `cmd:"" help:"Run a package's tests/*.yaml against stubbed actions."`
	Action   TestActionCmd   `cmd:"" help:"Run an action's tests/*.yaml against golden outputs, or the protocol conformance suite."`
}

// TestPipelineCmd is "millstream test pipeline".
type TestPipelineCmd struct {
	Dir     string        `arg:"" optional:"" type:"path" help:"Package directory (default: the current one)."`
	Timeout time.Duration `default:"60s" help:"How long each test may take to settle."`
}

// pipelineTest is one tests/*.yaml file.
type pipelineTest struct {
	Name    string `yaml:"name"`
	Trigger string `yaml:"trigger"`
	Payload any    `yaml:"payload"`
	Stubs   map[string]struct {
		Outputs []struct {
			Key     string `yaml:"key"`
			Payload any    `yaml:"payload"`
		} `yaml:"outputs"`
		Error *struct {
			Message   string `yaml:"message"`
			Retryable bool   `yaml:"retryable"`
		} `yaml:"error"`
	} `yaml:"stubs"`
	Expect map[string]int `yaml:"expect"`
}

// Run runs each test of the package, in file order, on a fresh temporary store: the test fires its trigger
// (default: the first manual one) with its payload, stubbed steps complete with their stub's outputs or error,
// the other steps run their actions, and once the run settles the item count of each stream under expect is
// compared. It prints PASS or FAIL per test and fails when any test fails.
func (c TestPipelineCmd) Run(rt *Runtime) error {
	pkg, err := pipeline.Load(dirOrCurrent(c.Dir))
	if err != nil {
		return err
	}
	env, err := environment(rt)
	if err != nil {
		return err
	}
	actions := resolver(rt, env, false)
	if diags := pipeline.Check(pkg, actions, rt.Env, checkOptions(env)); pipeline.HasErrors(diags) {
		for _, d := range diags {
			fmt.Fprintf(rt.IO.Err, "%s: %s\n", pkg.Pipeline.Name, d)
		}
		return errFailed
	}
	var files []string
	for name := range pkg.Files {
		if dir, file := path.Split(name); dir == "tests/" && (strings.HasSuffix(file, ".yaml") ||
			strings.HasSuffix(file, ".yml")) {
			files = append(files, name)
		}
	}
	if len(files) == 0 {
		return fmt.Errorf("%s: no tests/*.yaml", pkg.Dir)
	}
	slices.Sort(files)
	// Each test's artifacts go to a directory of its own under one temporary directory, which the artifact store
	// creates on first use.
	artifactsDir, err := os.MkdirTemp("", "millstream-test-artifacts-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(artifactsDir) }()
	failed := 0
	for i, file := range files {
		var tc pipelineTest
		if err := yaml.Unmarshal(pkg.Files[file], &tc); err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
		if tc.Name == "" {
			tc.Name = file
		}
		problems, err := c.runTest(rt, pkg, tc, actions, env, filepath.Join(artifactsDir, strconv.Itoa(i)))
		if err != nil {
			problems = append(problems, err.Error())
		}
		if len(problems) > 0 {
			failed++
			fmt.Fprintf(rt.IO.Out, "FAIL %s\n", tc.Name)
			for _, p := range problems {
				fmt.Fprintf(rt.IO.Out, "  %s\n", p)
			}
			continue
		}
		fmt.Fprintf(rt.IO.Out, "PASS %s\n", tc.Name)
	}
	fmt.Fprintf(rt.IO.Err, "%s: %d passed, %d failed\n", pkg.Pipeline.Name, len(files)-failed, failed)
	if failed > 0 {
		return errFailed
	}
	return nil
}

// runTest runs one test and returns its failed expectations.
func (c TestPipelineCmd) runTest(rt *Runtime, pkg *pipeline.Package, tc pipelineTest,
	actions pipeline.ActionResolver, env *pipeline.Environment, artifactsDir string,
) ([]string, error) {
	p := pkg.Pipeline
	trigger := tc.Trigger
	if trigger == "" {
		var ok bool
		if trigger, ok = p.FirstManualTrigger(); !ok {
			return nil, fmt.Errorf("no trigger given and pipeline %s has no manual trigger", p.Name)
		}
	}
	stubs, err := testStubs(p, tc)
	if err != nil {
		return nil, err
	}
	payload, err := jsonValue(tc.Payload)
	if err != nil {
		return nil, fmt.Errorf("payload: %w", err)
	}

	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = st.Close() }()
	e, err := engine.New(engine.Options{Store: st, Env: rt.Env, NoCron: true, Stubs: stubs,
		Artifacts: artifacts.New(artifactsDir), Resolver: actions, Agents: agents(rt, env)})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rt.Ctx, c.Timeout)
	defer cancel()
	if _, err := e.Apply(ctx, pkg); err != nil {
		return nil, err
	}
	runID, _, err := e.Fire(ctx, p.Name, trigger, "test", payload)
	if err != nil {
		return nil, err
	}
	pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{Launchers: launchers(rt, env)}})
	defer func() { _ = pool.Close() }()
	n, err := node.New(e, node.Options{Owner: "test", Slots: 4, Pool: pool, PullInterval: 20 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	runCtx, stop := context.WithCancel(ctx)
	nodeDone := make(chan error, 1)
	go func() { nodeDone <- n.Run(runCtx) }()
	waitErr := e.WaitSettled(ctx, []engine.RunRef{{Pipeline: p.Name, ID: runID}})
	stop()
	if err := <-nodeDone; err != nil {
		return nil, err
	}
	if waitErr != nil {
		return nil, waitErr
	}
	var problems []string
	for _, stream := range sortedKeys(tc.Expect) {
		items, err := st.Items(rt.Ctx, store.ItemFilter{Pipeline: p.Name, Stream: stream, RunID: runID})
		if err != nil {
			return nil, err
		}
		if want := tc.Expect[stream]; len(items) != want {
			problems = append(problems, fmt.Sprintf("stream %s: %d items, want %d", stream, len(items), want))
		}
	}
	return problems, nil
}

// testStubs turns a test's stubs into the engine's, checking each names a step.
func testStubs(p *pipeline.Pipeline, tc pipelineTest) (map[store.StepRef]engine.Stub, error) {
	stubs := map[store.StepRef]engine.Stub{}
	for _, step := range sortedKeys(tc.Stubs) {
		if _, ok := p.Steps[step]; !ok {
			return nil, fmt.Errorf("stub for step %s: pipeline %s has no such step", step, p.Name)
		}
		s := tc.Stubs[step]
		var stub engine.Stub
		if s.Error != nil {
			stub.Err = &exec.ActionError{Message: s.Error.Message, Retryable: s.Error.Retryable}
		}
		for i, o := range s.Outputs {
			body, err := jsonValue(o.Payload)
			if err != nil {
				return nil, fmt.Errorf("stub for step %s: outputs[%d]: %w", step, i, err)
			}
			stub.Outputs = append(stub.Outputs, store.Output{Key: o.Key, Payload: body})
		}
		stubs[store.StepRef{Pipeline: p.Name, Step: step}] = stub
	}
	return stubs, nil
}

// jsonValue encodes a value decoded from YAML as JSON; nil stays nil (an empty payload).
func jsonValue(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}
