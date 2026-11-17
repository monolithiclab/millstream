package pipeline

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// fakeRegistry stands in for the M2 registry resolver.
type fakeRegistry struct{}

func (fakeRegistry) Resolve(_ *Package, ref string) (*Action, error) {
	return &Action{Ref: ref, Name: "lab/x", Kind: KindContainer}, nil
}

// agentWith is an api agent manifest with an output schema and one more runtime.agent entry.
func agentWith(entry string) string {
	return "name: x\nkind: agent\noutput: { type: object }\nruntime:\n  agent:\n    harness: api\n    model: m\n    " +
		entry + "\n"
}

// TestAgentUnavailable: what a later milestone brings is unavailable, which check reports as a warning.
func TestAgentUnavailable(t *testing.T) {
	t.Parallel()
	pkg := memPackage(t, [][2]string{{"pipeline.yaml", minimalPipeline}, {"actions/x/action.yaml", agentWith("mcp: [x]")},
		{"actions/x/prompt.md", "p"}}...)
	if _, err := (Resolver{}).Resolve(pkg, "./actions/x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
}

func TestResolve(t *testing.T) {
	t.Parallel()
	pkg := memPackage(t, [][2]string{
		{"pipeline.yaml", minimalPipeline},
		{"actions/a/action.yaml", `name: lab/a
version: 1.0.0
description: An action
kind: exec
effects: external
input: { $ref: schema/in.yaml }
output: { type: object }
runtime: { exec: { command: [score, --fast] } }
`},
		{"actions/a/schema/in.yaml", "type: object\nrequired: [url]\n"},
		{"actions/a/bin/plan9-mips/score", "binary"},
		{"actions/path/action.yaml", "name: p\nkind: exec\nruntime: { exec: { command: [python3, main.py] } }\n"},
		{"actions/script/action.yaml", "name: s\nkind: exec\nruntime: { exec: { command: [./run.sh] } }\n"},
		{"actions/script/run.sh", "#!/bin/sh\n"},
		{"actions/agent/action.yaml", "name: g\nkind: agent\noutput: { type: object }\n" +
			"runtime: { agent: { harness: claude-code, model: m, prompt: prompts/score.md, tools: [WebFetch], " +
			"maxCostUsd: 0.5 } }\n"},
		{"actions/agent/prompts/score.md", "Score it."},
		{"actions/container/action.yaml", "name: c\nkind: container\nlifecycle: perItem\n" +
			"runtime: { container: { image: report:dev, network: [api.example.com] } }\n"},
	}...)
	r := Resolver{Platform: "plan9-mips", Images: testImages}
	tests := []struct {
		ref  string
		want Action
	}{
		{"filter", Action{Ref: "filter", Builtin: true, Name: "filter"}},
		{"./actions/a", Action{
			Ref: "./actions/a", Name: "lab/a", Version: "1.0.0", Description: "An action", Kind: KindExec,
			Effects: EffectsExternal, Lifecycle: LifecycleLong, Dir: "actions/a",
			Input: []byte(`{"required":["url"],"type":"object"}` + "\n"), Output: []byte(`{"type":"object"}` + "\n"),
			Command: []string{filepath.Join(pkg.Dir, "actions", "a", "bin", "plan9-mips", "score"), "--fast"},
		}},
		{"./actions/path/", Action{
			Ref: "./actions/path/", Name: "p", Kind: KindExec, Lifecycle: LifecycleLong, Dir: "actions/path",
			Command: []string{"python3", "main.py"},
		}},
		{"./actions/script", Action{
			Ref: "./actions/script", Name: "s", Kind: KindExec, Lifecycle: LifecycleLong, Dir: "actions/script",
			Command: []string{filepath.Join(pkg.Dir, "actions", "script", "run.sh")},
		}},
		{"./actions/agent", Action{Ref: "./actions/agent", Name: "g", Kind: KindAgent, Lifecycle: LifecyclePerItem,
			Dir: "actions/agent", Output: []byte(`{"type":"object"}` + "\n"), Agent: &AgentRuntime{
				Harness: HarnessClaudeCode, Model: "m", Prompt: "actions/agent/prompts/score.md", Tools: []string{"WebFetch"},
				MaxTurns: DefaultMaxTurns, RepairTurns: DefaultRepairTurns, MaxCostUSD: 0.5,
			}}},
		{"./actions/container", Action{
			Ref: "./actions/container", Name: "c", Kind: KindContainer, Lifecycle: LifecyclePerItem,
			Dir: "actions/container", Image: "report:dev", Pin: idReport, ImageDigest: idReport, Network: true,
			Unpinned: true,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got, err := r.Resolve(pkg, tt.ref)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*got, tt.want) {
				t.Fatalf("got  %+v\nwant %+v", *got, tt.want)
			}
		})
	}

	t.Run("registry", func(t *testing.T) {
		if _, err := r.Resolve(pkg, "lab/x@1"); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("err = %v, want ErrUnavailable", err)
		}
		got, err := Resolver{Registry: fakeRegistry{}}.Resolve(pkg, "lab/x@1")
		if err != nil || got.Kind != KindContainer {
			t.Fatalf("got %+v, %v", got, err)
		}
	})
}

func TestResolveErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		ref      string
		manifest string
		extra    [][2]string
		want     string
	}{
		{"unknown", "nope", "", nil, `unknown action "nope"`},
		{"registry without version", "lab/x", "", nil, `unknown action "lab/x"`},
		{"escapes the package", "./../x", "", nil, "must be a directory inside the package"},
		{"package root", "./", "", nil, "must be a directory inside the package"},
		{"no manifest", "./actions/none", "", nil, "no actions/none/action.yaml in the package"},
		{"bad YAML", "./actions/x", "name: [", nil, "parse YAML"},
		{"empty", "./actions/x", "", nil, "empty manifest"},
		{"not a mapping", "./actions/x", "- a\n", nil, "must be a mapping"},
		{"unknown key", "./actions/x", "name: x\nkind: exec\nimage: y\n", nil, "image: unknown key"},
		{"missing name", "./actions/x", "kind: exec\n", nil, "name: is required"},
		{"bad kind", "./actions/x", "name: x\nkind: wasm\n", nil, "kind: must be one of exec, container, agent"},
		{"bad effects", "./actions/x", "name: x\nkind: agent\neffects: some\n", nil, "effects: must be one of none, external"},
		{"bad lifecycle", "./actions/x", "name: x\nkind: agent\nlifecycle: daily\n", nil, "lifecycle: must be one of long, perItem"},
		{"kind not a string", "./actions/x", "name: x\nkind: [exec]\n", nil, "kind: must be a string"},
		{"schema not a mapping", "./actions/x", "name: x\nkind: agent\ninput: in.json\n", nil, "input: must be a mapping"},
		{"exec without command", "./actions/x", "name: x\nkind: exec\n", nil, "an exec action needs a command"},
		{"command not strings", "./actions/x", "name: x\nkind: exec\nruntime: { exec: { command: [1] } }\n", nil,
			"runtime.exec.command[0]: must be a non-empty string"},
		{"command missing", "./actions/x", "name: x\nkind: exec\nruntime: { exec: { command: [./gone] } }\n", nil,
			"actions/x/gone is not in the package"},
		{"command absolute", "./actions/x", "name: x\nkind: exec\nruntime: { exec: { command: [/usr/bin/python3] } }\n",
			nil, "/usr/bin/python3 is an absolute path"},
		{"command outside the package", "./actions/x",
			"name: x\nkind: exec\nruntime: { exec: { command: [../../../outside] } }\n", nil,
			"../../../outside leaves the package"},
		{"schema file missing", "./actions/x", "name: x\nkind: container\ninput: { $ref: in.json }\n", nil,
			"input: schema file actions/x/in.json is not in the package"},
		{"schema file invalid", "./actions/x", "name: x\nkind: container\noutput: { $ref: out.json }\n",
			[][2]string{{"actions/x/out.json", "{"}}, "output: schema file actions/x/out.json"},
		{"schema does not compile", "./actions/x", "name: x\nkind: container\nconfig: { type: 3 }\n", nil, "config: compile schema"},
		{"agent without runtime", "./actions/x", "name: x\nkind: agent\n", nil, "an agent action needs runtime.agent"},
		{"agent unknown key", "./actions/x", agentWith("temperature: 1"), nil, "runtime.agent.temperature: unknown key"},
		{"agent no harness", "./actions/x", "name: x\nkind: agent\nruntime: { agent: { model: m } }\n", nil,
			"runtime.agent.harness: is required"},
		{"agent bad harness", "./actions/x", "name: x\nkind: agent\nruntime: { agent: { harness: gpt, model: m } }\n",
			nil, "runtime.agent.harness: must be one of claude-code, api"},
		{"agent empty model", "./actions/x", "name: x\nkind: agent\nruntime: { agent: { harness: api, model: \"\" } }\n",
			nil, "runtime.agent.model: must be a non-empty string"},
		{"agent tools", "./actions/x", agentWith("tools: WebFetch"), nil, "runtime.agent.tools: must be a list"},
		{"agent max turns", "./actions/x", agentWith("maxTurns: 0"), nil, "runtime.agent.maxTurns: must be a positive"},
		{"agent max turns float", "./actions/x", agentWith("maxTurns: 1.5"), nil, "runtime.agent.maxTurns"},
		{"agent repair turns", "./actions/x", agentWith("repairTurns: -1"), nil,
			"runtime.agent.repairTurns: must be a non-negative integer"},
		{"agent repair turns float", "./actions/x", agentWith("repairTurns: 1.5"), nil, "runtime.agent.repairTurns"},
		{"agent max cost", "./actions/x", agentWith("maxCostUsd: 0"), nil, "runtime.agent.maxCostUsd: must be a positive"},
		{"agent max cost string", "./actions/x", agentWith("maxCostUsd: cheap"), nil, "runtime.agent.maxCostUsd"},
		{"agent bad provider", "./actions/x", agentWith("provider: azure"), nil, "runtime.agent.provider: must be one of"},
		{"provider on claude-code", "./actions/x",
			"name: x\nkind: agent\nruntime: { agent: { harness: claude-code, model: m, provider: openai } }\n", nil,
			"provider and baseUrl configure the api harness only"},
		{"openai without base", "./actions/x", agentWith("provider: openai"), nil, "the openai provider needs"},
		{"base not a URL", "./actions/x", agentWith("baseUrl: api.example.com"), nil, "must be an http or https URL"},
		{"prompt missing", "./actions/x", agentWith(""), nil, "runtime.agent.prompt: actions/x/prompt.md is not in the package"},
		{"prompt outside", "./actions/x", agentWith("prompt: ../../../p.md"), nil, "../../../p.md leaves the package"},
		{"no output schema", "./actions/x", "name: x\nkind: agent\nruntime: { agent: { harness: api, model: m } }\n",
			[][2]string{{"actions/x/prompt.md", "p"}}, "an agent action declares its output schema"},
		{"mcp", "./actions/x", agentWith("mcp: [github]"), [][2]string{{"actions/x/prompt.md", "p"}},
			"MCP servers arrive in a later milestone"},
		{"api tools", "./actions/x", agentWith("tools: [WebFetch]"), [][2]string{{"actions/x/prompt.md", "p"}},
			"api harness tools arrive later"},
		{"remote ref refused", "./actions/x", "name: x\nkind: container\nconfig: { properties: { a: { $ref: 'https://x/s.json' } } }\n",
			nil, "is refused"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files := [][2]string{{"pipeline.yaml", minimalPipeline}, {"actions/x/action.yaml", tt.manifest}}
			pkg := memPackage(t, append(files, tt.extra...)...)
			_, err := Resolver{}.Resolve(pkg, tt.ref)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}
