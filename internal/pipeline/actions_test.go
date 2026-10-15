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

func TestResolve(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeTree(t, dir, [][2]string{
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
		{"actions/agent/action.yaml", "name: g\nkind: agent\n"},
		{"actions/container/action.yaml", "name: c\nkind: container\nlifecycle: perItem\n"},
	})
	pkg, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := Resolver{Platform: "plan9-mips"}
	tests := []struct {
		ref  string
		want Action
	}{
		{"filter", Action{Ref: "filter", Builtin: true, Name: "filter"}},
		{"./actions/a", Action{
			Ref: "./actions/a", Name: "lab/a", Version: "1.0.0", Description: "An action", Kind: KindExec,
			Effects: EffectsExternal, Lifecycle: LifecycleLong, Dir: "actions/a",
			Input: []byte(`{"required":["url"],"type":"object"}` + "\n"), Output: []byte(`{"type":"object"}` + "\n"),
			Command: []string{filepath.Join(dir, "actions", "a", "bin", "plan9-mips", "score"), "--fast"},
		}},
		{"./actions/path/", Action{
			Ref: "./actions/path/", Name: "p", Kind: KindExec, Lifecycle: LifecycleLong, Dir: "actions/path",
			Command: []string{"python3", "main.py"},
		}},
		{"./actions/script", Action{
			Ref: "./actions/script", Name: "s", Kind: KindExec, Lifecycle: LifecycleLong, Dir: "actions/script",
			Command: []string{filepath.Join(dir, "actions", "script", "run.sh")},
		}},
		{"./actions/agent", Action{Ref: "./actions/agent", Name: "g", Kind: KindAgent, Lifecycle: LifecyclePerItem, Dir: "actions/agent"}},
		{"./actions/container", Action{Ref: "./actions/container", Name: "c", Kind: KindContainer, Lifecycle: LifecyclePerItem, Dir: "actions/container"}},
	}
	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			t.Parallel()
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
		t.Parallel()
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
		{"schema file missing", "./actions/x", "name: x\nkind: agent\ninput: { $ref: in.json }\n", nil,
			"input: schema file actions/x/in.json is not in the package"},
		{"schema file invalid", "./actions/x", "name: x\nkind: agent\noutput: { $ref: out.json }\n",
			[][2]string{{"actions/x/out.json", "{"}}, "output: schema file actions/x/out.json"},
		{"schema does not compile", "./actions/x", "name: x\nkind: agent\nconfig: { type: 3 }\n", nil, "config: compile schema"},
		{"remote ref refused", "./actions/x", "name: x\nkind: agent\nconfig: { properties: { a: { $ref: 'https://x/s.json' } } }\n",
			nil, "is refused"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			files := [][2]string{{"pipeline.yaml", minimalPipeline}, {"actions/x/action.yaml", tt.manifest}}
			writeTree(t, dir, append(files, tt.extra...))
			pkg, err := Load(dir)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Resolver{}.Resolve(pkg, tt.ref)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}
