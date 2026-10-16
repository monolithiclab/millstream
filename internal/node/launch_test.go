package node

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

func envMap(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLaunchSpec(t *testing.T) {
	const pkgDir = "/pkg"
	action := &pipeline.Action{Ref: "./actions/x", Dir: "actions/x", Command: []string{"/pkg/actions/x/run", "-v"}}
	resolve := func(string) (*pipeline.Action, string, error) { return action, pkgDir, nil }
	cfg, vars := map[string]any{"a": 1.0}, map[string]any{"v": "w"}
	base := Launch{Action: "lab/x", Version: "1.0.0", Digest: "sha256:d", Ref: "./actions/x", Lifecycle: "long",
		Config: cfg, Vars: vars}
	with := func(f func(*Launch)) Launch {
		l := base
		f(&l)
		return l
	}
	want := func(f func(*exec.Spec)) exec.Spec {
		s := exec.Spec{Action: "lab/x", Version: "1.0.0", Digest: "sha256:d", Lifecycle: "long", Config: cfg, Vars: vars}
		f(&s)
		return s
	}
	price := func(v float64) *float64 { return &v }
	hostEnv := map[string]string{"PATH": "/bin", "HOME": "/home/n", "USER": "n", "SECRET_OF_HOST": "no"}
	harnessEnv := map[string]string{agent.EnvHarnessDir: "/rec", agent.EnvHarnessMode: "replay"}
	sandbox := Agents{Sandbox: &AgentSandbox{Image: "agent:1", Binary: "/bin/ms-linux"}}
	unsandboxed := Agents{AllowUnsandboxed: true, Executable: "/usr/local/bin/millstream"}
	agentArgs := []string{"agent", "--package", pkgDir, "--action", "./actions/x"}
	lessons := json.RawMessage(`[{"lesson":1}]`)
	cwd, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	actionErr := errors.New("no such action")
	passed := &LaunchError{Class: "custom", Err: errors.New("custom")}

	tests := []struct {
		name    string
		launch  Launch
		host    Host
		secrets map[string]string
		want    exec.Spec
		class   string // the LaunchError class wanted, empty for success
		wantErr error  // the error wanted wrapped, nil for any
		warned  bool
	}{
		{
			name: "container",
			launch: with(func(l *Launch) {
				l.Kind, l.Image, l.Network, l.Command = pipeline.KindContainer, "img@sha256:i", true, []string{"run"}
			}),
			host: Host{Action: func(string) (*pipeline.Action, string, error) {
				t.Error("a container action resolved its reference")
				return nil, "", actionErr
			}, Env: envMap(hostEnv)},
			want: want(func(s *exec.Spec) {
				s.Kind, s.Image, s.Network, s.Command = pipeline.KindContainer, "img@sha256:i", true, []string{"run"}
			}),
		},
		{
			name:   "exec",
			launch: with(func(l *Launch) { l.Kind = pipeline.KindExec }),
			host:   Host{Action: resolve, Env: envMap(hostEnv)},
			want: want(func(s *exec.Spec) {
				s.Kind, s.Dir = pipeline.KindExec, filepath.Join(pkgDir, "actions", "x")
				s.Command, s.Env = []string{"/pkg/actions/x/run", "-v"}, []string{"PATH=/bin", "HOME=/home/n"}
			}),
		},
		{
			name:    "action does not resolve",
			launch:  with(func(l *Launch) { l.Kind = pipeline.KindExec }),
			host:    Host{Action: func(string) (*pipeline.Action, string, error) { return nil, "", actionErr }},
			class:   ClassConfig,
			wantErr: actionErr,
		},
		{
			name:    "launch error passes through",
			launch:  with(func(l *Launch) { l.Kind = pipeline.KindAgent }),
			host:    Host{Action: func(string) (*pipeline.Action, string, error) { return nil, "", passed }},
			class:   "custom",
			wantErr: passed,
		},
		{
			name: "agent in the sandbox",
			launch: with(func(l *Launch) {
				l.Kind, l.Network, l.Lessons = pipeline.KindAgent, true, lessons
				l.Agent = &AgentLaunch{Secrets: []string{"apiKey"}, PriceIn: price(3), PriceOut: price(15.5)}
			}),
			host:    Host{Action: resolve, Env: envMap(harnessEnv), Agents: sandbox},
			secrets: map[string]string{"apiKey": "sk-1", "other": "unused"},
			want: want(func(s *exec.Spec) {
				s.Kind, s.Image, s.Network, s.Lessons = pipeline.KindContainer, "agent:1", true, lessons
				s.Command = append([]string{AgentBinaryPath}, append(slices.Clone(agentArgs),
					"--secret", "apiKey", "--price-in", "3", "--price-out", "15.5")...)
				s.Mounts = []exec.Mount{
					{Source: "/bin/ms-linux", Target: AgentBinaryPath, ReadOnly: true},
					{Source: pkgDir, Target: pkgDir, ReadOnly: true},
					{Source: "/rec", Target: "/rec", ReadOnly: true},
				}
				s.Env = []string{"HOME=/tmp", pipeline.SecretEnv("apiKey") + "=sk-1",
					agent.EnvHarnessDir + "=/rec", agent.EnvHarnessMode + "=replay"}
			}),
		},
		{
			name: "agent recording in the sandbox, one price only",
			launch: with(func(l *Launch) {
				l.Kind = pipeline.KindAgent
				l.Agent = &AgentLaunch{PriceIn: price(3)}
			}),
			host: Host{Action: resolve, Agents: sandbox,
				Env: envMap(map[string]string{agent.EnvHarnessDir: "rec", agent.EnvHarnessMode: agent.ModeRecord})},
			want: want(func(s *exec.Spec) {
				s.Kind, s.Image = pipeline.KindContainer, "agent:1"
				s.Command = append([]string{AgentBinaryPath}, agentArgs...)
				s.Mounts = []exec.Mount{
					{Source: "/bin/ms-linux", Target: AgentBinaryPath, ReadOnly: true},
					{Source: pkgDir, Target: pkgDir, ReadOnly: true},
					{Source: filepath.Join(cwd, "rec"), Target: filepath.Join(cwd, "rec")},
				}
				s.Env = []string{"HOME=/tmp", agent.EnvHarnessDir + "=" + filepath.Join(cwd, "rec"),
					agent.EnvHarnessMode + "=" + agent.ModeRecord}
			}),
		},
		{
			name:   "agent unsandboxed",
			launch: with(func(l *Launch) { l.Kind = pipeline.KindAgent }),
			host:   Host{Action: resolve, Env: envMap(hostEnv), Agents: unsandboxed},
			want: want(func(s *exec.Spec) {
				s.Kind, s.Dir = pipeline.KindExec, filepath.Join(pkgDir, "actions", "x")
				s.Command = append([]string{"/usr/local/bin/millstream"}, agentArgs...)
				s.Env = []string{"PATH=/bin", "HOME=/home/n", "USER=n"}
			}),
			warned: true,
		},
		{
			name:   "agent without a sandbox",
			launch: with(func(l *Launch) { l.Kind = pipeline.KindAgent }),
			host:   Host{Action: resolve, Env: envMap(hostEnv), Agents: Agents{AllowUnsandboxed: true}},
			class:  ClassSandbox,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs syncBuffer
			tt.host.Log = newJSONLogger(&logs)
			got, err := tt.launch.Spec(tt.host, tt.secrets)
			if tt.class != "" {
				var le *LaunchError
				if !errors.As(err, &le) || le.Class != tt.class || le.Retryable {
					t.Fatalf("err = %v, want a LaunchError of class %s", err, tt.class)
				}
				if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want it to wrap %v", err, tt.wantErr)
				}
				if le.Error() != le.Err.Error() || le.Unwrap() != le.Err {
					t.Fatalf("LaunchError: Error %q, Unwrap %v", le.Error(), le.Unwrap())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("spec\n got %+v\nwant %+v", got, tt.want)
			}
			if warned := strings.Contains(logs.String(), "without a sandbox"); warned != tt.warned {
				t.Fatalf("warned = %v, logs %q", warned, logs.String())
			}
		})
	}
	// A nil logger discards the unsandboxed warning.
	if _, err := with(func(l *Launch) { l.Kind = pipeline.KindAgent }).Spec(
		Host{Action: resolve, Env: envMap(nil), Agents: unsandboxed}, nil); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		agents Agents
		want   bool
	}{
		{Agents{}, false},
		{sandbox, true},
		{unsandboxed, true},
		{Agents{AllowUnsandboxed: true}, false},
		{Agents{Executable: "/usr/local/bin/millstream"}, false},
	} {
		if got := tt.agents.CanRun(); got != tt.want {
			t.Errorf("%+v.CanRun() = %v", tt.agents, got)
		}
	}
}

func TestRuntimesAndPlacement(t *testing.T) {
	for _, tt := range []struct {
		action *pipeline.Action
		want   string
	}{
		{nil, ""},
		{&pipeline.Action{Builtin: true, Kind: pipeline.KindExec}, ""},
		{&pipeline.Action{Kind: pipeline.KindExec}, "exec"},
		{&pipeline.Action{Kind: pipeline.KindContainer}, "container"},
		{&pipeline.Action{Kind: pipeline.KindAgent, Agent: &pipeline.AgentRuntime{Harness: pipeline.HarnessClaudeCode}},
			"agent:claude-code"},
		{&pipeline.Action{Kind: pipeline.KindAgent}, "agent"},
	} {
		if got := RuntimeOf(tt.action); got != tt.want {
			t.Errorf("RuntimeOf(%+v) = %q, want %q", tt.action, got, tt.want)
		}
	}

	claudeOnPath := func(p string) bool { return p == "claude" }
	nothingOnPath := func(string) bool { return false }
	unsandboxed := Agents{AllowUnsandboxed: true, Executable: "/usr/local/bin/millstream"}
	for _, tt := range []struct {
		name   string
		kinds  []string
		agents Agents
		onPath func(string) bool
		want   []string
	}{
		{"no agents", []string{"exec", "container"}, Agents{}, claudeOnPath, []string{"container", "exec"}},
		{"sandbox", []string{"exec"}, Agents{Sandbox: &AgentSandbox{}}, nothingOnPath,
			[]string{"agent:api", "agent:claude-code", "exec"}},
		{"unsandboxed with claude", []string{"exec"}, unsandboxed, claudeOnPath,
			[]string{"agent:api", "agent:claude-code", "exec"}},
		{"unsandboxed without claude", []string{"exec"}, unsandboxed, nothingOnPath, []string{"agent:api", "exec"}},
		{"deduplicated", []string{"exec", "agent:api", "exec"}, unsandboxed, nothingOnPath,
			[]string{"agent:api", "exec"}},
	} {
		if got := Runtimes(tt.kinds, tt.agents, tt.onPath); !slices.Equal(got, tt.want) {
			t.Errorf("%s: Runtimes = %v, want %v", tt.name, got, tt.want)
		}
	}

	gpu := Placement{Runtime: "exec", Labels: map[string]string{"zone": "eu", "gpu": "true"}}
	for _, tt := range []struct {
		p    Placement
		want string
	}{
		{Placement{}, "any runtime"},
		{Placement{Runtime: "exec"}, "runtime exec"},
		{Placement{Labels: map[string]string{"gpu": "true"}}, "labels gpu=true"},
		{gpu, "runtime exec, labels gpu=true,zone=eu"},
	} {
		if got := tt.p.String(); got != tt.want {
			t.Errorf("%+v.String() = %q, want %q", tt.p, got, tt.want)
		}
	}
	for _, tt := range []struct {
		p        Placement
		runtimes []string
		labels   map[string]string
		want     bool
	}{
		{Placement{}, nil, nil, true},
		{gpu, []string{"container", "exec"}, map[string]string{"gpu": "true", "zone": "eu", "extra": "x"}, true},
		{gpu, []string{"container"}, map[string]string{"gpu": "true", "zone": "eu"}, false},
		{gpu, []string{"exec"}, map[string]string{"gpu": "true"}, false},
		{gpu, []string{"exec"}, map[string]string{"gpu": "false", "zone": "eu"}, false},
	} {
		if got := tt.p.Matches(tt.runtimes, tt.labels); got != tt.want {
			t.Errorf("%v.Matches(%v, %v) = %v", tt.p, tt.runtimes, tt.labels, got)
		}
	}
}
