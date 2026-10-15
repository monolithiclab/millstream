package engine

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

const agentManifest = `name: lab/score
version: 1.0.0
kind: agent
output: { type: object, properties: { score: { type: integer } }, required: [score] }
runtime: { agent: { harness: api, model: claude-sonnet-5-5, apiKeySecret: key, network: [api.anthropic.com] } }
`

// agentAssignment applies a package with the agent step score and returns its assignment.
func agentAssignment(t *testing.T, agents Agents, environ map[string]string) (node.Assignment, *env) {
	t.Helper()
	v, _ := agentRun(t, agents, environ, nil, "")
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("Pull = %v, %v", as, err)
	}
	return as[0], v
}

// agentRun applies a package with the agent step score, its files plus extra under actions/score and lessons, and
// the top-level keys settings (budget), and fires a run.
func agentRun(t *testing.T, agents Agents, environ, extra map[string]string, settings string) (*env, string) {
	t.Helper()
	v := newEnv(t, Options{Env: func(k string) (string, bool) { s, ok := environ[k]; return s, ok }, Agents: agents})
	pkg := writePackage(t, "pipeline: p\nsecrets: [key]\n"+settings+"triggers:\n  adhoc: { manual: {} }\nsteps:\n"+
		"  score: { action: ./actions/score, in: trigger.adhoc, config: { rooms: 2 } }\n")
	files := map[string]string{"actions/score/action.yaml": agentManifest, "actions/score/prompt.md": "Score {{ .Payload }}.\n"}
	for name, body := range extra {
		files[name] = body
	}
	for name, body := range files {
		// On disk as well: the adapter reads the action from the package directory.
		pkg.Files[name] = []byte(body)
		p := filepath.Join(pkg.Dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := files["lessons/score.yaml"]; ok {
		s := pkg.Pipeline.Steps["score"]
		s.Lessons = "lessons/score.yaml" // as Load sets it when the file exists
		pkg.Pipeline.Steps["score"] = s
	}
	v.apply(pkg)
	return v, v.fire("p", "adhoc", "k")
}

// TestAgentAssignmentLessons: the step's lessons file reaches the adapter in hello, as JSON; one that does not
// parse fails the delivery as a config error, not retried.
func TestAgentAssignmentLessons(t *testing.T) {
	t.Parallel()
	agents := Agents{AllowUnsandboxed: true, Executable: "/usr/local/bin/millstream"}
	environ := map[string]string{"MILLSTREAM_SECRET_KEY": "sk-0123456789"}
	v, _ := agentRun(t, agents, environ, map[string]string{"lessons/score.yaml": "rules: [{ text: Be strict. }]\n"}, "")
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 || string(as[0].Spec.Lessons) != `{"rules":[{"text":"Be strict."}]}` {
		t.Fatalf("Pull = %+v, %v", as, err)
	}

	v, id := agentRun(t, agents, environ, map[string]string{"lessons/score.yaml": "rules: 3\n"}, "")
	if got := v.pull(1); len(got) != 0 {
		t.Fatalf("assignments = %v", got)
	}
	if d := v.deliveries(id)[0]; d.ErrorClass != ClassConfig || !strings.Contains(d.ErrorMessage, "rules: must be a list") {
		t.Fatalf("delivery %+v", d)
	}
}

// TestAgentAssignmentSandboxed: with a sandbox, the adapter runs in its container with the binary and the package
// mounted read-only, the manifest's network, the secrets passed by environment and the harness variables, its
// recordings directory made absolute.
func TestAgentAssignmentSandboxed(t *testing.T) {
	t.Parallel()
	environ := map[string]string{"MILLSTREAM_SECRET_KEY": "sk-0123456789", "PATH": "/usr/bin",
		agent.EnvHarnessMode: agent.ModeReplay, agent.EnvHarnessDir: "testdata/recordings"}
	a, _ := agentAssignment(t, Agents{
		Sandbox: &AgentSandbox{Image: "agents:1", Binary: "/opt/millstream-linux"},
		Prices:  map[string]pipeline.Price{"claude-sonnet-5-5": {InPerMtok: 3, OutPerMtok: 15}},
	}, environ)
	s := a.Spec
	recordings, _ := filepath.Abs("testdata/recordings")
	pkgDir := s.Mounts[1].Source
	wantCmd := []string{AgentBinaryPath, "agent", "--package", pkgDir, "--action", "./actions/score", "--secret", "key",
		"--price-in", "3", "--price-out", "15"}
	wantMounts := []exec.Mount{{Source: "/opt/millstream-linux", Target: AgentBinaryPath, ReadOnly: true},
		{Source: pkgDir, Target: pkgDir, ReadOnly: true}, {Source: recordings, Target: recordings, ReadOnly: true}}
	wantEnv := []string{"HOME=/tmp", "MILLSTREAM_SECRET_KEY=sk-0123456789", agent.EnvHarnessDir + "=" + recordings,
		agent.EnvHarnessMode + "=replay"}
	if s.Kind != pipeline.KindContainer || s.Image != "agents:1" || !s.Network || s.Lifecycle != pipeline.LifecyclePerItem ||
		!slices.Equal(s.Command, wantCmd) || !slices.Equal(s.Mounts, wantMounts) || !slices.Equal(s.Env, wantEnv) ||
		s.Config["rooms"] != int64(2) || s.Action != "lab/score" || !strings.HasPrefix(s.Digest, "sha256:") {
		t.Fatalf("spec = %+v", s)
	}
	if _, err := os.Stat(filepath.Join(pkgDir, "actions", "score", "prompt.md")); err != nil {
		t.Fatalf("the mounted package: %v", err)
	}
}

// TestAgentAssignmentUnsandboxed: allowed by the environment file, the adapter runs as this binary's agent
// subcommand in the action directory, with the engine's PATH, HOME and TMPDIR only, logged at every start; a
// recordings directory being recorded is mounted writable in a sandbox.
func TestAgentAssignmentUnsandboxed(t *testing.T) {
	t.Parallel()
	environ := map[string]string{"MILLSTREAM_SECRET_KEY": "sk-0123456789", "PATH": "/usr/bin", "OTHER": "x"}
	a, v := agentAssignment(t, Agents{AllowUnsandboxed: true, Executable: "/usr/local/bin/millstream"}, environ)
	s := a.Spec
	if s.Kind != pipeline.KindExec || s.Command[0] != "/usr/local/bin/millstream" || s.Command[1] != "agent" ||
		slices.Contains(s.Command, "--price-in") || filepath.Base(s.Dir) != "score" || s.Mounts != nil ||
		!slices.Equal(s.Env, []string{"PATH=/usr/bin", "MILLSTREAM_SECRET_KEY=sk-0123456789"}) {
		t.Fatalf("spec = %+v", s)
	}
	if !strings.Contains(v.logs.String(), "agent running without a sandbox") {
		t.Fatalf("logs: %s", v.logs.String())
	}

	environ[agent.EnvHarnessMode], environ[agent.EnvHarnessDir] = agent.ModeRecord, "/rec"
	a, _ = agentAssignment(t, Agents{Sandbox: &AgentSandbox{Image: "agents:1", Binary: "/b"}}, environ)
	if m := a.Spec.Mounts[2]; m.Source != "/rec" || m.ReadOnly {
		t.Fatalf("recording mount %+v", m)
	}
}

// TestAgentBudgetReservation: the engine reserves the agent action's maxCostUsd on each lease, and the reported
// cost replaces the reservation; with a day budget of one reservation, the next delivery is not leased and the
// agent step is paused with its cause.
func TestAgentBudgetReservation(t *testing.T) {
	t.Parallel()
	agents := Agents{AllowUnsandboxed: true, Executable: "/usr/local/bin/millstream"}
	environ := map[string]string{"MILLSTREAM_SECRET_KEY": "sk-0123456789"}
	manifest := strings.Replace(agentManifest, "apiKeySecret: key", "apiKeySecret: key, maxCostUsd: 0.5", 1)
	v, id := agentRun(t, agents, environ, map[string]string{"actions/score/action.yaml": manifest},
		"budget: { dayUsd: 0.5 }\n")
	v.fire("p", "adhoc", "k2")
	as, err := v.e.Pull(v.ctx, "node-a", 2)
	if err != nil || len(as) != 1 || as[0].Lease.RunID != id {
		t.Fatalf("Pull = %+v, %v", as, err)
	}
	if d := v.deliveries(id)[0]; d.ReservedUSD != 0.5 {
		t.Fatalf("delivery = %+v", d)
	}
	l := as[0].Lease
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, CostUSD: 0.125,
		Outputs: []store.Output{{Payload: []byte(`{"score":1}`)}}}); err != nil {
		t.Fatal(err)
	}
	if d := v.deliveries(id)[0]; d.ReservedUSD != 0 || d.CostUSD != 0.125 {
		t.Fatalf("delivery = %+v", d)
	}
	if as, err := v.e.Pull(v.ctx, "node-a", 2); err != nil || len(as) != 0 {
		t.Fatalf("Pull past the budget = %+v, %v", as, err)
	}
	ps, err := v.st.PipelineState(v.ctx, "p")
	if err != nil || ps.PauseCauses["score"] != "budget.day" || len(v.events(store.EventBudgetReached)) != 1 {
		t.Fatalf("state = %+v, %v", ps, err)
	}
}
