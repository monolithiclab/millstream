//go:build e2e

package engine

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/pipeline"
)

// Variants the e2e build adds: the default build covers the same paths through the other tests.

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

// TestAgentAssignmentUnsandboxed: allowed by the environment file, the adapter runs as this binary's agent
// subcommand in the action directory, with the engine's variables the claude CLI needs (agent.ClaudeEnv, USER
// included) only, logged at every start; a recordings directory being recorded is mounted writable in a sandbox.
func TestAgentAssignmentUnsandboxed(t *testing.T) {
	environ := map[string]string{"MILLSTREAM_SECRET_KEY": "sk-0123456789", "PATH": "/usr/bin", "OTHER": "x",
		"USER": "u", "CLAUDE_CONFIG_DIR": "/c"}
	a, v := agentAssignment(t, Agents{AllowUnsandboxed: true, Executable: "/usr/local/bin/millstream"}, environ)
	s := a.Spec
	if s.Kind != pipeline.KindExec || s.Command[0] != "/usr/local/bin/millstream" || s.Command[1] != "agent" ||
		slices.Contains(s.Command, "--price-in") || filepath.Base(s.Dir) != "score" || s.Mounts != nil ||
		!slices.Equal(s.Env, []string{"PATH=/usr/bin", "USER=u", "CLAUDE_CONFIG_DIR=/c",
			"MILLSTREAM_SECRET_KEY=sk-0123456789"}) {
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
