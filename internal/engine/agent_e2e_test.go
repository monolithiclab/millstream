//go:build e2e

package engine

import (
	"strings"
	"testing"
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
