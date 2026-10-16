package node

import (
	"maps"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// A node's capacity is the runtimes it offers (spec section 5.10): exec, container, and agent:<harness> for each
// agent harness it can run. A delivery goes only to a node offering the runtime its action needs (RuntimeOf).

// RuntimeOf is the runtime an action of kind needs, an agent's with its harness; empty for a builtin, which runs in
// the engine whatever node leased it.
func RuntimeOf(a *pipeline.Action) string {
	switch {
	case a == nil || a.Builtin:
		return ""
	case a.Kind == pipeline.KindAgent && a.Agent != nil:
		return pipeline.KindAgent + ":" + a.Agent.Harness
	}
	return a.Kind
}

// Runtimes are the runtimes a machine offers: the kinds it has a launcher for (exec, container), and the agent
// harnesses when it runs the agent adapter at all: api always, claude-code in the sandbox (whose image carries the
// claude CLI) or unsandboxed when claude is on its PATH.
func Runtimes(kinds []string, agents Agents, onPath func(program string) bool) []string {
	out := slices.Clone(kinds)
	if agents.CanRun() {
		out = append(out, pipeline.KindAgent+":"+pipeline.HarnessAPI)
		if agents.Sandbox != nil || onPath("claude") {
			out = append(out, pipeline.KindAgent+":"+pipeline.HarnessClaudeCode)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// Placement is where a step's deliveries may go: a node offering Runtime (any node when it is empty) and carrying
// every one of Labels.
type Placement struct {
	Runtime string
	Labels  map[string]string
}

// String describes what the placement needs, as an unschedulable step reports it: "runtime exec, labels gpu=true".
func (p Placement) String() string {
	var parts []string
	if p.Runtime != "" {
		parts = append(parts, "runtime "+p.Runtime)
	}
	if len(p.Labels) > 0 {
		labels := make([]string, 0, len(p.Labels))
		for _, k := range slices.Sorted(maps.Keys(p.Labels)) {
			labels = append(labels, k+"="+p.Labels[k])
		}
		parts = append(parts, "labels "+strings.Join(labels, ","))
	}
	if len(parts) == 0 {
		return "any runtime"
	}
	return strings.Join(parts, ", ")
}

// Matches reports whether a node offering runtimes and carrying labels can take the step's deliveries.
func (p Placement) Matches(runtimes []string, labels map[string]string) bool {
	if p.Runtime != "" && !slices.Contains(runtimes, p.Runtime) {
		return false
	}
	for k, v := range p.Labels {
		if got, ok := labels[k]; !ok || got != v {
			return false
		}
	}
	return true
}
