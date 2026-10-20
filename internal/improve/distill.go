package improve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/sandbox"
)

// The distill proposal (spec section 11.5): a distillable insight names an agent step; gather-evidence reads the
// training side of the step's split from the engine (which keeps the holdout to itself), the draft agent writes Go
// code with the SDK that may abstain, its unit tests and its evals, and validate adds what is not the agent's to
// write: the action's manifest (the agent's schemas, effects none), its go.mod, the distill.Spec the engine
// recomputes the holdout from, and the pipeline change making the code the step's action with the agent under
// fallback on abstain.

// DistillEvidence is what validate needs back to assemble a distill draft.
type DistillEvidence struct {
	Step string `json:"step"`
	// Agent is the step's action, kept as fallback; Action the new action directory's name.
	Agent  string `json:"agent"`
	Action string `json:"action"`
	// HoldoutFrom is the split's boundary, recorded in the action's distill.Spec.
	HoldoutFrom time.Time `json:"holdoutFrom"`
}

// maxTrainingPairs bounds the training pairs a brief holds.
const maxTrainingPairs = 400

// training reads the training side of an agent step's distill split.
func (c *client) training(ctx context.Context, pipelineName, step string) (api.DistillTraining, error) {
	var out api.DistillTraining
	return out, c.get(ctx, "/pipelines/"+url.PathEscape(pipelineName)+"/steps/"+url.PathEscape(step)+
		"/distill-training", &out)
}

// distillEvidence gathers a distill idea: the step's training set and the brief, nil when the step's history
// cannot be split or the step is not an agent's (an API refusal, logged).
func (a *action) distillEvidence(ctx context.Context, id idea, files api.RevisionFiles) (*DistillEvidence, string,
	error,
) {
	name := id.pipeline()
	t, err := a.api.training(ctx, name, id.step)
	var ae *apiError
	switch {
	case errors.As(err, &ae) && !ae.retryable():
		a.log.Info("no distill training set", "pipeline", name, "step", id.step, "error", ae.msg)
		return nil, "", nil
	case err != nil:
		return nil, "", err
	}
	action := id.step + "-rules"
	for i := 2; hasDir(files.Files, "actions/"+action+"/"); i++ {
		action = fmt.Sprintf("%s-rules-%d", id.step, i)
	}
	de := &DistillEvidence{Step: id.step, Agent: t.Agent, Action: action, HoldoutFrom: t.HoldoutFrom}
	b, err := distillBrief(id, files, t, action)
	return de, b, err
}

// distillBrief is the draft agent's brief for a distill: the insight, the agent's schemas, prompt and lessons, the
// training pairs, and what to write. It holds nothing that changes between two gatherings of the same history (no
// times, no ids), so recordings key on it.
func distillBrief(id idea, files api.RevisionFiles, t api.DistillTraining, action string) (string, error) {
	pkg, err := packageOf(files)
	if err != nil {
		return "", err
	}
	agent, err := pipeline.Resolver{}.Resolve(pkg, t.Agent)
	if err != nil {
		return "", fmt.Errorf("the agent %s: %w", t.Agent, err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Pipeline %s\n\n", id.pipeline())
	writeInsight(&b, id.insight)
	dir := "actions/" + action + "/"
	fmt.Fprintf(&b, "## The step to distill\n\nStep `%s` runs the agent `%s`. Write the Go exec action `%s` that "+
		"computes the agent's output with rules, using the Millstream Go SDK, to become the step's action; the agent "+
		"stays as its fallback for the items the code abstains on. Write these files under `%s`:\n\n"+
		"- `main.go`: package main, calling `millstream.MapKeyed` (import `millstream \"%s\"`) with a function "+
		"`func(c millstream.Context, in In) ([]millstream.Keyed[Out], error)` returning the one output, every field "+
		"the output schema requires, under the key the agent gives it, its input's (`c.Item().Key`), or "+
		"`nil, c.Abstain(\"why\")` for an input the rules are not sure of. Use the standard library only.\n- `main_test.go`: unit tests of the rules.\n- `evals/<case>.yaml`: a few eval cases, each an `input` "+
		"and `assert` (CEL over `output`).\n\nThe manifest (`action.yaml`, the agent's schemas), `go.mod` and the "+
		"pipeline change are written for you: do not write them. The code is validated on recent items you do not "+
		"see: it must agree with the truth on at least 98%% of the items it does not abstain on, and abstain on at "+
		"most 40%%.\n\n", id.step, t.Agent, action, dir, sandbox.SDKModule)
	for _, s := range []struct {
		title  string
		schema []byte
	}{{"input", agent.Input}, {"output", agent.Output}} {
		var v any
		if json.Unmarshal(s.schema, &v) == nil {
			writeJSON(&b, "The agent's "+s.title+" schema", v)
		}
	}
	if agent.Agent != nil {
		if prompt, ok := files.Files[agent.Agent.Prompt]; ok {
			fmt.Fprintf(&b, "## The agent's prompt\n\n````\n%s````\n\n", ensureNewline(prompt))
		}
	}
	if !t.Lessons.Empty() {
		writeJSON(&b, "The step's lessons", t.Lessons)
	}
	n := min(len(t.Training), maxTrainingPairs)
	fmt.Fprintf(&b, "## Training pairs\n\n%d of the step's %d input and output pairs, one a line (the outputs a "+
		"reviewer edited are the edited ones):\n\n```jsonl\n", n, t.Pairs)
	for _, p := range t.Training[:n] {
		line, err := json.Marshal(map[string]any{"input": p.Input, "outputs": p.Outputs})
		if err != nil {
			return "", err
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	b.WriteString("```\n")
	return b.String(), nil
}

// writeInsight writes an insight's section: its evidence without the volatile keys.
func writeInsight(b *strings.Builder, in *api.Insight) {
	if in == nil {
		return
	}
	fmt.Fprintf(b, "## Insight %s: %s of %s, %s\n\n", in.ID, in.Kind, in.Scope, in.Severity)
	var ev map[string]any
	if json.Unmarshal(in.Evidence, &ev) == nil {
		for k := range ev {
			if volatile[k] || k == "proposals" {
				delete(ev, k)
			}
		}
		writeJSON(b, "Evidence", ev)
	}
}

// packageOf parses a revision's files as a package, in memory.
func packageOf(files api.RevisionFiles) (*pipeline.Package, error) {
	m := make(map[string][]byte, len(files.Files))
	for p, c := range files.Files {
		m[p] = []byte(c)
	}
	return pipeline.FromFiles("", m)
}

func hasDir(files map[string]string, prefix string) bool {
	for p := range files {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// written are the files of a distilled action validate writes, which a draft may not.
var written = []string{pipeline.ManifestFile, "go.mod", distill.SpecFile}

// distillFiles assembles a distill draft on the base revision's files: the draft's files under the new action's
// directory, and the manifest, go.mod, spec and pipeline change. The result is every file the patch changes, with
// its whole new content.
func distillFiles(ev Evidence, d Draft, files api.RevisionFiles) (map[string]string, error) {
	de := ev.Distill
	if de == nil {
		return nil, errors.New("the evidence names no step to distill")
	}
	if d.Patch != "" || len(d.Files) == 0 {
		return nil, errors.New("a distill draft gives its files, not a patch")
	}
	dir := "actions/" + de.Action + "/"
	out := map[string]string{}
	hasMain := false
	for _, f := range d.Files {
		rel, ok := strings.CutPrefix(f.Path, dir)
		switch {
		case !ok || rel == "":
			return nil, fmt.Errorf("%s: a distill draft writes only under %s", f.Path, dir)
		case slices.Contains(written, rel):
			return nil, fmt.Errorf("%s: written by the improve pipeline, not the draft", f.Path)
		case path.Ext(rel) == ".go" && !strings.HasSuffix(rel, "_test.go") && strings.Contains(rel, "/"):
			return nil, fmt.Errorf("%s: the action is one package main, its Go files at %s", f.Path, dir)
		}
		hasMain = hasMain || rel == "main.go"
		out[f.Path] = f.Content
	}
	if !hasMain {
		return nil, fmt.Errorf("the draft has no %smain.go", dir)
	}
	pkg, err := packageOf(files)
	if err != nil {
		return nil, err
	}
	agent, err := pipeline.Resolver{}.Resolve(pkg, de.Agent)
	if err != nil {
		return nil, fmt.Errorf("the agent %s: %w", de.Agent, err)
	}
	manifest, err := distillManifest(ev.Pipeline, de, agent)
	if err != nil {
		return nil, err
	}
	gomod, err := sandbox.GoMod(de.Action)
	if err != nil {
		return nil, err
	}
	p, err := withFallback(files.Files[pipeline.FileName], de.Step, de.Agent, "./actions/"+de.Action)
	if err != nil {
		return nil, err
	}
	out[dir+pipeline.ManifestFile] = manifest
	out[dir+"go.mod"] = gomod
	out[dir+distill.SpecFile] = string(distill.Spec{Step: de.Step, Agent: de.Agent, HoldoutFrom: de.HoldoutFrom}.Format())
	out[pipeline.FileName] = p
	return out, nil
}

// distillManifest is the distilled action's manifest: an exec action with no effects (audits need both actions'
// effects to be none), taking and giving what the agent does, run as a bare program of its name.
func distillManifest(pipelineName string, de *DistillEvidence, agent *pipeline.Action) (string, error) {
	schema := func(raw []byte) (string, error) {
		if len(raw) == 0 {
			return "{ type: object }", nil
		}
		var buf bytes.Buffer
		err := json.Compact(&buf, raw)
		return buf.String(), err
	}
	in, err := schema(agent.Input)
	if err != nil {
		return "", err
	}
	out, err := schema(agent.Output)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("# Distilled from %s by the improve pipeline (spec section 11.5): rules that abstain where "+
		"they are unsure,\n# the step handing those items to the agent, its fallback.\nname: %s/%s\nversion: 1.0.0\n"+
		"description: Rules distilled from %s, abstaining where unsure\nkind: exec\neffects: none\ninput: %s\n"+
		"output: %s\nruntime: { exec: { command: [%s] } }\n", de.Agent, pipelineName, de.Action, de.Agent, in, out,
		de.Action), nil
}

// actionLine matches a step's action key on its own line: its indentation and value.
var actionLine = regexp.MustCompile(`^(\s+)action:\s*(\S+)\s*(#.*)?$`)

// withFallback rewrites a pipeline file so that step runs action, with agent as its fallback on abstain: the step's
// block-style action line is replaced and the fallback key added after it, the rest of the file unchanged. The
// result must parse to exactly that.
func withFallback(src, step, agent, action string) (string, error) {
	lines := strings.SplitAfter(src, "\n")
	head := slices.Index(lines, "steps:\n")
	start := -1
	for i := head + 1; head >= 0 && i < len(lines); i++ {
		if strings.TrimRight(lines[i], " \n") == "  "+step+":" {
			start = i
			break
		}
	}
	if start < 0 {
		return "", fmt.Errorf("%s: no block-style step %s under steps", pipeline.FileName, step)
	}
	at := -1
	for i := start + 1; i < len(lines); i++ {
		l := strings.TrimRight(lines[i], "\n")
		if strings.TrimSpace(l) != "" && !strings.HasPrefix(l, "   ") {
			break // the next step or top-level key
		}
		if strings.HasPrefix(strings.TrimSpace(l), "fallback:") {
			return "", fmt.Errorf("step %s has a fallback already", step)
		}
		if m := actionLine.FindStringSubmatch(l); m != nil && len(m[1]) == 4 {
			if m[2] != agent {
				return "", fmt.Errorf("step %s runs %s, not %s", step, m[2], agent)
			}
			at = i
		}
	}
	if at < 0 {
		return "", fmt.Errorf("step %s has no action line", step)
	}
	lines[at] = "    action: " + action + "\n    fallback: { action: " + agent + ", when: [abstain] }\n"
	out := strings.Join(lines, "")
	p, err := pipeline.Parse([]byte(out))
	if err != nil {
		return "", fmt.Errorf("the distilled pipeline file: %w", err)
	}
	if s := p.Steps[step]; s.Action != action || s.Fallback == nil || s.Fallback.Action != agent {
		return "", fmt.Errorf("the distilled pipeline file does not make %s the action of %s", action, step)
	}
	return out, nil
}
