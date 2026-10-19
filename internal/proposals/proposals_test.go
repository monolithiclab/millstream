package proposals

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// basePipeline is the base revision's pipeline file: line 6 names the score step's action.
const basePipeline = `pipeline: listings
triggers:
  adhoc: { manual: {} }
steps:
  score:
    action: ./actions/scorer
    in: trigger.adhoc
    out: [scored]
`

// manifest is an exec action running the fake action, whose rules mode (exectest) emits one output as an action
// named anything but *-fallback and a second one, on a disagreeing input, under such a name.
func manifest(name string) []string {
	return []string{"name: " + name, "version: 1.0.0", "kind: exec", `runtime: { exec: { command: ["fakeaction"] } }`}
}

func basePackage(t *testing.T) *pipeline.Package {
	t.Helper()
	pkg, err := pipeline.FromFiles("", baseFiles())
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

// baseFiles are the base revision's files.
func baseFiles() map[string][]byte {
	return map[string][]byte{
		"pipeline.yaml":                       []byte(basePipeline),
		"actions/scorer/action.yaml":          []byte(strings.Join(manifest("scorer"), "\n") + "\n"),
		"actions/scorer/prompt.md":            []byte("Score the listing.\n"),
		"actions/scorer/evals/disagree.yaml":  []byte("config: { mode: rules }\ninput: { disagree: true, score: 2 }\nassert: [output.score == 2]\n"),
		"actions/scorer/evals/one-score.yaml": []byte("config: { mode: rules }\ninput: {}\nassert: [output.score == 1]\n"),
	}
}

// distill is the distill patch moving the score step to a new action of that name.
func distill(name string, extra ...string) string {
	return edit("pipeline.yaml", 6, "  score:", "    action: ./actions/scorer", "    action: ./actions/"+name,
		"    in: trigger.adhoc") + newFile("actions/"+name+"/action.yaml", manifest(name)...) + strings.Join(extra, "")
}

var (
	lessonsPatch = newFile("lessons/score.yaml", "rules:", "  - text: Score 0 without a surface.")
	promptPatch  = "--- a/actions/scorer/prompt.md\n+++ b/actions/scorer/prompt.md\n@@ -1 +1 @@\n" +
		"-Score the listing.\n+Score the listing from 0 to 100.\n"
	configPatch = "--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -7,2 +7,3 @@\n     in: trigger.adhoc\n" +
		"+    concurrency: 2\n     out: [scored]\n"
	// brokenPatch reads a stream nothing writes: check fails.
	brokenPatch = edit("pipeline.yaml", 7, "    action: ./actions/scorer", "    in: trigger.adhoc", "    in: nowhere",
		"    out: [scored]")
)

// Each kind touches only its paths (spec section 11.7), and a prepared patch names the steps it touches.
func TestPrepare(t *testing.T) {
	base := basePackage(t)
	tests := []struct {
		kind  store.ProposalKind
		patch string
		steps []string
		why   string // the refusal, "" when allowed
	}{
		{store.ProposalLessons, lessonsPatch, []string{"score"}, ""},
		{store.ProposalLessons, newFile("lessons/nope.yaml", "rules: []"), nil, "a step of the pipeline"},
		{store.ProposalLessons, newFile("lessons/score.yml", "rules: []"), nil, "lessons/<step>.yaml"},
		{store.ProposalLessons, configPatch, nil, "pipeline.yaml"},
		{store.ProposalPrompt, promptPatch, []string{"score"}, ""},
		{store.ProposalPrompt, newFile("actions/other/prompt.md", "x"), nil, "an action of the package"},
		{store.ProposalPrompt, newFile("actions/scorer/main.go", "package main"), nil, "prompt.md"},
		{store.ProposalPrompt, "--- a/actions/scorer/prompt.md\n+++ /dev/null\n@@ -1 +0,0 @@\n-Score the listing.\n", nil,
			"needs its prompt"},
		{store.ProposalConfig, configPatch, []string{"score"}, ""},
		{store.ProposalTopology, configPatch, []string{"score"}, ""},
		{store.ProposalConfig, configPatch + lessonsPatch, nil, "pipeline.yaml alone"},
		{store.ProposalConfig, newFile(".github/workflows/ci.yaml", "on: push"), nil, "pipeline.yaml alone"},
		{store.ProposalTopology, newFile("Makefile", "all:"), nil, "pipeline.yaml alone"},
		{store.ProposalDistill, distill("coded"), []string{"score"}, ""},
		{store.ProposalDistill, edit("pipeline.yaml", 6, "  score:", "    action: ./actions/scorer",
			"    action: ./actions/scorer", "    in: trigger.adhoc"), nil, "adds a new actions/<name>/"},
		{store.ProposalDistill, distill("coded", newFile("actions/scorer/main.go", "x")), nil, "exists"},
		{store.ProposalDistill, distill("coded", newFile("actions/more/x.go", "x")), nil, "one action directory"},
		{store.ProposalDistill, distill("coded", newFile("actions/coded", "x")), nil, "a new actions/<name>/"},
		{store.ProposalDistill, distill("coded", newFile("bin/tool", "x")), nil, "a new actions/<name>/"},
		{"other", lessonsPatch, nil, "is not one of"},
		{store.ProposalConfig, strings.Replace(configPatch, "concurrency: 2", "concurrency: [", 1), nil, "does not parse"},
		{store.ProposalLessons, "diff --git a/lessons/score.yaml b/lessons/score.yaml\nnew file mode 120000\n", nil,
			"symbolic links"},
		{store.ProposalConfig, "diff --git a/pipeline.yaml b/pipeline.yaml\nold mode 100644\nnew mode 100755\n", nil,
			"mode change"},
		{store.ProposalLessons, newFile("lessons/../../etc/x", "x"), nil, "diff --git"},
	}
	for _, tt := range tests {
		t.Run(string(tt.kind)+" "+tt.why, func(t *testing.T) {
			p, err := Prepare(tt.kind, base, tt.patch)
			if tt.why != "" {
				if !errors.Is(err, ErrPatch) || !strings.Contains(err.Error(), tt.why) {
					t.Fatalf("Prepare = %v, want a refusal saying %q", err, tt.why)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(p.Steps, tt.steps) || p.SHA256 != Hash(tt.patch) || p.Patched == nil || len(p.Paths()) == 0 {
				t.Fatalf("prepared: steps %v sha %s", p.Steps, p.SHA256)
			}
		})
	}
}

func TestFingerprint(t *testing.T) {
	a := Fingerprint("p", store.ProposalLessons, []string{"i2", "i1"}, []string{"b", "a"})
	if a != Fingerprint("p", store.ProposalLessons, []string{"i1", "i2"}, []string{"a", "b"}) || len(a) != 32 {
		t.Fatalf("fingerprint %q depends on order", a)
	}
	if a == Fingerprint("p", store.ProposalPrompt, []string{"i1", "i2"}, []string{"a", "b"}) {
		t.Fatal("the kind does not change the fingerprint")
	}
}
