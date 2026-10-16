package pipeline

import (
	"strings"
	"testing"
)

const lessonsYAML = `rules:
  - text: Listings without a surface area are not scored; return score 0 and reason "missingSurface".
    source: proposal/p-0142
    added: 2026-11-03
examples:
  - input: { surface: 40, floor: 0, street: main road }
    output: { score: 35 }
    why: "Reviewer lowered the score: ground-floor flats on a main road."
    source: review/r-8812
`

func TestParseLessons(t *testing.T) {
	t.Parallel()
	l, err := ParseLessons([]byte(lessonsYAML))
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Rules) != 1 || l.Rules[0].Added != "2026-11-03" || l.Rules[0].Source != "proposal/p-0142" ||
		len(l.Examples) != 1 || l.Examples[0].Why == "" || l.Examples[0].Input.(map[string]any)["surface"] != int64(40) ||
		l.Empty() {
		t.Fatalf("lessons %+v", l)
	}
	if empty, err := ParseLessons(nil); err != nil || !empty.Empty() {
		t.Fatalf("empty file: %+v, %v", empty, err)
	}
	tests := map[string]struct{ src, want string }{
		"unknown key":      {"tips: []\n", "tips: unknown key"},
		"rules not a list": {"rules: x\n", "rules: must be a list"},
		"rule scalar":      {"rules: [x]\n", "rules[0]: must be a mapping"},
		"rule no text":     {"rules: [{ source: a }]\n", "rules[0].text: is required"},
		"rule empty text":  {"rules: [{ text: \"\" }]\n", "rules[0].text: must not be empty"},
		"rule key":         {"rules: [{ text: a, weight: 2 }]\n", "rules[0].weight: unknown key"},
		"example output":   {"examples: [{ input: {} }]\n", "examples[0].output: is required"},
		"example key":      {"examples: [{ input: 1, output: 2, note: x }]\n", "examples[0].note: unknown key"},
		"example why":      {"examples: [{ input: 1, output: 2, why: [x] }]\n", "examples[0].why: must be a string"},
		"not a mapping":    {"- a\n", "must be a mapping"},
		"yaml":             {"rules: [\n", "parse YAML"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseLessons([]byte(tt.src)); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestStepLessons(t *testing.T) {
	t.Parallel()
	pkg := &Package{Files: map[string][]byte{"lessons/score.yaml": []byte(lessonsYAML), "bad.yaml": []byte("x: 1\n")}}
	if l, data, err := StepLessons(pkg, Step{}); l != nil || data != nil || err != nil {
		t.Fatalf("no lessons: %v, %v", l, err)
	}
	if l, data, err := StepLessons(pkg, Step{Lessons: "lessons/./score.yaml"}); err != nil || len(l.Rules) != 1 ||
		string(data) != lessonsYAML {
		t.Fatalf("lessons: %+v, %v", l, err)
	}
	for file, want := range map[string]string{
		"missing.yaml":   "lessons file missing.yaml is not in the package",
		"../outside.yml": "leaves the package",
		"bad.yaml":       "bad.yaml: line 1: x: unknown key",
	} {
		if _, _, err := StepLessons(pkg, Step{Lessons: file}); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: err = %v, want %q", file, err, want)
		}
	}
	if got := LessonsTokens([]byte("abcdé")); got != 2 {
		t.Fatalf("LessonsTokens = %d, want 2 (five characters)", got)
	}
}

// TestCheckLessons: a lessons file over lessons.maxTokens fails check, never truncated; a file that does not parse
// or does not exist fails it; lessons on a step that is not an agent step are a warning.
func TestCheckLessons(t *testing.T) {
	t.Parallel()
	big := "rules:\n  - text: " + strings.Repeat("x", 400) + "\n"
	tests := []struct {
		name    string
		yaml    string
		files   [][2]string
		max     int
		want    string
		warning bool
	}{
		{"within the limit", "  agent: { action: ./actions/agent, in: trigger.go }\n",
			[][2]string{{"lessons/agent.yaml", lessonsYAML}}, 0, "", false},
		{"over the limit", "  agent: { action: ./actions/agent, in: trigger.go }\n",
			[][2]string{{"lessons/agent.yaml", big}}, 100, "error steps.agent.lessons: lessons/agent.yaml is about 105 tokens " +
				"(characters / 4), over lessons.maxTokens 100: merge or prune its rules and examples, it is never truncated", false},
		{"explicit path", "  agent: { action: ./actions/agent, in: trigger.go, lessons: shared/score.yaml }\n",
			[][2]string{{"shared/score.yaml", big}}, 100, "lessons.maxTokens 100", false},
		{"missing file", "  agent: { action: ./actions/agent, in: trigger.go, lessons: gone.yaml }\n", nil, 0,
			"error steps.agent.lessons: lessons file gone.yaml is not in the package", false},
		{"bad file", "  agent: { action: ./actions/agent, in: trigger.go }\n",
			[][2]string{{"lessons/agent.yaml", "rule: []\n"}}, 0, "error steps.agent.lessons: lessons/agent.yaml: line 1: rule: unknown key", false},
		{"not an agent", "  score: { action: ./actions/score, in: trigger.go }\n",
			[][2]string{{"lessons/score.yaml", lessonsYAML}}, 0, "warning steps.score.lessons: lessons apply to agent steps only", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkg := memPackage(t, append(append([][2]string{{"pipeline.yaml", checkHeader + "steps:\n" + tt.yaml}},
				checkActions...), tt.files...)...)
			var lessons []string
			diags := Check(pkg, Resolver{}, envOf(nil), CheckOptions{LessonsMaxTokens: tt.max})
			for _, d := range diags {
				if strings.HasSuffix(d.Path, ".lessons") {
					lessons = append(lessons, d.String())
				}
			}
			switch {
			case tt.want == "" && len(lessons) > 0:
				t.Fatalf("findings %q", lessons)
			case tt.want != "" && (len(lessons) != 1 || !strings.Contains(lessons[0], tt.want)):
				t.Fatalf("findings %q, want %q", lessons, tt.want)
			case tt.want != "" && HasErrors(diags) == tt.warning:
				t.Fatalf("HasErrors = %v", HasErrors(diags))
			}
		})
	}
}

// TestLessonsChangeTheRevision: a lessons file is part of the package hash, so editing it makes a new revision.
func TestLessonsChangeTheRevision(t *testing.T) {
	t.Parallel()
	before := memPackage(t, [2]string{"pipeline.yaml", minimalPipeline}, [2]string{"lessons/x.yaml", lessonsYAML})
	edited := strings.Replace(lessonsYAML, "score 0", "score 1", 1)
	after := memPackage(t, [2]string{"pipeline.yaml", minimalPipeline}, [2]string{"lessons/x.yaml", edited})
	if after.Hash == before.Hash {
		t.Fatal("hash unchanged after a lessons edit")
	}
}
