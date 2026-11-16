package improve

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// devEnv is a development environment file running the improve pipeline through the api harness.
const devEnv = `environment: development
lessons: { maxTokens: 1500 }
improve: { enabled: true, harness: api, publish: patch-file, patchFile: { dir: proposals }, minSeverity: info, budget: { dayUsd: 3 },
  filter: { maxFalseDrop: 0.05 } }
`

var (
	checked     *pipeline.Package
	checkDiags  []pipeline.Diagnostic
	checkFailed error
)

// TestMain loads and checks the package as a development deployment configures it, once, outside the budget: the
// schemas and CEL environments compile there.
func TestMain(m *testing.M) {
	env, err := pipeline.ParseEnvironment([]byte(devEnv))
	if err == nil {
		var files map[string][]byte
		if files, err = Files(env); err == nil {
			if checked, err = pipeline.FromFiles("", files); err == nil {
				checkDiags = pipeline.Check(checked, pipeline.Resolver{}, func(string) (string, bool) { return "", false },
					pipeline.CheckOptions{LessonsMaxTokens: env.Lessons.MaxTokens,
						Secrets: func(string) pipeline.SecretLookup {
							return func(string) (string, error) { return "set", nil }
						}})
			}
		}
	}
	checkFailed = err
	os.Exit(m.Run())
}

// The package loads and passes check with the environment's settings filled in, and its triggers feed
// gather-evidence: problems at improve.minSeverity, opportunities at any severity, the weekly cron and the manual
// "Propose a fix".
func TestPackage(t *testing.T) {
	if checkFailed != nil {
		t.Fatal(checkFailed)
	}
	for _, d := range checkDiags {
		if d.Severity == pipeline.SeverityError {
			t.Errorf("check: %s: %s", d.Path, d.Message)
		}
	}
	p := checked.Pipeline
	if p.Name != Name {
		t.Fatalf("pipeline %q", p.Name)
	}
	tr := p.Triggers
	problems, opportunities := tr["insights"].Insight, tr["opportunities"].Insight
	if problems == nil || problems.MinSeverity != "" {
		t.Fatalf("insights trigger %+v: want improve.minSeverity", tr["insights"])
	}
	if opportunities == nil || opportunities.MinSeverity != pipeline.SeverityInfo ||
		!slices.Equal(opportunities.Kinds, pipeline.InsightOpportunities) {
		t.Fatalf("opportunities trigger %+v: want %v at any severity", tr["opportunities"], pipeline.InsightOpportunities)
	}
	// A kind that drafts fires exactly one of them, so no insight starts two runs and pays for two drafts; the
	// others (cost, waste, drift, unschedulable) fire none, as no idea answers them.
	for _, kind := range pipeline.InsightKinds {
		fires := btoi(slices.Contains(problems.Kinds, kind)) + btoi(slices.Contains(opportunities.Kinds, kind))
		if want := btoi(slices.Contains(drafting, kind)); fires != want {
			t.Errorf("a %s insight fires %d improve triggers, want %d", kind, fires, want)
		}
	}
	if tr["weekly"].Cron == nil || tr["propose"].Manual == nil {
		t.Errorf("triggers %+v", tr)
	}
	if got := p.Steps["gather-evidence"].In; !slices.Equal(got, []string{"trigger.insights", "trigger.opportunities",
		"trigger.weekly", "trigger.propose"}) {
		t.Errorf("gather-evidence reads %v", got)
	}
	if p.Budget.DayUSD != 3 {
		t.Errorf("budget %+v, want improve.budget.dayUsd", p.Budget)
	}
	for name, want := range map[string]any{"publisher": "patch-file", "lessonsMaxTokens": 1500, "maxFalseDrop": 0.05,
		"minSeverity": "info", "distillAuditRate": 0.02} {
		if got := p.Vars[name]; got != want && !(isNumber(got) && toFloat(got) == toFloat(want)) {
			t.Errorf("var %s = %v (%T), want %v", name, got, got, want)
		}
	}
	draft, err := pipeline.Resolver{}.Resolve(checked, "./actions/draft")
	if err != nil {
		t.Fatal(err)
	}
	if a := draft.Agent; a.Harness != pipeline.HarnessAPI || a.Model != APIModel || a.APIKeySecret != APIKeySecret {
		t.Errorf("draft agent %+v, want improve.harness api", a)
	}
	if !slices.Contains(p.Secrets, APIKeySecret) {
		t.Errorf("secrets %v lack %s", p.Secrets, APIKeySecret)
	}
	gather, err := pipeline.Resolver{}.Resolve(checked, "./actions/gather-evidence")
	if err != nil || !slices.Equal(gather.Command, []string{"millstream", "improve-action", "gather-evidence"}) {
		t.Errorf("gather-evidence runs %v (%v), want the binary's improve-action", gather.Command, err)
	}
}

func isNumber(v any) bool {
	switch v.(type) {
	case int, int64, float64:
		return true
	}
	return false
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case float64:
		return x
	}
	return -1
}

// The source keeps the claude-code harness and no budget; Fork renames the pipeline where it names itself.
func TestSourceAndFork(t *testing.T) {
	src, err := Source()
	if err != nil {
		t.Fatal(err)
	}
	if m := string(src["actions/draft/action.yaml"]); !strings.Contains(m, "harness: claude-code") {
		t.Errorf("draft manifest:\n%s", m)
	}
	files, err := Fork("my-improve")
	if err != nil {
		t.Fatal(err)
	}
	p := string(files[pipeline.FileName])
	if strings.Contains(p, "pipeline: improve") || !strings.Contains(p, "\npipeline: my-improve\n") ||
		strings.Count(p, "pipeline: my-improve #") != 2 || strings.Contains(p, "budget:") {
		t.Errorf("forked pipeline.yaml:\n%s", p)
	}
}

// The pipeline runs only in development, with improve.enabled and a harness.
func TestCheck(t *testing.T) {
	for _, c := range []struct {
		name, env, def string
		want           error
	}{
		{"production", "environment: production\nimprove: { harness: api }\n", pipeline.ModeDevelopment,
			pipeline.ErrProduction},
		{"serve's default", "improve: { harness: api }\n", pipeline.ModeProduction, pipeline.ErrProduction},
		{"disabled", "improve: { harness: api }\n", pipeline.ModeDevelopment, ErrDisabled},
		{"no harness", "improve: { enabled: true }\n", pipeline.ModeDevelopment, ErrNoHarness},
		{"ready", devEnv, pipeline.ModeProduction, nil},
	} {
		env, err := pipeline.ParseEnvironment([]byte(c.env))
		if err != nil {
			t.Fatal(c.name, err)
		}
		if err := Check(env, c.def); !errors.Is(err, c.want) || (c.want == nil) != (err == nil) {
			t.Errorf("%s: %v, want %v", c.name, err, c.want)
		}
	}
}

// drafting are the insight kinds ideasOf makes proposals from.
var drafting = []string{pipeline.InsightSlowStep, pipeline.InsightFailureCluster, pipeline.InsightReviewPattern,
	pipeline.InsightDistillable}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
