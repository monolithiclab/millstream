package improve

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
)

// distillPipeline is a block-style pipeline whose score step runs an agent.
const distillPipeline = `pipeline: listings
triggers: { adhoc: { manual: {} } }
steps:
  score:
    action: ./actions/score # the agent
    in: trigger.adhoc
    out: scored

  keep:
    action: filter
    in: scored
    out: kept
    config: { expr: "item.tier == 'low'" }
`

const agentManifest = `name: lab/score
version: 1.0.0
kind: agent
input: { type: object, properties: { id: { type: string }, price: { type: integer } }, required: [id, price] }
output: { type: object, properties: { id: { type: string }, tier: { enum: [low, high] } }, required: [id, tier] }
runtime: { agent: { harness: claude-code, model: sonnet, maxCostUsd: 0.1 } }
`

var holdoutFrom = time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)

// distillAPI is the fake API with a distillable insight on score and its training set.
func distillAPI() *fakeAPI {
	f := newFakeAPI()
	f.files = map[string]string{"pipeline.yaml": distillPipeline, "actions/score/action.yaml": agentManifest,
		"actions/score/prompt.md": "Tier listing {{ .Payload.id }}.\n", "actions/score-rules/README.md": "taken\n"}
	f.insights = append(f.insights, api.Insight{Pipeline: "listings", ID: "in-distill", Kind: "distillable",
		Scope: "score", Severity: "info", State: "open",
		Evidence: json.RawMessage(`{"deliveries":600,"samples":["it_1"],"monthlySavingUsd":12.5}`)})
	f.training = api.DistillTraining{Pipeline: "listings", Step: "score", Agent: "./actions/score", Source: "local",
		HoldoutFrom: holdoutFrom, Pairs: 3, Holdout: 1, WithheldExamples: 1,
		Training: []distill.Pair{{Key: "A", Input: json.RawMessage(`{"id":"A","price":10}`),
			Outputs: []distill.Output{{Key: "A", Payload: json.RawMessage(`{"id":"A","tier":"low"}`)}}},
			{Key: "B", Input: json.RawMessage(`{"id":"B","price":90}`),
				Outputs: []distill.Output{{Key: "B", Payload: json.RawMessage(`{"id":"B","tier":"high"}`)}}}},
		Lessons: pipeline.Lessons{Rules: []pipeline.LessonRule{{Text: "Under 50 is low."}}}}
	return f
}

// A distillable insight's run gathers a distill idea: the training side of the engine's split, never the holdout,
// in a brief without times or ids, the new action named after the step (past the directories taken), and the
// boundary kept for validate; a step whose history the engine cannot split is skipped.
func TestGatherDistill(t *testing.T) {
	f := distillAPI()
	a := testAction(t, f, config{Pipeline: "improve"})
	outs, err := gather(context.Background(), a, item(`{"insightId":"in-distill","pipeline":"listings"}`))
	if err != nil {
		t.Fatal(err)
	}
	evs := evidenceOf(t, outs)
	if len(evs) != 1 {
		t.Fatalf("evidence %+v", evs)
	}
	ev := evs[0]
	de := ev.Distill
	if ev.Kind != "distill" || ev.Insights[0] != "in-distill" || de == nil || de.Step != "score" ||
		de.Agent != "./actions/score" || de.Action != "score-rules-2" || !de.HoldoutFrom.Equal(holdoutFrom) {
		t.Fatalf("evidence %+v, distill %+v", ev, de)
	}
	for _, want := range []string{"actions/score-rules-2/", `{"input":{"id":"A","price":10},"outputs":[{"key":"A",` +
		`"payload":{"id":"A","tier":"low"}}]}`, `"tier": {`, "Tier listing {{ .Payload.id }}.", "Under 50 is low.",
		"2 of the step's 3", "c.Abstain", `"monthlySavingUsd": 12.5`} {
		if !strings.Contains(ev.Brief, want) {
			t.Errorf("the brief lacks %q:\n%s", want, ev.Brief)
		}
	}
	for _, leak := range []string{"2026-10-02", "it_1", "samples"} {
		if strings.Contains(ev.Brief, leak) {
			t.Errorf("the brief holds %q", leak)
		}
	}
	f.trainErr = http.StatusBadRequest
	if outs, err = gather(context.Background(), a, item(`{"insightId":"in-distill"}`)); err != nil || len(outs) != 0 {
		t.Errorf("unsplittable: %+v, %v", outs, err)
	}
}

const rulesMain = "package main\n\nfunc main() {}\n"

// validate assembles a distill draft: the code, tests and evals the agent wrote, and the manifest (the agent's
// schemas), go.mod, spec and pipeline change it writes itself; a draft writing anything else is refused.
func TestValidateDistill(t *testing.T) {
	f := distillAPI()
	ev := Evidence{Pipeline: "listings", BaseRevision: baseHash, Kind: "distill", Fingerprint: "improve-d",
		Insights: []string{"in-distill"}, Distill: &DistillEvidence{Step: "score", Agent: "./actions/score",
			Action: "score-rules", HoldoutFrom: holdoutFrom}}
	raw, _ := json.Marshal(ev)
	f.items["improve/e1"] = api.Item{ID: "e1", Payload: raw}
	a := testAction(t, f, config{Pipeline: "improve"})
	draft := func(files ...string) string {
		d := Draft{Kind: "distill", Rationale: "Tiers follow the price.", Insights: []string{"in-distill"}}
		for i := 0; i < len(files); i += 2 {
			d.Files = append(d.Files, DraftFile{Path: files[i], Content: files[i+1]})
		}
		b, _ := json.Marshal(d)
		return string(b)
	}
	good := draft("actions/score-rules/main.go", rulesMain, "actions/score-rules/main_test.go", "package main\n",
		"actions/score-rules/evals/low.yaml", "input: { id: A, price: 10 }\nassert: [output.tier == 'low']\n")
	outs, err := validate(context.Background(), a, item(good))
	if err != nil {
		t.Fatal(err)
	}
	if o := outs[0].Payload.(Outcome); o.Outcome != OutcomeProposed || len(f.created) != 1 {
		t.Fatalf("outcome %+v", o)
	}
	req := f.created[0]
	p, err := patch.Parse([]byte(req.Patch))
	if err != nil {
		t.Fatal(err)
	}
	base := map[string][]byte{}
	for k, v := range f.files {
		base[k] = []byte(v)
	}
	files, err := p.Apply(base)
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := pipeline.FromFiles("", files)
	if err != nil {
		t.Fatal(err)
	}
	st := pkg.Pipeline.Steps["score"]
	act, err := pipeline.Resolver{}.Resolve(pkg, st.Action)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := distill.ParseSpec(files["actions/score-rules/distill.yaml"])
	if err != nil || req.Kind != "distill" || st.Action != "./actions/score-rules" || st.Fallback == nil ||
		st.Fallback.Action != "./actions/score" || st.Fallback.When[0] != "abstain" || act.Kind != "exec" ||
		act.Effects != "none" || !strings.Contains(string(act.Output), `"tier"`) || act.Command[0] != "score-rules" ||
		!strings.Contains(string(files["actions/score-rules/go.mod"]), "require github.com/monolithiclab/millstream/sdk/go") ||
		spec.Step != "score" || !spec.HoldoutFrom.Equal(holdoutFrom) || string(files["actions/score-rules/main.go"]) != rulesMain ||
		!strings.Contains(string(files["pipeline.yaml"]), "  keep:\n    action: filter\n") {
		t.Fatalf("assembled %v (%v):\n%s", st, err, files["pipeline.yaml"])
	}
	for name, body := range map[string]string{
		"outside":     draft("actions/score-rules/main.go", rulesMain, "actions/other/x.go", "package x\n"),
		"manifest":    draft("actions/score-rules/main.go", rulesMain, "actions/score-rules/action.yaml", "x"),
		"pipeline":    draft("actions/score-rules/main.go", rulesMain, "pipeline.yaml", "x"),
		"no main":     draft("actions/score-rules/rules.go", rulesMain),
		"nested code": draft("actions/score-rules/main.go", rulesMain, "actions/score-rules/sub/x.go", "package x\n"),
		"a patch":     `{"kind":"distill","rationale":"r","patch":"--- a/x\n+++ b/x\n"}`,
	} {
		outs, err := validate(context.Background(), a, item(body))
		if err != nil || outs[0].Payload.(Outcome).Outcome != OutcomeRefused {
			t.Errorf("%s: %+v, %v", name, outs, err)
		}
	}
}

// A distill keeps a sensor on what it replaces: the patch audits the code against the agent at
// improve.distill.auditRate, when the agent declares no effects as audits require, and 0 turns it off.
func TestValidateDistillAuditRate(t *testing.T) {
	for name, c := range map[string]struct {
		rate    float64
		effects string
		want    float64
	}{
		"agent without effects":   {0.02, pipeline.EffectsNone, 0.02},
		"agent with effects":      {0.02, pipeline.EffectsExternal, 0},
		"agent that says nothing": {0.02, "", 0},
		"turned off":              {0, pipeline.EffectsNone, 0},
	} {
		t.Run(name, func(t *testing.T) {
			f := distillAPI()
			manifest := f.files["actions/score/action.yaml"]
			if c.effects != "" {
				manifest += "effects: " + c.effects + "\n"
			}
			f.files["actions/score/action.yaml"] = manifest
			raw, _ := json.Marshal(Evidence{Pipeline: "listings", BaseRevision: baseHash, Kind: "distill",
				Fingerprint: "improve-d", Insights: []string{"in-distill"}, Distill: &DistillEvidence{Step: "score",
					Agent: "./actions/score", Action: "score-rules", HoldoutFrom: holdoutFrom}})
			f.items["improve/e1"] = api.Item{ID: "e1", Payload: raw}
			a := testAction(t, f, config{Pipeline: "improve", DistillAuditRate: c.rate})
			body, _ := json.Marshal(Draft{Kind: "distill", Rationale: "Tiers follow the price.",
				Insights: []string{"in-distill"}, Files: []DraftFile{{Path: "actions/score-rules/main.go", Content: rulesMain}}})
			outs, err := validate(context.Background(), a, item(string(body)))
			if err != nil {
				t.Fatal(err)
			}
			if o := outs[0].Payload.(Outcome); o.Outcome != OutcomeProposed {
				t.Fatalf("outcome %+v", o)
			}
			p, err := patch.Parse([]byte(f.created[0].Patch))
			if err != nil {
				t.Fatal(err)
			}
			base := map[string][]byte{}
			for k, v := range f.files {
				base[k] = []byte(v)
			}
			files, err := p.Apply(base)
			if err != nil {
				t.Fatal(err)
			}
			pkg, err := pipeline.FromFiles("", files)
			if err != nil {
				t.Fatal(err)
			}
			if got := pkg.Pipeline.Steps["score"].Fallback; got == nil || got.AuditRate != c.want {
				t.Errorf("fallback %+v, want auditRate %v", got, c.want)
			}
			// A reviewer reads in the diff that nothing will check the code, rather than miss a key that is absent.
			flagged := strings.Contains(string(files["pipeline.yaml"]), "# no auditRate: ./actions/score does not declare")
			if want := c.rate > 0 && c.want == 0; flagged != want {
				t.Errorf("note in the pipeline file: %v, want %v\n%s", flagged, want, files["pipeline.yaml"])
			}
		})
	}
}

// withFallback moves a block-style step's agent under fallback, and refuses what it cannot rewrite exactly.
func TestWithFallback(t *testing.T) {
	got, err := withFallback(distillPipeline, "score", "./actions/score", "./actions/rules", 0, "")
	if err != nil || !strings.Contains(got, "  score:\n    action: ./actions/rules\n"+
		"    fallback: { action: ./actions/score, when: [abstain] }\n    in: trigger.adhoc\n") {
		t.Fatalf("%v:\n%s", err, got)
	}
	flow := "pipeline: p\ntriggers: { a: { manual: {} } }\nsteps:\n  score: { action: ./actions/score, in: trigger.a, out: s }\n"
	withFb := strings.Replace(distillPipeline, "    in: trigger.adhoc\n",
		"    fallback: { action: ./actions/other }\n    in: trigger.adhoc\n", 1)
	for name, c := range map[string][2]string{
		"flow style":     {flow, "./actions/score"},
		"has a fallback": {withFb, "./actions/score"},
		"another agent":  {distillPipeline, "./actions/agent"},
		"no step":        {strings.Replace(distillPipeline, "  score:", "  scoring:", 1), "./actions/score"},
	} {
		if _, err := withFallback(c[0], "score", c[1], "./actions/rules", 0, ""); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
