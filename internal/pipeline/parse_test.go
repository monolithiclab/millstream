package pipeline

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

const referencePipeline = `
pipeline: property-sourcing
description: Daily sourcing of renovation opportunities

vars:
  city: Lyon
  maxPrice: 400000
  minScore: 70
  minMarginPct: 15.5
  region: null

secrets: [portalToken]

triggers:
  daily: { cron: "0 6 * * *", tz: Europe/Paris }
  rescore: { manual: {} }

streams:
  listings.raw: { retention: 90d }

steps:
  crawl:
    action: lab/crawl-portal@1.2
    in: trigger.daily
    out: listings.raw
    config:
      filters: { city: "${vars.city}", maxPrice: "${vars.maxPrice}" }
      token: "${secrets.portalToken}"
  score:
    action: lab/score-listing@2
    in: [listings.raw, trigger.rescore]
    out: listings.scored
    concurrency: 8
  keep-good:
    action: filter
    in: listings.scored
    out: listings.shortlist
    config: { expr: "item.score >= vars.minScore" }
  report:
    action: lab/pdf-report@1
    in: listings.shortlist

budget:
  runUsd: 25
`

func TestParseReference(t *testing.T) {
	t.Parallel()
	p, err := Parse([]byte(referencePipeline))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if p.Name != "property-sourcing" || p.Description != "Daily sourcing of renovation opportunities" {
		t.Errorf("name/description = %q/%q", p.Name, p.Description)
	}
	wantVars := map[string]any{"city": "Lyon", "maxPrice": int64(400000), "minScore": int64(70),
		"minMarginPct": 15.5, "region": nil}
	if !reflect.DeepEqual(p.Vars, wantVars) {
		t.Errorf("vars = %#v, want %#v", p.Vars, wantVars)
	}
	if !reflect.DeepEqual(p.Secrets, []string{"portalToken"}) {
		t.Errorf("secrets = %v", p.Secrets)
	}
	if c := p.Triggers["daily"].Cron; c == nil || *c != (CronSpec{Expr: "0 6 * * *", TZ: "Europe/Paris", Catchup: CatchupLatest}) {
		t.Errorf("daily cron = %+v", c)
	}
	if p.Triggers["rescore"].Manual == nil {
		t.Errorf("rescore is not manual: %+v", p.Triggers["rescore"])
	}
	if got := p.Streams["listings.raw"].Retention; got != 90*24*time.Hour {
		t.Errorf("retention = %v", got)
	}
	crawl := p.Steps["crawl"]
	want := Step{
		Name:   "crawl",
		Action: "lab/crawl-portal@1.2",
		In:     []string{"trigger.daily"},
		Out:    Out{Streams: []string{"listings.raw"}},
		DLQ:    "crawl.dlq",
		Config: map[string]any{
			"filters": map[string]any{"city": "${vars.city}", "maxPrice": "${vars.maxPrice}"},
			"token":   "${secrets.portalToken}",
		},
		Concurrency: DefaultConcurrency,
		Retry:       Retry{MaxAttempts: 3, Backoff: 10 * time.Second, MaxBackoff: 10 * time.Minute},
		Timeout:     10 * time.Minute,
	}
	if !reflect.DeepEqual(crawl, want) {
		t.Errorf("crawl = %+v\nwant    %+v", crawl, want)
	}
	if got := p.Steps["score"]; got.Concurrency != 8 || !reflect.DeepEqual(got.In, []string{"listings.raw", "trigger.rescore"}) {
		t.Errorf("score = %+v", got)
	}
	if got := p.Steps["report"]; len(got.Out.Streams)+len(got.Out.Routes) != 0 || len(got.Config) != 0 {
		t.Errorf("report is not terminal with empty config: %+v", got)
	}
	if p.Budget.RunUSD != 25 {
		t.Errorf("budget = %+v", p.Budget)
	}
}

// pipelineWith wraps a steps block in the minimal valid pipeline.
func pipelineWith(steps string) string {
	return "pipeline: p\ntriggers:\n  go: { manual: {} }\nsteps:\n" + steps
}

func TestParseOut(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		out     string
		want    Out
		wantErr string
	}{
		{name: "one stream", out: "out: a", want: Out{Streams: []string{"a"}}},
		{name: "list of streams", out: "out: [a, b]", want: Out{Streams: []string{"a", "b"}}},
		{
			name: "routes",
			out:  "out:\n      - { when: \"item.x == 1\", out: a }\n      - { when: \"true\", out: b }",
			want: Out{Routes: []Route{{When: "item.x == 1", Out: "a"}, {When: "true", Out: "b"}}},
		},
		{name: "terminal", out: "", want: Out{}},
		{name: "null is terminal", out: "out: null", want: Out{}},
		{name: "stream then route", out: "out: [a, { when: \"true\", out: b }]", wantErr: "steps.s.out: a list holds either stream names or route objects, never both"},
		{name: "route then stream", out: "out: [{ when: \"true\", out: b }, a]", wantErr: "steps.s.out: a list holds either"},
		{name: "empty list", out: "out: []", wantErr: "steps.s.out: must not be empty"},
		{name: "mapping", out: "out: { a: b }", wantErr: "steps.s.out: must be a stream, a list of streams or a list of route objects"},
		{name: "route without when", out: "out: [{ out: b }]", wantErr: "steps.s.out[0].when: is required"},
		{name: "route without out", out: "out: [{ when: \"true\" }]", wantErr: "steps.s.out[0].out: is required"},
		{name: "route unknown key", out: "out: [{ when: \"true\", out: b, to: c }]", wantErr: "steps.s.out[0].to: unknown key"},
		{name: "empty stream", out: "out: \"\"", wantErr: "steps.s.out: must not be empty"},
		{name: "nested list", out: "out: [[a]]", wantErr: "steps.s.out[0]: must be a string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Parse([]byte(pipelineWith("  s:\n    action: x\n    in: trigger.go\n    " + tt.out + "\n")))
			if tt.wantErr != "" {
				assertErr(t, err, tt.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := p.Steps["s"].Out; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("out = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseStepOptions(t *testing.T) {
	t.Parallel()
	p, err := Parse([]byte(pipelineWith(`  s:
    action: x
    in: trigger.go
    dlq: shared.dlq
    concurrency: 4
    rate: 10/min
    retry: { maxAttempts: 5, backoff: 1s, maxBackoff: 1m }
    timeout: 30s
    paused: true
    placement: { labels: { gpu: "true" } }
    fallback: { action: lab/rules@1, when: [abstain, error], auditRate: 0.02 }
    lessons: lessons/custom.yaml
`)))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	s := p.Steps["s"]
	want := Step{
		Name: "s", Action: "x", In: []string{"trigger.go"}, DLQ: "shared.dlq", Config: map[string]any{},
		Concurrency: 4, Rate: "10/min",
		Retry:     Retry{MaxAttempts: 5, Backoff: time.Second, MaxBackoff: time.Minute},
		Timeout:   30 * time.Second,
		Paused:    true,
		Placement: &Placement{Labels: map[string]string{"gpu": "true"}},
		Fallback:  &Fallback{Action: "lab/rules@1", When: []string{"abstain", "error"}, AuditRate: 0.02},
		Lessons:   "lessons/custom.yaml",
	}
	if !reflect.DeepEqual(s, want) {
		t.Errorf("step = %+v\nwant   %+v", s, want)
	}
}

func TestParseTopLevelOptions(t *testing.T) {
	t.Parallel()
	p, err := Parse([]byte(`
pipeline: deal-flow
triggers:
  hook: { webhook: { path: /hooks/x }, inputSchema: { type: object } }
  nightly: { cron: "0 1 * * *", catchup: all }
streams:
  approved: { pipeline: property-sourcing, stream: listings.approved, retention: 7d }
  out: { schema: schemas/out.json, redact: [/email] }
steps:
  s: { action: x, in: approved, fallback: { action: y } }
budget: { runUsd: 1.5, dayUsd: 10 }
limits: { concurrency: 3 }
placement: { labels: { zone: eu } }
insights: { enabled: true }
improve: { enabled: false }
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if w := p.Triggers["hook"]; w.Webhook == nil || w.Webhook.Path != "/hooks/x" ||
		!reflect.DeepEqual(w.InputSchema, map[string]any{"type": "object"}) {
		t.Errorf("hook = %+v", w)
	}
	if c := p.Triggers["nightly"].Cron; c == nil || c.Catchup != CatchupAll || c.TZ != "" {
		t.Errorf("nightly = %+v", c)
	}
	if imp := p.Streams["approved"].Import; imp == nil || *imp != (StreamImport{"property-sourcing", "listings.approved"}) ||
		p.Streams["approved"].Retention != 7*24*time.Hour {
		t.Errorf("approved = %+v", p.Streams["approved"])
	}
	if d := p.Streams["out"]; d.Schema != "schemas/out.json" || !reflect.DeepEqual(d.Redact, []string{"/email"}) {
		t.Errorf("out = %+v", d)
	}
	if fb := p.Steps["s"].Fallback; fb == nil || !reflect.DeepEqual(fb.When, []string{"abstain"}) {
		t.Errorf("fallback default when = %+v", fb)
	}
	if p.Budget != (Budget{RunUSD: 1.5, DayUSD: 10}) || p.Limits.Concurrency != 3 || p.Placement.Labels["zone"] != "eu" {
		t.Errorf("settings = %+v %+v %+v", p.Budget, p.Limits, p.Placement)
	}
	if p.Insights["enabled"] != true || p.Improve["enabled"] != false {
		t.Errorf("insights/improve = %v %v", p.Insights, p.Improve)
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()
	step := func(extra string) string {
		return pipelineWith("  score:\n    action: x\n    in: trigger.go\n" + extra)
	}
	top := "pipeline: p\ntriggers: { go: { manual: {} } }\nsteps: { s: { action: x, in: trigger.go } }\n"
	tests := []struct {
		name, doc, wantErr string
	}{
		{"empty", "", "empty pipeline file"},
		{"comment only", "# nothing\n", "empty pipeline file"},
		{"two documents", pipelineWith("  s: { action: x, in: a }\n") + "---\npipeline: q\n", "single YAML document"},
		{"bad yaml", "pipeline: [", "parse YAML"},
		{"not a mapping", "- a\n", "line 1: must be a mapping"},
		{"unknown top key", "pipeline: p\nversion: 2\n", "line 2: version: unknown key"},
		{"duplicate key", "pipeline: p\npipeline: q\n", "pipeline: duplicate key"},
		{"missing pipeline", "triggers: {}\n", "pipeline: is required"},
		{"bad pipeline name", "pipeline: Bad_Name\n", "pipeline: \"Bad_Name\" must be lowercase"},
		{"missing triggers", "pipeline: p\nsteps: { s: { action: x, in: a } }\n", "triggers: is required unless the pipeline imports a stream"},
		{"no triggers", "pipeline: p\ntriggers: {}\nsteps: {}\n", "triggers: must declare at least one trigger"},
		{"missing steps", "pipeline: p\ntriggers: { go: { manual: {} } }\n", "steps: is required"},
		{"no steps", "pipeline: p\ntriggers: { go: { manual: {} } }\nsteps: {}\n", "steps: must declare at least one step"},
		{"unknown step key", step("    confg: {}\n"), "steps.score.confg: unknown key"},
		{"config not a mapping", step("    config: [1]\n"), "steps.score.config: must be a mapping"},
		{"config nested key error", step("    config: { a: { ? [x] : 1 } }\n"), "steps.score.config.a: keys must be strings"},
		{"missing action", pipelineWith("  s: { in: a }\n"), "steps.s.action: is required"},
		{"missing in", pipelineWith("  s: { action: x }\n"), "steps.s.in: is required"},
		{"empty in", pipelineWith("  s: { action: x, in: [] }\n"), "steps.s.in: must not be empty"},
		{"in not strings", pipelineWith("  s: { action: x, in: [{a: b}] }\n"), "steps.s.in[0]: must be a string"},
		{"bad concurrency", step("    concurrency: 0\n"), "steps.score.concurrency: must be a positive integer"},
		{"string concurrency", step("    concurrency: \"4\"\n"), "steps.score.concurrency: must be a positive integer"},
		{"bad rate", step("    rate: 10/fortnight\n"), "steps.score.rate: invalid rate \"10/fortnight\": unknown period"},
		{"bad timeout", step("    timeout: soon\n"), "steps.score.timeout: invalid duration \"soon\""},
		{"bad paused", step("    paused: yes please\n"), "steps.score.paused: must be true or false"},
		{"retry unknown key", step("    retry: { tries: 2 }\n"), "steps.score.retry.tries: unknown key"},
		{"retry backoff order", step("    retry: { backoff: 1m, maxBackoff: 1s }\n"), "steps.score.retry: maxBackoff must not be shorter"},
		{"retry bad max", step("    retry: { maxBackoff: 0s }\n"), "steps.score.retry.maxBackoff: duration \"0s\" must be positive"},
		{"retry bad attempts", step("    retry: { maxAttempts: -1 }\n"), "steps.score.retry.maxAttempts: must be a positive integer"},
		{"retry bad backoff", step("    retry: { backoff: x }\n"), "steps.score.retry.backoff: invalid duration"},
		{"retry not mapping", step("    retry: 3\n"), "steps.score.retry: must be a mapping"},
		{"placement unknown", step("    placement: { gpu: true }\n"), "steps.score.placement.gpu: unknown key"},
		{"placement label", step("    placement: { labels: { gpu: [1] } }\n"), "steps.score.placement.labels.gpu: must be a string"},
		{"placement labels mapping", step("    placement: { labels: [] }\n"), "steps.score.placement.labels: must be a mapping"},
		{"placement mapping", step("    placement: x\n"), "steps.score.placement: must be a mapping"},
		{"fallback when", step("    fallback: { action: y, when: [never] }\n"), "steps.score.fallback.when[0]: must be abstain or error"},
		{"fallback when type", step("    fallback: { action: y, when: [[x]] }\n"), "steps.score.fallback.when[0]: must be a string"},
		{"fallback rate", step("    fallback: { action: y, auditRate: 2 }\n"), "steps.score.fallback.auditRate: must be between 0 and 1"},
		{"fallback rate type", step("    fallback: { action: y, auditRate: x }\n"), "steps.score.fallback.auditRate: must be a non-negative number"},
		{"fallback action", step("    fallback: { when: [error] }\n"), "steps.score.fallback.action: is required"},
		{"fallback empty action", step("    fallback: { action: \"\" }\n"), "steps.score.fallback.action: must not be empty"},
		{"fallback unknown", step("    fallback: { action: y, mode: z }\n"), "steps.score.fallback.mode: unknown key"},
		{"fallback mapping", step("    fallback: y\n"), "steps.score.fallback: must be a mapping"},
		{"empty dlq", step("    dlq: \"\"\n"), "steps.score.dlq: must not be empty"},
		{"lessons type", step("    lessons: [a]\n"), "steps.score.lessons: must be a string"},
		{"step not mapping", pipelineWith("  s: x\n"), "steps.s: must be a mapping"},
		{"trigger two kinds", "pipeline: p\ntriggers: { t: { manual: {}, cron: \"* * * * *\" } }\n", "triggers.t: must set exactly one of cron, manual or webhook"},
		{"trigger insight", "pipeline: p\ntriggers: { t: { insight: {} } }\n", "triggers.t: must set exactly one of"},
		{"trigger cron key on manual", "pipeline: p\ntriggers: { t: { manual: {}, tz: UTC } }\n", "triggers.t.tz: unknown key"},
		{"manual key", "pipeline: p\ntriggers: { t: { manual: { schema: x } } }\n", "triggers.t.manual.schema: unknown key"},
		{"manual not mapping", "pipeline: p\ntriggers: { t: { manual: x } }\n", "triggers.t.manual: must be a mapping"},
		{"cron catchup", "pipeline: p\ntriggers: { t: { cron: \"* * * * *\", catchup: some } }\n", "triggers.t.catchup: must be latest, all or none"},
		{"cron catchup type", "pipeline: p\ntriggers: { t: { cron: \"* * * * *\", catchup: [a] } }\n", "triggers.t.catchup: must be a string"},
		{"cron empty", "pipeline: p\ntriggers: { t: { cron: \"\" } }\n", "triggers.t.cron: must not be empty"},
		{"cron unknown", "pipeline: p\ntriggers: { t: { cron: \"* * * * *\", zone: x } }\n", "triggers.t.zone: unknown key"},
		{"cron tz", "pipeline: p\ntriggers: { t: { cron: \"* * * * *\", tz: \"\" } }\n", "triggers.t.tz: must not be empty"},
		{"webhook path", "pipeline: p\ntriggers: { t: { webhook: {} } }\n", "triggers.t.webhook.path: is required"},
		{"webhook key", "pipeline: p\ntriggers: { t: { webhook: { path: /x, secret: y } } }\n", "triggers.t.webhook.secret: unknown key"},
		{"webhook mapping", "pipeline: p\ntriggers: { t: { webhook: x } }\n", "triggers.t.webhook: must be a mapping"},
		{"webhook unknown", "pipeline: p\ntriggers: { t: { webhook: { path: /x }, token: y } }\n", "triggers.t.token: unknown key"},
		{"trigger not mapping", "pipeline: p\ntriggers: { t: x }\n", "triggers.t: must be a mapping"},
		{"triggers not mapping", "pipeline: p\ntriggers: [a]\n", "triggers: must be a mapping"},
		{"stream mixed import", top + "streams: { a: { pipeline: q, stream: s, redact: [/x] } }\n", "streams.a.redact: unknown key"},
		{"stream import missing", top + "streams: { a: { pipeline: q } }\n", "streams.a.stream: is required"},
		{"stream import empty", top + "streams: { a: { pipeline: \"\", stream: s } }\n", "streams.a.pipeline: must not be empty"},
		{"stream unknown", top + "streams: { a: { ttl: 1d } }\n", "streams.a.ttl: unknown key"},
		{"stream retention", top + "streams: { a: { retention: forever } }\n", "streams.a.retention: invalid duration"},
		{"stream retention type", top + "streams: { a: { retention: [1] } }\n", "streams.a.retention: must be a string"},
		{"stream redact", top + "streams: { a: { redact: /x } }\n", "streams.a.redact: must be a list of strings"},
		{"stream schema", top + "streams: { a: { schema: { ? [x] : 1 } } }\n", "streams.a.schema: keys must be strings"},
		{"streams not mapping", top + "streams: [a]\n", "streams: must be a mapping"},
		{"stream not mapping", top + "streams: { a: b }\n", "streams.a: must be a mapping"},
		{"input schema keys", "pipeline: p\ntriggers: { t: { manual: {}, inputSchema: { ? [x] : 1 } } }\n", "triggers.t.inputSchema: keys must be strings"},
		{"vars not mapping", "pipeline: p\nvars: [a]\n", "vars: must be a mapping"},
		{"vars bad value", "pipeline: p\nvars: { a: !!binary aGk= }\n", "vars.a: unsupported YAML value of type !!binary"},
		{"vars nested bad", "pipeline: p\nvars: { a: [!!binary aGk=] }\n", "vars.a[0]: unsupported YAML value"},
		{"vars big int", top + "vars: { a: !!int 99999999999999999999 }\n", "vars.a: integer out of range"},
		{"secrets not list", "pipeline: p\nsecrets: token\n", "secrets: must be a list of strings"},
		{"secrets duplicate", "pipeline: p\nsecrets: [a, a]\n", "secrets[1]: duplicate secret \"a\""},
		{"description type", "pipeline: p\ndescription: [a]\n", "description: must be a string"},
		{"name type", "pipeline: [p]\n", "pipeline: must be a string"},
		{"budget unknown", top + "budget: { monthUsd: 1 }\n", "budget.monthUsd: unknown key"},
		{"budget negative", top + "budget: { runUsd: -1 }\n", "budget.runUsd: must be a non-negative number"},
		{"budget day", top + "budget: { dayUsd: x }\n", "budget.dayUsd: must be a non-negative number"},
		{"budget mapping", top + "budget: 3\n", "budget: must be a mapping"},
		{"limits unknown", top + "limits: { rate: 1 }\n", "limits.rate: unknown key"},
		{"limits value", top + "limits: { concurrency: 0 }\n", "limits.concurrency: must be a positive integer"},
		{"limits mapping", top + "limits: 3\n", "limits: must be a mapping"},
		{"placement top", top + "placement: { zone: x }\n", "placement.zone: unknown key"},
		{"insights mapping", top + "insights: true\n", "insights: must be a mapping"},
		{"improve mapping", top + "improve: [a]\n", "improve: must be a mapping"},
		{"improve keys", top + "improve: { ? [x] : 1 }\n", "improve: keys must be strings"},
		{"null key", "pipeline: p\n~: 1\n", "keys must be strings"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.doc))
			assertErr(t, err, tt.wantErr)
		})
	}
}

func TestParseAliases(t *testing.T) {
	t.Parallel()
	p, err := Parse([]byte(pipelineWith(`  a: &step { action: x, in: trigger.go, out: s }
  b: *step
`)))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if p.Steps["b"].Action != "x" || p.Steps["b"].DLQ != "b.dlq" {
		t.Errorf("aliased step = %+v", p.Steps["b"])
	}
}

func TestParseErrorFormat(t *testing.T) {
	t.Parallel()
	if got := (&ParseError{Line: 3, Msg: "boom"}).Error(); got != "line 3: boom" {
		t.Errorf("got %q", got)
	}
}

func assertErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want one containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not contain %q", err, want)
	}
}
