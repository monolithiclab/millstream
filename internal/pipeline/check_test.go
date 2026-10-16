package pipeline

import (
	"strings"
	"testing"
)

// checkActions are local actions shared by the Check tests.
var checkActions = [][2]string{
	{"actions/crawl/action.yaml", `name: crawl
kind: exec
effects: none
output: { $ref: schema/output.json }
config:
  type: object
  properties:
    city: { type: string }
    maxPrice: { type: integer }
    token: { type: string, minLength: 20 }
    tags: { type: array, items: { type: string } }
  required: [city]
  additionalProperties: false
runtime: { exec: { command: [./crawl.sh] } }
`},
	{"actions/crawl/crawl.sh", "#!/bin/sh\n"},
	{"actions/crawl/schema/output.json", `{"type": "object", "properties": {"url": {"type": "string"},
	  "price": {"type": "integer"}}, "required": ["url"]}`},
	{"actions/score/action.yaml", `name: score
kind: exec
input: { type: object, properties: { url: { type: string }, price: { type: number } }, required: [url, price] }
output: { type: object, properties: { score: { type: number } }, required: [score] }
runtime: { exec: { command: [score] } }
`},
	{"actions/agent/action.yaml", "name: agent\nkind: agent\noutput: { type: object }\n" +
		"runtime: { agent: { harness: api, model: m, network: [api.anthropic.com] } }\n"},
	{"actions/agent/prompt.md", "Score {{ .Payload }}.\n"},
	{"actions/keyed/action.yaml", "name: keyed\nkind: agent\noutput: { type: object }\n" +
		"runtime: { agent: { harness: claude-code, model: m, apiKeySecret: anthropicKey } }\n"},
	{"actions/keyed/prompt.md", "Score.\n"},
	{"actions/notify/action.yaml", "name: notify\nkind: exec\neffects: external\nruntime: { exec: { command: [notify] } }\n"},
}

func checkPackage(t *testing.T, pipelineYAML string) *Package {
	t.Helper()
	return memPackage(t, append([][2]string{{"pipeline.yaml", pipelineYAML}}, checkActions...)...)
}

const checkHeader = "pipeline: p\ntriggers:\n  go: { manual: {} }\n"

func TestCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		yaml string
		env  map[string]string
		want []string
	}{
		{
			name: "valid",
			yaml: checkHeader + `vars: { city: Lyon, maxPrice: 400000 }
secrets: [portalToken]
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.go
    out: listings
    config: { city: "${vars.city}", maxPrice: "${vars.maxPrice}", token: "${secrets.portalToken}" }
  score: { action: ./actions/score, in: listings, out: [scored, audit] }
  sink: { action: ./actions/notify, in: [scored, audit] }
`,
			env: map[string]string{"MILLSTREAM_SECRET_PORTAL_TOKEN": "x"},
			want: []string{
				"error steps.score.in[0]: output of step crawl on stream listings against the input schema of " +
					"./actions/score at /properties/price: the consumer requires property \"price\", which the producer declares but does not require",
			},
		},
		{
			name: "references",
			yaml: `pipeline: p
secrets: [missing]
triggers: { go: { manual: {} }, idle: { manual: {} } }
streams:
  trigger.nope: {}
  trigger.go: { schema: { type: object } }
  orphan: { retention: 1d }
steps:
  a: { action: ./actions/notify, in: [trigger.ghost, nowhere], out: [trigger.go, written], dlq: trigger.x }
`,
			want: []string{
				"error secrets[0]: secret missing is not set: set MILLSTREAM_SECRET_MISSING",
				"error steps.a.dlq: stream trigger.x is a trigger stream: only its trigger writes it",
				"error steps.a.in[0]: there is no trigger ghost",
				"error steps.a.in[1]: stream nowhere is read but no step writes it",
				"error steps.a.out[0]: stream trigger.go is a trigger stream: only its trigger writes it",
				"warning steps.a.out[1]: stream written is written by step a but no step reads it",
				"error streams.orphan: stream orphan is declared but no step reads or writes it",
				"error streams.trigger.go: a trigger stream is written by its trigger only: set the trigger's inputSchema instead",
				"error streams.trigger.nope: the trigger. prefix is reserved for trigger streams and there is no trigger nope",
				"warning triggers.go: no step reads trigger.go",
				"warning triggers.idle: no step reads trigger.idle",
			},
		},
		{
			name: "actions and routes",
			yaml: checkHeader + `steps:
  a:
    action: lab/crawl@1
    in: trigger.go
    out:
      - { when: "item.city == 'Lyon'", out: lyon }
      - { when: "item.city +", out: other }
      - { when: "secrets.x == ''", out: other }
  b: { action: ./actions/missing, in: [lyon, other] }
  c: { action: frobnicate, in: lyon }
  d: { action: ../outside, in: lyon }
`,
			want: []string{
				"warning steps.a.action: registry action lab/crawl@1: no registries are configured: not available in this build",
				"error steps.a.out[1].when: compile \"item.city +\": ERROR: <input>:1:12: Syntax error: ...",
				"error steps.a.out[2].when: compile \"secrets.x == ''\": secrets are only available in config interpolation",
				"error steps.b.action: action ./actions/missing: no actions/missing/action.yaml in the package",
				"error steps.c.action: unknown action \"frobnicate\": want a builtin (filter, dedupe, review), a local path " +
					"(./actions/x) or a registry reference (namespace/name@version)",
				"error steps.d.action: unknown action \"../outside\": want a builtin (filter, dedupe, review), a local path " +
					"(./actions/x) or a registry reference (namespace/name@version)",
			},
		},
		{
			name: "cycles",
			yaml: checkHeader + `steps:
  a: { action: ./actions/notify, in: [trigger.go, c.out], out: a.out }
  b: { action: ./actions/notify, in: a.out, out: b.out }
  c: { action: ./actions/notify, in: b.out, out: c.out }
  self: { action: ./actions/notify, in: [trigger.go, self.dlq], out: x }
  x: { action: ./actions/notify, in: x }
`,
			want: []string{
				"error steps.a: cycle in the step graph: a -> b -> c -> a",
				"error steps.self: cycle in the step graph: self -> self",
			},
		},
		{
			name: "config",
			yaml: checkHeader + `vars: { city: Lyon, maxPrice: 400000, required: null, n: 3 }
secrets: [portalToken]
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.go
    out: listings
    config:
      city: "${vars.n}"
      maxPrice: "${vars.required}"
      token: "${secrets.portalToken}"
      tags: ["${vars.city}", "${vars.maxPrice}", "${vars.city + 1}", "${secrets.other}", "${vars.ghost}", "${run.id}x", "${vars"]
      extra: true
  sink: { action: ./actions/notify, in: listings, config: { anything: 1 } }
`,
			env: map[string]string{"MILLSTREAM_SECRET_PORTAL_TOKEN": "x"},
			want: []string{
				"error steps.crawl.config: does not match the config schema of ./actions/crawl: additional properties 'extra' not allowed",
				"error steps.crawl.config.city: does not match the config schema of ./actions/crawl: got number, want string",
				"error steps.crawl.config.tags[1]: does not match the config schema of ./actions/crawl: got number, want string",
				"error steps.crawl.config.tags[2]: with the default variables: evaluate \"vars.city + 1\": no such overload",
				"error steps.crawl.config.tags[3]: secret other is not declared under secrets",
				"error steps.crawl.config.tags[4]: variable ghost is not declared under vars",
				"error steps.crawl.config.tags[6]: unterminated ${ at offset 0",
				"warning steps.sink.config: action ./actions/notify declares no config schema: config is not checked",
			},
		},
		{
			name: "builtins",
			yaml: checkHeader + `steps:
  src: { action: ./actions/crawl, in: trigger.go, out: raw, config: { city: x } }
  keep: { action: filter, in: raw, out: kept, config: { expr: "item.price <", typo: 1 } }
  keep2: { action: filter, in: raw, out: kept, config: { expr: 3 } }
  keep3: { action: filter, in: raw, out: kept }
  dedupe: { action: dedupe, in: kept, out: unique, config: { key: "item.url", window: forever } }
  dedupe2: { action: dedupe, in: kept, out: unique, config: { key: "secrets.x", window: 3 } }
  review: { action: review, in: unique, out: approved, config: { autoApprove: "item.score >" } }
  score: { action: ./actions/score, in: approved }
`,
			want: []string{
				"error steps.dedupe.config.window: invalid duration \"forever\"",
				"error steps.dedupe2.config.key: compile \"secrets.x\": secrets are only available in config interpolation",
				"error steps.dedupe2.config.window: must be a duration such as 30d",
				"error steps.keep.config.expr: compile \"item.price <\": ERROR: <input>:1:13: Syntax error: ...",
				"error steps.keep.config.typo: unknown key for the filter builtin",
				"error steps.keep2.config.expr: must be a CEL expression",
				"error steps.keep3.config.expr: is required by the filter builtin",
				"error steps.review.config.autoApprove: compile \"item.score >\": ERROR: <input>:1:13: Syntax error: ...",
				"error steps.score.in[0]: output of step src on stream approved against the input schema of " +
					"./actions/score at /properties/price: the consumer requires property \"price\", which the producer declares but does not require",
			},
		},
		{
			name: "agent reaching external effects",
			yaml: checkHeader + `steps:
  agent: { action: ./actions/agent, in: trigger.go, out: [scored, other] }
  keep: { action: filter, in: scored, out: kept, config: { expr: "true" } }
  send: { action: ./actions/notify, in: kept }
  gate: { action: review, in: other, out: approved, config: { inbox: a } }
  auto: { action: review, in: other, out: auto, config: { autoApprove: "true" } }
  send2: { action: ./actions/notify, in: [approved, auto] }
`,
			want: []string{
				"warning steps.agent: agent output can reach step send, whose action has external effects, without a " +
					"review step that has no autoApprove in between",
				"warning steps.agent: agent output can reach step send2, whose action has external effects, without a " +
					"review step that has no autoApprove in between",
			},
		},
		{
			name: "agent needs",
			yaml: checkHeader + `steps:
  agent: { action: ./actions/keyed, in: trigger.go }
`,
			want: []string{
				"error steps.agent.action: action ./actions/keyed reads its API key from secret anthropicKey, which is " +
					"not declared under secrets",
				"warning steps.agent.action: action ./actions/keyed declares no runtime.agent.network: in its sandbox it " +
					"cannot reach its model; list the model's endpoint there (api.anthropic.com)",
			},
		},
		{
			name: "agent under a budget without maxCostUsd",
			yaml: "pipeline: p\nbudget: { dayUsd: 5 }\ntriggers:\n  go: { manual: {} }\n" + `steps:
  agent: { action: ./actions/agent, in: trigger.go }
`,
			want: []string{
				"error steps.agent.action: agent step agent: runtime.agent.maxCostUsd is required when the pipeline " +
					"sets budget.runUsd or budget.dayUsd",
			},
		},
		{
			name: "imports and unresolved steps",
			yaml: checkHeader + `streams:
  approved: { pipeline: other, stream: listings.approved }
steps:
  agent: { action: ./actions/agent, in: [trigger.go, approved], out: [drafts, approved], dlq: approved }
  ghost: { action: ./actions/ghost, in: drafts, out: sent }
  send: { action: ./actions/notify, in: sent, config: { n: .nan } }
`,
			want: []string{
				"error steps.agent.dlq: stream approved is imported from pipeline other: only the import writes it",
				"error steps.agent.out[1]: stream approved is imported from pipeline other: only the import writes it",
				"error steps.ghost.action: action ./actions/ghost: no actions/ghost/action.yaml in the package",
				"warning steps.send.config: action ./actions/notify declares no config schema: config is not checked",
			},
		},
		{
			name: "trigger input schema",
			yaml: `pipeline: p
triggers:
  go: { manual: {}, inputSchema: { type: object, properties: { url: { type: string } } } }
  bad: { manual: {}, inputSchema: { $ref: missing.json } }
steps:
  score: { action: ./actions/score, in: [trigger.go, trigger.bad] }
`,
			want: []string{
				"error steps.score.in[0]: trigger go on stream trigger.go against the input schema of ./actions/score at " +
					"/properties/price: the consumer requires property \"price\", which the producer does not declare",
				"error steps.score.in[0]: trigger go on stream trigger.go against the input schema of ./actions/score at " +
					"/properties/url: the consumer requires property \"url\", which the producer declares but does not require",
				"error triggers.bad.inputSchema: schema file missing.json is not in the package",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkg := checkPackage(t, tt.yaml)
			got := Check(pkg, Resolver{}, envOf(tt.env), CheckOptions{})
			lines := make([]string, len(got))
			for i, d := range got {
				lines[i] = d.String()
			}
			var kept []string // builtin warnings are pinned by TestCheckBuiltinWarning
			for _, l := range lines {
				if !strings.Contains(l, "builtin arrives in") {
					kept = append(kept, l)
				}
			}
			if !matchLines(kept, tt.want) {
				t.Fatalf("got:\n%s\nwant:\n%s", strings.Join(kept, "\n"), strings.Join(tt.want, "\n"))
			}
			if HasErrors(got) != strings.Contains(strings.Join(tt.want, "\n"), "error ") {
				t.Fatalf("HasErrors = %v", HasErrors(got))
			}
		})
	}
}

// Every review key is checked: a bad value of each is located by its key, and a valid config passes.
func TestCheckReviewConfig(t *testing.T) {
	t.Parallel()
	pkg := checkPackage(t, checkHeader+`secrets: [hook]
steps:
  keep: { action: filter, in: trigger.go, out: kept, config: { expr: "true" } }
  gate:
    action: review
    in: kept
    out: approved
    config: { inbox: "a b", timeout: soon, editable: [notes, 3], notify: "ftp://x", display: "{{.item", reasons: [x, x],
      autoApprove: "item.n >", sampleRate: 0.001, typo: 1 }
  late: { action: review, in: kept, out: other, config: { onTimeout: approve, notify: { secret: other } } }
  bad: { action: review, in: kept, out: third, config: { timeout: 1h, onTimeout: { escalate: "a/b" }, notify: { url: x } } }
  hidden: { action: review, in: kept, out: fifth, config: { notify: { secret: hook } } }
  good:
    action: review
    in: kept
    out: fourth
    config: { inbox: analysts, timeout: 72h, onTimeout: { escalate: leads }, editable: [/notes], notify: "https://hooks.example/x",
      display: "{{.item.n}}", reasons: [overpriced], autoApprove: "item.n > 3", sampleRate: 0.5 }
`)
	var got []string
	for _, d := range Check(pkg, Resolver{}, envOf(map[string]string{"MILLSTREAM_SECRET_HOOK": "https://hooks.example/t"}), CheckOptions{}) {
		if d.Severity == SeverityError { // the streams no step reads warn
			got = append(got, d.String())
		}
	}
	want := []string{
		"error steps.bad.config.notify: must be an http or https URL or { secret: <name> }",
		"error steps.bad.config.onTimeout: must be reject, approve or { escalate: <inbox> }",
		"error steps.gate.config.autoApprove: compile \"item.n >\": ERROR: <input>:1:9: Syntax error: ...",
		"error steps.gate.config.display: template: display:1: unclosed action",
		"error steps.gate.config.editable[0]: must be a JSON pointer such as /notes",
		"error steps.gate.config.editable[1]: must be a non-empty string",
		"error steps.gate.config.inbox: an inbox name is letters, digits, dots, dashes and underscores",
		"error steps.gate.config.notify: must be an http or https URL or { secret: <name> }",
		"error steps.gate.config.reasons[1]: reason code x is listed twice",
		"error steps.gate.config.sampleRate: must be at least 0.01, so the rule is always measured",
		"error steps.gate.config.timeout: invalid duration \"soon\"",
		"error steps.gate.config.typo: unknown key for the review builtin",
		"error steps.late.config.notify: secret other is not declared under secrets",
		"error steps.late.config.onTimeout: needs a timeout",
	}
	for i := range got {
		if j := strings.Index(got[i], "Syntax error:"); j >= 0 {
			got[i] = got[i][:j] + "Syntax error: ..."
		}
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestValuePath(t *testing.T) {
	t.Parallel()
	v := map[string]any{"a": []any{map[string]any{"b/c": 1}}, "s": "x"}
	tests := []struct{ ptr, want string }{
		{"", "cfg"},
		{"/a/0/b~1c", "cfg.a[0].b/c"},
		{"/a/7", "cfg.a[7]"},
		{"/s/deeper", "cfg.s.deeper"},
	}
	for _, tt := range tests {
		t.Run(tt.ptr, func(t *testing.T) {
			if got := valuePath("cfg", v, tt.ptr); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// matchLines compares got to want line by line; a want ending in "..." matches by prefix, for messages that embed
// a library's wording.
func matchLines(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i, w := range want {
		if prefix, ok := strings.CutSuffix(w, "..."); ok {
			if !strings.HasPrefix(got[i], prefix) {
				return false
			}
		} else if got[i] != w {
			return false
		}
	}
	return true
}

// brokenResolver resolves every reference to an action whose config schema does not compile.
type brokenResolver struct{}

func (brokenResolver) Resolve(_ *Package, ref string) (*Action, error) {
	return &Action{Ref: ref, Kind: KindExec, Config: []byte(`{"type": 3}`)}, nil
}

func TestCheckConfigSchemaErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		resolver ActionResolver
		config   string
		want     string
	}{
		{"schema does not compile", brokenResolver{}, "{ a: 1 }", "error steps.s.config: action x: config schema: compile schema..."},
		{"config does not encode", Resolver{}, "{ city: x, n: .nan }", "error steps.s.config: encode config: json: unsupported value: NaN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := "./actions/crawl"
			if _, ok := tt.resolver.(brokenResolver); ok {
				action = "x"
			}
			pkg := checkPackage(t, checkHeader+"steps:\n  s: { action: "+action+", in: trigger.go, config: "+tt.config+" }\n")
			got := Check(pkg, tt.resolver, envOf(nil), CheckOptions{})
			if len(got) != 1 || !matchLines([]string{got[0].String()}, []string{tt.want}) {
				t.Fatalf("got %v, want %q", got, tt.want)
			}
		})
	}
}
