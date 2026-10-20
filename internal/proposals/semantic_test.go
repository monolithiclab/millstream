package proposals

import (
	"errors"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// semanticBase is a pipeline with a secret, a variable, a threshold filter and a review.
const semanticBase = `pipeline: listings
vars: { minScore: 50, region: idf }
secrets: [tok]
triggers:
  adhoc: { manual: {} }
steps:
  score: { action: ./actions/scorer, in: trigger.adhoc, out: scored, config: { key: "${secrets.tok}" }, timeout: 1m }
  keep: { action: filter, in: scored, out: kept, config: { expr: "item.score > 50" } }
  check: { action: review, in: kept, out: ok, config: { inbox: a, reasons: [low] } }
`

// TestSemantic: what a config or topology proposal may change in the parsed pipeline file (spec section 11.7).
func TestSemantic(t *testing.T) {
	base, err := pipeline.Parse([]byte(semanticBase))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name       string
		kind       store.ProposalKind
		from, to   string
		refusedFor string // "" when allowed
	}{
		{"timeout", store.ProposalConfig, "timeout: 1m", "timeout: 2m, retry: { maxAttempts: 5 }, concurrency: 3", ""},
		{"threshold", store.ProposalConfig, `item.score > 50`, `item.score > 60`, ""},
		{"numeric variable", store.ProposalConfig, "minScore: 50", "minScore: 60", ""},
		{"review reasons", store.ProposalConfig, "reasons: [low]", "reasons: [low, far], autoApprove: { when: \"true\" }", ""},
		{"another action", store.ProposalConfig, "./actions/scorer", "./actions/other", "change step score"},
		{"review inbox", store.ProposalConfig, "inbox: a", "inbox: b", "change step check"},
		{"text variable", store.ProposalConfig, "region: idf", "region: paca", "change the variable region"},
		{"new step", store.ProposalConfig, "steps:\n", "steps:\n  x: { action: filter, in: ok, config: { expr: \"true\" } }\n",
			"add or remove a step"},
		{"trigger added", store.ProposalTopology, "adhoc: { manual: {} }", "adhoc: { manual: {} }\n  nightly: { cron: \"0 1 * * *\" }",
			"trigger"},
		{"secret declared", store.ProposalTopology, "secrets: [tok]", "secrets: [tok, other]", "declare the secret other"},
		{"secret read by another step", store.ProposalTopology, `expr: "item.score > 50"`,
			`expr: "item.score > 50", note: "${secrets.tok}"`, "make step keep read the secret tok"},
		{"notify secret", store.ProposalTopology, "inbox: a,", "inbox: a, notify: { secret: tok },",
			"make step check read the secret tok"},
		{"filter moved", store.ProposalTopology, "in: scored, out: kept", "in: scored, out: kept, concurrency: 2", ""},
	} {
		src := strings.Replace(semanticBase, c.from, c.to, 1)
		patched, err := pipeline.Parse([]byte(src))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		err = semantic(c.kind, base, patched)
		if c.refusedFor == "" {
			if err != nil {
				t.Errorf("%s: %v", c.name, err)
			}
			continue
		}
		if !errors.Is(err, patch.ErrPatch) || !strings.Contains(err.Error(), c.refusedFor) {
			t.Errorf("%s: %v, want a refusal for %q", c.name, err, c.refusedFor)
		}
	}
}
