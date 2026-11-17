package metrics

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// alertsFile is the sample rule file the deployment guide points to.
const alertsFile = "../../deploy/prometheus/alerts.yaml"

// Every metric a sample alert rule names is one the engine exposes, so renaming a metric breaks this test, not
// the alerting of a deployment that loaded the rules.
func TestAlertRulesNameExposedMetrics(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(alertsFile)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Groups []struct {
			Rules []struct{ Alert, Expr string }
		}
	}
	if err := yaml.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	exposed := exposedNames()
	rules := 0
	for _, g := range file.Groups {
		for _, r := range g.Rules {
			rules++
			names := exprMetrics(r.Expr)
			if len(names) == 0 {
				t.Errorf("rule %s: no metric found in %q", r.Alert, r.Expr)
			}
			for _, n := range names {
				if !exposed[n] {
					t.Errorf("rule %s names %s, which /metrics does not expose", r.Alert, n)
				}
			}
		}
	}
	if rules == 0 {
		t.Error("no rules in " + alertsFile)
	}
}

// exposedNames is every series name /metrics can serve for the engine's metrics, and the scrape's own up.
func exposedNames() map[string]bool {
	names := map[string]bool{"up": true}
	for name, kind := range kinds {
		exposed, _ := exposedName(name, kind)
		names[exposed] = true
		if kind == Histogram {
			for _, s := range []string{"_bucket", "_sum", "_count"} {
				names[exposed+s] = true
			}
		}
		if kind == Distribution {
			for _, s := range []string{"_sum", "_count"} {
				names[exposed+s] = true
			}
		}
	}
	return names
}

var (
	promString   = regexp.MustCompile(`"[^"]*"`)
	promMatchers = regexp.MustCompile(`\{[^}]*\}|\[[^\]]*\]`)
	promOffset   = regexp.MustCompile(`\boffset\s+\S+`)
	promGrouping = regexp.MustCompile(`\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)`)
	promIdent    = regexp.MustCompile(`[a-zA-Z_:][a-zA-Z0-9_:]*(\s*\()?`)
	promKeywords = []string{"and", "or", "unless", "bool", "offset", "by", "without", "on", "ignoring"}
)

// exprMetrics returns the metric names of a PromQL expression: its identifiers that are not function calls,
// keywords, label matchers, ranges, groupings or strings.
func exprMetrics(expr string) []string {
	expr = promString.ReplaceAllString(expr, "")
	expr = promOffset.ReplaceAllString(expr, "")
	expr = promGrouping.ReplaceAllString(expr, "")
	expr = promMatchers.ReplaceAllString(expr, "")
	var names []string
	for _, id := range promIdent.FindAllString(expr, -1) {
		if strings.HasSuffix(id, "(") || slices.Contains(promKeywords, id) {
			continue
		}
		names = append(names, id)
	}
	return names
}
