package pipeline

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestInsightsConfig(t *testing.T) {
	t.Parallel()
	all := []string{InsightSlowStep, InsightFailureCluster, InsightCost, InsightUnschedulable}
	tests := []struct {
		name, yaml string
		every      time.Duration
		kinds      []string
		err        string
	}{
		{"defaults", "", DefaultInsightsEvery, all, ""},
		{"every and disable", "insights: { every: 1h, disable: [cost, drift] }", time.Hour,
			[]string{InsightSlowStep, InsightFailureCluster, InsightUnschedulable}, ""},
		{"days", "insights: { every: 1d, enabled: true }", 24 * time.Hour, all, ""},
		{"off", "insights: { enabled: false }", DefaultInsightsEvery, nil, ""},
		{"enabled not a boolean", "insights: { enabled: yes please }", 0, nil, "insights.enabled: want a boolean"},
		{"bad every", "insights: { every: soon }", 0, nil, "insights.every: invalid duration"},
		{"every not a string", "insights: { every: 5 }", 0, nil, "insights.every: want a duration"},
		{"disable not a list", "insights: { disable: cost }", 0, nil, "insights.disable: want a list"},
		{"unknown kind", "insights: { disable: [slowness] }", 0, nil, "slowness is not an insight kind"},
		{"unknown key", "insights: { window: 7 }", 0, nil, "insights.window: unknown key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Parse([]byte(checkHeader + "steps: { a: { action: ./a, in: trigger.go } }\n" + tt.yaml + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := p.InsightsConfig()
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("error %v, want %q", err, tt.err)
				}
				return
			}
			if err != nil || cfg.Every != tt.every || !slices.Equal(cfg.Kinds(all), tt.kinds) {
				t.Fatalf("config = %+v, kinds %v, %v", cfg, cfg.Kinds(all), err)
			}
		})
	}
}

func TestCheckInsights(t *testing.T) {
	t.Parallel()
	pkg := checkPackage(t, checkHeader+"insights: { disable: [nope] }\nsteps:\n  a: { action: ./actions/notify, in: trigger.go }\n")
	diags := Check(pkg, Resolver{}, func(string) (string, bool) { return "", false }, CheckOptions{})
	if !slices.ContainsFunc(diags, func(d Diagnostic) bool {
		return d.Severity == SeverityError && d.Path == "insights" && strings.Contains(d.Message, "nope")
	}) {
		t.Fatalf("diagnostics = %v", diags)
	}
}
