package cli

import (
	"strings"
	"testing"
)

func Test_graphCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		argv     []string
		wantCode int
		want     []string // stdout must contain each
		wantErr  string
	}{
		{"mermaid by default", []string{"graph", fixture("valid")}, 0, []string{
			"flowchart LR\n", `n0["crawl"]`, `-->|"listings.good"|`, `-->|"score.dlq"|`, `(["trigger.daily"])`,
		}, ""},
		{"dot", []string{"graph", "--format", "dot", fixture("valid")}, 0, []string{
			`digraph "listings" {`, `"step:crawl" -> "step:score" [label="listings.raw"];`,
		}, ""},
		{"unknown format", []string{"graph", "--format", "svg", fixture("valid")}, 2, nil, "--format must be one of"},
		{"load error", []string{"graph", fixture("no-in")}, 1, nil, "steps.a.in: is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			code, out, errOut := runCLI(t, "dev", tt.argv...)
			if code != tt.wantCode {
				t.Fatalf("code = %d, want %d; stderr %q", code, tt.wantCode, errOut)
			}
			for _, w := range tt.want {
				if !strings.Contains(out, w) {
					t.Fatalf("stdout misses %q:\n%s", w, out)
				}
			}
			if !strings.Contains(errOut, tt.wantErr) {
				t.Fatalf("stderr = %q, want %q", errOut, tt.wantErr)
			}
		})
	}
}
