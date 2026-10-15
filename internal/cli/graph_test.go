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
		cmd      runner // run directly instead of argv
		wantCode int
		want     []string // stdout must contain each
		wantErr  string
	}{
		{"mermaid by default", nil, GraphCmd{Dir: fixture("valid"), Format: "mermaid"}, 0, []string{
			"flowchart LR\n", `n0["crawl"]`, `-->|"listings.good"|`, `-->|"score.dlq"|`, `(["trigger.daily"])`,
		}, ""},
		{"dot", []string{"graph", "--format", "dot", fixture("valid")}, nil, 0, []string{
			`digraph "listings" {`, `"step:crawl" -> "step:score" [label="listings.raw"];`,
		}, ""},
		{"unknown format", []string{"graph", "--format", "svg", fixture("valid")}, nil, 2, nil, "--format must be one of"},
		{"load error", nil, GraphCmd{Dir: fixture("no-in"), Format: "mermaid"}, 1, nil, "steps.a.in: is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			run := func() (int, string, string) { return runCLI(t, "dev", tt.argv...) }
			if tt.cmd != nil {
				run = func() (int, string, string) { return direct(nil, tt.cmd) }
			}
			code, out, errOut := run()
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
