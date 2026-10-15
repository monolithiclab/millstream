package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func Test_testPipelineCommand(t *testing.T) {
	t.Parallel()
	pkg := runPackage(t, "stubbed")
	out, errOut := mustRun(t, 1, "test", "pipeline", pkg)
	want := "PASS stubbed crawl, real score\nFAIL failing crawl\n  stream listings: 0 items, want 1\n"
	if out != want || !strings.HasSuffix(errOut, "listings: 1 passed, 1 failed\n") {
		t.Fatalf("stdout:\n%s\nwant:\n%s\nstderr:\n%s", out, want, errOut)
	}

	if err := os.Remove(filepath.Join(pkg, "tests", "b-fail.yaml")); err != nil {
		t.Fatal(err)
	}
	out, _ = mustRun(t, 0, "test", "pipeline", pkg)
	if out != "PASS stubbed crawl, real score\n" {
		t.Fatalf("stdout = %q", out)
	}
}

func Test_testPipelineErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		test    string // tests/x.yaml content; empty for no tests directory
		wantOut string
		wantErr string
	}{
		{"no tests", "", "", "no tests/*.yaml"},
		{"bad yaml", "name: [", "", "tests/x.yaml: yaml:"},
		{"unknown step", "stubs: { nope: { outputs: [] } }", "FAIL tests/x.yaml\n  stub for step nope: pipeline listings has no such step\n", ""},
		{"unknown trigger", "trigger: weekly", "FAIL tests/x.yaml\n  fire listings.weekly: ...", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pkg := runPackage(t, "run")
			if tt.test != "" {
				if err := os.MkdirAll(filepath.Join(pkg, "tests"), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(pkg, "tests", "x.yaml"), []byte(tt.test), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			code, out, errOut := runCLIEnv(t, "dev", nil, "test", "pipeline", pkg)
			if prefix, ok := strings.CutSuffix(tt.wantOut, "..."); ok {
				if !strings.HasPrefix(out, prefix) {
					t.Fatalf("stdout = %q, want prefix %q", out, prefix)
				}
			} else if out != tt.wantOut {
				t.Fatalf("stdout = %q, want %q", out, tt.wantOut)
			}
			if !strings.Contains(errOut, tt.wantErr) || (tt.wantOut != "PASS tests/x.yaml\n") != (code == 1) {
				t.Fatalf("code %d, stderr %q, want %q", code, errOut, tt.wantErr)
			}
		})
	}
	code, _, errOut := runCLIEnv(t, "dev", nil, "test", "pipeline", fixture("unknown-stream"))
	if code != 1 || !strings.Contains(errOut, "stream listing is read but no step writes it") {
		t.Fatalf("check errors: code %d, stderr %q", code, errOut)
	}
}
