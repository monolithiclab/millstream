package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixture returns the absolute path of testdata/<name>: Kong makes a relative path argument absolute with a getwd,
// which costs more than the rest of a parse.
func fixture(name string) string { return filepath.Join(testdata, name) }

const syntaxError = `Syntax error: mismatched input '<EOF>'`

func Test_checkCommand(t *testing.T) {
	t.Parallel()
	secret := map[string]string{"MILLSTREAM_SECRET_PORTAL_TOKEN": "tok"}
	tests := []struct {
		fixture  string
		env      map[string]string
		wantCode int
		// wantOut lists stdout lines; one ending in "..." matches by prefix.
		wantOut []string
		wantErr string
	}{
		{"valid", secret, 0, nil, "listings: 0 errors, 0 warnings\n"},
		{"no-in", nil, 1, nil, "pipeline.yaml: line 4: steps.a.in: is required\n"},
		{"mixed-out", nil, 1, nil, "line 4: steps.a.out: a list holds either stream names or route objects, never both\n"},
		{"unknown-action", nil, 1, []string{
			`error steps.bogus.action: unknown action "frobnicate": want a builtin (filter, dedupe, review), a local path ` +
				`(./actions/x) or a registry reference (namespace/name@version)`,
			"error steps.local.action: action ./actions/missing: no actions/missing/action.yaml in the package",
			"warning steps.registry.action: registry action lab/score@2: needs Docker or Podman: not available in this build",
		}, "p: 2 errors, 1 warning\n"},
		{"unknown-trigger", nil, 1, []string{"error steps.a.in[1]: there is no trigger weekly"}, "p: 1 error, 0 warnings\n"},
		{"unknown-stream", nil, 1, []string{
			"warning steps.a.out: stream listings is written by step a but no step reads it",
			"error steps.b.in[0]: stream listing is read but no step writes it",
			"error streams.listings.typo: stream listings.typo is declared but no step reads or writes it",
		}, "p: 2 errors, 1 warning\n"},
		{"unread-output", nil, 0, []string{
			"warning steps.a.out[1]: stream audit is written by step a but no step reads it",
		}, "p: 0 errors, 1 warning\n"},
		{"bad-when", nil, 1, []string{
			`error steps.a.out[0].when: compile "item.score >= ": ERROR: <input>:1:15: ` + syntaxError + "...",
		}, "p: 1 error, 0 warnings\n"},
		{"out-trigger", nil, 1, []string{
			"error steps.a.out: stream trigger.go is a trigger stream: only its trigger writes it",
		}, "p: 1 error, 0 warnings\n"},
		{"reserved-prefix", nil, 1, []string{
			"error streams.trigger.weekly: the trigger. prefix is reserved for trigger streams and there is no trigger weekly",
		}, "p: 1 error, 0 warnings\n"},
		{"secrets", nil, 1, []string{
			"error secrets[0]: secret portalToken is not set: set MILLSTREAM_SECRET_PORTAL_TOKEN",
			"warning steps.a.config: action ./actions/a declares no config schema: config is not checked",
			"error steps.a.config.other: secret apiKey is not declared under secrets",
		}, "p: 2 errors, 1 warning\n"},
		{"cycle", nil, 1, []string{"error steps.a: cycle in the step graph: a -> b -> c -> a"}, "p: 1 error, 0 warnings\n"},
		{"config-schema", secret, 1, []string{
			"error steps.a.config: does not match the config schema of ./actions/a: additional properties 'unexpected' not allowed",
			"error steps.a.config: does not match the config schema of ./actions/a: missing property 'city'",
			"error steps.a.config.maxPrice: does not match the config schema of ./actions/a: got string, want integer",
		}, "p: 3 errors, 0 warnings\n"},
		{"builtin-cel", nil, 1, []string{
			`error steps.dedupe.config.key: compile "item.address +": ERROR: <input>:1:15: ` + syntaxError + "...",
			`error steps.keep.config.expr: compile "secrets.token == ''": secrets are only available in config interpolation`,
		}, "p: 2 errors, 0 warnings\n"},
		{"schema-compat", nil, 1, []string{
			"error steps.score.in[0]: output of step crawl on stream kept against the input schema of ./actions/score at " +
				`/properties/city: the consumer requires property "city", which the producer does not declare`,
			"error steps.score.in[0]: output of step crawl on stream kept against the input schema of ./actions/score at " +
				"/properties/price: the producer may emit null, which the consumer does not allow",
			"warning steps.score.in[0]: output of step crawl on stream kept against the input schema of ./actions/score at " +
				`/properties/kind: cannot prove the value is one of the consumer's enum ["flat", "house"]: the producer does not constrain it`,
		}, "p: 2 errors, 1 warning\n"},
		{"agent-external", nil, 0, []string{
			"warning steps.agent: agent output can reach step send, whose action has external effects, without a review " +
				"step that has no autoApprove in between",
		}, "p: 0 errors, 1 warning\n"},
	}
	// The default build checks a sample, enough for what the command owns: the exit code, the diagnostic lines and
	// the summary. The pipeline package tests the diagnostics themselves; the e2e build checks every fixture.
	sample := map[string]bool{"valid": true, "no-in": true, "unknown-action": true, "unread-output": true}
	for _, tt := range tests {
		if !e2eBuild && !sample[tt.fixture] {
			continue
		}
		t.Run(tt.fixture, func(t *testing.T) {
			t.Parallel()
			run := func() (int, string, string) { return direct(tt.env, CheckCmd{Dirs: []string{fixture(tt.fixture)}}) }
			if tt.fixture == "valid" { // through the parser; the others without
				run = func() (int, string, string) { return runCLIEnv(t, "dev", tt.env, "check", fixture(tt.fixture)) }
			}
			code, out, errOut := run()
			if code != tt.wantCode {
				t.Fatalf("code = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, tt.wantCode, out, errOut)
			}
			if !matchLines(splitLines(out), tt.wantOut) {
				t.Fatalf("stdout:\n%s\nwant:\n%s", out, strings.Join(tt.wantOut, "\n"))
			}
			if !strings.HasSuffix(errOut, tt.wantErr) {
				t.Fatalf("stderr = %q, want suffix %q", errOut, tt.wantErr)
			}
		})
	}
}

// The directory arguments default to the current directory. The e2e build also runs check there: internal/cli,
// whose testdata loading the package reads whole.
func Test_checkDefaultsToCurrentDirectory(t *testing.T) {
	t.Parallel()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if got := dirOrCurrent(""); got != wd {
		t.Fatalf("dirOrCurrent of nothing = %q, want %q", got, wd)
	}
	if got := dirOrCurrent("pkg"); got != "pkg" {
		t.Fatalf("dirOrCurrent(pkg) = %q", got)
	}
	if !e2eBuild {
		return
	}
	code, _, errOut := runCLI(t, "dev", "check")
	if code != 1 || !strings.Contains(errOut, "no pipeline.yaml") {
		t.Fatalf("code=%d err=%q", code, errOut)
	}
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

// matchLines compares got to want line by line; a want ending in "..." matches by prefix.
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

// Several packages are checked against each other too: an import is checked when its producer is among them, and
// lines carry the pipeline's name.
func Test_checkSeveralPackages(t *testing.T) {
	t.Parallel()
	noSchema := "warning steps.%s.config: action ./actions/fake declares no config schema: config is not checked\n"
	code, out, errOut := direct(nil, CheckCmd{Dirs: []string{fixture("run"), fixture("imports")}})
	if code != 0 || out != "listings "+fmt.Sprintf(noSchema, "crawl")+"listings "+fmt.Sprintf(noSchema, "score")+
		"deals "+fmt.Sprintf(noSchema, "notify") || errOut != "listings: 0 errors, 2 warnings\ndeals: 0 errors, 1 warning\n" {
		t.Fatalf("code %d\nstdout:\n%s\nstderr:\n%s", code, out, errOut)
	}
	code, out, _ = direct(nil, CheckCmd{Dirs: []string{fixture("imports")}})
	if code != 0 || out != fmt.Sprintf(noSchema, "notify")+"warning streams.approved: pipeline listings is not among "+
		"the packages checked: the import of its stream listings is not checked\n" {
		t.Fatalf("one consumer alone: code %d\n%s", code, out)
	}
}
