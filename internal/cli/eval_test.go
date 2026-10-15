package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// writeEvals writes eval cases under the fake action of a copy of the run fixture and returns the action directory.
func writeEvals(t *testing.T, pkg string, cases map[string]string) string {
	t.Helper()
	action := filepath.Join(pkg, "actions", "fake")
	for name, body := range cases {
		p := filepath.Join(action, "evals", name)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return action
}

// Test_evalCommand: eval records the judge against a fake model, then replays it with the model gone, prints a
// score per case and the total, records the scores with --db and shows each case's previous score; a failing
// assertion exits 1.
func Test_evalCommand(t *testing.T) {
	t.Parallel()
	verdict, _ := json.Marshal(map[string]any{
		"content": []map[string]string{{"type": "text", "text": "```json\n{\"score\": 0.5, \"reason\": \"Two of them.\"}\n```"}},
		"usage":   map[string]int{"input_tokens": 10, "output_tokens": 5},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(verdict) }))
	defer srv.Close()
	pkg := runPackage(t, "run")
	action := writeEvals(t, pkg, map[string]string{
		"keyed.yaml": "config: { mode: keyed, n: 1 }\nassert: [output == 0]\n",
		"judged.yaml": "config: { mode: keyed, n: 2 }\nassert: [size(output) == 2]\n" +
			"judge: { rubric: Two outputs., model: claude-haiku-5, apiKeySecret: judgeKey, baseUrl: \"" + srv.URL + "\" }\n",
	})
	env := map[string]string{"MILLSTREAM_SECRET_JUDGE_KEY": "sk-judge"}
	db := dbPath(t)
	code, out, errOut := runCLICtx(t, context.Background(), env, "eval", action, "--harness-mode", "record")
	if code != 0 || !regexp.MustCompile(`judged\s+1/1\s+0\.50\s+\$0\s+0\.75\nkeyed\s+1/1\s+-\s+\$0\s+1\.00\nTOTAL\s+0\.88\n`).
		MatchString(out) || !strings.Contains(out, "judged: judge: Two of them.") {
		t.Fatalf("record: code %d\n%s\n%s", code, out, errOut)
	}
	if _, err := os.Stat(filepath.Dir(db)); err == nil {
		t.Fatal("eval without --db created the state database")
	}
	srv.Close()
	for i, want := range []string{`keyed\s+1/1\s+-\s+\$0\s+1\.00\s+-\n`, `keyed\s+1/1\s+-\s+\$0\s+1\.00\s+1\.00\n`} {
		out, _ := mustRun(t, 0, "--db", db, "eval", action)
		if !regexp.MustCompile(want).MatchString(out) {
			t.Fatalf("replay %d:\n%s", i, out)
		}
	}
	st, err := sqlite.Open(db, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := st.EvalResults(context.Background(), "fake")
	if cerr := st.Close(); err == nil {
		err = cerr
	}
	if err != nil || len(results) != 4 || !strings.HasPrefix(results[0].ActionDigest, "sha256:") {
		t.Fatalf("EvalResults = %+v, %v", results, err)
	}

	writeEvals(t, pkg, map[string]string{"wrong.yaml": "config: { mode: keyed, n: 1 }\nassert: [output == 1]\n"})
	out, _ = mustRun(t, 1, "eval", action, "--case", "wrong")
	if !strings.Contains(out, "wrong: failed: output == 1") || strings.Contains(out, "keyed") {
		t.Fatalf("failing case:\n%s", out)
	}
	writeEvals(t, pkg, map[string]string{"down.yaml": "config: { mode: error }\nassert: [\"true\"]\n"})
	out, _ = mustRun(t, 1, "eval", action, "--case", "down", "--package", pkg)
	if !strings.Contains(out, "down: error: the action failed: portal answered 503") {
		t.Fatalf("failing action:\n%s", out)
	}
}

// Test_evalCommandUsage: what eval cannot run exits 2 with why.
func Test_evalCommandUsage(t *testing.T) {
	t.Parallel()
	pkg := runPackage(t, "run")
	action := writeEvals(t, pkg, map[string]string{"keyed.yaml": "config: { mode: keyed, n: 1 }\nassert: [output == 0]\n"})
	lone := t.TempDir()
	writeEvals(t, lone, map[string]string{"x.yaml": "assert: [\"true\"]\n"})
	tests := []struct {
		argv []string
		code int
		want string
	}{
		{[]string{"eval", action, "--case", "nope"}, 2, "no eval case to run"},
		{[]string{"eval", filepath.Join(pkg, "actions")}, 2, "no eval case to run"},
		{[]string{"eval", action, "--package", t.TempDir()}, 2, "is not inside the package"},
		{[]string{"eval", filepath.Join(lone, "actions", "fake")}, 2, "name the package with --package"},
		{[]string{"eval", action, "--harness-mode", "dry"}, 2, "--harness-mode"},
		{[]string{"eval", filepath.Join(lone, "actions", "fake"), "--package", lone}, 1, "no pipeline.yaml"},
	}
	for _, tt := range tests {
		code, _, errOut := runCLICtx(t, context.Background(), nil, tt.argv...)
		if code != tt.code || !strings.Contains(errOut, tt.want) {
			t.Errorf("%v: code %d, stderr %q; want %d, %q", tt.argv, code, errOut, tt.code, tt.want)
		}
	}
}
