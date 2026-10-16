package eval

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"sync/atomic"
	"testing"

	"github.com/monolithiclab/millstream/internal/agent/agenttest"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest/fakeexec"
)

func TestMain(m *testing.M) {
	// The tests allocate heavily: a higher collector target trades memory for test time, as in the store tests.
	debug.SetGCPercent(400)
	code := m.Run()
	fakeexec.Cleanup()
	os.Exit(code)
}

const (
	testPipeline = "pipeline: p\nsecrets: [apiToken]\ntriggers: { go: { manual: {} } }\nsteps:\n" +
		"  echo: { action: ./actions/echo, in: trigger.go }\n  score: { action: ./actions/score, in: trigger.go }\n"
	echoManifest  = "name: lab/echo\nversion: 1.0.0\nkind: exec\nruntime: { exec: { command: [./fakeaction] } }\n"
	agentManifest = `name: lab/score
kind: agent
output: { type: object, properties: { score: { type: integer } }, required: [score] }
runtime: { agent: { harness: api, model: claude-sonnet-5-5, apiKeySecret: apiToken, baseUrl: "%s" } }
`
)

// writePackage writes a package with the exec action ./actions/echo (the fake action, once placeFake placed it) and the agent action
// ./actions/score calling baseURL, plus files, and returns its directory.
func writePackage(t *testing.T, baseURL string, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	all := map[string]string{
		"pipeline.yaml": testPipeline, "actions/echo/action.yaml": echoManifest,
		"actions/score/action.yaml": fmt.Sprintf(agentManifest, baseURL),
		"actions/score/prompt.md":   "Score {{ .Payload.url }}.\n",
	}
	for name, body := range files {
		all[name] = body
	}
	for name, body := range all {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// placeFake places the fake action as the echo action's command: a stub the in-process launcher ignores in the
// default build, the built binary in the e2e build.
func placeFake(t *testing.T, dir string) {
	t.Helper()
	if err := fakeexec.Place(filepath.Join(dir, "actions", "echo", "fakeaction")); err != nil {
		t.Fatal(err)
	}
}

// pkgOptions are the options of a run of the action ref of pkg.
func pkgOptions(t *testing.T, pkg *pipeline.Package, ref, mode, recordings string) Options {
	t.Helper()
	act, err := pipeline.Resolver{}.Resolve(pkg, ref)
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"PATH": os.Getenv("PATH"), "MILLSTREAM_SECRET_API_TOKEN": "sk-test"}
	return Options{Package: pkg, Action: act, Ref: ref, Mode: mode, Dir: recordings,
		Env: func(k string) (string, bool) { v, ok := env[k]; return v, ok }, Launchers: fakeexec.Launchers()}
}

// echoCase builds writePackage's package and returns it with the only eval case of its echo action: in memory when
// the fake action runs in-process, which needs no file; on disk with the fake action binary otherwise (e2e build).
func echoCase(t *testing.T, baseURL string, files map[string]string) (*pipeline.Package, *Case) {
	t.Helper()
	if fakeexec.InProcess {
		all := map[string]string{"actions/echo/fakeaction": ""} // the command, which the launcher never runs
		maps.Copy(all, files)
		pkg := memPackage(t, baseURL, all)
		return pkg, memCase(t, pkg, "actions/echo")
	}
	dir := writePackage(t, baseURL, files)
	placeFake(t, dir)
	pkg, err := pipeline.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	return pkg, loadOne(t, filepath.Join(dir, "actions", "echo"))
}

// loadOne loads the only case of the action directory.
func loadOne(t *testing.T, dir string) *Case {
	t.Helper()
	cases, err := Load(dir)
	if err != nil || len(cases) != 1 {
		t.Fatalf("Load = %v, %v", cases, err)
	}
	return cases[0]
}

// fakeModel is a Messages API answering every request with text, in process, and counting the requests.
func fakeModel(t *testing.T, text string) (*model, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	body, _ := json.Marshal(map[string]any{
		"id": "msg_test", "type": "message", "role": "assistant",
		"content": []map[string]string{{"type": "text", "text": text}},
		"usage":   map[string]int{"input_tokens": 1000, "output_tokens": 100},
	})
	return &model{URL: agenttest.URL, Client: agenttest.Client(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))}, calls
}

// model is a fake provider: its URL for manifests and judges, and the client that reaches it.
type model struct {
	URL    string
	Client *http.Client
}

// memPackage builds writePackage's package in memory, for the tests of agent actions, which run in this process
// in every build and need no file.
func memPackage(t *testing.T, baseURL string, files map[string]string) *pipeline.Package {
	t.Helper()
	all := map[string][]byte{
		"pipeline.yaml": []byte(testPipeline), "actions/echo/action.yaml": []byte(echoManifest),
		"actions/score/action.yaml": []byte(fmt.Sprintf(agentManifest, baseURL)),
		"actions/score/prompt.md":   []byte("Score {{ .Payload.url }}.\n"),
	}
	for name, body := range files {
		all[name] = []byte(body)
	}
	pkg, err := pipeline.FromFiles("/pkg", all)
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

// memCase parses the only eval case of the action directory dir of pkg, as Load reads it from disk.
func memCase(t *testing.T, pkg *pipeline.Package, dir string) *Case {
	t.Helper()
	var names []string
	for name := range pkg.Files {
		if path.Dir(name) == dir+"/"+Dir && path.Ext(name) == ".yaml" {
			names = append(names, name)
		}
	}
	cases, err := parseCases(names, func(name string) ([]byte, error) { return pkg.Files[name], nil })
	if err != nil || len(cases) != 1 {
		t.Fatalf("parseCases = %v, %v", cases, err)
	}
	return cases[0]
}
