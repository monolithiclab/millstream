package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/improve"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// Test_improveRefusals: millstream improve, and run serving the API, run the improve pipeline only in development
// with improve.enabled and a harness; init pipeline --from improve copies the package to fork it.
func Test_improveRefusals(t *testing.T) {
	t.Parallel()
	dir := testdir.New(t)
	for _, c := range []struct{ env, want string }{
		{"environment: production\n", "runs in development deployments only"},
		{"environment: development\n", "improve.enabled"},
		{"environment: development\nimprove: { enabled: true }\n", "improve.harness"},
	} {
		file := filepath.Join(dir, "env.yaml")
		if err := os.WriteFile(file, []byte(c.env), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, errOut := mustRun(t, 1, "--db", dsn(unusedDB()), "--env-file", file, "improve", "--insight", "x"); !strings.Contains(errOut, c.want) {
			t.Errorf("%q: stderr %q, want %q", c.env, errOut, c.want)
		}
		if c.want == "improve.harness" {
			if _, errOut := mustRun(t, 1, "--db", dsn(unusedDB()), "--env-file", file, "run", dir); !strings.Contains(errOut,
				c.want) {
				t.Errorf("run with %q: stderr %q", c.env, errOut)
			}
		}
	}

	out, _ := mustRun(t, 0, "init", "pipeline", "my-improve", "--from", "improve", "--dir", dir)
	if !strings.Contains(out, "a copy of the improve package") {
		t.Errorf("init --from improve: %q", out)
	}
	p, err := os.ReadFile(filepath.Join(dir, "my-improve", "pipeline.yaml"))
	if err != nil || !strings.Contains(string(p), "\npipeline: my-improve\n") {
		t.Errorf("forked pipeline.yaml: %v\n%s", err, p)
	}
	if _, err := os.Stat(filepath.Join(dir, "my-improve", "actions", "draft", "prompt.md")); err != nil {
		t.Error(err)
	}
	mustRun(t, 2, "init", "pipeline", "x", "--from", "nope", "--dir", dir)
}

// Test_improveHost: the host materializes the package the environment configures, gives the improve pipeline its
// API's URL and the token it mints, and leaves every other pipeline's secrets as they were.
func Test_improveHost(t *testing.T) {
	t.Parallel()
	dir := testdir.New(t)
	rt := &Runtime{DB: dsn(filepath.Join(dir, "state", "millstream.db")), Env: func(string) (string, bool) { return "", false }}
	env, err := pipeline.ParseEnvironment([]byte("environment: development\nimprove: { enabled: true, harness: claude-code }\n"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := newImproveHost(rt, env, pipeline.ModeDevelopment)
	if err != nil || h == nil || h.pkg.Pipeline.Name != improve.Name {
		t.Fatalf("host %+v, %v", h, err)
	}
	base := func(string) pipeline.SecretLookup {
		return func(name string) (string, error) { return "base-" + name, nil }
	}
	st := memoryStore(t)
	e, err := engine.New(engine.Options{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.start(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	h.served("http://127.0.0.1:7070/")
	lookup := h.check(pipeline.CheckOptions{Secrets: base}).Secrets
	for _, c := range []struct{ pipeline, secret, want string }{
		{improve.Name, improve.SecretAPI, "http://127.0.0.1:7070/"},
		{improve.Name, improve.APIKeySecret, "base-" + improve.APIKeySecret},
		{"listings", improve.SecretAPI, "base-" + improve.SecretAPI},
	} {
		if got, _ := lookup(c.pipeline)(c.secret); got != c.want {
			t.Errorf("%s %s = %q, want %q", c.pipeline, c.secret, got, c.want)
		}
	}
	if token, _ := lookup(improve.Name)(improve.SecretToken); !strings.HasPrefix(token, "mspr_") {
		t.Errorf("token %q", token)
	}
	var none *improveHost
	if got, _ := none.secrets(base)(improve.Name)(improve.SecretAPI); got != "base-"+improve.SecretAPI {
		t.Errorf("a nil host changed a secret: %q", got)
	}
}
