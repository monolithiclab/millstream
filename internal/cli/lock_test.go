package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container/containertest"
)

const (
	reportID  = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	rebuiltID = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	scoreID   = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
)

// containerPipeline has a local container action, a registry action read twice and an exec-free builtin.
const containerPipeline = `pipeline: boxes
triggers: { go: { manual: {} } }
steps:
  report: { action: ./actions/report, in: trigger.go, out: reports }
  score: { action: lab/score@1, in: reports, out: scored }
  again: { action: lab/score@1, in: scored, out: rescored }
  keep: { action: filter, in: rescored, config: { expr: "true" } }
`

const scoreLabel = `{"name": "lab/score", "version": "1.2.0", "kind": "container", "runtime": {"container": {}}}`

// containerSetup writes the container package, an environment file mapping lab, and a fake docker answering for
// report:dev with id reportImage. It returns the package, the environment file and the CLI's environment.
func containerSetup(t *testing.T, reportImage string) (pkg, envFile string, env map[string]string) {
	t.Helper()
	pkg = t.TempDir()
	files := map[string]string{
		"pipeline.yaml":              containerPipeline,
		"actions/report/action.yaml": "name: report\nkind: container\nruntime: { container: { image: report:dev } }\n",
	}
	for name, body := range files {
		p := filepath.Join(pkg, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	envFile = filepath.Join(t.TempDir(), "env.yaml")
	if err := os.WriteFile(envFile, []byte("registries: { lab: ghcr.io/lab/actions }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	if _, err := containertest.Write(bin, "docker", map[string]containertest.Image{
		"report:dev":                      {ID: reportImage},
		"ghcr.io/lab/actions/score:1.2.0": {ID: scoreID, Labels: map[string]string{pipeline.ManifestLabel: scoreLabel}},
	}, map[string][]string{"ghcr.io/lab/actions/score": {"1.2.0", "0.9.0"}}); err != nil {
		t.Fatal(err)
	}
	return pkg, envFile, map[string]string{"PATH": bin}
}

func TestLockCommand(t *testing.T) {
	t.Parallel()
	pkg, envFile, env := containerSetup(t, reportID)
	ctx := context.Background()

	code, out, _ := runCLICtx(t, ctx, env, "--env-file", envFile, "check", pkg)
	if code != 0 || !strings.Contains(out, "warning steps.report.action: image report:dev ("+reportID+") is not pinned") ||
		!strings.Contains(out, "warning steps.score.action: image ghcr.io/lab/actions/score:1.2.0") {
		t.Fatalf("check before lock: %d\n%s", code, out)
	}

	code, out, errOut := runCLICtx(t, ctx, env, "--env-file", envFile, "lock", pkg)
	if code != 0 {
		t.Fatalf("lock: %d\n%s%s", code, out, errOut)
	}
	if want := "./actions/report report:dev " + reportID + "\nlab/score@1 ghcr.io/lab/actions/score:1.2.0 " + scoreID +
		"\n"; out != want {
		t.Fatalf("lock printed:\n%s\nwant:\n%s", out, want)
	}
	data, err := os.ReadFile(filepath.Join(pkg, pipeline.LockFile)) // #nosec G304 -- the test's package
	if err != nil {
		t.Fatal(err)
	}
	l, err := pipeline.ParseLock(data)
	if err != nil {
		t.Fatal(err)
	}
	if l.Actions["lab/score@1"] != (pipeline.LockEntry{Version: "1.2.0", Image: "ghcr.io/lab/actions/score:1.2.0",
		Digest: scoreID}) || l.Actions["./actions/report"] != (pipeline.LockEntry{Image: "report:dev", Digest: reportID}) ||
		len(l.Actions) != 2 {
		t.Fatalf("lock file %+v", l.Actions)
	}

	code, out, _ = runCLICtx(t, ctx, env, "--env-file", envFile, "check", pkg)
	if code != 0 || strings.Contains(out, "not pinned") {
		t.Fatalf("check after lock: %d\n%s", code, out)
	}
}

// TestLockMismatch: once the image is rebuilt, check and run refuse the package until it is locked again.
func TestLockMismatch(t *testing.T) {
	t.Parallel()
	pkg, envFile, env := containerSetup(t, rebuiltID)
	lock := "actions:\n  ./actions/report: { image: \"report:dev\", digest: \"" + reportID + "\" }\n"
	if err := os.WriteFile(filepath.Join(pkg, pipeline.LockFile), []byte(lock), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	code, out, _ := runCLICtx(t, ctx, env, "--env-file", envFile, "check", pkg)
	want := "error steps.report.action: action ./actions/report: image report:dev is " + rebuiltID +
		" but millstream.lock pins report:dev (" + reportID + ")"
	if code != 1 || !strings.Contains(out, want) {
		t.Fatalf("check: %d\n%s", code, out)
	}
	code, _, errOut := runCLICtx(t, ctx, env, "--env-file", envFile, "--db", dbPath(t), "run", "--once", pkg)
	if code != 1 || !strings.Contains(errOut, "millstream.lock pins") {
		t.Fatalf("run: %d\n%s", code, errOut)
	}
	if code, _, _ := runCLICtx(t, ctx, env, "--env-file", envFile, "lock", pkg); code != 0 {
		t.Fatalf("relock: %d", code)
	}
	if code, out, _ := runCLICtx(t, ctx, env, "--env-file", envFile, "check", pkg); code != 0 {
		t.Fatalf("check after relocking: %d\n%s", code, out)
	}
}

func TestLockAndEnvironmentErrors(t *testing.T) {
	t.Parallel()
	pkg, envFile, env := containerSetup(t, reportID)
	bad := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(bad, []byte("listen: 127.0.0.1:7070\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		env     map[string]string
		args    []string
		code    int
		wantErr string
	}{
		{"missing environment file", env, []string{"--env-file", filepath.Join(pkg, "absent.yaml"), "check", pkg}, 1,
			"environment file:"},
		{"bad environment file", env, []string{"--env-file", bad, "run", "--once", pkg}, 1,
			"listen: is not supported by this build yet"},
		{"bad environment file for lock", env, []string{"--env-file", bad, "lock", pkg}, 1, "listen"},
		{"bad environment file for test", env, []string{"--env-file", bad, "test", "pipeline", pkg}, 1, "listen"},
		{"no container runtime", nil, []string{"--env-file", envFile, "lock", pkg}, 1, "needs Docker or Podman"},
		{"no package", env, []string{"lock", filepath.Join(pkg, "absent")}, 1, "load package"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			code, _, errOut := runCLICtx(t, context.Background(), tt.env, tt.args...)
			if code != tt.code || !strings.Contains(errOut, tt.wantErr) {
				t.Fatalf("code %d, stderr:\n%s", code, errOut)
			}
		})
	}
}

func TestLockWithoutContainerActions(t *testing.T) {
	t.Parallel()
	pkg := runPackage(t, "run")
	code, out, errOut := runCLICtx(t, context.Background(), nil, "lock", pkg)
	if code != 0 || out != "" || !strings.Contains(errOut, "no container or registry actions to lock") {
		t.Fatalf("lock: %d\n%s%s", code, out, errOut)
	}
	if _, err := os.Stat(filepath.Join(pkg, pipeline.LockFile)); !os.IsNotExist(err) {
		t.Fatalf("a lock file was written: %v", err)
	}
}
