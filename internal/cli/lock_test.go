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
	reportID = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	scoreID  = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
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

// containerSetup writes the container package, an environment file mapping lab and, in the e2e build, a fake docker
// answering for report:dev with id reportImage: a shell script, so the tests that run it are in lock_e2e_test.go.
// It returns the package, the environment file and the CLI's environment.
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
	if !e2eBuild { // the default build's cases fail before looking for docker
		return pkg, envFile, map[string]string{"PATH": filepath.Join(pkg, "no-bin")}
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

func TestLockAndEnvironmentErrors(t *testing.T) {
	t.Parallel()
	pkg, envFile, env := containerSetup(t, reportID)
	bad := filepath.Join(filepath.Dir(envFile), "bad.yaml")
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
	pkg := sharedPackage(t, "run")
	code, out, errOut := runCLICtx(t, context.Background(), nil, "lock", pkg)
	if code != 0 || out != "" || !strings.Contains(errOut, "no container or registry actions to lock") {
		t.Fatalf("lock: %d\n%s%s", code, out, errOut)
	}
	if _, err := os.Stat(filepath.Join(pkg, pipeline.LockFile)); !os.IsNotExist(err) {
		t.Fatalf("a lock file was written: %v", err)
	}
}
