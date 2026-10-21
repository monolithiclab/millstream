//go:build e2e

package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// Evidence moves between two deployments through the CLI (spec section 11.2): exported from the one that ran the
// fixture, refused by a production deployment, imported by a development one (once, then found), which reports
// whether its package diverged from the production revision; a purge in production then lists the bundle that held
// the key it erased.
func Test_evidenceExportImport(t *testing.T) {
	t.Parallel()
	pkg, prod, dev := runPackage(t, "run"), migratedDB(t), migratedDB(t)
	mustRun(t, 0, "--db", dsn(prod), "run", "--once", pkg)
	dir := testdir.New(t)
	bundle := filepath.Join(dir, "bundle.tar.gz")
	envFile := func(mode string) string {
		path := filepath.Join(dir, mode+".yaml")
		if err := os.WriteFile(path, []byte("environment: "+mode+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	out, _ := mustRun(t, 0, "--db", dsn(prod), "evidence", "export", "--pipeline", "listings", "--since", "1d",
		"--out", bundle)
	if !strings.HasPrefix(out, "evidence_") || !strings.Contains(out, "written to "+bundle) {
		t.Fatalf("export: %q", out)
	}
	if info, err := os.Stat(bundle); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle file: %v, %v", info, err)
	}
	if _, errOut := mustRun(t, 1, "--db", dsn(prod), "--env-file", envFile("production"), "evidence", "import",
		bundle); !strings.Contains(errOut, "a production deployment does not import evidence") {
		t.Fatalf("import in production: %q", errOut)
	}
	devEnv := envFile("development")
	if out, _ := mustRun(t, 0, "--db", dsn(dev), "--env-file", devEnv, "evidence", "import", bundle); !strings.Contains(out,
		" of listings imported, expires ") || !strings.Contains(out, "listings is not applied here") {
		t.Fatalf("import: %q", out)
	}
	mustRun(t, 0, "--db", dsn(dev), "--env-file", devEnv, "apply", pkg)
	if out, _ := mustRun(t, 0, "--db", dsn(dev), "--env-file", devEnv, "evidence", "import", bundle); !strings.Contains(out,
		"(imported already)\n") || strings.Contains(out, "diverged") {
		t.Fatalf("import again on the same package: %q", out)
	}
	f, err := os.OpenFile(filepath.Join(pkg, "pipeline.yaml"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("description: changed in development\n")
	_ = f.Close()
	mustRun(t, 0, "--db", dsn(dev), "--env-file", devEnv, "apply", pkg)
	if out, _ := mustRun(t, 0, "--db", dsn(dev), "--env-file", devEnv, "evidence", "import", bundle); !strings.Contains(out,
		"has diverged from the production revision") {
		t.Fatalf("import on a diverged package: %q", out)
	}
	if out, _ := mustRun(t, 0, "--db", dsn(prod), "purge", "--key", "k0"); !strings.Contains(out,
		"evidence bundles that held it: evidence_") {
		t.Fatalf("purge: %q", out)
	}
}
