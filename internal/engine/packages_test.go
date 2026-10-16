package engine

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// ApplyPackages checks, applies with the message, and says when the package was active already; another engine of
// the same store, started from it, loads the package from its bundle into its package directory, read-only.
func TestApplyPackagesAndLoadFromStore(t *testing.T) {
	v := newEnv(t, Options{})
	pkg := newPackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "a")
	applied, findings, err := v.e.ApplyPackages(v.ctx, []*pipeline.Package{pkg}, ApplyOptions{Message: "first"})
	if err != nil || len(applied) != 1 || !applied[0].Created || applied[0].PackageHash != pkg.Hash {
		t.Fatalf("ApplyPackages = %+v, %v, %v", applied, findings, err)
	}
	if rev, _ := v.st.Revision(v.ctx, applied[0].Revision); rev.Message != "first" {
		t.Fatalf("message = %q", rev.Message)
	}
	again, _, err := v.e.ApplyPackages(v.ctx, []*pipeline.Package{pkg}, ApplyOptions{})
	if err != nil || again[0].Created || again[0].Revision != applied[0].Revision {
		t.Fatalf("applying the active package again = %+v, %v", again, err)
	}

	dir := t.TempDir()
	served, err := New(Options{Store: v.st, PackageDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	rev, err := served.revision(v.ctx, applied[0].Revision)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := served.pkg(v.ctx, rev.hash)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Dir != filepath.Join(dir, pkg.Hash) || loaded.Hash != pkg.Hash {
		t.Fatalf("loaded package %s at %s", loaded.Hash, loaded.Dir)
	}
	info, err := os.Stat(filepath.Join(loaded.Dir, "actions", "a", "run"))
	if err != nil || info.Mode().Perm()&0o200 != 0 || info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("package file: %v, %v; want read-only and executable", info, err)
	}
	// Without a package directory, an engine runs only what it applied itself.
	if _, err := v.e.pkg(v.ctx, "unknown"); err == nil {
		t.Fatal("an unknown package loaded")
	}
}

// A check error, here a secret the environment does not map and two packages of one pipeline, refuses the whole
// apply with every finding, as an invalid request.
func TestApplyPackagesRefusesCheckErrors(t *testing.T) {
	v := newEnv(t, Options{Check: pipeline.CheckOptions{Secrets: (&pipeline.Environment{
		Secrets: map[string]map[string]pipeline.SecretSource{},
	}).SecretLookupFor(func(string) (string, bool) { return "", false })}})
	src := strings.Replace(manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "triggers:",
		"secrets: [token]\ntriggers:", 1)
	pkg := newPackage(t, src, "a")
	_, _, err := v.e.ApplyPackages(v.ctx, []*pipeline.Package{pkg, pkg}, ApplyOptions{})
	var ce *CheckError
	if !errors.As(err, &ce) || !errors.Is(err, store.ErrInvalid) ||
		!strings.Contains(err.Error(), "secret token is not mapped for pipeline p") ||
		!strings.Contains(err.Error(), "two packages of pipeline p") {
		t.Fatalf("ApplyPackages = %v", err)
	}
	if ps, _ := v.st.Pipelines(v.ctx); len(ps) != 0 {
		t.Fatalf("a refused apply applied %v", ps)
	}
	if _, _, err := v.e.ApplyPackages(v.ctx, nil, ApplyOptions{}); !errors.Is(err, store.ErrInvalid) {
		t.Fatalf("no package: %v", err)
	}
}

func TestDecodeBundleRefusesEscapingPaths(t *testing.T) {
	for _, bundle := range []string{`{"../x":""}`, `{"/etc/x":""}`, `{"a/../b":""}`, `[]`} {
		if _, err := DecodeBundle([]byte(bundle)); err == nil {
			t.Errorf("DecodeBundle(%s): no error", bundle)
		}
	}
}

// A package directory written already is reused; without a package directory, or with files that are no package,
// nothing is written.
func TestPackageFromFiles(t *testing.T) {
	files := map[string][]byte{pipeline.FileName: []byte(manualPipeline("p", "  a: { action: filter, in: trigger.adhoc, config: { expr: \"true\" } }\n"))}
	v := newEnv(t, Options{})
	if _, err := v.e.PackageFromFiles(files); err == nil {
		t.Fatal("no package directory: no error")
	}
	e, err := New(Options{Store: v.st, PackageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	first, err := e.PackageFromFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	again, err := e.PackageFromFiles(files)
	if err != nil || again.Dir != first.Dir {
		t.Fatalf("again: %v, %v", again, err)
	}
	if _, err := e.PackageFromFiles(map[string][]byte{"other.yaml": nil}); err == nil {
		t.Fatal("no pipeline file: no error")
	}
	// A bundle whose files hash to another package than the one named is refused.
	id, err := v.st.ApplyRevision(v.ctx, store.PackageRecord{Hash: "not-its-hash", Bundle: must(EncodeBundle(files)),
		Pipeline: first.Pipeline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.pkg(v.ctx, "not-its-hash"); err == nil || !strings.Contains(err.Error(), "its files hash to") {
		t.Fatalf("revision %d: %v", id, err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
