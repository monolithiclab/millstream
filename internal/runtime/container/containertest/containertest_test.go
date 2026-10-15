package containertest

import (
	"bytes"
	"os"
	osexec "os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// testImages and testTags are the canned data both fakes are checked with.
var (
	testImages = map[string]Image{"it's:1": {ID: "sha256:a", Labels: map[string]string{"k": "v"}}}
	testTags   = map[string][]string{"repo": {"1.0.0", "2.0.0"}}
)

// checkCLI checks a fake CLI, run answering an invocation and calls listing those it recorded: image inspect and
// image ls answer from the canned data, an absent image fails, and every other invocation is recorded.
func checkCLI(t *testing.T, run func(args ...string) (string, error), calls func() []string) {
	t.Helper()
	if out, err := run("image", "inspect", "it's:1"); err != nil || !strings.Contains(out, `"Id":"sha256:a"`) {
		t.Fatalf("inspect: %q, %v", out, err)
	}
	if _, err := run("image", "inspect", "absent"); err == nil {
		t.Fatal("inspect of an absent image succeeded")
	}
	if out, err := run("image", "ls", "--format", "{{.Tag}}", "repo"); err != nil || out != "1.0.0\n2.0.0\n" {
		t.Fatalf("ls: %q, %v", out, err)
	}
	if out, err := run("image", "ls", "--format", "{{.Tag}}", "other"); err != nil || out != "" {
		t.Fatalf("ls of an unknown repository: %q, %v", out, err)
	}
	if len(calls()) != 0 {
		t.Fatal("calls recorded before any")
	}
	if _, err := run("rm", "--force", "x"); err != nil {
		t.Fatal(err)
	}
	if got := calls(); !slices.Equal(got, []string{"rm --force x"}) {
		t.Fatalf("calls = %q", got)
	}
}

// TestFake: the in-process fake answers as the script does; it writes nowhere when the command has no writers.
func TestFake(t *testing.T) {
	t.Parallel()
	f := &Fake{Images: testImages, Tags: testTags}
	checkCLI(t, func(args ...string) (string, error) {
		var out bytes.Buffer
		cmd := &osexec.Cmd{Path: "/fake/docker", Args: append([]string{"docker"}, args...), Stdout: &out}
		err := f.Run(cmd)
		return out.String(), err
	}, f.Calls)
	if err := f.Run(&osexec.Cmd{Args: []string{"docker", "image", "inspect", "it's:1"}}); err != nil {
		t.Fatal(err)
	}
}

// TestWrite: Write writes an executable script quoting the canned data for the shell, and Calls reads back the
// invocations the script appends (running it is the e2e build's TestFakeCLI).
func TestWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bin, err := Write(dir, "docker", testImages, testTags)
	if err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile(bin) // #nosec G304 -- the script just written
	if info, statErr := os.Stat(bin); err != nil || statErr != nil || info.Mode()&0o111 == 0 {
		t.Fatalf("script %s: %v, %v", bin, err, statErr)
	}
	for _, want := range []string{`'it'\''s:1') printf`, `'repo') printf '%s\n' '1.0.0` + "\n" + `2.0.0'`,
		`>> '` + filepath.Join(dir, "calls") + `'`} {
		if !strings.Contains(string(script), want) {
			t.Errorf("script lacks %q:\n%s", want, script)
		}
	}
	if Calls(dir) != nil {
		t.Fatal("calls before any")
	}
	if err := os.WriteFile(filepath.Join(dir, "calls"), []byte("rm --force x\nrm y\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := Calls(dir); !slices.Equal(got, []string{"rm --force x", "rm y"}) {
		t.Fatalf("calls = %q", got)
	}
	if _, err := Write(filepath.Join(dir, "missing"), "docker", nil, nil); err == nil {
		t.Fatal("Write into a missing directory succeeded")
	}
}
