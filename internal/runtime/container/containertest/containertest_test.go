package containertest

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestFakeCLI(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bin, err := Write(dir, "docker", map[string]Image{"it's:1": {ID: "sha256:a", Labels: map[string]string{"k": "v"}}},
		map[string][]string{"repo": {"1.0.0", "2.0.0"}})
	if err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, error) {
		cmd := exec.Command(bin, args...) // #nosec G204 -- the script just written
		cmd.Env = []string{}
		out, err := cmd.Output()
		return string(out), err
	}
	if out, err := run("image", "inspect", "it's:1"); err != nil || !strings.Contains(out, `"Id":"sha256:a"`) {
		t.Fatalf("inspect: %q, %v", out, err)
	}
	if _, err := run("image", "inspect", "absent"); err == nil {
		t.Fatal("inspect of an absent image succeeded")
	}
	if out, err := run("image", "ls", "--format", "{{.Tag}}", "repo"); err != nil || out != "1.0.0\n2.0.0\n" {
		t.Fatalf("ls: %q, %v", out, err)
	}
	if len(Calls(dir)) != 0 {
		t.Fatal("calls recorded before any")
	}
	if _, err := run("rm", "--force", "x"); err != nil {
		t.Fatal(err)
	}
	if got := Calls(dir); !slices.Equal(got, []string{"rm --force x"}) {
		t.Fatalf("calls = %q", got)
	}
}
