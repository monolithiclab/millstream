//go:build e2e

package publish

import (
	"context"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// gitImage is the publish container of the tests: git, with the fake forge CLIs mounted in. Pinned by digest.
const gitImage = "alpine/git:v2.49.1@sha256:c0280cf9572316299b08544065d3bf35db65043d5e3963982ec50647d2746e26"

// forgeRepo is a bare repository on main holding the fixture's package under pipelines/extra, and a directory of
// fake gh and glab CLIs that record their arguments under rec and print a pull request URL.
type forgeRepo struct {
	bare, bin, rec string
}

func newForgeRepo(t *testing.T) forgeRepo {
	t.Helper()
	root := t.TempDir()
	f := forgeRepo{bare: filepath.Join(root, "remote.git"), bin: filepath.Join(root, "bin"),
		rec: filepath.Join(root, "rec")}
	seed := filepath.Join(root, "seed")
	for _, d := range []string{f.bin, f.rec, filepath.Join(seed, "pipelines", "extra")} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(seed, "pipelines", "extra", "pipeline.yaml"), []byte(baseSrc), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, root, "init", "--quiet", "--bare", "-b", "main", f.bare)
	gitRun(t, seed, "init", "--quiet", "-b", "main")
	gitRun(t, seed, "add", ".")
	gitRun(t, seed, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "--quiet", "-m", "seed")
	gitRun(t, seed, "push", "--quiet", f.bare, "main")
	for _, cli := range []string{"gh", "glab"} {
		script := "#!/bin/sh\nif [ \"$1 $2\" = \"auth git-credential\" ]; then exit 0; fi\n" +
			"printf '%s\\n' \"$*\" > " + filepath.Join(f.rec, cli) + "\necho https://forge.example/pr/1\n"
		if err := os.WriteFile(filepath.Join(f.bin, cli), []byte(script), 0o755); err != nil { // #nosec G306 -- a test CLI
			t.Fatal(err)
		}
	}
	return f
}

func gitRun(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := osexec.Command("git", args...) // #nosec G204 -- the test's own arguments
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// check checks what the action pushed and asked the forge CLI.
func (f forgeRepo) check(t *testing.T, cli string) {
	t.Helper()
	msg := gitRun(t, f.bare, "log", "-1", "--format=%B", "millstream/pr_1")
	if !strings.Contains(msg, "Millstream-Proposal: pr_1") || strings.Contains(msg, "pr_forged") {
		t.Fatalf("commit message %q", msg)
	}
	if got := gitRun(t, f.bare, "show", "millstream/pr_1:pipelines/extra/pipeline.yaml"); !strings.Contains(got,
		`expr: "false"`) {
		t.Fatalf("pushed %q", got)
	}
	if got := Trailers(msg); len(got) != 1 || got[0] != "pr_1" {
		t.Fatalf("Trailers = %q", got)
	}
	args, err := os.ReadFile(filepath.Join(f.rec, cli)) // #nosec G304 -- the test's own file
	if err != nil {
		t.Fatal(err)
	}
	want := "pr create --base main --head millstream/pr_1"
	if cli == "glab" {
		want = "mr create --source-branch millstream/pr_1 --target-branch main"
	}
	if !strings.Contains(string(args), want) || cli == "glab" && !strings.Contains(string(args), "Validation report") {
		t.Fatalf("%s %s", cli, args)
	}
}

// hostJobs runs the git jobs on the host, the fake CLIs first on PATH: the scripts the container runs.
type hostJobs struct{ bin string }

func (h hostJobs) run(ctx context.Context, j gitJob) (string, error) {
	cmd := osexec.CommandContext(ctx, "/bin/sh", "-euc", j.script) // #nosec G204 -- the action's own script
	cmd.Dir = j.work
	cmd.Env = append([]string{"WORK=" + j.work, "HOME=" + j.work, "PATH=" + h.bin + ":/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin"},
		j.env...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, stderr.String())
	}
	return string(out), nil
}

// TestGitPRHost: both git actions publish to a local repository with the scripts their container runs, and a second
// publication of the same proposal is refused: its branch exists and is never forced.
func TestGitPRHost(t *testing.T) {
	_, packages, acc := fixture(t)
	for _, action := range []string{pipeline.PublishGitPRGH, pipeline.PublishGitPRGLab} {
		f := newForgeRepo(t)
		p, err := New(pipeline.Improve{Publish: action, Git: pipeline.GitConfig{Repo: f.bare, Path: "pipelines/extra",
			Image: gitImage}}, Deps{Packages: packages, jobs: hostJobs{bin: f.bin}})
		if err != nil {
			t.Fatal(err)
		}
		res, err := p.Publish(t.Context(), acc)
		if err != nil || res.RevisionOrURL != "https://forge.example/pr/1" {
			t.Fatalf("%s: Publish = %+v, %v", action, res, err)
		}
		f.check(t, strings.TrimPrefix(action, "git-pr-"))
		again := acc
		again.Rationale = "Another commit."
		if _, err := p.Publish(t.Context(), again); err == nil || !strings.Contains(err.Error(), "rejected") {
			t.Fatalf("%s: a second commit on the proposal's branch: %v, want the push rejected", action, err)
		}
	}
}

// TestGitPRContainer: git-pr-gh runs in its pinned container, with the repository and the fake gh mounted in and a
// credential passed by name, and publishes the same.
func TestGitPRContainer(t *testing.T) {
	rt, ok := container.Find(pipeline.RuntimeDocker, os.LookupEnv)
	if !ok {
		t.Skip("docker is not installed: the publish container test is skipped")
	}
	if osexec.Command(rt.Bin, "image", "inspect", gitImage).Run() != nil { // #nosec G204 -- docker, our image
		if out, err := osexec.Command(rt.Bin, "pull", "--quiet", gitImage).CombinedOutput(); err != nil { // #nosec G204
			t.Skipf("the publish image is not available: %s", out)
		}
	}
	_, packages, acc := fixture(t)
	f := newForgeRepo(t)
	p, err := New(pipeline.Improve{Publish: pipeline.PublishGitPRGH, Git: pipeline.GitConfig{Repo: f.bare,
		Path: "pipelines/{pipeline}", Image: gitImage, Env: []string{"GH_TOKEN"}}}, Deps{Packages: packages,
		Env: func(k string) (string, bool) { return "not-a-token", k == "GH_TOKEN" },
		jobs: containerJobs{rt: rt, image: gitImage, mounts: []exec.Mount{
			{Source: f.bare, Target: f.bare}, {Source: f.rec, Target: f.rec},
			{Source: filepath.Join(f.bin, "gh"), Target: "/usr/local/bin/gh", ReadOnly: true},
		}}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := p.Publish(t.Context(), acc)
	if err != nil || res.RevisionOrURL != "https://forge.example/pr/1" {
		t.Fatalf("Publish = %+v, %v", res, err)
	}
	f.check(t, "gh")
}
