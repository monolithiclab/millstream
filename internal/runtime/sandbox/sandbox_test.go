package sandbox

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// probeFiles is a package with a proposed Go action, probe, and an exec action that is not a Go module, other.
func probeFiles() map[string][]byte {
	return map[string][]byte{
		pipeline.FileName:           []byte("pipeline: p\ntriggers: { t: { manual: {} } }\nsteps:\n  s: { action: ./actions/probe, in: trigger.t }\n"),
		"actions/probe/action.yaml": []byte("name: probe\nkind: exec\nruntime: { exec: { command: [probe, --fast] } }\n"),
		"actions/probe/go.mod":      []byte("module probe\n\ngo 1.27.0\n"),
		"actions/probe/main.go":     []byte("package main\n\nfunc main() {}\n"),
		"actions/other/action.yaml": []byte("name: other\nkind: exec\nruntime: { exec: { command: [python3, x.py] } }\n"),
	}
}

// TestLaunchers: a proposed Go action is built with no network against the embedded SDK, its source read-only, and
// runs in the sandbox's container with no network, its directory read-only and no variable of the node; every other
// action runs with the launchers given. An exec action that is not a Go module, and one that does not build, are
// refused.
func TestLaunchers(t *testing.T) {
	t.Parallel()
	pkg, err := pipeline.Materialize(testdir.New(t), probeFiles())
	if err != nil {
		t.Fatal(err)
	}
	var builds []container.Job
	s := &Sandbox{Runtime: container.Runtime{Name: pipeline.RuntimeDocker, Bin: "/fake/docker", User: "501:20"},
		Jobs: jobFunc(func(_ context.Context, j container.Job, _, _ io.Writer) error {
			builds = append(builds, j)
			for _, m := range j.Mounts {
				if m.Target == outPath {
					if _, err := os.Stat(filepath.Join(m.Source, "go.work")); err != nil {
						return err
					}
					return os.WriteFile(filepath.Join(m.Source, "action"), []byte("binary"), 0o600)
				}
			}
			return errors.New("no output mount")
		})}
	base := map[string]exec.Launcher{pipeline.KindExec: exec.ExecLauncher{}, pipeline.KindContainer: container.Launcher{}}
	ls, cleanup, err := s.Launchers(t.Context(), pkg, []string{"actions/probe/"}, base)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if len(builds) != 1 || builds[0].Network || builds[0].Image != DefaultImage ||
		!slices.Contains(builds[0].Env, "GOPROXY=off") || !builds[0].Mounts[0].ReadOnly || !builds[0].Mounts[1].ReadOnly {
		t.Fatalf("builds = %+v", builds)
	}
	if _, ok := ls[pipeline.KindContainer].(container.Launcher); !ok {
		t.Fatalf("the container launcher was replaced: %T", ls[pipeline.KindContainer])
	}
	probe := exec.Spec{Action: "probe", Dir: filepath.Join(pkg.Dir, "actions", "probe"), Command: []string{"probe", "--fast"},
		Env: []string{"PATH=/usr/bin", "GH_TOKEN=secret"}}
	cmd, err := ls[pipeline.KindExec].Command(probe, "/tmp/millstream-work-1")
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{"--network none", "--read-only", "--cap-drop ALL",
		"dst=/sandbox/action,readonly", "dst=" + probe.Dir + ",readonly", DefaultImage + " /sandbox/action --fast"} {
		if !strings.Contains(args, want) {
			t.Fatalf("sandboxed command %q lacks %q", args, want)
		}
	}
	if strings.Contains(args, "GH_TOKEN") || slices.ContainsFunc(cmd.Env, func(kv string) bool {
		return strings.HasPrefix(kv, "GH_TOKEN")
	}) {
		t.Fatalf("a variable of the node reached the sandbox: %q %q", args, cmd.Env)
	}
	other := exec.Spec{Action: "other", Dir: filepath.Join(pkg.Dir, "actions", "other"), Command: []string{"/bin/echo"}}
	if cmd, err := ls[pipeline.KindExec].Command(other, "/tmp/w"); err != nil || cmd.Path != "/bin/echo" {
		t.Fatalf("another action: %v, %v", cmd, err)
	}
	if _, _, err := s.Launchers(t.Context(), pkg, []string{"actions/other"}, base); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("an exec action that is not a Go module: %v", err)
	}
	s.Jobs = jobFunc(func(_ context.Context, _ container.Job, _, stderr io.Writer) error {
		_, _ = io.WriteString(stderr, "main.go:3: undefined: x")
		return errors.New("exit status 1")
	})
	if _, _, err := s.Launchers(t.Context(), pkg, []string{"actions/probe"}, base); err == nil ||
		!strings.Contains(err.Error(), "undefined: x") {
		t.Fatalf("a build failure: %v", err)
	}
}

// TestConfine: a container action under a proposed directory loses the network its manifest declares; others keep
// theirs.
func TestConfine(t *testing.T) {
	t.Parallel()
	r := Confine(fixedResolver{}, []string{"actions/new/"})
	for ref, want := range map[string]bool{"./actions/new": false, "./actions/old": true} {
		a, err := r.Resolve(nil, ref)
		if err != nil || a.Network != want {
			t.Fatalf("%s: network %v, %v; want %v", ref, a.Network, err, want)
		}
	}
	if Confine(fixedResolver{}, nil) != (fixedResolver{}) {
		t.Fatal("nothing to confine: the resolver is wrapped")
	}
}

// fixedResolver resolves ./actions/<x> to a container action in actions/<x> with the network.
type fixedResolver struct{}

func (fixedResolver) Resolve(_ *pipeline.Package, ref string) (*pipeline.Action, error) {
	return &pipeline.Action{Ref: ref, Kind: pipeline.KindContainer, Network: true, Dir: strings.TrimPrefix(ref, "./")}, nil
}

// TestRouterStarts: an action the sandbox did not build starts with the base launcher, a starter included.
func TestRouterStarts(t *testing.T) {
	t.Parallel()
	started := false
	r := router{base: exec.FuncLauncher{F: func(context.Context, []string, []string, io.Reader, io.Writer, io.Writer) int {
		started = true
		return 0
	}}, built: map[string]string{"/pkg/actions/probe": "/b/action"}}
	proc, err := r.Start(exec.Spec{Action: "other", Dir: "/pkg/actions/other"}, "/tmp/w")
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Wait(); err != nil || !started {
		t.Fatalf("Wait = %v, started %v", err, started)
	}
	r.Kill(exec.Spec{Dir: "/pkg/actions/other"}, "/tmp/w")
	if b, ok := r.boxed(exec.Spec{Dir: "/pkg/actions/probe", Command: []string{"probe"}}); !ok ||
		!slices.Equal(b.Command, []string{binaryPath}) {
		t.Fatalf("boxed = %+v, %v", b, ok)
	}
	if New(container.Runtime{}).image() != DefaultImage {
		t.Fatal("the default image")
	}
}

// jobFunc is a container.JobRunner of a function.
type jobFunc func(ctx context.Context, j container.Job, stdout, stderr io.Writer) error

func (f jobFunc) RunJob(ctx context.Context, j container.Job, stdout, stderr io.Writer) error {
	return f(ctx, j, stdout, stderr)
}
