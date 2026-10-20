// Package sandbox runs proposed code (spec section 15.3): the exec actions a proposal's patch adds or changes are
// untrusted until the proposal is accepted, so a backtest or an eval runs them here rather than with the node's
// privileges. Each is a Go action (a go.mod in its directory), built and run in a pinned Go image by the container
// runtime: the build with no network against the SDK the binary embeds (millstream.Source) and the standard library
// alone, as the node's user, its source read-only; the action with no network, a read-only root, no capabilities,
// its action directory read-only, and no variable of the node's environment, so no credential.
//
// Launchers returns launchers that route each process by its action directory: the sandboxed ones to the container,
// every other to the launchers it was given.
package sandbox

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	osexec "os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"

	millstream "github.com/monolithiclab/millstream/sdk/go"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// DefaultImage is the image proposed Go actions are built and run in: the build stage of the release Dockerfile,
// pinned by digest. It is never pulled at run time.
const DefaultImage = "golang:1.27.2-trixie@sha256:47667934874913a449b96faa7ef4777fa285d9ec29126ee80ed61c45b7cccf0e"

// Where a sandboxed action's pieces are in its container.
const (
	binaryPath = "/sandbox/action"
	srcPath    = "/src"
	sdkPath    = "/sdk"
	outPath    = "/out"
)

// ErrUnsupported marks proposed code the sandbox cannot run: an exec action that is not a Go module.
var ErrUnsupported = errors.New("the sandbox runs proposed Go actions only")

// Sandbox builds and runs proposed exec actions in containers.
type Sandbox struct {
	Runtime container.Runtime
	// Image is the Go image; empty means DefaultImage.
	Image string
	// Jobs runs the builds; nil means Runtime. Tests fake it.
	Jobs container.JobRunner
}

// New returns a sandbox over a container runtime.
func New(rt container.Runtime) *Sandbox { return &Sandbox{Runtime: rt} }

func (s *Sandbox) image() string { return cmp.Or(s.Image, DefaultImage) }

// Launchers builds the exec actions of pkg under dirs (action directories relative to the package, pkg on disk) in
// the sandbox, and returns launchers running them there and every other action with base. cleanup removes the
// builds once their processes are gone. An action that is not a Go module wraps ErrUnsupported; one that does not
// build fails with the compiler's output.
func (s *Sandbox) Launchers(ctx context.Context, pkg *pipeline.Package, dirs []string,
	base map[string]exec.Launcher,
) (launchers map[string]exec.Launcher, cleanup func(), err error) {
	work, err := os.MkdirTemp("", "millstream-sandbox-")
	if err != nil {
		return nil, nil, err
	}
	remove := func() { _ = os.RemoveAll(work) }
	defer func() {
		if err != nil {
			remove()
		}
	}()
	sdk := filepath.Join(work, "sdk")
	goVersion, err := writeSDK(sdk)
	if err != nil {
		return nil, nil, err
	}
	r := router{base: base[pipeline.KindExec], built: map[string]string{},
		box: container.Launcher{Runtime: s.Runtime}, image: s.image()}
	if r.base == nil {
		r.base = exec.ExecLauncher{}
	}
	for i, dir := range dirs {
		dir = strings.TrimSuffix(dir, "/")
		if _, ok := pkg.Files[path.Join(dir, "go.mod")]; !ok {
			return nil, nil, fmt.Errorf("%w: %s has no go.mod", ErrUnsupported, dir)
		}
		out := filepath.Join(work, fmt.Sprintf("build-%d", i))
		if err := os.Mkdir(out, 0o750); err != nil {
			return nil, nil, err
		}
		src := filepath.Join(pkg.Dir, filepath.FromSlash(dir))
		if err := s.build(ctx, src, sdk, out, goVersion); err != nil {
			return nil, nil, fmt.Errorf("build the proposed action %s in the sandbox: %w", dir, err)
		}
		r.built[src] = filepath.Join(out, "action")
	}
	launchers = maps.Clone(base)
	if launchers == nil {
		launchers = map[string]exec.Launcher{}
	}
	launchers[pipeline.KindExec] = r
	return launchers, remove, nil
}

// writeSDK writes the SDK's source (millstream.Source, its tests left out) under dir and returns its go version.
func writeSDK(dir string) (string, error) {
	goVersion := ""
	err := fs.WalkDir(millstream.Source, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasSuffix(p, "_test.go") {
			return err
		}
		data, err := millstream.Source.ReadFile(p)
		if err != nil {
			return err
		}
		if p == "go.mod" {
			sc := bufio.NewScanner(bytes.NewReader(data))
			for sc.Scan() {
				if v, ok := strings.CutPrefix(strings.TrimSpace(sc.Text()), "go "); ok {
					goVersion = strings.TrimSpace(v)
				}
			}
		}
		target := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
	if err == nil && goVersion == "" {
		err = errors.New("the embedded SDK's go.mod names no go version")
	}
	return goVersion, err
}

// build compiles the Go module at src into out/action, in the image with no network: a workspace of the module and
// the SDK, the proxy off, the toolchain the image's, the caches in the container's private /tmp.
func (s *Sandbox) build(ctx context.Context, src, sdk, out, goVersion string) error {
	work := fmt.Sprintf("go %s\n\nuse (\n\t%s\n\t%s\n)\n", goVersion, srcPath, sdkPath)
	if err := os.WriteFile(filepath.Join(out, "go.work"), []byte(work), 0o600); err != nil {
		return err
	}
	j := container.Job{Image: s.image(), Workdir: srcPath,
		Command: []string{"go", "build", "-trimpath", "-o", outPath + "/action", "."},
		Mounts: []exec.Mount{{Source: src, Target: srcPath, ReadOnly: true}, {Source: sdk, Target: sdkPath, ReadOnly: true},
			{Source: out, Target: outPath}},
		Env: []string{"HOME=/tmp", "GOCACHE=/tmp/go-cache", "GOPATH=/tmp/go", "GOWORK=" + outPath + "/go.work",
			"GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local", "GOFLAGS=-mod=readonly", "CGO_ENABLED=0"}}
	var jobs container.JobRunner = s.Runtime
	if s.Jobs != nil {
		jobs = s.Jobs
	}
	var stderr bytes.Buffer
	if err := jobs.RunJob(ctx, j, io.Discard, &stderr); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// router launches the exec processes whose action directory was built in the sandbox's container, and the others
// with base.
type router struct {
	base  exec.Launcher
	built map[string]string // action directory on disk -> its binary
	box   container.Launcher
	image string
}

var (
	_ exec.Launcher = router{}
	_ exec.Starter  = router{}
)

// boxed is spec as the sandbox's container runs it: the binary in place of the command's program, its arguments
// kept, the action directory read-only at its own path, no network, and HOME the private /tmp alone.
func (r router) boxed(spec exec.Spec) (exec.Spec, bool) {
	bin, ok := r.built[spec.Dir]
	if !ok {
		return spec, false
	}
	var args []string
	if len(spec.Command) > 1 {
		args = slices.Clone(spec.Command[1:])
	}
	return exec.Spec{Action: spec.Action, Version: spec.Version, Kind: pipeline.KindContainer, Digest: spec.Digest,
		Command: append([]string{binaryPath}, args...), Image: r.image, Lifecycle: spec.Lifecycle,
		Config: spec.Config, Vars: spec.Vars, Lessons: spec.Lessons, Env: []string{"HOME=/tmp"},
		Mounts: []exec.Mount{{Source: bin, Target: binaryPath, ReadOnly: true},
			{Source: spec.Dir, Target: spec.Dir, ReadOnly: true}}}, true
}

// Command implements exec.Launcher.
func (r router) Command(spec exec.Spec, workdir string) (*osexec.Cmd, error) {
	if b, ok := r.boxed(spec); ok {
		return r.box.Command(b, workdir)
	}
	return r.base.Command(spec, workdir)
}

// Kill implements exec.Launcher.
func (r router) Kill(spec exec.Spec, workdir string) {
	if b, ok := r.boxed(spec); ok {
		r.box.Kill(b, workdir)
		return
	}
	r.base.Kill(spec, workdir)
}

// Start implements exec.Starter, so that a base launcher that starts its actions itself (tests') still does.
func (r router) Start(spec exec.Spec, workdir string) (*exec.Proc, error) {
	if b, ok := r.boxed(spec); ok {
		return exec.Launch(r.box, b, workdir)
	}
	return exec.Launch(r.base, spec, workdir)
}

// Builder builds proposed exec actions and returns launchers running them in the sandbox and every other action with
// base: what backtests and evals hold, a *Sandbox when a container runtime is installed, nil otherwise.
type Builder interface {
	Launchers(ctx context.Context, pkg *pipeline.Package, dirs []string, base map[string]exec.Launcher) (
		map[string]exec.Launcher, func(), error)
}

var _ Builder = (*Sandbox)(nil)

// Proposed returns the action directories of pkg's steps (their actions and fallbacks) holding a path the patch
// touches, slash-terminated and sorted: proposed code. execs are those of exec actions, which run only in the
// sandbox. Actions that do not resolve are left out: check reports them.
func Proposed(pkg *pipeline.Package, r pipeline.ActionResolver, touched []string) (all, execs []string) {
	for _, step := range pkg.Pipeline.Steps {
		refs := []string{step.Action}
		if step.Fallback != nil {
			refs = append(refs, step.Fallback.Action)
		}
		for _, ref := range refs {
			if pipeline.IsBuiltin(ref) {
				continue
			}
			a, err := r.Resolve(pkg, ref)
			if err != nil || a.Dir == "" {
				continue
			}
			dir := strings.TrimSuffix(a.Dir, "/") + "/"
			if slices.Contains(all, dir) || !slices.ContainsFunc(touched, func(p string) bool {
				return strings.HasPrefix(p, dir)
			}) {
				continue
			}
			all = append(all, dir)
			if a.Kind == pipeline.KindExec {
				execs = append(execs, dir)
			}
		}
	}
	slices.Sort(all)
	slices.Sort(execs)
	return all, execs
}

// Confine resolves actions as r does, except that a container action under one of dirs (action directories relative
// to the package, proposed code) gets no network, whatever its manifest declares.
func Confine(r pipeline.ActionResolver, dirs []string) pipeline.ActionResolver {
	if len(dirs) == 0 {
		return r
	}
	return confined{r: r, dirs: dirs}
}

type confined struct {
	r    pipeline.ActionResolver
	dirs []string
}

func (c confined) Resolve(pkg *pipeline.Package, ref string) (*pipeline.Action, error) {
	a, err := c.r.Resolve(pkg, ref)
	if err != nil || a.Kind != pipeline.KindContainer || !a.Network ||
		!slices.Contains(c.dirs, strings.TrimSuffix(a.Dir, "/")+"/") {
		return a, err
	}
	confined := *a
	confined.Network = false
	return &confined, nil
}
