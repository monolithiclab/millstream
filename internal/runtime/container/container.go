// Package container is the container runtime (spec section 6.4): a Launcher that starts an action process as
// `docker run -i` (or Podman) so the exec runtime's process, pool, protocol, timeouts and process-group handling
// apply unchanged, and the image inspection the action resolver pins images with.
//
// A container gets what the protocol promises and nothing else: its delivery working directory, mounted at the same
// path, with MILLSTREAM_WORKDIR pointing at it; the variables the engine passes explicitly; a read-only root file
// system with a private /tmp; no capabilities; and no network unless the action's manifest declares some. It runs as
// the node's user, so the files it writes in the working directory remain the node's to read and remove. Images are
// never pulled at run time (--pull=never): they are pinned by digest in the lock file and present on the node.
//
// The runtime CLI is started with its own environment (Runtime.Env), never the action's: DOCKER_HOST and the like
// reach the CLI, not the container. Killing a process kills the CLI's process group and removes the container by
// name, since a killed client leaves its container running.
package container

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// cliEnv are the variables of the node's environment the runtime CLI receives.
var cliEnv = []string{"PATH", "HOME", "TMPDIR", "DOCKER_HOST", "DOCKER_CONFIG", "DOCKER_CONTEXT", "DOCKER_CERT_PATH",
	"DOCKER_TLS_VERIFY", "CONTAINER_HOST", "CONTAINERS_CONF", "XDG_RUNTIME_DIR", "XDG_CONFIG_HOME"}

// removeTimeout bounds the removal of a killed process's container.
const removeTimeout = 15 * time.Second

// Runtime is a container runtime's CLI.
type Runtime struct {
	// Name is pipeline.RuntimeDocker or pipeline.RuntimePodman: their command lines differ slightly.
	Name string
	// Bin is the CLI's path.
	Bin string
	// Env is the CLI's whole environment.
	Env []string
	// User is the --user of containers, uid:gid; empty runs them as the image's user.
	User string
}

// Find locates the runtime's CLI on the PATH that env resolves, with the CLI's environment taken from env. ok is
// false when the CLI is not installed.
func Find(name string, env func(string) (string, bool)) (Runtime, bool) {
	rt := Runtime{Name: name, User: nodeUser()}
	for _, k := range cliEnv {
		if v, ok := env(k); ok {
			rt.Env = append(rt.Env, k+"="+v)
		}
	}
	path, _ := env("PATH")
	for _, dir := range filepath.SplitList(path) {
		if dir == "" {
			continue
		}
		p := filepath.Join(dir, name)
		if fi, err := os.Stat(p); err == nil && fi.Mode().IsRegular() && fi.Mode().Perm()&0o111 != 0 {
			rt.Bin = p
			return rt, true
		}
	}
	return rt, false
}

// nodeUser is uid:gid of the node, or empty where there are no such ids.
func nodeUser() string {
	if os.Getuid() < 0 {
		return ""
	}
	return strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid())
}

// command returns the CLI invocation of args.
func (r Runtime) command(ctx context.Context, args ...string) *osexec.Cmd {
	cmd := osexec.CommandContext(ctx, r.Bin, args...) // #nosec G204 -- the runtime CLI with arguments we build
	cmd.Env = r.Env
	return cmd
}

// output runs the CLI and returns its standard output; a failure carries its standard error.
func (r Runtime) output(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := r.command(ctx, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", r.Name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// Launcher implements exec.Launcher for container actions.
type Launcher struct {
	Runtime Runtime
}

var _ exec.Launcher = Launcher{}

// Command implements exec.Launcher: docker run -i with the action's image and command.
func (l Launcher) Command(spec exec.Spec, workdir string) (*osexec.Cmd, error) {
	if spec.Image == "" {
		return nil, fmt.Errorf("action %s: no image", spec.Action)
	}
	return l.Runtime.command(context.Background(), l.Args(spec, workdir)...), nil
}

// Kill implements exec.Launcher: it removes the container, which outlives its killed client.
func (l Launcher) Kill(_ exec.Spec, workdir string) {
	ctx, cancel := context.WithTimeout(context.Background(), removeTimeout)
	defer cancel()
	_ = l.Runtime.command(ctx, "rm", "--force", Name(workdir)).Run()
}

// Args are the CLI arguments that run spec with workdir.
func (l Launcher) Args(spec exec.Spec, workdir string) []string {
	args := []string{"run", "--interactive", "--rm", "--pull=never", "--name", Name(workdir),
		"--read-only", "--tmpfs", "/tmp", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"--volume", workdir + ":" + workdir, "--workdir", workdir, "--env", "MILLSTREAM_WORKDIR=" + workdir}
	if !spec.Network {
		args = append(args, "--network", "none")
	}
	switch {
	case l.Runtime.Name == pipeline.RuntimePodman && l.Runtime.User != "" && !strings.HasPrefix(l.Runtime.User, "0:"):
		args = append(args, "--userns", "keep-id") // rootless Podman maps the node's user into the container
	case l.Runtime.User != "":
		args = append(args, "--user", l.Runtime.User)
	}
	for _, kv := range spec.Env {
		args = append(args, "--env", kv)
	}
	args = append(args, spec.Image)
	return append(args, spec.Command...)
}

// Name is the container name of the process whose working directory is workdir: unique per process, so a killed
// process's container can be found and removed.
func Name(workdir string) string { return "millstream-" + filepath.Base(workdir) }

// Images implements pipeline.Images over the runtime CLI.
type Images struct {
	Runtime Runtime
}

var _ pipeline.Images = Images{}

// Inspect implements pipeline.Images with image inspect.
func (i Images) Inspect(ref string) (pipeline.Image, error) {
	out, err := i.Runtime.output("image", "inspect", ref)
	if err != nil {
		return pipeline.Image{}, err
	}
	var imgs []struct {
		ID          string   `json:"Id"`
		RepoDigests []string `json:"RepoDigests"`
		Config      struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
		Labels map[string]string `json:"Labels"` // Podman also has them at the top level
	}
	if err := json.Unmarshal(out, &imgs); err != nil {
		return pipeline.Image{}, fmt.Errorf("%s image inspect %s: %w", i.Runtime.Name, ref, err)
	}
	if len(imgs) != 1 {
		return pipeline.Image{}, fmt.Errorf("%s image inspect %s: %d images", i.Runtime.Name, ref, len(imgs))
	}
	img := imgs[0]
	id := img.ID
	if id == "" {
		return pipeline.Image{}, errors.New(i.Runtime.Name + " image inspect " + ref + ": no image id")
	}
	if !strings.HasPrefix(id, "sha256:") { // Podman prints the bare hex
		id = "sha256:" + id
	}
	labels := img.Config.Labels
	if labels == nil {
		labels = img.Labels
	}
	return pipeline.Image{ID: id, RepoDigests: img.RepoDigests, Labels: labels}, nil
}

// Tags implements pipeline.Images with image ls.
func (i Images) Tags(repo string) ([]string, error) {
	out, err := i.Runtime.output("image", "ls", "--format", "{{.Tag}}", repo)
	if err != nil {
		return nil, err
	}
	var tags []string
	for _, t := range strings.Fields(string(out)) {
		if t != "<none>" {
			tags = append(tags, t)
		}
	}
	return tags, nil
}
