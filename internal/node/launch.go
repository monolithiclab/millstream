package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// Error classes of a delivery whose action process cannot be built on the machine it landed on.
const (
	// ClassConfig: the action does not resolve against the package, or a harness directory does not resolve.
	// Never retried (spec section 5.5).
	ClassConfig = "config"
	// ClassSandbox: an agent step has no container sandbox to run in and agents.allowUnsandboxed is not set (spec
	// section 7.3). Never retried.
	ClassSandbox = "sandbox"
)

// AgentBinaryPath is where a sandbox container sees the millstream binary that runs the agent adapter.
const AgentBinaryPath = "/millstream"

// envPassed are the variables of the machine's environment an exec action process receives; nothing else is passed.
var envPassed = []string{"PATH", "HOME", "TMPDIR"}

// Launch is a delivery's action process as the engine resolves it, wherever the package lives: the action's identity,
// the step's config interpolated for the run, and for an agent the adapter's arguments. It is what travels to a
// remote node (spec section 5.10), which builds the process spec from it (Spec) against its own copy of the package,
// its own environment and its own sandbox; the engine builds its embedded node's the same way.
type Launch struct {
	Kind    string `json:"kind"` // exec, container or agent
	Action  string `json:"action"`
	Version string `json:"version"`
	// Digest identifies the action's content: the action directory's, or a container's image digest.
	Digest string `json:"digest"`
	// PackageHash is the package the run's revision applied, which the node fetches by hash and caches.
	PackageHash string `json:"packageHash"`
	// Ref is the step's action reference, resolved again on the node against its copy of the package for an exec
	// action (whose bin/<os>-<arch> program is the node's platform's) and an agent.
	Ref string `json:"ref"`
	// Command is a container action's command in its image; empty for the image's own, and for the other kinds.
	Command   []string        `json:"command,omitempty"`
	Lifecycle string          `json:"lifecycle,omitempty"`
	Config    map[string]any  `json:"config"`
	Vars      map[string]any  `json:"vars"`
	Image     string          `json:"image,omitempty"`
	Network   bool            `json:"network,omitempty"`
	Lessons   json.RawMessage `json:"lessons,omitempty"`
	Agent     *AgentLaunch    `json:"agent,omitempty"`
}

// AgentLaunch is what the agent adapter of an agent step receives besides its action: the secret names it reads from
// its environment, and the model's prices from the engine's environment file.
type AgentLaunch struct {
	Secrets  []string `json:"secrets,omitempty"`
	PriceIn  *float64 `json:"priceIn,omitempty"`
	PriceOut *float64 `json:"priceOut,omitempty"`
}

// Agents says how a machine runs the agent adapter of agent steps (spec section 7.3): in a container sandbox when
// one is configured, else as a child process only when the environment file allows it.
type Agents struct {
	// Sandbox runs the adapter in a container; nil when no container runtime, agent image or Linux build of
	// millstream is available.
	Sandbox *AgentSandbox
	// AllowUnsandboxed lets the adapter run without a sandbox, as a child process running Executable
	// (agents.allowUnsandboxed); every such start is logged.
	AllowUnsandboxed bool
	// Executable is the millstream binary whose agent subcommand is the adapter, outside a sandbox.
	Executable string
}

// CanRun reports whether the agent adapter runs here at all.
func (a Agents) CanRun() bool { return a.Sandbox != nil || (a.AllowUnsandboxed && a.Executable != "") }

// AgentSandbox is the container the adapter runs in: Image (agents.image), with Binary, a Linux build of millstream,
// mounted read-only at AgentBinaryPath, and the package mounted read-only at its own path.
type AgentSandbox struct {
	Image  string
	Binary string
}

// Host is the machine a Launch becomes a process spec on.
type Host struct {
	// Action resolves the step's action reference against the package on this machine, returning the action and the
	// package's directory.
	Action func(ref string) (*pipeline.Action, string, error)
	// Env looks up this machine's environment: the PATH, HOME and TMPDIR exec actions receive, and the agent
	// harness's record and replay variables.
	Env    func(string) (string, bool)
	Agents Agents
	// Log receives the warning of every agent started without a sandbox; nil discards it.
	Log *slog.Logger
}

// LaunchError is why a Launch could not become a process spec: the delivery fails with its class.
type LaunchError struct {
	Class     string
	Retryable bool
	Err       error
}

func (e *LaunchError) Error() string { return e.Err.Error() }
func (e *LaunchError) Unwrap() error { return e.Err }

func launchErr(class string, format string, args ...any) error {
	return &LaunchError{Class: class, Err: fmt.Errorf(format, args...)}
}

// Spec builds the action process of l on h, with the run's secrets (an agent adapter receives them in its
// environment, for redaction and its API key).
func (l Launch) Spec(h Host, secrets map[string]string) (exec.Spec, error) {
	spec := exec.Spec{
		Action: l.Action, Version: l.Version, Kind: l.Kind, Digest: l.Digest, Command: slices.Clone(l.Command),
		Lifecycle: l.Lifecycle, Config: l.Config, Vars: l.Vars,
	}
	switch l.Kind {
	case pipeline.KindContainer:
		// A container gets no variable of the machine's environment: the host's PATH and HOME mean nothing in it.
		spec.Image, spec.Network = l.Image, l.Network
		return spec, nil
	case pipeline.KindAgent:
		spec.Lessons = l.Lessons
	}
	a, pkgDir, err := h.Action(l.Ref)
	if err != nil {
		var le *LaunchError
		if errors.As(err, &le) {
			return exec.Spec{}, err
		}
		return exec.Spec{}, launchErr(ClassConfig, "%w", err)
	}
	dir := filepath.Join(pkgDir, filepath.FromSlash(a.Dir))
	if l.Kind == pipeline.KindAgent {
		return spec, l.agentSpec(&spec, h, a, pkgDir, dir, secrets)
	}
	spec.Command, spec.Dir, spec.Env = slices.Clone(a.Command), dir, envOf(h.Env, envPassed)
	return spec, nil
}

// envOf returns the variables keys of env that are set.
func envOf(env func(string) (string, bool), keys []string) []string {
	var out []string
	for _, k := range keys {
		if v, ok := env(k); ok {
			out = append(out, k+"="+v)
		}
	}
	return out
}

// agentSpec makes spec run the agent adapter for an agent action (spec section 7.3): millstream agent in the sandbox
// container when one is configured, with the binary and the package mounted read-only and the manifest's network
// policy; else as a child process when the environment file allows it, logged at every start; else the delivery
// fails, not retried. The adapter receives the pipeline's secrets in its environment, for redaction and the API key,
// and the record and replay variables of the machine's.
func (l Launch) agentSpec(spec *exec.Spec, h Host, a *pipeline.Action, pkgDir, dir string, secrets map[string]string,
) error {
	ag := h.Agents
	args := []string{"agent", "--package", pkgDir, "--action", a.Ref}
	var env []string
	var al AgentLaunch
	if l.Agent != nil {
		al = *l.Agent
	}
	for _, name := range al.Secrets {
		args = append(args, "--secret", name)
		if v, ok := secrets[name]; ok { // an unset one stays unset: the adapter says so if its harness needs it
			env = append(env, pipeline.SecretEnv(name)+"="+v)
		}
	}
	if al.PriceIn != nil && al.PriceOut != nil {
		args = append(args, "--price-in", strconv.FormatFloat(*al.PriceIn, 'f', -1, 64),
			"--price-out", strconv.FormatFloat(*al.PriceOut, 'f', -1, 64))
	}
	mode, _ := h.Env(agent.EnvHarnessMode)
	hdir, _ := h.Env(agent.EnvHarnessDir)
	if hdir != "" {
		abs, err := filepath.Abs(hdir) // the adapter does not run in this process's working directory
		if err != nil {
			return launchErr(ClassConfig, "%s: %w", agent.EnvHarnessDir, err)
		}
		hdir = abs
		env = append(env, agent.EnvHarnessDir+"="+hdir)
	}
	if mode != "" {
		env = append(env, agent.EnvHarnessMode+"="+mode)
	}
	switch {
	case ag.Sandbox != nil:
		spec.Kind, spec.Image, spec.Network = pipeline.KindContainer, ag.Sandbox.Image, l.Network
		spec.Command = append([]string{AgentBinaryPath}, args...)
		spec.Mounts = []exec.Mount{
			{Source: ag.Sandbox.Binary, Target: AgentBinaryPath, ReadOnly: true},
			{Source: pkgDir, Target: pkgDir, ReadOnly: true},
		}
		if hdir != "" {
			spec.Mounts = append(spec.Mounts, exec.Mount{Source: hdir, Target: hdir, ReadOnly: mode != agent.ModeRecord})
		}
		// The root file system is read-only: HOME is the container's private /tmp, where a harness keeps its state.
		spec.Env = append([]string{"HOME=/tmp"}, env...)
	case ag.AllowUnsandboxed && ag.Executable != "":
		if h.Log != nil {
			h.Log.Warn("agent running without a sandbox: agents.allowUnsandboxed is true in the environment file")
		}
		spec.Kind, spec.Dir = pipeline.KindExec, dir
		spec.Command = append([]string{ag.Executable}, args...)
		// The adapter hands the claude CLI its own variables (agent.ClaudeEnv), a superset of envPassed.
		spec.Env = append(envOf(h.Env, agent.ClaudeEnv), env...)
	default:
		return launchErr(ClassSandbox, "agent action %s needs a container sandbox (a container runtime, "+
			"agents.image, and off Linux a Linux build of millstream in agents.linuxBinary), or "+
			"agents.allowUnsandboxed: true in the environment file", a.Ref)
	}
	return nil
}
