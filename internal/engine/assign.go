package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/schema"
	"github.com/monolithiclab/millstream/internal/store"
)

// Error classes of deliveries the engine fails at dispatch.
const (
	// ClassUnsupported: the step's action is a builtin, or a kind this build cannot run yet (review arrives in M3),
	// or it needs a runtime this machine lacks. Never retried.
	ClassUnsupported = "unsupported"
	// ClassConfig: the step's config does not interpolate, a secret is unset, or the action does not resolve.
	// Never retried (spec section 5.5).
	ClassConfig = "config"
	// ClassInputSchema: the item's payload does not match the action's input schema (spec section 5.5). Never
	// retried, and no process is started.
	ClassInputSchema = "inputSchema"
	// ClassEngine: the engine could not prepare the delivery (its revision's package is not loaded, a store read
	// failed). Retried.
	ClassEngine = "engine"
	// ClassSandbox: an agent step has no container sandbox to run in and the environment file does not allow it to
	// run without one (spec section 7.3). Never retried.
	ClassSandbox = "sandbox"
)

// AgentBinaryPath is where a sandbox container sees the millstream binary that runs the agent adapter.
const AgentBinaryPath = "/millstream"

// envPassed are the variables of the engine's environment an action process receives; nothing else is passed.
var envPassed = []string{"PATH", "HOME", "TMPDIR"}

// dispatchError is why a lease could not become an assignment.
type dispatchError struct {
	class     string
	retryable bool
	err       error
}

func (e *dispatchError) Error() string { return e.err.Error() }

func dispatchErr(class string, retryable bool, format string, args ...any) error {
	return &dispatchError{class: class, retryable: retryable, err: fmt.Errorf(format, args...)}
}

// actionKey identifies a resolved action: the package it belongs to and the step's reference.
type actionKey struct{ hash, ref string }

// resolvedAction is an action reference resolved against a package, or the dispatch error it resolves to.
type resolvedAction struct {
	a             *pipeline.Action
	dir           string // the action directory on disk
	pkgDir        string // the package directory on disk
	digest        string
	input, config *schema.Schema // the action's schemas, nil when it declares none
	err           error
}

// assign resolves a lease into what a node runs: the action process of the run's revision, the step's config
// interpolated over the run's vars, run and the secrets read now, and the item message.
func (e *Engine) assign(ctx context.Context, l store.Lease) (node.Assignment, error) {
	rev, err := e.revision(ctx, l.Revision)
	if err != nil {
		return node.Assignment{}, dispatchErr(ClassEngine, true, "revision %d: %w", l.Revision, err)
	}
	step, ok := rev.p.Steps[l.Step]
	if !ok {
		return node.Assignment{}, dispatchErr(ClassConfig, false, "step %s is not in revision %d", l.Step, l.Revision)
	}
	act := e.action(rev, step.Action)
	if act.err != nil {
		return node.Assignment{}, act.err
	}
	if err := validateJSON(act.input, l.Item.Payload); err != nil {
		return node.Assignment{}, dispatchErr(ClassInputSchema, false, "item %s does not match the input schema of %s: %w",
			l.Item.ID, step.Action, err)
	}
	run, err := e.st.Run(ctx, l.Pipeline, l.RunID)
	if err != nil {
		return node.Assignment{}, dispatchErr(ClassEngine, true, "read run %s: %w", l.RunID, err)
	}
	secrets := map[string]string{}
	for _, name := range rev.p.Secrets {
		v, ok := e.opts.Env(pipeline.SecretEnv(name))
		if !ok {
			return node.Assignment{}, dispatchErr(ClassConfig, false, "secret %s is not set: set %s", name,
				pipeline.SecretEnv(name))
		}
		secrets[name] = v
	}
	red := redact.New(secrets)
	e.setRedactor(l.Ref(), red)
	runInfo := expr.Run{ID: run.ID, Trigger: run.Trigger, StartedAt: run.StartedAt, Revision: rev.hash}
	config, err := expr.Interpolate(step.Config, expr.InterpCtx{Vars: run.Vars, Run: runInfo, Secrets: secrets})
	if err != nil {
		return node.Assignment{}, dispatchErr(ClassConfig, false, "interpolate config of step %s: %w", l.Step, err)
	}
	if act.config != nil {
		// Validation sees JSON values, as the action will: the round trip turns Go types (time.Time) into them.
		raw, err := json.Marshal(config)
		if err == nil {
			err = validateJSON(act.config, raw)
		}
		if err != nil {
			return node.Assignment{}, dispatchErr(ClassConfig, false,
				"config of step %s does not match the config schema of %s: %w", l.Step, step.Action, err)
		}
	}
	spec := exec.Spec{
		Action: act.a.Name, Version: act.a.Version, Kind: act.a.Kind, Digest: act.digest,
		Command: slices.Clone(act.a.Command), Lifecycle: act.a.Lifecycle, Config: config, Vars: run.Vars,
	}
	switch act.a.Kind {
	case pipeline.KindContainer:
		// A container gets no variable of the engine's environment: the host's PATH and HOME mean nothing in it.
		spec.Image, spec.Network = act.a.Pin, act.a.Network
	case pipeline.KindAgent:
		if err := e.agentSpec(&spec, act, rev.p.Secrets, secrets, l); err != nil {
			return node.Assignment{}, err
		}
	default:
		spec.Dir, spec.Env = act.dir, e.processEnv()
	}
	return node.Assignment{
		Lease: l,
		Spec:  spec,
		ItemMsg: protocol.Item{
			Delivery: l.DeliveryID, ID: l.Item.ID, Key: l.Item.Key, Payload: l.Item.Payload,
			Artifacts: []protocol.Artifact{},
			Run: protocol.Run{
				ID: run.ID, Trigger: run.Trigger, StartedAt: run.StartedAt, Revision: rev.hash,
			},
			Attempt:        l.Attempt,
			IdempotencyKey: ids.Idempotency(l.Step, l.Item.ID, l.Generation),
		},
		Timeout: step.Timeout,
		Redact:  red,
	}, nil
}

// setRedactor records the redactor of a delivery in flight, so its failure messages and logs are redacted in the
// engine too, whatever the node did.
func (e *Engine) setRedactor(ref store.LeaseRef, r *redact.Redactor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if f, ok := e.inflight[ref.DeliveryID]; ok && f.token == ref.FencingToken {
		f.redact = r
		e.inflight[ref.DeliveryID] = f
	}
}

// action resolves a step's action reference against its revision's package, once per package and reference.
func (e *Engine) action(rev *revision, ref string) *resolvedAction {
	k := actionKey{rev.hash, ref}
	e.mu.Lock()
	defer e.mu.Unlock()
	if ra := e.actions[k]; ra != nil {
		return ra
	}
	pkg := e.packages[rev.hash]
	if pkg == nil {
		// Not cached: the package may be applied later.
		return &resolvedAction{err: dispatchErr(ClassEngine, true,
			"the package of revision %d (%s) is not loaded in this engine: run it with that package", rev.id, rev.hash)}
	}
	ra := resolve(e.opts.Resolver, pkg, ref)
	e.actions[k] = ra
	return ra
}

func resolve(r pipeline.ActionResolver, pkg *pipeline.Package, ref string) *resolvedAction {
	a, err := r.Resolve(pkg, ref)
	switch {
	case errors.Is(err, pipeline.ErrUnavailable):
		return &resolvedAction{err: dispatchErr(ClassUnsupported, false, "%w", err)}
	case err != nil:
		return &resolvedAction{err: dispatchErr(ClassConfig, false, "%w", err)}
	case a.Builtin: // runBuiltin runs builtins before assign is reached; a resolver may still return one
		return &resolvedAction{err: dispatchErr(ClassUnsupported, false,
			"the %s builtin runs in the engine, never on a node", a.Name)}
	case a.Kind != pipeline.KindExec && a.Kind != pipeline.KindContainer && a.Kind != pipeline.KindAgent:
		return &resolvedAction{err: dispatchErr(ClassUnsupported, false,
			"action %s is of kind %s: this build runs exec, container and agent actions only", ref, a.Kind)}
	}
	ra := &resolvedAction{a: a, dir: filepath.Join(pkg.Dir, filepath.FromSlash(a.Dir)), pkgDir: pkg.Dir,
		digest: digest(pkg, a.Dir)}
	if a.Kind == pipeline.KindContainer {
		ra.digest = a.ImageDigest // the image is the action's content
	}
	for _, sc := range []struct {
		doc []byte
		dst **schema.Schema
	}{{a.Input, &ra.input}, {a.Config, &ra.config}} {
		if sc.doc == nil {
			continue
		}
		s, err := schema.Compile(sc.doc)
		if err != nil {
			return &resolvedAction{err: dispatchErr(ClassConfig, false, "action %s: %w", ref, err)}
		}
		*sc.dst = s
	}
	return ra
}

// validateJSON validates a JSON document against s; a nil s accepts anything.
func validateJSON(s *schema.Schema, doc []byte) error {
	if s == nil {
		return nil
	}
	var v any
	if err := json.Unmarshal(doc, &v); err != nil {
		return err
	}
	return s.Validate(v)
}

// digest is the content hash of an action directory: SHA-256 over path NUL content NUL for each of its files, in
// path order.
func digest(pkg *pipeline.Package, dir string) string {
	h := sha256.New()
	prefix := dir + "/"
	for _, p := range slices.Sorted(maps.Keys(pkg.Files)) {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		h.Write([]byte(p))
		h.Write([]byte{0})
		h.Write(pkg.Files[p])
		h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// processEnv is the environment of action processes: PATH, HOME and TMPDIR from the engine's, when set.
func (e *Engine) processEnv() []string {
	var env []string
	for _, k := range envPassed {
		if v, ok := e.opts.Env(k); ok {
			env = append(env, k+"="+v)
		}
	}
	return env
}

// agentSpec makes spec run the agent adapter for an agent action (spec section 7.3): millstream agent in the
// sandbox container when one is configured, with the binary and the package mounted read-only and the manifest's
// network policy; else as a child process when the environment file allows it, logged at every start; else the
// delivery fails, not retried. The adapter receives the pipeline's secrets in its environment, for redaction and the
// API key, and the record/replay variables of the engine's.
func (e *Engine) agentSpec(spec *exec.Spec, act *resolvedAction, names []string, secrets map[string]string,
	l store.Lease,
) error {
	ag := e.opts.Agents
	args := []string{"agent", "--package", act.pkgDir, "--action", act.a.Ref}
	var env []string
	for _, name := range names {
		args = append(args, "--secret", name)
		env = append(env, pipeline.SecretEnv(name)+"="+secrets[name])
	}
	if price, ok := ag.Prices[act.a.Agent.Model]; ok {
		args = append(args, "--price-in", strconv.FormatFloat(price.InPerMtok, 'f', -1, 64),
			"--price-out", strconv.FormatFloat(price.OutPerMtok, 'f', -1, 64))
	}
	mode, _ := e.opts.Env(agent.EnvHarnessMode)
	dir, _ := e.opts.Env(agent.EnvHarnessDir)
	if dir != "" {
		abs, err := filepath.Abs(dir) // the adapter does not run in the engine's working directory
		if err != nil {
			return dispatchErr(ClassConfig, false, "%s: %w", agent.EnvHarnessDir, err)
		}
		dir = abs
		env = append(env, agent.EnvHarnessDir+"="+dir)
	}
	if mode != "" {
		env = append(env, agent.EnvHarnessMode+"="+mode)
	}
	switch {
	case ag.Sandbox != nil:
		spec.Kind, spec.Image, spec.Network = pipeline.KindContainer, ag.Sandbox.Image, len(act.a.Agent.Network) > 0
		spec.Command = append([]string{AgentBinaryPath}, args...)
		spec.Mounts = []exec.Mount{
			{Source: ag.Sandbox.Binary, Target: AgentBinaryPath, ReadOnly: true},
			{Source: act.pkgDir, Target: act.pkgDir, ReadOnly: true},
		}
		if dir != "" {
			spec.Mounts = append(spec.Mounts, exec.Mount{Source: dir, Target: dir, ReadOnly: mode != agent.ModeRecord})
		}
		// The root file system is read-only: HOME is the container's private /tmp, where a harness keeps its state.
		spec.Env = append([]string{"HOME=/tmp"}, env...)
	case ag.AllowUnsandboxed && ag.Executable != "":
		e.log.Warn("agent running without a sandbox: agents.allowUnsandboxed is true in the environment file",
			"pipeline", l.Pipeline, "step", l.Step, "deliveryId", l.DeliveryID)
		spec.Kind, spec.Dir = pipeline.KindExec, act.dir
		spec.Command = append([]string{ag.Executable}, args...)
		spec.Env = append(e.processEnv(), env...)
	default:
		return dispatchErr(ClassSandbox, false, "agent step %s needs a container sandbox (a container runtime, "+
			"agents.image, and off Linux a Linux build of millstream in agents.linuxBinary), or "+
			"agents.allowUnsandboxed: true in the environment file", l.Step)
	}
	return nil
}
