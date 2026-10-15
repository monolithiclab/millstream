package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

// Error classes of deliveries the engine fails at dispatch.
const (
	// ClassUnsupported: the step's action is a builtin, a registry action or a kind this build cannot run (M1
	// runs exec actions only). Never retried.
	ClassUnsupported = "unsupported"
	// ClassConfig: the step's config does not interpolate, a secret is unset, or the action does not resolve.
	// Never retried (spec section 5.5).
	ClassConfig = "config"
	// ClassEngine: the engine could not prepare the delivery (its revision's package is not loaded, a store read
	// failed). Retried.
	ClassEngine = "engine"
)

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
	a      *pipeline.Action
	dir    string // the action directory on disk
	digest string
	err    error
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
	runInfo := expr.Run{ID: run.ID, Trigger: run.Trigger, StartedAt: run.StartedAt, Revision: rev.hash}
	config, err := expr.Interpolate(step.Config, expr.InterpCtx{Vars: run.Vars, Run: runInfo, Secrets: secrets})
	if err != nil {
		return node.Assignment{}, dispatchErr(ClassConfig, false, "interpolate config of step %s: %w", l.Step, err)
	}
	return node.Assignment{
		Lease: l,
		Spec: exec.Spec{
			Action: act.a.Name, Version: act.a.Version, Digest: act.digest, Command: slices.Clone(act.a.Command),
			Dir: act.dir, Lifecycle: act.a.Lifecycle, Config: config, Vars: run.Vars, Env: e.processEnv(),
		},
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
	}, nil
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
	case a.Builtin:
		return &resolvedAction{err: dispatchErr(ClassUnsupported, false,
			"the %s builtin arrives in M2: this build cannot run it", a.Name)}
	case a.Kind != pipeline.KindExec:
		return &resolvedAction{err: dispatchErr(ClassUnsupported, false,
			"action %s is of kind %s: this build runs exec actions only", ref, a.Kind)}
	}
	return &resolvedAction{a: a, dir: filepath.Join(pkg.Dir, filepath.FromSlash(a.Dir)), digest: digest(pkg, a.Dir)}
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
