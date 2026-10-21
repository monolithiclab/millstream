package cli

import (
	"cmp"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strings"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/sandbox"
)

// environment loads the environment's three layers: the environment file (--env-file, else ./millstream.yaml
// when it exists), the MILLSTREAM_* variables, then the flags given: --db sets store.dsn.
func environment(rt *Runtime) (*pipeline.Environment, error) {
	var names []string
	if rt.Environ != nil {
		names = rt.Environ()
	}
	path, required := cmp.Or(rt.EnvFile, pipeline.EnvironmentFile), rt.EnvFile != ""
	env, err := pipeline.LoadEnvironment(path, required, rt.Env, names)
	if err != nil || rt.DB == "" {
		return env, err
	}
	if !strings.Contains(rt.DB, "://") {
		return nil, usageError("--db: want a DSN, such as sqlite://" + rt.DB)
	}
	env.Store.DSN = rt.DB
	if err := env.Validate(); err != nil {
		return nil, usageError("--db: " + err.Error())
	}
	return env, nil
}

// resolver returns the action resolver of a command: container images through the runtime the environment selects,
// when its CLI is installed, and registry references through the environment's registries. ignoreLock skips the
// comparison with the lock file, for millstream lock.
func resolver(rt *Runtime, env *pipeline.Environment, ignoreLock bool) pipeline.Resolver {
	r := pipeline.Resolver{IgnoreLock: ignoreLock}
	if crt, ok := container.Find(env.Runtime.Container, rt.Env); ok {
		r.Images = container.Images{Runtime: crt}
	}
	r.Registry = pipeline.Registry{Prefixes: env.Registries, Resolver: r}
	return r
}

// launchers returns the process launchers of the embedded node: exec always, container when the runtime the
// environment selects is installed.
func launchers(rt *Runtime, env *pipeline.Environment) map[string]exec.Launcher {
	if rt.Launchers != nil {
		return rt.Launchers
	}
	l := map[string]exec.Launcher{pipeline.KindExec: exec.ExecLauncher{}}
	if crt, ok := container.Find(env.Runtime.Container, rt.Env); ok {
		l[pipeline.KindContainer] = container.Launcher{Runtime: crt}
	}
	return l
}

// candidateSandbox is the container sandbox proposed exec code runs in (spec section 15.3), nil without a container
// runtime: backtests and evals then refuse to run it.
func candidateSandbox(rt *Runtime, env *pipeline.Environment) sandbox.Builder {
	crt, ok := container.Find(env.Runtime.Container, rt.Env)
	if !ok || rt.Launchers != nil { // tests that run actions in-process run no container
		return nil
	}
	return sandbox.New(crt)
}

// agents returns how the engine runs agent steps: in a container sandbox when the runtime the environment selects is
// installed and agents.image is set, with agents.linuxBinary as the adapter (the running binary on Linux); else
// unsandboxed as this binary's agent subcommand, when agents.allowUnsandboxed allows it.
func agents(rt *Runtime, env *pipeline.Environment) engine.Agents {
	a := engine.Agents{Agents: node.Agents{AllowUnsandboxed: env.Agents.AllowUnsandboxed}, Prices: env.Prices}
	a.Executable, _ = os.Executable()
	bin := env.Agents.LinuxBinary // computed default: the running binary, on Linux
	if bin == "" && runtime.GOOS == "linux" {
		bin = a.Executable
	}
	if _, ok := container.Find(env.Runtime.Container, rt.Env); ok && env.Agents.Image != "" && bin != "" {
		a.Sandbox = &engine.AgentSandbox{Image: env.Agents.Image, Binary: bin}
	}
	return a
}

// checkOptions are the environment file's settings check applies.
func checkOptions(rt *Runtime, env *pipeline.Environment) pipeline.CheckOptions {
	return pipeline.CheckOptions{LessonsMaxTokens: env.Lessons.MaxTokens, Secrets: secrets(rt, env)}
}

// secrets resolves each pipeline's secrets as the environment file maps them (spec section 4.5).
func secrets(rt *Runtime, env *pipeline.Environment) func(string) pipeline.SecretLookup {
	return env.SecretLookupFor(rt.Env)
}

// notifier returns the notifier of review webhooks, with inbox links under ui.baseUrl.
func notifier(rt *Runtime, env *pipeline.Environment, log *slog.Logger) *review.Notifier {
	client := rt.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	return &review.Notifier{BaseURL: env.UI.BaseURL, Client: client, Logger: log}
}
