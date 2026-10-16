package cli

import (
	"log/slog"
	"net/http"
	"os"
	"runtime"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// environment reads the environment file: --env-file, else ./millstream.yaml when it exists.
func environment(rt *Runtime) (*pipeline.Environment, error) {
	if rt.EnvFile != "" {
		return pipeline.LoadEnvironment(rt.EnvFile, true)
	}
	return pipeline.LoadEnvironment(pipeline.EnvironmentFile, false)
}

// resolver returns the action resolver of a command: container images through the runtime the environment selects,
// when its CLI is installed, and registry references through the environment's registries. ignoreLock skips the
// comparison with the lock file, for millstream lock.
func resolver(rt *Runtime, env *pipeline.Environment, ignoreLock bool) pipeline.Resolver {
	r := pipeline.Resolver{IgnoreLock: ignoreLock}
	if crt, ok := container.Find(env.ContainerRuntime, rt.Env); ok {
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
	if crt, ok := container.Find(env.ContainerRuntime, rt.Env); ok {
		l[pipeline.KindContainer] = container.Launcher{Runtime: crt}
	}
	return l
}

// agents returns how the engine runs agent steps: in a container sandbox when the runtime the environment selects is
// installed and agents.image is set, with agents.linuxBinary as the adapter (the running binary on Linux); else
// unsandboxed as this binary's agent subcommand, when agents.allowUnsandboxed allows it.
func agents(rt *Runtime, env *pipeline.Environment) engine.Agents {
	a := engine.Agents{AllowUnsandboxed: env.AllowUnsandboxed, Prices: env.Prices}
	a.Executable, _ = os.Executable()
	bin := env.AgentBinary
	if bin == "" && runtime.GOOS == "linux" {
		bin = a.Executable
	}
	if _, ok := container.Find(env.ContainerRuntime, rt.Env); ok && env.AgentImage != "" && bin != "" {
		a.Sandbox = &engine.AgentSandbox{Image: env.AgentImage, Binary: bin}
	}
	return a
}

// checkOptions are the environment file's settings check applies.
func checkOptions(env *pipeline.Environment) pipeline.CheckOptions {
	return pipeline.CheckOptions{LessonsMaxTokens: env.LessonsMaxTokens}
}

// notifier returns the notifier of review webhooks, with inbox links under ui.baseUrl.
func notifier(rt *Runtime, env *pipeline.Environment, log *slog.Logger) *review.Notifier {
	client := rt.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	return &review.Notifier{BaseURL: env.UIBaseURL, Client: client, Logger: log}
}
