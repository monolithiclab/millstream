package cli

import (
	"github.com/monolithiclab/millstream/internal/pipeline"
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
	l := map[string]exec.Launcher{pipeline.KindExec: exec.ExecLauncher{}}
	if crt, ok := container.Find(env.ContainerRuntime, rt.Env); ok {
		l[pipeline.KindContainer] = container.Launcher{Runtime: crt}
	}
	return l
}
