//go:build !e2e

package cli

import (
	"context"
	"io"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
)

// testLaunchers returns the launchers the tests inject into the CLI: every exec action runs in-process, the fake
// action as exectest.Serve and the engine's unsandboxed agent command (<executable> agent …) as this package's own
// agent subcommand, which sees only the environment the engine gives it.
func testLaunchers() map[string]exec.Launcher {
	return map[string]exec.Launcher{pipeline.KindExec: exec.FuncLauncher{F: serveAction}}
}

// serveAction is the in-process action of testLaunchers.
func serveAction(ctx context.Context, args, env []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) > 1 && args[1] == "agent" {
		return Run(ctx, args[1:], IO{In: stdin, Out: stdout, Err: stderr}, "test", func(rt *Runtime) {
			rt.Env = exectest.EnvLookup(env)
		})
	}
	return exectest.Serve(ctx, args, env, stdin, stdout, stderr)
}
