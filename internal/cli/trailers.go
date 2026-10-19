package cli

import (
	"context"
	osexec "os/exec"
	"time"

	"github.com/monolithiclab/millstream/internal/publish"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// proposalTrailers returns the proposals the Millstream-Proposal trailers of the last commit that touched dir name
// (spec section 11.3): none when dir is not in a git checkout or git is not installed. git runs with hooks disabled
// and without the system's or the user's configuration.
func proposalTrailers(rt *Runtime, dir string) []string {
	path, _ := rt.Env("PATH")
	git, ok := exec.LookPath("git", path)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(rt.Ctx, 10*time.Second)
	defer cancel()
	cmd := osexec.CommandContext(ctx, git, "-c", "core.hooksPath=/dev/null", "-C", dir, "log", "-1", "--format=%B",
		"--", ".") // #nosec G204 -- git, with arguments we build
	cmd.Env = []string{"PATH=" + path, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	return publish.Trailers(string(out))
}
