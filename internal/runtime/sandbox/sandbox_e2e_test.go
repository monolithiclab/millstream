//go:build e2e

package sandbox

import (
	"os"
	osexec "os/exec"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/eval"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// Runtime returns the local Docker runtime with the sandbox's image present (pulled once when missing), or skips.
func dockerSandbox(t *testing.T) *Sandbox {
	t.Helper()
	rt, ok := container.Find(pipeline.RuntimeDocker, os.LookupEnv)
	if !ok {
		t.Skip("docker is not installed: the sandbox tests are skipped")
	}
	if osexec.Command(rt.Bin, "image", "inspect", DefaultImage).Run() != nil { // #nosec G204 -- docker, our image
		if out, err := osexec.Command(rt.Bin, "pull", "--quiet", DefaultImage).CombinedOutput(); err != nil { // #nosec G204
			t.Skipf("the sandbox image is not available: %s", out)
		}
	}
	return New(rt)
}

// probeMain tries what proposed code must not do: reach the network, write outside its working directory, read a
// credential of the node's environment; and says what it managed.
const probeMain = `package main

import (
	"net"
	"os"
	"time"

	millstream "github.com/monolithiclab/millstream/sdk/go"
)

type In struct {
	Addr string ` + "`json:\"addr\"`" + `
}

type Out struct {
	Ran    bool ` + "`json:\"ran\"`" + `
	Dialed bool ` + "`json:\"dialed\"`" + `
	Wrote  bool ` + "`json:\"wrote\"`" + `
	Token  bool ` + "`json:\"token\"`" + `
}

func main() {
	millstream.Map(func(_ millstream.Context, in In) ([]Out, error) {
		c, err := net.DialTimeout("tcp", in.Addr, 3*time.Second)
		if err == nil {
			_ = c.Close()
		}
		werr := os.WriteFile("/etc/probe", []byte("x"), 0o600)
		_, token := os.LookupEnv("GH_TOKEN")
		return []Out{{Ran: true, Dialed: err == nil, Wrote: werr == nil, Token: token}}, nil
	})
}
`

// TestSandboxedAction: a proposed Go action using the SDK builds with no network and runs in the sandbox, where it
// reaches no network, writes nothing outside its working directory and sees no credential of the node.
func TestSandboxedAction(t *testing.T) {
	s := dockerSandbox(t)
	files := probeFiles()
	files["actions/probe/go.mod"] = []byte("module probe\n\ngo 1.27.0\n\nrequire github.com/monolithiclab/millstream/sdk/go v0.0.0\n")
	files["actions/probe/main.go"] = []byte(probeMain)
	files["actions/probe/evals/probe.yaml"] = []byte("input: { addr: \"1.1.1.1:53\" }\n" +
		"assert: [output.ran, \"!output.dialed\", \"!output.wrote\", \"!output.token\"]\n")
	pkg, err := pipeline.Materialize(testdir.New(t), files)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_TOKEN", "secret")
	ls, cleanup, err := s.Launchers(t.Context(), pkg, []string{"actions/probe/"},
		map[string]exec.Launcher{pipeline.KindExec: exec.ExecLauncher{}})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	act, err := pipeline.Resolver{}.Resolve(pkg, "./actions/probe")
	if err != nil {
		t.Fatal(err)
	}
	cases, err := eval.Load(pkg.Dir + "/actions/probe")
	if err != nil {
		t.Fatal(err)
	}
	res := eval.Run(t.Context(), eval.Options{Package: pkg, Action: act, Ref: "./actions/probe", Launchers: ls,
		Env: os.LookupEnv}, cases[0])
	if res.Err != "" || res.Passed != res.Total {
		t.Fatalf("the probe: %+v", res)
	}
	files["actions/probe/main.go"] = []byte(strings.Replace(probeMain, "func main() {", "func main() {\n\tundefined()", 1))
	broken, err := pipeline.Materialize(testdir.New(t), files)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Launchers(t.Context(), broken, []string{"actions/probe"}, nil); err == nil ||
		!strings.Contains(err.Error(), "undefined") {
		t.Fatalf("a build failure: %v", err)
	}
}
