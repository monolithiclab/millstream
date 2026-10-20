//go:build e2e

package backtest

import (
	"context"
	"maps"
	"os"
	osexec "os/exec"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/sandbox"
)

// codedMain scores as the fixture's fn does in score mode with no bonus, and tries the network first: in the
// sandbox the dial fails, so the outputs match the history.
const codedMain = `package main

import (
	"net"
	"time"

	millstream "github.com/monolithiclab/millstream/sdk/go"
)

type Listing struct {
	ID    string  ` + "`json:\"id\"`" + `
	Score float64 ` + "`json:\"score\"`" + `
}

func main() {
	millstream.MapKeyed(func(c millstream.Context, in Listing) ([]millstream.Keyed[Listing], error) {
		if conn, err := net.DialTimeout("tcp", "1.1.1.1:53", 2*time.Second); err == nil {
			_ = conn.Close()
			in.Score = -1 // reached the network: the replay disagrees
		}
		return []millstream.Keyed[Listing]{{Key: c.Item().Key, Value: in}}, nil
	})
}
`

// TestSandboxedExecCode: a patch adding a Go exec action and moving score onto it, refused without a sandbox, is
// backtested with the action built and run in the container sandbox, where it reaches no network and agrees with
// the history; the steps around it still run in-process.
func TestSandboxedExecCode(t *testing.T) {
	rt, ok := container.Find(pipeline.RuntimeDocker, os.LookupEnv)
	if !ok {
		t.Skip("docker is not installed: the sandbox backtest is skipped")
	}
	if osexec.Command(rt.Bin, "image", "inspect", sandbox.DefaultImage).Run() != nil { // #nosec G204 -- docker
		t.Skip("the sandbox image is not present: TestSandboxedAction pulls it")
	}
	st, hash := recordHistory(t)
	files := fixtureFiles()
	after := maps.Clone(files)
	after["actions/coded/action.yaml"] = []byte("name: lab/coded\nversion: 0.1.0\nkind: exec\nruntime: { exec: { command: [coded] } }\n")
	after["actions/coded/go.mod"] = []byte("module coded\n\ngo 1.27.0\n\nrequire github.com/monolithiclab/millstream/sdk/go v0.0.0\n")
	after["actions/coded/main.go"] = []byte(codedMain)
	after[pipeline.FileName] = []byte(strings.Replace(fixturePipeline,
		"score: { action: ./actions/fn, in: listings, out: scored, config: { mode: score, bonus: 0 } }",
		"score: { action: ./actions/coded, in: listings, out: scored }", 1))
	data, err := patch.Format(hash, files, after)
	if err != nil {
		t.Fatal(err)
	}
	req := Request{Pipeline: "bt", BaseRevision: hash, Patch: data}
	if _, err := New(Options{Store: st, Launchers: launchers(), Timeout: time.Minute}).Run(context.Background(), req); err == nil {
		t.Fatal("exec code backtested without a sandbox")
	}
	rep, err := New(Options{Store: st, Launchers: launchers(), Sandbox: sandbox.New(rt), Timeout: 2 * time.Minute}).
		Run(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	score := stepReport(t, rep, "score")
	if score.After.Ran != 6 || score.After.Failed != 0 || score.After.Agreement != 1 || rep.After.Agreement != 1 {
		t.Fatalf("score: before %+v, after %+v; overall after %+v", score.Before, score.After, rep.After)
	}
}
