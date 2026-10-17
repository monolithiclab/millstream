package pipeline

import (
	"os"
	"runtime/debug"
	"testing"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/schema"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// TestMain sets up before the tests what the process builds once, on first use: the CEL environments, the YAML
// decoding of a pipeline and the JSON Schema compiler. It belongs to no test.
func TestMain(m *testing.M) {
	// The tests allocate heavily: a higher collector target trades memory for test time, as in the engine, cli and
	// store tests.
	debug.SetGCPercent(400)
	warm()
	code := m.Run()
	testdir.Remove()
	os.Exit(code)
}

func warm() {
	_, _ = Parse([]byte("pipeline: warm\ntriggers: { go: { manual: {} } }\nsteps:\n" +
		"  a: { action: ./actions/a, in: trigger.go, out: [{ when: \"item.n > 0\", out: s }], config: { n: \"${1}\" } }\n" +
		"  b: { action: filter, in: s, config: { expr: \"item.n > 1\" } }\n"))
	_, _ = expr.CompileItem("item.n > 1")
	_, _ = expr.CompileAssertion("output.n > 1")
	_, _ = schema.Compile([]byte(`{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}`))
}
