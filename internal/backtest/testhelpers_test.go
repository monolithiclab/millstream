package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// The fixture: crawl emits listings with scores, score adds its config's bonus, keep filters on vars.minScore,
// enrich marks what passes, and notify (effects: external) would tell a CRM.
const fixturePipeline = `pipeline: bt
vars: { minScore: 50 }
triggers: { adhoc: { manual: {} } }
steps:
  crawl: { action: ./actions/fn, in: trigger.adhoc, out: listings, config: { mode: crawl } }
  score: { action: ./actions/fn, in: listings, out: scored, config: { mode: score, bonus: 0 } }
  keep: { action: filter, in: scored, out: shortlist, config: { expr: "item.score >= vars.minScore" } }
  enrich: { action: ./actions/fn, in: shortlist, out: enriched, config: { mode: enrich } }
  notify: { action: ./actions/crm, in: enriched, out: sent }
`

// fixtureFiles are the fixture package's files.
func fixtureFiles() map[string][]byte {
	return map[string][]byte{
		pipeline.FileName:          []byte(fixturePipeline),
		"actions/fn/action.yaml":   []byte("name: lab/fn\nversion: 1.0.0\nkind: exec\nruntime: { exec: { command: [fn] } }\n"),
		"actions/crm/action.yaml":  []byte("name: lab/crm\nversion: 1.0.0\nkind: exec\neffects: external\nruntime: { exec: { command: [crm] } }\n"),
		"actions/crm/notes.txt":    []byte("the CRM client\n"),
		"actions/fn/README.md":     []byte("fn\n"),
		"lessons/unused-file.yaml": []byte("lessons: []\n"),
	}
}

// edit returns files with the pipeline file's old text replaced by new, and the patch from files to them.
func edit(t *testing.T, base string, files map[string][]byte, old, cur string) (map[string][]byte, []byte) {
	t.Helper()
	after := maps.Clone(files)
	src := string(files[pipeline.FileName])
	if strings.Count(src, old) != 1 {
		t.Fatalf("%q is not in the pipeline once", old)
	}
	after[pipeline.FileName] = []byte(strings.Replace(src, old, cur, 1))
	data, err := patch.Format(base, files, after)
	if err != nil {
		t.Fatal(err)
	}
	return after, data
}

// crmCalls counts the deliveries the external action received.
var crmCalls atomic.Int64

// launchers run the fixture's actions in-process.
func launchers() map[string]exec.Launcher {
	return map[string]exec.Launcher{pipeline.KindExec: exec.FuncLauncher{F: serve}}
}

// serve is the fixture's actions: fn by its config's mode, and crm.
func serve(_ context.Context, args, _ []string, stdin io.Reader, stdout, _ io.Writer) int {
	r, w := protocol.NewReader(stdin), protocol.NewWriter(stdout)
	m, err := r.Read()
	if err != nil {
		return 1
	}
	hello, _ := m.(protocol.Hello)
	if w.Write(protocol.Ready{}) != nil {
		return 1
	}
	for {
		m, err := r.Read()
		if err != nil {
			return 0
		}
		it, ok := m.(protocol.Item)
		if !ok {
			return 0
		}
		var in map[string]any
		_ = json.Unmarshal(it.Payload, &in)
		emit := func(key string, v any) {
			b, _ := json.Marshal(v)
			_ = w.Write(protocol.Emit{Delivery: it.Delivery, Key: key, Payload: b})
		}
		switch mode, _ := hello.Config["mode"].(string); {
		case args[0] == "crm":
			crmCalls.Add(1)
			emit(it.Key, map[string]any{"sent": in["id"]})
		case mode == "crawl":
			n, _ := in["n"].(float64)
			base, _ := in["base"].(float64)
			for i := range int(n) {
				id := fmt.Sprintf("L%d", i)
				emit(id, map[string]any{"id": id, "score": base + 10*float64(i)})
			}
		case mode == "score":
			bonus, _ := hello.Config["bonus"].(float64)
			score, _ := in["score"].(float64)
			emit(it.Key, map[string]any{"id": in["id"], "score": score + bonus})
			if cost, ok := hello.Config["cost"].(float64); ok {
				_ = w.Write(protocol.Metric{Delivery: it.Delivery, Name: protocol.MetricCostUSD, Value: cost})
			}
		default:
			in["enriched"] = true
			emit(it.Key, in)
		}
		_ = w.Write(protocol.Done{Delivery: it.Delivery})
	}
}

// seedHistory runs the fixture twice (three listings scored 40, 50 and 60 each time: the first filtered out) in a
// store of its own and returns it with the revision's package hash; the external action's calls are then reset.
func seedHistory() (store.Store, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		return nil, "", err
	}
	fail := func(err error) (store.Store, string, error) {
		_ = st.Close()
		return nil, "", err
	}
	dir, err := os.MkdirTemp("", "backtest-history-")
	if err != nil {
		return fail(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	pkg, err := pipeline.Materialize(dir, fixtureFiles())
	if err != nil {
		return fail(err)
	}
	e, err := engine.New(engine.Options{Store: st, NoCron: true, Artifacts: artifacts.New(filepath.Join(dir, "a"))})
	if err != nil {
		return fail(err)
	}
	if _, err := e.Apply(ctx, pkg); err != nil {
		return fail(err)
	}
	var refs []engine.RunRef
	for _, key := range []string{"day1", "day2"} {
		id, _, err := e.Fire(ctx, "bt", "adhoc", key, json.RawMessage(`{"n":3,"base":40}`))
		if err != nil {
			return fail(err)
		}
		refs = append(refs, engine.RunRef{Pipeline: "bt", ID: id})
	}
	pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{Launchers: launchers()}})
	defer func() { _ = pool.Close() }()
	n, err := node.New(e, node.Options{Owner: "history", Slots: 4, Pool: pool, PullInterval: 10 * time.Millisecond})
	if err != nil {
		return fail(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done, builtinsDone := make(chan error, 1), make(chan struct{})
	go func() { done <- n.Run(runCtx) }()
	go func() { e.RunBuiltins(runCtx); close(builtinsDone) }()
	werr := e.WaitSettled(ctx, refs)
	stop()
	<-builtinsDone
	if err := errors.Join(<-done, werr); err != nil {
		return fail(err)
	}
	crmCalls.Store(0)
	return st, pkg.Hash, nil
}

func stepReport(t *testing.T, rep *Report, step string) StepReport {
	t.Helper()
	for _, s := range rep.Steps {
		if s.Step == step {
			return s
		}
	}
	t.Fatalf("no report for step %s in %+v", step, rep.Steps)
	return StepReport{}
}
