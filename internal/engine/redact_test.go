package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

const secretPipeline = `
pipeline: portal
secrets: [apiToken]
triggers:
  adhoc: { manual: {} }
steps:
  fetch:
    action: ./actions/fake
    in: trigger.adhoc
    config: { mode: secret, secret: "${secrets.apiToken}" }
`

const secretValue = "tok-5f2a9c71"

// A secret an action echoes to stderr and into its error message reaches neither the logs, nor the delivery's
// stored error, nor the dead letter: each holds [redacted:apiToken] instead (spec section 4.5).
func TestSecretsRedacted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "actions", "fake"), 0o750); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"pipeline.yaml":            secretPipeline,
		"actions/fake/action.yaml": "name: fake\nversion: 1.0.0\nkind: exec\nruntime: { exec: { command: [\"./fakeaction\"] } }\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	linkFakeAction(t, dir, "actions/fake/fakeaction")
	pkg, err := pipeline.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	logs := &syncBuffer{}
	env := func(k string) (string, bool) {
		if k == pipeline.SecretEnv("apiToken") {
			return secretValue, true
		}
		return "", false
	}
	e, err := New(Options{Store: st, NoCron: true, TickInterval: 20 * time.Millisecond, Logger: newLogger(logs), Env: env})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := e.Apply(ctx, pkg); err != nil {
		t.Fatal(err)
	}
	runID, _, err := e.Fire(ctx, "portal", "adhoc", "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{ShutdownTimeout: 300 * time.Millisecond}})
	t.Cleanup(func() { _ = pool.Close() })
	n, err := node.New(e, node.Options{Owner: "node-a", Pool: pool, PullInterval: 20 * time.Millisecond,
		Logger: newLogger(logs)})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	engineDone, nodeDone := make(chan error, 1), make(chan error, 1)
	go func() { engineDone <- e.Run(runCtx) }()
	<-e.Ready()
	go func() { nodeDone <- n.Run(runCtx) }()
	if err := e.WaitSettled(ctx, []RunRef{{Pipeline: "portal", ID: runID}}); err != nil {
		t.Fatalf("WaitSettled: %v\nlogs:\n%s", err, logs.String())
	}
	stop()
	if err := <-nodeDone; err != nil {
		t.Fatalf("node: %v", err)
	}
	if err := <-engineDone; err != nil {
		t.Fatalf("engine: %v", err)
	}
	pool.Close() // flushes the last stderr lines

	const redacted = "[redacted:apiToken]"
	check := func(what, text string) {
		t.Helper()
		if strings.Contains(text, secretValue) {
			t.Errorf("%s holds the secret: %s", what, text)
		}
		if !strings.Contains(text, redacted) {
			t.Errorf("%s does not hold %s: %s", what, redacted, text)
		}
	}
	check("logs", logs.String())
	ds, err := st.Deliveries(ctx, store.DeliveryFilter{RunID: runID})
	if err != nil || len(ds) != 1 || ds[0].State != store.DeliveryDead {
		t.Fatalf("deliveries = %+v, %v; want one dead", ds, err)
	}
	check("stored error", ds[0].ErrorMessage)
	dls, err := st.DeadLetters(ctx, store.DeadLetterFilter{RunID: runID})
	if err != nil || len(dls) != 1 {
		t.Fatalf("dead letters = %+v, %v", dls, err)
	}
	check("dead letter", dls[0].Error)
	items, err := st.Items(ctx, store.ItemFilter{Stream: "fetch.dlq"})
	if err != nil || len(items) != 1 {
		t.Fatalf("dlq items = %+v, %v", items, err)
	}
	check("dead-letter payload", string(items[0].Payload))
}
