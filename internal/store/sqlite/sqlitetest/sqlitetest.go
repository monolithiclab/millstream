// Package sqlitetest prepares a test binary for the in-memory SQLite stores its tests open. What a process does once
// (SQLite's initialisation, the template image's load, the first statements of the common units, which closed
// Memory stores hand on prepared, and the CEL and schema set-up of parsing a pipeline) belongs to the process, not to
// whichever test happens to run first: a TestMain calls Warm before the tests, as it builds its shared fixtures.
// scripts/test-times leaves TestMain out of the budget.
package sqlitetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/schema"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// fixture is the pipeline Warm runs a delivery of: a trigger, a step and a stream with a routed output.
const fixture = `
pipeline: warm
triggers:
  go: { manual: {} }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.go
    out:
      - { when: "item.n > 0", out: listings }
  score:
    action: ./actions/score
    in: listings
`

// Warm builds the CEL environments and the JSON Schema compiler, opens a Memory store, runs the common units and
// reads on it (a run fired, a delivery done, another dead, replayed and discarded, a pause, a cancel), and closes the
// store, leaving its connection in the pool with those statements prepared. Failures are ignored: the tests then pay the set-up themselves and report what fails.
func Warm() {
	_, _ = expr.CompileItem("item.n > 1")
	_, _ = expr.Interpolate(map[string]any{"n": "${1}"}, expr.InterpCtx{})
	_, _ = schema.Compile([]byte(`{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}`))
	s, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		return
	}
	defer func() { _ = s.Close() }()
	p, err := pipeline.Parse([]byte(fixture))
	if err != nil {
		return
	}
	ctx := context.Background()
	sum := sha256.Sum256([]byte(fixture))
	if _, err := s.ApplyRevision(ctx, store.PackageRecord{Hash: hex.EncodeToString(sum[:]), Pipeline: p}); err != nil {
		return
	}
	if _, err := s.Fire(ctx, store.Firing{Pipeline: "warm", Trigger: "go", Key: "warm", RunID: "warm"}); err != nil {
		return
	}
	ls, err := s.Lease(ctx, store.LeaseRequest{Owner: "warm", Max: 1, TTL: time.Minute})
	if err != nil || len(ls) != 1 {
		return
	}
	_, _ = s.Renew(ctx, "warm", []store.LeaseRef{ls[0].Ref()}, time.Minute)
	_, _ = s.Done(ctx, store.DoneRequest{LeaseRef: ls[0].Ref(), Attempt: ls[0].Attempt,
		Outputs: []store.Output{{Key: "a", Payload: json.RawMessage(`{"n":1}`)}}})
	// The score delivery fails for good: a dead letter, replayed, then discarded.
	if ls, err = s.Lease(ctx, store.LeaseRequest{Owner: "warm", Max: 1, TTL: time.Minute}); err == nil && len(ls) == 1 {
		_, _ = s.Fail(ctx, store.FailRequest{LeaseRef: ls[0].Ref(), Attempt: ls[0].Attempt, Class: "warm",
			Message: "warm"})
	}
	if dls, err := s.DeadLetters(ctx, store.DeadLetterFilter{Pipeline: "warm"}); err == nil && len(dls) > 0 {
		_, _ = s.ReplayDeadLetters(ctx, "warm", "warm", []string{dls[0].ID}, false)
		_, _ = s.DiscardDeadLetters(ctx, "warm", []string{dls[0].ID})
	}
	_ = s.SetPaused(ctx, "warm", "", true)
	_ = s.SetPaused(ctx, "warm", "", false)
	_, _ = s.ExpireLeases(ctx)
	_ = s.Cancel(ctx, "warm", "warm")
	// The reads every package's tests make.
	_, _ = s.Runs(ctx, store.RunFilter{})
	_, _ = s.Run(ctx, "warm", "warm")
	_, _ = s.Items(ctx, store.ItemFilter{Pipeline: "warm"})
	_, _ = s.Deliveries(ctx, store.DeliveryFilter{Pipeline: "warm"})
	_, _ = s.Events(ctx, 0, 10)
	_, _ = s.Pipelines(ctx)
	_, _ = s.PipelineState(ctx, "warm")
	_, _ = s.ActiveRevision(ctx, "warm")
}
