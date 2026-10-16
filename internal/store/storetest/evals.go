package storetest

import (
	"math"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestEvalResults(t *testing.T, open Opener) {
	e := newEnv(t, open)
	first := []store.EvalResult{
		{Action: "lab/score", ActionDigest: "sha256:a", Case: "one", Score: 0.5},
		{Action: "lab/score", ActionDigest: "sha256:a", Case: "two", Score: 1},
		{Action: "lab/other", ActionDigest: "sha256:b", Case: "one", Score: 0},
	}
	if err := e.s.RecordEval(e.ctx, first); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(time.Hour)
	if err := e.s.RecordEval(e.ctx, []store.EvalResult{
		{Action: "lab/score", ActionDigest: "sha256:c", Case: "one", Score: 0.75},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := e.s.EvalResults(e.ctx, "lab/score")
	if err != nil || len(got) != 3 {
		t.Fatalf("EvalResults = %+v, %v", got, err)
	}
	if got[0].Case != "one" || got[0].Score != 0.75 || got[0].ActionDigest != "sha256:c" ||
		!got[0].At.Equal(t0.Add(time.Hour)) || got[2].Case != "one" || got[2].Score != 0.5 || !got[2].At.Equal(t0) {
		t.Fatalf("EvalResults = %+v; want newest first, at the store clock", got)
	}
	if all, err := e.s.EvalResults(e.ctx, ""); err != nil || len(all) != 4 || all[0].Score != 0.75 ||
		all[1].Action != "lab/other" {
		t.Fatalf("every action's results = %+v, %v", all, err)
	}
	for _, bad := range []store.EvalResult{
		{Action: "lab/score", Case: "one", Score: 1.5},
		{Action: "lab/score", Case: "one", Score: math.NaN()},
		{Action: "", Case: "one"},
		{Action: "lab/score"},
	} {
		wantErr(t, e.s.RecordEval(e.ctx, []store.EvalResult{bad}), store.ErrInvalid)
	}
}
