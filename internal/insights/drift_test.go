package insights

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Drift from audits: a step whose fallback disagreed with its primary on 2 of 8 audits in the window is a warning,
// its disagreeing items as samples and its failed audit counted apart; a step at 1 of 21 is under the threshold,
// and audits before the window are not counted.
func TestDriftFromAudits(t *testing.T) {
	t.Parallel()
	var evs []store.Event
	ended := func(h int, step, item, data string) {
		evs = append(evs, store.Event{TS: now.Add(-time.Duration(h) * time.Hour), Kind: store.EventAuditEnded,
			Data: json.RawMessage(fmt.Sprintf(`{"step":%q,"itemId":%q,%s}`, step, item, data))})
	}
	for i := range 6 {
		ended(3, "score", fmt.Sprint("ok", i), `"state":"done","agreement":1`)
	}
	ended(2, "score", "it_a", `"state":"done","agreement":0`)
	ended(2, "score", "it_b", `"state":"done","agreement":0`)
	ended(2, "score", "it_c", `"state":"failed","class":"leaseExpired"`)
	ended(30, "score", "it_old", `"state":"done","agreement":0`) // before the window
	for i := range 20 {
		ended(1, "rules", fmt.Sprint("r", i), `"state":"done","agreement":1`)
	}
	ended(1, "rules", "it_r", `"state":"done","agreement":0`)
	d := &Data{Pipeline: "shop", Now: now}
	var err error
	if d.Audits, err = readAudits(t.Context(), eventStore{evs: evs}, "shop", time.Time{}); err != nil {
		t.Fatal(err)
	}
	got := Compute(d, []string{pipeline.InsightDrift})
	if len(got) != 1 || got[0].Scope != "score" || got[0].Severity != SeverityWarning {
		t.Fatalf("drift insights = %+v", got)
	}
	want := `{"agreed":6,"agreement":0.75,"audits":9,"disagreed":2,"disagreementShare":0.25,"failed":1,` +
		`"samples":["it_a","it_b"],"source":"audits","threshold":0.05}`
	if string(got[0].Evidence) != want {
		t.Fatalf("evidence = %s\nwant       %s", got[0].Evidence, want)
	}
	if s := Summary(got[0]); s != "the fallback disagreed with the primary on 25% of 9 audits (2 disagreed, 1 failed)" {
		t.Fatalf("summary = %q", s)
	}
}
