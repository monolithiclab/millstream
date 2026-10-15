package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

// After a run whose scorer fails every item (see newState), the run's metrics, insights and report are in the
// database: a failure cluster to list, acknowledge and mute.
func TestMetricsInsightsAndReport(t *testing.T) {
	t.Parallel()
	s := newState(t, true)
	const runID = "run_r1_adhoc"

	out, _ := s.must(0, "metrics", "--pipeline", "listings", "--name", "items_dead")
	if !strings.Contains(out, "items_dead") || !strings.Contains(out, "class=action,step=score") {
		t.Fatalf("metrics:\n%s", out)
	}
	out, _ = s.must(0, "metrics", "--since", "all", "--prometheus")
	if !strings.Contains(out, `millstream_items_out_total{pipeline="listings",step="crawl",stream="listings"} 3`) ||
		!strings.Contains(out, "# TYPE millstream_delivery_latency_seconds histogram") {
		t.Fatalf("exposition:\n%s", out)
	}
	s.must(2, "metrics", "--since", "soon")

	out, _ = s.must(0, "insights")
	if !strings.Contains(out, "failureCluster") || !strings.Contains(out, "score") {
		t.Fatalf("insights:\n%s", out)
	}
	out, _ = s.must(0, "insights", "list", "--json", "--state", "open")
	var in struct {
		ID, Kind, State string
		Evidence        map[string]any
	}
	for _, line := range splitLines(out) {
		if err := json.Unmarshal([]byte(line), &in); err != nil {
			t.Fatal(err)
		}
		if in.Kind == "failureCluster" {
			break
		}
	}
	if in.Kind != "failureCluster" || in.Evidence["count"] != float64(3) {
		t.Fatalf("insight = %+v", in)
	}
	out, _ = s.must(0, "insights", "ack", in.ID)
	if out != in.ID+" acknowledged\n" {
		t.Fatalf("ack: %q", out)
	}
	s.must(0, "insights", "mute", in.ID, "--pipeline", "listings")
	out, _ = s.must(0, "insights", "list", "--state", "muted")
	if !strings.Contains(out, in.ID) {
		t.Fatalf("muted insights:\n%s", out)
	}
	s.must(1, "insights", "ack", "in_missing")

	out, _ = s.must(0, "run", "report", "listings", runID)
	for _, want := range []string{"Run run_r1_adhoc of listings (trigger): completedWithErrors", "Critical path",
		"Slowest steps:", "Failures by cluster:", "portal answered <n>", "No earlier run to compare with."} {
		if !strings.Contains(out, want) {
			t.Fatalf("report lacks %q:\n%s", want, out)
		}
	}
	out, _ = s.must(0, "run", "report", "listings", runID, "--json")
	var r struct {
		Run      string
		Failures []struct{ Count int }
	}
	if err := json.Unmarshal([]byte(out), &r); err != nil || r.Run != runID || len(r.Failures) != 1 ||
		r.Failures[0].Count != 3 {
		t.Fatalf("report JSON = %+v, %v\n%s", r, err, out)
	}
	s.must(1, "run", "report", "listings", "run_missing")
}
