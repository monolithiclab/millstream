package cli

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/testdir"
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
	out, _ = s.must(0, "insights", "acknowledge", in.ID)
	if out != in.ID+" acknowledged\n" {
		t.Fatalf("acknowledge: %q", out)
	}
	s.must(0, "insights", "mute", in.ID, "--pipeline", "listings")
	out, _ = s.must(0, "insights", "list", "--state", "muted")
	if !strings.Contains(out, in.ID) {
		t.Fatalf("muted insights:\n%s", out)
	}

	out, _ = s.must(0, "report", "listings", runID)
	for _, want := range []string{"Run run_r1_adhoc of listings (trigger): completedWithErrors", "Critical path",
		"Slowest steps:", "Failures by cluster:", "portal answered <n>", "No earlier run to compare with."} {
		if !strings.Contains(out, want) {
			t.Fatalf("report lacks %q:\n%s", want, out)
		}
	}
	out, _ = s.must(0, "report", "listings", runID, "--json")
	var r struct {
		Run      string
		Failures []struct{ Count int }
	}
	if err := json.Unmarshal([]byte(out), &r); err != nil || r.Run != runID || len(r.Failures) != 1 ||
		r.Failures[0].Count != 3 {
		t.Fatalf("report JSON = %+v, %v\n%s", r, err, out)
	}
	// The failure cluster's evidence: exported from this process and imported into the same store (a development one
	// without an environment file); the e2e build moves it between two deployments and purges a key it holds.
	bundle := filepath.Join(testdir.New(t), "bundle.tar.gz")
	for _, c := range []struct {
		cmd  runner
		want string
	}{
		{EvidenceExportCmd{Pipeline: "listings", Since: "30d", Insight: in.ID, Out: bundle}, "(3 sampled)"},
		{EvidenceImportCmd{Bundle: bundle}, " of listings imported, expires "},
	} {
		if out, _, err := s.call(nil, c.cmd); err != nil || !strings.Contains(out, c.want) {
			t.Fatalf("%T: %q, %v; want %q", c.cmd, out, err, c.want)
		}
	}
	s.checkRefusals(t, nil, []refusal{
		{MetricsCmd{Since: "soon"}, true, "--since: invalid duration"},
		{InsightsAcknowledgeCmd{insightRef{ID: "in_missing"}}, false, "insight in_missing: not found"},
		{ReportCmd{Pipeline: "listings", RunID: "run_missing"}, false, "run run_missing: not found"},
		{EvidenceExportCmd{Pipeline: "listings", Since: "soon"}, true, "--since: invalid duration"},
		{EvidenceExportCmd{Pipeline: "listings", Since: "1d", Insight: "in_missing"}, false, "in_missing of listings: not found"},
		{EvidenceImportCmd{Bundle: filepath.Join(testdata, "inbox-patch.json")}, false, "not an evidence bundle"},
	})
}
