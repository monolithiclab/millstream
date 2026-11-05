//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/examples/m5-distill/tiers"
	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/improve"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// The M5 exit's two deployments (spec section 11.2): production runs the pipelines and exports their evidence, with
// a sample large enough to carry every tier delivery; development runs the improve pipeline and publishes with apply.
var (
	exitProdEnvFile = "environment: production\nagents: { allowUnsandboxed: true }\n" +
		"improve: { evidence: { sample: 1000 } }\n" + exitPrices
	exitDevEnvFile = "environment: development\nagents: { allowUnsandboxed: true }\n" +
		"improve: { enabled: true, harness: api, publish: apply }\n" + exitPrices
	exitPrices = fmt.Sprintf("prices: { %s: { inPerMtok: %d, outPerMtok: %d }, %s: { inPerMtok: 3, outPerMtok: 15 }, "+
		"%s: { inPerMtok: 2, outPerMtok: 10 } }\n", m2Model, m2InPerMtok, m2OutPerMtok, tierModel, improve.APIModel)
)

// exitDay is the day of new listings both deployments run once the proposals are published: production on the
// agent, development on the distilled code with the agent as its fallback.
const exitDay = 3

// TestM5Exit is the M5 exit criterion (spec section 19): evidence exported from a production deployment of the
// reference pipelines yields, in development, an accepted lessons proposal and an accepted distill proposal whose code
// cuts agent cost with holdout agreement of at least 0.98.
//
// Production runs the M2 fixture with its review step, where analysts correct the scores of poor listings
// (a reviewPattern), and the distill fixture's two days of tiering (tier is distillable); it refuses improve and
// publish: apply, and exports a bundle of each pipeline. Development applies the same packages, imports the bundles
// and has no history of its own: millstream improve drafts both proposals from the imported insights through the
// recorded draft agent, and the engine validates them on the imported evidence, the lessons by a backtest and the
// distill on its holdout in the container sandbox. Both are accepted with the patch hash read and published with
// apply through the API. A day of new listings then costs development, on the distilled code with the agent as
// fallback, a fraction of what it costs production on the agent.
func TestM5Exit(t *testing.T) {
	t.Parallel()
	dockerSandbox(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	prodEnv, devEnv := filepath.Join(dir, "production.yaml"), filepath.Join(dir, "development.yaml")
	applyEnv := filepath.Join(dir, "production-apply.yaml")
	for f, body := range map[string]string{prodEnv: exitProdEnvFile, devEnv: exitDevEnvFile,
		applyEnv: "environment: production\nimprove: { publish: apply }\n"} {
		if err := os.WriteFile(f, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	recordings := t.TempDir()
	copyRecordings(t, m2Recordings, recordings)
	copyRecordings(t, improveRecordings(root), recordings)
	copyRecordings(t, distillRecordings(root), recordings)
	recordTiers(t, recordings, 1, 2, exitDay)
	env := append(m2Env(recordings), "MILLSTREAM_SECRET_TIER_KEY=sk-fake-0123456789",
		"MILLSTREAM_SECRET_ANTHROPIC_KEY=sk-fake-0123456789")
	prod, dev := dbPath(t), dbPath(t)

	// Production: the history, and what it refuses.
	lessonsInsight := reviewHistory(t, prod, env, prodEnv)
	distillInsight := distillHistory(t, prod, env, prodEnv)
	if r := millstream(t, prod, env, "--env-file", prodEnv, "improve"); r.code == 0 ||
		!strings.Contains(r.stderr, "development deployments only") {
		t.Fatalf("improve in production: %s", r)
	}
	if r := millstream(t, prod, nil, "--env-file", applyEnv, "status"); r.code == 0 ||
		!strings.Contains(r.stderr, "publish: apply is refused in production") {
		t.Fatalf("publish: apply in production: %s", r)
	}
	bundles := map[string]string{}
	for _, p := range []string{improvePipeline, distillPipeline} {
		bundles[p] = filepath.Join(dir, p+".tar.gz")
		r := mustExit(t, 0, prod, nil, "--env-file", prodEnv, "evidence", "export", "--pipeline", p, "--since", "1d",
			"--out", bundles[p])
		t.Logf("production: %s", strings.TrimSpace(r.stdout))
	}
	// Production's day of new listings, on the agent: the cost the distill proposal is measured against.
	if r := runDay(t, prod, env, prodEnv, fixtureDistill, exitDay); r.code != 0 {
		t.Fatalf("production's day %d: %s", exitDay, r)
	}
	prodDay := tierDay(t, prod, exitDay)

	// Development: the same packages, the bundles imported, no history of its own.
	mustExit(t, 0, dev, env, "--env-file", devEnv, "apply", fixtureM2Review, fixtureDistill)
	for _, p := range []string{improvePipeline, distillPipeline} {
		r := mustExit(t, 0, dev, nil, "--env-file", devEnv, "evidence", "import", bundles[p])
		if !strings.Contains(r.stdout, " of "+p+" imported, expires ") || strings.Contains(r.stdout, "diverged") {
			t.Fatalf("import %s: %s", p, r)
		}
	}
	imported := map[string]api.Insight{}
	for _, line := range strings.Split(strings.TrimSpace(mustExit(t, 0, dev, nil, "insights", "list", "--json").stdout),
		"\n") {
		var in api.Insight
		if err := json.Unmarshal([]byte(line), &in); err != nil {
			t.Fatalf("insights in development: %v: %s", err, line)
		}
		imported[in.ID] = in
	}
	for _, in := range []api.Insight{lessonsInsight, distillInsight} {
		if got, ok := imported[in.ID]; !ok || got.Kind != in.Kind || got.Imported == "" {
			t.Fatalf("development lists %+v for the production insight %s (%s)", got, in.ID, in.Kind)
		}
	}

	// The improve pipeline drafts both proposals, and the engine validates them on the imported evidence.
	lessons := improveOne(t, dev, env, devEnv, lessonsInsight, improvePipeline, "lessons")
	var lr lessonsReport
	if err := json.Unmarshal(lessons.Report, &lr); err != nil {
		t.Fatal(err)
	}
	bt := lr.Backtest.Report
	if !lr.Passed || !lr.Backtest.Passed || bt.Source != "evidence" || len(bt.Bundles) != 1 || bt.Sampled == 0 ||
		len(bt.ChangedSteps) != 1 || bt.ChangedSteps[0] != "score" ||
		bt.After.ReviewAgreement <= bt.Before.ReviewAgreement || lessons.Paths[0] != "lessons/score.yaml" ||
		!strings.Contains(lessons.Patch, "lower its score by 25 points") {
		t.Fatalf("lessons proposal %+v\nreport %s", lessons, lessons.Report)
	}
	distilled := improveOne(t, dev, env, devEnv, distillInsight, distillPipeline, "distill")
	var dr distillReport
	if err := json.Unmarshal(distilled.Report, &dr); err != nil {
		t.Fatal(err)
	}
	d := dr.Distill
	if !dr.Passed || !d.Passed || d.Step != "tier" || d.Action != "./actions/tier-rules" ||
		d.Fallback != "./actions/tier" || d.Holdout < 250 || d.Covered < 200 || d.Agreement < 0.98 ||
		d.Coverage < 0.6 || d.Agreed != d.Covered || d.AgentCostSavedUSD <= 0 ||
		d.AgentCostSavedUSD >= d.AgentCostUSD || len(dr.NewEvals) != 1 || !dr.NewEvals[0].Passed {
		t.Fatalf("distill proposal %+v\nreport %s", distilled, distilled.Report)
	}

	// Both are accepted on the patch read and published with apply, through the API of a served engine.
	pathDir := t.TempDir() // where the distilled action is built, its manifest's bare command found on PATH
	devRunEnv := append(slices.Clone(env), "PATH="+pathDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	devProc := startServe(t, dev, devRunEnv, devEnv)
	revisions := map[string]int64{}
	for _, p := range []api.Proposal{lessons, distilled} {
		mustExit(t, 0, dev, nil, append([]string{"--server", devProc.url, "proposals", "accept", p.ID,
			"--patch-sha256", p.PatchSHA256}, reviewer...)...)
		var pub api.PublishedProposal
		apiJSON(t, devProc.url, http.MethodPost, "/proposals/"+p.ID+"/publish",
			api.PublishRequest{PatchSHA256: p.PatchSHA256}, &pub)
		var pl api.Pipeline
		apiJSON(t, devProc.url, http.MethodGet, "/pipelines/"+p.Pipeline, nil, &pl)
		if pub.Publisher != pipeline.PublishApply || pub.Again || pl.Revision == 0 {
			t.Fatalf("published %s: %+v, pipeline %+v", p.ID, pub, pl)
		}
		revisions[p.Pipeline] = pl.Revision
	}
	var lessonsFiles, distillFiles api.RevisionFiles
	apiJSON(t, devProc.url, http.MethodGet, fmt.Sprintf("/pipelines/%s/revisions/%d/files", improvePipeline,
		revisions[improvePipeline]), nil, &lessonsFiles)
	apiJSON(t, devProc.url, http.MethodGet, fmt.Sprintf("/pipelines/%s/revisions/%d/files", distillPipeline,
		revisions[distillPipeline]), nil, &distillFiles)
	devProc.stop(t)
	if !strings.Contains(lessonsFiles.Files["lessons/score.yaml"], "lower its score by 25 points") {
		t.Fatalf("the lessons revision's lessons:\n%s", lessonsFiles.Files["lessons/score.yaml"])
	}

	// The distill revision runs a day of new listings: the code tiers what it covers, the agent the rest.
	pkg := materialize(t, distillFiles)
	buildDistilled(t, root, pkg, filepath.Join(pathDir, "tier-rules"))
	if r := runDay(t, dev, devRunEnv, devEnv, pkg, exitDay); r.code != 0 {
		t.Fatalf("development's day %d: %s", exitDay, r)
	}
	devDay := tierDay(t, dev, exitDay)
	if devDay.revision != revisions[distillPipeline] {
		t.Fatalf("development ran revision %d, want the published %d", devDay.revision, revisions[distillPipeline])
	}
	abstained := 0
	for _, l := range tiers.Day(exitDay, distillPerDay) {
		if l.SurfaceM2 <= 0 {
			abstained++
		}
	}
	if prodDay.items != distillPerDay || prodDay.fallback != 0 || prodDay.wrong != 0 ||
		devDay.items != distillPerDay || devDay.fallback != abstained || devDay.wrong != 0 ||
		devDay.agentCalls != abstained || prodDay.agentCalls != distillPerDay ||
		prodDay.audits != 0 || devDay.audits == 0 || devDay.agreeing != devDay.audits ||
		devDay.costUSD <= 0 || devDay.costUSD+devDay.auditUSD >= prodDay.costUSD/10 {
		t.Fatalf("day %d: production %+v, development %+v (%d listings without a surface)", exitDay, prodDay,
			devDay, abstained)
	}
	t.Logf("lessons: backtest on %d sampled deliveries of bundle %s, review agreement %.2f -> %.2f",
		bt.Sampled, bt.Bundles[0], bt.Before.ReviewAgreement, bt.After.ReviewAgreement)
	t.Logf("distill: holdout %d, covered %d (coverage %.3f), agreed %d (agreement %.3f), agent cost over the holdout "+
		"%.6f USD, saved %.6f USD", d.Holdout, d.Covered, d.Coverage, d.Agreed, d.Agreement, d.AgentCostUSD,
		d.AgentCostSavedUSD)
	t.Logf("day %d: production %d agent calls, %.6f USD; development %d by the code, %d by the agent, %.6f USD (%.1f%% of "+
		"production's), and %d audits, all agreeing, %.6f USD (%.1f%% with them)", exitDay, prodDay.agentCalls, prodDay.costUSD,
		devDay.items-devDay.fallback, devDay.fallback, devDay.costUSD, 100*devDay.costUSD/prodDay.costUSD, devDay.audits,
		devDay.auditUSD, 100*(devDay.costUSD+devDay.auditUSD)/prodDay.costUSD)
}

// improveOne runs millstream improve on one insight and returns the one proposal of pipelineName it leaves ready.
func improveOne(t *testing.T, db string, env []string, envFile string, in api.Insight, pipelineName,
	kind string,
) api.Proposal {
	t.Helper()
	r := millstream(t, db, env, "--env-file", envFile, "improve", "--insight", in.ID)
	out := mustExit(t, 0, db, nil, "proposals", "list", "--pipeline", pipelineName, "--json")
	var p api.Proposal
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.stdout)), &p); err != nil {
		t.Fatalf("improve: %s\nproposals: %v: %s", r, err, out.stdout)
	}
	if r.code != 0 || !strings.Contains(r.stdout, "proposed "+kind+" on "+pipelineName) || p.Kind != kind ||
		p.State != string(store.ProposalReady) || p.CreatedBy != improve.Name || len(p.Insights) != 1 ||
		p.Insights[0] != in.ID {
		t.Fatalf("improve: %s\nproposal %+v\nreport %s", r, p, p.Report)
	}
	return p
}

// tierRun is what a day of the distill fixture did at its tier step.
type tierRun struct {
	revision int64
	// items are the tiered listings, fallback those the agent tiered under the step's fallback, wrong those whose
	// tier is not tiers.Tier's.
	items, fallback, wrong int
	// audits are the audits of those items that ended, agreeing how many agreed, auditUSD what they cost together.
	audits, agreeing int
	auditUSD         float64
	// agentCalls are the tier deliveries that cost anything, costUSD what they cost together.
	agentCalls int
	costUSD    float64
}

// tierDay reads what the tier step did with day's listings on db.
func tierDay(t *testing.T, db string, day int) tierRun {
	t.Helper()
	st := openStore(t, db)
	defer func() { _ = st.Close() }()
	ctx := t.Context()
	prefix := fmt.Sprintf("D%d-", day)
	listed, err := st.Items(ctx, store.ItemFilter{Pipeline: distillPipeline, Stream: "listings.new"})
	if err != nil {
		t.Fatal(err)
	}
	inputs := map[string]bool{}
	var out tierRun
	runID := ""
	for _, it := range listed {
		if strings.HasPrefix(it.Key, prefix) {
			inputs[it.ID], runID = true, it.RunID // one run lists the day
		}
	}
	run, err := st.Run(ctx, distillPipeline, runID)
	if err != nil {
		t.Fatal(err)
	}
	out.revision = int64(run.Revision)
	tiered, err := st.Items(ctx, store.ItemFilter{Pipeline: distillPipeline, Stream: "listings.tiered"})
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range tiered {
		if !strings.HasPrefix(it.Key, prefix) {
			continue
		}
		out.items++
		if it.ProducedBy == store.ProducedByFallback {
			out.fallback++
		}
		var got tiers.Tiered
		if err := json.Unmarshal(it.Payload, &got); err != nil || got.Tier != tiers.Tier(got.Listing) {
			out.wrong++
		}
	}
	ds, err := st.Deliveries(ctx, store.DeliveryFilter{Pipeline: distillPipeline})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if d.Step == "tier" && inputs[d.ItemID] && d.CostUSD > 0 {
			out.agentCalls++
			out.costUSD += d.CostUSD
		}
	}
	audits, err := st.Audits(ctx, store.AuditFilter{Pipeline: distillPipeline})
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range audits {
		if inputs[a.ItemID] && a.Agreement != nil {
			out.audits++
			out.auditUSD += a.CostUSD
			if *a.Agreement == 1 {
				out.agreeing++
			}
		}
	}
	return out
}

// startServe starts serve on db under envFile, on a free loopback port, until the test ends or it is stopped.
func startServe(t *testing.T, db string, env []string, envFile string) *engineProc {
	t.Helper()
	p := &engineProc{name: "development", log: newProcLog(), done: make(chan struct{})}
	cmd, _, _ := command(t.Context(), db, env, "--env-file", envFile, "serve", "--listen", "127.0.0.1:0",
		"--lease-ttl", "1s")
	cmd.Stdout, cmd.Stderr = p.log, p.log
	p.cmd = cmd
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		p.code = exitCode(t, cmd.Wait())
		close(p.done)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-p.done
		if t.Failed() {
			t.Logf("serve:\n%s", lastLines(p.log.String(), 60))
		}
	})
	line := p.waitLine(t, "serving the UI and the API on ", time.Minute)
	_, url, _ := strings.Cut(line, "serving the UI and the API on ")
	p.url = strings.TrimSuffix(strings.TrimSpace(url), "/")
	return p
}

// apiJSON sends a request with body as JSON to the API at base (/api/v1 added) and decodes its 2xx answer into out.
func apiJSON(t *testing.T, base, method, route string, body, out any) {
	t.Helper()
	data := ""
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		data = string(b)
	}
	code, answer := apiCall(t, base, method, "/api/v1"+route, data, nil)
	if code/100 != 2 {
		t.Fatalf("%s %s: %d: %s", method, route, code, answer)
	}
	if err := json.Unmarshal([]byte(answer), out); err != nil {
		t.Fatalf("%s %s: %v: %s", method, route, err, answer)
	}
}

// materialize writes a revision's files over a copy of the distill fixture, whose binary files (the compiled list
// action) the API does not serve: the package is the revision's, its hash unchanged.
func materialize(t *testing.T, files api.RevisionFiles) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), distillPipeline)
	if err := os.CopyFS(dir, os.DirFS(fixtureDistill)); err != nil {
		t.Fatal(err)
	}
	for name, content := range files.Files {
		p := filepath.Join(dir, filepath.FromSlash(path.Clean(name)))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// buildDistilled compiles the distilled action of the package at pkg to out, against the repository's SDK, as the
// sandbox builds it: the program its manifest names, which the step finds on PATH.
func buildDistilled(t *testing.T, root, pkg, out string) {
	t.Helper()
	tmp, err := filepath.EvalSymlinks(t.TempDir()) // the go command compares its real working directory
	if err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(tmp, "tier-rules") // a copy, so the build leaves the package as it is
	if err := os.CopyFS(src, os.DirFS(filepath.Join(pkg, "actions", "tier-rules"))); err != nil {
		t.Fatal(err)
	}
	gomod, err := os.ReadFile(filepath.Join(src, "go.mod")) // #nosec G304 -- the revision's
	if err != nil {
		t.Fatal(err)
	}
	version := ""
	for _, line := range strings.Split(string(gomod), "\n") {
		if v, ok := strings.CutPrefix(line, "go "); ok {
			version = strings.TrimSpace(v)
		}
	}
	work := filepath.Join(tmp, "go.work")
	if err := os.WriteFile(work, []byte(fmt.Sprintf("go %s\n\nuse (\n\t./tier-rules\n\t%s\n)\n", version,
		filepath.Join(root, "sdk", "go"))), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := goBuildEnv(src, out, ".", []string{"GOWORK=" + work, "GOFLAGS="}); err != nil {
		t.Fatal(err)
	}
}
