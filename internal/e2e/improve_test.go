//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
	"github.com/monolithiclab/millstream/examples/m2-fixture/pool"
	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/improve"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// update rewrites the improve pipeline's draft recording (internal/improve/testdata/recordings) from the fake
// drafter: go test -tags e2e ./internal/e2e -run TestImprove -update (make recordings).
var update = flag.Bool("update", false, "rewrite the improve pipeline's draft recording")

// fixtureM2Review is the M2 fixture with a review step after score, where analysts correct the scores; improveEnv
// runs the improve pipeline, its draft agent through the api harness, replayed.
var fixtureM2Review, improveEnv string

// improvePipeline is the review fixture's pipeline name, and improveRecordings the committed draft recordings.
const improvePipeline = "m2-review"

func improveRecordings(root string) string {
	return filepath.Join(root, "internal", "improve", "testdata", "recordings")
}

// improveEnvFile is the development environment the improve pipeline runs in.
var improveEnvFile = "environment: development\nagents: { allowUnsandboxed: true }\n" +
	"improve: { enabled: true, harness: api }\n" +
	fmt.Sprintf("prices: { %s: { inPerMtok: %d, outPerMtok: %d }, %s: { inPerMtok: 2, outPerMtok: 10 } }\n",
		m2Model, m2InPerMtok, m2OutPerMtok, improve.APIModel)

// buildImprove derives the review fixture from the built M2 fixture in dir, and writes improveEnv; buildM2 calls it.
func buildImprove(dir string) error {
	fixtureM2Review, improveEnv = filepath.Join(dir, "m2-review"), filepath.Join(dir, "improve.yaml")
	if err := os.CopyFS(fixtureM2Review, os.DirFS(fixtureM2)); err != nil {
		return err
	}
	return errors.Join(
		rewrite(filepath.Join(fixtureM2Review, "pipeline.yaml"),
			[2]string{"pipeline: m2-fixture\n", "pipeline: " + improvePipeline + "\n"},
			[2]string{"  keep-good:\n    action: filter\n    in: listings.scored\n", reviewAfterScore}),
		os.WriteFile(improveEnv, []byte(improveEnvFile), 0o600))
}

// reviewAfterScore is the review step the improve fixture inserts between score and keep-good.
const reviewAfterScore = `  review:
    action: review
    in: listings.scored
    out: listings.reviewed
    config: { inbox: analysts, editable: [/score, /reason] }

  keep-good:
    action: filter
    in: listings.reviewed
`

// TestImprove is Task 6's scenario (spec section 11.3): the M2 fixture with analysts lowering the scores of poor
// listings yields a reviewPattern insight whose hint is a lessons proposal on score; millstream improve --insight
// gathers its evidence, drafts the lessons file through the recorded draft agent, and the engine validates the
// proposal through a real backtest of score, replayed from the M2 fixture's recordings: it is ready.
func TestImprove(t *testing.T) {
	t.Parallel()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	db := dbPath(t)
	recordings := t.TempDir()
	dirs := []string{m2Recordings, improveRecordings(root)}
	if *update { // the first improve run misses the draft's recording, which is then written for the brief it found
		dirs = dirs[:1]
	}
	for _, dir := range dirs {
		files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
		for _, f := range files {
			data, err := os.ReadFile(f) // #nosec G304 -- fixture recordings
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(recordings, filepath.Base(f)), data, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	env := append(m2Env(recordings), "MILLSTREAM_SECRET_ANTHROPIC_KEY=sk-fake-0123456789")
	runDay := func(day int) result {
		return millstream(t, db, env, "--env-file", improveEnv, "run", "--once", "--request-id",
			"day-"+strconv.Itoa(day), "--lease-ttl", "1s", "--var", "day="+strconv.Itoa(day), fixtureM2Review)
	}
	// Every listing once, four a day, each parked at the review.
	for _, day := range []int{1, 5, 9} {
		if r := runDay(day); r.code != 1 {
			t.Fatalf("day %d: %s", day, r)
		}
	}
	byID := map[string]listing.Listing{}
	for _, l := range pool.All() {
		byID[l.ID] = l
	}
	edits := 0
	for _, r := range inbox(t, db, "list", "--json") {
		l := byID[r.Item.Key]
		args := []string{"inbox", "approve", r.DeliveryID}
		if l.Condition == listing.ConditionPoor {
			args = append(args, "--set", fmt.Sprintf("/score=%d", pool.Score(l, lessonsV2Penalty)))
			edits++
		}
		mustExit(t, 0, db, nil, append(args, reviewer...)...)
	}
	if edits < 3 {
		t.Fatalf("%d poor listings edited, want the 3 a recurring edit takes", edits)
	}
	// A day of listings already seen: its run settles at once, the decided runs finish, and the insights are
	// projected with the decisions.
	runDay(13)
	in := reviewInsight(t, db)

	r := millstream(t, db, env, "--env-file", improveEnv, "improve", "--insight", in.ID)
	if *update {
		writeDraftRecording(t, root, db, recordings, in)
		r = millstream(t, db, env, "--env-file", improveEnv, "improve", "--insight", in.ID)
	}
	var ps []api.Proposal
	out := mustExit(t, 0, db, nil, "proposals", "list", "--pipeline", improvePipeline, "--json")
	for _, line := range strings.Split(strings.TrimSpace(out.stdout), "\n") {
		var p api.Proposal
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("improve: %s\nproposals: %v: %s", r, err, line)
		}
		ps = append(ps, p)
	}
	if r.code != 0 || !strings.Contains(r.stdout, "proposed lessons on "+improvePipeline) ||
		!strings.Contains(r.stdout, " ready") || len(ps) != 1 {
		t.Fatalf("improve: %s\nproposals: %+v", r, ps)
	}
	p := ps[0]
	var rep struct {
		Passed   bool `json:"passed"`
		Backtest struct {
			Passed bool `json:"passed"`
			Report struct {
				ChangedSteps []string `json:"changedSteps"`
				Sampled      int      `json:"sampled"`
				After        struct {
					ReviewAgreement float64 `json:"reviewAgreement"`
				} `json:"after"`
				Before struct {
					ReviewAgreement float64 `json:"reviewAgreement"`
				} `json:"before"`
			} `json:"report"`
		} `json:"backtest"`
	}
	if err := json.Unmarshal(p.Report, &rep); err != nil {
		t.Fatal(err)
	}
	bt := rep.Backtest.Report
	if p.Kind != "lessons" || p.State != string(store.ProposalReady) || p.CreatedBy != improve.Name ||
		len(p.Insights) != 1 || p.Insights[0] != in.ID || p.Paths[0] != "lessons/score.yaml" || !rep.Passed ||
		!rep.Backtest.Passed || bt.Sampled == 0 || len(bt.ChangedSteps) != 1 || bt.ChangedSteps[0] != "score" ||
		bt.After.ReviewAgreement <= bt.Before.ReviewAgreement {
		t.Fatalf("proposal %+v\nreport %s", p, p.Report)
	}
	if !strings.Contains(p.Patch, "lower its score by 25 points") {
		t.Errorf("patch:\n%s", p.Patch)
	}
	// The same idea again is not drafted twice: its proposal is open.
	r = mustExit(t, 0, db, env, "--env-file", improveEnv, "improve", "--insight", in.ID)
	if strings.Contains(r.stdout, "proposed") {
		t.Errorf("a second draft of an open idea: %s", r.stdout)
	}
	t.Logf("review agreement %.2f -> %.2f over %d sampled deliveries", bt.Before.ReviewAgreement,
		bt.After.ReviewAgreement, bt.Sampled)
}

// reviewInsight returns the review fixture's reviewPattern insight, which must hint a lessons proposal on score.
func reviewInsight(t *testing.T, db string) api.Insight {
	t.Helper()
	r := mustExit(t, 0, db, nil, "insights", "list", "--pipeline", improvePipeline, "--json")
	for _, line := range strings.Split(strings.TrimSpace(r.stdout), "\n") {
		var in api.Insight
		if json.Unmarshal([]byte(line), &in) == nil && in.Kind == pipeline.InsightReviewPattern {
			if !strings.Contains(string(in.Evidence), `"kind":"lessons"`) {
				t.Fatalf("the reviewPattern hints no lessons proposal: %s", in.Evidence)
			}
			return in
		}
	}
	t.Fatalf("no reviewPattern insight:\n%s", r.stdout)
	return api.Insight{}
}

// writeDraftRecording records the fake drafter's answer to the draft the last improve run could not replay: the
// lessons file of the M2 fixture's second version, with its rule lowering poor listings by 25 points.
func writeDraftRecording(t *testing.T, root, db, recordings string, in api.Insight) {
	t.Helper()
	st := openStore(t, db)
	defer func() { _ = st.Close() }()
	items, err := st.Items(t.Context(), store.ItemFilter{Pipeline: improve.Name, Stream: "improve.evidence"})
	if err != nil || len(items) == 0 {
		t.Fatalf("no evidence gathered: %v", err)
	}
	payload := items[len(items)-1].Payload
	envFile, err := pipeline.ParseEnvironment([]byte(improveEnvFile))
	if err != nil {
		t.Fatal(err)
	}
	files, err := improve.Files(envFile)
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := pipeline.FromFiles("", files)
	if err != nil {
		t.Fatal(err)
	}
	act, err := pipeline.Resolver{}.Resolve(pkg, "./actions/draft")
	if err != nil {
		t.Fatal(err)
	}
	tmpl, err := agent.ParsePrompt(act.Agent.Prompt, string(pkg.Files[act.Agent.Prompt]))
	if err != nil {
		t.Fatal(err)
	}
	var data, vars map[string]any
	raw, _ := json.Marshal(pkg.Pipeline.Vars) // as the run resolved them: the defaults, decoded from JSON
	if err := json.Unmarshal(raw, &vars); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		t.Fatal(err)
	}
	prompt, err := agent.Render(tmpl, agent.PromptData{Payload: data, Vars: vars})
	if err != nil {
		t.Fatal(err)
	}
	lessons, err := os.ReadFile(filepath.Join(root, "examples", "m2-fixture", "testdata", "score-lessons-v2.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	answer, _ := json.Marshal(improve.Draft{Kind: "lessons", Insights: []string{in.ID},
		Rationale: "Analysts lowered the score of every poor listing they reviewed by about 15 points: the score " +
			"step underestimates hidden renovation costs. The rule now lowers a poor listing's score by 25 points.",
		Files: []improve.DraftFile{{Path: "lessons/score.yaml", Content: string(lessons)}}})
	req := agent.Request{Model: act.Agent.Model, Prompt: prompt, Schema: act.Output}
	text := "```json\n" + string(answer) + "\n```"
	drafter := fakeDrafter{Text: text, TokensIn: int64(len(prompt) / 4), TokensOut: int64(len(text) / 4), Turns: 1}
	dir := improveRecordings(root)
	old, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, f := range old {
		_ = os.Remove(f)
	}
	for _, d := range []string{dir, recordings} {
		rec := agent.Recorder{Harness: drafter, Name: pipeline.HarnessAPI, Mode: agent.ModeRecord, Dir: d}
		if _, err := rec.Run(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	key := agent.Key(pipeline.HarnessAPI, req)
	t.Logf("recorded the draft %s", key)
}

// fakeDrafter answers every draft with its response.
type fakeDrafter agent.Response

func (f fakeDrafter) Run(context.Context, agent.Request) (agent.Response, error) {
	return agent.Response(f), nil
}
