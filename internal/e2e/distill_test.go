//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	osexec "os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/examples/m5-distill/tiers"
	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/agent/agenttest"
	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/improve"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/sandbox"
	"github.com/monolithiclab/millstream/internal/store"
)

func init() { extraBuilds = append(extraBuilds, buildDistill) }

// fixtureDistill is examples/m5-distill with its list action compiled; distillEnv runs it in development with the
// improve pipeline, as improveEnv does, the fake tier model priced.
var fixtureDistill, distillEnv string

// The distill fixture's pipeline, its days and model.
const (
	distillPipeline = "m5-distill"
	distillDays     = 2
	distillPerDay   = 270
	tierModel       = "fake-tierer-1"
)

var distillEnvFile = "environment: development\nagents: { allowUnsandboxed: true }\n" +
	"improve: { enabled: true, harness: api }\n" +
	fmt.Sprintf("prices: { %s: { inPerMtok: 3, outPerMtok: 15 }, %s: { inPerMtok: 2, outPerMtok: 10 } }\n",
		tierModel, improve.APIModel)

// buildDistill copies the distill fixture into dir, its list action compiled, and writes distillEnv.
func buildDistill(root, dir string) error {
	fixtureDistill, distillEnv = filepath.Join(dir, "m5-distill"), filepath.Join(dir, "distill.yaml")
	if err := os.CopyFS(fixtureDistill, os.DirFS(filepath.Join(root, "examples", "m5-distill"))); err != nil {
		return err
	}
	platform := runtime.GOOS + "-" + runtime.GOARCH
	return errors.Join(os.RemoveAll(filepath.Join(fixtureDistill, "testdata")),
		os.RemoveAll(filepath.Join(fixtureDistill, "tiers")),
		goBuild(root, filepath.Join(fixtureDistill, "actions", "list", "bin", platform, "list"),
			"./examples/m5-distill/actions/list"),
		os.WriteFile(distillEnv, []byte(distillEnvFile), 0o600))
}

// distillRecordings are the committed recordings of the draft agent's distill answer.
func distillRecordings(root string) string { return filepath.Join(improveRecordings(root), "distill") }

// TestDistill is Task 7's scenario (spec section 11.5): two days of listings through the fixture's tier agent, whose
// outputs follow tiers.Tier, make tier distillable; millstream improve --insight gathers the training side of the
// engine's split (the first day), the recorded draft agent answers Go code with the SDK that abstains on a listing
// without a surface, and the engine builds and runs it in the container sandbox on the holdout (the second day, which
// the agent never saw): agreement at least 0.98 on at least 200 covered items, and the proposal is ready, its report
// naming the agent cost the code saves.
func TestDistill(t *testing.T) {
	t.Parallel()
	dockerSandbox(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	db := dbPath(t)
	recordings := t.TempDir()
	recordTiers(t, recordings, 1, 2)
	if !*update { // the first improve run misses the draft's recording, which is then written for the brief it found
		copyRecordings(t, distillRecordings(root), recordings)
	}
	env := []string{agent.EnvHarnessMode + "=" + agent.ModeReplay, agent.EnvHarnessDir + "=" + recordings,
		"MILLSTREAM_SECRET_TIER_KEY=sk-fake-0123456789", "MILLSTREAM_SECRET_ANTHROPIC_KEY=sk-fake-0123456789"}
	in := distillHistory(t, db, env, distillEnv)
	r := millstream(t, db, env, "--env-file", distillEnv, "improve", "--insight", in.ID)
	if *update {
		writeDistillRecording(t, root, db, recordings, in)
		r = millstream(t, db, env, "--env-file", distillEnv, "improve", "--insight", in.ID)
	}
	out := mustExit(t, 0, db, nil, "proposals", "list", "--pipeline", distillPipeline, "--json")
	var p api.Proposal
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.stdout)), &p); err != nil {
		t.Fatalf("improve: %s\nproposals: %v: %s", r, err, out.stdout)
	}
	var rep distillReport
	if err := json.Unmarshal(p.Report, &rep); err != nil {
		t.Fatal(err)
	}
	d := rep.Distill
	if r.code != 0 || p.Kind != "distill" || p.State != string(store.ProposalReady) || !rep.Passed || !d.Passed ||
		d.Step != "tier" || d.Action != "./actions/tier-rules" || d.Fallback != "./actions/tier" ||
		d.Holdout < 250 || d.Covered < 200 || d.Agreement < 0.98 || d.Coverage < 0.6 || d.AgentCostSavedUSD <= 0 ||
		rep.Backtest.Skipped == "" || len(rep.NewEvals) != 1 || !rep.NewEvals[0].Passed {
		t.Fatalf("improve: %s\nproposal %+v\nreport %s", r, p, p.Report)
	}
	if !strings.Contains(p.Patch, "+    fallback: { action: ./actions/tier, when: [abstain], auditRate: 0.02 }") ||
		!strings.Contains(p.Patch, "+++ b/actions/tier-rules/main.go") {
		t.Errorf("patch:\n%s", p.Patch)
	}
	t.Logf("holdout %d, covered %d (%.2f), agreement %.3f, agent cost saved %.4f USD", d.Holdout, d.Covered,
		d.Coverage, d.Agreement, d.AgentCostSavedUSD)
}

// distillReport is a distill proposal's validation report, as far as the tests read it.
type distillReport struct {
	Passed   bool `json:"passed"`
	Backtest struct {
		Skipped string `json:"skipped"`
	} `json:"backtest"`
	NewEvals []struct {
		Passed bool `json:"passed"`
	} `json:"newEvals"`
	Distill struct {
		Step              string   `json:"step"`
		Action            string   `json:"action"`
		Fallback          string   `json:"fallback"`
		Holdout           int      `json:"holdout"`
		Covered           int      `json:"covered"`
		Agreed            int      `json:"agreed"`
		Coverage          float64  `json:"coverage"`
		Agreement         float64  `json:"agreement"`
		AgentCostUSD      float64  `json:"agentCostUsd"`
		AgentCostSavedUSD float64  `json:"agentCostSavedUsd"`
		Passed            bool     `json:"passed"`
		Error             string   `json:"error"`
		Disagreements     []string `json:"disagreements"`
	} `json:"distill"`
}

// distillHistory runs the distill fixture's days on db under envFile and returns the distillable insight on tier
// they open.
func distillHistory(t *testing.T, db string, env []string, envFile string) api.Insight {
	t.Helper()
	for day := 1; day <= distillDays; day++ {
		if r := runDay(t, db, env, envFile, fixtureDistill, day); r.code != 0 {
			t.Fatalf("day %d: %s", day, r)
		}
	}
	return distillableInsight(t, db)
}

// runDay runs the package pkg on db under envFile once for one day, under the request id day-<day>.
func runDay(t *testing.T, db string, env []string, envFile, pkg string, day int) result {
	t.Helper()
	return millstream(t, db, env, "--env-file", envFile, "run", "--once", "--request-id", "day-"+strconv.Itoa(day),
		"--lease-ttl", "1s", "--var", "day="+strconv.Itoa(day), pkg)
}

// dockerSandbox skips the test without Docker or the candidate-code sandbox's image (pulled once when missing).
func dockerSandbox(t *testing.T) {
	t.Helper()
	rt, ok := container.Find(pipeline.RuntimeDocker, os.LookupEnv)
	if !ok {
		t.Skip("docker is not installed: the distill scenario is skipped")
	}
	if osexec.Command(rt.Bin, "image", "inspect", sandbox.DefaultImage).Run() != nil { // #nosec G204 -- our image
		if out, err := osexec.Command(rt.Bin, "pull", "--quiet", sandbox.DefaultImage).CombinedOutput(); err != nil { // #nosec G204
			t.Skipf("the sandbox image is not available: %s", out)
		}
	}
}

// distillableInsight returns the fixture's distillable insight, on tier.
func distillableInsight(t *testing.T, db string) api.Insight {
	t.Helper()
	r := mustExit(t, 0, db, nil, "insights", "list", "--pipeline", distillPipeline, "--json")
	for _, line := range strings.Split(strings.TrimSpace(r.stdout), "\n") {
		var in api.Insight
		if json.Unmarshal([]byte(line), &in) == nil && in.Kind == pipeline.InsightDistillable && in.Scope == "tier" {
			return in
		}
	}
	t.Fatalf("no distillable insight on tier:\n%s", r.stdout)
	return api.Insight{}
}

// copyRecordings copies the recordings of from into to.
func copyRecordings(t *testing.T, from, to string) {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(from, "*.json"))
	for _, f := range files {
		data, err := os.ReadFile(f) // #nosec G304 -- fixture recordings
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(to, filepath.Base(f)), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// writeDistillRecording records the fake drafter's distill answer, the code in examples/m5-distill/testdata/
// tier-rules, for the evidence the last improve run gathered.
func writeDistillRecording(t *testing.T, root, db, recordings string, in api.Insight) {
	t.Helper()
	payload := lastEvidence(t, db)
	var ev improve.Evidence
	if err := json.Unmarshal(payload, &ev); err != nil || ev.Distill == nil {
		t.Fatalf("not a distill's evidence (%v): %s", err, payload)
	}
	src := filepath.Join(root, "examples", "m5-distill", "testdata", "tier-rules")
	answer := improve.Draft{Kind: "distill", Insights: []string{in.ID},
		Rationale: "Every tier the agent gave follows the listing's price per square metre: low under 3,000 EUR, " +
			"mid under 6,000, high above. The code applies that rule and abstains on a listing without a surface."}
	err := filepath.WalkDir(src, func(p string, de os.DirEntry, err error) error {
		if err != nil || de.IsDir() {
			return err
		}
		data, err := os.ReadFile(p) // #nosec G304 -- fixture files
		rel, _ := filepath.Rel(src, p)
		answer.Files = append(answer.Files, improve.DraftFile{
			Path: "actions/" + ev.Distill.Action + "/" + filepath.ToSlash(rel), Content: string(data)})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	dir := distillRecordings(root)
	old, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, f := range old {
		_ = os.Remove(f)
	}
	recordDraft(t, payload, answer, dir, recordings)
}

// tierInPrompt finds the listing a tier prompt is about.
var tierInPrompt = regexp.MustCompile(`Listing (D\d+-\d+):`)

// recordTiers records the tier agent on every listing of the days given, through the agent adapter and the real
// api harness pointed at a fake model applying tiers.Tier, into dir. Token counts are the characters divided by
// four, so the costs are reproducible.
func recordTiers(t *testing.T, dir string, days ...int) {
	t.Helper()
	all := map[string]tiers.Listing{}
	var listings []tiers.Listing
	for _, day := range days {
		for _, l := range tiers.Day(day, distillPerDay) {
			all[l.ID] = l
			listings = append(listings, l)
		}
	}
	model := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model    string `json:"model"`
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		body, _ := io.ReadAll(r.Body)
		if json.Unmarshal(body, &req) != nil || len(req.Messages) != 1 {
			http.Error(w, "not a chat completion", http.StatusBadRequest)
			return
		}
		prompt := req.Messages[0].Content
		m := tierInPrompt.FindStringSubmatch(prompt)
		l, ok := all[m[min(1, len(m)-1)]]
		if m == nil || !ok {
			http.Error(w, "no listing in the prompt", http.StatusBadRequest)
			return
		}
		answer, _ := json.Marshal(tiers.Tiered{Listing: l, Tier: tiers.Tier(l)})
		text := "```json\n" + string(answer) + "\n```"
		resp, _ := json.Marshal(map[string]any{"id": "chatcmpl-fake", "object": "chat.completion", "model": req.Model,
			"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": text},
				"finish_reason": "stop"}},
			"usage": map[string]int{"prompt_tokens": len(prompt) / 4, "completion_tokens": len(text) / 4}})
		_, _ = w.Write(resp)
	})
	files := map[string][]byte{}
	for _, f := range []string{pipeline.FileName, "actions/tier/action.yaml", "actions/tier/prompt.md"} {
		data, err := os.ReadFile(filepath.Join(fixtureDistill, filepath.FromSlash(f))) // #nosec G304 -- fixture
		if err != nil {
			t.Fatal(err)
		}
		files[f] = data
	}
	pkg, err := pipeline.FromFiles(".", files)
	if err != nil {
		t.Fatal(err)
	}
	var in bytes.Buffer
	w := protocol.NewWriter(&in)
	msgs := []protocol.Message{protocol.Hello{Protocol: 1, Action: "m5-distill/tier", Config: map[string]any{},
		Vars: map[string]any{}}}
	for i, l := range listings {
		payload, _ := json.Marshal(l)
		msgs = append(msgs, protocol.Item{Delivery: fmt.Sprintf("dl_%d", i), ID: "it_" + l.ID, Key: l.ID,
			Payload: payload, Attempt: 1})
	}
	for _, m := range append(msgs, protocol.Shutdown{}) {
		if err := w.Write(m); err != nil {
			t.Fatal(err)
		}
	}
	env := map[string]string{"MILLSTREAM_WORKDIR": t.TempDir(), "MILLSTREAM_SECRET_TIER_KEY": "sk-fake-0123456789",
		agent.EnvHarnessMode: agent.ModeRecord, agent.EnvHarnessDir: dir}
	var outBuf bytes.Buffer
	err = agent.Run(t.Context(), &in, &outBuf, agent.Options{
		Package: ".", Loaded: pkg, Action: "./actions/tier", Secrets: []string{"tierKey"},
		Env: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		NewHarness: func(rt *pipeline.AgentRuntime, apiKey string) agent.Harness {
			return agent.API{Provider: rt.Provider, BaseURL: agenttest.URL, APIKey: apiKey, Client: agenttest.Client(model)}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := 0
	for rd := protocol.NewReader(&outBuf); ; {
		m, err := rd.Read()
		if err != nil {
			break
		}
		switch m := m.(type) {
		case protocol.Done:
			done++
		case protocol.Error:
			t.Fatalf("recording %s: %s", m.Delivery, m.Message)
		}
	}
	if done != len(listings) {
		t.Fatalf("recorded %d of %d listings", done, len(listings))
	}
}
