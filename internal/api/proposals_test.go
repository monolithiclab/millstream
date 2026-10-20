package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/publish"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// proposalSrc is a pipeline of builtins, which check passes without any action: line 4 holds the filter.
const proposalSrc = "pipeline: extra\n%striggers: { adhoc: { manual: {} } }\nsteps:\n" +
	"  pass: { action: filter, in: trigger.adhoc, config: { expr: \"%s\" } }\n"

// filterPatch changes the filter's expression from one to another, at line 4.
func filterPatch(from, to string) string {
	return fmt.Sprintf("--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -4 +4 @@\n"+
		"-  pass: { action: filter, in: trigger.adhoc, config: { expr: %q } }\n"+
		"+  pass: { action: filter, in: trigger.adhoc, config: { expr: %q } }\n", from, to)
}

func proposalBody(kind, patch string) string {
	b, _ := json.Marshal(ProposalRequest{Pipeline: "extra", Kind: kind, Patch: patch, Rationale: "why not"})
	return string(b)
}

// Proposals through the API on a store of their own: the proposals-only token creates and revalidates but never
// accepts, or reaches anything else; a patch outside its kind's paths is invalid; an acceptance needs the patch's
// hash and makes the overlapping proposal stale, not revalidated on the same base (engine's TestProposals covers the
// rebase); a rejection needs a reason, listed with the rejected proposals.
func TestProposalActions(t *testing.T) {
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	// The real backtest runner: the pipeline has no history, so the backtests sample nothing and pass.
	eng, err := engine.New(engine.Options{Store: st, PackageDir: testdir.New(t),
		Proposals: engine.ProposalOptions{Backtest: backtest.ForProposals(backtest.New(backtest.Options{Store: st}), 0)}})
	if err != nil {
		t.Fatal(err)
	}
	h, err := New(Options{Store: st, Engine: eng, User: "tester"})
	if err != nil {
		t.Fatal(err)
	}
	call := func(method, target, body, token string, want int) []byte {
		t.Helper()
		req := request{method: method, target: "/api/v1" + target, body: body}
		if token != "" {
			req.header = map[string]string{"Authorization": "Bearer " + token}
		}
		rec := do(t, h, req)
		if rec.Code != want {
			t.Fatalf("%s %s: %d %s, want %d", method, target, rec.Code, rec.Body.String(), want)
		}
		return rec.Body.Bytes()
	}
	proposal := func(data []byte) Proposal {
		t.Helper()
		var p Proposal
		if err := json.Unmarshal(data, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	call(http.MethodPost, "/revisions", applyBody("", fmt.Sprintf(proposalSrc, "", "true")), "", http.StatusOK)

	var tok ProposalToken
	if err := json.Unmarshal(call(http.MethodPost, "/proposal-token", `{}`, "", http.StatusCreated), &tok); err != nil ||
		!strings.HasPrefix(tok.Token, "mspr_") {
		t.Fatalf("token %q, %v", tok.Token, err)
	}
	p1 := proposal(call(http.MethodPost, "/proposals", proposalBody("config", filterPatch("true", "item.n > 1")),
		tok.Token, http.StatusCreated))
	if p1.State != "ready" || p1.CreatedBy != "improve" || p1.Steps[0] != "pass" || p1.Paths[0] != "pipeline.yaml" ||
		!strings.Contains(string(p1.Report), `"passed":true`) {
		t.Fatalf("created %+v", p1)
	}
	call(http.MethodPost, "/proposals/"+p1.ID+"/accept", `{"patchSha256":"`+p1.PatchSHA256+`"}`, tok.Token,
		http.StatusForbidden)
	call(http.MethodGet, "/inbox", "", tok.Token, http.StatusForbidden)
	call(http.MethodPost, "/pipelines/extra/pause", `{}`, tok.Token, http.StatusForbidden)
	// The reads the improve pipeline gathers evidence with pass: a revision's files among them.
	var files RevisionFiles
	if err := json.Unmarshal(call(http.MethodGet, "/pipelines/extra/revisions/1/files", "", tok.Token, http.StatusOK),
		&files); err != nil || !strings.Contains(files.Files["pipeline.yaml"], "pipeline: extra") || files.PackageHash == "" {
		t.Fatalf("files %+v, %v", files, err)
	}
	call(http.MethodGet, "/pipelines/extra/revisions/9/files", "", tok.Token, http.StatusNotFound)
	call(http.MethodPost, "/proposal-token", `{}`, tok.Token, http.StatusForbidden)
	p2 := proposal(call(http.MethodPost, "/proposals", proposalBody("topology", filterPatch("true", "item.n > 2")),
		tok.Token, http.StatusCreated))
	call(http.MethodGet, "/proposals/"+p2.ID, "", tok.Token, http.StatusOK)
	// The backtest routes pass the token's gate: this server runs no backtests.
	call(http.MethodGet, "/backtests/bt_nope", "", tok.Token, http.StatusServiceUnavailable)

	for _, bad := range []struct{ kind, patch string }{
		{"topology", "--- /dev/null\n+++ b/Makefile\n@@ -0,0 +1 @@\n+all:\n"},
		{"lessons", "diff --git a/lessons/pass.yaml b/lessons/pass.yaml\nnew file mode 120000\n"},
		{"config", "--- a/../pipeline.yaml\n+++ b/../pipeline.yaml\n@@ -1 +1 @@\n-a\n+b\n"},
		{"nonsense", filterPatch("true", "false")},
	} {
		call(http.MethodPost, "/proposals", proposalBody(bad.kind, bad.patch), tok.Token, http.StatusBadRequest)
	}
	call(http.MethodPost, "/proposals", `{"pipeline":"nope","kind":"config","patch":"x"}`, "", http.StatusNotFound)
	call(http.MethodPost, "/proposals", `{}`, "", http.StatusBadRequest)

	call(http.MethodPost, "/proposals/"+p1.ID+"/accept", `{}`, "", http.StatusBadRequest)
	call(http.MethodPost, "/proposals/"+p1.ID+"/accept", `{"patchSha256":"other"}`, "", http.StatusConflict)
	accepted := proposal(call(http.MethodPost, "/proposals/"+p1.ID+"/accept", `{"patchSha256":"`+p1.PatchSHA256+`"}`,
		"", http.StatusOK))
	if accepted.State != "accepted" || accepted.DecidedBy != "tester" || accepted.DecidedAt == nil {
		t.Fatalf("accepted %+v", accepted)
	}
	if p := proposal(call(http.MethodGet, "/proposals/"+p2.ID, "", "", http.StatusOK)); p.State != "stale" {
		t.Fatalf("the overlapping proposal is %s", p.State)
	}
	call(http.MethodPost, "/proposals/"+p2.ID+"/revalidate", `{}`, tok.Token, http.StatusConflict) // base unchanged
	broken := proposal(call(http.MethodPost, "/proposals", proposalBody("config", filterPatch("true", "item.n >")),
		tok.Token, http.StatusCreated))
	if broken.State != "drafting" || !strings.Contains(string(broken.Report), `"passed":false`) {
		t.Fatalf("a proposal whose check fails: %+v", broken)
	}
	if p := proposal(call(http.MethodPost, "/proposals/"+broken.ID+"/revalidate", `{}`, tok.Token,
		http.StatusOK)); p.State != "drafting" {
		t.Fatalf("revalidated: %+v", p)
	}

	call(http.MethodPost, "/proposals/"+p2.ID+"/reject", `{}`, "", http.StatusBadRequest)
	call(http.MethodPost, "/proposals/"+p2.ID+"/reject", `{"reason":"the filter is right"}`, "", http.StatusOK)
	call(http.MethodPost, "/proposals/"+p2.ID+"/reject", `{"reason":"again"}`, "", http.StatusConflict)
	call(http.MethodPost, "/proposals/nope/reject", `{"reason":"x"}`, "", http.StatusNotFound)
	var rejected []Proposal
	if err := json.Unmarshal(call(http.MethodGet, "/proposals?pipeline=extra&state=rejected", "", tok.Token,
		http.StatusOK), &rejected); err != nil || len(rejected) != 1 || rejected[0].Reason != "the filter is right" {
		t.Fatalf("rejected %+v, %v", rejected, err)
	}
	call(http.MethodGet, "/proposals?state=nope", "", "", http.StatusBadRequest)
	call(http.MethodGet, "/proposals/nope", "", "", http.StatusNotFound)
	// Publishing, with the token: once, through the publish action (apply here), answering where it went again.
	pubBody := `{"patchSha256":"` + p1.PatchSHA256 + `"}`
	call(http.MethodPost, "/proposals/"+p1.ID+"/publish", pubBody, tok.Token, http.StatusServiceUnavailable)
	if h.opts.Publisher, err = publish.New(pipeline.Improve{Publish: pipeline.PublishApply},
		publish.Deps{Packages: eng.StoredPackage, Applier: eng}); err != nil {
		t.Fatal(err)
	}
	call(http.MethodPost, "/proposals/"+p1.ID+"/publish", `{"patchSha256":"other"}`, tok.Token, http.StatusBadRequest)
	call(http.MethodPost, "/proposals/"+p1.ID+"/publish", `{}`, tok.Token, http.StatusBadRequest)
	for _, again := range []bool{false, true} {
		var got PublishedProposal
		if err := json.Unmarshal(call(http.MethodPost, "/proposals/"+p1.ID+"/publish", pubBody, tok.Token,
			http.StatusOK), &got); err != nil || got.Location != "2" || got.Publisher != "apply" || got.Again != again {
			t.Fatalf("published %+v, %v; want again %v", got, err, again)
		}
	}
	call(http.MethodPost, "/proposals/"+p2.ID+"/publish", pubBody, tok.Token, http.StatusConflict)
	// The proposal says where it went, read alone or listed; an unpublished one says nothing.
	var one Proposal
	if err := json.Unmarshal(call(http.MethodGet, "/proposals/"+p1.ID, "", tok.Token, http.StatusOK), &one); err != nil ||
		one.Published == nil || one.Published.Publisher != "apply" || one.Published.Location != "2" ||
		one.Published.At.IsZero() {
		t.Fatalf("published proposal %+v, %v", one.Published, err)
	}
	var all []Proposal
	if err := json.Unmarshal(call(http.MethodGet, "/proposals?pipeline=extra", "", tok.Token, http.StatusOK),
		&all); err != nil {
		t.Fatal(err)
	}
	for _, p := range all {
		if (p.ID == p1.ID) != (p.Published != nil) {
			t.Fatalf("listed %s published %+v", p.ID, p.Published)
		}
	}
}
