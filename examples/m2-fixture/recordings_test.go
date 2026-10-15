package m2fixture

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
	"github.com/monolithiclab/millstream/examples/m2-fixture/pool"
	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
)

// update rewrites recordings/: go test ./examples/m2-fixture -run TestRecordings -update (make recordings).
var update = flag.Bool("update", false, "rewrite recordings/ from the fake model server")

const recordingsDir = "recordings"

// lessonsVersions are the lessons files the score step is recorded with: the committed one, and the edit the
// end-to-end suite applies.
var lessonsVersions = []string{"lessons/score.yaml", "testdata/score-lessons-v2.yaml"}

// listingInPrompt finds the listing a prompt is about.
var listingInPrompt = regexp.MustCompile(`Listing (L\d{3}):`)

// scored is the fake model's answer: the listing as given, its score and the reason.
type scored struct {
	listing.Scored
	Reason string `json:"reason"`
}

// fakeModel is an OpenAI-compatible chat completions endpoint that scores the listing a prompt names with
// pool.Score, lowered by the penalty its lessons ask for, and answers with a fenced JSON block. Token counts are
// the characters divided by four, so the recordings and their costs are reproducible.
func fakeModel(t *testing.T) http.Handler {
	t.Helper()
	all := map[string]listing.Listing{}
	for _, l := range pool.All() {
		all[l.ID] = l
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model    string `json:"model"`
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/chat/completions" || json.Unmarshal(body, &req) != nil || len(req.Messages) != 1 {
			http.Error(w, "not a chat completion: "+r.URL.Path, http.StatusBadRequest)
			return
		}
		prompt := req.Messages[0].Content
		m := listingInPrompt.FindStringSubmatch(prompt)
		l, ok := all[m[min(1, len(m)-1)]]
		if m == nil || !ok {
			http.Error(w, "no pool listing in the prompt", http.StatusBadRequest)
			return
		}
		penalty := pool.Penalty(prompt)
		reason := fmt.Sprintf("%s condition at %d EUR for %d m² in %s.", l.Condition, l.PriceEur, l.SurfaceM2, l.City)
		if penalty > 0 && l.Condition == listing.ConditionPoor {
			reason += fmt.Sprintf(" Lowered by %d points for hidden renovation costs.", penalty)
		}
		answer, _ := json.Marshal(scored{Scored: listing.Scored{Listing: l, Score: pool.Score(l, penalty)},
			Reason: reason})
		text := "```json\n" + string(answer) + "\n```"
		resp, _ := json.Marshal(map[string]any{
			"id": "chatcmpl-fake", "object": "chat.completion", "model": req.Model,
			"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": text},
				"finish_reason": "stop"}},
			"usage": map[string]int{"prompt_tokens": len(prompt) / 4, "completion_tokens": len(text) / 4},
		})
		_, _ = w.Write(resp)
	})
}

// handlerTransport serves requests with a handler, in process.
type handlerTransport struct{ h http.Handler }

func (ht handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	ht.h.ServeHTTP(rec, req)
	resp := rec.Result()
	resp.Request = req
	return resp, nil
}

// record runs the score action over listings with the lessons file, through the agent adapter and the
// real api harness pointed at the fake model, recording into dir.
func record(t *testing.T, model http.Handler, lessonsFile, dir string, listings []listing.Listing) {
	t.Helper()
	data, err := os.ReadFile(lessonsFile) // #nosec G304 -- the package's own files
	if err != nil {
		t.Fatal(err)
	}
	l, err := pipeline.ParseLessons(data)
	if err != nil {
		t.Fatal(err)
	}
	lessons, _ := json.Marshal(l) // as the engine sends them in hello
	env := map[string]string{"MILLSTREAM_WORKDIR": t.TempDir(), "MILLSTREAM_SECRET_SCORER_KEY": "sk-fake-0123456789",
		agent.EnvHarnessMode: agent.ModeRecord, agent.EnvHarnessDir: dir}
	var in bytes.Buffer
	w := protocol.NewWriter(&in)
	msgs := []protocol.Message{protocol.Hello{Protocol: 1, Action: "m2-fixture/score", Config: map[string]any{},
		Vars: map[string]any{}, Lessons: lessons}}
	for i, li := range listings {
		payload, _ := json.Marshal(li)
		msgs = append(msgs, protocol.Item{Delivery: fmt.Sprintf("dl_%d", i), ID: "it_" + li.ID, Key: li.ID,
			Payload: payload, Attempt: 1})
	}
	for _, m := range append(msgs, protocol.Shutdown{}) {
		if err := w.Write(m); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	err = agent.Run(t.Context(), &in, &out, agent.Options{
		Package: ".", Loaded: loadFixture(t), Action: "./actions/score", Secrets: []string{"scorerKey"},
		Env: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		NewHarness: func(rt *pipeline.AgentRuntime, apiKey string) agent.Harness {
			return agent.API{Provider: rt.Provider, BaseURL: "http://model.test", APIKey: apiKey,
				Client: &http.Client{Transport: handlerTransport{model}}}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := 0
	for r := protocol.NewReader(&out); ; {
		m, err := r.Read()
		if err != nil {
			break
		}
		switch m := m.(type) {
		case protocol.Done:
			done++
		case protocol.Error:
			t.Fatalf("%s with %s: %s", m.Delivery, lessonsFile, m.Message)
		}
	}
	if done != len(listings) {
		t.Fatalf("%s: %d deliveries done, want %d", lessonsFile, done, len(listings))
	}
}

// TestRecordings records the score action with each lessons version against the fake model, and checks the
// recordings are reproducible: recordings/ holds the same file for each. In the e2e build (and with -update, which
// rewrites recordings/) it records the whole pool and checks recordings/ holds exactly those recordings; the default
// build records a sample, a listing in good condition and one in poor condition, which the lessons versions score
// differently. Nothing reaches the network.
func TestRecordings(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if *update {
		dir = recordingsDir
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	listings := pool.All()
	full := fullRecordings || *update
	if !full {
		listings = recordingsSample(t, listings)
	}
	model := fakeModel(t)
	t.Run("record", func(t *testing.T) {
		for _, f := range lessonsVersions {
			t.Run(filepath.Base(f), func(t *testing.T) {
				t.Parallel() // the versions record distinct files
				record(t, model, f, dir, listings)
			})
		}
	})
	if *update || t.Failed() {
		return
	}
	got, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	want, _ := filepath.Glob(filepath.Join(recordingsDir, "*.json"))
	if len(want) != pool.Size*len(lessonsVersions) || len(got) != len(listings)*len(lessonsVersions) {
		t.Fatalf("recorded %d files, %s has %d: run make recordings", len(got), recordingsDir, len(want))
	}
	for _, gf := range got {
		wf := filepath.Join(recordingsDir, filepath.Base(gf))
		gb, _ := os.ReadFile(gf)   // #nosec G304 -- test files
		wb, err := os.ReadFile(wf) // #nosec G304 -- test files
		if err != nil || !bytes.Equal(gb, wb) {
			t.Fatalf("%s differs from a fresh recording (%v): run make recordings", wf, err)
		}
	}
}

// recordingsSample is the first listing in good condition and the first in poor condition.
func recordingsSample(t *testing.T, all []listing.Listing) []listing.Listing {
	t.Helper()
	var good, poor *listing.Listing
	for i := range all {
		switch {
		case all[i].Condition == listing.ConditionPoor && poor == nil:
			poor = &all[i]
		case all[i].Condition != listing.ConditionPoor && good == nil:
			good = &all[i]
		}
	}
	if good == nil || poor == nil {
		t.Fatal("the pool lacks a listing in good or in poor condition")
	}
	return []listing.Listing{*good, *poor}
}

// fixture is this package, loaded once: every recording run resolves its score action in it.
var fixture = sync.OnceValues(func() (*pipeline.Package, error) { return pipeline.Load(".") })

func loadFixture(t *testing.T) *pipeline.Package {
	t.Helper()
	pkg, err := fixture()
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}
