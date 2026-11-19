package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/agent/agenttest"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/scaffold"
)

// update rewrites the listings example's recordings: go test ./examples/fakemodel -run TestRecordings -update (make
// recordings).
var update = flag.Bool("update", false, "rewrite the listings example's recordings from the fake model")

// listingsRecordings is where the listings example, embedded in the binary, keeps its recordings.
const listingsRecordings = "../../internal/scaffold/listings/recordings"

// TestHandler: a listing at 3,000 EUR per square metre scores 100, one at 4,000 scores 50, and a prompt naming no
// listing is refused.
func TestHandler(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		prompt string
		code   int
		answer string
	}{
		{"Listing A1 in Lyon: 50 m², condition poor, asking 150000 EUR.", http.StatusOK,
			`{"city":"Lyon","condition":"poor","id":"A1","priceEur":150000,"reason":"3000 EUR per square metre in Lyon.",` +
				`"score":100,"surfaceM2":50}`},
		{"Listing A2 in Lille: 40 m², condition good, asking 160000 EUR.", http.StatusOK, `"score":50`},
		{"Score this.", http.StatusBadRequest, "no listing in the prompt"},
	} {
		body, _ := json.Marshal(map[string]any{"model": "fake-scorer",
			"messages": []map[string]string{{"role": "user", "content": tt.prompt}}})
		rec := httptest.NewRecorder()
		handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body)))
		var resp struct {
			Choices []struct {
				Message struct{ Content string } `json:"message"`
			} `json:"choices"`
		}
		got := rec.Body.String()
		if rec.Code == http.StatusOK && json.Unmarshal(rec.Body.Bytes(), &resp) == nil && len(resp.Choices) == 1 {
			got = resp.Choices[0].Message.Content
		}
		if rec.Code != tt.code || !strings.Contains(got, tt.answer) {
			t.Errorf("%q: %d %s", tt.prompt, rec.Code, got)
		}
	}
}

// TestRecordings rewrites the listings example's recordings with -update (make recordings): its score agent, through
// the agent adapter and the api harness pointed at this fake model in process, over the example's listings. Without
// -update it skips: the example's end-to-end test (internal/cli, Test_initListings) replays the committed recordings,
// and a prompt they no longer match fails there.
func TestRecordings(t *testing.T) {
	if !*update {
		t.Skip("rewrites the listings example's recordings with -update")
	}
	files, err := scaffold.ListingsFiles()
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := pipeline.FromFiles(".", files)
	if err != nil {
		t.Fatal(err)
	}
	var listings []map[string]any
	if err := json.Unmarshal(files["actions/crawl/listings.json"], &listings); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(listingsRecordings); err != nil {
		t.Fatal(err)
	}
	var in bytes.Buffer
	w := protocol.NewWriter(&in)
	msgs := []protocol.Message{protocol.Hello{Protocol: 1, Action: "listings/score", Config: map[string]any{},
		Vars: map[string]any{}}}
	for i, l := range listings {
		payload, _ := json.Marshal(l)
		msgs = append(msgs, protocol.Item{Delivery: fmt.Sprintf("dl_%d", i), ID: fmt.Sprint("it_", l["id"]),
			Key: fmt.Sprint(l["id"]), Payload: payload, Attempt: 1})
	}
	for _, m := range append(msgs, protocol.Shutdown{}) {
		if err := w.Write(m); err != nil {
			t.Fatal(err)
		}
	}
	env := map[string]string{"MILLSTREAM_WORKDIR": t.TempDir(), "MILLSTREAM_SECRET_SCORER_KEY": "sk-fake-0123456789",
		agent.EnvHarnessMode: agent.ModeRecord, agent.EnvHarnessDir: listingsRecordings}
	var out bytes.Buffer
	err = agent.Run(t.Context(), &in, &out, agent.Options{
		Package: ".", Loaded: pkg, Action: "./actions/score", Secrets: []string{"scorerKey"},
		Env: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		NewHarness: func(rt *pipeline.AgentRuntime, apiKey string) agent.Harness {
			return agent.API{Provider: rt.Provider, BaseURL: agenttest.URL + "/v1", APIKey: apiKey,
				Client: agenttest.Client(handler())}
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
			t.Fatalf("%s: %s", m.Delivery, m.Message)
		}
	}
	if done != len(listings) {
		t.Fatalf("%d deliveries done, want %d", done, len(listings))
	}
}

// Test_fakemodelStdlibOnly: main.go imports the standard library only, so the getting-started guide can run it with
// go run main.go from a single downloaded file, outside any module.
func Test_fakemodelStdlibOnly(t *testing.T) {
	t.Parallel()
	f, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range f.Imports {
		if path, _ := strconv.Unquote(imp.Path.Value); strings.Contains(path, ".") {
			t.Errorf("main.go imports %s: the stand-in model must import the standard library only", path)
		}
	}
}
