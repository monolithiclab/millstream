package agent

import (
	"bytes"
	"encoding/json"
	"flag"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// update rewrites testdata/recordings: go test ./internal/agent -run TestRecordings -update (make recordings).
var update = flag.Bool("update", false, "rewrite the recordings under testdata from the fake model server")

const recordingsDir = "testdata/recordings"

// recordedAnswers are the fake model server's answers, by listing URL.
var recordedAnswers = map[string]string{
	"https://example.com/listings/1": `{"score": 72, "reason": "Dated kitchen, sound structure."}`,
	"https://example.com/listings/2": `{"score": 15, "reason": "Recently renovated."}`,
}

// recordingSession runs the testdata package's agent action in a harness mode.
func recordingSession(t *testing.T, mode, dir string) *session {
	t.Helper()
	s := dirSession(t, "testdata/score", nil)
	s.opts.Secrets = []string{"anthropicKey"}
	s.env = map[string]string{"MILLSTREAM_SECRET_ANTHROPIC_KEY": "sk-ant-test-key",
		EnvHarnessMode: mode, EnvHarnessDir: dir}
	return s
}

// TestRecordings records the testdata action against a fake Messages API, through the real api harness, and
// checks the recordings under testdata match: they are reproducible, and -update rewrites them. Nothing reaches the
// network.
func TestRecordings(t *testing.T) {
	t.Parallel()
	dir := testdir.New(t)
	if *update {
		dir = recordingsDir
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	for _, url := range slices.Sorted(maps.Keys(recordedAnswers)) {
		body, _ := json.Marshal(map[string]any{
			"id": "msg_test", "type": "message", "role": "assistant",
			"content": []map[string]string{{"type": "text", "text": "```json\n" + recordedAnswers[url] + "\n```"}},
			"usage":   map[string]int{"input_tokens": 150, "output_tokens": 25},
		})
		srv := newFakeAPI(t, http.StatusOK, string(body))
		s := recordingSession(t, ModeRecord, dir)
		s.opts.NewHarness = func(rt *pipeline.AgentRuntime, apiKey string) Harness {
			return API{Provider: rt.Provider, BaseURL: srv.URL, Client: srv.Client(), APIKey: apiKey}
		}
		s.config = map[string]any{}
		msgs := s.run(`{"url": "` + url + `"}`)
		if _, ok := msgs[len(msgs)-1].(protocol.Done); !ok {
			t.Fatalf("record %s: %+v", url, msgs)
		}
	}
	if *update {
		return
	}
	got, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	want, _ := filepath.Glob(filepath.Join(recordingsDir, "*.json"))
	if len(got) != len(want) || len(want) != len(recordedAnswers) {
		t.Fatalf("recorded %d files, testdata has %d: run make recordings", len(got), len(want))
	}
	for _, w := range want {
		g := filepath.Join(dir, filepath.Base(w))
		gb, _ := os.ReadFile(g) // #nosec G304 -- test files
		wb, _ := os.ReadFile(w) // #nosec G304 -- test files
		if !bytes.Equal(gb, wb) {
			t.Fatalf("%s differs from a fresh recording: run make recordings", w)
		}
	}
}

// TestReplay runs the testdata action from its recordings through the default harness: no network, no API call,
// so no API key either. A payload never recorded misses, permanently.
func TestReplay(t *testing.T) {
	t.Parallel()
	s := recordingSession(t, ModeReplay, recordingsDir)
	delete(s.env, "MILLSTREAM_SECRET_ANTHROPIC_KEY")
	s.config = map[string]any{}
	msgs := s.run(`{"url": "https://example.com/listings/1"}`, `{"url": "https://example.com/listings/3"}`)
	var emit *protocol.Emit
	var last protocol.Message
	for _, m := range msgs {
		if e, ok := m.(protocol.Emit); ok {
			emit = &e
		}
		last = m
	}
	if emit == nil || string(emit.Payload) != `{"score":72,"reason":"Dated kitchen, sound structure."}` {
		t.Fatalf("messages %+v", msgs)
	}
	if e, ok := last.(protocol.Error); !ok || e.Retryable {
		t.Fatalf("an unrecorded payload: %+v", last)
	}
}
