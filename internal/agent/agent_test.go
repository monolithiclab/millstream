package agent

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"

	"github.com/monolithiclab/millstream/internal/testdir"
)

const (
	testPipeline = "pipeline: p\ntriggers: { go: { manual: {} } }\nsteps:\n  score: { action: ./actions/score, in: trigger.go }\n"
	testManifest = `name: lab/score
kind: agent
output: { type: object, properties: { score: { type: integer } }, required: [score] }
runtime: { agent: { harness: api, model: claude-sonnet-5-5, apiKeySecret: apiToken } }
`
	testPrompt = "Score {{ .Payload.url }} for {{ .Vars.city }} with at least {{ .Config.rooms }} rooms.\n"
)

func TestMain(m *testing.M) {
	// The tests allocate heavily: a higher collector target trades memory for test time, as in the store tests.
	debug.SetGCPercent(400)
	code := m.Run()
	testdir.Remove()
	os.Exit(code)
}

// memPackage builds, in memory, a package with the agent action ./actions/score of manifest and prompt.
func memPackage(t *testing.T, manifest, prompt string) *pipeline.Package {
	t.Helper()
	pkg, err := pipeline.FromFiles("/pkg", map[string][]byte{"pipeline.yaml": []byte(testPipeline),
		"actions/score/action.yaml": []byte(manifest), "actions/score/prompt.md": []byte(prompt)})
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

// fakeHarness answers every request with fn and records the requests.
type fakeHarness struct {
	mu   sync.Mutex
	reqs []Request
	fn   func(Request) (Response, error)
}

func (f *fakeHarness) Run(_ context.Context, req Request) (Response, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	return f.fn(req)
}

// session is one adapter process run over a hello and items.
type session struct {
	t       *testing.T
	opts    Options
	env     map[string]string
	workdir string
	config  map[string]any
	lessons json.RawMessage
	logs    bytes.Buffer
	apiKey  string
}

// newSession runs the action of a package loaded in memory.
func newSession(t *testing.T, pkg *pipeline.Package, h Harness) *session {
	t.Helper()
	s := dirSession(t, pkg.Dir, h)
	s.opts.Loaded = pkg
	return s
}

// dirSession runs the action of the package the adapter loads from dir.
func dirSession(t *testing.T, pkg string, h Harness) *session {
	t.Helper()
	s := &session{t: t, config: map[string]any{"rooms": 2}}
	s.env = map[string]string{"MILLSTREAM_SECRET_API_TOKEN": "sk-test-0123456789"}
	s.opts = Options{
		Package: pkg, Action: "./actions/score", Secrets: []string{"apiToken"},
		Env:    func(k string) (string, bool) { v, ok := s.env[k]; return v, ok },
		Logger: slog.New(slog.NewTextHandler(&s.logs, nil)),
	}
	if h != nil {
		s.opts.NewHarness = func(_ *pipeline.AgentRuntime, apiKey string) Harness {
			s.apiKey = apiKey
			return h
		}
	}
	return s
}

// withWorkdir gives the session a working directory, where the adapter writes its transcripts: only the tests of
// transcripts and artifacts need one.
func (s *session) withWorkdir() *session {
	s.t.Helper()
	return s.inWorkdir(testdir.New(s.t))
}

// inWorkdir gives the session the working directory dir, which sessions run one after the other may share.
func (s *session) inWorkdir(dir string) *session {
	s.workdir = dir
	s.env["MILLSTREAM_WORKDIR"] = dir
	return s
}

// run sends hello, one item per payload and shutdown, and returns the adapter's messages.
func (s *session) run(payloads ...string) []protocol.Message {
	s.t.Helper()
	var in bytes.Buffer
	w := protocol.NewWriter(&in)
	msgs := []protocol.Message{protocol.Hello{Protocol: 1, Action: "lab/score", Config: s.config,
		Vars: map[string]any{"city": "Lyon"}, Lessons: s.lessons}}
	for i, p := range payloads {
		msgs = append(msgs, protocol.Item{Delivery: "dl_" + string(rune('a'+i)), ID: "it_1", Key: "k",
			Payload: json.RawMessage(p), Attempt: i + 1})
	}
	for _, m := range append(msgs, protocol.Shutdown{}) {
		if err := w.Write(m); err != nil {
			s.t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := Run(s.t.Context(), &in, &out, s.opts); err != nil {
		s.t.Fatalf("Run: %v", err)
	}
	var got []protocol.Message
	r := protocol.NewReader(&out)
	for {
		m, err := r.Read()
		if err != nil {
			break
		}
		got = append(got, m)
	}
	if len(got) == 0 || got[0].Type() != protocol.TypeReady {
		s.t.Fatalf("messages = %+v, want ready first", got)
	}
	return got[1:]
}

// transcript reads the transcript the last attempt wrote.
func (s *session) transcript() (Transcript, string) {
	s.t.Helper()
	f, err := os.Open(filepath.Join(s.workdir, TranscriptFile))
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f) // stored compressed (spec section 7.2)
	if err != nil {
		s.t.Fatal(err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		s.t.Fatal(err)
	}
	var tr Transcript
	if err := json.Unmarshal(raw, &tr); err != nil {
		s.t.Fatal(err)
	}
	return tr, string(raw)
}

func answer(text string, in, out int64) func(Request) (Response, error) {
	return func(req Request) (Response, error) {
		return Response{Text: text, TokensIn: in, TokensOut: out, Turns: 1,
			Transcript: []Turn{{Role: "user", Text: req.Prompt}, {Role: "assistant", Text: text}}}, nil
	}
}

func compact(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

var transcriptArtifact = []protocol.Artifact{{Name: TranscriptFile, Path: TranscriptFile}}

// TestRunEmitsOutput: the prompt is rendered with payload, config and vars; the fenced JSON answer is emitted with
// the five metrics, cost from the price table, and done carries the transcript.
func TestRunEmitsOutput(t *testing.T) {
	t.Parallel()
	h := &fakeHarness{fn: answer("Here it is.\n```json\n{\"score\": 7}\n```\n", 1000, 500)}
	s := newSession(t, memPackage(t, testManifest, testPrompt), h).withWorkdir()
	s.opts.Price = &pipeline.Price{InPerMtok: 3, OutPerMtok: 15}
	msgs := s.run(`{"url": "https://example.com/1"}`)
	if len(msgs) != 7 {
		t.Fatalf("messages = %+v", msgs)
	}
	want := map[string]float64{MetricTokensIn: 1000, MetricTokensOut: 500, MetricCostUSD: 0.0105, MetricTurns: 1}
	for _, m := range msgs[:5] {
		mt, ok := m.(protocol.Metric)
		if !ok || mt.Delivery != "dl_a" || mt.Labels["model"] != "claude-sonnet-5-5" {
			t.Fatalf("metric %+v", m)
		}
		if v, ok := want[mt.Name]; ok && (v-mt.Value > 1e-9 || mt.Value-v > 1e-9) {
			t.Fatalf("metric %s = %v, want %v", mt.Name, mt.Value, v)
		}
	}
	emit, ok := msgs[5].(protocol.Emit)
	if !ok || string(emit.Payload) != `{"score":7}` || emit.Key != "k" {
		t.Fatalf("emit %+v", msgs[5])
	}
	done, ok := msgs[6].(protocol.Done)
	if !ok || len(done.Artifacts) != 1 || done.Artifacts[0] != transcriptArtifact[0] {
		t.Fatalf("done %+v", msgs[6])
	}
	wantPrompt := "Score https://example.com/1 for Lyon with at least 2 rooms.\n"
	if h.reqs[0].Prompt != wantPrompt || h.reqs[0].Model != "claude-sonnet-5-5" || h.reqs[0].MaxTurns != 10 ||
		!strings.Contains(string(h.reqs[0].Schema), `"required":["score"]`) || s.apiKey != "sk-test-0123456789" {
		t.Fatalf("request %+v, api key %q", h.reqs[0], s.apiKey)
	}
	tr, _ := s.transcript()
	if tr.Prompt != wantPrompt || compact(t, tr.Output) != `{"score":7}` || tr.Error != "" || len(tr.Turns) != 2 ||
		tr.Attempt != 1 || tr.Delivery != "dl_a" || tr.CostUSD < 0.0104 || tr.Harness != "api" {
		t.Fatalf("transcript %+v", tr)
	}
}

// TestRunSchemaMismatch: an answer that fails the output schema is a retryable error, and each attempt keeps its
// transcript, the error and the rejected output in it.
func TestRunSchemaMismatch(t *testing.T) {
	t.Parallel()
	h := &fakeHarness{fn: answer("```json\n{\"score\": \"high\"}\n```", 10, 5)}
	s := newSession(t, memPackage(t, testManifest, testPrompt), h).withWorkdir()
	for attempt := 1; attempt <= 2; attempt++ {
		_ = os.Remove(filepath.Join(s.workdir, TranscriptFile)) // the node empties the working directory
		payloads := []string{`{"url": "u"}`, `{"url": "u"}`}[:attempt]
		msgs := s.run(payloads...)
		last, ok := msgs[len(msgs)-1].(protocol.Error)
		if !ok || !last.Retryable || !strings.Contains(last.Message, "does not match the output schema") ||
			!strings.Contains(last.Message, TranscriptFile) || len(last.Artifacts) != 1 {
			t.Fatalf("attempt %d: %+v", attempt, msgs[len(msgs)-1])
		}
		if !slices.ContainsFunc(msgs, func(m protocol.Message) bool {
			mt, ok := m.(protocol.Metric)
			return ok && mt.Name == MetricSchemaFailures && mt.Value == 1 && mt.Labels["model"] != ""
		}) {
			t.Fatalf("attempt %d: no schema-validation failure metric in %+v", attempt, msgs)
		}
		tr, _ := s.transcript()
		if tr.Attempt != attempt || !strings.Contains(tr.Error, "output schema") || compact(t, tr.Output) != `{"score":"high"}` {
			t.Fatalf("attempt %d transcript %+v", attempt, tr)
		}
	}
}

// TestRunAbstains: an answer that is the abstain object alone is not validated against the output schema: it is
// answered abstain with its reason, the metrics and the transcript; an object with more keys is an output as any.
func TestRunAbstains(t *testing.T) {
	t.Parallel()
	h := &fakeHarness{fn: answer("```json\n{\"abstain\": \"too vague\"}\n```", 10, 5)}
	s := newSession(t, memPackage(t, testManifest, testPrompt), h).withWorkdir()
	msgs := s.run(`{"url": "u"}`)
	ab, ok := msgs[len(msgs)-1].(protocol.Abstain)
	if !ok || ab.Delivery != "dl_a" || ab.Reason != "too vague" || len(ab.Artifacts) != 1 || len(msgs) != 6 {
		t.Fatalf("messages %+v", msgs)
	}
	if _, ok := abstains(map[string]any{"abstain": "x", "score": 1.0}); ok {
		t.Fatal("an object with another key abstained")
	}
}

// TestRunFailures: what each failure answers, retryable or not, always with the transcript once the harness ran.
func TestRunFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		prompt    string
		fn        func(Request) (Response, error)
		retryable bool
		message   string
		metrics   bool
	}{
		{"no structured output", testPrompt, answer("I cannot score this.", 1, 1), true, "no structured output", true},
		{"invalid fenced block", testPrompt, answer("```json\n{score: 7}\n```", 1, 1), true, "is not JSON", true},
		{"harness permanent", testPrompt, func(Request) (Response, error) {
			return Response{}, permanent("replay: no recording")
		}, false, "no recording", true},
		{"harness transient", testPrompt, func(Request) (Response, error) {
			return Response{}, errors.New("connection reset")
		}, true, "connection reset", true},
		{"template fails", "{{ .Nope }}", answer("", 0, 0), false, "render prompt", false},
	}
	workdir := testdir.New(t) // the cases run in turn, each writing its transcript over the last
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSession(t, memPackage(t, testManifest, tt.prompt), &fakeHarness{fn: tt.fn}).inWorkdir(workdir)
			msgs := s.run(`{"url": "u"}`)
			last, ok := msgs[len(msgs)-1].(protocol.Error)
			if !ok || last.Retryable != tt.retryable || !strings.Contains(last.Message, tt.message) ||
				len(last.Artifacts) != 1 {
				t.Fatalf("answer %+v", msgs[len(msgs)-1])
			}
			if got := len(msgs) == 6; got != tt.metrics {
				t.Fatalf("metrics sent %v, want %v: %+v", got, tt.metrics, msgs)
			}
		})
	}
}

// TestRunSetupErrors: an action that cannot run answers every item with a non-retryable error naming why.
func TestRunSetupErrors(t *testing.T) {
	t.Parallel()
	exec := "name: lab/score\nkind: exec\nruntime: { exec: { command: [x] } }\n"
	tests := []struct {
		name     string
		manifest string
		env      map[string]string
		message  string
	}{
		{"not an agent", exec, nil, "is of kind exec, not agent"},
		{"api key unset", testManifest, map[string]string{"MILLSTREAM_SECRET_API_TOKEN": ""}, "secret apiToken"},
		{"bad mode", testManifest, map[string]string{EnvHarnessMode: "dry"}, `"dry" is not one of`},
		{"replay without a directory", testManifest, map[string]string{EnvHarnessMode: ModeReplay}, EnvHarnessDir},
		{"bad template", testManifest, map[string]string{"prompt": "{{ .Payload"}, "prompt:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prompt := testPrompt
			if p, ok := tt.env["prompt"]; ok {
				prompt = p
			}
			s := newSession(t, memPackage(t, tt.manifest, prompt), &fakeHarness{fn: answer("", 0, 0)})
			for k, v := range tt.env {
				if v == "" {
					delete(s.env, k)
				} else {
					s.env[k] = v
				}
			}
			msgs := s.run(`{"url": "u"}`, `{"url": "v"}`)
			for _, m := range msgs {
				e, ok := m.(protocol.Error)
				if !ok || e.Retryable || !strings.Contains(e.Message, tt.message) {
					t.Fatalf("messages %+v, want non-retryable errors with %q", msgs, tt.message)
				}
			}
			if len(msgs) != 2 || !strings.Contains(s.logs.String(), "agent action cannot run") {
				t.Fatalf("messages %+v, logs %s", msgs, s.logs.String())
			}
		})
	}
	s := dirSession(t, filepath.Join(testdir.New(t), "missing"), nil)
	if msgs := s.run(`{}`); len(msgs) != 1 || msgs[0].(protocol.Error).Retryable {
		t.Fatalf("missing package: %+v", msgs)
	}
}

// TestRunRedactsSecrets: secret values, raw or JSON-escaped, never reach the transcript; the harness gets the API
// key, and the error message the node redacts.
func TestRunRedactsSecrets(t *testing.T) {
	t.Parallel()
	secret := `sk-"quoted"\value<é>`
	h := &fakeHarness{fn: func(req Request) (Response, error) {
		data, _ := json.Marshal(map[string]string{"echo": req.Prompt}) // escapes <, > as \u003c, \u003e
		return Response{Text: "```json\n{\"score\": 1}\n```", Transcript: []Turn{
			{Role: "user", Text: req.Prompt}, {Role: "assistant", Text: "token " + secret, Data: data},
		}}, nil
	}}
	s := newSession(t, memPackage(t, testManifest, "Use {{ .Config.token }} on {{ .Payload.url }}."), h).withWorkdir()
	s.env["MILLSTREAM_SECRET_API_TOKEN"] = secret
	s.config = map[string]any{"token": secret}
	s.run(`{"url": "u"}`)
	_, raw := s.transcript()
	if strings.Contains(raw, "quoted") || strings.Contains(raw, "value") || !strings.Contains(raw, "[redacted:apiToken]") {
		t.Fatalf("transcript leaks the secret:\n%s", raw)
	}
	if s.apiKey != secret || !strings.Contains(h.reqs[0].Prompt, secret) {
		t.Fatalf("api key %q, prompt %q", s.apiKey, h.reqs[0].Prompt)
	}
}

// TestRunRedactsSecretsFromRecordings: a recording, committed with the tests, stores the response with the secret
// values redacted, in its text, structured output and transcript (the prompt turn included); its key is still the
// hash of the request as sent, so a replay of the same request finds it.
func TestRunRedactsSecretsFromRecordings(t *testing.T) {
	t.Parallel()
	secret := `sk-"quoted"\value<é>`
	h := &fakeHarness{fn: func(req Request) (Response, error) {
		data, _ := json.Marshal(map[string]string{"echo": req.Prompt})
		structured, _ := json.Marshal(map[string]any{"score": 1, "note": secret})
		return Response{Text: "```json\n{\"score\": 1}\n```\n" + secret, Structured: structured, Transcript: []Turn{
			{Role: "user", Text: req.Prompt}, {Role: "assistant", Text: "token " + secret, Data: data},
		}}, nil
	}}
	dir := testdir.New(t)
	s := newSession(t, memPackage(t, testManifest, "Use {{ .Config.token }} on {{ .Payload.url }}."), h)
	s.env["MILLSTREAM_SECRET_API_TOKEN"] = secret
	s.env[EnvHarnessMode], s.env[EnvHarnessDir] = ModeRecord, dir
	s.config = map[string]any{"token": secret}
	s.run(`{"url": "u"}`)
	if len(h.reqs) != 1 {
		t.Fatalf("harness called %d times", len(h.reqs))
	}
	key := Key(pipeline.HarnessAPI, h.reqs[0])
	raw, err := os.ReadFile(filepath.Join(dir, key+".json")) // #nosec G304 -- the test's own directory
	if err != nil {
		t.Fatalf("no recording under the unredacted request's key: %v", err)
	}
	if strings.Contains(string(raw), "quoted") || strings.Contains(string(raw), "value") ||
		!strings.Contains(string(raw), "[redacted:apiToken]") {
		t.Fatalf("recording leaks the secret:\n%s", raw)
	}
}

// TestCost: the harness's own cost wins over the price table; without either, the cost is 0 and a warning says why.
func TestCost(t *testing.T) {
	t.Parallel()
	reported := 0.25
	price := &pipeline.Price{InPerMtok: 3, OutPerMtok: 15}
	tests := []struct {
		name  string
		resp  Response
		price *pipeline.Price
		want  float64
		warn  bool
	}{
		{"reported", Response{TokensIn: 1e6, CostUSD: &reported}, price, 0.25, false},
		{"priced", Response{TokensIn: 2e6, TokensOut: 1e6}, price, 21, false},
		{"unknown", Response{TokensIn: 10}, nil, 0, true},
		{"nothing spent", Response{}, nil, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			a := &adapter{opts: Options{Price: tt.price, Logger: slog.New(slog.NewTextHandler(&logs, nil))},
				agent: &pipeline.AgentRuntime{Model: "m"}}
			if got := a.cost(tt.resp); got != tt.want || strings.Contains(logs.String(), "cost unknown") != tt.warn {
				t.Fatalf("cost = %v (want %v), logs %q", got, tt.want, logs.String())
			}
		})
	}
}

// TestRunWithoutWorkdir: without a working directory the transcript is not kept, logged, and the item answered.
func TestRunWithoutWorkdir(t *testing.T) {
	t.Parallel()
	s := newSession(t, memPackage(t, testManifest, testPrompt), &fakeHarness{fn: answer(`{"score": 3}`, 1, 1)})
	delete(s.env, "MILLSTREAM_WORKDIR")
	msgs := s.run(`{"url": "u"}`)
	if done, ok := msgs[len(msgs)-1].(protocol.Done); !ok || done.Artifacts != nil ||
		!strings.Contains(s.logs.String(), "transcript not kept") {
		t.Fatalf("messages %+v, logs %s", msgs, s.logs.String())
	}
}

// TestRunProtocol: the adapter needs hello first and refuses messages it does not expect.
func TestRunProtocol(t *testing.T) {
	t.Parallel()
	for name, in := range map[string]string{
		"no hello":      `{"type":"shutdown"}` + "\n",
		"empty":         "",
		"emit received": `{"type":"hello","protocol":1}` + "\n" + `{"type":"emit","delivery":"d"}` + "\n",
		"garbage":       `{"type":"hello","protocol":1}` + "\nnot json\n",
	} {
		t.Run(name, func(t *testing.T) {
			if err := Run(t.Context(), strings.NewReader(in), &bytes.Buffer{}, Options{}); err == nil {
				t.Fatal("Run accepted it")
			}
		})
	}
	// The end of input after hello is a clean exit.
	if err := Run(t.Context(), strings.NewReader(`{"type":"hello","protocol":1}`+"\n"), &bytes.Buffer{},
		Options{}); err != nil {
		t.Fatalf("EOF after hello: %v", err)
	}
}

const testLessons = `{"rules":[{"text":"Ground-floor flats on a main road score below 40.","source":"review/r-1"}],` +
	`"examples":[{"input":{"floor":0},"output":{"score":35},"why":"Noise."}]}`

// TestRunLessons: the lessons from hello are appended to a template that does not mention them, rendered by one
// that does, and change the rendered prompt, hence the recording key.
func TestRunLessons(t *testing.T) {
	t.Parallel()
	prompts := func(prompt string, lessons json.RawMessage) string {
		t.Helper()
		h := &fakeHarness{fn: answer(`{"score": 1}`, 1, 1)}
		s := newSession(t, memPackage(t, testManifest, prompt), h)
		s.lessons = lessons
		s.run(`{"url": "u"}`)
		return h.reqs[0].Prompt
	}
	plain := prompts(testPrompt, nil)
	appended := prompts(testPrompt, json.RawMessage(testLessons))
	want := plain + "\n\n## Lessons from past runs\n\nRules:\n\n- Ground-floor flats on a main road score below 40.\n" +
		"\nExamples:\n\n- Input: {\"floor\":0}\n  Output: {\"score\":35}\n  Why: Noise.\n"
	if appended != want {
		t.Fatalf("prompt with lessons:\n%s\nwant:\n%s", appended, want)
	}
	own := prompts("Rules:{{ range .Lessons.Rules }} {{ .Text }}{{ end }}", json.RawMessage(testLessons))
	if own != "Rules: Ground-floor flats on a main road score below 40." {
		t.Fatalf("a template using .Lessons: %q", own)
	}
	if empty := prompts("Rules:{{ range .Lessons.Rules }} {{ .Text }}{{ end }}", nil); empty != "Rules:" {
		t.Fatalf("a template using .Lessons without lessons: %q", empty)
	}
	edited := prompts(testPrompt, json.RawMessage(strings.Replace(testLessons, "below 40", "below 30", 1)))
	key := func(p string) string { return Key(pipeline.HarnessAPI, Request{Model: "m", Prompt: p}) }
	if edited == appended || key(edited) == key(appended) || key(appended) == key(plain) {
		t.Fatal("editing the lessons left the prompt or the recording key unchanged")
	}

	s := newSession(t, memPackage(t, testManifest, testPrompt), &fakeHarness{fn: answer("", 0, 0)})
	s.lessons = json.RawMessage(`{"rules": 3}`)
	if msgs := s.run(`{}`); len(msgs) != 1 || !strings.Contains(msgs[0].(protocol.Error).Message, "hello lessons") {
		t.Fatalf("malformed lessons: %+v", msgs)
	}
}
