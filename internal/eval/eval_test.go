package eval

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/pipeline"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// TestLoad: a case takes its name from its file, its values become JSON values, and every malformed case is
// refused with its file named.
func TestLoad(t *testing.T) {
	t.Parallel()
	dir := writePackage(t, "http://unused", map[string]string{
		"actions/echo/evals/lyon.yaml": "input: { city: Lyon, rooms: 3 }\nconfig: { mode: echo }\nassert: [\"true\"]\n",
	})
	c := loadOne(t, filepath.Join(dir, "actions", "echo"))
	in, _ := c.Input.(map[string]any)
	if c.Name != "lyon" || in["rooms"] != int64(3) || c.Config["mode"] != "echo" || c.Vars == nil {
		t.Fatalf("case = %+v", c)
	}
	if cases, err := Load(filepath.Join(dir, "actions", "score")); err != nil || len(cases) != 0 {
		t.Fatalf("no evals = %v, %v", cases, err)
	}

	tests := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{"nothing to check", map[string]string{"a.yaml": "input: {}\n"}, "needs assertions"},
		{"bad CEL", map[string]string{"a.yaml": "assert: [\"output.\"]\n"}, "assert[0]"},
		{"not a bool", map[string]string{"a.yaml": "assert: [\"1 + 1\"]\n"}, "must evaluate to a bool"},
		{"secrets", map[string]string{"a.yaml": "assert: [\"secrets.x == ''\"]\n"}, "secrets are only available"},
		{"unknown key", map[string]string{"a.yaml": "asert: [\"true\"]\n"}, "field asert not found"},
		{"judge without rubric", map[string]string{"a.yaml": "judge: { model: m }\n"}, "a rubric and a model"},
		{"judge provider", map[string]string{"a.yaml": "judge: { rubric: r, model: m, provider: x }\n"}, "judge.provider"},
		{"openai without url", map[string]string{"a.yaml": "judge: { rubric: r, model: m, provider: openai }\n"},
			"judge.baseUrl"},
		{"same name", map[string]string{"a.yaml": "name: x\nassert: [\"true\"]\n", "b.yaml": "name: x\nassert: [\"true\"]\n"},
			`case "x" is also named by`},
		{"not YAML", map[string]string{"a.yaml": "assert: [\n"}, "a.yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// What Load reads, in memory: the files in name order, as Glob lists them.
			names := slices.Sorted(maps.Keys(tt.files))
			_, err := parseCases(names, func(name string) ([]byte, error) { return []byte(tt.files[name]), nil })
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseCases: %v, want %q", err, tt.want)
			}
		})
	}
}

// TestRunExec: an exec action runs through the exec runtime; assertions see its output, the input and the vars,
// and the score is the share that hold. One that fails to evaluate is a failure with its reason.
func TestRunExec(t *testing.T) {
	t.Parallel()
	pkg, c := echoCase(t, "http://unused", map[string]string{"actions/echo/evals/lyon.yaml": `
input: { city: Lyon, rooms: 3 }
config: { mode: echo }
vars: { minRooms: 2 }
assert:
  - output["in"].city == input.city
  - output["in"].rooms >= vars.minRooms
  - output["in"].rooms > 5
  - output.missing == 1
`})
	r := Run(t.Context(), pkgOptions(t, pkg, "./actions/echo", agent.ModeReplay, ""), c)
	if r.Err != "" || r.Passed != 2 || r.Total != 4 || r.Score != 0.5 || r.Passes() || len(r.Failed) != 2 ||
		r.Failed[0] != `output["in"].rooms > 5` || !strings.Contains(r.Failed[1], "no such key") {
		t.Fatalf("result = %+v", r)
	}
}

// TestRunActionFailure: an action that answers an error, of a kind without a launcher, or of a kind eval does not
// run, scores 0 with the reason.
func TestRunActionFailure(t *testing.T) {
	t.Parallel()
	pkg, c := echoCase(t, "http://unused", map[string]string{
		"actions/echo/evals/down.yaml": "config: { mode: error }\nassert: [\"true\"]\n",
	})
	opts := pkgOptions(t, pkg, "./actions/echo", agent.ModeReplay, "")
	if r := Run(t.Context(), opts, c); r.Passes() || r.Score != 0 || !strings.Contains(r.Err, "portal answered 503") {
		t.Fatalf("result = %+v", r)
	}
	opts.Action.Kind = pipeline.KindContainer // no launcher for it in opts
	if r := Run(t.Context(), opts, c); !strings.Contains(r.Err, "of kind container: this node has no runtime for it") {
		t.Fatalf("result = %+v", r)
	}
	opts.Action.Kind = "review"
	if r := Run(t.Context(), opts, c); !strings.Contains(r.Err, "only agent, exec and container actions run here") {
		t.Fatalf("result = %+v", r)
	}
}

// TestRunAgentRecordReplay: an agent action runs in-process through the adapter. Recorded against a fake model, it
// replays with the model gone and no API key, with the same output and its cost from the price table; a case never
// recorded misses with an error.
func TestRunAgentRecordReplay(t *testing.T) {
	t.Parallel()
	srv, calls := fakeModel(t, "```json\n{\"score\": 7}\n```")
	pkg := memPackage(t, srv.URL, map[string]string{
		"actions/score/evals/one.yaml": "input: { url: \"https://example.com/1\" }\nassert: [\"output.score == 7\"]\n",
	})
	c, recordings := memCase(t, pkg, "actions/score"), testdir.New(t)
	opts := pkgOptions(t, pkg, "./actions/score", agent.ModeRecord, recordings)
	opts.Client = srv.Client
	opts.Price = &pipeline.Price{InPerMtok: 3, OutPerMtok: 15}
	if r := Run(t.Context(), opts, c); !r.Passes() || r.Score != 1 || r.CostUSD != 0.0045 {
		t.Fatalf("record = %+v", r)
	}
	opts.Client = nil // the model is gone: a replay never calls it
	opts.Mode = agent.ModeReplay
	opts.Env = func(k string) (string, bool) { return "", false }
	if r := Run(t.Context(), opts, c); !r.Passes() || r.Score != 1 || calls.Load() != 1 {
		t.Fatalf("replay = %+v after %d calls", r, calls.Load())
	}
	// A miss says how to record: the environment variable, or what the caller names (eval's flag).
	c.Input = map[string]any{"url": "https://example.com/2"}
	if r := Run(t.Context(), opts, c); r.Passes() || !strings.Contains(r.Err, "no recording") ||
		!strings.Contains(r.Err, "record it with MILLSTREAM_HARNESS_MODE=record (") {
		t.Fatalf("unrecorded = %+v", r)
	}
	opts.RecordHint = agent.FlagRecordHint
	if r := Run(t.Context(), opts, c); !strings.Contains(r.Err, "record it with --harness-mode record (") {
		t.Fatalf("unrecorded, with a hint = %+v", r)
	}
}

// TestJudge: the judge asks the api harness for a verdict on the output against the rubric; recorded once against a
// fake model, it replays from the recording. The case's score is the mean of its assertions' share and the judge's
// score, and the judge's score never fails a case. A verdict out of range is an error. The recording holds the
// pipeline's secrets redacted, here one in the input the judge's prompt carries.
func TestJudge(t *testing.T) {
	t.Parallel()
	srv, calls := fakeModel(t, "Fine.\n```json\n{\"score\": 0.5, \"reason\": \"The city is right.\"}\n```")
	// The keyed mode's output is deterministic, unlike echo's (a pid): the judge's prompt, and its recording key,
	// holds the output.
	pkg, c := echoCase(t, srv.URL, map[string]string{"actions/echo/evals/judged.yaml": `
input: { city: Lyon, note: sk-test }
config: { mode: keyed, n: 1 }
assert: [output == 0]
judge: { rubric: "The output is zero.", model: claude-haiku-5, baseUrl: "` + srv.URL + `", apiKeySecret: apiToken }
`})
	recordings := testdir.New(t)
	opts := pkgOptions(t, pkg, "./actions/echo", agent.ModeRecord, recordings)
	opts.Client = srv.Client
	r := Run(t.Context(), opts, c)
	if !r.Passes() || r.Judge == nil || r.Judge.Score != 0.5 || r.Judge.Reason != "The city is right." || r.Score != 0.75 {
		t.Fatalf("record = %+v", r)
	}
	files, _ := os.ReadDir(recordings)
	if len(files) != 1 {
		t.Fatalf("recordings = %v", files)
	}
	raw, _ := os.ReadFile(filepath.Join(recordings, files[0].Name())) // #nosec G304 -- test file
	if !strings.Contains(string(raw), "The output is zero.") || !strings.Contains(string(raw), `\"city\": \"Lyon\"`) ||
		strings.Contains(string(raw), "sk-test") || !strings.Contains(string(raw), "[redacted:apiToken]") {
		t.Fatalf("recording = %s", raw)
	}
	opts.Client = nil // the model is gone: a replay never calls it
	opts.Mode = agent.ModeReplay
	if r := Run(t.Context(), opts, c); r.Judge == nil || r.Score != 0.75 || calls.Load() != 1 {
		t.Fatalf("replay = %+v after %d calls", r, calls.Load())
	}

	bad, _ := fakeModel(t, "```json\n{\"score\": 2, \"reason\": \"x\"}\n```")
	c.Judge.BaseURL, opts.Client = bad.URL, bad.Client
	opts.Mode, opts.Dir = agent.ModeLive, ""
	if r := Run(t.Context(), opts, c); r.Passes() || r.Score != 0 || !strings.Contains(r.Err, "judge: the verdict") {
		t.Fatalf("out of range = %+v", r)
	}
	if c.Judge.Provider != pipeline.ProviderAnthropic {
		t.Fatalf("provider = %q, want the default", c.Judge.Provider)
	}
}
