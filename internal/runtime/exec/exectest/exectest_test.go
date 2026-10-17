package exectest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/protocol"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// TestPlace: Place puts a file at its destination, creating the directory and replacing a file already there, and
// fails under a file or for a missing binary.
func TestPlace(t *testing.T) {
	t.Parallel()
	bin := filepath.Join(testdir.New(t), "fakeaction")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o700); err != nil { // #nosec G306 -- a test executable
		t.Fatal(err)
	}
	dst := filepath.Join(testdir.New(t), "actions", "fake", "fakeaction")
	if err := Place(bin, dst); err != nil {
		t.Fatalf("Place: %v", err)
	}
	if info, err := os.Stat(dst); err != nil || info.Mode()&0o111 == 0 {
		t.Errorf("placed binary %s: %v, %v", dst, info, err)
	}
	if err := Place(bin, dst); err != nil { // a file is there: Place replaces it
		t.Fatalf("Place again: %v", err)
	}
	if err := Place(bin, filepath.Join(dst, "under-a-file")); err == nil {
		t.Fatal("Place under a file succeeded")
	}
	if err := Place(filepath.Join(testdir.New(t), "missing"), filepath.Join(testdir.New(t), "x")); err == nil {
		t.Fatal("Place of a missing binary succeeded")
	}
}

// failWriter fails every write.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

// serveCase is one run of the in-process fake action.
type serveCase struct {
	name   string
	config string   // hello's config, JSON
	items  []string // the items' payloads, JSON
	input  string   // raw input instead of hello, the items and shutdown
	noEnd  bool     // stdin ends after the items, without shutdown
	cancel bool     // the context is cancelled from the start, as when the runtime kills the action
	stdout io.Writer
	want   string // the types of the messages written, "!" standing for a line that is not a message
	status int
	stderr string
	check  func(t *testing.T, msgs []protocol.Message, workdir string)
}

// TestServe drives the in-process fake action through each of its modes. It is handed its whole input at once
// (hello, the items, shutdown), then its output, stderr and exit status are checked. The modes that hang get a
// cancelled context, which ends them as the runtime's kill does.
func TestServe(t *testing.T) {
	t.Parallel()
	tests := []serveCase{
		{name: "echo", config: `{"mode":"echo"}`, items: []string{`{"city":"Lyon"}`}, want: "ready emit done",
			check: func(t *testing.T, msgs []protocol.Message, workdir string) {
				t.Helper()
				var out struct {
					PID     int             `json:"pid"`
					Workdir string          `json:"workdir"`
					In      json.RawMessage `json:"in"`
				}
				e, _ := msgs[1].(protocol.Emit)
				if err := json.Unmarshal(e.Payload, &out); err != nil || out.PID <= 1<<30 || out.Workdir != workdir ||
					string(out.In) != `{"city":"Lyon"}` || e.Key != "key" {
					t.Errorf("emit = %+v (%s): %v", e, e.Payload, err)
				}
			}},
		{name: "stdin ends", config: `{"mode":"echo"}`, items: []string{`{}`}, noEnd: true, want: "ready emit done"},
		{name: "keyed", config: `{"mode":"keyed","n":2}`, items: []string{`{}`}, want: "ready emit emit metric done",
			check: func(t *testing.T, msgs []protocol.Message, _ string) {
				t.Helper()
				e0, _ := msgs[1].(protocol.Emit)
				e1, _ := msgs[2].(protocol.Emit)
				m, _ := msgs[3].(protocol.Metric)
				if e0.Key != "k0" || e1.Key != "k1" || string(e1.Payload) != "1" || m.Value != 2 {
					t.Errorf("keyed = %+v %+v %+v", e0, e1, m)
				}
			}},
		{name: "payload overrides the mode", config: `{"mode":"keyed","n":1}`, items: []string{`{"mode":"echo"}`},
			want: "ready emit done"},
		{name: "error", config: `{"mode":"error","retryable":true}`, items: []string{`{}`}, want: "ready error",
			check: func(t *testing.T, msgs []protocol.Message, _ string) {
				t.Helper()
				if e, _ := msgs[1].(protocol.Error); !e.Retryable || e.Message != "portal answered 503" {
					t.Errorf("error = %+v", e)
				}
			}},
		{name: "wrongId", config: `{"mode":"wrongId"}`, items: []string{`{}`}, want: "ready emit",
			check: func(t *testing.T, msgs []protocol.Message, _ string) {
				t.Helper()
				if e, _ := msgs[1].(protocol.Emit); e.Delivery != "dl_other" {
					t.Errorf("emit = %+v", e)
				}
			}},
		{name: "emitAfterDone", config: `{"mode":"emitAfterDone"}`, items: []string{`{}`}, want: "ready done emit"},
		{name: "lateMetric", config: `{"mode":"lateMetric"}`, items: []string{`{}`}, want: "ready done metric"},
		{name: "stderr", config: `{"mode":"stderr"}`, items: []string{`{}`}, want: "ready emit done",
			stderr: "fetching page\nparsed listing\n"},
		{name: "secret", config: `{"mode":"secret","secret":"tok-123"}`, items: []string{`{}`}, want: "ready error",
			stderr: "calling the portal with tok-123\n",
			check: func(t *testing.T, msgs []protocol.Message, _ string) {
				t.Helper()
				if e, _ := msgs[1].(protocol.Error); e.Retryable || !strings.Contains(e.Message, "tok-123") {
					t.Errorf("error = %+v", e)
				}
			}},
		{name: "cost", config: `{"mode":"cost","costs":[0.25,0.5]}`, items: []string{`{}`},
			want: "ready metric metric metric done",
			check: func(t *testing.T, msgs []protocol.Message, _ string) {
				t.Helper()
				m0, _ := msgs[1].(protocol.Metric)
				m2, _ := msgs[3].(protocol.Metric)
				if m0.Name != protocol.MetricCostUSD || m0.Value != 0.25 || m2.Name != "tokens_in" {
					t.Errorf("metrics = %+v %+v", m0, m2)
				}
			}},
		{name: "cost failing", config: `{"mode":"cost","costs":[1],"fail":true}`, items: []string{`{}`},
			want: "ready metric metric error"},
		{name: "artifact", config: `{"mode":"artifact"}`,
			items: []string{`{"files":{"out/a.txt":"made"},"links":{"l":"out/a.txt"},` +
				`"emit":[{"name":"a","path":"out/a.txt"}],"attempt":[{"name":"log","path":"l"}]}`},
			want: "ready emit done",
			check: func(t *testing.T, msgs []protocol.Message, workdir string) {
				t.Helper()
				e, _ := msgs[1].(protocol.Emit)
				d, _ := msgs[2].(protocol.Done)
				if string(e.Payload) != `{"in":"in.txt=given"}` || len(e.Artifacts) != 1 || len(d.Artifacts) != 1 {
					t.Errorf("emit = %+v (%s), done = %+v", e, e.Payload, d)
				}
				if b, err := os.ReadFile(filepath.Join(workdir, "l")); err != nil || string(b) != "made" { // #nosec G304 -- test file
					t.Errorf("link reads %q, %v", b, err)
				}
			}},
		{name: "artifact failing", config: `{"mode":"artifact"}`, items: []string{`{"fail":true}`},
			want: "ready emit error"},
		{name: "artifact not an object", config: `{"mode":"artifact"}`, items: []string{`[1]`}, want: "ready",
			status: 1, stderr: "fakeaction: json"},
		{name: "artifact link exists", config: `{"mode":"artifact"}`, items: []string{`{"links":{"in.txt":"x"}}`},
			want: "ready", status: 1, stderr: "file exists"},
		{name: "slow", config: `{"mode":"slow","sleepMs":0}`, items: []string{`{}`}, want: "ready emit done"},
		{name: "slow killed", config: `{"mode":"slow","sleepMs":3600000}`, items: []string{`{}`}, cancel: true,
			want: "ready", status: 1, stderr: "context canceled"},
		{name: "garbage", config: `{"mode":"garbage"}`, items: []string{`{}`, `{}`}, want: "ready !"},
		{name: "crash", config: `{"mode":"crash"}`, items: []string{`{}`}, want: "ready emit", status: 3},
		{name: "hang", config: `{"mode":"hang"}`, items: []string{`{}`}, cancel: true, want: "ready", status: 1,
			stderr: "context canceled"},
		{name: "hang at shutdown", config: `{"mode":"hang"}`, items: []string{`{"mode":"echo"}`}, cancel: true,
			want: "ready emit done", status: 1, stderr: "context canceled"},
		{name: "noReady", config: `{"mode":"noReady"}`, cancel: true, want: "", status: 1},
		{name: "noRead", config: `{"mode":"noRead"}`, items: []string{`{}`}, cancel: true, want: "ready", status: 1},
		{name: "child", config: `{"mode":"child"}`, items: []string{`{}`}, want: "ready", status: 1,
			stderr: "child mode needs a process"},
		{name: "no input", want: "", status: 1, stderr: "read hello: EOF"},
		{name: "no hello", input: `{"type":"shutdown"}` + "\n", want: "", status: 1, stderr: "want hello"},
		{name: "unexpected message", input: `{"type":"hello"}` + "\n" + `{"type":"ready"}` + "\n",
			want: "ready", status: 1, stderr: "unexpected ready message"},
		{name: "bad line", input: `{"type":"hello"}` + "\nnot json\n", want: "ready", status: 1,
			stderr: "malformed line"},
		{name: "stdout fails", config: `{}`, stdout: failWriter{}, status: 1, stderr: "broken pipe"},
	}
	dir := testdir.New(t) // the artifact cases' working directories, under one temporary directory
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t, dir)
		})
	}
}

func (tt serveCase) run(t *testing.T, dir string) {
	t.Helper()
	// Only the artifact mode touches its working directory: the others get a path that does not exist.
	workdir := "/nonexistent/workdir"
	if strings.Contains(tt.config, `"artifact"`) {
		var err error
		if workdir, err = os.MkdirTemp(dir, "work-"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(workdir, "in.txt"), []byte("given"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	in := tt.input
	if in == "" && tt.config != "" {
		in = `{"type":"hello","protocol":1,"config":` + tt.config + "}\n"
		for _, p := range tt.items {
			in += `{"type":"item","delivery":"dl_1","key":"key","payload":` + p +
				`,"artifacts":[{"name":"in","path":"in.txt"}]}` + "\n"
		}
		if !tt.noEnd {
			in += `{"type":"shutdown"}` + "\n"
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if tt.cancel {
		cancel()
	}
	var stdout, stderr bytes.Buffer
	out := tt.stdout
	if out == nil {
		out = &stdout
	}
	status := Serve(ctx, []string{"fakeaction"}, []string{"MILLSTREAM_WORKDIR=" + workdir}, strings.NewReader(in),
		out, &stderr)
	if status != tt.status || !strings.Contains(stderr.String(), tt.stderr) {
		t.Errorf("status %d, stderr %q; want %d, %q", status, stderr.String(), tt.status, tt.stderr)
	}
	msgs, types := readAll(bytes.NewReader(stdout.Bytes()))
	if types != tt.want {
		t.Fatalf("messages %q, want %q (%s)", types, tt.want, stdout.String())
	}
	if tt.check != nil {
		tt.check(t, msgs, workdir)
	}
}

// readAll decodes the messages written and lists their types, "!" standing for a line that is not a message.
func readAll(r io.Reader) ([]protocol.Message, string) {
	pr := protocol.NewReader(r)
	var msgs []protocol.Message
	var types []string
	for {
		m, err := pr.Read()
		if errors.Is(err, io.EOF) {
			return msgs, strings.Join(types, " ")
		}
		if err != nil {
			return msgs, strings.Join(append(types, "!"), " ")
		}
		msgs = append(msgs, m)
		types = append(types, m.Type())
	}
}
