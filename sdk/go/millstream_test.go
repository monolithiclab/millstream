package millstream

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// serveCase is a run of an action over stdio, with its exit status, its output and what its stderr holds.
type serveCase struct {
	name     string
	handler  Handler
	input    []string
	env      map[string]string
	wantCode int
	wantOut  string
	stderr   string
}

// serveCases are the action runs TestServe makes in-process and the e2e build's TestMapProcess as processes.
func serveCases(workdir string) []serveCase {
	return []serveCase{
		{"map", MapHandler(score), []string{hello, item("dl_1", `{"url":"u","price":1}`), `{"type":"shutdown"}`}, nil, 0,
			`{"type":"ready"}` + "\n" +
				`{"type":"metric","delivery":"dl_1","name":"scored","value":1,"labels":{"city":"Lyon"}}` + "\n" +
				`{"type":"emit","delivery":"dl_1","payload":{"url":"u","score":2}}` + "\n" +
				`{"type":"done","delivery":"dl_1"}` + "\n", ""},
		{"mapKeyed", MapKeyedHandler(split), []string{hello, item("dl_1", `{"url":"a"}`)}, nil, 0,
			`{"type":"ready"}` + "\n" + `{"type":"emit","delivery":"dl_1","key":"a","payload":"A"}` + "\n" +
				`{"type":"done","delivery":"dl_1"}` + "\n", ""},
		{"map", MapHandler(score), []string{"garbage"}, nil, 1, "", `"msg":"action stopped"`},
		// An action abstaining, its abstention wrapped, then answering an audit of the item it would abstain on.
		{"abstain", MapHandler(rules), []string{hello, item("dl_1", `{"url":"u"}`),
			`{"type":"item","delivery":"dl_2","id":"it_2","payload":{"url":"u"},"audit":true}`}, nil, 0,
			`{"type":"ready"}` + "\n" + `{"type":"abstain","delivery":"dl_1","reason":"no rule for u"}` + "\n" +
				`{"type":"emit","delivery":"dl_2","payload":{"url":"audited","score":0}}` + "\n" +
				`{"type":"done","delivery":"dl_2"}` + "\n", ""},
		// An action reading an input artifact from the working directory and attaching the file it writes: the emit
		// names it by its path relative to the working directory.
		{"artifact", MapHandler(annotate(workdir)), []string{hello, `{"type":"item","delivery":"dl_1","id":"it_1",` +
			`"payload":{},"artifacts":[{"name":"page.html","path":"in/page.html"}]}`},
			map[string]string{"MILLSTREAM_WORKDIR": workdir}, 0,
			`{"type":"ready"}` + "\n" + `{"type":"emit","delivery":"dl_1","payload":"annotated",` +
				`"artifacts":[{"name":"notes.txt","path":"notes.txt"}]}` + "\n" + `{"type":"done","delivery":"dl_1"}` + "\n",
			""},
	}
}

// TestServe runs actions in-process as Map and MapKeyed run them: the exit status, the output and, when the action
// stops, the reason logged on stderr.
func TestServe(t *testing.T) {
	t.Parallel()
	workdir := artifactWorkdir(t)
	for _, tt := range serveCases(workdir) {
		var stdout, stderr strings.Builder
		code := serve(strings.NewReader(strings.Join(tt.input, "\n")+"\n"), &stdout, &stderr,
			func(k string) string { return tt.env[k] }, tt.handler)
		tt.check(t, code, stdout.String(), stderr.String())
	}
	checkNotes(t, workdir)
}

func (tt serveCase) check(t *testing.T, code int, stdout, stderr string) {
	t.Helper()
	if code != tt.wantCode || stdout != tt.wantOut || !strings.Contains(stderr, tt.stderr) {
		t.Fatalf("%s: code %d, stdout:\n%s\nstderr:\n%s", tt.name, code, stdout, stderr)
	}
}

type listing struct {
	URL   string `json:"url"`
	Price int    `json:"price"`
}

type scored struct {
	URL   string `json:"url"`
	Score int    `json:"score"`
}

const hello = `{"type":"hello","protocol":1,"action":"lab/score","version":"1.0.0","config":{"factor":2},"vars":{"city":"Lyon"},"future":true}`

func item(delivery, payload string) string {
	return fmt.Sprintf(`{"type":"item","delivery":%q,"id":"it_%s","key":"k","payload":%s,"attempt":1,"idempotencyKey":"ik"}`,
		delivery, delivery, payload)
}

// score doubles the price into a score, using the config factor; a price of 0 fails retryably, a negative one
// fails for good.
func score(c Context, in listing) ([]scored, error) {
	var cfg struct {
		Factor int `json:"factor"`
	}
	if err := c.ConfigInto(&cfg); err != nil {
		return nil, err
	}
	switch {
	case in.Price == 0:
		return nil, Retryable(errors.New("price not loaded yet"))
	case in.Price < 0:
		return nil, errors.New("negative price")
	case in.Price > 1000:
		return []scored{{URL: in.URL}, {URL: in.URL}}, nil
	}
	c.Metric("scored", 1, map[string]string{"city": c.Vars()["city"].(string)})
	c.Logger().Info("scoring", "item", c.Item().ID)
	return []scored{{URL: in.URL, Score: in.Price * cfg.Factor}}, nil
}

// rules abstains on every item, but on an audit, which it answers.
func rules(c Context, in listing) ([]scored, error) {
	if c.Item().Audit {
		return []scored{{URL: "audited"}}, nil
	}
	err := fmt.Errorf("rules: %w", c.Abstain("no rule for "+in.URL))
	if !IsAbstain(err) || err.Error() != "rules: abstain: no rule for u" {
		return nil, fmt.Errorf("abstention %v", err)
	}
	return nil, err
}

// split emits one keyed output per character of the URL, or several unkeyed ones when asked to misbehave.
func split(c Context, in listing) ([]Keyed[string], error) {
	if c.Item().Attempt != 1 || c.Item().IdempotencyKey != "ik" || c.Item().Key != "k" {
		return nil, fmt.Errorf("item info %+v", c.Item())
	}
	if in.URL == "unkeyed" {
		return []Keyed[string]{{Value: "a"}, {Value: "b"}}, nil
	}
	var out []Keyed[string]
	for _, ch := range in.URL {
		out = append(out, Keyed[string]{Key: string(ch), Value: strings.ToUpper(string(ch))})
	}
	return out, nil
}

func TestRunWithIO(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		handler Handler
		input   []string
		want    []string
		wantErr string
	}{
		{
			name:    "Map with one output, then end of input",
			handler: MapHandler(score),
			input:   []string{hello, item("dl_1", `{"url":"u","price":21}`)},
			want: []string{
				`{"type":"ready"}`,
				`{"type":"metric","delivery":"dl_1","name":"scored","value":1,"labels":{"city":"Lyon"}}`,
				`{"type":"emit","delivery":"dl_1","payload":{"url":"u","score":42}}`,
				`{"type":"done","delivery":"dl_1"}`,
			},
		},
		{
			name:    "Map errors, then shutdown",
			handler: MapHandler(score),
			input: []string{
				hello,
				item("dl_1", `{"url":"u","price":0}`),
				item("dl_2", `{"url":"u","price":-1}`),
				item("dl_3", `{"url":"u","price":"cheap"}`),
				item("dl_4", `{"url":"u","price":5000}`),
				`{"type":"shutdown"}`,
				item("dl_5", `{"url":"never read","price":1}`),
			},
			want: []string{
				`{"type":"ready"}`,
				`{"type":"error","delivery":"dl_1","message":"price not loaded yet","retryable":true}`,
				`{"type":"error","delivery":"dl_2","message":"negative price","retryable":false}`,
				`{"type":"error","delivery":"dl_3","message":"decode input: json: cannot unmarshal string into Go struct field listing.price of type int","retryable":false}`,
				`{"type":"error","delivery":"dl_4","message":"Map returned 2 outputs: use MapKeyed to emit several keyed outputs","retryable":false}`,
			},
		},
		{
			name:    "MapKeyed with several outputs",
			handler: MapKeyedHandler(split),
			input: []string{
				hello, item("dl_1", `{"url":"ab"}`), item("dl_2", `{"url":""}`), item("dl_3", `{"url":"unkeyed"}`),
				item("dl_4", `{"url":"aba"}`),
			},
			want: []string{
				`{"type":"ready"}`,
				`{"type":"emit","delivery":"dl_1","key":"a","payload":"A"}`,
				`{"type":"emit","delivery":"dl_1","key":"b","payload":"B"}`,
				`{"type":"done","delivery":"dl_1"}`,
				`{"type":"done","delivery":"dl_2"}`,
				`{"type":"error","delivery":"dl_3","message":"output 0 of 2 has no key: a delivery with several outputs must key them all","retryable":false}`,
				`{"type":"error","delivery":"dl_4","message":"outputs 0 and 2 share the key \"a\": a delivery's output keys must be distinct","retryable":false}`,
			},
		},
		{
			name: "output that does not encode",
			handler: MapHandler(func(Context, listing) ([]func(), error) {
				return []func(){func() {}}, nil
			}),
			input: []string{hello, item("dl_1", `{}`)},
			want: []string{
				`{"type":"ready"}`,
				`{"type":"error","delivery":"dl_1","message":"encode output 0: json: unsupported type: func()","retryable":false}`,
			},
		},
		{
			name: "output too large for a line",
			handler: MapHandler(func(Context, listing) ([]string, error) {
				return []string{strings.Repeat("x", maxLine)}, nil
			}),
			input: []string{hello, item("dl_1", `{}`)},
			want: []string{
				`{"type":"ready"}`,
				`{"type":"error","delivery":"dl_1","message":"output 0 is 8388610 bytes: a protocol line holds at most 8388608; write large data as an artifact under MILLSTREAM_WORKDIR","retryable":false}`,
			},
		},
		{name: "garbage hello", handler: MapHandler(score), input: []string{"{"}, wantErr: "malformed line"},
		{name: "no hello", handler: MapHandler(score), input: nil, wantErr: "protocol violation: no hello"},
		{name: "item before hello", handler: MapHandler(score), input: []string{item("dl_1", `{}`)}, wantErr: `want hello first, got "item"`},
		{name: "unsupported protocol", handler: MapHandler(score), input: []string{`{"type":"hello","protocol":2}`}, wantErr: "protocol 2 is not supported"},
		{
			name: "garbage line", handler: MapHandler(score), input: []string{hello, "garbage"},
			want: []string{`{"type":"ready"}`}, wantErr: "protocol violation: malformed line",
		},
		{
			name: "unexpected message", handler: MapHandler(score), input: []string{hello, `{"type":"ready"}`},
			want: []string{`{"type":"ready"}`}, wantErr: `unexpected message "ready"`,
		},
		{
			name: "oversized line", handler: MapHandler(score), input: []string{hello, strings.Repeat("x", maxLine+1)},
			want: []string{`{"type":"ready"}`}, wantErr: "line longer than",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			in := strings.Join(tt.input, "\n")
			var out strings.Builder
			err := RunWithIO(strings.NewReader(in), &out, tt.handler)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("RunWithIO: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
			want := strings.Join(tt.want, "\n")
			if want != "" {
				want += "\n"
			}
			if out.String() != want {
				t.Fatalf("wrote:\n%s\nwant:\n%s", out.String(), want)
			}
		})
	}
}

// TestRunWithIOOverPipes drives the loop interactively, as the engine does: each answer is read before the next
// message is sent.
func TestRunWithIOOverPipes(t *testing.T) {
	t.Parallel()
	toAction, engineSide := io.Pipe()
	actionSide, fromAction := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- RunWithIO(toAction, fromAction, MapKeyedHandler(split))
		_ = fromAction.Close()
	}()
	answers := bufio.NewScanner(actionSide)
	send := func(line string) {
		t.Helper()
		if _, err := io.WriteString(engineSide, line+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(want ...string) {
		t.Helper()
		for _, w := range want {
			if !answers.Scan() {
				t.Fatalf("no answer, want %s (%v)", w, answers.Err())
			}
			if answers.Text() != w {
				t.Fatalf("got %s, want %s", answers.Text(), w)
			}
		}
	}
	send(hello)
	expect(`{"type":"ready"}`)
	send(item("dl_1", `{"url":"x"}`))
	expect(`{"type":"emit","delivery":"dl_1","key":"x","payload":"X"}`, `{"type":"done","delivery":"dl_1"}`)
	send(`{"type":"shutdown"}`)
	if err := <-done; err != nil {
		t.Fatalf("RunWithIO: %v", err)
	}
	if answers.Scan() {
		t.Fatalf("unexpected line after shutdown: %s", answers.Text())
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("pipe broken") }

func TestRunWithIOReadError(t *testing.T) {
	t.Parallel()
	err := RunWithIO(failingReader{}, io.Discard, MapHandler(score))
	if err == nil || errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "read message: pipe broken") {
		t.Fatalf("err = %v", err)
	}
}

type failingWriter struct{ after int }

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.after == 0 {
		return 0, errors.New("pipe closed")
	}
	w.after--
	return len(p), nil
}

func TestRunWithIOWriteErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		after int
		input []string
	}{
		{"ready", 0, []string{hello}},
		{"emit", 1, []string{hello, item("dl_1", `{"url":"ab"}`)}},
		{"done", 1, []string{hello, item("dl_1", `{"url":""}`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := RunWithIO(strings.NewReader(strings.Join(tt.input, "\n")), &failingWriter{after: tt.after},
				MapKeyedHandler(split))
			if err == nil || !strings.Contains(err.Error(), "write message: pipe closed") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestContext(t *testing.T) {
	t.Parallel()
	var logs strings.Builder
	c := &itemContext{out: &writer{w: &failingWriter{}}, logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	if len(c.Config()) != 0 || len(c.Vars()) != 0 {
		t.Fatal("nil config and vars must read as empty maps")
	}
	c.Metric("lost", 1, nil) // a failed write is logged, not fatal
	if !strings.Contains(logs.String(), `"msg":"send metric"`) {
		t.Fatalf("logs = %s", logs.String())
	}
	c.hello.Config = map[string]any{"bad": func() {}}
	if err := c.ConfigInto(&struct{}{}); err == nil || !strings.Contains(err.Error(), "encode config") {
		t.Fatalf("err = %v", err)
	}
	c.hello.Config = map[string]any{"n": "x"}
	var cfg struct {
		N int `json:"n"`
	}
	if err := c.ConfigInto(&cfg); err == nil || !strings.Contains(err.Error(), "decode config") {
		t.Fatalf("err = %v", err)
	}
}

func TestRetryable(t *testing.T) {
	t.Parallel()
	base := errors.New("timeout")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil stays nil", Retryable(nil), false},
		{"plain error", base, false},
		{"marked", Retryable(base), true},
		{"wrapped after marking", fmt.Errorf("fetch: %w", Retryable(base)), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsRetryable(tt.err); got != tt.want {
				t.Fatalf("IsRetryable = %v, want %v", got, tt.want)
			}
		})
	}
	if Retryable(nil) != nil {
		t.Fatal("Retryable(nil) must be nil")
	}
	if err := Retryable(base); err.Error() != "timeout" || !errors.Is(err, base) {
		t.Fatalf("Retryable must keep the message and unwrap: %v", err)
	}
}

func TestMessagesAreJSON(t *testing.T) {
	t.Parallel()
	var w strings.Builder
	out := &writer{w: &w}
	if err := out.send(errorMsg{Type: "error", Delivery: "dl_1", Message: "a\nb"}); err != nil {
		t.Fatal(err)
	}
	if strings.Count(w.String(), "\n") != 1 {
		t.Fatalf("a message must be one line: %q", w.String())
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(w.String()), &m); err != nil || m["message"] != "a\nb" {
		t.Fatalf("got %v, %v", m, err)
	}
	if err := out.send(func() {}); err == nil || !strings.Contains(err.Error(), "encode message") {
		t.Fatalf("err = %v", err)
	}
}
