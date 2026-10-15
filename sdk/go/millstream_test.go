package millstream

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// helperEnv makes the test binary act as an action process, for the tests of Map and MapKeyed, which own the
// process's stdio and exit status.
const helperEnv = "MILLSTREAM_SDK_TEST_ACTION"

func TestMain(m *testing.M) {
	switch os.Getenv(helperEnv) {
	case "map":
		Map(score)
		os.Exit(0)
	case "mapKeyed":
		MapKeyed(split)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestMapProcess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		mode     string
		input    []string
		wantCode int
		wantOut  string
	}{
		{"map", []string{hello, item("dl_1", `{"url":"u","price":1}`), `{"type":"shutdown"}`}, 0,
			`{"type":"ready"}` + "\n" +
				`{"type":"metric","delivery":"dl_1","name":"scored","value":1,"labels":{"city":"Lyon"}}` + "\n" +
				`{"type":"emit","delivery":"dl_1","payload":{"url":"u","score":2}}` + "\n" +
				`{"type":"done","delivery":"dl_1"}` + "\n"},
		{"mapKeyed", []string{hello, item("dl_1", `{"url":"a"}`)}, 0,
			`{"type":"ready"}` + "\n" + `{"type":"emit","delivery":"dl_1","key":"a","payload":"A"}` + "\n" +
				`{"type":"done","delivery":"dl_1"}` + "\n"},
		{"map", []string{"garbage"}, 1, ""},
	}
	for _, tt := range tests {
		t.Run(tt.mode+fmt.Sprint(tt.wantCode), func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(os.Args[0], "-test.run=^$") // #nosec G204 -- the test binary itself
			cmd.Env = append(os.Environ(), helperEnv+"="+tt.mode)
			cmd.Stdin = strings.NewReader(strings.Join(tt.input, "\n") + "\n")
			var stdout, stderr strings.Builder
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			code := 0
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				code = exit.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if code != tt.wantCode || stdout.String() != tt.wantOut {
				t.Fatalf("code %d, stdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
			}
			if code != 0 && !strings.Contains(stderr.String(), `"msg":"action stopped"`) {
				t.Fatalf("stderr must log why the action stopped: %s", stderr.String())
			}
		})
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
			input:   []string{hello, item("dl_1", `{"url":"ab"}`), item("dl_2", `{"url":""}`), item("dl_3", `{"url":"unkeyed"}`)},
			want: []string{
				`{"type":"ready"}`,
				`{"type":"emit","delivery":"dl_1","key":"a","payload":"A"}`,
				`{"type":"emit","delivery":"dl_1","key":"b","payload":"B"}`,
				`{"type":"done","delivery":"dl_1"}`,
				`{"type":"done","delivery":"dl_2"}`,
				`{"type":"error","delivery":"dl_3","message":"output 0 of 2 has no key: a delivery with several outputs must key them all","retryable":false}`,
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
	c := &itemContext{out: &writer{w: &failingWriter{}}, logger: newLogger()}
	if len(c.Config()) != 0 || len(c.Vars()) != 0 {
		t.Fatal("nil config and vars must read as empty maps")
	}
	c.Metric("lost", 1, nil) // a failed write is logged, not fatal
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
