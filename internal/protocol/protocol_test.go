package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	startedAt := time.Date(2026, 10, 14, 6, 0, 0, 0, time.UTC)
	tests := []struct {
		msg  Message
		line string
	}{
		{
			Hello{Protocol: Version, Action: "lab/score", Version: "2.1.0", Config: map[string]any{"k": "v"}, Vars: map[string]any{"n": 1.0}},
			`{"type":"hello","protocol":1,"action":"lab/score","version":"2.1.0","config":{"k":"v"},"vars":{"n":1}}`,
		},
		{
			Item{
				Delivery: "dl_1", ID: "it_1", Key: "k", Payload: json.RawMessage(`{"url":"u"}`),
				Artifacts: []Artifact{{Name: "in.pdf", Path: "in/in.pdf"}},
				Run:       Run{ID: "run_1", Trigger: "daily", StartedAt: startedAt, Revision: "abc"},
				Attempt:   2, IdempotencyKey: "ik",
			},
			`{"type":"item","delivery":"dl_1","id":"it_1","key":"k","payload":{"url":"u"},` +
				`"artifacts":[{"name":"in.pdf","path":"in/in.pdf"}],` +
				`"run":{"id":"run_1","trigger":"daily","startedAt":"2026-10-14T06:00:00Z","revision":"abc"},` +
				`"attempt":2,"idempotencyKey":"ik"}`,
		},
		{Shutdown{}, `{"type":"shutdown"}`},
		{Ready{}, `{"type":"ready"}`},
		{
			Emit{Delivery: "dl_1", Key: "a", Payload: json.RawMessage(`{"score":3}`), Artifacts: []Artifact{{Name: "r", Path: "out/r"}}},
			`{"type":"emit","delivery":"dl_1","key":"a","payload":{"score":3},"artifacts":[{"name":"r","path":"out/r"}]}`,
		},
		{Emit{Delivery: "dl_1", Payload: json.RawMessage(`1`)}, `{"type":"emit","delivery":"dl_1","payload":1}`},
		{
			Metric{Delivery: "dl_1", Name: "pages_fetched", Value: 12, Labels: map[string]string{"source": "portal"}},
			`{"type":"metric","delivery":"dl_1","name":"pages_fetched","value":12,"labels":{"source":"portal"}}`,
		},
		{Done{Delivery: "dl_1"}, `{"type":"done","delivery":"dl_1"}`},
		{Error{Delivery: "dl_1", Message: "boom", Retryable: true}, `{"type":"error","delivery":"dl_1","message":"boom","retryable":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.msg.Type(), func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := NewWriter(&buf).Write(tt.msg); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tt.line+"\n" {
				t.Fatalf("wrote %s\nwant  %s", got, tt.line)
			}
			r := NewReader(&buf)
			got, err := r.Read()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.msg) {
				t.Fatalf("read %#v\nwant %#v", got, tt.msg)
			}
			if _, err := r.Read(); !errors.Is(err, io.EOF) {
				t.Fatalf("second Read = %v, want io.EOF", err)
			}
		})
	}
}

func TestReadErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"unknown type", `{"type":"abstain","delivery":"dl_1"}` + "\n", `unknown message type "abstain"`},
		{"missing type", `{"delivery":"dl_1"}` + "\n", `unknown message type ""`},
		{"not JSON", "garbage\n", "malformed line"},
		{"not an object", "[1]\n", "malformed line"},
		{"empty line", "\n", "malformed line"},
		{"wrong field type", `{"type":"done","delivery":3}` + "\n", "malformed done message"},
		{"oversized line", `{"type":"emit","payload":"` + strings.Repeat("x", testLine) + `"}` + "\n", "line longer than 1024"},
		{"oversized last line by one byte", strings.Repeat(" ", testLine+1), "line longer than"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := newReader(strings.NewReader(tt.input), testLine).Read()
			if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want ErrProtocol with %q", err, tt.want)
			}
		})
	}
}

// A line of exactly the limit passes the length limit, with or without its newline. The lines are malformed
// from their first byte, so the reader gets past the limit and refuses them cheaply as JSON: decoding a full 8 MiB
// message is the e2e suite's TestReadLimitsFullLine.
func TestReadLimits(t *testing.T) {
	t.Parallel()
	line := "x" + strings.Repeat(" ", testLine-1)
	for name, input := range map[string]string{"last line": line, "with newline": line + "\n"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := newReader(strings.NewReader(input), testLine).Read()
			if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "malformed line") {
				t.Fatalf("err = %v, want ErrProtocol for a malformed line, not for its length", err)
			}
		})
	}
}

func TestReadUnknownFieldsIgnored(t *testing.T) {
	t.Parallel()
	m, err := NewReader(strings.NewReader(`{"type":"item","delivery":"dl_1","audit":false}`)).Read()
	if err != nil || m.(Item).Delivery != "dl_1" {
		t.Fatalf("got %#v, %v", m, err)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("pipe broken") }

func TestReadIOError(t *testing.T) {
	t.Parallel()
	_, err := NewReader(failingReader{}).Read()
	if err == nil || errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "read message: pipe broken") {
		t.Fatalf("err = %v", err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("pipe closed") }

type badPayload struct{ Done }

func (badPayload) MarshalJSON() ([]byte, error) { return nil, errors.New("cannot encode") }

func TestWriteErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		w    io.Writer
		msg  Message
		want string
	}{
		{"write fails", failingWriter{}, Done{Delivery: "dl_1"}, "write done message: pipe closed"},
		{"encode fails", io.Discard, badPayload{}, "encode done message"},
		{"oversized", io.Discard, Emit{Key: strings.Repeat("x", testLine), Payload: json.RawMessage(`1`)}, "exceeds 1024"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := (&Writer{w: tt.w, maxLine: testLine}).Write(tt.msg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestWriterConcurrentLines(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if err := w.Write(Metric{Delivery: "dl_1", Name: "n", Value: 1}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	r := NewReader(&buf)
	for i := range 50 {
		if _, err := r.Read(); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
	}
}

// testLine is the line limit of the tests of the limits: lines of MaxLine's 8 MiB are the e2e build's
// (protocol_e2e_test.go).
const testLine = 1024

func TestReaderAndWriterLimitIsMaxLine(t *testing.T) {
	t.Parallel()
	if r, w := NewReader(strings.NewReader("")), NewWriter(io.Discard); r.maxLine != MaxLine || w.maxLine != MaxLine {
		t.Fatalf("limits %d and %d, want MaxLine", r.maxLine, w.maxLine)
	}
	if w := NewWriterLimit(io.Discard, testLine); w.maxLine != testLine {
		t.Fatalf("NewWriterLimit: limit %d, want %d", w.maxLine, testLine)
	}
}
