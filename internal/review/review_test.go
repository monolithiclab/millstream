package review

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Sampling is a pure function of the item id: the same id is always sampled alike, and over 1000 ids the share
// sampled stays near the rate.
func TestSampled(t *testing.T) {
	t.Parallel()
	for _, rate := range []float64{0.01, 0.05, 0.5} {
		n := 0
		for i := range 1000 {
			id := fmt.Sprintf("it_%04d", i)
			first := Sampled(id, rate)
			if Sampled(id, rate) != first {
				t.Fatalf("%s sampled differently twice", id)
			}
			if first {
				n++
			}
		}
		if lo, hi := rate*1000*0.5-3, rate*1000*1.5+3; float64(n) < lo || float64(n) > hi {
			t.Errorf("rate %g: %d of 1000 sampled, want within [%g, %g]", rate, n, lo, hi)
		}
	}
	if Sampled("x", 0) || !Sampled("x", 1) {
		t.Fatal("rates 0 and 1 must sample nothing and everything")
	}
}

func TestPatch(t *testing.T) {
	t.Parallel()
	doc := map[string]any{"price": 100.0, "tags": []any{"a", "b"}, "o": map[string]any{"x/y": 1.0}}
	tests := []struct{ patch, want, err string }{
		{`[{"op":"add","path":"/notes","value":"n"}]`, `{"notes":"n","o":{"x/y":1},"price":100,"tags":["a","b"]}`, ""},
		{`[{"op":"add","path":"/tags/1","value":"z"},{"op":"add","path":"/tags/-","value":"e"}]`,
			`{"o":{"x/y":1},"price":100,"tags":["a","z","b","e"]}`, ""},
		{`[{"op":"remove","path":"/tags/0"},{"op":"replace","path":"/o/x~1y","value":2}]`,
			`{"o":{"x/y":2},"price":100,"tags":["b"]}`, ""},
		{`[{"op":"move","from":"/price","path":"/cost"},{"op":"copy","from":"/tags/1","path":"/first"}]`,
			`{"cost":100,"first":"b","o":{"x/y":1},"tags":["a","b"]}`, ""},
		{`[{"op":"test","path":"/price","value":100},{"op":"replace","path":"","value":{"all":true}}]`, `{"all":true}`, ""},
		{`[{"op":"test","path":"/price","value":5}]`, "", "does not hold the tested value"},
		{`[{"op":"replace","path":"/missing","value":1}]`, "", "/missing does not exist"},
		{`[{"op":"remove","path":"/tags/9"}]`, "", "out of range"},
		{`[{"op":"add","path":"/tags/01","value":1}]`, "", "not an array index"},
		{`[{"op":"add","path":"/price/x","value":1}]`, "", "not an object or an array"},
		{`[{"op":"remove","path":""}]`, "", "whole document"},
		{`[{"op":"move","from":"/o","path":"/o/inner"}]`, "", "into itself"},
		{`[{"op":"copy","from":"/nope","path":"/x"}]`, "", "/nope does not exist"},
		{`[{"op":"add","path":"price","value":1}]`, "", "is not a JSON pointer"},
		{`[{"op":"frob","path":"/price"}]`, "", "unknown op"},
	}
	for _, tt := range tests {
		ops, err := ParsePatch([]byte(tt.patch))
		if err != nil {
			t.Fatal(err)
		}
		got, err := Apply(doc, ops)
		if tt.err != "" {
			if err == nil || !strings.Contains(err.Error(), tt.err) || !errors.Is(err, ErrPatch) {
				t.Errorf("%s: error %v, want %q", tt.patch, err, tt.err)
			}
			continue
		}
		if b, _ := json.Marshal(got); err != nil || string(b) != tt.want {
			t.Errorf("%s = %s, %v; want %s", tt.patch, b, err, tt.want)
		}
	}
	if b, _ := json.Marshal(doc); string(b) != `{"o":{"x/y":1},"price":100,"tags":["a","b"]}` {
		t.Fatalf("Apply changed its input: %s", b)
	}
	if _, err := ParsePatch([]byte(`{}`)); !errors.Is(err, ErrPatch) {
		t.Fatalf("a patch that is not an array: %v", err)
	}
}

func TestDiffAndEditable(t *testing.T) {
	t.Parallel()
	a := map[string]any{"price": 100, "gone": true, "o": map[string]any{"k": 1}, "tags": []any{"a"}}
	b := map[string]any{"price": 150, "new": nil, "o": map[string]any{"k": 1}, "tags": []any{"a", "b"}}
	diff := Diff(a, b)
	got, _ := json.Marshal(diff)
	want := `[{"op":"remove","path":"/gone"},{"op":"replace","path":"/price","value":150},` +
		`{"op":"replace","path":"/tags","value":["a","b"]},{"op":"add","path":"/new","value":null}]`
	if string(got) != want {
		t.Fatalf("diff = %s\nwant   %s", got, want)
	}
	if applied, err := Apply(a, diff); err != nil || fmt.Sprint(normal(applied)) != fmt.Sprint(normal(b)) {
		t.Fatalf("applying the diff = %v, %v", applied, err)
	}
	if p := Pointers(append(diff, Op{Op: "add", Path: "/new"})); strings.Join(p, " ") != "/gone /price /tags /new" {
		t.Fatalf("pointers = %v", p)
	}
	editable := []string{"/notes", "/price"}
	for _, tt := range []struct {
		ops []Op
		ok  bool
	}{
		{[]Op{{Op: "replace", Path: "/price"}, {Op: "add", Path: "/notes/0"}}, true},
		{[]Op{{Op: "replace", Path: "/priceEur"}}, false},
		{[]Op{{Op: "move", From: "/city", Path: "/notes"}}, false},
	} {
		if err := Editable(tt.ops, editable); (err == nil) != tt.ok {
			t.Errorf("Editable(%v) = %v", tt.ops, err)
		}
	}
}

// A notification reaches the webhook as Slack-compatible JSON with the inbox link; a failing webhook is retried three
// times with doubling backoff, through the injected sleeper, then counted as failed.
func TestNotifier(t *testing.T) {
	t.Parallel()
	var (
		mu     sync.Mutex
		bodies []Message
		fails  int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m Message
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &m)
		mu.Lock()
		defer mu.Unlock()
		bodies = append(bodies, m)
		if fails > 0 {
			fails--
			http.Error(w, "busy", http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()
	// The handler runs on the server's goroutines: the test reads and resets its state under mu too.
	reset := func(failing int) {
		mu.Lock()
		fails, bodies = failing, nil
		mu.Unlock()
	}
	posts := func() []Message {
		mu.Lock()
		defer mu.Unlock()
		return bodies
	}
	var slept []time.Duration
	n := &Notifier{BaseURL: "https://ms.example/", Client: srv.Client(),
		Sleep: func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }}
	note := Notification{URL: srv.URL, Pipeline: "p", Step: "gate", Inbox: "ana lysts", DeliveryID: "dl_1",
		ItemKey: "k1", Display: "Lyon, 100 000 EUR", Deadline: time.Date(2026, 10, 18, 6, 0, 0, 0, time.UTC)}

	reset(2)
	if err := n.Send(t.Context(), note); err != nil {
		t.Fatal(err)
	}
	if len(posts()) != 3 || fmt.Sprint(slept) != "[1s 2s]" {
		t.Fatalf("posts = %d, slept %v", len(posts()), slept)
	}
	m := posts()[0]
	if m.Text != "Review needed in inbox ana lysts: p/gate, item k1. https://ms.example/inbox/ana%20lysts/dl_1 "+
		"Decide before 2026-10-18T06:00:00Z." || len(m.Blocks) != 1 || m.Blocks[0].Type != "section" ||
		!strings.Contains(m.Blocks[0].Text.Text, "Lyon, 100 000 EUR\n<https://ms.example/inbox/ana%20lysts/dl_1|Open in the inbox>") {
		t.Fatalf("message = %+v", m)
	}

	reset(10)
	slept = nil
	n.Notify(t.Context(), note)
	n.Wait()
	if n.Failures() != 1 || len(posts()) != 4 || fmt.Sprint(slept) != "[1s 2s 4s]" {
		t.Fatalf("failures %d after %d posts, slept %v", n.Failures(), len(posts()), slept)
	}

	stop := &Notifier{Client: srv.Client(), Sleep: func(context.Context, time.Duration) error { return context.Canceled }}
	reset(1)
	if err := stop.Send(t.Context(), Notification{URL: srv.URL, Escalated: true}); err == nil ||
		!strings.Contains(err.Error(), "retry abandoned") {
		t.Fatalf("a cancelled retry: %v", err)
	}
	if err := stop.Send(t.Context(), Notification{URL: "http://[::1]:namedport"}); err == nil {
		t.Fatal("a malformed URL was posted")
	}
	long := Payload(DefaultBaseURL, Notification{Inbox: "i", DeliveryID: "d", Display: strings.Repeat("é", 4000),
		Escalated: true})
	if len(long.Blocks[0].Text.Text) > maxBlockText || !strings.HasPrefix(long.Text, "Review escalated") {
		t.Fatalf("long display: %d bytes, %q", len(long.Blocks[0].Text.Text), long.Text[:20])
	}
}
