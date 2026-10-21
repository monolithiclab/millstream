package review

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
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
	// add, replace and test need a value, which may be null; the other ops take none.
	for _, patch := range []string{`[{"op":"add","path":"/x"}]`, `[{"op":"replace","path":"/x"}]`,
		`[{"op":"test","path":"/x"}]`} {
		if _, err := ParsePatch([]byte(patch)); !errors.Is(err, ErrPatch) || !strings.Contains(err.Error(), "no value") {
			t.Errorf("%s: %v", patch, err)
		}
	}
	if ops, err := ParsePatch([]byte(`[{"op":"add","path":"/x","value":null},{"op":"remove","path":"/y"}]`)); err != nil ||
		len(ops) != 2 || ops[0].Value != nil {
		t.Errorf("a null value and a remove: %+v, %v", ops, err)
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

// hook is a scripted webhook transport: it records each body posted and answers 503 while fails lasts. With hold
// set, each post first waits for it to close (or its request to be cancelled).
type hook struct {
	mu     sync.Mutex
	bodies []Message
	fails  int
	err    error // returned instead of a response when set
	hold   chan struct{}
}

func (h *hook) RoundTrip(r *http.Request) (*http.Response, error) {
	if h.hold != nil {
		select {
		case <-h.hold:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
	var m Message
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &m)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bodies = append(h.bodies, m)
	if h.err != nil {
		return nil, h.err
	}
	status := http.StatusOK
	if h.fails > 0 {
		h.fails--
		status = http.StatusServiceUnavailable
	}
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Body: io.NopCloser(strings.NewReader("")),
		Header: http.Header{}}, nil
}

func (h *hook) reset(failing int) {
	h.mu.Lock()
	h.fails, h.bodies = failing, nil
	h.mu.Unlock()
}

func (h *hook) posts() []Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.bodies)
}

// A notification reaches the webhook as Slack-compatible JSON with the inbox link; a failing webhook is retried three
// times with doubling backoff, through the injected sleeper, then counted as failed.
func TestNotifier(t *testing.T) {
	h := &hook{}
	var slept []time.Duration
	n := &Notifier{BaseURL: "https://ms.example/", Client: &http.Client{Transport: h},
		Sleep: func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }}
	note := Notification{URL: "https://hooks.example/x", Pipeline: "p", Step: "gate", Inbox: "ana lysts",
		DeliveryID: "dl_1", ItemKey: "k1", Display: "Lyon, 100 000 EUR",
		Deadline: time.Date(2026, 10, 18, 6, 0, 0, 0, time.UTC)}

	h.reset(2)
	if err := n.Send(t.Context(), note); err != nil {
		t.Fatal(err)
	}
	if len(h.posts()) != 3 || fmt.Sprint(slept) != "[1s 2s]" {
		t.Fatalf("posts = %d, slept %v", len(h.posts()), slept)
	}
	m := h.posts()[0]
	if m.Text != "Review needed in inbox ana lysts: p/gate, item k1. https://ms.example/inbox/ana%20lysts/dl_1 "+
		"Decide before 2026-10-18T06:00:00Z." || len(m.Blocks) != 1 || m.Blocks[0].Type != "section" ||
		!strings.Contains(m.Blocks[0].Text.Text, "Lyon, 100 000 EUR\n<https://ms.example/inbox/ana%20lysts/dl_1|Open in the inbox>") {
		t.Fatalf("message = %+v", m)
	}

	h.reset(10)
	slept = nil
	n.Notify(note)
	n.Wait()
	if n.Failures() != 1 || len(h.posts()) != 4 || fmt.Sprint(slept) != "[1s 2s 4s]" {
		t.Fatalf("failures %d after %d posts, slept %v", n.Failures(), len(h.posts()), slept)
	}
	n.Shutdown(time.Second)

	stop := &Notifier{Client: &http.Client{Transport: h},
		Sleep: func(context.Context, time.Duration) error { return context.Canceled }}
	h.reset(1)
	if err := stop.Send(t.Context(), Notification{URL: "https://hooks.example/x", Escalated: true}); err == nil ||
		!strings.Contains(err.Error(), "retry abandoned") {
		t.Fatalf("a cancelled retry: %v", err)
	}
	if err := stop.Send(t.Context(), Notification{URL: "http://[::1]:namedport"}); err == nil ||
		strings.Contains(err.Error(), "namedport") {
		t.Fatalf("a malformed URL: %v", err)
	}
	long := Payload("http://127.0.0.1:7070", Notification{Inbox: "i", DeliveryID: "d", Display: strings.Repeat("é", 4000),
		Escalated: true})
	if len(long.Blocks[0].Text.Text) > maxBlockText || !strings.HasPrefix(long.Text, "Review escalated") {
		t.Fatalf("long display: %d bytes, %q", len(long.Blocks[0].Text.Text), long.Text[:20])
	}
	cut := Payload("http://127.0.0.1:7070", Notification{Inbox: "i", DeliveryID: "d", Display: strings.Repeat("<", 2000)})
	if body := cut.Blocks[0].Text.Text; strings.Contains(body, "&l…") || strings.Contains(body, "&…") ||
		!strings.Contains(body, "&lt;…") {
		t.Fatalf("a cut through an entity: %q", body[len(body)-80:])
	}
}

// Every value interpolated into the message is escaped for mrkdwn, so an item cannot forge a link or a mention.
func TestPayloadEscapes(t *testing.T) {
	m := Payload("https://ms.example", Notification{Inbox: "a", DeliveryID: "d", Pipeline: "p<1>", Step: "s&t",
		ItemKey: "<https://evil.example|click>", Display: "<!channel> & <@U1>"})
	all := m.Text + m.Blocks[0].Text.Text
	for _, bad := range []string{"<https://evil", "<!channel>", "<@U1>", "p<1>", "s&t"} {
		if strings.Contains(all, bad) {
			t.Errorf("%q left unescaped in %q", bad, all)
		}
	}
	if !strings.Contains(m.Blocks[0].Text.Text, "&lt;!channel&gt; &amp; &lt;@U1&gt;") ||
		!strings.Contains(m.Text, "item &lt;https://evil.example|click&gt;") {
		t.Fatalf("message = %+v", m)
	}
	if !strings.HasSuffix(m.Blocks[0].Text.Text, "\n<https://ms.example/inbox/a/d|Open in the inbox>") {
		t.Fatalf("the inbox link itself must stay a link: %q", m.Blocks[0].Text.Text)
	}
}

// The logs never carry a webhook URL, which may hold a token: a transport error names the host, and a webhook read
// from a secret is named by the secret; a secret that is not set fails at once.
func TestNotifierKeepsTheURLOutOfLogs(t *testing.T) {
	var logs bytes.Buffer
	h := &hook{err: errors.New("connection refused")}
	n := &Notifier{Client: &http.Client{Transport: h}, Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		Sleep: func(context.Context, time.Duration) error { return nil }}
	n.Notify(Notification{URL: "https://hooks.example/services/T0/SECRET-TOKEN", DeliveryID: "d1"})
	n.Notify(Notification{URL: "https://hooks.example/services/T0/SECRET-TOKEN", Secret: "slackHook", DeliveryID: "d2"})
	n.Notify(Notification{Secret: "unset", DeliveryID: "d3"})
	n.Notify(Notification{URL: "ftp://hooks.example/x", DeliveryID: "d4"})
	n.Shutdown(time.Second)
	out := logs.String()
	if strings.Contains(out, "SECRET-TOKEN") || !strings.Contains(out, "webhook=hooks.example") ||
		!strings.Contains(out, "webhook=[redacted:slackHook]") || !strings.Contains(out, "connection refused") ||
		!strings.Contains(out, "secret unset is not set to an http or https URL") ||
		!strings.Contains(out, "the webhook is not an http or https URL") || n.Failures() != 4 {
		t.Fatalf("failures %d, logs:\n%s", n.Failures(), out)
	}
}

// Notify never blocks: what finds the queue full is dropped and counted; after Shutdown everything is dropped.
func TestNotifierQueueBound(t *testing.T) {
	h := &hook{hold: make(chan struct{})}
	n := &Notifier{Client: &http.Client{Transport: h}, QueueSize: 2}
	note := Notification{URL: "https://hooks.example/x"}
	for range MaxInFlight + 2 + 3 { // the workers take MaxInFlight, the queue 2, the rest drop
		n.Notify(note)
	}
	// The workers may not all have taken theirs yet: at least the 3 beyond every slot dropped.
	if f := n.Failures(); f < 3 {
		t.Fatalf("failures = %d, want the overflow dropped", f)
	}
	dropped := n.Failures()
	close(h.hold)
	n.Shutdown(time.Minute)
	if got := int64(len(h.posts())) + dropped; got != MaxInFlight+5 || n.Failures() != dropped {
		t.Fatalf("%d posted, %d dropped, failures now %d", len(h.posts()), dropped, n.Failures())
	}
	n.Notify(note)
	if n.Failures() != dropped+1 {
		t.Fatal("a notification after Shutdown was not dropped")
	}
	n.Shutdown(time.Minute) // a second call does nothing
}

// Shutdown lets what is queued and in flight finish, starts no new retry, and cancels the posts still running when
// its grace runs out.
func TestNotifierShutdown(t *testing.T) {
	h := &hook{hold: make(chan struct{}), fails: 1}
	retried := make(chan struct{}, 1)
	n := &Notifier{Client: &http.Client{Transport: h}, QueueSize: 1,
		Sleep: func(ctx context.Context, _ time.Duration) error {
			retried <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}}
	n.Notify(Notification{URL: "https://hooks.example/x", DeliveryID: "d1"})
	close(h.hold)
	<-retried // d1 failed once and waits for its retry
	n.Notify(Notification{URL: "https://hooks.example/x", DeliveryID: "d2"})
	n.Shutdown(time.Minute)
	// d1's retry was abandoned, d2 was posted though queued when Shutdown began.
	if p := h.posts(); len(p) < 2 || n.Failures() != 1 {
		t.Fatalf("%d posts, failures %d", len(p), n.Failures())
	}

	stuck := &hook{hold: make(chan struct{})}
	s := &Notifier{Client: &http.Client{Transport: stuck}}
	s.Notify(Notification{URL: "https://hooks.example/x"})
	s.Shutdown(time.Millisecond) // the post never ends by itself: the grace cancels it
	if s.Failures() != 1 || len(stuck.posts()) != 0 {
		t.Fatalf("failures %d, posts %d", s.Failures(), len(stuck.posts()))
	}
}
