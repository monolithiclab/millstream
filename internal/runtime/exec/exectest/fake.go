package exectest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
)

// Serve is the fake action, in-process: it has exec.FuncLauncher's F signature, so tests run it with
// exec.FuncLauncher{F: exectest.Serve} instead of the built binary, and start no process. Each call is a new
// "process", with its own pid from a counter. The child mode needs a real process and fails in-process.
//
// The binary (testdata/fakeaction, which Build compiles) runs the same code. Its behaviour comes from the "mode"
// key of its config, which an item's payload may override with its own "mode" key:
//   - echo: emits {"pid", "workdir", "in"} under the item's key, then done
//   - keyed: emits config "n" outputs keyed k0, k1… and a metric, then done
//   - crash: emits once, then exits with status 3 mid-delivery
//   - garbage: writes a line that is not JSON
//   - hang: never answers the item, and ignores shutdown
//   - stderr: writes two lines to stderr, then echoes
//   - error: answers error, retryable as config "retryable" says
//   - wrongId: emits for another delivery
//   - emitAfterDone: answers done, then emits for the same delivery
//   - slow: sleeps config "sleepMs" milliseconds, then echoes
//   - noReady: never answers hello
//   - child: starts a child process sleeping for a minute, logs "child <pid>" on stderr, then hangs
//   - noRead: answers ready, then never reads stdin again and hangs
//   - secret: writes config "secret" to stderr, then answers a non-retryable error whose message contains it
//   - lateMetric: answers done, then sends a metric for the same delivery
//   - cost: sends a cost_usd metric for each number of config "costs" and a metric of another name, then answers
//     done, or a non-retryable error when config "fail" is true
//   - rules: a rule-based action a fallback backs (spec section 11.5), its outputs deterministic so that audits
//     compare: run as an action whose name does not end in -fallback, it abstains on an item whose payload has
//     "abstain" true and otherwise emits {"score": the payload's "score", 1 by default}; as the fallback (its name
//     ends in -fallback) it emits the same, one more when the payload has "disagree" true, and answers a
//     non-retryable error when the payload has "fallbackError" true
//   - artifact: writes the payload's "files" (path to content) in MILLSTREAM_WORKDIR, creates its "links" (path to
//     target) as symbolic links, then emits, with the payload's "emit" list as artifacts, an output mapping each
//     input artifact's name to its content, and answers done with the payload's "attempt" list as the attempt's
//     artifacts, or error with them when the payload's "fail" is true
func Serve(ctx context.Context, _, env []string, stdin io.Reader, stdout, stderr io.Writer) int {
	f := &fake{ctx: ctx, pid: pidBase + int(fakePIDs.Add(1)), stdout: stdout, stderr: stderr, inProcess: true,
		getenv: func(k string) string { v, _ := EnvLookup(env)(k); return v }}
	return f.main(stdin)
}

// EnvLookup looks variables up in env, a process environment (KEY=value entries), as an in-process action sees it.
func EnvLookup(env []string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		for _, kv := range env {
			if v, ok := strings.CutPrefix(kv, k+"="); ok {
				return v, true
			}
		}
		return "", false
	}
}

// Main runs the fake action as the process it is: the binary's main.
func Main() int {
	f := &fake{ctx: context.Background(), pid: os.Getpid(), stdout: os.Stdout, stderr: os.Stderr, getenv: os.Getenv}
	return f.main(os.Stdin)
}

// fakePIDs numbers the in-process fakes, from pidBase, far above real pids.
var fakePIDs atomic.Int64

const pidBase = 1 << 30

// fake is one run of the fake action.
type fake struct {
	ctx       context.Context
	pid       int
	stdout    io.Writer
	stderr    io.Writer
	getenv    func(string) string
	inProcess bool
	action    string // hello's action name
}

// errExit carries an exit status out of a mode.
type errExit int

func (e errExit) Error() string { return fmt.Sprintf("exit %d", int(e)) }

func (f *fake) main(stdin io.Reader) int {
	err := f.run(stdin)
	var code errExit
	switch {
	case errors.As(err, &code):
		return int(code)
	case err != nil:
		_, _ = fmt.Fprintln(f.stderr, "fakeaction:", err)
		return 1
	}
	return 0
}
func (f *fake) run(stdin io.Reader) error {
	r := protocol.NewReader(stdin)
	w := protocol.NewWriter(f.stdout)
	m, err := r.Read()
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	hello, ok := m.(protocol.Hello)
	if !ok {
		return fmt.Errorf("first message is %s, want hello", m.Type())
	}
	mode, _ := hello.Config["mode"].(string)
	f.action = hello.Action
	if mode == "noReady" {
		return f.hang()
	}
	if err := w.Write(protocol.Ready{}); err != nil {
		return err
	}
	if mode == "noRead" {
		return f.hang()
	}
	for {
		m, err := r.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch m := m.(type) {
		case protocol.Shutdown:
			if mode == "hang" {
				return f.hang()
			}
			return nil
		case protocol.Item:
			if err := f.handle(w, hello.Config, mode, m); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected %s message", m.Type())
		}
	}
}

func (f *fake) handle(w *protocol.Writer, config map[string]any, mode string, it protocol.Item) error {
	var in map[string]any
	_ = json.Unmarshal(it.Payload, &in)
	if m, ok := in["mode"].(string); ok {
		mode = m
	}
	switch mode {
	case "keyed":
		n, _ := config["n"].(float64)
		for i := range int(n) {
			emit := protocol.Emit{Delivery: it.Delivery, Key: fmt.Sprintf("k%d", i), Payload: json.RawMessage(fmt.Sprint(i))}
			if err := w.Write(emit); err != nil {
				return err
			}
		}
		if err := w.Write(protocol.Metric{Delivery: it.Delivery, Name: "outputs", Value: n}); err != nil {
			return err
		}
		return w.Write(protocol.Done{Delivery: it.Delivery})
	case "crash":
		_ = w.Write(protocol.Emit{Delivery: it.Delivery, Payload: it.Payload})
		return errExit(3)
	case "garbage":
		_, err := fmt.Fprintln(f.stdout, "this is not json")
		return err
	case "hang":
		return f.hang()
	case "stderr":
		_, _ = fmt.Fprintln(f.stderr, "fetching page")
		_, _ = fmt.Fprintln(f.stderr, "parsed listing")
	case "error":
		retryable, _ := config["retryable"].(bool)
		return w.Write(protocol.Error{Delivery: it.Delivery, Message: "portal answered 503", Retryable: retryable})
	case "wrongId":
		return w.Write(protocol.Emit{Delivery: "dl_other", Payload: it.Payload})
	case "emitAfterDone":
		if err := w.Write(protocol.Done{Delivery: it.Delivery}); err != nil {
			return err
		}
		return w.Write(protocol.Emit{Delivery: it.Delivery, Payload: it.Payload})
	case "child":
		if f.inProcess {
			return errors.New("child mode needs a process")
		}
		child := osexec.Command("/bin/sleep", "60")
		if err := child.Start(); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(f.stderr, "child %d\n", child.Process.Pid)
		return f.hang()
	case "secret":
		secret, _ := config["secret"].(string)
		_, _ = fmt.Fprintln(f.stderr, "calling the portal with "+secret)
		return w.Write(protocol.Error{Delivery: it.Delivery, Message: "portal refused token " + secret})
	case "lateMetric":
		if err := w.Write(protocol.Done{Delivery: it.Delivery}); err != nil {
			return err
		}
		return w.Write(protocol.Metric{Delivery: it.Delivery, Name: "late", Value: 1})
	case "cost":
		costs, _ := config["costs"].([]any)
		for _, c := range costs {
			v, _ := c.(float64)
			if err := w.Write(protocol.Metric{Delivery: it.Delivery, Name: protocol.MetricCostUSD, Value: v}); err != nil {
				return err
			}
		}
		if err := w.Write(protocol.Metric{Delivery: it.Delivery, Name: "tokens_in", Value: 100}); err != nil {
			return err
		}
		if fail, _ := config["fail"].(bool); fail {
			return w.Write(protocol.Error{Delivery: it.Delivery, Message: "over"})
		}
		return w.Write(protocol.Done{Delivery: it.Delivery})
	case "artifact":
		return f.artifacts(w, it)
	case "rules":
		return f.rules(w, in, it)
	case "slow":
		ms, _ := config["sleepMs"].(float64)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
		case <-f.ctx.Done():
			return f.ctx.Err()
		}
	}
	return f.echo(w, it)
}

func (f *fake) echo(w *protocol.Writer, it protocol.Item) error {
	out, err := json.Marshal(map[string]any{
		"pid": f.pid, "workdir": f.getenv("MILLSTREAM_WORKDIR"), "in": it.Payload,
	})
	if err != nil {
		return err
	}
	if err := w.Write(protocol.Emit{Delivery: it.Delivery, Key: it.Key, Payload: out}); err != nil {
		return err
	}
	return w.Write(protocol.Done{Delivery: it.Delivery})
}

func (f *fake) rules(w *protocol.Writer, in map[string]any, it protocol.Item) error {
	fallback := strings.HasSuffix(f.action, "-fallback")
	score, ok := in["score"].(float64)
	if !ok {
		score = 1
	}
	switch {
	case !fallback && in["abstain"] == true:
		return w.Write(protocol.Abstain{Delivery: it.Delivery, Reason: "no rule for this item"})
	case fallback && in["fallbackError"] == true:
		return w.Write(protocol.Error{Delivery: it.Delivery, Message: "the fallback failed"})
	case fallback && in["disagree"] == true:
		score++
	}
	if err := w.Write(protocol.Emit{Delivery: it.Delivery, Key: it.Key, Payload: json.RawMessage(fmt.Sprintf(`{"score":%g}`, score))}); err != nil {
		return err
	}
	return w.Write(protocol.Done{Delivery: it.Delivery})
}

func (f *fake) artifacts(w *protocol.Writer, it protocol.Item) error {
	var in struct {
		Files   map[string]string   `json:"files"`
		Links   map[string]string   `json:"links"`
		Emit    []protocol.Artifact `json:"emit"`
		Attempt []protocol.Artifact `json:"attempt"`
		Fail    bool                `json:"fail"`
	}
	if err := json.Unmarshal(it.Payload, &in); err != nil {
		return err
	}
	dir := f.getenv("MILLSTREAM_WORKDIR")
	for p, content := range in.Files {
		full := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			return err
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			return err
		}
	}
	for p, target := range in.Links {
		if err := os.Symlink(target, filepath.Join(dir, filepath.FromSlash(p))); err != nil {
			return err
		}
	}
	inputs := map[string]string{}
	for _, a := range it.Artifacts {
		b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(a.Path))) // #nosec G304 -- the fake's own working directory
		if err != nil {
			return err
		}
		inputs[a.Name] = a.Path + "=" + string(b)
	}
	out, err := json.Marshal(inputs)
	if err != nil {
		return err
	}
	if err := w.Write(protocol.Emit{Delivery: it.Delivery, Payload: out, Artifacts: in.Emit}); err != nil {
		return err
	}
	if in.Fail {
		return w.Write(protocol.Error{Delivery: it.Delivery, Message: "failed", Retryable: true, Artifacts: in.Attempt})
	}
	return w.Write(protocol.Done{Delivery: it.Delivery, Artifacts: in.Attempt})
}

// hang blocks until the action is killed: for good in a process (the timer keeps the runtime's deadlock detector
// quiet), until ctx ends in-process.
func (f *fake) hang() error {
	select {
	case <-time.After(time.Hour):
	case <-f.ctx.Done():
	}
	return f.ctx.Err()
}
