// Command fakeaction is an action process for the tests of the exec runtime and the node. Its behaviour comes from
// the "mode" key of its config, which an item's payload may override with its own "mode" key:
//
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
//   - artifact: writes the payload's "files" (path to content) in MILLSTREAM_WORKDIR, creates its "links" (path to
//     target) as symbolic links, then emits, with the payload's "emit" list as artifacts, an output mapping each
//     input artifact's name to its content, and answers done with the payload's "attempt" list as the attempt's
//     artifacts, or error with them when the payload's "fail" is true
//
// It lives under testdata so the module's ./... patterns skip it; exectest builds it.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fakeaction:", err)
		os.Exit(1)
	}
}

func run() error {
	r := protocol.NewReader(os.Stdin)
	w := protocol.NewWriter(os.Stdout)
	m, err := r.Read()
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	hello, ok := m.(protocol.Hello)
	if !ok {
		return fmt.Errorf("first message is %s, want hello", m.Type())
	}
	mode, _ := hello.Config["mode"].(string)
	if mode == "noReady" {
		hang()
	}
	if err := w.Write(protocol.Ready{}); err != nil {
		return err
	}
	if mode == "noRead" {
		hang()
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
				hang()
			}
			return nil
		case protocol.Item:
			if err := handle(w, hello.Config, mode, m); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected %s message", m.Type())
		}
	}
}

func handle(w *protocol.Writer, config map[string]any, mode string, it protocol.Item) error {
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
		os.Exit(3)
	case "garbage":
		_, err := fmt.Fprintln(os.Stdout, "this is not json")
		return err
	case "hang":
		hang()
	case "stderr":
		fmt.Fprintln(os.Stderr, "fetching page")
		fmt.Fprintln(os.Stderr, "parsed listing")
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
		child := exec.Command("/bin/sleep", "60")
		if err := child.Start(); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "child %d\n", child.Process.Pid)
		hang()
	case "secret":
		secret, _ := config["secret"].(string)
		fmt.Fprintln(os.Stderr, "calling the portal with "+secret)
		return w.Write(protocol.Error{Delivery: it.Delivery, Message: "portal refused token " + secret})
	case "lateMetric":
		if err := w.Write(protocol.Done{Delivery: it.Delivery}); err != nil {
			return err
		}
		return w.Write(protocol.Metric{Delivery: it.Delivery, Name: "late", Value: 1})
	case "artifact":
		return artifacts(w, it)
	case "slow":
		ms, _ := config["sleepMs"].(float64)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	return echo(w, it)
}

func echo(w *protocol.Writer, it protocol.Item) error {
	out, err := json.Marshal(map[string]any{
		"pid": os.Getpid(), "workdir": os.Getenv("MILLSTREAM_WORKDIR"), "in": it.Payload,
	})
	if err != nil {
		return err
	}
	if err := w.Write(protocol.Emit{Delivery: it.Delivery, Key: it.Key, Payload: out}); err != nil {
		return err
	}
	return w.Write(protocol.Done{Delivery: it.Delivery})
}

func artifacts(w *protocol.Writer, it protocol.Item) error {
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
	dir := os.Getenv("MILLSTREAM_WORKDIR")
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
		b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(a.Path)))
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

// hang blocks for good; a bare select{} would trip the runtime's deadlock detector and exit.
func hang() { time.Sleep(time.Hour) }
