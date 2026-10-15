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
//
// It lives under testdata so the module's ./... patterns skip it; exectest builds it.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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

// hang blocks for good; a bare select{} would trip the runtime's deadlock detector and exit.
func hang() { time.Sleep(time.Hour) }
