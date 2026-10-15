// Package millstream is the Go SDK for Millstream actions: a thin wrapper over the action protocol (JSON objects,
// one per line, on stdin and stdout; see docs/protocol.md in the Millstream repository). An action's main calls Map
// or MapKeyed with a function from one input item to its outputs; the SDK answers hello, decodes each item into the
// function's input type, emits its outputs and reports done or error, one delivery at a time. Files travel as
// artifacts through MILLSTREAM_WORKDIR: Context.InputArtifact finds the input item's, Context.Artifact attaches
// files the action wrote to its outputs.
//
// The SDK has no dependency on the engine: it repeats the few message shapes it needs.
package millstream

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ProtocolVersion is the protocol version this SDK speaks.
const ProtocolVersion = 1

// maxLine is the longest line the protocol allows, newline excluded.
const maxLine = 8 << 20

// emitOverhead bounds what an emit message adds around its payload and key: type, delivery id, field names.
const emitOverhead = 256

// ErrProtocol is returned, wrapped, when the engine breaks the protocol.
var ErrProtocol = errors.New("protocol violation")

// Context is what an action function sees for one item.
type Context interface {
	// Config returns the step's config, interpolated by the engine for the run.
	Config() map[string]any
	// ConfigInto decodes the config into v, as encoding/json would.
	ConfigInto(v any) error
	// Vars returns the run's variables.
	Vars() map[string]any
	// Metric records a metric for the current delivery.
	Metric(name string, value float64, labels map[string]string)
	// Logger returns a JSON logger on stderr; the node re-emits each line with the delivery's keys.
	Logger() *slog.Logger
	// Item describes the item being processed.
	Item() ItemInfo
	// Artifact attaches the file at path to the delivery's outputs as an artifact called name: with Map, to its
	// output; with MapKeyed, to every output of the delivery. path is relative to MILLSTREAM_WORKDIR or an
	// absolute path inside it, and must be a regular file there; names are file names, distinct within a delivery.
	// The node moves the file to the artifact store when the delivery is done; a delivery without outputs drops
	// its artifacts.
	Artifact(name, path string) error
	// InputArtifact returns the absolute path of the input item's artifact called name, placed in the working
	// directory before the item arrived.
	InputArtifact(name string) (string, error)
}

// ItemInfo describes the item being processed.
type ItemInfo struct {
	ID      string
	Key     string
	Attempt int
	// IdempotencyKey is stable across retries of the delivery and new on replay: pass it to external systems that
	// deduplicate requests.
	IdempotencyKey string
}

// Keyed is one keyed output. A delivery with more than one output must key all of them, with distinct keys.
type Keyed[T any] struct {
	Key   string
	Value T
}

// Handler processes one item's payload into keyed JSON outputs. Build one with MapHandler or MapKeyedHandler.
type Handler func(c Context, payload json.RawMessage) ([]Keyed[json.RawMessage], error)

// Map runs an action that turns each input into zero or one output, on the process's stdin and stdout, and returns
// when the engine shuts it down. On a protocol or I/O failure it logs the error and exits with status 1.
func Map[In, Out any](fn func(Context, In) ([]Out, error)) { run(MapHandler(fn)) }

// MapKeyed runs an action that turns each input into any number of keyed outputs; see Map.
func MapKeyed[In, Out any](fn func(Context, In) ([]Keyed[Out], error)) { run(MapKeyedHandler(fn)) }

// MapHandler adapts a Map function to a Handler. More than one output is an error: several outputs need keys,
// which MapKeyed provides.
func MapHandler[In, Out any](fn func(Context, In) ([]Out, error)) Handler {
	return MapKeyedHandler(func(c Context, in In) ([]Keyed[Out], error) {
		outs, err := fn(c, in)
		if err != nil {
			return nil, err
		}
		if len(outs) > 1 {
			return nil, fmt.Errorf("Map returned %d outputs: use MapKeyed to emit several keyed outputs", len(outs))
		}
		keyed := make([]Keyed[Out], len(outs))
		for i, o := range outs {
			keyed[i].Value = o
		}
		return keyed, nil
	})
}

// MapKeyedHandler adapts a MapKeyed function to a Handler.
func MapKeyedHandler[In, Out any](fn func(Context, In) ([]Keyed[Out], error)) Handler {
	return func(c Context, payload json.RawMessage) ([]Keyed[json.RawMessage], error) {
		var in In
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, fmt.Errorf("decode input: %w", err)
		}
		outs, err := fn(c, in)
		if err != nil {
			return nil, err
		}
		if err := keyRule(outs); err != nil {
			return nil, err
		}
		encoded := make([]Keyed[json.RawMessage], len(outs))
		for i, o := range outs {
			b, err := json.Marshal(o.Value)
			if err != nil {
				return nil, fmt.Errorf("encode output %d: %w", i, err)
			}
			if len(b)+len(o.Key)+emitOverhead > maxLine {
				return nil, fmt.Errorf("output %d is %d bytes: a protocol line holds at most %d; write large data as an "+
					"artifact under MILLSTREAM_WORKDIR", i, len(b), maxLine)
			}
			encoded[i] = Keyed[json.RawMessage]{Key: o.Key, Value: b}
		}
		return encoded, nil
	}
}

// keyRule enforces the engine's key rule (docs/protocol.md): several outputs must all be keyed, with distinct
// keys. The engine dead-letters a delivery that breaks it, so the SDK reports it as a non-retryable error first.
func keyRule[Out any](outs []Keyed[Out]) error {
	if len(outs) < 2 {
		return nil
	}
	seen := make(map[string]int, len(outs))
	for i, o := range outs {
		if o.Key == "" {
			return fmt.Errorf("output %d of %d has no key: a delivery with several outputs must key them all", i, len(outs))
		}
		if j, dup := seen[o.Key]; dup {
			return fmt.Errorf("outputs %d and %d share the key %q: a delivery's output keys must be distinct", j, i, o.Key)
		}
		seen[o.Key] = i
	}
	return nil
}

func run(h Handler) {
	if err := RunWithIO(os.Stdin, os.Stdout, h); err != nil {
		newLogger().Error("action stopped", "error", err.Error())
		os.Exit(1)
	}
}

// retryable marks an error the engine should retry.
type retryable struct{ err error }

func (r retryable) Error() string { return r.err.Error() }
func (r retryable) Unwrap() error { return r.err }

// Retryable marks err as retryable: the engine retries the delivery with backoff instead of dead-lettering it.
// Retryable(nil) is nil.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return retryable{err}
}

// IsRetryable reports whether err, or an error it wraps, was marked with Retryable.
func IsRetryable(err error) bool { return errors.As(err, new(retryable)) }

// Messages. Unknown fields are ignored, so an engine speaking a later minor revision still works.
type (
	artifactMsg struct {
		Name string `json:"name"`
		Path string `json:"path"`
	}
	inMsg struct {
		Type           string          `json:"type"`
		Protocol       int             `json:"protocol"`
		Config         map[string]any  `json:"config"`
		Vars           map[string]any  `json:"vars"`
		Delivery       string          `json:"delivery"`
		ID             string          `json:"id"`
		Key            string          `json:"key"`
		Payload        json.RawMessage `json:"payload"`
		Attempt        int             `json:"attempt"`
		IdempotencyKey string          `json:"idempotencyKey"`
		Artifacts      []artifactMsg   `json:"artifacts"`
	}
	readyMsg struct {
		Type string `json:"type"`
	}
	emitMsg struct {
		Type      string          `json:"type"`
		Delivery  string          `json:"delivery"`
		Key       string          `json:"key,omitempty"`
		Payload   json.RawMessage `json:"payload"`
		Artifacts []artifactMsg   `json:"artifacts,omitempty"`
	}
	metricMsg struct {
		Type     string            `json:"type"`
		Delivery string            `json:"delivery"`
		Name     string            `json:"name"`
		Value    float64           `json:"value"`
		Labels   map[string]string `json:"labels,omitempty"`
	}
	doneMsg struct {
		Type     string `json:"type"`
		Delivery string `json:"delivery"`
	}
	errorMsg struct {
		Type      string `json:"type"`
		Delivery  string `json:"delivery"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	}
)

// writer writes one message per line, each in a single write. Metric may be called from the action's own
// goroutines, hence the lock.
type writer struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *writer) send(m any) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.w.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	return nil
}

// RunWithIO runs the protocol loop over r and w with h: it reads hello, answers ready, then handles each item,
// one at a time, until shutdown or the end of r, which return nil. A broken protocol or a failed write is an
// error; a failed item is not, it is reported to the engine with an error message.
func RunWithIO(r io.Reader, w io.Writer, h Handler) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxLine+1)
	out := &writer{w: w}
	logger := newLogger()

	hello, err := read(sc)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: no hello", ErrProtocol)
		}
		return err
	}
	if hello.Type != "hello" {
		return fmt.Errorf("%w: want hello first, got %q", ErrProtocol, hello.Type)
	}
	if hello.Protocol != ProtocolVersion {
		return fmt.Errorf("%w: protocol %d is not supported, this SDK speaks %d", ErrProtocol, hello.Protocol,
			ProtocolVersion)
	}
	if err := out.send(readyMsg{Type: "ready"}); err != nil {
		return err
	}
	for {
		m, err := read(sc)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch m.Type {
		case "shutdown":
			return nil
		case "item":
			if err := handle(out, logger, hello, m, h); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unexpected message %q", ErrProtocol, m.Type)
		}
	}
}

// read reads one message; io.EOF at the end of input.
func read(sc *bufio.Scanner) (inMsg, error) {
	var m inMsg
	if !sc.Scan() {
		if errors.Is(sc.Err(), bufio.ErrTooLong) {
			return m, fmt.Errorf("%w: line longer than %d bytes", ErrProtocol, maxLine)
		}
		if sc.Err() != nil {
			return m, fmt.Errorf("read message: %w", sc.Err())
		}
		return m, io.EOF
	}
	line := sc.Bytes()
	if err := json.Unmarshal(line, &m); err != nil {
		return m, fmt.Errorf("%w: malformed line: %w", ErrProtocol, err)
	}
	return m, nil
}

// handle processes one item and reports its outcome.
func handle(out *writer, logger *slog.Logger, hello, item inMsg, h Handler) error {
	c := &itemContext{hello: hello, item: item, out: out, logger: logger, workdir: os.Getenv("MILLSTREAM_WORKDIR")}
	outs, err := h(c, item.Payload)
	if err != nil {
		return out.send(errorMsg{Type: "error", Delivery: item.Delivery, Message: err.Error(), Retryable: IsRetryable(err)})
	}
	for _, o := range outs {
		emit := emitMsg{Type: "emit", Delivery: item.Delivery, Key: o.Key, Payload: o.Value, Artifacts: c.artifacts}
		if err := out.send(emit); err != nil {
			return err
		}
	}
	return out.send(doneMsg{Type: "done", Delivery: item.Delivery})
}

func newLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(os.Stderr, nil)) }

// itemContext is the Context of one item.
type itemContext struct {
	hello, item inMsg
	out         *writer
	logger      *slog.Logger
	workdir     string        // MILLSTREAM_WORKDIR
	artifacts   []artifactMsg // attached with Artifact
}

func (c *itemContext) Config() map[string]any { return orEmpty(c.hello.Config) }

func (c *itemContext) ConfigInto(v any) error {
	b, err := json.Marshal(c.Config())
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	return nil
}

func (c *itemContext) Vars() map[string]any { return orEmpty(c.hello.Vars) }

func (c *itemContext) Metric(name string, value float64, labels map[string]string) {
	err := c.out.send(metricMsg{Type: "metric", Delivery: c.item.Delivery, Name: name, Value: value, Labels: labels})
	if err != nil {
		c.logger.Error("send metric", "metric", name, "error", err.Error())
	}
}

func (c *itemContext) Logger() *slog.Logger { return c.logger }

func (c *itemContext) Item() ItemInfo {
	return ItemInfo{ID: c.item.ID, Key: c.item.Key, Attempt: c.item.Attempt, IdempotencyKey: c.item.IdempotencyKey}
}

func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func (c *itemContext) Artifact(name, path string) error {
	if name == "" || len(name) > 255 || name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") {
		return fmt.Errorf("artifact name %q: want a file name of at most 255 bytes without a path separator", name)
	}
	for _, a := range c.artifacts {
		if a.Name == name {
			return fmt.Errorf("artifact %s is already attached", name)
		}
	}
	if c.workdir == "" {
		return errors.New("artifact " + name + ": MILLSTREAM_WORKDIR is not set")
	}
	rel := path
	if filepath.IsAbs(path) {
		var err error
		if rel, err = filepath.Rel(c.workdir, path); err != nil {
			return fmt.Errorf("artifact %s: %w", name, err)
		}
	}
	if !filepath.IsLocal(rel) {
		return fmt.Errorf("artifact %s: %s is not inside MILLSTREAM_WORKDIR", name, path)
	}
	fi, err := os.Lstat(filepath.Join(c.workdir, rel))
	if err != nil {
		return fmt.Errorf("artifact %s: %w", name, err)
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("artifact %s: %s is not a regular file", name, path)
	}
	c.artifacts = append(c.artifacts, artifactMsg{Name: name, Path: filepath.ToSlash(rel)})
	return nil
}

func (c *itemContext) InputArtifact(name string) (string, error) {
	for _, a := range c.item.Artifacts {
		if a.Name == name {
			return filepath.Join(c.workdir, filepath.FromSlash(a.Path)), nil
		}
	}
	return "", fmt.Errorf("the item has no artifact %s", name)
}
