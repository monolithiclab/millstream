// Package protocol is the action protocol (spec section 6.3, docs/protocol.md): JSON objects, one per line, on an
// action process's stdin (engine to action) and stdout (action to engine). Every message carries its kind in
// "type"; keys are lowerCamelCase. A line longer than MaxLine, a line that is not a JSON object, and an unknown
// type are protocol violations (ErrProtocol), on which the engine kills the process and retries the delivery.
// Unknown keys are ignored, so either side may add optional fields in a later revision of protocol 1.
package protocol

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Version is the protocol version the engine announces in hello.
const Version = 1

// MaxLine is the longest line either side may write, newline excluded: 8 MiB.
const MaxLine = 8 << 20

// ErrProtocol is returned, wrapped, for a protocol violation.
var ErrProtocol = errors.New("protocol violation")

// Message types.
const (
	TypeHello    = "hello"
	TypeItem     = "item"
	TypeShutdown = "shutdown"
	TypeReady    = "ready"
	TypeEmit     = "emit"
	TypeMetric   = "metric"
	TypeDone     = "done"
	TypeError    = "error"
)

// Message is one protocol message.
type Message interface {
	// Type is the message's "type" value.
	Type() string
}

// Hello opens a process: the action, its config and the run's vars, and for an agent step its lessons (spec
// section 11.4) as JSON, rules and examples. A process serves only deliveries with the same action digest, config,
// vars and lessons as its hello.
type Hello struct {
	Protocol int             `json:"protocol"`
	Action   string          `json:"action"`
	Version  string          `json:"version"`
	Config   map[string]any  `json:"config"`
	Vars     map[string]any  `json:"vars"`
	Lessons  json.RawMessage `json:"lessons,omitempty"`
}

// Item hands the process one delivery.
type Item struct {
	Delivery  string          `json:"delivery"`
	ID        string          `json:"id"`
	Key       string          `json:"key"`
	Payload   json.RawMessage `json:"payload"`
	Artifacts []Artifact      `json:"artifacts"`
	Run       Run             `json:"run"`
	Attempt   int             `json:"attempt"`
	// IdempotencyKey is hash(step, item id, generation): stable across retries, new on replay.
	IdempotencyKey string `json:"idempotencyKey"`
}

// Run is the run a delivery belongs to.
type Run struct {
	ID        string    `json:"id"`
	Trigger   string    `json:"trigger"`
	StartedAt time.Time `json:"startedAt"`
	Revision  string    `json:"revision"`
}

// Artifact is a file under the process's working directory (MILLSTREAM_WORKDIR), by path relative to it.
type Artifact struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// Shutdown asks the process to exit once it is idle.
type Shutdown struct{}

// Ready answers hello: the process accepts items.
type Ready struct{}

// Emit stages one output of a delivery; outputs are published when the delivery is done.
type Emit struct {
	Delivery  string          `json:"delivery"`
	Key       string          `json:"key,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Artifacts []Artifact      `json:"artifacts,omitempty"`
}

// MetricCostUSD is the metric an action reports what an attempt spent with, in US dollars: the attempt's cost is
// the sum of its cost_usd metrics, and it settles the delivery's budget reservation (spec section 5.9).
const MetricCostUSD = "cost_usd"

// Metric records a measurement for a delivery.
type Metric struct {
	Delivery string            `json:"delivery"`
	Name     string            `json:"name"`
	Value    float64           `json:"value"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// Done ends a delivery successfully: its staged outputs are published. Artifacts are files of the attempt itself,
// such as an agent's transcript: kept with the delivery's attempt, never carried by its outputs.
type Done struct {
	Delivery  string     `json:"delivery"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
}

// Error ends a delivery in failure; Retryable asks for a retry with backoff instead of a dead letter. Artifacts are
// files of the failed attempt, kept with it as on done, so a failure can be investigated.
type Error struct {
	Delivery  string     `json:"delivery"`
	Message   string     `json:"message"`
	Retryable bool       `json:"retryable"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
}

func (Hello) Type() string    { return TypeHello }
func (Item) Type() string     { return TypeItem }
func (Shutdown) Type() string { return TypeShutdown }
func (Ready) Type() string    { return TypeReady }
func (Emit) Type() string     { return TypeEmit }
func (Metric) Type() string   { return TypeMetric }
func (Done) Type() string     { return TypeDone }
func (Error) Type() string    { return TypeError }

// decoders decode a line into the message of each type.
var decoders = map[string]func([]byte) (Message, error){
	TypeHello:    decode[Hello],
	TypeItem:     decode[Item],
	TypeShutdown: decode[Shutdown],
	TypeReady:    decode[Ready],
	TypeEmit:     decode[Emit],
	TypeMetric:   decode[Metric],
	TypeDone:     decode[Done],
	TypeError:    decode[Error],
}

func decode[T Message](line []byte) (Message, error) {
	var m T
	err := json.Unmarshal(line, &m)
	return m, err
}

// Reader reads messages, one per line.
type Reader struct {
	sc      *bufio.Scanner
	maxLine int // the longest line: MaxLine, but in tests
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader { return newReader(r, MaxLine) }

func newReader(r io.Reader, maxLine int) *Reader {
	sc := bufio.NewScanner(r)
	// +1: the scanner counts the newline against the limit.
	sc.Buffer(make([]byte, 0, min(64<<10, maxLine+1)), maxLine+1)
	return &Reader{sc: sc, maxLine: maxLine}
}

// Read returns the next message as a value (Hello, Item, ...), or io.EOF at the end of input. A line over MaxLine,
// a line that is not a JSON object, and a missing or unknown type are errors wrapping ErrProtocol; after one, the
// stream cannot be trusted and should be abandoned.
func (r *Reader) Read() (Message, error) {
	if !r.sc.Scan() {
		switch err := r.sc.Err(); {
		case errors.Is(err, bufio.ErrTooLong):
			return nil, fmt.Errorf("%w: line longer than %d bytes", ErrProtocol, r.maxLine)
		case err != nil:
			return nil, fmt.Errorf("read message: %w", err)
		default:
			return nil, io.EOF
		}
	}
	line := r.sc.Bytes()
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &head); err != nil {
		return nil, fmt.Errorf("%w: malformed line: %w", ErrProtocol, err)
	}
	dec, ok := decoders[head.Type]
	if !ok {
		return nil, fmt.Errorf("%w: unknown message type %q", ErrProtocol, head.Type)
	}
	m, err := dec(line)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed %s message: %w", ErrProtocol, head.Type, err)
	}
	return m, nil
}

// Writer writes messages, one per line, each in a single unbuffered write. It is safe for concurrent use.
type Writer struct {
	mu      sync.Mutex
	w       io.Writer
	maxLine int // the longest line: MaxLine, but in tests
}

// NewWriter returns a Writer over w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w, maxLine: MaxLine} }

// Write encodes m with its type as one line and writes it. A message that would exceed MaxLine is refused with
// ErrProtocol and nothing is written.
func (w *Writer) Write(m Message) error {
	body, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode %s message: %w", m.Type(), err)
	}
	// Types are fixed identifiers, so they need no escaping.
	line := make([]byte, 0, len(body)+len(m.Type())+12)
	line = append(line, `{"type":"`+m.Type()+`"`...)
	if len(body) > 2 { // body is a JSON object: splice its fields after the type
		line = append(line, ',')
		line = append(line, body[1:]...)
	} else {
		line = append(line, '}')
	}
	if len(line) > w.maxLine {
		return fmt.Errorf("%w: %s message of %d bytes exceeds %d", ErrProtocol, m.Type(), len(line), w.maxLine)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.w.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write %s message: %w", m.Type(), err)
	}
	return nil
}
