package improve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/protocol"
)

// Options configure an action process.
type Options struct {
	// Client sends the API requests; nil means a client with a minute's timeout.
	Client *http.Client
	// Logger receives the action's logs (stderr, which the node relays); nil discards them.
	Logger *slog.Logger
	// Publishers are the publishers publish may hand accepted proposals to, by improve.publish's name; nil means
	// those registered (RegisterPublisher).
	Publishers map[string]Publisher
	// Poll is how often validate polls a backtest; zero means a second.
	Poll time.Duration
}

// config is the steps' config: the API and its token, and each step's settings.
type config struct {
	API              string  `json:"api"`
	Token            string  `json:"token"`
	Pipeline         string  `json:"pipeline"`
	LessonsMaxTokens int     `json:"lessonsMaxTokens"`
	MaxFalseDrop     float64 `json:"maxFalseDrop"`
	MinSeverity      string  `json:"minSeverity"`
	Publisher        string  `json:"publisher"`
}

// output is an output of a step, keyed.
type output struct {
	Key     string
	Payload any
}

// handler handles one item of a step: its outputs, or an error that fails the delivery.
type handler func(ctx context.Context, a *action, it protocol.Item) ([]output, error)

var handlers = map[string]handler{
	"gather-evidence": gather,
	"validate":        validate,
	"propose":         propose,
	"publish":         publish,
}

// action is the state of one action process.
type action struct {
	opts Options
	cfg  config
	api  *client
	log  *slog.Logger
}

// Serve serves step on in and out, the action protocol, until shutdown or the end of input. An error of the API
// fails the delivery as retryable, one of the item itself as permanent.
func Serve(ctx context.Context, step string, in io.Reader, out io.Writer, opts Options) error {
	h, ok := handlers[step]
	if !ok {
		return fmt.Errorf("improve: no step %q: want one of %s", step, strings.Join(slices.Sorted(maps.Keys(handlers)), ", "))
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Client == nil {
		opts.Client = &http.Client{Timeout: time.Minute}
	}
	if opts.Poll <= 0 {
		opts.Poll = time.Second
	}
	r, w := protocol.NewReader(in), protocol.NewWriter(out)
	m, err := r.Read()
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	hello, ok := m.(protocol.Hello)
	if !ok {
		return fmt.Errorf("%w: first message is %s, want hello", protocol.ErrProtocol, m.Type())
	}
	a := &action{opts: opts, log: opts.Logger}
	setupErr := a.setup(hello)
	if err := w.Write(protocol.Ready{}); err != nil {
		return err
	}
	for {
		m, err := r.Read()
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		}
		switch m := m.(type) {
		case protocol.Shutdown:
			return nil
		case protocol.Item:
			if setupErr != nil {
				err = w.Write(protocol.Error{Delivery: m.Delivery, Message: setupErr.Error()})
			} else {
				err = a.answer(ctx, w, h, m)
			}
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unexpected %s message", protocol.ErrProtocol, m.Type())
		}
	}
}

// setup reads the config from hello.
func (a *action) setup(hello protocol.Hello) error {
	raw, err := json.Marshal(hello.Config)
	if err == nil {
		err = json.Unmarshal(raw, &a.cfg)
	}
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if a.cfg.API == "" || a.cfg.Token == "" {
		return errors.New("config: api and token are required: the engine sets the improveApi and improveToken " +
			"secrets when it runs the improve pipeline")
	}
	a.api = &client{base: strings.TrimSuffix(a.cfg.API, "/") + api.Prefix, token: a.cfg.Token, http: a.opts.Client}
	return nil
}

// answer runs the handler on one item and writes its outputs and done, or its error.
func (a *action) answer(ctx context.Context, w *protocol.Writer, h handler, it protocol.Item) error {
	outs, err := h(ctx, a, it)
	if err != nil {
		var ae *apiError
		retryable := errors.As(err, &ae) && ae.retryable()
		return w.Write(protocol.Error{Delivery: it.Delivery, Message: err.Error(), Retryable: retryable})
	}
	for _, o := range outs {
		payload, err := json.Marshal(o.Payload)
		if err != nil {
			return w.Write(protocol.Error{Delivery: it.Delivery, Message: err.Error()})
		}
		if err := w.Write(protocol.Emit{Delivery: it.Delivery, Key: o.Key, Payload: payload}); err != nil {
			return err
		}
	}
	return w.Write(protocol.Done{Delivery: it.Delivery})
}

// decode decodes an item's payload into v.
func decode(it protocol.Item, v any) error {
	if err := json.Unmarshal(it.Payload, v); err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	return nil
}
