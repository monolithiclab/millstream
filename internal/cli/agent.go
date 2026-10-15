package cli

import (
	"log/slog"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/pipeline"
)

// AgentCmd is "millstream agent", hidden: the action process the engine starts for agent steps (spec section 7.1).
// It speaks the action protocol on stdin and stdout; its logs go to stderr, which the node relays.
type AgentCmd struct {
	Package  string   `required:"" help:"The package directory."`
	Action   string   `required:"" help:"The step's reference to the agent action (./actions/x)."`
	Secret   []string `help:"A pipeline secret's name; its value is read from MILLSTREAM_SECRET_<NAME>."`
	PriceIn  *float64 `name:"price-in" help:"The model's input price per million tokens, from the environment file."`
	PriceOut *float64 `name:"price-out" help:"The model's output price per million tokens, from the environment file."`
}

// Run serves the protocol until shutdown.
func (c AgentCmd) Run(rt *Runtime) error {
	opts := agent.Options{
		Package: c.Package, Action: c.Action, Secrets: c.Secret, Env: rt.Env,
		Logger: slog.New(slog.NewTextHandler(rt.IO.Err, nil)),
	}
	if c.PriceIn != nil && c.PriceOut != nil {
		opts.Price = &pipeline.Price{InPerMtok: *c.PriceIn, OutPerMtok: *c.PriceOut}
	}
	return agent.Run(rt.Ctx, rt.IO.In, rt.IO.Out, opts)
}
