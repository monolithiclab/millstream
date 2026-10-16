package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/ui"
)

// shutdownTimeout bounds how long run waits for the API's requests to end once it stops; live streams end at once.
const shutdownTimeout = 5 * time.Second

// The server's timeouts: reading a request (live streams lift it once open, api.startSSE) and an idle keep-alive
// connection.
const (
	readTimeout = 30 * time.Second
	idleTimeout = 2 * time.Minute
)

// apiServer serves the API and the web UI of millstream run (spec section 12), with the broker of its live streams.
type apiServer struct {
	st     store.Store
	http   *http.Server
	broker *api.Broker
	ln     net.Listener
	url    string
	log    *slog.Logger
}

// server builds run's API server over the store and the engine: the broker wakes on the store's commits.
func (c RunPackagesCmd) server(rt *Runtime, env *pipeline.Environment, st store.Store, e *engine.Engine, notes *review.Notifier,
	logs *api.LogRing, embedded api.Node, log *slog.Logger,
) (*apiServer, error) {
	b, err := api.NewBroker(api.BrokerOptions{Store: st, Logger: log})
	if err != nil {
		return nil, err
	}
	a, err := api.New(api.Options{Store: st, Engine: e, Artifacts: artifactStore(rt, env), Broker: b, Logs: logs,
		Nodes: []api.Node{embedded}, Version: rt.Version, User: identity(rt), Logger: log,
		NotificationFailures: notes.Failures})
	if err != nil {
		return nil, err
	}
	return &apiServer{st: st, broker: b, log: log, http: &http.Server{Handler: api.Handler(a, ui.Handler()),
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: readTimeout, IdleTimeout: idleTimeout}}, nil
}

// listen binds the address and starts the broker at the end of the event log, so a client connecting at once sees
// every later event.
func (s *apiServer) listen(ctx context.Context, addr string) error {
	if err := s.broker.Start(ctx); err != nil {
		return err
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	s.ln, s.url = ln, "http://"+ln.Addr().String()+"/"
	return nil
}

// serve serves until ctx ends, then shuts the server down: the broker ends the live streams, the other requests get
// shutdownTimeout to finish.
func (s *apiServer) serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.st.OnCommit(s.broker.Notify)
	defer s.st.OnCommit(nil)
	brokerDone := make(chan error, 1)
	go func() { brokerDone <- s.broker.Run(ctx) }()
	s.http.BaseContext = func(net.Listener) context.Context { return ctx }
	served := make(chan error, 1)
	go func() { served <- s.http.Serve(s.ln) }()
	var err error
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		err = s.http.Shutdown(shutdown)
		<-served
	case err = <-served:
		cancel()
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return errors.Join(err, <-brokerDone)
}
