package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
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

// apiServer is the API and the web UI of millstream run and serve over one store (spec section 12), with the broker
// of its live streams. A site serves it.
type apiServer struct {
	st      store.Store
	handler http.Handler
	broker  *api.Broker
}

// newAPIServer builds the API server of run and serve over the store and the engine: the broker wakes on the store's
// commits. lead is who the engine is on a deployment with a standby (zero for run).
func newAPIServer(rt *Runtime, env *pipeline.Environment, st store.Store, e *engine.Engine, notes *review.Notifier,
	logs *api.LogRing, nodes []api.Node, lead leadership, log *slog.Logger,
) (*apiServer, error) {
	b, err := api.NewBroker(api.BrokerOptions{Store: st, Logger: log})
	if err != nil {
		return nil, err
	}
	a, err := api.New(api.Options{Store: st, Engine: e, Artifacts: artifactStore(rt, env), Broker: b, Logs: logs,
		Nodes: nodes, Version: rt.Version, User: identity(rt), Logger: log, Identity: lead.identity,
		Deployment: lead.deployment, NotificationFailures: notes.Failures})
	if err != nil {
		return nil, err
	}
	return &apiServer{st: st, broker: b, handler: api.Handler(a, ui.Handler())}, nil
}

// start starts the broker at the end of the event log, so a client connecting once the server is set on its site
// sees every later event.
func (s *apiServer) start(ctx context.Context) error { return s.broker.Start(ctx) }

// run feeds the live streams until ctx ends, which ends them.
func (s *apiServer) run(ctx context.Context) error {
	s.st.OnCommit(s.broker.Notify)
	defer s.st.OnCommit(nil)
	return s.broker.Run(ctx)
}

// site is the listener run and serve answer on, with the handler of the engine's current term: serve sets the
// active engine's API or the standby's on the same address across a failover. Between terms it answers
// unavailable.
type site struct {
	http *http.Server
	ln   net.Listener
	addr string // the address bound
	url  string
	term atomic.Pointer[term]
	// announce is called once, when the site first serves a handler: run and serve say where they serve then.
	announce func()
	once     sync.Once
}

// term is a handler and the context of the term it serves: when that ends, so do its requests (live streams).
type term struct {
	ctx context.Context
	h   http.Handler
}

// listenSite binds the address; announce, when set, is the site's.
func listenSite(ctx context.Context, addr string, announce func()) (*site, error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}
	s := &site{ln: ln, addr: ln.Addr().String(), url: "http://" + ln.Addr().String() + "/", announce: announce}
	s.http = &http.Server{Handler: http.HandlerFunc(s.serveHTTP), ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout: readTimeout, IdleTimeout: idleTimeout}
	return s, nil
}

// set makes h the handler until ctx ends, when the site answers unavailable again unless another term set its own.
func (s *site) set(ctx context.Context, h http.Handler) {
	t := &term{ctx: ctx, h: h}
	s.term.Store(t)
	if s.announce != nil {
		s.once.Do(s.announce)
	}
	context.AfterFunc(ctx, func() { s.term.CompareAndSwap(t, nil) })
}

func (s *site) serveHTTP(w http.ResponseWriter, r *http.Request) {
	t := s.term.Load()
	if t == nil || t.ctx.Err() != nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "the engine is starting or switching over: retry in a moment", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer context.AfterFunc(t.ctx, cancel)()
	t.h.ServeHTTP(w, r.WithContext(ctx))
}

// serve serves until ctx ends, then shuts the server down: the terms' brokers end the live streams, the other
// requests get shutdownTimeout to finish.
func (s *site) serve(ctx context.Context) error {
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
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
