package cli

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
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
	tls    *tls.Config // nil serves plain HTTP
	ln     net.Listener
	url    string
	log    *slog.Logger
}

// newAPIServer builds the API server of run and serve over the store and the engine: the broker wakes on the store's
// commits; sec turns token authentication on and serves HTTPS.
func newAPIServer(rt *Runtime, env *pipeline.Environment, st store.Store, e *engine.Engine, notes *review.Notifier,
	logs *api.LogRing, nodes []api.Node, log *slog.Logger, sec apiSecurity,
) (*apiServer, error) {
	b, err := api.NewBroker(api.BrokerOptions{Store: st, Logger: log})
	if err != nil {
		return nil, err
	}
	a, err := api.New(api.Options{Store: st, Engine: e, Artifacts: artifactStore(rt, env), Broker: b, Logs: logs,
		Nodes: nodes, Version: rt.Version, User: identity(rt), Auth: sec.auth, Logger: log,
		NotificationFailures: notes.Failures})
	if err != nil {
		return nil, err
	}
	return &apiServer{st: st, broker: b, tls: sec.tls, log: log, http: &http.Server{Handler: api.Handler(a, ui.Handler()),
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: readTimeout, IdleTimeout: idleTimeout}}, nil
}

// apiSecurity is what the API server reads from the environment, auth.token and tls: run and serve read it before
// they open anything, so that a token or a certificate they cannot read fails at once.
type apiSecurity struct {
	auth *api.Auth   // nil without auth.token
	tls  *tls.Config // nil without tls
}

func newAPISecurity(rt *Runtime, env *pipeline.Environment, listen string) (apiSecurity, error) {
	auth, err := apiAuth(rt, env, listen)
	if err != nil {
		return apiSecurity{}, err
	}
	tc, err := apiTLS(env)
	return apiSecurity{auth: auth, tls: tc}, err
}

// apiTLS is the API server's TLS configuration from tls, nil without it.
func apiTLS(env *pipeline.Environment) (*tls.Config, error) {
	if env.TLS == nil {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(env.TLS.Cert, env.TLS.Key)
	if err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// apiAuth is the API's token authentication from auth.token, nil without it (spec section 15.2). Besides loopback
// names, the server answers the listen address's host (unless it is the unspecified address) and ui.baseUrl's, and
// takes state-changing requests from ui.baseUrl's origin; the session cookie is Secure when the UI is reached over
// HTTPS: tls, a non-loopback listener (behind a TLS-terminating proxy) or an https ui.baseUrl.
func apiAuth(rt *Runtime, env *pipeline.Environment, listen string) (*api.Auth, error) {
	if env.AuthToken == nil {
		return nil, nil
	}
	token, err := env.AuthToken.Value(rt.Env)
	if err != nil {
		return nil, fmt.Errorf("auth.token: %w", err)
	}
	a := &api.Auth{Token: token, User: env.AuthUser, MetricsPublic: env.MetricsPublic,
		SecureCookie: env.TLS != nil || api.CheckLoopback(listen) != nil}
	if host, _, err := net.SplitHostPort(listen); err == nil && host != "" {
		if ip := net.ParseIP(host); ip == nil || !ip.IsUnspecified() {
			a.Hosts = append(a.Hosts, host)
		}
	}
	if env.UIBaseURL != "" {
		u, err := url.Parse(env.UIBaseURL)
		if err != nil {
			return nil, fmt.Errorf("ui.baseUrl: %w", err)
		}
		a.Hosts = append(a.Hosts, u.Host)
		a.Origins = append(a.Origins, u.Scheme+"://"+u.Host)
		a.SecureCookie = a.SecureCookie || u.Scheme == "https"
	}
	return a, nil
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
	if s.tls != nil {
		s.ln, s.url = tls.NewListener(ln, s.tls), "https://"+ln.Addr().String()+"/"
	}
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
