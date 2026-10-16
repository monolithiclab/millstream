// Package api is Millstream's HTTP API (spec section 12.1): REST under /api/v1 over the store's reads and the
// engine's actions, its OpenAPI document at /api/v1/openapi.yaml, and live data over server-sent events.
//
// Every record but a pipeline is scoped by its pipeline (spec section 4.10): two pipelines may hold a run or a
// delivery with the same id, so resources sit under /api/v1/pipelines/{pipeline}. The inbox, insights and metrics
// read across pipelines and take the pipeline as a filter.
//
// Errors are {"error": {"code", "message"}} with a stable code mapped from the store's errors (errorCode). JSON keys
// are lowerCamelCase (D18). Lists that grow without bound page by an opaque cursor: {"items", "next"}, next absent on
// the last page. The OpenAPI document is the contract: the package's tests check that every route is documented and
// that every response they see validates against the document's schemas.
//
// Without Options.Auth the server is reached on a loopback address only (CheckLoopback), and a decision is recorded
// under the X-Millstream-User header, else $USER. With it, every request carries the deployment's token, as a bearer
// token or the UI's session cookie (auth.go), and the reviewer is Auth.User.
package api

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Prefix is where the API is mounted.
const Prefix = "/api/v1"

// MetricsPath is where the Prometheus exposition is served, beside the API.
const MetricsPath = "/metrics"

// UserHeader names the person deciding a review on a server without token authentication.
const UserHeader = "X-Millstream-User"

// DefaultMaxBody bounds a request body without Options.MaxBody: a decision's edit is the largest a client sends.
const DefaultMaxBody = 1 << 20

// DefaultMaxPackagesBody bounds an apply's body, its packages' files, without Options.MaxPackagesBody.
const DefaultMaxPackagesBody = 64 << 20

// Engine is what the actions need of the engine beyond the store: firing a trigger and starting a replay run, which
// resolve variables against the revision, and applying packages, which the engine checks and keeps.
type Engine interface {
	Fire(ctx context.Context, pipeline, trigger, key string, payload json.RawMessage) (string, bool, error)
	Replay(ctx context.Context, req engine.ReplayRequest) (string, bool, error)
	PackageFromFiles(files map[string][]byte) (*pipeline.Package, error)
	ApplyPackages(ctx context.Context, pkgs []*pipeline.Package, opts engine.ApplyOptions) ([]engine.Applied,
		[]engine.Finding, error)
}

// Options configure a Server. Store is required.
type Options struct {
	Store store.Store
	// Engine fires triggers and starts replays; nil answers those actions unavailable.
	Engine Engine
	// Artifacts serves artifact downloads; nil answers them unavailable.
	Artifacts *artifacts.Store
	// Broker feeds the live event streams; nil answers them unavailable.
	Broker *Broker
	// Logs feeds the live log tail; nil answers it unavailable.
	Logs *LogRing
	// Nodes are the nodes the engine serves (the embedded one in M3), without their leases, which come from the
	// store.
	Nodes []Node
	// Version is the binary's version, reported for the engine and the embedded node.
	Version string
	// NotificationFailures counts the engine's failed review notifications; nil reports none.
	NotificationFailures func() int64
	// User is who decides when a request names nobody (the server's user, without Auth); empty makes a request
	// name its reviewer.
	User string
	// Auth requires the deployment's token on every request (spec section 15.2); nil serves loopback clients
	// without one.
	Auth *Auth
	// Clock is the clock of relative ranges, run reports and the live streams' heartbeats; nil means SystemClock.
	Clock Clock
	// Heartbeat is how often a quiet live stream sends a comment; zero means DefaultHeartbeat.
	Heartbeat time.Duration
	// Logger receives the errors the API answers as internal; nil discards them.
	Logger *slog.Logger
	// MaxBody bounds a request body; zero means DefaultMaxBody.
	MaxBody int64
	// MaxPackagesBody bounds an apply's body; zero means DefaultMaxPackagesBody.
	MaxPackagesBody int64
}

// Server serves the API. It is an http.Handler for the paths under Prefix.
type Server struct {
	opts     Options
	log      *slog.Logger
	mux      *http.ServeMux
	sessions sessions
	tokenSum [32]byte // the SHA-256 of Auth.Token
}

// handler is a route's handler: an error is answered by writeError.
type handler func(s *Server, w http.ResponseWriter, r *http.Request) error

// route is one operation: a method and a ServeMux path pattern under Prefix.
type route struct {
	method, path string
	h            handler
}

// routes are the API's operations. The OpenAPI document documents each one (TestRoutesDocumented).
var routes = []route{
	{http.MethodGet, "/openapi.yaml", (*Server).openAPI},
	{http.MethodGet, "/session", (*Server).getSession},
	{http.MethodPost, "/session", (*Server).createSession},
	{http.MethodDelete, "/session", (*Server).deleteSession},
	{http.MethodGet, "/pipelines", (*Server).listPipelines},
	{http.MethodGet, "/pipelines/{pipeline}", (*Server).getPipeline},
	{http.MethodPost, "/pipelines/{pipeline}/pause", (*Server).pausePipeline},
	{http.MethodPost, "/pipelines/{pipeline}/resume", (*Server).resumePipeline},
	{http.MethodPost, "/pipelines/{pipeline}/disable", (*Server).disablePipeline},
	{http.MethodPost, "/pipelines/{pipeline}/rollback", (*Server).rollbackPipeline},
	{http.MethodPost, "/revisions", (*Server).applyPackages},
	{http.MethodPost, "/pipelines/{pipeline}/steps/{step}/pause", (*Server).pauseStep},
	{http.MethodPost, "/pipelines/{pipeline}/steps/{step}/resume", (*Server).resumeStep},
	{http.MethodGet, "/pipelines/{pipeline}/revisions", (*Server).listRevisions},
	{http.MethodGet, "/pipelines/{pipeline}/revisions/{revision}", (*Server).getRevision},
	{http.MethodGet, "/pipelines/{pipeline}/streams", (*Server).listStreams},
	{http.MethodGet, "/pipelines/{pipeline}/runs", (*Server).listRuns},
	{http.MethodPost, "/pipelines/{pipeline}/runs", (*Server).triggerRun},
	{http.MethodGet, "/pipelines/{pipeline}/runs/{run}", (*Server).getRun},
	{http.MethodPost, "/pipelines/{pipeline}/runs/{run}/cancel", (*Server).cancelRun},
	{http.MethodGet, "/pipelines/{pipeline}/runs/{run}/report", (*Server).getRunReport},
	{http.MethodGet, "/pipelines/{pipeline}/runs/{run}/deliveries", (*Server).listRunDeliveries},
	{http.MethodPost, "/pipelines/{pipeline}/replay", (*Server).replay},
	{http.MethodGet, "/pipelines/{pipeline}/items", (*Server).listItems},
	{http.MethodGet, "/pipelines/{pipeline}/items/{item}", (*Server).getItem},
	{http.MethodGet, "/pipelines/{pipeline}/items/{item}/lineage", (*Server).getLineage},
	{http.MethodGet, "/pipelines/{pipeline}/items/{item}/deliveries", (*Server).listItemDeliveries},
	{http.MethodGet, "/pipelines/{pipeline}/items/{item}/artifacts/{name}", (*Server).getItemArtifact},
	{http.MethodGet, "/pipelines/{pipeline}/deliveries/{delivery}", (*Server).getDelivery},
	{http.MethodGet, "/pipelines/{pipeline}/deliveries/{delivery}/attempts/{attempt}/artifacts/{name}",
		(*Server).getAttemptArtifact},
	{http.MethodGet, "/pipelines/{pipeline}/dead-letters", (*Server).listDeadLetters},
	{http.MethodPost, "/pipelines/{pipeline}/dead-letters/replay", (*Server).replayDeadLetters},
	{http.MethodPost, "/pipelines/{pipeline}/dead-letters/discard", (*Server).discardDeadLetters},
	{http.MethodGet, "/pipelines/{pipeline}/lessons", (*Server).listLessons},
	{http.MethodGet, "/pipelines/{pipeline}/reviews/{delivery}", (*Server).getReview},
	{http.MethodPost, "/pipelines/{pipeline}/reviews/{delivery}/decision", (*Server).decideReview},
	{http.MethodGet, "/pipelines/{pipeline}/samples/{delivery}", (*Server).getSample},
	{http.MethodPost, "/pipelines/{pipeline}/samples/{delivery}/decision", (*Server).decideSample},
	{http.MethodPost, "/pipelines/{pipeline}/insights/{insight}/acknowledge", (*Server).acknowledgeInsight},
	{http.MethodPost, "/pipelines/{pipeline}/insights/{insight}/mute", (*Server).muteInsight},
	{http.MethodGet, "/inbox", (*Server).listInbox},
	{http.MethodGet, "/inbox/samples", (*Server).listSamples},
	{http.MethodGet, "/insights", (*Server).listInsights},
	{http.MethodGet, "/metrics", (*Server).getMetrics},
	{http.MethodGet, "/metrics/names", (*Server).listMetricNames},
	{http.MethodGet, "/nodes", (*Server).listNodes},
	{http.MethodGet, "/live/pipelines/{pipeline}", (*Server).livePipeline},
	{http.MethodGet, "/live/runs/{run}", (*Server).liveRun},
	{http.MethodGet, "/live/items/{item}", (*Server).liveItem},
	{http.MethodGet, "/live/logs", (*Server).liveLogs},
}

// New returns a server over opts.Store.
func New(opts Options) (*Server, error) {
	if opts.Store == nil {
		return nil, errors.New("api: no store")
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock{}
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = DefaultHeartbeat
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.MaxBody <= 0 {
		opts.MaxBody = DefaultMaxBody
	}
	if opts.MaxPackagesBody <= 0 {
		opts.MaxPackagesBody = DefaultMaxPackagesBody
	}
	if opts.Auth != nil {
		if opts.Auth.Token == "" {
			return nil, errors.New("api: an empty token")
		}
		a := *opts.Auth
		a.User = cmp.Or(a.User, pipeline.DefaultAuthUser)
		a.SessionTTL = cmp.Or(a.SessionTTL, DefaultSessionTTL)
		a.Hosts = make([]string, len(opts.Auth.Hosts))
		for i, h := range opts.Auth.Hosts {
			a.Hosts[i] = strings.ToLower(hostName(h))
		}
		a.Origins = make([]string, len(opts.Auth.Origins))
		for i, o := range opts.Auth.Origins {
			a.Origins[i] = strings.ToLower(strings.TrimSuffix(o, "/"))
		}
		opts.Auth = &a
	}
	s := &Server{opts: opts, log: opts.Logger, mux: http.NewServeMux(),
		sessions: sessions{byID: map[[32]byte]time.Time{}}}
	if opts.Auth != nil {
		s.tokenSum = sha256.Sum256([]byte(opts.Auth.Token))
	}
	s.mux.HandleFunc("GET "+MetricsPath, func(w http.ResponseWriter, r *http.Request) {
		if err := s.prometheus(w, r); err != nil {
			s.writeError(w, r, err)
		}
	})
	var paths []string
	for _, rt := range routes {
		h := rt.h
		s.mux.HandleFunc(rt.method+" "+Prefix+rt.path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && !jsonContent(r) {
				writeErrorBody(w, http.StatusUnsupportedMediaType, codeUnsupportedMediaType,
					"a POST carries Content-Type: application/json, an empty object when it has nothing to say")
				return
			}
			if err := h(s, w, r); err != nil {
				s.writeError(w, r, err)
			}
		})
		if !slices.Contains(paths, rt.path) {
			paths = append(paths, rt.path)
		}
	}
	// A path the API has, asked with a method it does not answer; any other path under /api/.
	for _, p := range paths {
		var allow []string
		for _, rt := range routes {
			if rt.path == p {
				allow = append(allow, rt.method)
			}
		}
		s.mux.HandleFunc(Prefix+p, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", strings.Join(allow, ", "))
			writeErrorBody(w, http.StatusMethodNotAllowed, codeMethodNotAllowed,
				fmt.Sprintf("%s %s: allowed %s", r.Method, r.URL.Path, strings.Join(allow, ", ")))
		})
	}
	s.mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeErrorBody(w, http.StatusNotFound, codeNotFound, "no such API path: "+r.URL.Path)
	})
	return s, nil
}

// ServeHTTP serves the API's paths and /metrics, and answers any other path under /api/ notFound. A state-changing
// request from another origin is forbidden; with Auth, a request without the token is unauthorized.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	cred := noCredential
	if s.opts.Auth != nil {
		cred = s.credential(r)
	}
	if !s.sameOrigin(r, cred) {
		writeErrorBody(w, http.StatusForbidden, codeForbidden, r.Method+" from another origin: "+
			strings.Join(r.Header.Values("Origin"), ", "))
		return
	}
	if s.opts.Auth != nil && cred == noCredential && !s.public(r) {
		writeUnauthorized(w, "sign in: send the token as Authorization: Bearer <token>, or sign in through the UI")
		return
	}
	s.mux.ServeHTTP(w, r)
}

// jsonContent reports whether a request's body is declared JSON. A browser sends a cross-site POST without asking
// first only with a form's content types: requiring JSON makes it ask (a CORS preflight), which the API never
// grants, so a page elsewhere cannot act on the API of a machine it runs on.
func jsonContent(r *http.Request) bool {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && mt == "application/json"
}

// Handler serves the API under /api/, the Prometheus exposition on /metrics, and everything else with ui, the web
// UI's single-page app, to requests naming a loopback host, or with Auth one of its hosts: a page whose name resolves
// to this machine (DNS rebinding) must not reach the API as if it were the UI.
//
// No response may be framed by another page (clickjacking): every one says so, and the UI's pages carry a content
// security policy that loads nothing from elsewhere. Artifacts replace it with their own sandbox (serveArtifact).
func Handler(s *Server, ui http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/", s)
	mux.Handle(MetricsPath, s)
	mux.Handle("/", ui)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Frame-Options", "DENY")
		if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == MetricsPath {
			h.Set("Content-Security-Policy", apiPolicy)
		} else {
			h.Set("Content-Security-Policy", uiPolicy)
		}
		if !s.hostAllowed(r.Host) {
			writeErrorBody(w, http.StatusForbidden, codeForbidden, "host "+r.Host+" is not one this server answers")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// Content security policies. The UI's scripts, styles, fonts and API calls are all its own; its styles include
// inline ones (React Flow positions nodes with style attributes), images may be data: or blob: URLs, and PDF
// artifacts are shown in an <object> of the same origin.
const (
	apiPolicy = "frame-ancestors 'none'"
	uiPolicy  = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; " +
		"font-src 'self'; connect-src 'self'; object-src 'self'; base-uri 'none'; form-action 'self'; " +
		"frame-ancestors 'none'"
	// artifactPolicy sandboxes what an action wrote, which only the UI's own pages may embed.
	artifactPolicy = "sandbox; default-src 'none'; frame-ancestors 'self'"
)

// CheckLoopback refuses a listen address that is not a loopback one: without auth.token the API changes state for
// whoever reaches it, so it is served to this machine only. A host name must be localhost.
func CheckLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("listen address %q: want host:port, such as 127.0.0.1:7070: %w", addr, err)
	}
	if host != "" && loopbackHost(host) {
		return nil
	}
	return fmt.Errorf("listen address %q is not a loopback address: without auth.token the API listens on "+
		"127.0.0.1, ::1 or localhost only", addr)
}

// loopbackHost reports whether a Host header names this machine: localhost or a loopback address, with or without
// a port.
func loopbackHost(hostport string) bool {
	host := hostName(hostport)
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// hostName is a Host header's name: without its port and an IPv6 address's brackets.
func hostName(hostport string) string {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	return strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
}

// Error codes: stable, for clients to branch on.
const (
	codeNotFound             = "notFound"
	codeConflict             = "conflict"
	codeInvalid              = "invalid"
	codeUnavailable          = "unavailable"
	codeMethodNotAllowed     = "methodNotAllowed"
	codeTooLarge             = "tooLarge"
	codeForbidden            = "forbidden"
	codeUnauthorized         = "unauthorized"
	codeUnsupportedMediaType = "unsupportedMediaType"
	codeInternal             = "internal"
)

// errUnavailable marks what this server cannot do as configured (no engine, no artifact store) or now (closing).
var errUnavailable = errors.New("unavailable")

// invalidf is a request error: its message is the client's to read.
func invalidf(format string, args ...any) error {
	return fmt.Errorf(format+": %w", append(args, store.ErrInvalid)...)
}

// errorCode maps an error to its status and code: the store's errors to theirs, a body over the limit to
// tooLarge, and anything else to internal.
func errorCode(err error) (int, string) {
	var tooLarge *http.MaxBytesError
	switch {
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, codeNotFound
	case errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrStaleLease):
		return http.StatusConflict, codeConflict
	case errors.As(err, &tooLarge):
		return http.StatusRequestEntityTooLarge, codeTooLarge
	case errors.Is(err, store.ErrInvalid), errors.Is(err, engine.ErrFiringConfig):
		return http.StatusBadRequest, codeInvalid
	case errors.Is(err, errUnavailable), errors.Is(err, store.ErrClosed), errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, codeUnavailable
	}
	return http.StatusInternalServerError, codeInternal
}

// writeError answers err. An internal error's message is logged, not sent: it may name the store's internals.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := errorCode(err)
	msg := err.Error()
	if code == codeInternal {
		s.log.Error("api", "method", r.Method, "path", r.URL.Path, "error", msg)
		msg = "internal error"
	}
	writeErrorBody(w, status, code, msg)
}

// errorBody is the body of every error answer.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeErrorBody(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{errorDetail{Code: code, Message: msg}})
}

// writeJSON answers v as JSON with status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v) // the status is sent: a client gone mid-body has nothing to be told
}

// decodeBody decodes the JSON body into v, refusing unknown fields and trailing data; an empty body leaves v as it
// is.
func (s *Server) decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	return s.decodeBodyMax(w, r, v, s.opts.MaxBody)
}

// decodeBodyMax is decodeBody with a body of at most limit bytes.
func (s *Server) decodeBodyMax(w http.ResponseWriter, r *http.Request, v any, limit int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			return err
		case errors.Is(err, io.EOF):
			return nil
		}
		return invalidf("request body: %s", err.Error())
	}
	if dec.More() {
		return invalidf("request body: more than one JSON value")
	}
	return nil
}

// user is who makes the request: with Auth its user, whatever the request says; else the X-Millstream-User
// header, else $USER.
func (s *Server) user(r *http.Request) (string, error) {
	if s.opts.Auth != nil {
		return s.opts.Auth.User, nil
	}
	if u := strings.TrimSpace(r.Header.Get(UserHeader)); u != "" {
		return u, nil
	}
	if s.opts.User != "" {
		return s.opts.User, nil
	}
	return "", invalidf("name who decides with the %s header (the server has no user)", UserHeader)
}
