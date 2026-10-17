// Package nodeapi is the node protocol (spec sections 5.10, 12.1 and 15.7), both sides: the handler the active engine
// serves under /node/v1 (api.Options.NodeAPI) for remote nodes, and the Client a remote node reaches it with
// (client.go), a node.Transport. Every request carries the deployment's
// node token, compared in constant time; every request but a registration names a node registered with this engine
// in this term. Leases are the capability: a node fetches a package, or obtains a URL for an artifact, only for a
// delivery it holds. Artifacts move through the S3-compatible store's presigned URLs when the engine has one, else
// through the engine's proxy over its local directory, whose URLs are signed by the engine (an HMAC under a key of
// its own, valid for URLTTL).
package nodeapi

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/nodewire"
	"github.com/monolithiclab/millstream/internal/store"
)

// Defaults.
const (
	// MaxPullWait caps a pull's long poll.
	MaxPullWait = 25 * time.Second
	// URLTTL is how long an artifact URL stays valid.
	URLTTL = 15 * time.Minute
	// maxBody bounds a request body, a delivery's outputs the largest (artifact contents go through Blob).
	maxBody = 64 << 20
	// maxLogLines bounds the lines of one relay.
	maxLogLines = 1000
)

// Options configure the handler. Engine and Token are required.
type Options struct {
	Engine *engine.Engine
	// Token is the deployment's node token (node.token).
	Token string
	// Artifacts is the engine's artifact store; nil refuses artifact URLs.
	Artifacts artifacts.Store
	// Version is the engine's version, told to registering nodes.
	Version string
	// Relay receives the log lines nodes relay, each with its node id: the engine's log and the live tail.
	Relay *slog.Logger
	// Logger receives the handler's own errors; nil discards them.
	Logger *slog.Logger
	// Now is the clock of artifact URLs' expiry; nil means time.Now.
	Now func() time.Time
	// Key signs the proxy's artifact URLs; nil draws a random one.
	Key []byte
}

// Handler serves the node protocol.
type Handler struct {
	opts     Options
	tokenSum [32]byte
	mux      *http.ServeMux
}

// New returns the handler.
func New(opts Options) (*Handler, error) {
	if opts.Engine == nil || opts.Token == "" {
		return nil, errors.New("nodeapi: an engine and a node token are required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Relay == nil {
		opts.Relay = opts.Logger
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Key == nil {
		opts.Key = make([]byte, 32)
		_, _ = rand.Read(opts.Key) // crypto/rand never fails
	}
	h := &Handler{opts: opts, tokenSum: sha256.Sum256([]byte(opts.Token)), mux: http.NewServeMux()}
	for path, fn := range map[string]func(http.ResponseWriter, *http.Request) error{
		"POST " + nodewire.PathRegister:    h.register,
		"POST " + nodewire.PathPull:        h.pull,
		"POST " + nodewire.PathHeartbeat:   h.heartbeat,
		"POST " + nodewire.PathDone:        h.done,
		"POST " + nodewire.PathFail:        h.fail,
		"POST " + nodewire.PathLeave:       h.leave,
		"POST " + nodewire.PathLogs:        h.logs,
		"GET " + nodewire.PathPackage:      h.pkg,
		"POST " + nodewire.PathArtifactURL: h.artifactURL,
		"GET " + nodewire.PathBlob:         h.getBlob,
		"PUT " + nodewire.PathBlob:         h.putBlob,
	} {
		h.mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if err := fn(w, r); err != nil {
				h.fail500(w, r, err)
			}
		})
	}
	return h, nil
}

// ServeHTTP authenticates the node token, then serves the operation.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !authorized(w, r, h.tokenSum) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	h.mux.ServeHTTP(w, r)
}

// statusError is an answer other than a success, with its code.
type statusError struct {
	status int
	code   string
	msg    string
}

func (e *statusError) Error() string { return e.msg }

func errAnswer(status int, code, format string, args ...any) error {
	return &statusError{status: status, code: code, msg: fmt.Sprintf(format, args...)}
}

// fail500 answers err: a statusError as itself, the engine's errors by their meaning, anything else internal.
func (h *Handler) fail500(w http.ResponseWriter, r *http.Request, err error) {
	var se *statusError
	switch {
	case errors.As(err, &se):
		writeError(w, se.status, se.code, se.msg)
	case errors.Is(err, engine.ErrUnregistered):
		writeError(w, http.StatusConflict, nodewire.CodeUnregistered, "register with this engine first")
	case errors.Is(err, store.ErrRevoked):
		writeError(w, http.StatusForbidden, nodewire.CodeRevoked, "this node id was revoked")
	case errors.Is(err, engine.ErrNotHeld):
		writeError(w, http.StatusForbidden, nodewire.CodeNotHeld, err.Error())
	case errors.Is(err, store.ErrStaleLease):
		writeError(w, http.StatusConflict, nodewire.CodeStale, err.Error())
	case errors.Is(err, store.ErrInvalid):
		writeError(w, http.StatusBadRequest, nodewire.CodeInvalid, err.Error())
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, nodewire.CodeNotFound, err.Error())
	case unavailable(r, err):
		// The engine's term ended, its store is gone or read-only, or the node went away: never an implicit success,
		// which a node would take for an acceptance. It sends the request again, to this engine or the next.
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, nodewire.CodeUnavailable, "the engine cannot answer now: retry")
	default:
		h.opts.Logger.Error("node protocol", "path", r.URL.Path, "error", err.Error())
		writeError(w, http.StatusInternalServerError, nodewire.CodeInternal, "internal error")
	}
}

// unavailable reports whether err, or r's context, means the engine cannot answer now rather than a failure.
func unavailable(r *http.Request, err error) bool {
	return r.Context().Err() != nil || store.Retryable(err)
}

// authorized reports whether r carries the node token whose SHA-256 is sum, compared in constant time, and answers
// unauthorized when it does not.
func authorized(w http.ResponseWriter, r *http.Request, sum [32]byte) bool {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	got := sha256.Sum256([]byte(token))
	if ok && subtle.ConstantTimeCompare(got[:], sum[:]) == 1 {
		return true
	}
	w.Header().Set("WWW-Authenticate", `Bearer realm="millstream-node"`)
	writeError(w, http.StatusUnauthorized, nodewire.CodeUnauthorized, "the node token is missing or wrong")
	return false
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var body nodewire.Error
	body.Error.Code, body.Error.Message = code, msg
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the status is sent: a node gone mid-body has nothing to be told
}

// decode decodes a JSON body, refusing unknown fields.
func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "malformed request: %v", err)
	}
	return nil
}

// known checks that the node named is registered with this engine and not revoked.
func (h *Handler) known(id string) error {
	if id == "" {
		return errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "the request names no node")
	}
	return h.opts.Engine.CheckNode(id)
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) error {
	var req nodewire.Register
	if err := decode(r, &req); err != nil {
		return err
	}
	switch {
	case req.Protocol != nodewire.Protocol:
		return errAnswer(http.StatusBadRequest, nodewire.CodeIncompatible,
			"node protocol %d: this engine (%s) speaks protocol %d", req.Protocol, h.opts.Version, nodewire.Protocol)
	case req.Node == "" || req.Slots <= 0:
		return errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "a node id and a positive number of slots")
	case strings.HasPrefix(req.Node, "embedded-"):
		return errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "node ids starting embedded- are the engines'")
	}
	e := h.opts.Engine
	if err := e.RegisterNode(r.Context(), engine.NodeInfo{ID: req.Node, Version: req.Version, Labels: req.Labels,
		Runtimes: req.Runtimes, Slots: req.Slots, Nonce: req.Nonce}); err != nil {
		return err
	}
	lead := e.Leadership()
	writeJSON(w, http.StatusOK, nodewire.Registered{Engine: lead.ID, Epoch: lead.Epoch, Version: h.opts.Version,
		LeaseTTLMs: e.LeaseTTL().Milliseconds()})
	return nil
}

// pull leases deliveries for the node, waiting for some up to the request's wait: whenever a lease in flight ends,
// and every second, for what else made work due (a firing, a retry's backoff ending). Leasing itself stays within
// what is left of the wait, so the answer comes before the node's request times out (Client.Pull).
func (h *Handler) pull(w http.ResponseWriter, r *http.Request) error {
	var req nodewire.Pull
	if err := decode(r, &req); err != nil {
		return err
	}
	if err := h.known(req.Node); err != nil {
		return err
	}
	e, ctx := h.opts.Engine, r.Context()
	wait := min(time.Duration(req.WaitMs)*time.Millisecond, MaxPullWait)
	end := time.Now().Add(wait)
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		changed := e.Changes()
		as, err := e.PullWithin(ctx, req.Node, max(req.Max, 0), time.Until(end))
		if err != nil {
			return err
		}
		if len(as) > 0 {
			out := nodewire.Assignments{Assignments: make([]nodewire.Assignment, 0, len(as))}
			for _, a := range as {
				out.Assignments = append(out.Assignments, nodewire.Assignment{Lease: nodewire.FromLease(a.Lease),
					Launch: a.Launch, Secrets: a.Secrets, Item: a.ItemMsg, TimeoutMs: a.Timeout.Milliseconds()})
			}
			writeJSON(w, http.StatusOK, out)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			writeJSON(w, http.StatusOK, nodewire.Assignments{Assignments: []nodewire.Assignment{}})
			return nil
		case <-changed:
		case <-tick.C:
		}
	}
}

func (h *Handler) heartbeat(w http.ResponseWriter, r *http.Request) error {
	var req nodewire.Heartbeat
	if err := decode(r, &req); err != nil {
		return err
	}
	if req.Node == "" {
		return h.known("")
	}
	lost, err := h.opts.Engine.NodeHeartbeat(r.Context(), req.Node, nodewire.ToRefs(req.Leases))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, nodewire.Lost{Lost: nodewire.FromRefs(lost)})
	return nil
}

// report checks a Done or Fail before the engine applies it: from a known node, under a lease no other node holds,
// naming only artifacts stored in the lease's pipeline: a node references what it stored there for its lease, never
// another pipeline's content (spec section 15.7).
func (h *Handler) report(ctx context.Context, id string, ref store.LeaseRef, outputs []store.Output,
	kept []store.ArtifactRef,
) error {
	if err := h.known(id); err != nil {
		return err
	}
	if h.opts.Engine.HeldElsewhere(id, ref) {
		return fmt.Errorf("delivery %s is leased to another node: %w", ref.DeliveryID, store.ErrStaleLease)
	}
	seen := map[string]bool{}
	for _, refs := range append([][]store.ArtifactRef{kept}, outputArtifacts(outputs)...) {
		for _, a := range refs {
			if seen[a.SHA256] {
				continue
			}
			seen[a.SHA256] = true
			if h.opts.Artifacts == nil {
				return errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "the engine has no artifact store")
			}
			ok, err := h.opts.Artifacts.Exists(ctx, ref.Pipeline, a.SHA256)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("artifact %s (%s) is not stored in pipeline %s: %w", a.Name, a.SHA256,
					ref.Pipeline, store.ErrInvalid)
			}
		}
	}
	return nil
}

// outputArtifacts are the artifact references of each output.
func outputArtifacts(outputs []store.Output) [][]store.ArtifactRef {
	out := make([][]store.ArtifactRef, 0, len(outputs))
	for _, o := range outputs {
		out = append(out, o.Artifacts)
	}
	return out
}

func (h *Handler) done(w http.ResponseWriter, r *http.Request) error {
	var req nodewire.Done
	if err := decode(r, &req); err != nil {
		return err
	}
	ref, outputs := req.Lease.ToRef(), nodewire.ToOutputs(req.Outputs)
	if err := h.report(r.Context(), req.Node, ref, outputs, req.Artifacts); err != nil {
		return err
	}
	if err := h.opts.Engine.Done(r.Context(), store.DoneRequest{LeaseRef: ref, Owner: req.Node, Attempt: req.Attempt,
		Outputs: outputs, CostUSD: req.CostUSD, Artifacts: req.Artifacts, Metrics: req.Metrics}); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, nodewire.Ack{Accepted: true})
	return nil
}

func (h *Handler) fail(w http.ResponseWriter, r *http.Request) error {
	var req nodewire.Fail
	if err := decode(r, &req); err != nil {
		return err
	}
	ref := req.Lease.ToRef()
	if err := h.report(r.Context(), req.Node, ref, nil, req.Artifacts); err != nil {
		return err
	}
	if err := h.opts.Engine.Fail(r.Context(), store.FailRequest{LeaseRef: ref, Owner: req.Node, Attempt: req.Attempt,
		Class: req.Class, Message: req.Message, Retryable: req.Retryable, CostUSD: req.CostUSD,
		Unreported: req.Unreported, Artifacts: req.Artifacts, Metrics: req.Metrics}); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, nodewire.Ack{Accepted: true})
	return nil
}

func (h *Handler) leave(w http.ResponseWriter, r *http.Request) error {
	var req nodewire.Leave
	if err := decode(r, &req); err != nil {
		return err
	}
	if err := h.known(req.Node); err != nil {
		return err
	}
	if err := h.opts.Engine.SetNodeState(r.Context(), req.Node, store.NodeDraining, true); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, nodewire.Ack{Accepted: true})
	return nil
}

// logs re-emits a node's lines through Relay, at their level, with their attributes and the node's id.
func (h *Handler) logs(w http.ResponseWriter, r *http.Request) error {
	var req nodewire.Logs
	if err := decode(r, &req); err != nil {
		return err
	}
	if err := h.known(req.Node); err != nil {
		return err
	}
	if len(req.Lines) > maxLogLines {
		return errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "%d log lines, over %d", len(req.Lines),
			maxLogLines)
	}
	handler := h.opts.Relay.Handler()
	for _, l := range req.Lines {
		var level slog.Level
		if level.UnmarshalText([]byte(l.Level)) != nil {
			level = slog.LevelInfo
		}
		if !handler.Enabled(r.Context(), level) {
			continue
		}
		rec := slog.NewRecord(l.Time, level, l.Msg, 0)
		rec.AddAttrs(slog.String("nodeId", req.Node))
		for k, v := range l.Attrs {
			if k != "nodeId" {
				rec.AddAttrs(slog.Any(k, v))
			}
		}
		if err := handler.Handle(r.Context(), rec); err != nil {
			return err
		}
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// leaseQuery reads the node and lease a GET names in its query.
func leaseQuery(q url.Values) (string, store.LeaseRef, error) {
	token, err := strconv.ParseInt(q.Get("token"), 10, 64)
	if err != nil {
		return "", store.LeaseRef{}, errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "token: %v", err)
	}
	return q.Get("node"), store.LeaseRef{Pipeline: q.Get("pipeline"), DeliveryID: q.Get("delivery"),
		Step: q.Get("step"), FencingToken: token}, nil
}

// pkg answers the bundle of the package of a delivery the node holds.
func (h *Handler) pkg(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	id, ref, err := leaseQuery(q)
	if err != nil {
		return err
	}
	if err := h.known(id); err != nil {
		return err
	}
	bundle, err := h.opts.Engine.HeldPackage(r.Context(), id, ref, q.Get("hash"))
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, err = w.Write(bundle) // #nosec G705 -- a package bundle as JSON, never rendered by a browser
	return err
}

// artifactURL answers a URL that moves an artifact of a delivery the node holds: an input to read, an output to
// write into the delivery's pipeline.
func (h *Handler) artifactURL(w http.ResponseWriter, r *http.Request) error {
	var req nodewire.ArtifactURL
	if err := decode(r, &req); err != nil {
		return err
	}
	if err := h.known(req.Node); err != nil {
		return err
	}
	arts := h.opts.Artifacts
	if arts == nil {
		return errAnswer(http.StatusServiceUnavailable, nodewire.CodeUnavailable, "the engine has no artifact store")
	}
	ctx, e, ref, a := r.Context(), h.opts.Engine, req.Lease.ToRef(), req.Artifact
	var out nodewire.URL
	switch req.Op {
	case nodewire.OpGet:
		held, err := e.HeldInput(ctx, req.Node, ref, a.SHA256)
		if err != nil {
			return err
		}
		a = held
	case nodewire.OpPut:
		if err := e.HoldsLease(req.Node, ref); err != nil {
			return err
		}
		if err := store.ValidArtifact(a); err != nil {
			return err
		}
		exists, err := stored(ctx, arts, ref.Pipeline, a.SHA256)
		if err != nil {
			return err
		}
		if exists {
			writeJSON(w, http.StatusOK, nodewire.URL{Exists: true})
			return nil
		}
	default:
		return errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "op %q: want get or put", req.Op)
	}
	if p, ok := arts.(artifacts.Presigner); ok {
		var u string
		var err error
		if req.Op == nodewire.OpGet {
			u, err = p.PresignGet(ref.Pipeline, a.SHA256, URLTTL)
		} else {
			u, err = p.PresignPut(ref.Pipeline, a.SHA256, a.Size, URLTTL)
		}
		if err != nil {
			return err
		}
		out = nodewire.URL{URL: u, Direct: true}
	} else {
		out = nodewire.URL{URL: h.blobURL(req.Op, ref.Pipeline, a)}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// stored reports whether arts holds pipeline's artifact sha, touching it when it does (artifacts.Toucher): the node
// does not send it again, so the artifact sweep's grace counts from now as if it had (spec section 9.3).
func stored(ctx context.Context, arts artifacts.Store, pipeline, sha string) (bool, error) {
	t, ok := arts.(artifacts.Toucher)
	if !ok {
		return arts.Exists(ctx, pipeline, sha)
	}
	switch err := t.Touch(ctx, pipeline, sha); {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}

// blobURL is the proxy's signed URL for one artifact move: what the signature covers is the operation, the pipeline,
// the hash, the name, the size and the expiry.
func (h *Handler) blobURL(op, pipeline string, a store.ArtifactRef) string {
	exp := strconv.FormatInt(h.opts.Now().Add(URLTTL).Unix(), 10)
	q := url.Values{"op": {op}, "pipeline": {pipeline}, "sha256": {a.SHA256}, "name": {a.Name}, "exp": {exp},
		"size": {strconv.FormatInt(a.Size, 10)}}
	q.Set("sig", h.sign(q))
	return nodewire.PathBlob + "?" + q.Encode()
}

func (h *Handler) sign(q url.Values) string {
	mac := hmac.New(sha256.New, h.opts.Key)
	for _, k := range []string{"op", "pipeline", "sha256", "name", "size", "exp"} {
		mac.Write([]byte(q.Get(k)))
		mac.Write([]byte{0})
	}
	return hex.EncodeToString(mac.Sum(nil))
}

// blob checks a proxy URL's signature and expiry for op.
func (h *Handler) blob(r *http.Request, op string) (url.Values, error) {
	q := r.URL.Query()
	exp, err := strconv.ParseInt(q.Get("exp"), 10, 64)
	switch {
	case err != nil || q.Get("op") != op || !hmac.Equal([]byte(h.sign(q)), []byte(q.Get("sig"))):
		return nil, errAnswer(http.StatusForbidden, nodewire.CodeNotHeld, "the artifact URL is not one this engine signed")
	case h.opts.Now().Unix() > exp:
		return nil, errAnswer(http.StatusForbidden, nodewire.CodeNotHeld, "the artifact URL expired")
	case h.opts.Artifacts == nil:
		return nil, errAnswer(http.StatusServiceUnavailable, nodewire.CodeUnavailable, "the engine has no artifact store")
	}
	return q, nil
}

func (h *Handler) getBlob(w http.ResponseWriter, r *http.Request) error {
	q, err := h.blob(r, nodewire.OpGet)
	if err != nil {
		return err
	}
	f, err := h.opts.Artifacts.Open(r.Context(), q.Get("pipeline"), q.Get("sha256"))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = io.Copy(w, f)
	if err != nil {
		return context.Cause(r.Context()) // the node went away mid-body: nothing to answer
	}
	return nil
}

func (h *Handler) putBlob(w http.ResponseWriter, r *http.Request) error {
	q, err := h.blob(r, nodewire.OpPut)
	if err != nil {
		return err
	}
	// The body must be the size the URL was issued for, checked before anything is written.
	if size, err := strconv.ParseInt(q.Get("size"), 10, 64); err != nil || r.ContentLength != size {
		return errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "a body of %d bytes, not the %s declared",
			r.ContentLength, q.Get("size"))
	}
	ref, err := h.opts.Artifacts.Put(r.Context(), q.Get("pipeline"), q.Get("name"),
		http.MaxBytesReader(w, r.Body, r.ContentLength))
	if err != nil {
		return err
	}
	if ref.SHA256 != q.Get("sha256") {
		return errAnswer(http.StatusBadRequest, nodewire.CodeInvalid, "the content hashes to %s, not %s", ref.SHA256,
			q.Get("sha256"))
	}
	writeJSON(w, http.StatusOK, ref)
	return nil
}

// NewStandby returns the node protocol's handler on a standby engine: every request with the node token is refused
// unavailable, naming the active engine active returns (its id and address, empty when none is known), which a node
// follows (spec section 5.9).
func NewStandby(token string, active func(ctx context.Context) (id, address string)) http.Handler {
	sum := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r, sum) {
			return
		}
		msg := "this engine is the standby: nodes go to the active engine"
		if id, addr := active(r.Context()); id != "" {
			w.Header().Set(api.ActiveEngineHeader, id)
			msg += " " + id
			if addr != "" {
				w.Header().Set(api.ActiveAddressHeader, addr)
				msg += " at " + addr
			}
		}
		writeError(w, http.StatusServiceUnavailable, nodewire.CodeUnavailable, msg)
	})
}
