// The client side of the node protocol (spec section 5.10): a node.Transport over HTTPS (HTTP/2) to the active
// engine, or plain HTTP to a loopback one. It registers, pulls with a long poll, heartbeats, and reports outcomes
// under the leases' fencing tokens, resending a Done or Fail until the engine answers it: across a reconnection, and
// across a failover, after which the new active engine answers that the node must register again, and the node
// does. It follows the active engine: through the engines the join URLs list, and the address a standby names in
// its refusal. It fetches packages by hash into a cache under its state directory, and moves artifacts through the
// URLs the engine issues for the leases it holds. A revocation or a refused token is fatal (Fatal): the node stops.

package nodeapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/nodewire"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/store"
)

// ClassTransport is the error class of a delivery the node could not prepare because it could not reach the engine
// (its package did not arrive). Retried.
const ClassTransport = "transport"

// Fatal errors: the node stops.
var (
	// ErrUnauthorized means the engine refused the node token: rotated, until the node is given the new one.
	ErrUnauthorized = errors.New("the engine refused the node token")
	// ErrRevoked means the node's id was revoked (millstream nodes revoke).
	ErrRevoked = errors.New("this node was revoked")
	// ErrIncompatible means the engine speaks another node protocol version.
	ErrIncompatible = errors.New("the engine speaks another node protocol")
)

// Defaults for zero ClientOptions.
const (
	DefaultPullWait = 2 * time.Second
	minBackoff      = 250 * time.Millisecond
	maxBackoff      = 5 * time.Second
	// attemptTimeout bounds one request but a pull, whose wait it is added to: a connection that hangs (a
	// partition) is given up for the next engine.
	attemptTimeout = 10 * time.Second
	// maxRelay bounds the log lines kept for the relay while the engine is unreachable.
	maxRelay = 1000
)

// ClientOptions configure a Client. Join, Token, HTTP, Node and StateDir are required.
type ClientOptions struct {
	// Join are the engines' URLs, tried in turn: https, or http to a loopback address.
	Join []string
	// Token is the deployment's node token.
	Token string
	// HTTP makes the requests, to the engines and to the S3-compatible store's presigned URLs.
	HTTP *http.Client
	// Node is what the node registers with.
	Node nodewire.Register
	// StateDir holds the package cache (packages/<hash>).
	StateDir string
	// TmpDir is where outputs are spooled while their hash is computed; empty means the system's temporary directory.
	TmpDir string
	// Env, Agents: the machine a delivery's action process is built on (node.Host).
	Env    func(string) (string, bool)
	Agents node.Agents
	// PullWait is a pull's long poll; zero means DefaultPullWait. The node heartbeats beside it.
	PullWait time.Duration
	// Logger receives the transport's own logs; nil discards them.
	Logger *slog.Logger
}

// Client is a node.Transport to the active engine.
type Client struct {
	opts ClientOptions
	log  *slog.Logger

	mu   sync.Mutex
	urls []*url.URL // the join URLs and the active engines learned since
	cur  int
	reg  nodewire.Registered

	regMu    sync.Mutex
	regCount atomic.Int64 // registrations so far: a call that found the node unregistered registers once

	fatalOnce sync.Once
	fatal     chan struct{}
	fatalErr  error

	pkgs *packages

	relayMu      sync.Mutex
	relay        []nodewire.LogLine
	relayDropped int
}

var _ node.Transport = (*Client)(nil)

// New returns a transport, refusing a join URL that is neither https nor a loopback http one: nodes require TLS
// except to a loopback engine (spec section 15.7).
func NewClient(opts ClientOptions) (*Client, error) {
	switch {
	case len(opts.Join) == 0:
		return nil, errors.New("node: no engine to join")
	case opts.Token == "":
		return nil, errors.New("node: no node token (MILLSTREAM_NODE_TOKEN)")
	case opts.HTTP == nil || opts.Node.Node == "" || opts.StateDir == "":
		return nil, errors.New("node: an HTTP client, a node id and a state directory are required")
	}
	if opts.PullWait <= 0 {
		opts.PullWait = DefaultPullWait
	}
	if opts.Env == nil {
		opts.Env = func(string) (string, bool) { return "", false }
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Node.Nonce == "" {
		opts.Node.Nonce = rand.Text() // this process's: a drain holds across a failover (nodewire.Register)
	}
	t := &Client{opts: opts, log: opts.Logger, fatal: make(chan struct{}),
		pkgs: newPackages(opts.StateDir)}
	for _, raw := range opts.Join {
		u, err := CheckURL(raw)
		if err != nil {
			return nil, err
		}
		t.urls = append(t.urls, u)
	}
	return t, nil
}

// CheckURL parses an engine URL: https, or http to a loopback host (localhost, 127.0.0.0/8, ::1).
func CheckURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("node: engine URL %q: want https://host:port", raw)
	}
	if u.Scheme == "http" && !api.LoopbackHost(u.Host) {
		return nil, fmt.Errorf("node: engine URL %s: a node reaches an engine on another host over TLS only: "+
			"use https (spec section 15.7)", raw)
	}
	u.Path, u.RawQuery, u.Fragment = "", "", ""
	return u, nil
}

// Fatal is closed when the transport met a fatal error (Err): the node stops.
func (t *Client) Fatal() <-chan struct{} { return t.fatal }

// Err is the fatal error, once Fatal is closed.
func (t *Client) Err() error {
	select {
	case <-t.fatal:
		return t.fatalErr
	default:
		return nil
	}
}

func (t *Client) die(err error) error {
	t.fatalOnce.Do(func() {
		t.fatalErr = err
		close(t.fatal)
	})
	return err
}

// HeartbeatInterval is the heartbeat interval under the lease duration of the engine the node registered with last
// (node.Options.Heartbeat, node.HeartbeatFor).
func (t *Client) HeartbeatInterval() time.Duration {
	return node.HeartbeatFor(time.Duration(t.Registered().LeaseTTLMs) * time.Millisecond)
}

// Registered is what the engine answered the last registration.
func (t *Client) Registered() nodewire.Registered {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reg
}

// base returns the engine URL requests go to now.
func (t *Client) base() *url.URL {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.urls[t.cur]
}

// rotate moves on to the next engine URL, after from failed.
func (t *Client) rotate(from *url.URL) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.urls[t.cur] == from {
		t.cur = (t.cur + 1) % len(t.urls)
	}
}

// follow moves to the active engine a standby named at address (host:port), reached as from is: added to the URLs
// tried in turn. An unspecified host (an engine listening on every interface) names nothing reachable: the next
// URL is tried instead.
func (t *Client) follow(from *url.URL, address string) {
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	if err != nil || host == "" || (ip != nil && ip.IsUnspecified()) {
		t.rotate(from)
		return
	}
	u, err := CheckURL(from.Scheme + "://" + net.JoinHostPort(host, port))
	if err != nil {
		t.rotate(from)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	i := slices.IndexFunc(t.urls, func(x *url.URL) bool { return x.Host == u.Host && x.Scheme == u.Scheme })
	if i < 0 {
		t.urls = append(t.urls, u)
		i = len(t.urls) - 1
	}
	t.cur = i
}

// answer is an engine's error answer.
type answer struct {
	status int
	code   string
	msg    string
	active string // a standby's ActiveAddressHeader
}

func (a *answer) Error() string {
	return fmt.Sprintf("engine answered %d %s: %s", a.status, a.code, a.msg)
}

// once makes one request of the engine at base and decodes a success into out (when not nil).
func (t *Client) once(ctx context.Context, method string, base *url.URL, path string, q url.Values,
	payload []byte, out any, timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	u := *base
	u.Path = path
	u.RawQuery = q.Encode()
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+t.opts.Token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := t.opts.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readAnswer(resp)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if raw, ok := out.(*[]byte); ok {
		*raw, err = io.ReadAll(resp.Body)
		return err
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("engine answered %d with an unreadable body: %w", resp.StatusCode, err)
	}
	if c, ok := out.(interface{ Check() error }); ok {
		return c.Check()
	}
	return nil
}

func readAnswer(resp *http.Response) error {
	a := &answer{status: resp.StatusCode, active: resp.Header.Get(api.ActiveAddressHeader)}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var e nodewire.Error
	if json.Unmarshal(body, &e) == nil && e.Error.Code != "" {
		a.code, a.msg = e.Error.Code, e.Error.Message
	} else {
		a.msg = strings.TrimSpace(string(body))
		switch resp.StatusCode {
		case http.StatusServiceUnavailable:
			a.code = nodewire.CodeUnavailable // an engine between terms
		case http.StatusNotFound:
			a.code = nodewire.CodeNotFound
		default:
			a.code = nodewire.CodeInternal
		}
	}
	return a
}

// call makes a request of the active engine, following it: an engine that cannot be reached, or a standby, moves
// the next attempt to another engine; an engine that does not know the node has it register first. With retry, it
// tries again with a growing backoff until the engine answers or ctx ends; without, it returns the first failure.
// A refused token or a revocation is fatal; a stale lease wraps store.ErrStaleLease, a lease not held
// engine.ErrNotHeld's meaning (errNotHeld).
func (t *Client) call(ctx context.Context, method, path string, q url.Values, in, out any, retry bool,
	timeout time.Duration,
) error {
	var payload []byte
	if in != nil {
		var err error
		if payload, err = json.Marshal(in); err != nil {
			return err
		}
	}
	backoff := minBackoff
	for tries := 1; ; tries++ {
		if err := t.Err(); err != nil {
			return err
		}
		base, regs := t.base(), t.regCount.Load()
		err := t.once(ctx, method, base, path, q, payload, out, timeout)
		if err == nil {
			return nil
		}
		var a *answer
		switch {
		case !errors.As(err, &a):
			t.rotate(base) // unreachable: try the next engine
		case a.code == nodewire.CodeUnregistered:
			if rerr := t.reregister(ctx, regs); rerr != nil {
				return rerr
			}
			continue
		case a.code == nodewire.CodeUnauthorized:
			return t.die(ErrUnauthorized)
		case a.code == nodewire.CodeRevoked:
			return t.die(ErrRevoked)
		case a.code == nodewire.CodeStale:
			return fmt.Errorf("%s: %w", a.msg, store.ErrStaleLease)
		case a.code == nodewire.CodeNotHeld:
			return fmt.Errorf("%s: %w", a.msg, errNotHeld)
		case a.code == nodewire.CodeUnavailable && a.active != "":
			t.follow(base, a.active)
		case a.code == nodewire.CodeUnavailable:
			t.rotate(base)
		case a.status < 500:
			return err // the request itself is refused: sending it again changes nothing
		}
		if !retry || ctx.Err() != nil {
			return err
		}
		if !t.wait(ctx, tries, &backoff) {
			return err
		}
	}
}

// wait backs off before the next try, once every engine was tried since the last back-off: after a failure the
// next engine is tried at once. It reports false when ctx ended first.
func (t *Client) wait(ctx context.Context, tries int, backoff *time.Duration) bool {
	t.mu.Lock()
	n := len(t.urls)
	t.mu.Unlock()
	if tries%n != 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(*backoff):
	}
	*backoff = min(2**backoff, maxBackoff)
	return true
}

// errNotHeld is the engine refusing a fetch for a lease the node does not hold.
var errNotHeld = errors.New("the node holds no such lease")

// Register registers the node with the active engine, trying the engines in turn until one accepts it, ctx ends or
// the engine refuses it for good (fatal).
func (t *Client) Register(ctx context.Context) (nodewire.Registered, error) {
	return t.register(ctx)
}

// reregister registers again, unless another call did since regs.
func (t *Client) reregister(ctx context.Context, regs int64) error {
	t.regMu.Lock()
	defer t.regMu.Unlock()
	if t.regCount.Load() != regs {
		return nil
	}
	_, err := t.registerLocked(ctx)
	return err
}

func (t *Client) register(ctx context.Context) (nodewire.Registered, error) {
	t.regMu.Lock()
	defer t.regMu.Unlock()
	return t.registerLocked(ctx)
}

func (t *Client) registerLocked(ctx context.Context) (nodewire.Registered, error) {
	req := t.opts.Node
	req.Protocol = nodewire.Protocol
	payload, err := json.Marshal(req)
	if err != nil {
		return nodewire.Registered{}, err
	}
	backoff := minBackoff
	for tries := 1; ; tries++ {
		base := t.base()
		var reg nodewire.Registered
		err := t.once(ctx, http.MethodPost, base, nodewire.PathRegister, nil, payload, &reg, attemptTimeout)
		var a *answer
		switch {
		case err == nil:
			t.mu.Lock()
			t.reg = reg
			t.mu.Unlock()
			t.regCount.Add(1)
			t.log.Info("registered", "engine", reg.Engine, "epoch", reg.Epoch, "url", base.String(),
				"engineVersion", reg.Version)
			return reg, nil
		case !errors.As(err, &a):
			t.rotate(base)
		case a.code == nodewire.CodeUnauthorized:
			return nodewire.Registered{}, t.die(ErrUnauthorized)
		case a.code == nodewire.CodeRevoked:
			return nodewire.Registered{}, t.die(ErrRevoked)
		case a.code == nodewire.CodeIncompatible:
			return nodewire.Registered{}, t.die(fmt.Errorf("%w: %s", ErrIncompatible, a.msg))
		case a.code == nodewire.CodeUnavailable && a.active != "":
			t.follow(base, a.active)
		case a.code == nodewire.CodeUnavailable || a.status >= 500:
			t.rotate(base)
		default:
			return nodewire.Registered{}, t.die(err)
		}
		t.log.Warn("register", "url", base.String(), "error", err.Error())
		if !t.wait(ctx, tries, &backoff) {
			return nodewire.Registered{}, ctx.Err()
		}
	}
}

// Pull implements node.Transport: a long poll for up to maxN deliveries, each made ready to run here. One that
// cannot run here (its action does not resolve on this node, its agent has no sandbox) is failed at once with its
// class, and one whose package does not arrive as retryable.
func (t *Client) Pull(ctx context.Context, owner string, maxN int) ([]node.Assignment, error) {
	// The engine leases within what is left of the wait (Handler.pull): attemptTimeout above it is a hung
	// connection's.
	wait := t.opts.PullWait
	var res nodewire.Assignments
	if err := t.call(ctx, http.MethodPost, nodewire.PathPull, nil,
		nodewire.Pull{Node: owner, Max: maxN, WaitMs: wait.Milliseconds()}, &res, false,
		wait+attemptTimeout); err != nil {
		return nil, err
	}
	out := make([]node.Assignment, 0, len(res.Assignments))
	for _, w := range res.Assignments {
		a, err := t.assignment(ctx, w)
		if err != nil {
			t.failUnprepared(ctx, w, err)
			continue
		}
		out = append(out, a)
	}
	return out, nil
}

// assignment makes a wire assignment ready to run here: its package fetched or found in the cache, its process
// spec built against it.
func (t *Client) assignment(ctx context.Context, w nodewire.Assignment) (node.Assignment, error) {
	lease := w.Lease.ToLease()
	pkg, err := t.pkgs.get(w.Launch.PackageHash, func() ([]byte, error) {
		var bundle []byte
		err := t.call(ctx, http.MethodGet, nodewire.PathPackage,
			packageQuery(t.opts.Node.Node, lease.Ref(), w.Launch.PackageHash), nil, &bundle, true, attemptTimeout)
		return bundle, err
	})
	if err != nil {
		return node.Assignment{}, err
	}
	spec, err := w.Launch.Spec(node.Host{
		Action: func(ref string) (*pipeline.Action, string, error) { return t.pkgs.resolve(pkg, ref) },
		Env:    t.opts.Env, Agents: t.opts.Agents, Log: t.log.With("pipeline", lease.Pipeline, "step", lease.Step,
			"deliveryId", lease.DeliveryID),
	}, w.Secrets)
	if err != nil {
		return node.Assignment{}, err
	}
	return node.Assignment{Lease: lease, Launch: w.Launch, Secrets: w.Secrets, Spec: spec, ItemMsg: w.Item,
		Timeout: time.Duration(w.TimeoutMs) * time.Millisecond, Redact: redact.New(w.Secrets)}, nil
}

// failUnprepared fails a delivery the node could not make ready to run.
func (t *Client) failUnprepared(ctx context.Context, w nodewire.Assignment, err error) {
	class, retryable := ClassTransport, true
	var le *node.LaunchError
	if errors.As(err, &le) {
		class, retryable = le.Class, le.Retryable
	}
	red := redact.New(w.Secrets)
	t.log.Warn("delivery not prepared", "pipeline", w.Lease.Pipeline, "step", w.Lease.Step,
		"deliveryId", w.Lease.DeliveryID, "class", class, "error", red.String(err.Error()))
	lease := w.Lease.ToLease()
	if ferr := t.Fail(ctx, store.FailRequest{LeaseRef: lease.Ref(), Attempt: lease.Attempt, Class: class,
		Message: red.String(err.Error()), Retryable: retryable}); ferr != nil {
		t.log.Warn("fail unprepared delivery", "deliveryId", w.Lease.DeliveryID, "error", ferr.Error())
	}
}

// Heartbeat implements node.Transport: one attempt, the next heartbeat trying again.
func (t *Client) Heartbeat(ctx context.Context, owner string, refs []store.LeaseRef) ([]store.LeaseRef, error) {
	var res nodewire.Lost
	if err := t.call(ctx, http.MethodPost, nodewire.PathHeartbeat, nil,
		nodewire.Heartbeat{Node: owner, Leases: nodewire.FromRefs(refs)}, &res, false, attemptTimeout); err != nil {
		return nil, err
	}
	return nodewire.ToRefs(res.Lost), nil
}

// Done implements node.Transport, resending until the engine accepts it (nodewire.Ack: an empty answer, as a server
// cut off mid-request may give, is no acceptance) or ctx ends: a result buffered across a reconnection or a
// failover, refused as stale when the lease ended meanwhile.
func (t *Client) Done(ctx context.Context, req store.DoneRequest) error {
	return t.call(ctx, http.MethodPost, nodewire.PathDone, nil, nodewire.Done{
		Node: t.opts.Node.Node, Lease: nodewire.FromRef(req.LeaseRef), Attempt: req.Attempt,
		Outputs: nodewire.FromOutputs(req.Outputs), CostUSD: req.CostUSD, Artifacts: req.Artifacts,
		Metrics: req.Metrics,
	}, &nodewire.Ack{}, true, attemptTimeout)
}

// Fail implements node.Transport, resending as Done does.
func (t *Client) Fail(ctx context.Context, req store.FailRequest) error {
	return t.call(ctx, http.MethodPost, nodewire.PathFail, nil, nodewire.Fail{
		Node: t.opts.Node.Node, Lease: nodewire.FromRef(req.LeaseRef), Attempt: req.Attempt, Class: req.Class,
		Message: req.Message, Retryable: req.Retryable, CostUSD: req.CostUSD, Unreported: req.Unreported,
		Artifacts: req.Artifacts, Metrics: req.Metrics,
	}, &nodewire.Ack{}, true, attemptTimeout)
}

// Leave tells the engine the node is leaving: the leases it still holds end at once.
func (t *Client) Leave(ctx context.Context) error {
	return t.call(ctx, http.MethodPost, nodewire.PathLeave, nil, nodewire.Leave{Node: t.opts.Node.Node}, &nodewire.Ack{}, false,
		attemptTimeout)
}

// artifactURL asks the engine for a URL moving artifact a of the leased delivery.
func (t *Client) artifactURL(ctx context.Context, lease store.LeaseRef, op string, a store.ArtifactRef) (
	nodewire.URL, error,
) {
	var u nodewire.URL
	err := t.call(ctx, http.MethodPost, nodewire.PathArtifactURL, nil, nodewire.ArtifactURL{
		Node: t.opts.Node.Node, Lease: nodewire.FromRef(lease), Op: op, Artifact: a,
	}, &u, true, attemptTimeout)
	return u, err
}

// transfer makes the request that moves an artifact: to the presigned URL as is, or to the engine's proxy with the
// node token.
func (t *Client) transfer(ctx context.Context, method string, u nodewire.URL, body io.Reader, size int64,
	contentType string, header http.Header,
) (*http.Response, error) {
	target := u.URL
	if !u.Direct {
		rel, err := url.Parse(u.URL)
		if err != nil {
			return nil, err
		}
		target = t.base().ResolveReference(rel).String()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	if !u.Direct {
		req.Header.Set("Authorization", "Bearer "+t.opts.Token)
	}
	if body != nil {
		req.ContentLength = size
		req.Header.Set("Content-Type", contentType)
	}
	maps.Copy(req.Header, header)
	resp, err := t.opts.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer func() { _ = resp.Body.Close() }()
		return nil, fmt.Errorf("artifact %s: %w", method, readAnswer(resp))
	}
	return resp, nil
}

// OpenArtifact implements node.Artifacts: an input of a delivery the node holds, read through the URL the engine
// issues for it.
func (t *Client) OpenArtifact(ctx context.Context, lease store.LeaseRef, ref store.ArtifactRef) (io.ReadCloser,
	error,
) {
	u, err := t.artifactURL(ctx, lease, nodewire.OpGet, ref)
	if err != nil {
		return nil, fmt.Errorf("open artifact %s: %w", ref.Name, err)
	}
	resp, err := t.transfer(ctx, http.MethodGet, u, nil, 0, "", nil)
	if err != nil {
		return nil, fmt.Errorf("open artifact %s: %w", ref.Name, err)
	}
	return resp.Body, nil
}

// PutArtifact implements node.Artifacts: an output of a delivery the node holds, its reference computed here, sent
// through the URL the engine issues for it unless the store holds its content already.
func (t *Client) PutArtifact(ctx context.Context, lease store.LeaseRef, name string, r io.Reader) (
	store.ArtifactRef, error,
) {
	if !store.ValidArtifactName(name) {
		return store.ArtifactRef{}, fmt.Errorf("artifact name %q: want a file name without a path separator", name)
	}
	ref, f, err := artifacts.Spool(t.opts.TmpDir, name, r)
	if err != nil {
		return store.ArtifactRef{}, err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	u, err := t.artifactURL(ctx, lease, nodewire.OpPut, ref)
	if err != nil {
		return store.ArtifactRef{}, fmt.Errorf("put artifact %s: %w", name, err)
	}
	if u.Exists {
		return ref, nil
	}
	var header http.Header
	if u.Direct { // the presigned URL signs the content's checksum (artifacts.Presigner)
		header = http.Header{artifacts.ChecksumHeader: {artifacts.ChecksumSHA256(ref.SHA256)}}
	}
	resp, err := t.transfer(ctx, http.MethodPut, u, f, ref.Size, ref.Type, header)
	if err != nil {
		return store.ArtifactRef{}, fmt.Errorf("put artifact %s: %w", name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if !u.Direct {
		var stored store.ArtifactRef
		if err := json.NewDecoder(resp.Body).Decode(&stored); err != nil || stored.SHA256 != ref.SHA256 {
			return store.ArtifactRef{}, fmt.Errorf("put artifact %s: the engine stored %+v (%v)", name, stored, err)
		}
	}
	return ref, nil
}
