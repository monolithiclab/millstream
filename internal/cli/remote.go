package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/store"
)

// Commands that change state reach the active engine through its API when a server is named (spec section 5.9: on
// Postgres the active engine is the only writer), and write to the store from their own process otherwise: on
// SQLite under its file locking, on Postgres only with --direct, which takes the leadership lock.

// Environment variables of the API client.
const (
	// ServerEnv names the served engine's API URL when --server does not.
	ServerEnv = "MILLSTREAM_SERVER"
	// TokenEnv holds the API's bearer token, sent with every request when set (token auth: M4 Task 7).
	TokenEnv = "MILLSTREAM_TOKEN"
)

// apiTimeout bounds one API request of a CLI command.
const apiTimeout = 30 * time.Second

// apiClient calls a served engine's API.
type apiClient struct {
	base  *url.URL
	token string
	user  string // sent as the reviewer of decisions
	http  *http.Client
}

// remote returns the API client of the server --server or $MILLSTREAM_SERVER names, or nil when none is named or
// --direct asks to write from this process.
func remote(rt *Runtime) (*apiClient, error) {
	if rt.Direct {
		return nil, nil
	}
	server := rt.Server
	if server == "" {
		server, _ = rt.Env(ServerEnv)
	}
	if server == "" {
		return nil, nil
	}
	u, err := url.Parse(server)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, usageError(fmt.Sprintf("--server %q: want the API's base URL, such as http://127.0.0.1:7070", server))
	}
	c := &apiClient{base: u, http: rt.HTTPClient}
	if c.http == nil {
		c.http = &http.Client{Timeout: apiTimeout}
	}
	c.token, _ = rt.Env(TokenEnv)
	return c, nil
}

// apiError is an error answer of the API, matching the store error its code stands for.
type apiError struct {
	code string
	msg  string
}

func (e *apiError) Error() string { return e.msg }

func (e *apiError) Is(target error) bool {
	switch e.code {
	case "notFound":
		return target == store.ErrNotFound
	case "conflict":
		return target == store.ErrConflict
	case "invalid":
		return target == store.ErrInvalid
	}
	return false
}

// do sends a request with body as JSON (none when nil) and decodes a 2xx answer into out when it is not nil. It
// returns the answer's status.
func (c *apiClient) do(ctx context.Context, method, path string, body, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	u := c.base.JoinPath(api.Prefix, path)
	if i := strings.IndexByte(path, '?'); i >= 0 { // JoinPath escapes a query
		u = c.base.JoinPath(api.Prefix, path[:i])
		u.RawQuery = path[i+1:]
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rd)
	if err != nil {
		return 0, err
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.user != "" {
		req.Header.Set(api.UserHeader, c.user)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("reach the server: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	if err != nil {
		return res.StatusCode, fmt.Errorf("read the server's answer: %w", err)
	}
	if res.StatusCode >= 300 {
		var eb struct {
			Error struct{ Code, Message string } `json:"error"`
		}
		if json.Unmarshal(data, &eb) != nil || eb.Error.Code == "" {
			return res.StatusCode, fmt.Errorf("%s %s: %s", method, u.Path, res.Status)
		}
		return res.StatusCode, &apiError{code: eb.Error.Code, msg: eb.Error.Message}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return res.StatusCode, fmt.Errorf("decode the server's answer: %w", err)
		}
	}
	return res.StatusCode, nil
}

// post sends a POST, an empty object when body is nil.
func (c *apiClient) post(ctx context.Context, path string, body, out any) (int, error) {
	if body == nil {
		body = struct{}{}
	}
	return c.do(ctx, http.MethodPost, path, body, out)
}

// pipelinePath is /pipelines/{name} and the parts after it, each escaped.
func pipelinePath(name string, parts ...string) string {
	p := "/pipelines/" + url.PathEscape(name)
	for _, s := range parts {
		p += "/" + url.PathEscape(s)
	}
	return p
}

// errNoVarsRemote refuses --var through the API: a served engine resolves variables from its own environment.
var errNoVarsRemote = usageError("--var: the server resolves variables from its own environment; drop --var or use --direct")

// errRemotePipeline is the usage error of a command that needs --pipeline to go through the API.
func errRemotePipeline(what string) error {
	return usageError(fmt.Sprintf("--pipeline: name the %s's pipeline when going through the server", what))
}
