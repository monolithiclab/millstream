package improve

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/monolithiclab/millstream/internal/api"
)

// client calls the public HTTP API with the proposals-only token.
type client struct {
	base, token string
	http        *http.Client
}

// apiError is an API answer other than a success: its status and the error's code and message.
type apiError struct {
	method, path string
	status       int
	code, msg    string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s %s: %d %s: %s", e.method, e.path, e.status, e.code, e.msg)
}

// retryable reports whether the request may succeed later: a server error or an unavailable engine.
func (e *apiError) retryable() bool { return e.status >= 500 || e.status == http.StatusTooManyRequests }

// do sends a request, body encoded as JSON when not nil, and decodes a success into out when not nil. A transport
// failure is an apiError with status 503, retryable.
func (c *client) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	unreachable := func(err error) error {
		return &apiError{method: method, path: path, status: http.StatusServiceUnavailable, code: "unreachable",
			msg: err.Error()}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return unreachable(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return unreachable(err)
	}
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		return &apiError{method: method, path: path, status: resp.StatusCode, code: e.Error.Code, msg: e.Error.Message}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	return nil
}

func (c *client) get(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

// insights lists the insights, of one pipeline or of all, in one state or all.
func (c *client) insights(ctx context.Context, pipelineName, state string) ([]api.Insight, error) {
	q := url.Values{}
	if pipelineName != "" {
		q.Set("pipeline", pipelineName)
	}
	if state != "" {
		q.Set("state", state)
	}
	var out []api.Insight
	return out, c.get(ctx, "/insights?"+q.Encode(), &out)
}

// files returns a revision's package files, the pipeline's active one when revision is 0.
func (c *client) files(ctx context.Context, pipelineName string, revision int64) (api.RevisionFiles, error) {
	var out api.RevisionFiles
	if revision == 0 {
		var p api.Pipeline
		if err := c.get(ctx, "/pipelines/"+url.PathEscape(pipelineName), &p); err != nil {
			return out, err
		}
		revision = p.Revision
	}
	return out, c.get(ctx, fmt.Sprintf("/pipelines/%s/revisions/%d/files", url.PathEscape(pipelineName), revision),
		&out)
}

// proposals lists a pipeline's proposals.
func (c *client) proposals(ctx context.Context, pipelineName, state string) ([]api.Proposal, error) {
	q := url.Values{}
	if pipelineName != "" {
		q.Set("pipeline", pipelineName)
	}
	if state != "" {
		q.Set("state", state)
	}
	var out []api.Proposal
	return out, c.get(ctx, "/proposals?"+q.Encode(), &out)
}

func (c *client) proposal(ctx context.Context, id string) (api.Proposal, error) {
	var out api.Proposal
	return out, c.get(ctx, "/proposals/"+url.PathEscape(id), &out)
}

func (c *client) item(ctx context.Context, pipelineName, id string) (api.Item, error) {
	var out api.Item
	return out, c.get(ctx, "/pipelines/"+url.PathEscape(pipelineName)+"/items/"+url.PathEscape(id), &out)
}

func (c *client) lineage(ctx context.Context, pipelineName, id string) (api.Lineage, error) {
	var out api.Lineage
	return out, c.get(ctx, "/pipelines/"+url.PathEscape(pipelineName)+"/items/"+url.PathEscape(id)+"/lineage", &out)
}

// publish asks the engine to publish an accepted proposal whose validated patch hashes to patchSHA256.
func (c *client) publish(ctx context.Context, id, patchSHA256 string) (api.PublishedProposal, error) {
	var out api.PublishedProposal
	err := c.do(ctx, http.MethodPost, "/proposals/"+url.PathEscape(id)+"/publish",
		api.PublishRequest{PatchSHA256: patchSHA256}, &out)
	return out, err
}
