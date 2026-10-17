package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/store"
)

// Webhook triggers (spec section 4.6). POST /hooks/{pipeline}/{trigger} fires a webhook trigger of the pipeline's
// active revision with the request's body. It sits outside /api/v1 and outside the browser guards: the caller is a
// service elsewhere, not a browser, so neither the Host and Origin checks nor auth.token and the session cookie
// apply. The trigger's own token authenticates it instead, as a bearer token or the token query parameter, compared
// by its SHA-256 in constant time; the token is never logged, and the query parameter only for callers that cannot
// set a header, since proxies log URLs.

// HooksPrefix is where webhook triggers are served, hookRoute the pattern of one.
const (
	HooksPrefix = "/hooks/"
	hookRoute   = HooksPrefix + "{pipeline}/{trigger}"
)

// IdempotencyHeader names the request id a webhook request may give: the run's trigger key, so a request sent again
// with the same id fires nothing new.
const IdempotencyHeader = "Idempotency-Key"

// HookPath is the path of a pipeline's webhook trigger.
func HookPath(pipelineName, trigger string) string {
	return HooksPrefix + url.PathEscape(pipelineName) + "/" + url.PathEscape(trigger)
}

// WebhookRun is the answer of a webhook request: the run it fired, and created false when its request id had already
// fired it.
type WebhookRun struct {
	Pipeline string `json:"pipeline"`
	RunID    string `json:"runId"`
	Created  bool   `json:"created"`
}

// hook fires a webhook trigger. It answers 202 with the run, 200 when the request id had already fired it; 401
// without the trigger's token, 404 for a trigger the active revision does not have as a webhook trigger, 429 over the
// trigger's rate or past the address's failed token checks (failures.go), 413 over the body limit, 400 for a body
// that is not JSON or does not match the input schema, 409 for a disabled pipeline (a paused one fires: its
// deliveries wait), and 503 on a standby. The token is checked first, so a caller without it learns nothing of the
// pipeline, the trigger or the deployment.
func (s *Server) hook(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeErrorBody(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, r.Method+" "+r.URL.Path+": allowed POST")
		return nil
	}
	if s.refuseFailing(w, r) {
		return nil
	}
	name, trigger := r.PathValue("pipeline"), r.PathValue("trigger")
	if !s.hookCredential(r, name, trigger) {
		s.failed(r)
		w.Header().Set("WWW-Authenticate", `Bearer realm="millstream-hooks"`)
		writeErrorBody(w, http.StatusUnauthorized, codeUnauthorized,
			"send the trigger's token as Authorization: Bearer <token>, or as the token query parameter")
		return nil
	}
	if s.opts.Standby {
		s.refuseStandby(w, r)
		return nil
	}
	if !jsonContent(r) {
		writeErrorBody(w, http.StatusUnsupportedMediaType, codeUnsupportedMediaType,
			"a webhook request carries Content-Type: application/json")
		return nil
	}
	var body json.RawMessage
	if err := s.decodeBody(w, r, &body); err != nil {
		return err
	}
	if s.opts.Engine == nil {
		return fmt.Errorf("webhook: no engine: %w", errUnavailable)
	}
	id, created, err := s.opts.Engine.FireWebhook(r.Context(), name, trigger, r.Header.Get(IdempotencyHeader), body)
	if limited := (*engine.RateLimitError)(nil); errors.As(err, &limited) {
		writeTooManyRequests(w, limited.Wait, limited.Error())
		return nil
	}
	if err != nil {
		return err
	}
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, WebhookRun{Pipeline: name, RunID: id, Created: created})
	return nil
}

// hookCredential reports whether the request presents the trigger's token: as a bearer token, else as the token
// query parameter, compared by its hash in constant time. A trigger without a token, or a pipeline or trigger that
// does not exist, admits no request.
func (s *Server) hookCredential(r *http.Request, pipelineName, trigger string) bool {
	token, ok := bearerToken(r)
	if !ok {
		token = r.URL.Query().Get("token")
	}
	if token == "" {
		return false
	}
	want, err := s.opts.Store.TriggerTokenHash(r.Context(), pipelineName, trigger)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			s.log.Error("webhook token", "pipeline", pipelineName, "trigger", trigger, "error", err.Error())
		}
		return false
	}
	return subtle.ConstantTimeCompare([]byte(engine.TriggerTokenHash(token)), []byte(want)) == 1
}

// TriggerToken is a webhook trigger's token, shown once: when an apply creates it, or a rotation replaces it.
type TriggerToken struct {
	Trigger string `json:"trigger"`
	Token   string `json:"token"`
	// Path is where the trigger is served, under the server's address.
	Path string `json:"path"`
}

// rotateTriggerToken replaces a webhook trigger's token and answers the new one, once: the old one stops working.
func (s *Server) rotateTriggerToken(w http.ResponseWriter, r *http.Request) error {
	name, trigger := r.PathValue("pipeline"), r.PathValue("trigger")
	token, hash := engine.NewTriggerToken()
	if err := s.opts.Store.RotateTriggerToken(r.Context(), name, trigger, hash); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, TriggerToken{Trigger: trigger, Token: token, Path: HookPath(name, trigger)})
	return nil
}

// triggerTokens lists an apply's new tokens of a pipeline's webhook triggers, by trigger.
func triggerTokens(pipelineName string, tokens map[string]string) []TriggerToken {
	out := make([]TriggerToken, 0, len(tokens))
	for trigger, token := range tokens {
		out = append(out, TriggerToken{Trigger: trigger, Token: token, Path: HookPath(pipelineName, trigger)})
	}
	slices.SortFunc(out, func(a, b TriggerToken) int { return strings.Compare(a.Trigger, b.Trigger) })
	return out
}
