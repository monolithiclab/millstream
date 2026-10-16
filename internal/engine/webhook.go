package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/schema"
	"github.com/monolithiclab/millstream/internal/store"
)

// Webhook triggers (spec section 4.6): each has a token of its own, generated when an apply first meets the
// trigger and shown once; the store keeps its SHA-256 only. A request to the trigger's URL fires it with its body
// as the payload, validated against the trigger's input schema, keyed by its request id.

// triggerTokenPrefix starts every webhook token, so secret scanners and people can tell one when it leaks.
const triggerTokenPrefix = "mswh_"

// maxRequestIDLen bounds a request id: a manual firing's, a webhook request's or a replay's, part of its run's id.
const maxRequestIDLen = 128

// NewTriggerToken returns a new webhook token, 32 random bytes in URL-safe base64 after triggerTokenPrefix, and the
// hash the store keeps of it.
func NewTriggerToken() (token, hash string) {
	b := make([]byte, 32)
	_, _ = rand.Read(b) // crypto/rand never fails on the platforms Go supports
	token = triggerTokenPrefix + base64.RawURLEncoding.EncodeToString(b)
	return token, TriggerTokenHash(token)
}

// TriggerTokenHash is what the store keeps of a webhook token: its SHA-256, in hex.
func TriggerTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// createTriggerTokens gives each webhook trigger of p without a token a new one, and returns the tokens it stored by
// trigger: those that had one keep it, so applying a package again never breaks its callers.
func (e *Engine) createTriggerTokens(ctx context.Context, p *pipeline.Pipeline) (map[string]string, error) {
	tokens, hashes := map[string]string{}, map[string]string{}
	for name, t := range p.Triggers {
		if t.Webhook != nil {
			tokens[name], hashes[name] = NewTriggerToken()
		}
	}
	created, err := e.st.CreateTriggerTokens(ctx, p.Name, hashes)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(created))
	for _, name := range created {
		out[name] = tokens[name]
	}
	return out, nil
}

// FireWebhook fires a webhook trigger of the pipeline's active revision for a request: key is its request id
// (generated when empty), payload its body. It returns the run id, and created false when the request id had
// already fired it. Fire's rules apply, and each request takes a token of the trigger's rate first: over it, the
// error is a *RateLimitError.
func (e *Engine) FireWebhook(ctx context.Context, pipelineName, trigger, key string, payload json.RawMessage) (string,
	bool, error,
) {
	return e.fireRequest(ctx, store.SourceWebhook, pipelineName, trigger, key, payload)
}

// fireRequest fires a trigger for a request, by hand or a webhook's, under its request id.
func (e *Engine) fireRequest(ctx context.Context, source store.TriggerSource, pipelineName, trigger, key string,
	payload json.RawMessage,
) (string, bool, error) {
	key, err := e.requestKey(key)
	if err != nil {
		return "", false, fmt.Errorf("fire %s.%s: %w", pipelineName, trigger, err)
	}
	return e.fire(ctx, store.Firing{
		Pipeline: pipelineName, Trigger: trigger, Key: key, RunID: ids.ManualRun(key, trigger), Source: source,
		Payload: payload,
	})
}

// requestKey returns a request's id, generated when it gives none (NewRequestID), refusing one that would not make
// a run id: over maxRequestIDLen bytes, or holding anything but printable ASCII without spaces.
func (e *Engine) requestKey(key string) (string, error) {
	if key == "" {
		return NewRequestID(e.opts.Now()), nil
	}
	if len(key) > maxRequestIDLen {
		return "", fmt.Errorf("request id: want 1 to %d characters: %w", maxRequestIDLen, store.ErrInvalid)
	}
	for i := range len(key) {
		if c := key[i]; c <= ' ' || c > '~' {
			return "", fmt.Errorf("request id %q: want printable ASCII characters without spaces: %w", key,
				store.ErrInvalid)
		}
	}
	return key, nil
}

// RateLimitError refuses a webhook request over its trigger's rate.
type RateLimitError struct {
	Pipeline, Trigger, Rate string
	// Wait is how long until the trigger accepts a request again.
	Wait time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("trigger %s of %s takes at most %s: retry in %s", e.Trigger, e.Pipeline, e.Rate,
		e.Wait.Round(time.Second))
}

// takeWebhook takes a token of the webhook trigger's rate, from its bucket in the engine's memory: full when the
// trigger is first met or its rate changes (spec section 4.6).
func (e *Engine) takeWebhook(pipelineName, trigger, rate string) error {
	count, per, err := pipeline.ParseRate(rate)
	if err != nil { // parsing refuses an invalid rate: a stored revision always has a valid one
		return nil
	}
	now := e.opts.Now()
	k := store.StepRef{Pipeline: pipelineName, Step: pipeline.TriggerStreamPrefix + trigger}
	e.mu.Lock()
	defer e.mu.Unlock()
	b := e.hookBuckets[k]
	if b == nil || b.Rate() != rate {
		b = NewBucket(rate, count, per, now)
		e.hookBuckets[k] = b
	}
	if wait, ok := b.TakeAt(now); !ok {
		return &RateLimitError{Pipeline: pipelineName, Trigger: trigger, Rate: rate, Wait: wait}
	}
	return nil
}

// checkPayload validates a firing's payload (an empty one is {}) against the input schema of the trigger in rev
// (spec section 4.6), compiled once per package and trigger.
func (e *Engine) checkPayload(ctx context.Context, rev *revision, trigger string, payload json.RawMessage) error {
	k := actionKey{rev.hash, pipeline.TriggerStreamPrefix + trigger}
	e.mu.Lock()
	s, ok := e.triggerSchemas[k]
	e.mu.Unlock()
	if !ok && rev.p.Triggers[trigger].InputSchema != nil { // a schema may be a file of the package: load it
		pkg, err := e.pkg(ctx, rev.hash)
		if err != nil {
			return fmt.Errorf("the input schema of trigger %s: %w", trigger, err)
		}
		doc, err := pkg.TriggerSchema(trigger)
		if err == nil && doc != nil {
			s, err = schema.Compile(doc)
		}
		if err != nil {
			return fmt.Errorf("the input schema of trigger %s: %w", trigger, err)
		}
		e.mu.Lock()
		e.triggerSchemas[k] = s
		e.mu.Unlock()
	}
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	if err := validateJSON(s, payload); err != nil {
		return fmt.Errorf("the payload does not match the input schema of trigger %s: %w: %w", trigger, err,
			store.ErrInvalid)
	}
	return nil
}
