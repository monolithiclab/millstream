package engine

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
)

const webhookPipeline = `pipeline: p
triggers:
  hook: { webhook: {}, inputSchema: { type: object, required: [city], properties: { city: { type: string } } } }
  adhoc: { manual: {}, inputSchema: { type: object, properties: { n: { type: integer } } } }
steps:
  a: { action: ./actions/a, in: [trigger.hook, trigger.adhoc] }
`

// An apply gives each webhook trigger without a token a new one, shown once; a webhook request fires its trigger
// with a payload that matches the trigger's input schema, keyed by a request id that makes a run id. A manual firing
// is held to the same rules.
func TestWebhookTriggers(t *testing.T) {
	v := newEnv(t, Options{})
	pkg := newPackage(t, webhookPipeline, "a")
	a, err := v.e.Apply(v.ctx, pkg)
	if err != nil {
		t.Fatal(err)
	}
	token := a.Tokens["hook"]
	if len(a.Tokens) != 1 || !strings.HasPrefix(token, triggerTokenPrefix) || !a.Created {
		t.Fatalf("applied %+v; want one token for hook", a)
	}
	if h, err := v.st.TriggerTokenHash(v.ctx, "p", "hook"); err != nil || h != TriggerTokenHash(token) {
		t.Fatalf("stored hash %q, %v; want the token's", h, err)
	}
	if a, err := v.e.Apply(v.ctx, pkg); err != nil || len(a.Tokens) != 0 || a.Created {
		t.Fatalf("applied again: %+v, %v; want the token kept and nothing created", a, err)
	}
	if other, _ := NewTriggerToken(); other == token {
		t.Fatal("two tokens alike")
	}

	id, created, err := v.e.FireWebhook(v.ctx, "p", "hook", "req-1", json.RawMessage(`{"city":"Lyon"}`))
	if err != nil || !created || id != "run_req-1_hook" {
		t.Fatalf("FireWebhook = %s, %v, %v", id, created, err)
	}
	if r := v.run("p", id); r.TriggerSource != store.SourceWebhook || r.TriggerKey != "req-1" {
		t.Fatalf("run %+v; want a webhook firing keyed by its request id", r)
	}
	if again, created, err := v.e.FireWebhook(v.ctx, "p", "hook", "req-1", json.RawMessage(`{"city":"Lyon"}`)); err != nil ||
		created || again != id {
		t.Fatalf("FireWebhook again = %s, %v, %v; want the same run", again, created, err)
	}
	if id, _, err := v.e.FireWebhook(v.ctx, "p", "hook", "", json.RawMessage(`{"city":"Paris"}`)); err != nil ||
		!strings.HasPrefix(id, "run_2026") {
		t.Fatalf("FireWebhook without a request id = %s, %v", id, err)
	}
	for _, c := range []struct {
		name, trigger, key, payload string
		want                        error
	}{
		{"payload outside the schema", "hook", "r2", `{"city":1}`, store.ErrInvalid},
		{"missing field", "hook", "r3", `{}`, store.ErrInvalid},
		{"empty payload", "hook", "r4", ``, store.ErrInvalid},
		{"not JSON", "hook", "r5", `{`, store.ErrInvalid},
		{"request id with a space", "hook", "a b", `{"city":"Lyon"}`, store.ErrInvalid},
		{"request id too long", "hook", strings.Repeat("k", maxRequestIDLen+1), `{"city":"Lyon"}`, store.ErrInvalid},
		{"not a webhook trigger", "adhoc", "r6", `{}`, store.ErrNotFound},
		{"no such trigger", "nope", "r7", `{}`, store.ErrInvalid},
	} {
		if _, _, err := v.e.FireWebhook(v.ctx, "p", c.trigger, c.key, json.RawMessage(c.payload)); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", c.name, err, c.want)
		}
	}
	if _, _, err := v.e.Replay(v.ctx, ReplayRequest{Pipeline: "p", Step: "a", RequestID: "a b"}); !errors.Is(err,
		store.ErrInvalid) {
		t.Errorf("replay request id with a space: %v", err)
	}
	if _, _, err := v.e.Fire(v.ctx, "p", "adhoc", "m1", json.RawMessage(`{"n":"x"}`)); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("manual firing outside the schema: %v", err)
	}
	if _, _, err := v.e.Fire(v.ctx, "p", "adhoc", "m2", nil); err != nil {
		t.Errorf("manual firing without a payload: %v", err)
	}
}
