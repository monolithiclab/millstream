package engine

import (
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

const secretPipeline = `
pipeline: portal
secrets: [apiToken]
triggers:
  adhoc: { manual: {} }
steps:
  fetch:
    action: ./actions/fake
    in: trigger.adhoc
    config: { mode: secret, secret: "${secrets.apiToken}" }
`

const secretValue = "tok-5f2a9c71"

// The engine hands the node a redactor of the delivery's secrets, for its logs, and redacts them from the failure the
// node reports: the stored error, the dead letter and the dead-letter payload hold [redacted:apiToken] (spec section
// 4.5). TestSecretsRedacted, in the e2e build, runs the whole path through a real node and action.
func TestFailureRedacted(t *testing.T) {
	v := newEnv(t, Options{Env: func(k string) (string, bool) {
		return secretValue, k == pipeline.SecretEnv("apiToken")
	}})
	v.apply(newPackage(t, strings.Replace(secretPipeline, "./actions/fake", "./actions/a", 1), "a"))
	id := v.fire("portal", "adhoc", "k")
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("Pull = %v, %v", as, err)
	}
	a := as[0]
	const redacted = "[redacted:apiToken]"
	if a.Spec.Config["secret"] != secretValue || a.Redact.String("echo "+secretValue) != "echo "+redacted {
		t.Fatalf("config %v; the redactor turns the secret into %q", a.Spec.Config, a.Redact.String(secretValue))
	}
	if err := v.e.Fail(v.ctx, store.FailRequest{LeaseRef: a.Lease.Ref(), Attempt: 1, Class: node.ClassAction,
		Message: "portal refused " + secretValue}); err != nil {
		t.Fatal(err)
	}
	check := func(what, text string) {
		t.Helper()
		if strings.Contains(text, secretValue) || !strings.Contains(text, redacted) {
			t.Errorf("%s holds the secret or no %s: %s", what, redacted, text)
		}
	}
	ds := v.deliveries(id)
	if len(ds) != 1 || ds[0].State != store.DeliveryDead {
		t.Fatalf("deliveries = %+v; want one dead", ds)
	}
	check("stored error", ds[0].ErrorMessage)
	dls, err := v.st.DeadLetters(v.ctx, store.DeadLetterFilter{RunID: id})
	if err != nil || len(dls) != 1 {
		t.Fatalf("dead letters = %+v, %v", dls, err)
	}
	check("dead letter", dls[0].Error)
	items, err := v.st.Items(v.ctx, store.ItemFilter{Stream: "fetch.dlq"})
	if err != nil || len(items) != 1 {
		t.Fatalf("dlq items = %+v, %v", items, err)
	}
	check("dead-letter payload", string(items[0].Payload))
}
