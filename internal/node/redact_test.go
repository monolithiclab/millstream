package node

import (
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// The node redacts the delivery's secrets from the action's stderr, its own logs and the error it reports.
func TestNodeRedactsSecrets(t *testing.T) {
	t.Parallel()
	const secret = "tok-5f2a9c71"
	var logs syncBuffer
	a := assignment("dl_1", "secret", map[string]any{"secret": secret})
	a.Redact = redact.New(map[string]string{"apiToken": secret})
	tr := newFakeTransport(a)
	pool := exec.NewPool(exec.PoolOptions{})
	stop := startNode(t, tr, Options{Logger: newJSONLogger(&logs), Pool: pool})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_ = pool.Close() // drains the action's stderr
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.fails) != 1 || tr.fails[0].Message != "portal refused token [redacted:apiToken]" {
		t.Fatalf("fails = %+v; want the message redacted", tr.fails)
	}
	out := logs.String()
	if strings.Contains(out, secret) || !strings.Contains(out, "calling the portal with [redacted:apiToken]") {
		t.Fatalf("logs:\n%s", out)
	}
}
