//go:build e2e

package cli

import (
	"strings"
	"testing"
)

// Proposals through the CLI (spec section 11.3): created and validated to ready, listed and shown, one accepted
// (the overlapping one goes stale), the stale one rejected with its reason; a patch outside its kind's paths is
// refused, and the proposals-only token is minted.
func Test_proposalsLifecycle(t *testing.T) {
	t.Parallel()
	db, pkg := migratedDB(t), runPackage(t, "lifecycle-v1")
	run := func(want int, args ...string) string {
		t.Helper()
		out, _ := mustRun(t, want, append([]string{"--db", db}, args...)...)
		return out
	}
	run(0, "apply", pkg)
	create := func(file string) string {
		t.Helper()
		out := run(0, "proposals", "create", file, "--pipeline", "extra", "--kind", "config", "--rationale", "tighter",
			"--as", "ana")
		if !strings.Contains(out, " config ready on extra ") || !strings.Contains(out, "validation passed: check passed") {
			t.Fatalf("create: %q", out)
		}
		return strings.Fields(out)[0]
	}
	first := create(fixture("proposals/false.diff"))
	second := create(fixture("proposals/n.diff"))
	if out := run(0, "proposals", "list", "--state", "ready"); !strings.Contains(out, first) ||
		!strings.Contains(out, second) {
		t.Fatalf("list: %q", out)
	}
	if out := run(0, "proposals", "show", first); !strings.Contains(out, "rationale: tighter") ||
		!strings.Contains(out, `+steps: { pass: { action: filter, in: trigger.adhoc, config: { expr: "false" } } }`) {
		t.Fatalf("show: %q", out)
	}
	if out := run(0, "proposals", "accept", first, "--as", "ben"); !strings.Contains(out, first+" config accepted") {
		t.Fatalf("accept: %q", out)
	}
	if out := run(0, "proposals", "list", "--state", "stale", "--json"); !strings.Contains(out, `"id":"`+second+`"`) {
		t.Fatalf("stale: %q", out)
	}
	run(1, "proposals", "revalidate", second) // its base is still the active revision
	run(2, "proposals", "reject", second)
	if out := run(0, "proposals", "reject", second, "--reason", "one filter change at a time"); !strings.Contains(out,
		second+" config rejected") {
		t.Fatalf("reject: %q", out)
	}
	if out := run(0, "proposals", "list", "--state", "rejected"); !strings.Contains(out, "one filter change at a time") {
		t.Fatalf("rejected: %q", out)
	}
	if _, errOut := mustRun(t, 1, "--db", db, "proposals", "create", fixture("proposals/ci.diff"), "--pipeline", "extra",
		"--kind", "topology"); !strings.Contains(errOut, "may not create .github/workflows/ci.yaml") {
		t.Fatalf("a path outside the kind's: %q", errOut)
	}
	if out := run(0, "proposals", "token"); !strings.HasPrefix(out, "mspr_") {
		t.Fatalf("token: %q", out)
	}
}
