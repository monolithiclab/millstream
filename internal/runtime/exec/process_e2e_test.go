//go:build e2e

package exec

import (
	"strings"
	"testing"
	"time"
)

// These tests need a real command (/bin/sh, a missing binary); process_test.go has in-process versions of what
// does not.

func TestStartFailures(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	noCommand := fakeSpec("echo", nil)
	noCommand.Command = nil
	missing := fakeSpec("echo", nil)
	missing.Command = []string{"/nonexistent/action"}
	tests := []struct {
		name  string
		spec  Spec
		opts  Options
		class string
	}{
		{"no command", noCommand, testOpts, ClassCrash},
		{"missing binary", missing, testOpts, ClassCrash},
		{"no ready", fakeSpec("noReady", nil), Options{ReadyTimeout: 200 * time.Millisecond, ShutdownTimeout: 100 * time.Millisecond}, ClassTimeout},
		{"exits before ready", Spec{Action: "lab/false", Command: []string{"/bin/sh", "-c", "exit 4"}}, testOpts, ClassCrash},
		{"message before ready", Spec{Action: "lab/early", Command: []string{"/bin/sh", "-c", `echo '{"type":"done","delivery":"x"}'; exec sleep 5`}}, testOpts, ClassProtocol},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := Start(ctx, tt.spec, tt.opts)
			if p != nil {
				t.Fatal("Start returned a process on failure")
			}
			requireClass(t, err, tt.class)
		})
	}
}

func TestStderrOutsideDeliveryAndLongLines(t *testing.T) {
	t.Parallel()
	var base syncBuffer
	opts := testOpts
	opts.Logger = newJSONLogger(&base)
	long := strings.Repeat("x", maxLogLine+100)
	script := `printf '%s\n' "$1" >&2; echo starting >&2; echo '{"type":"ready"}'; read -r hello; exit 0`
	p, err := Start(t.Context(), Spec{Action: "lab/sh", Command: []string{"/bin/sh", "-c", script, "sh", long}}, opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	lines := jsonLogs(t, base.String())
	if len(lines) != 2 || len(lines[0]["msg"].(string)) != maxLogLine || lines[1]["msg"] != "starting" {
		t.Errorf("got %d lines; second = %v", len(lines), lines[len(lines)-1]["msg"])
	}
}

// TestOversizedItemAtMaxLine: TestOversizedItemIsPermanentAndKeepsProcess at the real limit, protocol.MaxLine.
func TestOversizedItemAtMaxLine(t *testing.T) {
	t.Parallel()
	testOversizedItem(t, testOpts)
}
