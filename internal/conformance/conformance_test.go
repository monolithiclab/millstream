package conformance

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
)

// The suite against the in-process fake action (exec.FuncLauncher, an exec.Starter): a conformant mode passes every
// check, and each violation fails the check that owns it, skipping those that need it. The real-process twin, through
// millstream test action --conformance, is in the cli package's e2e tests, with the other violations.
func TestRunInProcess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		mode string
		want map[string]string // check to status and a part of its reason
	}{
		{"keyed", map[string]string{
			CheckReady: Pass, CheckUnknownKeys: Pass, CheckAuditItem: Pass, CheckFraming: Pass, CheckItemAnswer: Pass,
			CheckKeyedOutputs: Pass + " 3 outputs with distinct keys", CheckIdleSilence: Pass,
			CheckMalformedConfig: Pass, CheckShutdown: Pass,
		}},
		{"rules", map[string]string{ // abstaining on its input, as an action a fallback backs may
			CheckItemAnswer: Pass + " abstain: no rule", CheckAuditItem: Pass + " abstain", CheckFraming: Pass,
			CheckKeyedOutputs: Skip,
		}},
		{"garbage", map[string]string{
			CheckFraming: Fail + " malformed line", CheckItemAnswer: Fail + " protocol violation",
			CheckIdleSilence: Skip + " needs item-answer", CheckShutdown: Pass,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			t.Parallel()
			results := Run(t.Context(), Target{
				Spec:     exec.Spec{Action: "fake", Version: "1.0.0", Config: map[string]any{"mode": tt.mode, "n": 3}},
				Launcher: exec.FuncLauncher{F: exectest.Serve}, Input: json.RawMessage(`{"abstain":true}`),
				ConfigSchema: json.RawMessage(`{"type":"object","properties":{"greeting":{"type":"string"}}}`),
				Timeout:      5 * time.Second, IdleWindow: time.Millisecond,
			})
			got := map[string]Result{}
			for _, r := range results {
				got[r.Name] = r
			}
			for check, want := range tt.want {
				status, reason, _ := strings.Cut(want, " ")
				if r := got[check]; r.Status != status || !strings.Contains(r.Reason, reason) {
					t.Errorf("%s = %s %q, want %s with %q", check, r.Status, r.Reason, status, reason)
				}
			}
			if Failed(results) != (tt.mode == "garbage") {
				t.Errorf("Failed = %v for %+v", Failed(results), results)
			}
		})
	}
}
