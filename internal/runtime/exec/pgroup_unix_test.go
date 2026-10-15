//go:build unix

package exec

import (
	"errors"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// A timeout kills the action's whole process group: a child it started does not outlive it.
func TestTimeoutKillsChildren(t *testing.T) {
	t.Parallel()
	logs := &syncBuffer{}
	opts := testOpts
	opts.Logger = newJSONLogger(logs)
	p, err := Start(t.Context(), fakeSpec("child", nil), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	d := delivery("dl_1", `{}`)
	d.Timeout = 500 * time.Millisecond
	_, err = p.Deliver(t.Context(), d)
	requireClass(t, err, ClassTimeout)
	waitExited(t, p)
	m := regexp.MustCompile(`child (\d+)`).FindStringSubmatch(logs.String())
	if m == nil {
		t.Fatalf("no child pid logged:\n%s", logs.String())
	}
	pid, _ := strconv.Atoi(m[1])
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			t.Fatalf("child %d outlived its killed parent", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
