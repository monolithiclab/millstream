package exec

import (
	osexec "os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// fakeLauncher runs the fake action for its kind and records its calls.
type fakeLauncher struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeLauncher) Command(spec Spec, workdir string) (*osexec.Cmd, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "command "+spec.Image)
	f.mu.Unlock()
	return ExecLauncher{}.Command(Spec{Action: spec.Action, Command: []string{fakeBin}, Dir: spec.Dir}, workdir)
}

func (f *fakeLauncher) Kill(spec Spec, _ string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "kill "+spec.Image)
}

func (f *fakeLauncher) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func containerSpec(mode string) Spec {
	s := fakeSpec(mode, nil)
	s.Kind, s.Image, s.Command = pipeline.KindContainer, "sha256:img", nil
	return s
}

// TestLauncherByKind: a container action starts through the container launcher, shares the protocol and the pool,
// and the launcher's Kill runs when the process is killed, not when it exits cleanly.
func TestLauncherByKind(t *testing.T) {
	t.Parallel()
	fl := &fakeLauncher{}
	opts := testOpts
	opts.Launchers = map[string]Launcher{pipeline.KindContainer: fl}
	pool := NewPool(PoolOptions{Options: opts})
	t.Cleanup(func() { _ = pool.Close() })
	if _, err := pool.Deliver(t.Context(), containerSpec("echo"), delivery("dl_1", `{}`)); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if got := fl.recorded(); !slices.Equal(got, []string{"command sha256:img"}) {
		t.Fatalf("a clean exit: calls %q", got)
	}

	p, err := Start(t.Context(), containerSpec("hang"), opts)
	if err != nil {
		t.Fatal(err)
	}
	d := delivery("dl_2", `{}`)
	d.Timeout = 100 * time.Millisecond
	_, err = p.Deliver(t.Context(), d)
	requireClass(t, err, ClassTimeout)
	_ = p.Close()
	if got := fl.recorded(); !slices.Equal(got, []string{"command sha256:img", "command sha256:img", "kill sha256:img"}) {
		t.Fatalf("a killed process: calls %q", got)
	}
}

func TestStartWithoutLauncher(t *testing.T) {
	t.Parallel()
	_, err := Start(t.Context(), containerSpec("echo"), testOpts)
	requireClass(t, err, ClassCrash)
	if !Permanent(err) || !strings.Contains(err.Error(), "no runtime for it") {
		t.Fatalf("err = %v", err)
	}
	s := fakeSpec("echo", nil)
	s.Command = nil
	_, err = Start(t.Context(), s, testOpts)
	requireClass(t, err, ClassCrash)
	if !strings.Contains(err.Error(), "has no command") {
		t.Fatalf("err = %v", err)
	}
}
