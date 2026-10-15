// Package exec is the exec runtime (spec sections 6.3 to 6.5): it runs an action as a child process speaking the
// JSON-lines protocol of internal/protocol on its stdin and stdout, one delivery at a time. Anything that goes wrong
// below the action itself (no ready, a timeout, an exit mid-delivery, a protocol violation) is a RuntimeError: the
// process is killed and never reused, and the delivery is retried. An action's own error message is an ActionError
// in the Result, retryable as the action says. The process's stderr is re-emitted line by line through slog with
// the delivery's keys; payloads are never logged. A Pool keeps long-lived processes for reuse.
package exec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	osexec "os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/store"
)

// Default timeouts (docs/protocol.md, ordering; spec section 6.5).
const (
	DefaultReadyTimeout    = 10 * time.Second
	DefaultShutdownTimeout = 5 * time.Second
)

// Spec says which process to run: the action, its command and the hello it receives.
type Spec struct {
	Action  string
	Version string
	// Digest identifies the action's content; with the config and the vars it keys process reuse.
	Digest  string
	Command []string
	// Dir is the process's working directory: the action directory on disk.
	Dir string
	// Lifecycle is pipeline.LifecycleLong (the default) or pipeline.LifecyclePerItem.
	Lifecycle string
	Config    map[string]any
	Vars      map[string]any
	// Env is the process's whole environment: the node's own is not inherited, so its secrets do not leak to
	// actions. MILLSTREAM_WORKDIR is added.
	Env []string
}

// Options tune processes. Zero values take the defaults.
type Options struct {
	// Logger receives stderr lines written before the first delivery; nil discards them.
	Logger          *slog.Logger
	ReadyTimeout    time.Duration
	ShutdownTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.ReadyTimeout <= 0 {
		o.ReadyTimeout = DefaultReadyTimeout
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = DefaultShutdownTimeout
	}
	return o
}

// Delivery is one item to hand to a process.
type Delivery struct {
	Item protocol.Item
	// Timeout bounds the delivery from the item to done or error; zero means no bound.
	Timeout time.Duration
	// Log carries the delivery's keys (pipeline, runId, step, itemId, deliveryId, attempt, node): the process's
	// stderr lines are re-emitted through it from this delivery until the next. Nil uses the process's logger.
	Log *slog.Logger
}

// Result is what a delivery produced. Outputs are set only when the action answered done; Err only when it
// answered error.
type Result struct {
	Outputs []store.Output
	Metrics []protocol.Metric
	Err     *ActionError
}

// ActionError is an action's own error message.
type ActionError struct {
	Message   string
	Retryable bool
}

// Process is a running action process. It serves one delivery at a time: Deliver must not be called concurrently.
type Process struct {
	spec    Spec
	opts    Options
	cmd     *osexec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  io.ReadCloser
	w       *protocol.Writer
	workdir string

	// msgs carries stdout's messages; it is closed when stdout ends, after readErr is set.
	msgs    chan protocol.Message
	readErr error
	// dead is closed when the process is killed; exited when Wait has returned, waitErr then set.
	dead     chan struct{}
	killOnce sync.Once
	exited   chan struct{}
	waitErr  error

	logMu sync.Mutex
	log   *slog.Logger // the latest delivery's logger, else opts.Logger
}

// Start spawns the process, sends hello and waits for ready. ctx bounds only the handshake: the process outlives
// it. On failure the process is gone and the error is a RuntimeError.
func Start(ctx context.Context, spec Spec, opts Options) (*Process, error) {
	if len(spec.Command) == 0 {
		return nil, runtimeErr(ClassCrash, "action %s has no command", spec.Action)
	}
	opts = opts.withDefaults()
	workdir, err := os.MkdirTemp("", "millstream-work-")
	if err != nil {
		return nil, runtimeErr(ClassCrash, "create workdir: %w", err)
	}
	p := &Process{
		spec: spec, opts: opts, workdir: workdir, log: opts.Logger,
		msgs: make(chan protocol.Message), dead: make(chan struct{}), exited: make(chan struct{}),
	}
	if err := p.spawn(); err != nil {
		_ = os.RemoveAll(workdir)
		return nil, err
	}
	if err := p.handshake(ctx); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func (p *Process) spawn() error {
	// #nosec G204 -- the command comes from the operator's package manifest: running it is the runtime's job.
	cmd := osexec.Command(p.spec.Command[0], p.spec.Command[1:]...)
	cmd.Dir = p.spec.Dir
	cmd.Env = append(append(make([]string, 0, len(p.spec.Env)+1), p.spec.Env...), "MILLSTREAM_WORKDIR="+p.workdir)
	var err error
	if p.stdin, err = cmd.StdinPipe(); err != nil {
		return runtimeErr(ClassCrash, "stdin pipe: %w", err)
	}
	if p.stdout, err = cmd.StdoutPipe(); err != nil {
		return runtimeErr(ClassCrash, "stdout pipe: %w", err)
	}
	if p.stderr, err = cmd.StderrPipe(); err != nil {
		return runtimeErr(ClassCrash, "stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return runtimeErr(ClassCrash, "start %s: %w", p.spec.Command[0], err)
	}
	p.cmd = cmd
	p.w = protocol.NewWriter(p.stdin)
	var readers sync.WaitGroup
	readers.Go(p.readStdout)
	readers.Go(p.relayStderr)
	go func() {
		// Wait only once both pipes are drained, so the last lines are not lost.
		readers.Wait()
		p.waitErr = cmd.Wait()
		close(p.exited)
	}()
	return nil
}

func (p *Process) readStdout() {
	defer close(p.msgs)
	r := protocol.NewReader(p.stdout)
	for {
		m, err := r.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.readErr = err
			}
			return
		}
		select {
		case p.msgs <- m:
		case <-p.dead:
			return
		}
	}
}

func (p *Process) handshake(ctx context.Context) error {
	hello := protocol.Hello{
		Protocol: protocol.Version, Action: p.spec.Action, Version: p.spec.Version,
		Config: p.spec.Config, Vars: p.spec.Vars,
	}
	if err := p.w.Write(hello); err != nil {
		p.kill()
		return p.exitErr(ctx, "send hello: %w", err)
	}
	timer := time.NewTimer(p.opts.ReadyTimeout)
	defer timer.Stop()
	select {
	case m, ok := <-p.msgs:
		if !ok {
			return p.exitErr(ctx, "exited before ready")
		}
		if _, ready := m.(protocol.Ready); !ready {
			p.kill()
			return runtimeErr(ClassProtocol, "%w: %s before ready", protocol.ErrProtocol, m.Type())
		}
		return nil
	case <-timer.C:
		p.kill()
		return runtimeErr(ClassTimeout, "no ready within %s", p.opts.ReadyTimeout)
	case <-ctx.Done():
		p.kill()
		return fmt.Errorf("await ready: %w", ctx.Err())
	}
}

// Deliver hands the process one item and collects its answer. A RuntimeError means the process is gone; a
// cancelled ctx kills it too and returns ctx's error. Outputs are returned only on done, so a failed delivery
// publishes nothing.
func (p *Process) Deliver(ctx context.Context, d Delivery) (Result, error) {
	id := d.Item.Delivery
	select {
	case m, ok := <-p.msgs:
		if !ok {
			return Result{}, p.exitErr(ctx, "exited while idle")
		}
		p.kill()
		return Result{}, runtimeErr(ClassProtocol, "%w: %s while idle", protocol.ErrProtocol, m.Type())
	default:
	}
	if err := clearDir(p.workdir); err != nil {
		p.kill()
		return Result{}, runtimeErr(ClassCrash, "clear workdir: %w", err)
	}
	// The logger stays until the next delivery: stderr and stdout are separate pipes, so a line written just
	// before done may be relayed after it.
	if d.Log != nil {
		p.setLog(d.Log)
	} else {
		p.setLog(p.opts.Logger)
	}
	if err := p.w.Write(d.Item); err != nil {
		p.kill()
		return Result{}, p.exitErr(ctx, "send item %s: %w", id, err)
	}
	var timeout <-chan time.Time
	if d.Timeout > 0 {
		timer := time.NewTimer(d.Timeout)
		defer timer.Stop()
		timeout = timer.C
	}
	var res Result
	for {
		select {
		case <-ctx.Done():
			p.kill()
			return Result{}, fmt.Errorf("delivery %s: %w", id, ctx.Err())
		case <-timeout:
			p.kill()
			return Result{}, runtimeErr(ClassTimeout, "delivery %s: no answer within %s", id, d.Timeout)
		case m, ok := <-p.msgs:
			if !ok {
				return Result{}, p.exitErr(ctx, "exited during delivery %s", id)
			}
			done, err := collect(&res, id, m)
			if err != nil {
				p.kill()
				return Result{}, runtimeErr(ClassProtocol, "delivery %s: %w", id, err)
			}
			if done {
				return res, nil
			}
		}
	}
}

// collect adds m to res and reports whether it ended the delivery.
func collect(res *Result, id string, m protocol.Message) (bool, error) {
	var got string
	switch m := m.(type) {
	case protocol.Emit:
		got = m.Delivery
		res.Outputs = append(res.Outputs, store.Output{Key: m.Key, Payload: m.Payload})
	case protocol.Metric:
		got = m.Delivery
		res.Metrics = append(res.Metrics, m)
	case protocol.Done:
		got = m.Delivery
	case protocol.Error:
		got = m.Delivery
		res.Outputs = nil
		res.Err = &ActionError{Message: m.Message, Retryable: m.Retryable}
	default:
		return false, fmt.Errorf("%w: unexpected %s message", protocol.ErrProtocol, m.Type())
	}
	if got != id {
		return false, fmt.Errorf("%w: %s message for delivery %q", protocol.ErrProtocol, m.Type(), got)
	}
	switch m.(type) {
	case protocol.Done, protocol.Error:
		return true, nil
	}
	return false, nil
}

// exitGrace is how long exitErr waits for an exiting process's status.
const exitGrace = time.Second

// exitErr explains why stdout ended: a protocol violation the reader hit, or the process's exit status, waited for
// up to exitGrace. The process is killed either way.
func (p *Process) exitErr(ctx context.Context, format string, args ...any) error {
	cause := fmt.Errorf(format, args...)
	if errors.Is(p.readErrIfDone(), protocol.ErrProtocol) {
		p.kill()
		return &RuntimeError{Class: ClassProtocol, Err: fmt.Errorf("%w: %w", cause, p.readErr)}
	}
	grace := time.NewTimer(exitGrace)
	defer grace.Stop()
	select {
	case <-p.exited:
	case <-grace.C:
	case <-ctx.Done():
	}
	p.kill()
	select {
	case <-p.exited:
		if p.waitErr != nil {
			return &RuntimeError{Class: ClassCrash, Err: fmt.Errorf("%w: %w", cause, p.waitErr)}
		}
	default:
	}
	return &RuntimeError{Class: ClassCrash, Err: cause}
}

// readErrIfDone returns the reader's error once stdout has ended, nil while it still runs.
func (p *Process) readErrIfDone() error {
	select {
	case _, ok := <-p.msgs:
		if !ok {
			return p.readErr
		}
	default:
	}
	return nil
}

// Alive reports whether the process can take another delivery.
func (p *Process) Alive() bool {
	select {
	case <-p.dead:
		return false
	case <-p.exited:
		return false
	default:
		return true
	}
}

// Close sends shutdown, waits for the process to exit, kills it after the shutdown timeout, and removes its
// working directory. It returns an error only when the process had to be killed.
func (p *Process) Close() error {
	var err error
	if p.Alive() && !p.shutdown() {
		err = fmt.Errorf("action %s: killed after not exiting within %s", p.spec.Action, p.opts.ShutdownTimeout)
	}
	p.kill()
	<-p.exited
	if rmErr := os.RemoveAll(p.workdir); rmErr != nil && err == nil {
		err = fmt.Errorf("remove workdir: %w", rmErr)
	}
	return err
}

// shutdown asks the process to exit and reports whether it did within the shutdown timeout.
func (p *Process) shutdown() bool {
	_ = p.w.Write(protocol.Shutdown{})
	_ = p.stdin.Close()
	timer := time.NewTimer(p.opts.ShutdownTimeout)
	defer timer.Stop()
	msgs := p.msgs
	for {
		select {
		case _, ok := <-msgs: // dropped: nothing may follow shutdown, and the process is going anyway
			if !ok {
				msgs = nil
			}
		case <-p.exited:
			return true
		case <-timer.C:
			return false
		}
	}
}

// kill ends the process and closes its pipes, so readers return even if a grandchild holds them open.
func (p *Process) kill() {
	p.killOnce.Do(func() {
		close(p.dead)
		_ = p.cmd.Process.Kill()
		_ = p.stdin.Close()
		_ = p.stdout.Close()
		_ = p.stderr.Close()
	})
}

func (p *Process) setLog(l *slog.Logger) {
	p.logMu.Lock()
	p.log = l
	p.logMu.Unlock()
}

func (p *Process) logger() *slog.Logger {
	p.logMu.Lock()
	defer p.logMu.Unlock()
	return p.log
}

// clearDir empties dir, so each delivery starts with a clean working directory.
func clearDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}
