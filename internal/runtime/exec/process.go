// Package exec is the action process runtime (spec sections 6.3 to 6.5): it runs an action as a child process
// speaking the JSON-lines protocol of internal/protocol on its stdin and stdout, one delivery at a time. A Launcher
// says how the process starts: ExecLauncher runs the action's command directly, the container package runs it under
// Docker or Podman; everything else is shared. Anything that goes wrong below the action itself (no ready, a
// timeout, an exit mid-delivery, a protocol violation) is a RuntimeError: the process is killed and never reused,
// and the delivery is retried. An action's own error message is an ActionError
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

	"github.com/monolithiclab/millstream/internal/pipeline"
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
	// Kind is the runtime that starts the process: pipeline.KindExec (also when empty) or pipeline.KindContainer.
	// Options.Launchers maps it to a Launcher.
	Kind string
	// Digest identifies the action's content; with the config and the vars it keys process reuse.
	Digest string
	// Command is the program and its arguments; for a container, the command run in the image, empty for the
	// image's own.
	Command []string
	// Image is a container action's image, pinned by digest (an image id or repository@digest).
	Image string
	// Network gives a container action the network; it has none otherwise.
	Network bool
	// Dir is an exec process's working directory: the action directory on disk.
	Dir string
	// Lifecycle is pipeline.LifecycleLong (the default) or pipeline.LifecyclePerItem.
	Lifecycle string
	Config    map[string]any
	Vars      map[string]any
	// Env is the process's whole environment: the node's own is not inherited, so its secrets do not leak to
	// actions. MILLSTREAM_WORKDIR is added.
	Env []string
}

// Launcher starts the processes of one kind of action (spec section 6.4). The runtime around it is shared: the
// protocol, the pool, the timeouts and the process group, which the runtime sets on the command and kills.
type Launcher interface {
	// Command returns the command that runs spec, with workdir as the process's MILLSTREAM_WORKDIR.
	Command(spec Spec, workdir string) (*osexec.Cmd, error)
	// Kill releases what outlives the process group once it is killed, such as a container. It is called only when
	// the process did not exit cleanly.
	Kill(spec Spec, workdir string)
}

// ExecLauncher runs exec actions as child processes of the node, in their action directory.
type ExecLauncher struct{}

// Command implements Launcher.
func (ExecLauncher) Command(spec Spec, workdir string) (*osexec.Cmd, error) {
	if len(spec.Command) == 0 {
		return nil, fmt.Errorf("action %s has no command", spec.Action)
	}
	// #nosec G204 -- the command comes from the operator's package manifest: running it is the runtime's job.
	cmd := osexec.Command(spec.Command[0], spec.Command[1:]...)
	cmd.Dir = spec.Dir
	cmd.Env = append(append(make([]string, 0, len(spec.Env)+1), spec.Env...), "MILLSTREAM_WORKDIR="+workdir)
	return cmd, nil
}

// Kill implements Launcher: the process group was everything.
func (ExecLauncher) Kill(Spec, string) {}

// Options tune processes. Zero values take the defaults.
type Options struct {
	// Launchers start the processes of each kind; exec actions default to ExecLauncher. A kind without one fails
	// its deliveries, permanently.
	Launchers map[string]Launcher
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

// launcher returns the launcher of kind.
func (o Options) launcher(kind string) (Launcher, bool) {
	if kind == "" {
		kind = pipeline.KindExec
	}
	if l, ok := o.Launchers[kind]; ok {
		return l, true
	}
	if kind == pipeline.KindExec {
		return ExecLauncher{}, true
	}
	return nil, false
}

// Delivery is one item to hand to a process.
type Delivery struct {
	// Item is the message sent; its Artifacts are set from Inputs once they are placed.
	Item protocol.Item
	// Inputs are the item's artifacts, placed under in/ in the working directory before the item is sent.
	Inputs []store.ArtifactRef
	// Artifacts moves input and output artifacts; nil fails a delivery that has any.
	Artifacts Artifacts
	// Timeout bounds the delivery from the item to done or error; zero means no bound.
	Timeout time.Duration
	// Log carries the delivery's keys (pipeline, runId, step, itemId, deliveryId, attempt, node): the process's
	// stderr lines are re-emitted through it from this delivery until the next. Nil uses the process's logger.
	Log *slog.Logger
}

// Result is what a delivery produced. Outputs are set only when the action answered done, with the artifacts they
// name already in the artifact store; Err only when it answered error.
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
	spec     Spec
	opts     Options
	launcher Launcher
	cmd      *osexec.Cmd
	stdin    io.WriteCloser
	stdout   io.ReadCloser
	stderr   io.ReadCloser
	w        *protocol.Writer
	workdir  string

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

	prev string // the delivery answered last, whose late metrics are ignored
}

// Start spawns the process, sends hello and waits for ready. ctx bounds only the handshake: the process outlives
// it. On failure the process is gone and the error is a RuntimeError.
func Start(ctx context.Context, spec Spec, opts Options) (*Process, error) {
	opts = opts.withDefaults()
	launcher, ok := opts.launcher(spec.Kind)
	if !ok {
		return nil, &RuntimeError{Class: ClassCrash, Permanent: true,
			Err: fmt.Errorf("action %s is of kind %s: this node has no runtime for it", spec.Action, spec.Kind)}
	}
	workdir, err := os.MkdirTemp("", "millstream-work-")
	if err != nil {
		return nil, runtimeErr(ClassCrash, "create workdir: %w", err)
	}
	p := &Process{
		spec: spec, opts: opts, launcher: launcher, workdir: workdir, log: opts.Logger,
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
	cmd, err := p.launcher.Command(p.spec, p.workdir)
	if err != nil {
		return runtimeErr(ClassCrash, "%w", err)
	}
	setProcessGroup(cmd) // so kill reaches the children the action starts
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
		return runtimeErr(ClassCrash, "start %s: %w", cmd.Path, err)
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
	// The ready timeout and ctx bound the whole handshake, the hello write included: a process that never reads
	// stdin must not block it on a full pipe. Killing the process closes stdin and unblocks the write.
	hctx, cancel := context.WithTimeoutCause(ctx, p.opts.ReadyTimeout, errReadyTimeout)
	defer cancel()
	stop := context.AfterFunc(hctx, p.kill)
	defer stop()
	if err := p.w.Write(hello); err != nil {
		if hctx.Err() != nil {
			return p.handshakeErr(hctx)
		}
		p.kill()
		return p.exitErr(ctx, "send hello: %w", err)
	}
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
	case <-hctx.Done():
		return p.handshakeErr(hctx)
	}
}

// errReadyTimeout and errDeliveryTimeout are the causes of a handshake or delivery context ending at its timeout.
var (
	errReadyTimeout    = errors.New("ready timeout")
	errDeliveryTimeout = errors.New("delivery timeout")
)

// handshakeErr explains a handshake whose context ended; the process is killed.
func (p *Process) handshakeErr(hctx context.Context) error {
	p.kill()
	if errors.Is(context.Cause(hctx), errReadyTimeout) {
		return runtimeErr(ClassTimeout, "no ready within %s", p.opts.ReadyTimeout)
	}
	return fmt.Errorf("await ready: %w", hctx.Err())
}

// Deliver places the delivery's input artifacts, hands the process the item and collects its answer; on done it
// moves the artifacts the outputs name to the artifact store before returning. A RuntimeError means the process is
// gone unless Alive says otherwise: an item too large for a protocol line is refused before anything is written, and
// an artifact that cannot be moved fails the delivery alone. A cancelled ctx kills the process and returns ctx's
// error. Outputs are returned only on done, so a failed delivery publishes nothing.
func (p *Process) Deliver(ctx context.Context, d Delivery) (Result, error) {
	id := d.Item.Delivery
	if err := p.drainIdle(ctx); err != nil {
		return Result{}, err
	}
	if err := clearDir(p.workdir); err != nil {
		p.kill()
		return Result{}, runtimeErr(ClassCrash, "clear workdir: %w", err)
	}
	if err := p.materialise(ctx, &d); err != nil {
		return Result{}, err
	}
	// The logger stays until the next delivery: stderr and stdout are separate pipes, so a line written just
	// before done may be relayed after it.
	if d.Log != nil {
		p.setLog(d.Log)
	} else {
		p.setLog(p.opts.Logger)
	}
	// ctx and the timeout bound the whole delivery, the item write included: a process that stops reading stdin
	// must not block it on a full pipe. Killing the process closes stdin and unblocks the write.
	dctx, cancel := ctx, context.CancelFunc(func() {})
	if d.Timeout > 0 {
		dctx, cancel = context.WithTimeoutCause(ctx, d.Timeout, errDeliveryTimeout)
	}
	defer cancel()
	stop := context.AfterFunc(dctx, p.kill)
	defer stop()
	if err := p.w.Write(d.Item); err != nil {
		switch {
		case errors.Is(err, protocol.ErrProtocol): // refused whole: nothing was written
			return Result{}, &RuntimeError{Class: ClassProtocol, Permanent: true, Err: fmt.Errorf("item %s: %w", id, err)}
		case dctx.Err() != nil:
			return Result{}, p.deliveryErr(dctx, d)
		}
		p.kill()
		return Result{}, p.exitErr(ctx, "send item %s: %w", id, err)
	}
	var res Result
	var emitted [][]protocol.Artifact // the artifacts each output named, moved once the delivery is done
	for {
		select {
		case <-dctx.Done():
			return Result{}, p.deliveryErr(dctx, d)
		case m, ok := <-p.msgs:
			if !ok {
				return Result{}, p.exitErr(ctx, "exited during delivery %s", id)
			}
			if p.lateMetric(m) {
				continue
			}
			done, err := collect(&res, &emitted, id, m)
			if err != nil {
				p.kill()
				return Result{}, runtimeErr(ClassProtocol, "delivery %s: %w", id, err)
			}
			if !done {
				continue
			}
			p.prev = id
			stop() // the action has answered: moving its files is no longer its time to spend
			if err := p.upload(ctx, d, &res, emitted); err != nil {
				return Result{}, err
			}
			return res, nil
		}
	}
}

// deliveryErr explains a delivery whose context ended; the process is killed.
func (p *Process) deliveryErr(dctx context.Context, d Delivery) error {
	p.kill()
	if errors.Is(context.Cause(dctx), errDeliveryTimeout) {
		return runtimeErr(ClassTimeout, "delivery %s: no answer within %s", d.Item.Delivery, d.Timeout)
	}
	return fmt.Errorf("delivery %s: %w", d.Item.Delivery, dctx.Err())
}

// drainIdle reads what the process wrote since its last delivery. A late metric of that delivery is ignored; any
// other message between deliveries is a protocol violation, and the process is killed.
func (p *Process) drainIdle(ctx context.Context) error {
	for {
		select {
		case m, ok := <-p.msgs:
			if !ok {
				return p.exitErr(ctx, "exited while idle")
			}
			if p.lateMetric(m) {
				continue
			}
			p.kill()
			return runtimeErr(ClassProtocol, "%w: %s while idle", protocol.ErrProtocol, m.Type())
		default:
			return nil
		}
	}
}

// lateMetric reports whether m is a metric for the delivery answered last, sent after its done or error. Metrics
// are informative and an SDK may flush one late, so it is logged at debug level and dropped rather than killing a
// healthy process.
func (p *Process) lateMetric(m protocol.Message) bool {
	mt, ok := m.(protocol.Metric)
	if !ok || p.prev == "" || mt.Delivery != p.prev {
		return false
	}
	p.logger().Debug("late metric ignored", "metric", mt.Name, "deliveryId", mt.Delivery)
	return true
}

// collect adds m to res, and an emit's artifacts to emitted, and reports whether m ended the delivery.
func collect(res *Result, emitted *[][]protocol.Artifact, id string, m protocol.Message) (bool, error) {
	var got string
	switch m := m.(type) {
	case protocol.Emit:
		got = m.Delivery
		res.Outputs = append(res.Outputs, store.Output{Key: m.Key, Payload: m.Payload})
		*emitted = append(*emitted, m.Artifacts)
	case protocol.Metric:
		got = m.Delivery
		res.Metrics = append(res.Metrics, m)
	case protocol.Done:
		got = m.Delivery
	case protocol.Error:
		got = m.Delivery
		res.Outputs, *emitted = nil, nil
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

// shutdown asks the process to exit and reports whether it did within the shutdown timeout. The timeout bounds the
// write too, a process that stopped reading stdin would block it on a full pipe: at the timeout the process is
// killed, which closes stdin.
func (p *Process) shutdown() bool {
	killer := time.AfterFunc(p.opts.ShutdownTimeout, p.kill)
	_ = p.w.Write(protocol.Shutdown{})
	_ = p.stdin.Close()
	msgs := p.msgs
	for {
		select {
		case _, ok := <-msgs: // dropped: nothing may follow shutdown, and the process is going anyway
			if !ok {
				msgs = nil
			}
		case <-p.exited:
			return killer.Stop() // false when the timeout killed it
		}
	}
}

// kill ends the process and every process of its group with SIGKILL, has the launcher release what outlives them
// unless the process had exited cleanly, and closes its pipes, so readers return even if a grandchild that escaped
// the group holds them open.
func (p *Process) kill() {
	p.killOnce.Do(func() {
		close(p.dead)
		killProcessGroup(p.cmd.Process)
		if !p.exitedCleanly() {
			p.launcher.Kill(p.spec, p.workdir)
		}
		_ = p.stdin.Close()
		_ = p.stdout.Close()
		_ = p.stderr.Close()
	})
}

// exitedCleanly reports whether the process has exited with status 0.
func (p *Process) exitedCleanly() bool {
	select {
	case <-p.exited:
		return p.waitErr == nil
	default:
		return false
	}
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
