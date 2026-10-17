// Package conformance is the action protocol conformance suite (docs/protocol.md, Conformance): generic checks any
// action must pass, whatever it computes. It drives the action's process itself, over os/exec (or an exec.Starter's
// pipes, for in-process test actions) and the raw lines of its stdout, rather than through the exec runtime, which
// hides what it sees as violations behind a retry: the suite reports each one. Every check is answered pass, fail or
// skip (when a check it needs failed), with a reason.
//
// The suite starts four processes: one for the handshake, an item, the silence between deliveries and shutdown; one
// for hello and item messages carrying keys the action cannot know; and two for a malformed config, null and with
// every value of an unexpected type.
package conformance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// Check names, in the order the suite reports them.
const (
	CheckReady           = "ready"
	CheckUnknownKeys     = "unknown-keys"
	CheckAuditItem       = "audit-item"
	CheckFraming         = "framing"
	CheckItemAnswer      = "item-answer"
	CheckKeyedOutputs    = "keyed-outputs"
	CheckIdleSilence     = "idle-silence"
	CheckMalformedConfig = "malformed-config"
	CheckShutdown        = "shutdown"
)

// Statuses of a check.
const (
	Pass = "PASS"
	Fail = "FAIL"
	Skip = "SKIP"
)

// DefaultTimeout bounds each wait of the suite: a ready, an item's answer, an exit.
const DefaultTimeout = 5 * time.Second

// maxIdleWindow is the longest the suite watches a process stay silent between deliveries.
const maxIdleWindow = 500 * time.Millisecond

// Target is the action under test.
type Target struct {
	// Spec is its process, with the config and vars of a valid hello, and Launcher what starts it.
	Spec     exec.Spec
	Launcher exec.Launcher
	// Input is the payload of the items the suite sends.
	Input json.RawMessage
	// ConfigSchema is the action's config schema, nil without one: its properties are among the keys the
	// malformed config sets to values of an unexpected type.
	ConfigSchema json.RawMessage
	// Timeout bounds each wait; zero is DefaultTimeout.
	Timeout time.Duration
	// IdleWindow is how long the suite watches the process stay silent between deliveries; zero is the shorter of
	// Timeout and 500 ms.
	IdleWindow time.Duration
}

// Result is one check's outcome.
type Result struct {
	Name, Status, Reason string
}

// Run runs the suite against t and returns every check's result, in a fixed order.
func Run(ctx context.Context, t Target) []Result {
	if t.Timeout <= 0 {
		t.Timeout = DefaultTimeout
	}
	if t.IdleWindow <= 0 {
		t.IdleWindow = min(maxIdleWindow, t.Timeout)
	}
	s := &suite{t: t, results: map[string]Result{}}
	s.main(ctx)
	s.unknownKeys(ctx)
	s.malformedConfig(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.violations) == 0 {
		s.set(CheckFraming, Pass, "every line is a known message, one JSON object under the line limit")
	} else {
		s.set(CheckFraming, Fail, "%s", strings.Join(s.violations, "; "))
	}
	out := make([]Result, 0, len(s.results))
	for _, name := range []string{CheckReady, CheckUnknownKeys, CheckAuditItem, CheckFraming, CheckItemAnswer,
		CheckKeyedOutputs,
		CheckIdleSilence, CheckMalformedConfig, CheckShutdown} {
		out = append(out, s.results[name])
	}
	return out
}

// Failed reports whether any check failed.
func Failed(results []Result) bool {
	return slices.ContainsFunc(results, func(r Result) bool { return r.Status == Fail })
}

type suite struct {
	t       Target
	results map[string]Result

	mu         sync.Mutex
	violations []string // framing violations, by session
}

func (s *suite) set(name, status, format string, args ...any) {
	s.results[name] = Result{Name: name, Status: status, Reason: fmt.Sprintf(format, args...)}
}

func (s *suite) hello(config map[string]any) protocol.Hello {
	return protocol.Hello{Protocol: protocol.Version, Action: s.t.Spec.Action, Version: s.t.Spec.Version,
		Config: config, Vars: s.t.Spec.Vars}
}

func (s *suite) item(delivery string) protocol.Item {
	return protocol.Item{
		Delivery: delivery, ID: "it_" + delivery, Key: delivery, Payload: s.t.Input, Artifacts: []protocol.Artifact{},
		Run: protocol.Run{ID: "run_conformance", Trigger: "conformance", StartedAt: time.Now().UTC(),
			Revision: "conformance"},
		Attempt: 1, IdempotencyKey: "idem_" + delivery,
	}
}

// main runs the handshake, one item, the silence after it and shutdown on one process.
func (s *suite) main(ctx context.Context) {
	needs := func(check string, names ...string) {
		for _, n := range names {
			s.set(n, Skip, "needs %s to pass", check)
		}
	}
	p, err := s.start(ctx, "main")
	if err != nil {
		s.set(CheckReady, Fail, "%v", err)
		needs(CheckReady, CheckItemAnswer, CheckKeyedOutputs, CheckIdleSilence, CheckShutdown)
		return
	}
	defer p.kill()
	took, err := p.handshake(s.hello(s.t.Spec.Config))
	if err != nil {
		s.set(CheckReady, Fail, "%v", err)
		needs(CheckReady, CheckItemAnswer, CheckKeyedOutputs, CheckIdleSilence, CheckShutdown)
		return
	}
	s.set(CheckReady, Pass, "ready in %s", took.Round(time.Millisecond))

	ans, err := p.deliver(s.item("dl_conformance_1"))
	if err != nil {
		s.set(CheckItemAnswer, Fail, "%v", err)
		needs(CheckItemAnswer, CheckKeyedOutputs, CheckIdleSilence)
	} else {
		s.set(CheckItemAnswer, Pass, "%s", ans)
		s.keyed(ans.emits)
		s.idle(p, "dl_conformance_1")
	}

	if err := p.send(protocol.Shutdown{}); err != nil {
		s.set(CheckShutdown, Fail, "%v", err)
		return
	}
	if err := p.waitExit(s.t.Timeout); err != nil {
		s.set(CheckShutdown, Fail, "%v", err)
		return
	}
	s.set(CheckShutdown, Pass, "exited with status 0")
}

// keyed checks the key rule over one delivery's outputs.
func (s *suite) keyed(emits []protocol.Emit) {
	if len(emits) < 2 {
		s.set(CheckKeyedOutputs, Skip, "%d output for the item: the rule applies to several", len(emits))
		return
	}
	seen := map[string]bool{}
	for i, e := range emits {
		if e.Key == "" {
			s.set(CheckKeyedOutputs, Fail, "output %d of %d has no key", i, len(emits))
			return
		}
		if seen[e.Key] {
			s.set(CheckKeyedOutputs, Fail, "two outputs share the key %q", e.Key)
			return
		}
		seen[e.Key] = true
	}
	s.set(CheckKeyedOutputs, Pass, "%d outputs with distinct keys", len(emits))
}

// idle watches the process between deliveries: nothing but a late metric of the delivery just answered.
func (s *suite) idle(p *process, answered string) {
	window := s.t.IdleWindow
	deadline := time.After(window)
	for {
		select {
		case <-deadline:
			s.set(CheckIdleSilence, Pass, "silent for %s after the answer", window)
			return
		case l, ok := <-p.lines:
			if !ok {
				s.set(CheckIdleSilence, Fail, "exited between deliveries%s", p.stderrTail())
				return
			}
			m, err := p.parse(l)
			if err != nil {
				s.set(CheckIdleSilence, Fail, "wrote between deliveries: %v", err)
				return
			}
			if metric, ok := m.(protocol.Metric); ok && metric.Delivery == answered {
				continue // a late metric is ignored by the engine, not a violation
			}
			s.set(CheckIdleSilence, Fail, "wrote %s between deliveries", m.Type())
			return
		}
	}
}

// unknownKeys sends a hello and an item carrying keys the action cannot know: it must answer both.
func (s *suite) unknownKeys(ctx context.Context) {
	p, err := s.start(ctx, "unknown-keys")
	if err != nil {
		s.set(CheckUnknownKeys, Fail, "%v", err)
		return
	}
	defer p.kill()
	extra := map[string]any{"conformanceUnknownKey": map[string]any{"nested": []any{1, "two", nil}}}
	if _, err := p.handshake(withKeys(s.hello(s.t.Spec.Config), extra)); err != nil {
		s.set(CheckUnknownKeys, Fail, "hello with an unknown key: %v", err)
		return
	}
	if _, err := p.deliver(withKeys(s.item("dl_conformance_2"), extra)); err != nil {
		s.set(CheckUnknownKeys, Fail, "item with an unknown key: %v", err)
		s.set(CheckAuditItem, Skip, "needs %s to pass", CheckUnknownKeys)
		return
	}
	s.set(CheckUnknownKeys, Pass, "hello and item with an unknown key answered")
	audit := s.item("dl_conformance_audit")
	audit.Audit = true
	ans, err := p.deliver(audit)
	if err != nil {
		s.set(CheckAuditItem, Fail, "item marked audit: %v", err)
		return
	}
	s.set(CheckAuditItem, Pass, "item marked audit answered: %s", ans)
}

// malformedConfig sends a null config, then one whose every value has an unexpected type: the process must take
// the hello and answer the item, with done or error.
func (s *suite) malformedConfig(ctx context.Context) {
	unexpected := []any{map[string]any{"unexpected": []any{nil}}}
	wrong := map[string]any{"conformanceUnexpected": unexpected}
	for k := range s.t.Spec.Config {
		wrong[k] = unexpected
	}
	var cs struct {
		Properties map[string]any `json:"properties"`
	}
	_ = json.Unmarshal(s.t.ConfigSchema, &cs) // a schema that is not an object has no properties to spoil
	for k := range cs.Properties {
		wrong[k] = unexpected
	}
	var outcomes []string
	for _, tc := range []struct {
		name   string
		config map[string]any
	}{{"null config", nil}, {"config of unexpected types", wrong}} {
		p, err := s.start(ctx, tc.name)
		if err == nil {
			_, err = p.handshake(s.hello(tc.config))
		}
		var ans answer
		if err == nil {
			ans, err = p.deliver(s.item("dl_conformance_3"))
		}
		if p != nil {
			p.kill()
		}
		if err != nil {
			s.set(CheckMalformedConfig, Fail, "%s: %v", tc.name, err)
			return
		}
		outcomes = append(outcomes, tc.name+": "+ans.String())
	}
	s.set(CheckMalformedConfig, Pass, "%s", strings.Join(outcomes, "; "))
}

// withKeys returns m as a JSON object with extra keys added.
func withKeys(m protocol.Message, extra map[string]any) rawMessage {
	body, _ := json.Marshal(m) // protocol messages always encode
	var obj map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber() // payload numbers go through unchanged
	_ = dec.Decode(&obj)
	maps.Copy(obj, extra)
	obj["type"] = m.Type()
	return rawMessage{typ: m.Type(), obj: obj}
}

// rawMessage is a message with keys the protocol package does not know, written as is.
type rawMessage struct {
	typ string
	obj map[string]any
}

func (r rawMessage) Type() string { return r.typ }

// process is a running action under test.
type process struct {
	s         *suite
	session   string
	stdin     io.WriteCloser
	terminate func() // ends the action and what its launcher started
	workdir   string
	lines     chan []byte // stdout lines; closed at the end of stdout
	exited    chan struct{}
	waitErr   error
	stderr    tailBuffer
	killOnce  sync.Once
}

// start spawns the action with exec.Launch (in a process group of its own, or in-process for a Starter), with a
// temporary MILLSTREAM_WORKDIR.
func (s *suite) start(ctx context.Context, session string) (*process, error) {
	workdir, err := os.MkdirTemp("", "millstream-conformance-")
	if err != nil {
		return nil, fmt.Errorf("create workdir: %w", err)
	}
	proc, err := exec.Launch(s.t.Launcher, s.t.Spec, workdir)
	if err != nil {
		_ = os.RemoveAll(workdir)
		return nil, fmt.Errorf("start the action: %w", err)
	}
	p := &process{s: s, session: session, stdin: proc.Stdin, workdir: workdir, lines: make(chan []byte, 64),
		exited: make(chan struct{}), terminate: func() {
			proc.Kill()
			s.t.Launcher.Kill(s.t.Spec, workdir)
		}}
	go func() {
		var readers sync.WaitGroup
		readers.Go(func() { p.read(proc.Stdout) })
		readers.Go(func() { _, _ = io.Copy(&p.stderr, proc.Stderr) })
		readers.Wait() // Wait only once both pipes are drained, so no line is lost
		p.waitErr = proc.Wait()
		close(p.exited)
	}()
	go func() {
		select {
		case <-ctx.Done():
			p.kill()
		case <-p.exited:
		}
	}()
	return p, nil
}

// read splits stdout into lines, recording a line over the limit as a violation.
func (p *process) read(stdout io.Reader) {
	defer close(p.lines)
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64<<10), protocol.MaxLine+1)
	for sc.Scan() {
		p.lines <- bytes.Clone(sc.Bytes())
	}
	if errors.Is(sc.Err(), bufio.ErrTooLong) {
		p.violation("a line longer than %d bytes", protocol.MaxLine)
	}
	_, _ = io.Copy(io.Discard, stdout) // let the process finish writing
}

// violation records a framing violation of this session.
func (p *process) violation(format string, args ...any) {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	p.s.violations = append(p.s.violations, p.session+": "+fmt.Sprintf(format, args...))
}

// actionTypes are the messages an action may write.
var actionTypes = []string{protocol.TypeReady, protocol.TypeEmit, protocol.TypeMetric, protocol.TypeDone,
	protocol.TypeError, protocol.TypeAbstain}

// parse decodes one line as a message an action may write; a line that is not one is a violation.
func (p *process) parse(line []byte) (protocol.Message, error) {
	m, err := protocol.NewReader(bytes.NewReader(append(line, '\n'))).Read()
	if err == nil && !slices.Contains(actionTypes, m.Type()) {
		err = fmt.Errorf("%w: %s is a message of the engine", protocol.ErrProtocol, m.Type())
	}
	if err != nil {
		err = fmt.Errorf("%w (line %q)", err, excerpt(line))
		p.violation("%v", err)
		return nil, err
	}
	return m, nil
}

func excerpt(line []byte) string {
	const n = 80
	if len(line) > n {
		return string(line[:n]) + "…"
	}
	return string(line)
}

// next returns the next message, by the deadline; what names the message awaited, for the errors.
func (p *process) next(deadline time.Time, what string) (protocol.Message, error) {
	select {
	case l, ok := <-p.lines:
		if !ok {
			return nil, p.endedErr("before " + what)
		}
		return p.parse(l)
	case <-time.After(time.Until(deadline)):
		return nil, fmt.Errorf("no %s within %s", what, p.s.t.Timeout)
	}
}

// send writes one message; a process that does not read its stdin is killed after the timeout.
func (p *process) send(m protocol.Message) error {
	var buf bytes.Buffer
	if r, ok := m.(rawMessage); ok {
		if err := json.NewEncoder(&buf).Encode(r.obj); err != nil { // one line, ending in a newline
			return err
		}
	} else if err := protocol.NewWriter(&buf).Write(m); err != nil {
		return err
	}
	line := buf.Bytes()
	done := make(chan error, 1)
	go func() {
		_, err := p.stdin.Write(line)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("write %s: %w%s", m.Type(), err, p.stderrTail())
		}
		return nil
	case <-time.After(p.s.t.Timeout):
		p.kill()
		return fmt.Errorf("the action did not read %s within %s", m.Type(), p.s.t.Timeout)
	}
}

// handshake sends hello and waits for ready, which must be the first message.
func (p *process) handshake(hello protocol.Message) (time.Duration, error) {
	start := time.Now()
	if err := p.send(hello); err != nil {
		return 0, err
	}
	m, err := p.next(time.Now().Add(p.s.t.Timeout), "ready")
	if err != nil {
		return 0, err
	}
	if m.Type() != protocol.TypeReady {
		return 0, fmt.Errorf("answered hello with %s, want ready", m.Type())
	}
	return time.Since(start), nil
}

// answer is how an action answered an item.
type answer struct {
	emits   []protocol.Emit
	metrics int
	err     *protocol.Error
	abstain *protocol.Abstain
}

func (a answer) String() string {
	if a.err != nil {
		return fmt.Sprintf("error (retryable %t): %s", a.err.Retryable, a.err.Message)
	}
	if a.abstain != nil {
		return fmt.Sprintf("abstain: %s", a.abstain.Reason)
	}
	return fmt.Sprintf("done with %d output(s) and %d metric(s)", len(a.emits), a.metrics)
}

// deliver sends an item and collects its answer, up to done, error or abstain, all for that delivery.
func (p *process) deliver(item protocol.Message) (answer, error) {
	var delivery string
	switch it := item.(type) {
	case protocol.Item:
		delivery = it.Delivery
	case rawMessage:
		delivery, _ = it.obj["delivery"].(string)
	}
	if err := p.send(item); err != nil {
		return answer{}, err
	}
	var ans answer
	deadline := time.Now().Add(p.s.t.Timeout)
	for {
		m, err := p.next(deadline, "done, error or abstain for the item")
		if err != nil {
			return ans, err
		}
		var about string
		switch m := m.(type) {
		case protocol.Emit:
			about = m.Delivery
			ans.emits = append(ans.emits, m)
		case protocol.Metric:
			about = m.Delivery
			ans.metrics++
		case protocol.Done:
			about = m.Delivery
		case protocol.Error:
			about = m.Delivery
			ans.err = &m
		case protocol.Abstain:
			about = m.Delivery
			ans.abstain = &m
		default:
			return ans, fmt.Errorf("wrote %s during a delivery", m.Type())
		}
		if about != delivery {
			return ans, fmt.Errorf("wrote %s for delivery %q, want %q", m.Type(), about, delivery)
		}
		switch m.Type() {
		case protocol.TypeDone, protocol.TypeError, protocol.TypeAbstain:
			return ans, nil
		}
	}
}

// endedErr says how the process ended once its stdout closed.
func (p *process) endedErr(when string) error {
	select {
	case <-p.exited:
		return fmt.Errorf("exited (%v) %s%s", p.waitErr, when, p.stderrTail())
	case <-time.After(p.s.t.Timeout):
		return fmt.Errorf("closed stdout %s", when)
	}
}

// waitExit waits for the process to exit by itself, with status 0, and checks what it wrote meanwhile.
func (p *process) waitExit(timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		select {
		case l, ok := <-p.lines:
			if !ok {
				select {
				case <-p.exited:
					if p.waitErr != nil {
						return fmt.Errorf("exited with %v after shutdown%s", p.waitErr, p.stderrTail())
					}
					return nil
				case <-deadline:
					return fmt.Errorf("closed stdout but did not exit within %s of shutdown", timeout)
				}
			}
			if m, err := p.parse(l); err == nil {
				return fmt.Errorf("wrote %s after shutdown", m.Type())
			}
		case <-deadline:
			return fmt.Errorf("did not exit within %s of shutdown", timeout)
		}
	}
}

// kill ends the process group if it still runs, releases what the launcher started, and removes the workdir.
func (p *process) kill() {
	p.killOnce.Do(func() {
		_ = p.stdin.Close()
		go func() {
			for range p.lines { // unblock the reader: nothing reads the lines any more
			}
		}()
		select {
		case <-p.exited:
		default:
			p.terminate()
			<-p.exited
		}
		_ = os.RemoveAll(p.workdir)
	})
}

// stderrTail is the end of what the process wrote on stderr, as a suffix for an error message.
func (p *process) stderrTail() string {
	if t := strings.TrimSpace(p.stderr.String()); t != "" {
		return ": stderr: " + t
	}
	return ""
}

// tailBuffer keeps the last bytes written to it.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
}

const tailSize = 512

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > tailSize {
		b.buf = b.buf[len(b.buf)-tailSize:]
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
