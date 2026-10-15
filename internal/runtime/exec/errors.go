package exec

import (
	"errors"
	"fmt"
)

// Runtime error classes, stored on the delivery by the Fail unit (spec section 5.5). They are retryable unless the
// error is Permanent.
const (
	// ClassTimeout: no ready within the ready timeout, or no done or error within the delivery's timeout.
	ClassTimeout = "timeout"
	// ClassCrash: the process could not start, or exited before answering.
	ClassCrash = "crash"
	// ClassProtocol: a protocol violation (malformed line, unknown or out-of-order message, another delivery's id).
	ClassProtocol = "protocol"
)

// RuntimeError is a delivery that failed in the runtime rather than in the action. Usually the process is gone
// (killed or exited) and the delivery is retried. A Permanent one is deterministic, such as an item too large for a
// protocol line: the process is left healthy, and retrying would fail the same way.
type RuntimeError struct {
	Class     string
	Permanent bool
	Err       error
}

func (e *RuntimeError) Error() string { return fmt.Sprintf("%s: %v", e.Class, e.Err) }

func (e *RuntimeError) Unwrap() error { return e.Err }

// ClassOf returns the runtime class of err, ClassCrash when err is not a RuntimeError.
func ClassOf(err error) string {
	if re, ok := errors.AsType[*RuntimeError](err); ok {
		return re.Class
	}
	return ClassCrash
}

// Permanent reports whether err is a RuntimeError that retrying cannot fix.
func Permanent(err error) bool {
	re, ok := errors.AsType[*RuntimeError](err)
	return ok && re.Permanent
}

func runtimeErr(class, format string, args ...any) *RuntimeError {
	return &RuntimeError{Class: class, Err: fmt.Errorf(format, args...)}
}
