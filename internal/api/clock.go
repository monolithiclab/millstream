package api

import "time"

// Clock is the live streams' clock: heartbeats, the coalescing of pipeline snapshots and the broker's poll. Tests
// inject a fake one and drive time explicitly.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer is a one-shot timer of a Clock.
type Timer interface {
	C() <-chan time.Time
	// Stop stops the timer; it reports false when it already fired or was stopped.
	Stop() bool
}

// SystemClock is the real clock.
type SystemClock struct{}

// Now returns the current time.
func (SystemClock) Now() time.Time { return time.Now() }

// NewTimer returns a timer firing once after d.
func (SystemClock) NewTimer(d time.Duration) Timer { return systemTimer{time.NewTimer(d)} }

type systemTimer struct{ t *time.Timer }

func (t systemTimer) C() <-chan time.Time { return t.t.C }
func (t systemTimer) Stop() bool          { return t.t.Stop() }
