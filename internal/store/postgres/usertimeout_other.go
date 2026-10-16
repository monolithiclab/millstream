//go:build !linux

package postgres

import (
	"syscall"
	"time"
)

// userTimeout is nil where TCP_USER_TIMEOUT does not exist: the engine's side then relies on keepalives and on the
// writer's ping and batch timeouts (package comment).
func userTimeout(time.Duration) func(network, address string, c syscall.RawConn) error { return nil }
