package postgres

import (
	"syscall"
	"time"
)

// userTimeout returns the dialer's control function setting TCP_USER_TIMEOUT on a TCP socket: the kernel drops the
// connection once data it sent stays unacknowledged that long, rather than retransmitting for many minutes.
func userTimeout(d time.Duration) func(network, address string, c syscall.RawConn) error {
	ms := int(min(d.Milliseconds(), 1<<31-1))
	return func(network, _ string, c syscall.RawConn) error {
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil
		}
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_USER_TIMEOUT, ms) // #nosec G115 -- a socket descriptor
		}); err != nil {
			return err
		}
		return serr
	}
}
