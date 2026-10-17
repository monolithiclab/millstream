package api

import (
	"maps"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Failed attempts (spec section 12.1): failed sign-ins (POST /session) and failed webhook token checks are counted
// per remote address, an IPv6 one by its /64. Past maxFailures within failureWindow of the first, the address is
// answered tooManyRequests, whatever it sends, until the window ends. A success does not count. The table lives in
// the server's memory, bounded by maxFailureAddrs, and runs on the server's clock.
const (
	maxFailures     = 10
	failureWindow   = time.Minute
	maxFailureAddrs = 4096
)

// failures counts each address's failed attempts in its current window.
type failures struct {
	mu     sync.Mutex
	byAddr map[string]failureCount
}

// failureCount is an address's failures since start.
type failureCount struct {
	start time.Time
	n     int
}

func newFailures() *failures { return &failures{byAddr: map[string]failureCount{}} }

// wait returns how long addr is refused for, zero when it may try.
func (f *failures) wait(addr string, now time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.byAddr[addr]
	if !ok || c.n < maxFailures {
		return 0
	}
	return max(c.start.Add(failureWindow).Sub(now), 0)
}

// fail counts a failed attempt from addr. Beyond maxFailureAddrs, expired windows are dropped first, then the oldest.
func (f *failures) fail(addr string, now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.byAddr[addr]
	if !ok && len(f.byAddr) >= maxFailureAddrs {
		maps.DeleteFunc(f.byAddr, func(_ string, c failureCount) bool { return c.expired(now) })
		if len(f.byAddr) >= maxFailureAddrs {
			var oldest string
			var at time.Time
			for a, c := range f.byAddr {
				if at.IsZero() || c.start.Before(at) {
					oldest, at = a, c.start
				}
			}
			delete(f.byAddr, oldest)
		}
	}
	if !ok || c.expired(now) {
		c = failureCount{start: now}
	}
	c.n++
	f.byAddr[addr] = c
}

func (c failureCount) expired(now time.Time) bool { return !c.start.Add(failureWindow).After(now) }

// remoteAddr is the address a request's failures count under: its peer's IP, an IPv6 one by its /64, since a host
// commonly holds a whole /64. Behind a reverse proxy, that is the proxy's.
func remoteAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() != nil {
		return host
	}
	return ip.Mask(net.CIDRMask(64, 128)).String() + "/64"
}

// refuseFailing answers tooManyRequests, with Retry-After, when the request's address is past its failed attempts,
// and reports whether it did.
func (s *Server) refuseFailing(w http.ResponseWriter, r *http.Request) bool {
	wait := s.failures.wait(remoteAddr(r), s.opts.Clock.Now())
	if wait <= 0 {
		return false
	}
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
	writeErrorBody(w, http.StatusTooManyRequests, codeTooManyRequests,
		"too many failed attempts from this address: retry after the Retry-After seconds")
	return true
}

// failed counts a failed attempt from the request's address.
func (s *Server) failed(r *http.Request) { s.failures.fail(remoteAddr(r), s.opts.Clock.Now()) }
