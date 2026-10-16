package api

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// Token authentication (spec sections 12.1 and 15.2). With Options.Auth, every request under /api/v1 (and /metrics,
// unless it is public) carries the deployment's token: as a bearer token (the CLI, scripts, Prometheus), or as the
// session cookie the UI gets by posting the token to /api/v1/session. Sessions live in the server's memory: a
// restart, and so a token change, signs everyone out.
//
// Cross-site requests: the session cookie is SameSite=Strict, so a page elsewhere never sends it; every POST
// carries a JSON content type, which a page elsewhere cannot send without a CORS preflight the server never grants;
// and a state-changing request naming another origin is forbidden, as is one authenticated by the cookie that names
// none.

// SessionCookie names the UI's session cookie.
const SessionCookie = "millstream_session"

// DefaultSessionTTL is how long a session lasts without Auth.SessionTTL.
const DefaultSessionTTL = 12 * time.Hour

// maxSessions bounds the sessions held: the oldest is dropped beyond it.
const maxSessions = 1024

// Auth turns token authentication on.
type Auth struct {
	// Token is the deployment's token (auth.token); it must not be empty.
	Token string
	// User decides every review (auth.user): the open-source edition is single-user, and X-Millstream-User is
	// ignored.
	User string
	// Hosts are the host names the server answers besides loopback ones (ui.baseUrl's, listen's), with or without
	// a port: behind a proxy, the name the browser asked for.
	Hosts []string
	// Origins are the origins (scheme://host[:port]) a state-changing request may come from besides the server's
	// own as the request names it: ui.baseUrl's, when a proxy rewrites the Host header.
	Origins []string
	// SecureCookie marks the session cookie Secure: the UI is reached over HTTPS (tls, a non-loopback listener, an
	// https ui.baseUrl).
	SecureCookie bool
	// SessionTTL is how long a session lasts; zero means DefaultSessionTTL.
	SessionTTL time.Duration
	// MetricsPublic serves /metrics without the token (metrics.public).
	MetricsPublic bool
}

// credential is how a request authenticated.
type credential int

const (
	noCredential credential = iota
	bearerCredential
	cookieCredential
)

// sessions are the UI's sessions, by the SHA-256 of their id, with their expiry.
type sessions struct {
	mu   sync.Mutex
	byID map[[32]byte]time.Time
}

// create starts a session expiring at expires and returns its id: 32 random bytes, URL-safe base64.
func (ss *sessions) create(now, expires time.Time) string {
	b := make([]byte, 32)
	_, _ = rand.Read(b) // crypto/rand never fails on the platforms Go supports
	id := base64.RawURLEncoding.EncodeToString(b)
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.byID) >= maxSessions {
		maps.DeleteFunc(ss.byID, func(_ [32]byte, exp time.Time) bool { return !exp.After(now) })
	}
	if len(ss.byID) >= maxSessions {
		var oldest [32]byte
		var at time.Time
		for k, exp := range ss.byID {
			if at.IsZero() || exp.Before(at) {
				oldest, at = k, exp
			}
		}
		delete(ss.byID, oldest)
	}
	ss.byID[sha256.Sum256([]byte(id))] = expires
	return id
}

// valid reports whether id names a session that has not expired.
func (ss *sessions) valid(id string, now time.Time) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	exp, ok := ss.byID[sha256.Sum256([]byte(id))]
	return ok && exp.After(now)
}

func (ss *sessions) remove(id string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.byID, sha256.Sum256([]byte(id)))
}

// tokenMatches compares a presented token with the deployment's in constant time, whatever their lengths: their
// SHA-256 sums.
func (s *Server) tokenMatches(got string) bool {
	g := sha256.Sum256([]byte(got))
	return subtle.ConstantTimeCompare(g[:], s.tokenSum[:]) == 1
}

// credential returns how r authenticated: a bearer token, which must be the deployment's (a wrong one does not fall
// back to the cookie), else a live session's cookie.
func (s *Server) credential(r *http.Request) credential {
	if h := r.Header.Get("Authorization"); h != "" {
		scheme, token, _ := strings.Cut(h, " ")
		if strings.EqualFold(scheme, "Bearer") && s.tokenMatches(strings.TrimSpace(token)) {
			return bearerCredential
		}
		return noCredential
	}
	if c, err := r.Cookie(SessionCookie); err == nil && s.sessions.valid(c.Value, s.opts.Clock.Now()) {
		return cookieCredential
	}
	return noCredential
}

// public reports whether a request goes without the token: signing in or out, and /metrics when metrics.public.
func (s *Server) public(r *http.Request) bool {
	switch r.URL.Path {
	case Prefix + "/session":
		return r.Method == http.MethodPost || r.Method == http.MethodDelete
	case MetricsPath:
		return s.opts.Auth.MetricsPublic
	}
	return false
}

// sameOrigin reports whether a request may change state from where it comes from: a GET or HEAD always; another
// method when its Origin is the server's own (the scheme and host the request names) or one of Auth.Origins, or
// when it names none and does not rest on the session cookie (the CLI, scripts).
func (s *Server) sameOrigin(r *http.Request, cred credential) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return cred != cookieCredential
	}
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	if strings.EqualFold(u.Host, r.Host) {
		return true
	}
	return s.opts.Auth != nil && slices.Contains(s.opts.Auth.Origins, strings.ToLower(origin))
}

// hostAllowed reports whether the server answers a request naming host: a loopback one, and with Auth one of its
// Hosts.
func (s *Server) hostAllowed(host string) bool {
	if loopbackHost(host) {
		return true
	}
	if s.opts.Auth == nil {
		return false
	}
	return slices.Contains(s.opts.Auth.Hosts, strings.ToLower(hostName(host)))
}

// Session is the answer of the session operations: whether the server requires the token, and who decides.
type Session struct {
	Auth bool   `json:"auth"`
	User string `json:"user,omitempty"`
}

// sessionLogin is the body of POST /session.
type sessionLogin struct {
	Token string `json:"token"`
}

// getSession says whether the server requires the token, and who the request decides as.
func (s *Server) getSession(w http.ResponseWriter, r *http.Request) error {
	user, _ := s.user(r)
	writeJSON(w, http.StatusOK, Session{Auth: s.opts.Auth != nil, User: user})
	return nil
}

// createSession signs the UI in: the deployment's token for a session cookie.
func (s *Server) createSession(w http.ResponseWriter, r *http.Request) error {
	if s.opts.Auth == nil {
		return fmt.Errorf("session: this server has no token, it serves loopback clients without signing in: %w",
			errUnavailable)
	}
	var body sessionLogin
	if err := s.decodeBody(w, r, &body); err != nil {
		return err
	}
	if !s.tokenMatches(body.Token) {
		writeUnauthorized(w, "the token does not match this server's")
		return nil
	}
	now := s.opts.Clock.Now()
	ttl := s.opts.Auth.SessionTTL
	id := s.sessions.create(now, now.Add(ttl))
	http.SetCookie(w, sessionCookie(id, int(ttl/time.Second), s.opts.Auth.SecureCookie))
	writeJSON(w, http.StatusOK, Session{Auth: true, User: s.opts.Auth.User})
	return nil
}

// deleteSession signs the UI out: the session ends and the cookie is cleared, whether or not it named one.
func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) error {
	secure := false
	if s.opts.Auth != nil {
		if c, err := r.Cookie(SessionCookie); err == nil {
			s.sessions.remove(c.Value)
		}
		secure = s.opts.Auth.SecureCookie
	}
	http.SetCookie(w, sessionCookie("", -1, secure))
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// sessionCookie is the session cookie: HttpOnly, SameSite=Strict, Secure when the UI is reached over HTTPS. A
// loopback server over plain HTTP cannot set a Secure cookie, so Secure follows Auth.SecureCookie; a negative maxAge
// clears it.
func sessionCookie(value string, maxAge int, secure bool) *http.Cookie {
	return &http.Cookie{Name: SessionCookie, Value: value, Path: "/", MaxAge: maxAge, HttpOnly: true, // #nosec G124
		Secure: secure, SameSite: http.SameSiteStrictMode}
}

// writeUnauthorized answers a request without the token.
func writeUnauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="millstream"`)
	writeErrorBody(w, http.StatusUnauthorized, codeUnauthorized, msg)
}
