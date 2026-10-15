// Package agenttest serves a fake model provider in process, for the tests of what calls the api harness.
package agenttest

import (
	"net/http"
	"net/http/httptest"
)

// URL is a base URL for a fake provider: Client's requests never leave the process, whatever their host.
const URL = "http://model.test"

// Client returns a client whose requests h serves, in process: no listener, no connection.
func Client(h http.Handler) *http.Client { return &http.Client{Transport: transport{h}} }

type transport struct{ h http.Handler }

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, req)
	resp := rec.Result()
	resp.Request = req
	return resp, nil
}
