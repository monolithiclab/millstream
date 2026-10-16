package api

import (
	_ "embed"
	"fmt"
	"net"
	"net/http"
)

// OpenAPI is the API's OpenAPI 3.1 document: the contract the handlers are tested against and the UI's types are
// generated from (make ui-types).
//
//go:embed openapi.yaml
var OpenAPI []byte

func (s *Server) openAPI(w http.ResponseWriter, _ *http.Request) error {
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(OpenAPI)
	return nil
}

// CheckLoopback refuses a listen address that is not a loopback one: until token authentication (M4), the API
// changes state for whoever reaches it, so it is served to this machine only. A host name must be localhost.
func CheckLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("listen address %q: want host:port, such as 127.0.0.1:7070: %w", addr, err)
	}
	if host != "" && loopbackHost(host) {
		return nil
	}
	return fmt.Errorf("listen address %q is not a loopback address: the API has no authentication until token "+
		"auth arrives (M4), so it listens on 127.0.0.1, ::1 or localhost only", addr)
}
