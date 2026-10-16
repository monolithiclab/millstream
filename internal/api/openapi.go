package api

import (
	_ "embed"
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
