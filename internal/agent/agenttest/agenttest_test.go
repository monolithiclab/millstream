package agenttest

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestClient(t *testing.T) {
	t.Parallel()
	c := Client(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, r.URL.Path)
	}))
	resp, err := c.Post(URL+"/v1/messages", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusTeapot || string(body) != "/v1/messages" || resp.Request == nil {
		t.Fatalf("response %d %q", resp.StatusCode, body)
	}
}
