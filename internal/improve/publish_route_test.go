package improve

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
)

// TestRoutePublisher: every publish action's name reaches the engine's publish route with the token and the
// validated hash; a patch that does not hash to it is refused before any request.
func TestRoutePublisher(t *testing.T) {
	t.Parallel()
	for _, name := range pipeline.PublishActions {
		if _, ok := publishers[name].(routePublisher); !ok {
			t.Fatalf("%s is not registered", name)
		}
	}
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var req api.PublishRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if r.URL.Path != "/api/v1/proposals/pr_1/publish" || r.Header.Get("Authorization") != "Bearer mspr_t" ||
			req.PatchSHA256 != patch.Hash([]byte("p")) {
			http.Error(w, `{"error":{"code":"invalid","message":"bad"}}`, http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(api.PublishedProposal{Proposal: "pr_1", Publisher: "patch-file",
			Location: "/x/pr_1.patch", Again: calls > 1})
	}))
	t.Cleanup(srv.Close)
	r := routePublisher{http: srv.Client()}
	req := PublishRequest{Proposal: api.Proposal{ID: "pr_1", Patch: "p", PatchSHA256: patch.Hash([]byte("p"))},
		Publisher: "patch-file", API: srv.URL, Token: "mspr_t"}
	for _, want := range []string{"published by patch-file", "published already by patch-file"} {
		res, err := r.Publish(t.Context(), req)
		if err != nil || !res.Published || res.Location != "/x/pr_1.patch" || res.Message != want {
			t.Fatalf("Publish = %+v, %v", res, err)
		}
	}
	req.Proposal.Patch = "tampered"
	if _, err := r.Publish(t.Context(), req); err == nil || !strings.Contains(err.Error(), "validated") || calls != 2 {
		t.Fatalf("a tampered patch: %v (calls %d)", err, calls)
	}
}
