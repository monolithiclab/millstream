package cli

import (
	"context"
	"testing"
)

// TestProposalTrailersWithoutGit: without git on PATH, a package carries no proposal.
func TestProposalTrailersWithoutGit(t *testing.T) {
	t.Parallel()
	rt := &Runtime{Ctx: context.Background(), Env: func(string) (string, bool) { return "", false }}
	if got := proposalTrailers(rt, t.TempDir()); got != nil {
		t.Fatalf("trailers = %q", got)
	}
}
