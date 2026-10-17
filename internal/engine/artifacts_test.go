package engine

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/store"

	"github.com/monolithiclab/millstream/internal/testdir"
)

func TestArtifactCalls(t *testing.T) {
	none := newEnv(t, Options{})
	if _, err := none.e.PutArtifact(t.Context(), store.LeaseRef{Pipeline: "listings"}, "a.txt", strings.NewReader("x")); !errors.Is(err, errNoArtifactStore) {
		t.Fatalf("PutArtifact without a store: %v", err)
	}
	v := newEnv(t, Options{Artifacts: artifacts.New(testdir.New(t))})
	ref, err := v.e.PutArtifact(t.Context(), store.LeaseRef{Pipeline: "listings"}, "a.txt", strings.NewReader("report"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := none.e.OpenArtifact(t.Context(), store.LeaseRef{Pipeline: "listings"}, ref); !errors.Is(err, errNoArtifactStore) {
		t.Fatalf("OpenArtifact without a store: %v", err)
	}
	r, err := v.e.OpenArtifact(t.Context(), store.LeaseRef{Pipeline: "listings"}, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if b, _ := io.ReadAll(r); string(b) != "report" {
		t.Fatalf("content %q", b)
	}
}
