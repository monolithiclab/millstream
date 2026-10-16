package pipeline

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var update = flag.Bool("update", false, "rewrite golden files")

func TestGraphGolden(t *testing.T) {
	t.Parallel()
	pkg, err := Load(filepath.Join("testdata", "graph", "pipeline.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, format := range []string{FormatMermaid, FormatDOT} {
		t.Run(format, func(t *testing.T) {
			got, err := Graph(pkg.Pipeline, format)
			if err != nil {
				t.Fatalf("Graph: %v", err)
			}
			for range 5 { // map iteration order must not leak into the output
				again, _ := Graph(pkg.Pipeline, format)
				if again != got {
					t.Fatalf("Graph output is not deterministic")
				}
			}
			golden := filepath.Join("testdata", "graph", format+".golden")
			if *update {
				if err := os.WriteFile(golden, []byte(got), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden) // #nosec G304 -- test fixture
			if err != nil {
				t.Fatal(err)
			}
			if got != string(want) {
				t.Errorf("Graph(%s) differs from %s; run go test -update to accept\ngot:\n%s", format, golden, got)
			}
		})
	}
}

func TestGraphUnknownFormat(t *testing.T) {
	t.Parallel()
	_, err := Graph(&Pipeline{}, "svg")
	assertErr(t, err, `unknown graph format "svg"`)
}
