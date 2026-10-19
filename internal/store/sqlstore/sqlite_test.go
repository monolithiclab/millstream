package sqlstore_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"
	"github.com/monolithiclab/millstream/internal/store/sqlstore"
	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// The package's own tests open SQLite Memory stores; the conformance suite (store/storetest) runs against them from
// the conformance/... packages, and here only its e2e-tagged tests (conformance_e2e_test.go). The Postgres store runs
// it all under the e2e tag.

func TestMain(m *testing.M) {
	sqlitetest.Main(m)
}

func init() {
	sqlstore.OpenForTest = func(t *testing.T, opts sqlstore.Options) *sqlstore.Store {
		t.Helper()
		s, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: opts.Now, Rand: opts.Rand})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		})
		return s.Store
	}
}

// memoryStore is the e2e wrappers' opener.
var memoryStore = sqlitetest.Memory

// TestConformanceListed checks that the wrappers of the conformance/... packages and of conformance_e2e_test.go run
// each test of storetest.Tests once, the e2e file's under the e2e tag only.
func TestConformanceListed(t *testing.T) {
	files, err := filepath.Glob("conformance/*/conformance_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no conformance/*/conformance_test.go: %v", err)
	}
	var got []string
	for _, f := range append(files, conformanceFiles...) {
		src, err := os.ReadFile(f) // #nosec G304 -- the module's own source
		if err != nil {
			t.Fatal(err)
		}
		for rest := string(src); ; {
			i := strings.Index(rest, "\nfunc Test")
			if i < 0 {
				break
			}
			rest = rest[i+len("\nfunc "):]
			name, body, _ := strings.Cut(rest, "(t *testing.T) {\n\tstoretest.")
			if called, _, _ := strings.Cut(body, "(t, memoryStore)\n}"); called != name {
				t.Errorf("%s: %s does not run storetest.%s(t, memoryStore) alone", f, name, name)
			}
			got = append(got, name)
		}
	}
	var want []string
	for _, tc := range storetest.Tests {
		want = append(want, tc.Name)
	}
	slices.Sort(got)
	slices.Sort(want)
	for i := 1; i < len(got); i++ {
		if got[i] == got[i-1] {
			t.Errorf("two wrappers run storetest.%s", got[i])
		}
	}
	for _, name := range want {
		if !slices.Contains(got, name) {
			t.Errorf("no wrapper runs storetest.%s", name)
		}
	}
	for _, name := range got {
		if !slices.Contains(want, name) {
			t.Errorf("%s is not in storetest.Tests, or not in this build", name)
		}
	}
}

// conformanceFiles are this package's files holding wrappers: conformance_e2e_test.go adds itself.
var conformanceFiles []string
