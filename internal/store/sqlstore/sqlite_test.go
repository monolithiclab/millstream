package sqlstore_test

import (
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/store/sqlstore"
	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// The conformance suite (store/storetest) runs here against SQLite, the default build's store: in this package,
// whose code it covers, so that coverage counts it. The Postgres store runs it under the e2e tag.

// TestMain raises the collector's target: the transpiled SQLite allocates heavily, and the tests trade memory for time.
func TestMain(m *testing.M) {
	debug.SetGCPercent(400)
	// SQLite's initialisation and the template's load are the process's, not its first test's: a Memory store
	// opened and closed here leaves the template loaded and its connection in the pool.
	if s, err := sqlite.Open(sqlite.Memory, sqlite.Options{}); err == nil {
		_ = s.Close()
	}
	os.Exit(m.Run())
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

// memoryStore is the conformance suite's opener: a Memory store, which reuses a closed one's connection and its prepared
// statements, where a clone would prepare them all again.
func memoryStore(t *testing.T, opts storetest.Options) storetest.Store {
	t.Helper()
	s, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: opts.Now, Rand: opts.Rand})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

// TestConformanceListed checks that conformance_test.go and conformance_e2e_test.go run each test of
// storetest.Tests, the e2e file's under the e2e tag only.
func TestConformanceListed(t *testing.T) {
	var got []string
	for _, f := range conformanceFiles {
		src, err := os.ReadFile(f) // #nosec G304 -- the package's own source
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

// conformanceFiles are the files holding the build's wrappers; conformance_e2e_test.go adds itself.
var conformanceFiles = []string{"conformance_test.go"}
