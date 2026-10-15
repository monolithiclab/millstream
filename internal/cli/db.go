package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// artifactStore is the local artifact store next to the state database: <directory of --db>/artifacts.
func artifactStore(rt *Runtime) *artifacts.Store {
	return artifacts.New(filepath.Join(filepath.Dir(rt.DB), "artifacts"))
}

// openStore opens the state database named by --db, creating its directory.
func openStore(rt *Runtime) (*sqlite.Store, error) {
	if err := os.MkdirAll(filepath.Dir(rt.DB), 0o750); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	return sqlite.Open(rt.DB, sqlite.Options{})
}

// withStore opens the state database, runs fn and closes it, reporting the first error; with Runtime.Store set, it
// runs fn on that store and leaves it open.
func withStore(rt *Runtime, fn func(st *sqlite.Store) error) (err error) {
	if rt.Store != nil {
		return fn(rt.Store)
	}
	st, err := openStore(rt)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); err == nil {
			err = cerr
		}
	}()
	return fn(st)
}
