package cli

import (
	"path/filepath"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/stores"
)

// artifactStore is the local artifact store next to the state database: <directory of the database>/artifacts, the
// store key's SQLite path or --db (with Postgres, --db's directory, until the artifacts key arrives).
func artifactStore(rt *Runtime, env *pipeline.Environment) *artifacts.Store {
	return artifacts.New(filepath.Join(filepath.Dir(env.Store.Path(rt.DB)), "artifacts"))
}

// openStore opens the log store the environment file's store key names, else the SQLite database --db names,
// creating its directory. --db given with a store key is a usage error: two places would name the store.
func openStore(rt *Runtime) (store.Store, error) {
	env, err := environment(rt)
	if err != nil {
		return nil, err
	}
	if rt.DBSet && env.Store.Set() {
		return nil, usageError("--db: the environment file's store key names the store already")
	}
	return stores.Open(rt.Ctx, env.Store, rt.DB, rt.Env)
}

// withStore opens the log store, runs fn and closes it, reporting the first error; with Runtime.Store set, it runs
// fn on that store and leaves it open.
func withStore(rt *Runtime, fn func(st store.Store) error) (err error) {
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
