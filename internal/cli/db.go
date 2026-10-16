package cli

import (
	"path/filepath"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/stores"
)

// artifactsDir is the local artifact store's directory: artifacts.local.path, else artifacts beside the state
// database (the store key's SQLite path, else --db; with Postgres, --db's directory).
func artifactsDir(rt *Runtime, env *pipeline.Environment) string {
	if env.Artifacts.LocalPath != "" {
		return env.Artifacts.LocalPath
	}
	return filepath.Join(filepath.Dir(env.Store.Path(rt.DB)), "artifacts")
}

// artifactStore is the local artifact store.
func artifactStore(rt *Runtime, env *pipeline.Environment) *artifacts.Store {
	return artifacts.New(artifactsDir(rt, env))
}

// packageDir is where an engine writes the packages it loads from the store: packages, beside the local artifact
// store.
func packageDir(rt *Runtime, env *pipeline.Environment) string {
	return filepath.Join(filepath.Dir(artifactsDir(rt, env)), "packages")
}

// storeMode is how a command opens the store.
type storeMode int

const (
	// readStore only reads: on Postgres without the leadership lock, its units refused.
	readStore storeMode = iota
	// writeStore changes state from this process: on Postgres only with --direct, taking the lock.
	writeStore
	// engineStore runs an engine (run, serve): on Postgres it takes the lock, waiting while another engine has it.
	engineStore
)

// openStore opens the log store the environment file's store key names, else the SQLite database --db names,
// creating its directory, in the mode the command needs. --db given with a store key is a usage error: two places
// would name the store. waiting, when set, is called if an engine store finds Postgres's leadership lock held.
func openStore(rt *Runtime, env *pipeline.Environment, mode storeMode, waiting func()) (store.Store, error) {
	if rt.DBSet && env.Store.Set() {
		return nil, usageError("--db: the environment file's store key names the store already")
	}
	postgres := env.Store.PostgresDSN != nil
	switch {
	case mode == readStore:
		return stores.OpenReader(rt.Ctx, env.Store, rt.DB, rt.Env)
	case mode == writeStore && postgres && !rt.Direct:
		return nil, usageError("on Postgres the active engine is the only writer: pass --server URL (or set " +
			ServerEnv + ") to go through its API, or --direct to take the leadership lock while no engine runs")
	}
	return stores.OpenLeader(rt.Ctx, env.Store, rt.DB, rt.Env, waiting)
}

// withStore opens the log store for reading, runs fn and closes it, reporting the first error; with Runtime.Store
// set, it runs fn on that store and leaves it open.
func withStore(rt *Runtime, fn func(st store.Store) error) error {
	return withStoreMode(rt, readStore, nil, fn)
}

// withWriter is withStore for a command that changes state from its own process.
func withWriter(rt *Runtime, fn func(st store.Store) error) error {
	return withStoreMode(rt, writeStore, nil, fn)
}

// withStoreMode is withStore in the mode given; waiting is openStore's.
func withStoreMode(rt *Runtime, mode storeMode, waiting func(), fn func(st store.Store) error) (err error) {
	if rt.Store != nil {
		return fn(rt.Store)
	}
	env, err := environment(rt)
	if err != nil {
		return err
	}
	st, err := openStore(rt, env, mode, waiting)
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
