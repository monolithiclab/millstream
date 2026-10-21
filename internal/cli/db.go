package cli

import (
	"context"
	"path/filepath"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/artifacts/s3"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
	"github.com/monolithiclab/millstream/internal/store/stores"
)

// artifactsDir is the local artifact store's directory: artifacts.url's file:// directory, else artifacts beside
// the SQLite database (with Postgres, beside the default one, in .millstream/).
func artifactsDir(env *pipeline.Environment) string {
	if dir, ok := env.Artifacts.Local(); ok {
		return dir
	}
	db, ok := env.Store.SQLitePath()
	if !ok {
		db, _ = pipeline.Defaults().Store.SQLitePath()
	}
	return filepath.Join(filepath.Dir(db), "artifacts")
}

// localArtifacts reads the environment file and opens its artifact store: what a command writing from its own
// process needs beside the store.
func localArtifacts(rt *Runtime) (*pipeline.Environment, artifacts.Store, error) {
	env, err := environment(rt)
	if err != nil {
		return nil, nil, err
	}
	arts, err := artifactStore(rt, env)
	return env, arts, err
}

// artifactStore is the artifact store artifacts.url names (spec section 9.2): an S3-compatible bucket, configured by
// the AWS variables, or the local directory.
func artifactStore(rt *Runtime, env *pipeline.Environment) (artifacts.Store, error) {
	bucket, prefix, ok := env.Artifacts.S3()
	if !ok {
		return artifacts.New(artifactsDir(env)), nil
	}
	cfg := s3.FromEnv(bucket, prefix, rt.Env)
	cfg.HTTP = rt.HTTPClient
	return s3.New(cfg)
}

// packageDir is where an engine writes the packages it loads from the store: packages, beside the local artifact
// store.
func packageDir(env *pipeline.Environment) string {
	return filepath.Join(filepath.Dir(artifactsDir(env)), "packages")
}

// storeMode is how a command opens the store.
type storeMode int

const (
	// readStore only reads: on Postgres without the leadership lock, its units refused.
	readStore storeMode = iota
	// writeStore changes state from this process: on Postgres only with --direct, taking the lock if it is free.
	writeStore
	// engineStore runs an engine (run, serve): on Postgres it takes the lock, waiting while another engine has it.
	engineStore
)

// openStore opens, within ctx, the log store store.dsn names, creating a SQLite database's directory, in the mode
// the command needs. pg are the options a store taking Postgres's leadership lock opens with (an engine's id, what it
// calls while it waits), the environment file's engine key setting how its write connection is watched.
func openStore(ctx context.Context, rt *Runtime, env *pipeline.Environment, mode storeMode, pg postgres.Options) (
	store.Store, error,
) {
	_, onSQLite := env.Store.SQLitePath()
	switch {
	case mode == readStore:
		return stores.OpenReader(ctx, env.Store.DSN)
	case mode == writeStore && !onSQLite && !rt.Direct:
		return nil, usageError("on Postgres the active engine is the only writer: pass --server URL (or set " +
			ServerEnv + ") to go through its API, or --direct to take the leadership lock while no engine runs")
	}
	pg.CheckEvery, pg.Keepalive = time.Duration(env.Engine.LockCheck), time.Duration(env.Engine.Keepalive)
	pg.TryLock = mode == writeStore // --direct: fail naming the active engine rather than wait for it
	return stores.OpenLeader(ctx, env.Store.DSN, pg)
}

// withStore opens the log store for reading, runs fn and closes it, reporting the first error; with Runtime.Store
// set, it runs fn on that store and leaves it open.
func withStore(rt *Runtime, fn func(st store.Store) error) error {
	return withStoreMode(rt, readStore, postgres.Options{}, fn)
}

// withWriter is withStore for a command that changes state from its own process.
func withWriter(rt *Runtime, fn func(st store.Store) error) error {
	return withStoreMode(rt, writeStore, postgres.Options{}, fn)
}

// withStoreMode is withStore in the mode given; pg is openStore's.
func withStoreMode(rt *Runtime, mode storeMode, pg postgres.Options, fn func(st store.Store) error) (err error) {
	if rt.Store != nil {
		return fn(rt.Store)
	}
	env, err := environment(rt)
	if err != nil {
		return err
	}
	st, err := openStore(rt.Ctx, rt, env, mode, pg)
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
