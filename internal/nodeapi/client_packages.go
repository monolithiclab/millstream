package nodeapi

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// packages is the node's package cache (spec section 5.10): each package fetched once by hash, written under
// <state>/packages/<hash> (pipeline.Materialize: complete or absent), and kept in memory with the actions resolved
// against it, an exec action's program for this node's platform.
type packages struct {
	dir string

	mu       sync.Mutex
	byHash   map[string]*pipeline.Package
	resolved map[[2]string]resolvedAction // by hash and reference
}

type resolvedAction struct {
	a   *pipeline.Action
	err error
}

func newPackages(stateDir string) *packages {
	return &packages{dir: filepath.Join(stateDir, "packages"), byHash: map[string]*pipeline.Package{},
		resolved: map[[2]string]resolvedAction{}}
}

// get returns the package under hash: from memory, from the cache directory, or fetched (a bundle) and written there.
// A package whose files do not hash to hash is refused.
func (c *packages) get(hash string, fetch func() ([]byte, error)) (*pipeline.Package, error) {
	if !store.ValidSHA256(hash) {
		return nil, fmt.Errorf("package hash %q is not a hex SHA-256", hash)
	}
	c.mu.Lock()
	p := c.byHash[hash]
	c.mu.Unlock()
	if p != nil {
		return p, nil
	}
	p, err := pipeline.Load(filepath.Join(c.dir, hash))
	if errors.Is(err, fs.ErrNotExist) {
		var bundle []byte
		if bundle, err = fetch(); err != nil {
			return nil, fmt.Errorf("fetch package %s: %w", hash, err)
		}
		var files map[string][]byte
		if files, err = pipeline.DecodeBundle(bundle); err != nil {
			return nil, fmt.Errorf("package %s: %w", hash, err)
		}
		if err := os.MkdirAll(c.dir, 0o700); err != nil {
			return nil, err
		}
		p, err = pipeline.Materialize(c.dir, files)
	}
	if err != nil {
		return nil, fmt.Errorf("package %s: %w", hash, err)
	}
	if p.Hash != hash {
		return nil, fmt.Errorf("package %s: its files hash to %s", hash, p.Hash)
	}
	c.mu.Lock()
	c.byHash[hash] = p
	c.mu.Unlock()
	return p, nil
}

// resolve resolves an action reference against a cached package, once per package and reference.
func (c *packages) resolve(p *pipeline.Package, ref string) (*pipeline.Action, string, error) {
	k := [2]string{p.Hash, ref}
	c.mu.Lock()
	r, ok := c.resolved[k]
	c.mu.Unlock()
	if !ok {
		r.a, r.err = pipeline.Resolver{}.Resolve(p, ref)
		c.mu.Lock()
		c.resolved[k] = r
		c.mu.Unlock()
	}
	return r.a, p.Dir, r.err
}

// packageQuery is the query of a package fetch: the node, the lease it holds and the package's hash.
func packageQuery(nodeID string, ref store.LeaseRef, hash string) url.Values {
	return url.Values{"node": {nodeID}, "pipeline": {ref.Pipeline}, "delivery": {ref.DeliveryID},
		"step": {ref.Step}, "token": {strconv.FormatInt(ref.FencingToken, 10)}, "hash": {hash}}
}
