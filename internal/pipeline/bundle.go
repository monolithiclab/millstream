package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
)

// A bundle is how a package travels and is stored (spec section 4.9): its files as a JSON object of base64 contents
// by slash-separated path. The store keeps one per revision; an engine started from the store, and a remote node,
// write it out into a directory named after its content hash before running its actions.

// DecodeBundle decodes a package bundle. A path that is absolute, or climbs out of the package, is refused.
func DecodeBundle(bundle []byte) (map[string][]byte, error) {
	var files map[string][]byte
	if err := json.Unmarshal(bundle, &files); err != nil {
		return nil, fmt.Errorf("decode bundle: %w", err)
	}
	for p := range files {
		if !filepath.IsLocal(filepath.FromSlash(p)) || path.Clean(p) != p {
			return nil, fmt.Errorf("bundle path %q is not a clean path inside the package", p)
		}
	}
	return files, nil
}

// EncodeBundle is the bundle of a package's files, which DecodeBundle reads.
func EncodeBundle(files map[string][]byte) ([]byte, error) { return json.Marshal(files) }

// Materialize writes a package's files under parent/<hash>, read-only, unless they are there already, and returns
// the package of that directory: what an action process runs from.
func Materialize(parent string, files map[string][]byte) (*Package, error) {
	p, err := FromFiles(parent, files)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(parent, p.Hash)
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		if err := writePackage(parent, dir, files); err != nil {
			return nil, fmt.Errorf("write package %s: %w", p.Hash, err)
		}
	} else if err != nil {
		return nil, err
	}
	p.Dir = dir
	return p, nil
}

// writePackage writes files into a temporary directory under parent, then renames it to dir: a crash leaves no
// partial package. The files are read-only and executable, as a package's binaries must be.
func writePackage(parent, dir string, files map[string][]byte) error {
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, ".tmp-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	root, err := os.OpenRoot(tmp)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	for _, p := range slices.Sorted(maps.Keys(files)) {
		if d := path.Dir(p); d != "." {
			if err := root.MkdirAll(d, 0o750); err != nil {
				return err
			}
		}
		f, err := root.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o500) // #nosec G302 -- a package's binaries run
		if err != nil {
			return err
		}
		_, werr := f.Write(files[p])
		if err := errors.Join(werr, f.Close()); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, dir); err != nil && !errors.Is(err, os.ErrExist) {
		if _, serr := os.Stat(dir); serr == nil {
			return nil // another process of this host wrote it first
		}
		return err
	}
	return nil
}
