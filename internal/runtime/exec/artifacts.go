package exec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/store"
)

// InputDir is the directory of the working directory where a delivery's input artifacts are placed, each under its
// name.
const InputDir = "in"

// Artifacts moves a delivery's files between the process's working directory and the artifact store. The node binds
// it to the delivery's pipeline: the engine's local store for the embedded node, signed URLs for remote ones (M4).
type Artifacts interface {
	// Open opens an input artifact's content.
	Open(ctx context.Context, ref store.ArtifactRef) (io.ReadCloser, error)
	// Put stores an output artifact's content and returns its reference under name.
	Put(ctx context.Context, name string, r io.Reader) (store.ArtifactRef, error)
}

// errNoArtifacts is a delivery with artifacts to move and no Artifacts to move them with.
var errNoArtifacts = errors.New("this node has no artifact store")

// materialise places the delivery's input artifacts under in/ in the working directory and lists them in the item,
// by path relative to it. The item always carries a list, empty when there is nothing to place.
func (p *Process) materialise(ctx context.Context, d *Delivery) error {
	d.Item.Artifacts = make([]protocol.Artifact, 0, len(d.Inputs))
	if len(d.Inputs) == 0 {
		return nil
	}
	if d.Artifacts == nil {
		return runtimeErr(ClassArtifact, "delivery %s has input artifacts: %w", d.Item.Delivery, errNoArtifacts)
	}
	root, err := os.OpenRoot(p.workdir)
	if err != nil {
		return runtimeErr(ClassArtifact, "open workdir: %w", err)
	}
	defer func() { _ = root.Close() }()
	if err := root.Mkdir(InputDir, 0o750); err != nil {
		return runtimeErr(ClassArtifact, "create %s: %w", InputDir, err)
	}
	for _, ref := range d.Inputs {
		rel := path.Join(InputDir, ref.Name)
		if err := place(ctx, root, rel, ref, d.Artifacts); err != nil {
			return runtimeErr(ClassArtifact, "input artifact %s: %w", ref.Name, err)
		}
		d.Item.Artifacts = append(d.Item.Artifacts, protocol.Artifact{Name: ref.Name, Path: rel})
	}
	return nil
}

func place(ctx context.Context, root *os.Root, rel string, ref store.ArtifactRef, a Artifacts) error {
	if !store.ValidArtifactName(ref.Name) {
		return fmt.Errorf("%q is not a file name", ref.Name)
	}
	src, err := a.Open(ctx, ref)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	// O_EXCL: two inputs with one name must not silently overwrite each other.
	dst, err := root.OpenFile(filepath.FromSlash(rel), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, err = io.Copy(dst, src)
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	return err
}

// upload moves the artifacts each output named in its emit to the artifact store and attaches their references to
// the output. A path that is not a regular file inside the working directory, reached without a symbolic link, is
// refused as a permanent protocol error: retrying would refuse it again. A failure of the store is retryable. The
// process is left running either way: it answered the delivery properly.
func (p *Process) upload(ctx context.Context, d Delivery, res *Result, emitted [][]protocol.Artifact) error {
	named := false
	for i, list := range emitted {
		seen := map[string]bool{}
		for _, a := range list {
			if !store.ValidArtifactName(a.Name) || seen[a.Name] {
				return refused("output %d: artifact name %q: want distinct file names without a path separator", i,
					a.Name)
			}
			seen[a.Name] = true
			named = true
		}
	}
	if !named {
		return nil
	}
	if d.Artifacts == nil {
		return runtimeErr(ClassArtifact, "delivery %s emits artifacts: %w", d.Item.Delivery, errNoArtifacts)
	}
	root, err := os.OpenRoot(p.workdir)
	if err != nil {
		return runtimeErr(ClassArtifact, "open workdir: %w", err)
	}
	defer func() { _ = root.Close() }()
	for i, list := range emitted {
		for _, a := range list {
			ref, err := put(ctx, root, a, d.Artifacts)
			if err != nil {
				return err
			}
			res.Outputs[i].Artifacts = append(res.Outputs[i].Artifacts, ref)
		}
	}
	return nil
}

// put stores one emitted artifact.
func put(ctx context.Context, root *os.Root, a protocol.Artifact, as Artifacts) (store.ArtifactRef, error) {
	f, err := openInside(root, a.Path)
	if err != nil {
		return store.ArtifactRef{}, refused("artifact %s: %v", a.Name, err)
	}
	defer func() { _ = f.Close() }()
	ref, err := as.Put(ctx, a.Name, f)
	if err != nil {
		return ref, runtimeErr(ClassArtifact, "store artifact %s: %w", a.Name, err)
	}
	return ref, nil
}

// openInside opens rel, a slash-separated path relative to root, refusing a path that leaves root, passes through
// a symbolic link or does not end at a regular file. os.Root already keeps every access inside root; refusing
// links as well keeps an action from publishing a file it reaches only through one.
func openInside(root *os.Root, rel string) (*os.File, error) {
	p := filepath.FromSlash(rel)
	if !filepath.IsLocal(p) {
		return nil, fmt.Errorf("path %q is not a relative path inside the working directory", rel)
	}
	parts := strings.Split(filepath.Clean(p), string(filepath.Separator))
	for i := range parts {
		sub := filepath.Join(parts[:i+1]...)
		fi, err := root.Lstat(sub)
		if err != nil {
			return nil, fmt.Errorf("path %q: %w", rel, err)
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return nil, fmt.Errorf("path %q goes through the symbolic link %s", rel, filepath.ToSlash(sub))
		}
	}
	f, err := root.Open(p)
	if err != nil {
		return nil, fmt.Errorf("path %q: %w", rel, err)
	}
	if fi, err := f.Stat(); err != nil || !fi.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("path %q is not a regular file", rel)
	}
	return f, nil
}

// refused is a permanent protocol error: the action named an artifact the node will never accept.
func refused(format string, args ...any) error {
	return &RuntimeError{Class: ClassProtocol, Permanent: true,
		Err: fmt.Errorf("%w: %s", protocol.ErrProtocol, fmt.Sprintf(format, args...))}
}
