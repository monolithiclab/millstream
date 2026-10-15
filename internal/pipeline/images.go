package pipeline

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// ManifestLabel is the image label holding a registry action's manifest, as JSON (spec section 6.2).
const ManifestLabel = "io.millstream.manifest"

// Images reads the local images of a container runtime (Docker or Podman). M2 resolves against local images only:
// listing a remote registry's tags arrives later, so a registry action's image must be pulled or built first.
type Images interface {
	// Inspect describes a local image by reference.
	Inspect(ref string) (Image, error)
	// Tags lists the tags of a repository's local images.
	Tags(repo string) ([]string, error)
}

// Image is a local image.
type Image struct {
	// ID is the image id, sha256:<hex>.
	ID string
	// RepoDigests are the registry digests the image is known by, repo@sha256:<hex>, when it was pulled or pushed.
	RepoDigests []string
	Labels      map[string]string
}

// pinFor returns how to run the image and the digest the lock file records: the registry digest of repo when the
// image has one (the reference a remote node can pull), else the image id.
func (img Image) pinFor(repo string) (pin, digest string) {
	for _, rd := range img.RepoDigests {
		if r, d, ok := strings.Cut(rd, "@"); ok && r == repo {
			return rd, d
		}
	}
	if len(img.RepoDigests) > 0 {
		_, d, _ := strings.Cut(img.RepoDigests[0], "@")
		return img.RepoDigests[0], d
	}
	return img.ID, img.ID
}

// repository returns an image reference without its tag or digest.
func repository(ref string) string {
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		ref = ref[:i]
	}
	return ref
}

// pin resolves a container action's image to its digest and checks it against the package's lock file: a lock entry
// whose digest differs is an error, a missing one leaves the action unpinned (check warns). IgnoreLock skips the
// comparison, for millstream lock itself.
func (r Resolver) pin(pkg *Package, a *Action, image string) error {
	img, err := r.Images.Inspect(image)
	if err != nil {
		return fmt.Errorf("image %s: %w", image, err)
	}
	a.Image = image
	a.Pin, a.ImageDigest = img.pinFor(repository(image))
	if r.IgnoreLock {
		return nil
	}
	l, err := pkg.lock()
	if err != nil {
		return err
	}
	e, ok := l.entry(a.Ref)
	switch {
	case !ok:
		a.Unpinned = true
	case e.Image != image || e.Digest != a.ImageDigest:
		return fmt.Errorf("image %s is %s but %s pins %s (%s): rebuild the image it pins, or run millstream lock",
			image, a.ImageDigest, LockFile, e.Image, e.Digest)
	}
	return nil
}

// entry returns the lock entry of ref; a nil lock has none.
func (l *Lock) entry(ref string) (LockEntry, bool) {
	if l == nil {
		return LockEntry{}, false
	}
	e, ok := l.Actions[ref]
	return e, ok
}

// Registry resolves registry references (spec section 6.2): the namespace maps to a repository prefix through the
// environment file, the range resolves to a version among the local image's tags (or the lock file's version), and
// the manifest is read from the image's io.millstream.manifest label. Registry actions are container actions.
type Registry struct {
	// Prefixes maps a namespace to its OCI repository prefix (the environment file's registries).
	Prefixes map[string]string
	// Resolver pins the image; its Images must be set.
	Resolver Resolver
}

// Resolve implements ActionResolver.
func (g Registry) Resolve(pkg *Package, ref string) (*Action, error) {
	if g.Resolver.Images == nil {
		return nil, fmt.Errorf("registry action %s: needs Docker or Podman: %w", ref, ErrUnavailable)
	}
	nsName, rng, _ := strings.Cut(ref, "@")
	ns, name, _ := strings.Cut(nsName, "/")
	prefix, ok := g.Prefixes[ns]
	if !ok {
		return nil, fmt.Errorf("registry action %s: namespace %s is not mapped: add registries.%s to the environment "+
			"file", ref, ns, ns)
	}
	want, err := parseRange(rng)
	if err != nil {
		return nil, fmt.Errorf("registry action %s: %w", ref, err)
	}
	repo := prefix + "/" + name
	version, err := g.version(pkg, ref, repo, want)
	if err != nil {
		return nil, fmt.Errorf("registry action %s: %w", ref, err)
	}
	image := repo + ":" + version
	img, err := g.Resolver.Images.Inspect(image)
	if err != nil {
		return nil, fmt.Errorf("registry action %s: image %s: %w", ref, image, err)
	}
	label, ok := img.Labels[ManifestLabel]
	if !ok {
		return nil, fmt.Errorf("registry action %s: image %s has no %s label", ref, image, ManifestLabel)
	}
	a, err := ParseImageManifest([]byte(label))
	if err != nil {
		return nil, fmt.Errorf("registry action %s: label %s of %s: %w", ref, ManifestLabel, image, err)
	}
	if a.Version != version {
		return nil, fmt.Errorf("registry action %s: image %s declares version %q in its manifest", ref, image, a.Version)
	}
	a.Ref = ref
	if err := g.Resolver.pin(pkg, a, image); err != nil {
		return nil, fmt.Errorf("registry action %s: %w", ref, err)
	}
	return a, nil
}

// version returns the version ref resolves to: the lock file's when it pins ref (it must satisfy the range), else
// the highest local tag in the range.
func (g Registry) version(pkg *Package, ref, repo string, want []int) (string, error) {
	if !g.Resolver.IgnoreLock {
		l, err := pkg.lock()
		if err != nil {
			return "", err
		}
		if e, ok := l.entry(ref); ok {
			v, err := parseVersion(e.Version)
			if err != nil || !inRange(v, want) {
				return "", fmt.Errorf("%s pins version %q, outside the range: run millstream lock", LockFile, e.Version)
			}
			return e.Version, nil
		}
	}
	tags, err := g.Resolver.Images.Tags(repo)
	if err != nil {
		return "", fmt.Errorf("list the tags of %s: %w", repo, err)
	}
	var best []int
	var bestTag string
	for _, tag := range tags {
		v, err := parseVersion(tag)
		if err != nil || !inRange(v, want) {
			continue
		}
		if best == nil || slices.Compare(v, best) > 0 {
			best, bestTag = v, tag
		}
	}
	if best == nil {
		return "", fmt.Errorf("no local image of %s has a tag in the range: pull or build one (remote tag listing "+
			"arrives later)", repo)
	}
	return bestTag, nil
}

// semver is an exact version without pre-release or build: MAJOR.MINOR.PATCH.
var semver = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func parseVersion(s string) ([]int, error) {
	if !semver.MatchString(s) {
		return nil, fmt.Errorf("%q is not a version MAJOR.MINOR.PATCH", s)
	}
	return parseInts(strings.Split(s, "."))
}

// parseRange parses the range of a registry reference: an exact version (1.4.2), or a prefix of one (1 or 1.4)
// matching every version that starts with it.
func parseRange(s string) ([]int, error) {
	parts := strings.Split(s, ".")
	if len(parts) > 3 {
		return nil, errBadRange(s)
	}
	v, err := parseInts(parts)
	if err != nil {
		return nil, errBadRange(s)
	}
	return v, nil
}

func errBadRange(s string) error {
	return fmt.Errorf("range %q: want an exact version (1.4.2) or a major or minor version (1, 1.4)", s)
}

func parseInts(parts []string) ([]int, error) {
	out := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || p != strconv.Itoa(n) {
			return nil, errors.New("not a number")
		}
		out[i] = n
	}
	return out, nil
}

// inRange reports whether version v starts with the components of the range.
func inRange(v, rng []int) bool { return len(rng) <= len(v) && slices.Equal(v[:len(rng)], rng) }
