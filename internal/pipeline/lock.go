package pipeline

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"go.yaml.in/yaml/v3"
)

// LockFile is the lock file at the root of a package (spec section 6.2): the exact version and image digest of
// each container and registry action, written by millstream lock. It is a file of the package, so it is part of
// the revision's hash.
const LockFile = "millstream.lock"

// Lock is a parsed lock file.
type Lock struct {
	// Actions are the pinned actions by reference, as steps write it (./actions/report, lab/score@2).
	Actions map[string]LockEntry
}

// LockEntry pins one action.
type LockEntry struct {
	// Version is the exact version a registry reference resolved to; empty for a local container action.
	Version string
	// Image is the image reference resolved (repository and tag).
	Image string
	// Digest is the image's digest: its registry digest when it has one, else its local image id.
	Digest string
}

// ParseLock parses a lock file strictly, with errors located by key path.
func ParseLock(data []byte) (*Lock, error) {
	l := &Lock{Actions: map[string]LockEntry{}}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}
	if len(doc.Content) == 0 {
		return l, nil
	}
	f, err := mapping(doc.Content[0], "")
	if err != nil {
		return nil, err
	}
	if err := f.allow("actions"); err != nil {
		return nil, err
	}
	n := f.get("actions")
	if isNull(n) {
		return l, nil
	}
	af, err := mapping(n, "actions")
	if err != nil {
		return nil, err
	}
	for _, k := range af.keys {
		ef, err := mapping(af.get(k.Value), af.sub(k.Value))
		if err != nil {
			return nil, err
		}
		if err := ef.allow("version", "image", "digest"); err != nil {
			return nil, err
		}
		var e LockEntry
		if err := optional(ef, "version", &e.Version, nonEmptyStr); err != nil {
			return nil, err
		}
		if e.Image, err = ef.requiredStr("image"); err != nil {
			return nil, err
		}
		if e.Digest, err = ef.requiredStr("digest"); err != nil {
			return nil, err
		}
		l.Actions[k.Value] = e
	}
	return l, nil
}

// Marshal encodes the lock file, actions in reference order, so that locking twice writes the same bytes.
func (l *Lock) Marshal() []byte {
	var b bytes.Buffer
	b.WriteString("# Written by millstream lock: the image of each container and registry action, pinned by digest.\n")
	b.WriteString("actions:")
	if len(l.Actions) == 0 {
		b.WriteString(" {}\n")
		return b.Bytes()
	}
	b.WriteString("\n")
	for _, ref := range slices.Sorted(maps.Keys(l.Actions)) {
		e := l.Actions[ref]
		fmt.Fprintf(&b, "  %s:\n", quote(ref))
		if e.Version != "" {
			fmt.Fprintf(&b, "    version: %s\n", quote(e.Version))
		}
		fmt.Fprintf(&b, "    image: %s\n    digest: %s\n", quote(e.Image), quote(e.Digest))
	}
	return b.Bytes()
}

// quote writes s as a double-quoted scalar, since references hold @ and versions look like numbers. The values
// are printable ASCII, for which Go's quoting is valid YAML.
func quote(s string) string { return strconv.Quote(s) }

// lock returns the package's lock file, or nil when it has none.
func (pkg *Package) lock() (*Lock, error) {
	data, ok := pkg.Files[LockFile]
	if !ok {
		return nil, nil
	}
	l, err := ParseLock(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", LockFile, err)
	}
	return l, nil
}
