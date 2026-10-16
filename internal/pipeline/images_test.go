package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// fakeImages serves canned local images.
type fakeImages struct {
	images map[string]Image
	tags   map[string][]string
}

func (f fakeImages) Inspect(ref string) (Image, error) {
	img, ok := f.images[ref]
	if !ok {
		return Image{}, fmt.Errorf("no such image: %s", ref)
	}
	return img, nil
}

func (f fakeImages) Tags(repo string) ([]string, error) {
	if repo == "broken/x" {
		return nil, errors.New("daemon down")
	}
	return f.tags[repo], nil
}

const (
	idReport = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	idScore1 = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	idScore2 = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	digest2  = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
)

func scoreLabel(version string) map[string]string {
	return map[string]string{ManifestLabel: `{"name": "lab/score", "version": "` + version + `", "kind": "container",
		"input": {"type": "object"}, "runtime": {"container": {"command": ["/score"], "network": ["api.example.com"]}}}`}
}

var testImages = fakeImages{
	images: map[string]Image{
		"report:dev":                      {ID: idReport},
		"ghcr.io/lab/actions/score:1.2.0": {ID: idScore1, Labels: scoreLabel("1.2.0")},
		"ghcr.io/lab/actions/score:1.10.0": {ID: idScore2, Labels: scoreLabel("1.10.0"),
			RepoDigests: []string{"mirror.example/score@sha256:9999", "ghcr.io/lab/actions/score@" + digest2}},
		"ghcr.io/lab/actions/score:2.0.0":   {ID: idScore1, Labels: scoreLabel("2.0.1")},
		"ghcr.io/lab/actions/nolabel:1.0.0": {ID: idScore1},
		"ghcr.io/lab/actions/exec:1.0.0": {ID: idScore1, Labels: map[string]string{
			ManifestLabel: `{"name": "lab/exec", "version": "1.0.0", "kind": "exec", "runtime": {"exec": {"command": ["x"]}}}`}},
	},
	tags: map[string][]string{
		"ghcr.io/lab/actions/score":   {"1.2.0", "1.10.0", "latest", "2.0.0", "1.9", "v1.11.0"},
		"ghcr.io/lab/actions/nolabel": {"1.0.0"},
		"ghcr.io/lab/actions/exec":    {"1.0.0"},
	},
}

const containerManifest = `
name: report
version: 0.1.0
kind: container
runtime: { container: { image: "report:dev", command: [/report, --plain] } }
`

func containerPackage(lock string) *Package {
	pkg := &Package{Dir: "/pkg", Files: map[string][]byte{"actions/report/action.yaml": []byte(containerManifest)}}
	if lock != "" {
		pkg.Files[LockFile] = []byte(lock)
	}
	return pkg
}

func testResolver(ignoreLock bool) Resolver {
	r := Resolver{Images: testImages, IgnoreLock: ignoreLock}
	r.Registry = Registry{Prefixes: map[string]string{"lab": "ghcr.io/lab/actions"}, Resolver: r}
	return r
}

func TestResolveLocalContainer(t *testing.T) {
	t.Parallel()
	a, err := testResolver(false).Resolve(containerPackage(""), "./actions/report")
	if err != nil {
		t.Fatal(err)
	}
	if a.Kind != KindContainer || a.Image != "report:dev" || a.Pin != idReport || a.ImageDigest != idReport ||
		!a.Unpinned || a.Network || strings.Join(a.Command, " ") != "/report --plain" || a.Lifecycle != LifecycleLong {
		t.Fatalf("action %+v", a)
	}
	locked := "actions:\n  ./actions/report: { image: report:dev, digest: " + idReport + " }\n"
	if a, err := testResolver(false).Resolve(containerPackage(locked), "./actions/report"); err != nil || a.Unpinned {
		t.Fatalf("locked: %+v, %v", a, err)
	}
	stale := "actions:\n  ./actions/report: { image: report:dev, digest: " + idScore1 + " }\n"
	if _, err := testResolver(false).Resolve(containerPackage(stale), "./actions/report"); err == nil ||
		!strings.Contains(err.Error(), "millstream.lock pins") {
		t.Fatalf("a digest mismatch: %v", err)
	}
	if a, err := testResolver(true).Resolve(containerPackage(stale), "./actions/report"); err != nil || a.Unpinned {
		t.Fatalf("ignoring the lock: %+v, %v", a, err)
	}
	if _, err := (Resolver{}).Resolve(containerPackage(""), "./actions/report"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("without a container runtime: %v", err)
	}
}

func TestResolveContainerManifestErrors(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"no image":      "name: r\nkind: container\n",
		"unknown key":   "name: r\nkind: container\nruntime: { container: { image: x, ports: [80] } }\n",
		"image list":    "name: r\nkind: container\nruntime: { container: { image: [x] } }\n",
		"command str":   "name: r\nkind: container\nruntime: { container: { image: x, command: /r } }\n",
		"network empty": "name: r\nkind: container\nruntime: { container: { image: x, network: [\"\"] } }\n",
		"missing image": "name: r\nkind: container\nruntime: { container: { image: absent:1 } }\n",
		"bad lock":      "",
	}
	for name, manifest := range tests {
		t.Run(name, func(t *testing.T) {
			pkg := containerPackage("")
			if manifest != "" {
				pkg.Files["actions/report/action.yaml"] = []byte(manifest)
			} else {
				pkg.Files[LockFile] = []byte("actions: { ./actions/report: { image: x } }\n")
			}
			if _, err := testResolver(false).Resolve(pkg, "./actions/report"); err == nil {
				t.Fatal("no error")
			}
		})
	}
}

func TestResolveRegistry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		ref, lock                        string
		wantVersion, wantPin, wantDigest string
		wantUnpinned                     bool
	}{
		{ref: "lab/score@1", wantVersion: "1.10.0", wantPin: "ghcr.io/lab/actions/score@" + digest2, wantDigest: digest2,
			wantUnpinned: true},
		{ref: "lab/score@1.2", wantVersion: "1.2.0", wantPin: idScore1, wantDigest: idScore1, wantUnpinned: true},
		{ref: "lab/score@1.2.0", wantVersion: "1.2.0", wantPin: idScore1, wantDigest: idScore1, wantUnpinned: true},
		{ref: "lab/score@1",
			lock:        "actions:\n  lab/score@1: { version: 1.2.0, image: ghcr.io/lab/actions/score:1.2.0, digest: " + idScore1 + " }\n",
			wantVersion: "1.2.0", wantPin: idScore1, wantDigest: idScore1},
	}
	for _, tt := range tests {
		t.Run(tt.ref+tt.lock, func(t *testing.T) {
			a, err := testResolver(false).Resolve(containerPackage(tt.lock), tt.ref)
			if err != nil {
				t.Fatal(err)
			}
			if a.Ref != tt.ref || a.Name != "lab/score" || a.Version != tt.wantVersion || a.Pin != tt.wantPin ||
				a.ImageDigest != tt.wantDigest || a.Unpinned != tt.wantUnpinned || !a.Network || a.Dir != "" ||
				a.Input == nil || a.Image != "ghcr.io/lab/actions/score:"+tt.wantVersion {
				t.Fatalf("action %+v", a)
			}
		})
	}
}

func TestResolveRegistryErrors(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, ref, lock, want string }{
		{"unmapped namespace", "other/score@1", "", "namespace other is not mapped"},
		{"bad range", "lab/score@^1", "", "range"},
		{"range too long", "lab/score@1.2.3.4", "", "range"},
		{"no tag in range", "lab/score@3", "", "no local image"},
		{"version mismatch", "lab/score@2", "", "declares version"},
		{"no label", "lab/nolabel@1", "", "has no io.millstream.manifest label"},
		{"exec label", "lab/exec@1", "", "a registry action is a container action"},
		{"absent image", "lab/absent@1", "", "no local image"},
		{"lock outside the range", "lab/score@2",
			"actions:\n  lab/score@2: { version: 1.2.0, image: ghcr.io/lab/actions/score:1.2.0, digest: " + idScore1 + " }\n",
			"outside the range"},
		{"lock digest mismatch", "lab/score@1",
			"actions:\n  lab/score@1: { version: 1.2.0, image: ghcr.io/lab/actions/score:1.2.0, digest: " + idScore2 + " }\n",
			"millstream.lock pins"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := testResolver(false).Resolve(containerPackage(tt.lock), tt.ref)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
	r := Resolver{}
	r.Registry = Registry{Resolver: r}
	if _, err := r.Resolve(containerPackage(""), "lab/score@1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("without a container runtime: %v", err)
	}
	broken := testResolver(false)
	broken.Registry = Registry{Prefixes: map[string]string{"lab": "broken"}, Resolver: broken}
	if _, err := broken.Resolve(containerPackage(""), "lab/x@1"); err == nil || !strings.Contains(err.Error(), "daemon down") {
		t.Fatalf("tags failing: %v", err)
	}
}
