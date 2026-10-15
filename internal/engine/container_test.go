package engine

import (
	"errors"
	"slices"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// oneImage is a container runtime holding one local image, report:dev.
type oneImage struct{}

const reportImageID = "sha256:5555555555555555555555555555555555555555555555555555555555555555"

func (oneImage) Inspect(ref string) (pipeline.Image, error) {
	if ref != "report:dev" {
		return pipeline.Image{}, errors.New("no such image")
	}
	return pipeline.Image{ID: reportImageID}, nil
}

func (oneImage) Tags(string) ([]string, error) { return nil, nil }

// TestContainerAssignment: a container action's delivery names its pinned image, its command and its network, and
// carries none of the engine's environment.
func TestContainerAssignment(t *testing.T) {
	t.Parallel()
	environ := map[string]string{"PATH": "/usr/bin", "HOME": "/home/u"}
	v := newEnv(t, Options{
		Env:      func(k string) (string, bool) { s, ok := environ[k]; return s, ok },
		Resolver: pipeline.Resolver{Images: oneImage{}},
	})
	pkg := newPackage(t, manualPipeline("p", "\n  r: { action: ./actions/r, in: trigger.adhoc }\n"))
	pkg.Files["actions/r/action.yaml"] = []byte("name: lab/report\nversion: 1.0.0\nkind: container\n" +
		"runtime: { container: { image: report:dev, command: [/report], network: [api.example.com] } }\n")
	v.apply(pkg)
	v.fire("p", "adhoc", "k")
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("Pull = %v, %v", as, err)
	}
	s := as[0].Spec
	if s.Kind != pipeline.KindContainer || s.Image != reportImageID || s.Digest != reportImageID || !s.Network ||
		!slices.Equal(s.Command, []string{"/report"}) || s.Env != nil || s.Dir != "" || s.Action != "lab/report" {
		t.Fatalf("spec = %+v", s)
	}
}
