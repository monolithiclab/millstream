package container

import (
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container/containertest"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

func TestImagesInspect(t *testing.T) {
	t.Parallel()
	rt, _ := fakeRuntime(t, map[string]containertest.Image{
		"report:dev": {ID: "sha256:abc", RepoDigests: []string{"ghcr.io/x/report@sha256:def"},
			Labels: map[string]string{pipeline.ManifestLabel: "{}"}},
	}, nil)
	img, err := Images{Runtime: rt}.Inspect("report:dev")
	if err != nil {
		t.Fatal(err)
	}
	if img.ID != "sha256:abc" || !slices.Equal(img.RepoDigests, []string{"ghcr.io/x/report@sha256:def"}) ||
		img.Labels[pipeline.ManifestLabel] != "{}" {
		t.Fatalf("image %+v", img)
	}
	if _, err := (Images{Runtime: rt}).Inspect("absent:1"); err == nil || !strings.Contains(err.Error(), "No such image") {
		t.Fatalf("an absent image: %v", err)
	}
}

// TestImagesInspectFormats covers Podman's output (a bare hex id, labels at the top level) and malformed output.
func TestImagesInspectFormats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, out, wantID, wantLabel string
		wantErr                      bool
	}{
		{"podman", `[{"Id": "abc", "Labels": {"k": "v"}, "Config": {}}]`, "sha256:abc", "v", false},
		{"docker", `[{"Id": "sha256:abc", "Config": {"Labels": {"k": "w"}}}]`, "sha256:abc", "w", false},
		{"no image", `[]`, "", "", true},
		{"no id", `[{"Config": {}}]`, "", "", true},
		{"not json", `oops`, "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			img, err := Images{Runtime: rawRuntime(t, pipeline.RuntimePodman, tt.out)}.Inspect("x")
			if (err != nil) != tt.wantErr || img.ID != tt.wantID || img.Labels["k"] != tt.wantLabel {
				t.Fatalf("Inspect = %+v, %v", img, err)
			}
		})
	}
}

func TestImagesTags(t *testing.T) {
	t.Parallel()
	rt, _ := fakeRuntime(t, nil, map[string][]string{"ghcr.io/x/score": {"1.0.0", "<none>", "latest"}})
	tags, err := Images{Runtime: rt}.Tags("ghcr.io/x/score")
	if err != nil || !slices.Equal(tags, []string{"1.0.0", "latest"}) {
		t.Fatalf("Tags = %v, %v", tags, err)
	}
	if tags, err := (Images{Runtime: rt}).Tags("ghcr.io/x/none"); err != nil || len(tags) != 0 {
		t.Fatalf("Tags of an unknown repository = %v, %v", tags, err)
	}
	if _, err := (Images{Runtime: missingRuntime(t)}).Tags("x"); err == nil {
		t.Fatal("Tags with a missing CLI: no error")
	}
}

func TestLauncherCommandAndKill(t *testing.T) {
	t.Parallel()
	rt, calls := fakeRuntime(t, nil, nil)
	l := Launcher{Runtime: rt}
	spec := exec.Spec{Action: "lab/fake", Kind: pipeline.KindContainer, Image: "sha256:abc"}
	cmd, err := l.Command(spec, "/tmp/millstream-work-7")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Path != rt.Bin || !slices.Equal(cmd.Env, rt.Env) || cmd.Args[1] != "run" ||
		cmd.Args[len(cmd.Args)-1] != "sha256:abc" {
		t.Fatalf("command %q env %q", cmd.Args, cmd.Env)
	}
	l.Kill(spec, "/tmp/millstream-work-7")
	if calls := calls(); !slices.Equal(calls, []string{"rm --force millstream-millstream-work-7"}) {
		t.Fatalf("calls = %q", calls)
	}
}
