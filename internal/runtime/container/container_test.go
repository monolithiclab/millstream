package container

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

func TestArgs(t *testing.T) {
	t.Parallel()
	spec := exec.Spec{Action: "lab/fake", Kind: pipeline.KindContainer, Image: "sha256:abc",
		Command: []string{"/fake", "--fast"}, Env: []string{"TZ=UTC"}}
	tests := []struct {
		name    string
		rt      Runtime
		network bool
		want    []string // in order, after the common flags
		absent  []string
	}{
		{"docker", Runtime{Name: pipeline.RuntimeDocker, User: "501:20"}, false,
			[]string{"--network", "none", "--user", "501:20", "--env", "TZ=UTC", "sha256:abc", "/fake", "--fast"},
			[]string{"--userns"}},
		{"docker with network", Runtime{Name: pipeline.RuntimeDocker, User: "501:20"}, true,
			[]string{"--user", "501:20", "--env", "TZ=UTC", "sha256:abc"}, []string{"none"}},
		{"podman rootless", Runtime{Name: pipeline.RuntimePodman, User: "1000:1000"}, false,
			[]string{"--network", "none", "--userns", "keep-id", "--env", "TZ=UTC", "sha256:abc"}, []string{"--user"}},
		{"podman as root", Runtime{Name: pipeline.RuntimePodman, User: "0:0"}, false,
			[]string{"--user", "0:0"}, []string{"--userns"}},
		{"no user", Runtime{Name: pipeline.RuntimeDocker}, false, []string{"sha256:abc"}, []string{"--user", "--userns"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := spec
			s.Network = tt.network
			args := Launcher{Runtime: tt.rt}.Args(s, "/tmp/millstream-work-42")
			common := []string{"run", "--interactive", "--rm", "--pull=never", "--name", "millstream-millstream-work-42",
				"--read-only", "--tmpfs", "/tmp", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
				"--volume", "/tmp/millstream-work-42:/tmp/millstream-work-42", "--workdir", "/tmp/millstream-work-42",
				"--env", "MILLSTREAM_WORKDIR=/tmp/millstream-work-42"}
			if !slices.Equal(args[:len(common)], common) {
				t.Fatalf("args = %q", args)
			}
			rest := strings.Join(args[len(common):], " ")
			if !strings.Contains(rest, strings.Join(tt.want, " ")) {
				t.Fatalf("args end %q, want %q in order", rest, tt.want)
			}
			for _, a := range tt.absent {
				if slices.Contains(args, a) {
					t.Fatalf("args %q contain %s", args, a)
				}
			}
		})
	}
	if _, err := (Launcher{}).Command(exec.Spec{Action: "x"}, "/w"); err == nil {
		t.Fatal("Command without an image: no error")
	}
}

func TestFind(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bin := filepath.Join(dir, "docker")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o700); err != nil { // #nosec G306 -- a fake CLI
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "podman"), nil, 0o600); err != nil { // not executable
		t.Fatal(err)
	}
	env := map[string]string{"PATH": "/nonexistent" + string(os.PathListSeparator) + dir, "DOCKER_HOST": "unix:///x",
		"SECRET": "s"}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	rt, ok := Find("docker", lookup)
	if !ok || rt.Bin != bin || rt.Name != "docker" {
		t.Fatalf("Find docker = %+v, %v", rt, ok)
	}
	if !slices.Contains(rt.Env, "DOCKER_HOST=unix:///x") || slices.ContainsFunc(rt.Env, func(kv string) bool {
		return strings.HasPrefix(kv, "SECRET=")
	}) {
		t.Fatalf("CLI environment %q", rt.Env)
	}
	if _, ok := Find("podman", lookup); ok {
		t.Fatal("Find found a file that is not executable")
	}
}
