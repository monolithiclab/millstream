package container

import (
	"context"
	"io"
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
			[]string{"--network", "none", "--user", "501:20", "--env", "TZ", "sha256:abc", "/fake", "--fast"},
			[]string{"--userns"}},
		{"docker with network", Runtime{Name: pipeline.RuntimeDocker, User: "501:20"}, true,
			[]string{"--user", "501:20", "--env", "TZ", "sha256:abc"}, []string{"none"}},
		{"podman rootless", Runtime{Name: pipeline.RuntimePodman, User: "1000:1000"}, false,
			[]string{"--network", "none", "--userns", "keep-id", "--env", "TZ", "sha256:abc"}, []string{"--user"}},
		{"podman as root", Runtime{Name: pipeline.RuntimePodman, User: "0:0"}, false,
			[]string{"--user", "0:0"}, []string{"--userns"}},
		{"no user", Runtime{Name: pipeline.RuntimeDocker}, false, []string{"sha256:abc"}, []string{"--user", "--userns"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := spec
			s.Network = tt.network
			args := Launcher{Runtime: tt.rt}.Args(s, "/tmp/millstream-work-42")
			common := []string{"run", "--interactive", "--rm", "--pull=never", "--name", "millstream-millstream-work-42",
				"--read-only", "--tmpfs", "/tmp", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
				"--mount", "type=bind,src=/tmp/millstream-work-42,dst=/tmp/millstream-work-42", "--workdir", "/tmp/millstream-work-42",
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

// TestArgsMountsAndSecrets: extra mounts follow the working directory's, read-only when asked, as --mount bind
// mounts so a path may hold a colon (one with a comma, --mount's separator, is refused); a variable the CLI
// does not have, a secret, is passed by name with its value only in the CLI's environment; one the CLI has (HOME) is
// passed inline, so the CLI keeps its own.
func TestArgsMountsAndSecrets(t *testing.T) {
	t.Parallel()
	l := Launcher{Runtime: Runtime{Name: pipeline.RuntimeDocker, Bin: "/usr/bin/docker", Env: []string{"PATH=/usr/bin", "HOME=/home/u"}}}
	spec := exec.Spec{Action: "lab/agent", Image: "agents:1",
		Mounts: []exec.Mount{{Source: "/opt/ms", Target: "/millstream", ReadOnly: true}, {Source: "/rec:a", Target: "/rec:a"}},
		Env:    []string{"HOME=/tmp", "MILLSTREAM_SECRET_API_KEY=sk-123456"}}
	cmd, err := l.Command(spec, "/w")
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{"--mount type=bind,src=/opt/ms,dst=/millstream,readonly " +
		"--mount type=bind,src=/rec:a,dst=/rec:a --env HOME=/tmp",
		"--env MILLSTREAM_SECRET_API_KEY agents:1"} {
		if !strings.Contains(args, want) {
			t.Fatalf("args %q lack %q", args, want)
		}
	}
	if strings.Contains(args, "sk-123456") || !slices.Contains(cmd.Env, "MILLSTREAM_SECRET_API_KEY=sk-123456") ||
		!slices.Contains(cmd.Env, "PATH=/usr/bin") || !slices.Contains(cmd.Env, "HOME=/home/u") ||
		slices.Contains(cmd.Env, "HOME=/tmp") {
		t.Fatalf("args %q, env %q", args, cmd.Env)
	}
	spec.Mounts = append(spec.Mounts, exec.Mount{Source: "/a,b", Target: "/a"})
	if _, err := l.Command(spec, "/w"); err == nil || !strings.Contains(err.Error(), "comma") {
		t.Fatalf("a mount path with a comma: %v", err)
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

// TestJobArgs: a job is hardened as an action's container is, its variables passed by name, and has the network
// only when it asks for it.
func TestJobArgs(t *testing.T) {
	t.Parallel()
	rt := Runtime{Name: pipeline.RuntimeDocker, User: "501:20", Env: []string{"PATH=/bin"}}
	j := Job{Name: "millstream-job-1", Image: "img@sha256:abc", Entrypoint: "/bin/sh", Command: []string{"-c", "true"},
		Workdir: "/work", Mounts: []exec.Mount{{Source: "/h/src", Target: "/src", ReadOnly: true}},
		Env: []string{"TOKEN=secret", "PATH=/x"}}
	got := strings.Join(rt.JobArgs(j), " ")
	want := "run --rm --pull=never --name millstream-job-1 --read-only --tmpfs /tmp --cap-drop ALL " +
		"--security-opt no-new-privileges --network none --user 501:20 --entrypoint /bin/sh --workdir /work " +
		"--mount type=bind,src=/h/src,dst=/src,readonly --env TOKEN --env PATH=/x img@sha256:abc -c true"
	if got != want {
		t.Fatalf("JobArgs\n%s\nwant\n%s", got, want)
	}
	j.Network = true
	if args := rt.JobArgs(j); slices.Contains(args, "none") || strings.Contains(strings.Join(args, " "), "secret") {
		t.Fatalf("a job with the network: %q", args)
	}
}

// TestRunJob: a job runs through the CLI, its container removed when its context ends first; a mount path with a
// comma is refused.
func TestRunJob(t *testing.T) {
	t.Parallel()
	rt, calls := fakeRuntime(t, nil, nil)
	ctx, cancel := context.WithCancel(t.Context())
	j := Job{Name: "millstream-job-2", Image: "img@sha256:abc", Env: []string{"TOKEN=secret"}}
	if err := rt.RunJob(ctx, j, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = rt.RunJob(ctx, j, io.Discard, io.Discard) // a process CLI (the e2e build's) is killed: either answer will do
	got := calls()
	if len(got) < 2 || !strings.HasPrefix(got[0], "run --rm") || !strings.HasSuffix(got[0], "--env TOKEN img@sha256:abc") ||
		got[len(got)-1] != "rm --force millstream-job-2" {
		t.Fatalf("calls = %q", got)
	}
	j.Mounts = []exec.Mount{{Source: "/a,b", Target: "/x"}}
	if err := rt.RunJob(t.Context(), j, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "comma") {
		t.Fatalf("a comma in a mount: %v", err)
	}
}
