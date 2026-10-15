package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func runCLI(t *testing.T, version string, argv ...string) (code int, stdout, stderr string) {
	t.Helper()
	return runCLIEnv(t, version, nil, argv...)
}

// runCLIEnv runs the CLI with env as the whole environment.
func runCLIEnv(t *testing.T, version string, env map[string]string, argv ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = Run(context.Background(), argv, IO{In: strings.NewReader(""), Out: &out, Err: &errb}, version,
		func(rt *Runtime) {
			rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
		})
	return code, out.String(), errb.String()
}

func Test_versionPrintsVersion(t *testing.T) {
	t.Parallel()
	code, out, _ := runCLI(t, "1.2.3", "version")
	if code != 0 || out != "millstream 1.2.3\n" {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func Test_unknownCommandPrintsUsage(t *testing.T) {
	t.Parallel()
	code, out, errOut := runCLI(t, "dev", "frobnicate")
	if code != 2 || out != "" || !strings.Contains(errOut, "Usage:") {
		t.Fatalf("code=%d out=%q err=%q", code, out, errOut)
	}
}

func Test_helpExitsZero(t *testing.T) {
	t.Parallel()
	code, out, _ := runCLI(t, "dev", "--help")
	if code != 0 || !strings.Contains(out, "Usage:") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func Test_configureSeamAdjustsRuntime(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	code := Run(context.Background(), []string{"version"}, IO{Out: &out, Err: &out}, "dev", func(rt *Runtime) { rt.Version = "x" })
	if code != 0 || out.String() != "millstream x\n" {
		t.Fatalf("code=%d out=%q", code, out.String())
	}
}
