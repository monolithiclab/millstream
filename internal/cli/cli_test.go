package cli

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
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
			rt.Launchers = testLaunchers()
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

// The default commands whose name is optional show under their parent's: its help prints their arguments and
// flags, then its other commands.
func Test_helpShowsImplicitCommands(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		argv []string
		want []string
	}{
		{[]string{"run", "--help"}, []string{"Usage: millstream run [<dirs> ...] [flags]", "--once", "--listen=HOST:PORT",
			"run report <pipeline> <run>"}},
		{[]string{"trigger", "--help"}, []string{"Usage: millstream trigger <pipeline> <trigger> [flags]", "--payload",
			"trigger token <pipeline> <trigger>"}},
		{[]string{"node", "--help"}, []string{"Usage: millstream node --join=", "--allow-exec", "node drain"}},
		{[]string{"--help"}, []string{"  run [<dirs> ...] [flags]", "  trigger <pipeline> <trigger> [flags]",
			"  node --join="}},
	} {
		code, out, _ := runCLI(t, "dev", tt.argv...)
		for _, w := range tt.want {
			if code != 0 || !strings.Contains(out, w) {
				t.Errorf("%v: code %d, no %q in:\n%s", tt.argv, code, w, out)
			}
		}
		if strings.Contains(out, "run packages") || strings.Contains(out, "trigger fire") {
			t.Errorf("%v names an implicit command:\n%s", tt.argv, out)
		}
	}
}

func Test_configureSeamAdjustsRuntime(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	code := Run(context.Background(), []string{"version"}, IO{Out: &out, Err: &out}, "dev", func(rt *Runtime) {
		rt.Version = "x"
		rt.Launchers = testLaunchers()
	})
	if code != 0 || out.String() != "millstream x\n" {
		t.Fatalf("code=%d out=%q", code, out.String())
	}
}

// The parser skips kong's hooks (parser.parse): no type of the command tree may define one, or it would never run.
// Kong's own --help flag, whose hook parse handles, is the exception.
func TestNoKongHooks(t *testing.T) {
	t.Parallel()
	p, err := takeParser(&Runtime{Ctx: context.Background(), IO: IO{Out: io.Discard, Err: io.Discard}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.release()
	seen := 0
	_ = kong.Visit(p.k.Model, func(n kong.Visitable, next kong.Next) error {
		var target reflect.Value
		switch n := n.(type) {
		case *kong.Application:
			target = n.Target
		case *kong.Node:
			target = n.Target
		case *kong.Value:
			target = n.Target
		}
		if target.IsValid() && target.Type().PkgPath() != reflect.TypeFor[kong.Kong]().PkgPath() {
			seen++
			for _, hook := range []string{"BeforeReset", "BeforeResolve", "BeforeApply", "AfterApply", "AfterRun"} {
				if _, ok := reflect.PointerTo(target.Type()).MethodByName(hook); ok {
					t.Errorf("%s defines %s, which the parser never calls", target.Type(), hook)
				}
			}
		}
		return next(nil)
	})
	if seen < 40 {
		t.Fatalf("visited %d targets: the walk missed the command tree", seen)
	}
}

// Who runs a command is $USER, else the account the system names, else nobody.
func Test_identity(t *testing.T) {
	t.Parallel()
	env := func(m map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
	}
	account := func() (string, error) { return "bob", nil }
	for _, tt := range []struct {
		rt   Runtime
		want string
	}{
		{Runtime{Env: env(map[string]string{"USER": "ana"}), CurrentUser: account}, "ana"},
		{Runtime{Env: env(map[string]string{"USER": ""}), CurrentUser: account}, "bob"},
		{Runtime{Env: env(nil), CurrentUser: noAccount}, ""},
	} {
		if got := identity(&tt.rt); got != tt.want {
			t.Errorf("identity = %q, want %q", got, tt.want)
		}
	}
}
