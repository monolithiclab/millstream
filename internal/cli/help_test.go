package cli

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the help golden files")

// runHelp runs the CLI with env as the whole environment, its streams reaching a terminal when tty is set.
func runHelp(t *testing.T, env map[string]string, tty bool, argv ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = Run(context.Background(), argv, IO{In: strings.NewReader(""), Out: &out, Err: &errb}, "dev",
		func(rt *Runtime) {
			rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
			rt.IsTerminal = func(io.Writer) bool { return tty }
		})
	return code, out.String(), errb.String()
}

// flagLine is a line listing a flag, as a command's help does and the top level and groups must not.
var flagLine = regexp.MustCompile(`(?m)^\s+(-\w, )?--\w`)

// The three levels of help, each against its golden file: the top level and a group list commands without flags,
// a command shows its usage, examples, arguments and flags. The bare command prints the top level.
func Test_helpLevels(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		golden string
		argv   []string
		flags  bool
	}{
		{"root", []string{"--help"}, false},
		{"root", nil, false},
		{"inbox", []string{"inbox", "--help"}, false},
		{"trigger", []string{"trigger", "--help"}, false},
		{"replay", []string{"replay", "--help"}, true},
	} {
		t.Run(strings.Join(tt.argv, " "), func(t *testing.T) {
			t.Parallel()
			code, out, errOut := runHelp(t, nil, false, tt.argv...)
			if code != 0 || errOut != "" {
				t.Fatalf("code %d, stderr %q", code, errOut)
			}
			if got := flagLine.MatchString(out); got != tt.flags {
				t.Errorf("lists flags: %v, want %v:\n%s", got, tt.flags, out)
			}
			path := filepath.Join("testdata", "help", tt.golden+".golden")
			if *update && tt.argv != nil {
				if err := os.WriteFile(path, []byte(out), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if out != string(want) {
				t.Errorf("differs from %s (rerun with -update):\n%s", path, out)
			}
		})
	}
}

// Every top-level command shows under a group, and every command of a group in its help.
func Test_helpListsEveryCommand(t *testing.T) {
	t.Parallel()
	_, root, _ := runHelp(t, nil, false, "--help")
	for _, w := range []string{"Develop:", "Run:", "Observe:", "Operate:", "Improve:", "Help:", "  init ", "  inbox ",
		"  dead-letters ", "  report ", "  backtest ", "  help ", "Global flags, listed in every command's help: --db, --env-file"} {
		if !strings.Contains(root, w) {
			t.Errorf("top-level help has no %q", w)
		}
	}
	if strings.Contains(root, "Commands:") || strings.Contains(root, "improve-action") {
		t.Errorf("top-level help lists an ungrouped or hidden command:\n%s", root)
	}
	_, inbox, _ := runHelp(t, nil, false, "inbox", "--help")
	for _, w := range []string{"  list ", "  show <delivery>", "  approve <delivery>", "  reject <delivery>",
		"  samples ", "  sample <delivery> <decision>"} {
		if !strings.Contains(inbox, w) {
			t.Errorf("inbox help has no %q:\n%s", w, inbox)
		}
	}
}

// A default command whose name is optional shows as its parent's default form, with a pointer to its help, which
// prints its usage under the parent's name, with its flags.
func Test_helpShowsImplicitCommands(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		argv []string
		want []string
	}{
		{[]string{"trigger", "--help"}, []string{"Usage: millstream trigger <pipeline> <trigger> [flags]",
			"Default command:", `"millstream trigger fire --help"`, "token <pipeline> <trigger>"}},
		{[]string{"trigger", "fire", "--help"}, []string{"Usage: millstream trigger <pipeline> <trigger> [flags]",
			"--payload=PAYLOAD"}},
	} {
		code, out, _ := runHelp(t, nil, false, tt.argv...)
		for _, w := range tt.want {
			if code != 0 || !strings.Contains(out, w) {
				t.Errorf("%v: code %d, no %q in:\n%s", tt.argv, code, w, out)
			}
		}
	}
}

// help prints the guide and its topics, and help <command…> is <command…> --help.
func Test_helpCommand(t *testing.T) {
	t.Parallel()
	code, out, _ := runHelp(t, nil, false, "help")
	for _, w := range []string{"Using Millstream", "Concepts", "init pipeline", "Exit codes: 0 success", "--json",
		"--server", "millstream help topics"} {
		if code != 0 || !strings.Contains(out, w) {
			t.Errorf("help: code %d, no %q", code, w)
		}
	}
	if strings.ContainsAny(out, "`*") || strings.Contains(out, "### ") {
		t.Errorf("help prints Markdown marks or a topic:\n%s", out)
	}
	_, topics, _ := runHelp(t, nil, false, "help", "topics")
	for _, w := range []string{"scripting", "environment", "exit-codes"} {
		if !strings.Contains(topics, "  "+w+" ") {
			t.Errorf("help topics has no %s:\n%s", w, topics)
		}
		if code, out, _ := runHelp(t, nil, false, "help", w); code != 0 || !strings.HasPrefix(out, w+"\n") {
			t.Errorf("help %s: code %d:\n%s", w, code, out)
		}
	}
	for _, argv := range [][]string{{"run"}, {"report"}, {"inbox"}, {"inbox", "samples"}, {"dead-letters", "replay"},
		{"help"}} {
		_, viaHelp, _ := runHelp(t, nil, false, append([]string{"help"}, argv...)...)
		_, viaFlag, _ := runHelp(t, nil, false, append(argv, "--help")...)
		if viaHelp != viaFlag || viaHelp == "" {
			t.Errorf("help %v differs from its --help:\n%s\n---\n%s", argv, viaHelp, viaFlag)
		}
	}
	if code, out, errOut := runHelp(t, nil, false, "help", "frobnicate"); code != 2 || out != "" ||
		!strings.Contains(errOut, `no command or topic "frobnicate"`) {
		t.Errorf("help frobnicate: code %d, stdout %q, stderr %q", code, out, errOut)
	}
}

// The aliases kept for the names of older notes, dlq and insights ack, select the command they name: its help is the
// same, under its own name.
func Test_aliases(t *testing.T) {
	t.Parallel()
	for alias, name := range map[string]string{
		"dlq list --help":     "dead-letters list --help",
		"dlq replay --help":   "dead-letters replay --help",
		"dlq discard --help":  "dead-letters discard --help",
		"insights ack --help": "insights acknowledge --help",
		"help dlq replay":     "help dead-letters replay",
		"help insights ack":   "help insights acknowledge",
	} {
		code, viaAlias, errOut := runHelp(t, nil, false, strings.Fields(alias)...)
		_, viaName, _ := runHelp(t, nil, false, strings.Fields(name)...)
		if code != 0 || viaAlias != viaName || viaName == "" {
			t.Errorf("%s: code %d, stderr %q, help:\n%s\nwant:\n%s", alias, code, errOut, viaAlias, viaName)
		}
	}
}

// Help is colored on a terminal, unless NO_COLOR or TERM=dumb say otherwise, and when --color or MILLSTREAM_COLOR
// say always; never in a pipe or a file (the tests' buffers).
func Test_helpColors(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		env  map[string]string
		tty  bool
		argv []string
		want bool
	}{
		{"buffer", nil, false, []string{"--help"}, false},
		{"terminal", nil, true, []string{"--help"}, true},
		{"terminal guide", nil, true, []string{"help"}, true},
		{"terminal leaf", nil, true, []string{"replay", "--help"}, true},
		{"always", nil, false, []string{"--color=always", "--help"}, true},
		{"always after", nil, false, []string{"inbox", "--help", "--color=always"}, true},
		{"always help", nil, false, []string{"--color=always", "help", "run"}, true},
		{"env always", map[string]string{"MILLSTREAM_COLOR": "always"}, false, []string{"--help"}, true},
		{"never", nil, true, []string{"--color=never", "--help"}, false},
		{"env never", map[string]string{"MILLSTREAM_COLOR": "never"}, true, []string{"help"}, false},
		{"NO_COLOR", map[string]string{"NO_COLOR": "1"}, true, []string{"--help"}, false},
		{"NO_COLOR guide", map[string]string{"NO_COLOR": "1"}, true, []string{"help"}, false},
		{"TERM=dumb", map[string]string{"TERM": "dumb"}, true, []string{"--help"}, false},
		{"flag beats NO_COLOR", map[string]string{"NO_COLOR": "1"}, false, []string{"--color=always", "--help"}, true},
	} {
		_, out, _ := runHelp(t, tt.env, tt.tty, tt.argv...)
		if got := strings.Contains(out, "\x1b["); got != tt.want {
			t.Errorf("%s: colored %v, want %v:\n%q", tt.name, got, tt.want, out)
		}
	}
	// Usage after an error goes to stderr, colored by the same rules, and never to stdout.
	code, out, errOut := runHelp(t, nil, false, "--color=always", "dead-letters")
	if code != 2 || out != "" || !strings.Contains(errOut, "\x1b[") {
		t.Errorf("usage error: code %d, stdout %q, stderr %q", code, out, errOut)
	}
}

// Only a character device is a terminal: a regular file is not.
func Test_isCharDevice(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if isCharDevice(f) || isCharDevice(&bytes.Buffer{}) {
		t.Error("a regular file or a buffer counts as a terminal")
	}
}

// The guide reads as Markdown documentation: every topic has a name and a summary line.
func Test_guideSections(t *testing.T) {
	t.Parallel()
	main, topics := guideSections()
	if !strings.HasPrefix(main, "# Using Millstream") || len(topics) < 3 {
		t.Fatalf("guide: %d topics, starts %q", len(topics), main[:min(len(main), 20)])
	}
	for _, tp := range topics {
		if tp.name == "" || strings.Contains(tp.name, " ") || tp.summary == "" {
			t.Errorf("topic %+v", tp)
		}
	}
}
