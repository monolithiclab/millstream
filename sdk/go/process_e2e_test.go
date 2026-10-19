//go:build e2e

package millstream

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// helperEnv makes the test binary act as an action process, for the tests of Map and MapKeyed, which own the
// process's stdio and exit status.
const helperEnv = "MILLSTREAM_SDK_TEST_ACTION"

func TestMain(m *testing.M) {
	switch os.Getenv(helperEnv) {
	case "map":
		Map(score)
		os.Exit(0)
	case "abstain":
		Map(rules)
		os.Exit(0)
	case "mapKeyed":
		MapKeyed(split)
		os.Exit(0)
	case "artifact":
		Map(annotate(os.Getenv("MILLSTREAM_WORKDIR")))
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestMapProcess runs the cases of TestServe as processes: the test binary re-executed as an action calling Map or
// MapKeyed on its own stdio.
func TestMapProcess(t *testing.T) {
	t.Parallel()
	workdir := artifactWorkdir(t)
	for _, tt := range serveCases(workdir) {
		cmd := exec.Command(os.Args[0], "-test.run=^$") // #nosec G204 -- the test binary itself
		cmd.Env = append(os.Environ(), helperEnv+"="+tt.name)
		for k, v := range tt.env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		cmd.Stdin = strings.NewReader(strings.Join(tt.input, "\n") + "\n")
		var stdout, stderr strings.Builder
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		code := 0
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			code = exit.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		tt.check(t, code, stdout.String(), stderr.String())
	}
	checkNotes(t, workdir)
}
