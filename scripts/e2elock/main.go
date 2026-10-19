// Command e2elock runs a command under a machine-wide lock, so that the heavy suites (make test-e2e, test-ui-e2e,
// soak) of several checkouts queue instead of overloading the machine together. macOS has no flock(1): this is it,
// on a file in the temporary directory ($TMPDIR, per user).
//
//	go run ./scripts/e2elock -- make test-e2e-locked
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

func main() {
	os.Exit(run(os.Args[1:], filepath.Join(os.TempDir(), "millstream-e2e.lock"), os.Stderr))
}

// run runs args under the lock at path, saying on stderr whom it waits for, and returns the command's exit code.
func run(args []string, path string, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, "usage: e2elock [--] command [args...]")
		return 2
	}
	f, err := acquire(path, stderr)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "e2elock:", err)
		return 1
	}
	defer func() { _ = f.Close() }()          // closing the file releases the lock
	cmd := exec.Command(args[0], args[1:]...) // #nosec G204 G702 -- running the given command is the point
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, stderr
	// Ctrl-C reaches the whole process group, the command included: wait for it to stop. SIGTERM is ours alone:
	// pass it on.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	if err := cmd.Start(); err != nil {
		_, _ = fmt.Fprintln(stderr, "e2elock:", err)
		return 1
	}
	go func() {
		for s := range sigs {
			if s == syscall.SIGTERM {
				_ = cmd.Process.Signal(s)
			}
		}
	}()
	err = cmd.Wait()
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		return max(exit.ExitCode(), 1) // killed by a signal: -1
	case err != nil:
		_, _ = fmt.Fprintln(stderr, "e2elock:", err)
		return 1
	}
	return 0
}

// acquire takes the lock, waiting for its holder, whose pid and directory the file holds, and records ours.
func acquire(path string, stderr io.Writer) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644) // #nosec G304 -- the lock file in $TMPDIR
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			_ = f.Close()
			return nil, err
		}
		holder, _ := io.ReadAll(f)
		_, _ = fmt.Fprintf(stderr, "waiting for the e2e lock held by %s\n", describe(string(holder)))
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	dir, _ := os.Getwd()
	if err := f.Truncate(0); err == nil {
		_, _ = f.WriteAt(fmt.Appendf(nil, "%d %s\n", os.Getpid(), dir), 0)
	}
	return f, nil
}

// describe renders the lock file's "<pid> <dir>" as "<pid> (<dir>)".
func describe(holder string) string {
	pid, dir, _ := strings.Cut(strings.TrimSpace(holder), " ")
	switch {
	case pid == "":
		return "another process"
	case dir == "":
		return pid
	}
	return pid + " (" + dir + ")"
}
