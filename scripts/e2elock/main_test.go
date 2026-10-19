package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// signalWriter sends each write on a channel.
type signalWriter chan string

func (w signalWriter) Write(p []byte) (int, error) {
	w <- string(p)
	return len(p), nil
}

// TestAcquireWaits: a second taker waits, naming the holder's pid and directory, and gets the lock once the holder
// lets it go, recording itself.
func TestAcquireWaits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	first, err := acquire(path, signalWriter(make(chan string)))
	if err != nil {
		t.Fatal(err)
	}
	said := make(signalWriter, 1)
	got := make(chan error, 1)
	go func() {
		f, err := acquire(path, said)
		if err == nil {
			err = f.Close()
		}
		got <- err
	}()
	dir, _ := os.Getwd()
	if msg := <-said; msg != "waiting for the e2e lock held by "+strconv.Itoa(os.Getpid())+" ("+dir+")\n" {
		t.Errorf("message %q", msg)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-got; err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if !strings.HasPrefix(string(data), strconv.Itoa(os.Getpid())+" ") {
		t.Errorf("lock file %q", data)
	}
}

func TestDescribe(t *testing.T) {
	for in, want := range map[string]string{"": "another process", "12\n": "12", "12 /w/x\n": "12 (/w/x)"} {
		if got := describe(in); got != want {
			t.Errorf("describe(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestUsage(t *testing.T) {
	var b strings.Builder
	if code := run([]string{"--"}, filepath.Join(t.TempDir(), "lock"), &b); code != 2 || !strings.Contains(b.String(), "usage") {
		t.Errorf("run() = %d, %q", code, b.String())
	}
}
