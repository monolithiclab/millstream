// Package pgtest runs the Postgres the e2e tests use: one container of the pinned official image per test binary,
// started on first use and removed by Stop, a template database migrated once, and a new database per test copied
// from it, so each test starts from an empty, migrated store in a few milliseconds. Without Docker, the tests that
// need Postgres skip, saying so.
//
// A test binary's TestMain calls Stop after the tests. A binary killed before it could (a -timeout panic, a kill)
// leaves its container behind: the next binary to start one removes the containers whose process is gone.
package pgtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/monolithiclab/millstream/internal/store/postgres"
)

// Image is the Postgres the tests run: the official image, postgres:17 (17.11), pinned by the digest of its
// multi-platform index, pulled through mirror.gcr.io (docs/development/testing.md says why).
// Refresh: docker buildx imagetools inspect mirror.gcr.io/library/postgres:17, then update the version.
const Image = "mirror.gcr.io/library/postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

// label marks the containers pgtest starts, with the pid of the test binary that started each.
const label = "millstream.pgtest.pid"

const (
	password = "pgtest"
	template = "millstream_template"
)

// server is the running container.
type server struct {
	id    string
	addr  string // host:port on the loopback interface
	admin *pgx.Conn
	mu    sync.Mutex // guards admin
}

var (
	started = sync.OnceValues(start)
	stopped atomic.Bool
	seq     atomic.Int64
)

// errNoDocker is why the tests skip without Docker.
var errNoDocker = errors.New("postgres tests skipped: Docker is unavailable")

// DSN returns the DSN of a new database for t, a copy of the migrated template, superuser access, its password in
// PGPASSWORD. It skips t when Docker is unavailable, and fails it when the server cannot start.
func DSN(t testing.TB) string {
	t.Helper()
	s, err := started()
	if errors.Is(err, errNoDocker) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("t%d", seq.Add(1))
	s.mu.Lock()
	_, err = s.admin.Exec(context.Background(), `CREATE DATABASE `+name+` TEMPLATE `+template)
	s.mu.Unlock()
	if err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	// The data directory is memory: a test's database goes when the test ends, whatever is still connected to it.
	t.Cleanup(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, _ = s.admin.Exec(context.Background(), `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`)
	})
	return "postgres://postgres@" + s.addr + "/" + name + "?sslmode=disable"
}

// Database returns the name of the database dsn names, for the queries Admin runs about it.
func Database(t testing.TB, dsn string) string {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Database
}

// Admin runs fn on the server's superuser connection, to the postgres database: for tests that act on the server
// itself, such as ending another session.
func Admin(t testing.TB, fn func(ctx context.Context, c *pgx.Conn) error) {
	t.Helper()
	s, err := started()
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := fn(context.Background(), s.admin); err != nil {
		t.Fatal(err)
	}
}

// Stop removes the container, if one started.
func Stop() {
	if stopped.Swap(true) {
		return
	}
	if s, err := started(); err == nil {
		_ = s.admin.Close(context.Background())
		_ = docker("rm", "-f", s.id)
	}
}

func (s *server) dsn(database string) string {
	return "postgres://postgres:" + password + "@" + s.addr + "/" + database + "?sslmode=disable"
}

// start starts the container, waits for the server, and creates the template database.
func start() (*server, error) {
	if stopped.Load() {
		return nil, errors.New("pgtest: Stop was called")
	}
	if err := docker("version", "--format", "{{.Server.Version}}"); err != nil {
		return nil, errNoDocker
	}
	removeOrphans()
	// The data directory is a tmpfs, so no volume outlives the container, whatever ends it.
	out, err := dockerOutput("run", "-d", "--rm", "--label", label+"="+strconv.Itoa(os.Getpid()),
		"-e", "POSTGRES_PASSWORD="+password, "-p", "127.0.0.1::5432", "--tmpfs", "/var/lib/postgresql/data", Image,
		// A test server: durability buys nothing, and costs every commit a flush.
		"-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off", "-c", "max_connections=500")
	if err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	s := &server{id: strings.TrimSpace(out)}
	fail := func(err error) (*server, error) {
		_ = docker("rm", "-f", s.id)
		return nil, err
	}
	port, err := dockerOutput("port", s.id, "5432/tcp")
	if err != nil {
		return fail(fmt.Errorf("postgres port: %w", err))
	}
	s.addr = strings.TrimSpace(strings.SplitN(port, "\n", 2)[0])
	// The DSNs DSN returns hold no password, as store.dsn may not: libpq's variable supplies it, to this process
	// and the commands it starts.
	if err := os.Setenv("PGPASSWORD", password); err != nil {
		return fail(err)
	}
	if s.admin, err = connect(s.dsn("postgres")); err != nil {
		return fail(err)
	}
	ctx := context.Background()
	if _, err := s.admin.Exec(ctx, `CREATE DATABASE `+template); err != nil {
		return fail(fmt.Errorf("create the template database: %w", err))
	}
	st, err := postgres.Open(ctx, s.dsn(template), postgres.Options{})
	if err != nil {
		return fail(fmt.Errorf("migrate the template database: %w", err))
	}
	if err := st.Close(); err != nil {
		return fail(err)
	}
	return s, nil
}

// connect waits for the server to accept connections: the image's entrypoint initialises the database with TCP
// closed, then starts the server for good.
func connect(dsn string) (*pgx.Conn, error) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		c, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			return c, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("postgres did not accept connections: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// removeOrphans removes the containers of test binaries that are gone.
func removeOrphans() {
	out, err := dockerOutput("ps", "-a", "--filter", "label="+label, "--format", `{{.ID}} {{.Label "`+label+`"}}`)
	if err != nil {
		return
	}
	for line := range strings.Lines(out) {
		id, pidText, _ := strings.Cut(strings.TrimSpace(line), " ")
		pid, err := strconv.Atoi(pidText)
		if id == "" || err != nil || pid == os.Getpid() {
			continue
		}
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			_ = docker("rm", "-f", id)
		}
	}
}

func docker(args ...string) error {
	_, err := dockerOutput(args...)
	return err
}

func dockerOutput(args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("docker", args...) // #nosec G204 -- fixed arguments, the test's own container
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}
