package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/nodeapi"
	"github.com/monolithiclab/millstream/internal/nodewire"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// nodes revoke records the id revoked on the store; nodes list shows it; node refuses what it cannot join with.
func Test_nodesCommands(t *testing.T) {
	t.Parallel()
	s := newEmptyState(t)
	if out, _ := s.must(0, "nodes", "revoke", "gpu-1"); out != "gpu-1 revoked\n" {
		t.Fatalf("revoke: %q", out)
	}
	out, _ := s.must(0, "nodes")
	if !regexp.MustCompile(`(?m)^ID\s+STATE\s+HEALTH.*\ngpu-1\s+revoked\s+lost\s+-\s+0\s+0\s+-\s+-\s+-\s*$`).MatchString(out) {
		t.Fatalf("nodes:\n%s", out)
	}
	dir := testdir.New(t)
	if _, errOut := s.must(2, "node", "--join", "https://engine:7070", "--state-dir", dir); !strings.Contains(errOut,
		"set MILLSTREAM_NODE_TOKEN") {
		t.Fatalf("node without a token: %q", errOut)
	}
	if _, errOut := s.must(1, "nodes", "drain", "--state-dir", dir); !strings.Contains(errOut, "no node runs from") {
		t.Fatalf("drain without a node: %q", errOut)
	}
	s.checkRefusals(t, map[string]string{NodeTokenEnv: "short"}, []refusal{
		{NodeCmd{Join: []string{"https://e:7070"}, Slots: 1, DrainTimeout: 1, StateDir: dir}, true,
			NodeTokenEnv + ": must be at least 32 characters long, not 5"},
	})
	s.checkRefusals(t, map[string]string{NodeTokenEnv: testToken}, []refusal{
		{NodeCmd{Join: []string{"http://engine:7070"}, Slots: 1, DrainTimeout: 1, StateDir: dir}, true,
			"over TLS only"},
		{NodeCmd{Join: []string{"https://e:7070"}, Slots: 0, DrainTimeout: 1, StateDir: dir}, true,
			"--slots and --drain-timeout must be positive"},
		{NodeCmd{Join: []string{"https://e:7070"}, Slots: 1, DrainTimeout: 1, StateDir: dir,
			CA: filepath.Join(dir, "missing.pem")}, false, "--ca"},
		{NodeCmd{Join: []string{"https://e:7070"}, Slots: 1, DrainTimeout: 1, StateDir: dir,
			Labels: map[string]string{"": "x"}}, true, "--labels"},
	})
}

// node joins an engine in memory, pulls, and on interruption drains, leaves and exits; its id and pid file live
// in the state directory, the id kept across runs.
func Test_nodeJoinsAndDrains(t *testing.T) {
	st := memoryStore(t)
	eng, err := engine.New(engine.Options{Store: st, LeaseTTL: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h, err := nodeapi.New(nodeapi.Options{Engine: eng, Token: testToken, Version: "dev"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pulled := sync.OnceFunc(cancel)
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == nodewire.PathPull {
			pulled() // interrupted as it pulls
		}
		h.ServeHTTP(w, r)
	})
	dir := testdir.New(t)
	var out, errb bytes.Buffer
	code := Run(ctx, []string{"--db", dsn(unusedDB()), "node", "--join", "https://engine:7070", "--state-dir", dir,
		"--labels", "gpu=true", "--slots", "2"}, IO{In: strings.NewReader(""), Out: &out, Err: &errb}, "dev",
		func(rt *Runtime) {
			rt.Env = func(k string) (string, bool) { v, ok := map[string]string{NodeTokenEnv: testToken}[k]; return v, ok }
			rt.HTTPClient = &http.Client{Transport: handlerTransport{hf}}
			rt.Launchers = testLaunchers()
		})
	if code != 0 || !strings.Contains(errb.String(), `"msg":"node joined"`) ||
		!strings.Contains(errb.String(), `"msg":"node drained"`) {
		t.Fatalf("node: %d\n%s", code, errb.String())
	}
	ns, err := st.Nodes(t.Context())
	if err != nil || len(ns) != 1 || ns[0].Labels["gpu"] != "true" || ns[0].Slots != 2 ||
		ns[0].State != store.NodeDraining {
		t.Fatalf("nodes = %+v, %v; want it registered, then draining once it left", ns, err)
	}
	id, _ := nodeID(dir)
	if id != ns[0].ID {
		t.Fatalf("id file %q, registered %q", id, ns[0].ID)
	}
	if _, ok := runningNode(dir); ok {
		t.Fatal("the pid file outlived the node")
	}
}

// An s3:// artifacts.url builds the S3 store from the AWS variables; node.token is read from
// its source, and refused, as check refuses it, when too short.
func Test_artifactStoreAndNodeToken(t *testing.T) {
	parse := func(src string) *pipeline.Environment {
		env, err := pipeline.ParseEnvironment([]byte(src))
		if err != nil {
			t.Fatal(err)
		}
		return env
	}
	rt := func(vars map[string]string) *Runtime {
		return &Runtime{DB: dsn(unusedDB()), Env: func(k string) (string, bool) { v, ok := vars[k]; return v, ok }}
	}
	s3env := parse("artifacts: { url: s3://b/pre }\n")
	for _, vars := range []map[string]string{nil, {"AWS_ACCESS_KEY_ID": "k"}, {"AWS_SECRET_ACCESS_KEY": "s"}} {
		if _, err := artifactStore(rt(vars), s3env); err == nil || !strings.Contains(err.Error(), "AWS_ACCESS_KEY_ID") {
			t.Errorf("an S3 store with %v: %v", vars, err)
		}
	}
	if a, err := artifactStore(rt(map[string]string{"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s",
		"AWS_ENDPOINT_URL": "http://s3.test"}), s3env); err != nil || fmt.Sprintf("%T", a) != "*s3.Store" {
		t.Errorf("S3 store from the AWS variables = %T, %v", a, err)
	}
	if _, err := artifactStore(rt(map[string]string{"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s",
		"AWS_ENDPOINT_URL": "s3.test"}), s3env); err == nil || !strings.Contains(err.Error(), "AWS_ENDPOINT_URL") {
		t.Errorf("an endpoint without a scheme: %v", err)
	}
	if a, err := artifactStore(rt(nil), parse("")); err != nil || fmt.Sprintf("%T", a) != "*artifacts.Dir" {
		t.Errorf("default store = %T, %v", a, err)
	}
	tokenEnv := parse("node: { token: { env: NT } }\n")
	if tok, err := readNodeToken(rt(map[string]string{"NT": testToken}), tokenEnv); err != nil || tok != testToken {
		t.Errorf("node token = %q, %v", tok, err)
	}
	if _, err := readNodeToken(rt(map[string]string{"NT": "x"}), tokenEnv); err == nil ||
		err.Error() != "node.token: must be at least 32 characters long, not 1" {
		t.Errorf("a short node token: %v", err)
	}
	if _, err := readNodeToken(rt(nil), tokenEnv); err == nil {
		t.Error("an unset node token")
	}
	if tok, err := readNodeToken(rt(nil), parse("")); err != nil || tok != "" {
		t.Errorf("no node.token = %q, %v", tok, err)
	}
	// check refuses a short token it can read, and skips one it cannot.
	both := parse("auth: { token: { env: AT } }\nnode: { token: { env: NT } }\n")
	if err := checkTokens(rt(map[string]string{"AT": testToken}), both); err != nil {
		t.Errorf("check with node.token unset: %v", err)
	}
	if err := checkTokens(rt(map[string]string{"AT": testToken, "NT": "x"}), both); err == nil ||
		!strings.HasPrefix(err.Error(), "node.token: must be at least 32") {
		t.Errorf("check with a short node.token: %v", err)
	}
}

// A pid file no process holds is stale whatever pid it names, this one's included: a node killed in a container
// left it, and the container started again runs the new node under the same pid, which must start.
func Test_lockStateDirOwnPid(t *testing.T) {
	dir := testdir.New(t)
	if err := os.WriteFile(filepath.Join(dir, nodePIDFile), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	release, err := lockStateDir(dir)
	if err != nil {
		t.Fatalf("a node refused its own stale pid file: %v", err)
	}
	release()
}

// nodes drain signals the node running from the state directory and waits until it is gone; a second one refuses
// to start meanwhile.
func Test_nodesDrain(t *testing.T) {
	dir := testdir.New(t)
	release, err := lockStateDir(dir) // this test process stands for the node
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockStateDir(dir); err == nil {
		t.Fatal("a second node started from the same state directory")
	}
	saved := signalProcess
	defer func() { signalProcess = saved }()
	var sent []os.Signal
	signalProcess = func(pid int, sig os.Signal) error {
		if sig == syscall.SIGTERM {
			sent = append(sent, sig)
			release() // the node drains and exits
			return nil
		}
		return saved(pid, sig)
	}
	s := newEmptyState(t)
	if out, _ := s.must(0, "nodes", "drain", "--state-dir", dir); !strings.Contains(out, "drained") || len(sent) != 1 {
		t.Fatalf("drain: %q, signals %v", out, sent)
	}
}
