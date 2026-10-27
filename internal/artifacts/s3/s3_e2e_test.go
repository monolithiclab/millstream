//go:build e2e

package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
)

// The store against a real S3-compatible server, so that the signatures are checked by someone else than their
// author. MinIO stopped publishing its server images in 2025 (the Docker Hub repository is gone, quay.io's needs an
// account), so the server is SeaweedFS's S3 gateway, with an identity whose keys it checks: chrislusf/seaweedfs,
// pinned by digest (resolved 2026-10-17), pulled through mirror.gcr.io. Refresh: docker buildx imagetools inspect
// mirror.gcr.io/chrislusf/seaweedfs:latest.
const s3Image = "mirror.gcr.io/chrislusf/seaweedfs@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d"

const (
	testAccessKey = "millstream-test"
	testSecretKey = "millstream-test-secret"
	testBucket    = "millstream-artifacts"
)

// startS3 starts the server in a container removed when the test ends, and returns its endpoint. Without Docker
// the test skips.
func startS3(t *testing.T) string {
	t.Helper()
	if err := exec.Command("docker", "version").Run(); err != nil {
		t.Skip("S3 tests skipped: Docker is unavailable")
	}
	// The image's entrypoint drops to its seaweed user, which cannot enter TempDir's 0700 on Linux.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil { // #nosec G302 -- read by the container's user
		t.Fatal(err)
	}
	config := `{"identities":[{"name":"test","credentials":[{"accessKey":"` + testAccessKey + `","secretKey":"` +
		testSecretKey + `"}],"actions":["Admin","Read","Write","List","Tagging"]}]}`
	if err := os.WriteFile(filepath.Join(dir, "s3.json"), []byte(config), 0o644); err != nil { // #nosec G306 -- read by the container's user
		t.Fatal(err)
	}
	// No --rm: a server that exits at once keeps its logs for the failure message below.
	out, err := exec.Command("docker", "run", "-d", "-p", "127.0.0.1::8333", "--tmpfs", "/data", // #nosec G204 -- the test's own container
		"-v", dir+":/etc/seaweedfs-test:ro", s3Image, "server", "-dir=/data", "-s3", "-s3.port=8333",
		"-s3.config=/etc/seaweedfs-test/s3.json").CombinedOutput()
	if err != nil {
		t.Fatalf("start the S3 server: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() }) // #nosec G204 -- the test's own container
	port, err := exec.Command("docker", "port", id, "8333/tcp").Output()   // #nosec G204 -- the test's own container
	if err != nil {
		logs, _ := exec.Command("docker", "logs", "--tail", "20", id).CombinedOutput() // #nosec G204 -- the test's own container
		t.Fatalf("the S3 server is not listening: %v: %s", err, logs)
	}
	return "http://" + strings.TrimSpace(strings.SplitN(string(port), "\n", 2)[0])
}

func TestAgainstS3Server(t *testing.T) {
	endpoint := startS3(t)
	ctx := context.Background()
	st, err := New(Config{Bucket: testBucket, Endpoint: endpoint, AccessKeyID: testAccessKey,
		SecretAccessKey: testSecretKey, Prefix: "deploy", TmpDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	// Create the bucket, signed, waiting for the server to start.
	deadline := time.Now().Add(time.Minute)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, endpoint+"/"+testBucket, nil)
		st.sign.sign(req, emptySHA256, time.Now())
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
			err = errors.New(resp.Status + ": " + string(body))
		}
		if time.Now().After(deadline) {
			t.Fatalf("create the bucket: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	content := bytes.Repeat([]byte("listing 42 "), 100)
	ref, err := st.Put(ctx, "listings", "page.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := st.Exists(ctx, "listings", ref.SHA256); err != nil || !ok {
		t.Fatalf("Exists = %v, %v", ok, err)
	}
	if ok, err := st.Exists(ctx, "other", ref.SHA256); err != nil || ok {
		t.Fatalf("Exists in another pipeline = %v, %v", ok, err)
	}
	r, err := st.Open(ctx, "listings", ref.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seek(500, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, content[500:]) {
		t.Fatalf("ranged read: %d bytes, %v", len(got), err)
	}
	_ = r.Close()
	if _, err := st.Open(ctx, "listings", strings.Repeat("0", 64)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open a missing object: %v", err)
	}
	if err := st.Copy(ctx, "listings", "deals", ref.SHA256); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.Exists(ctx, "deals", ref.SHA256); !ok {
		t.Fatal("the copy is missing")
	}

	// Presigned URLs, as a remote node uses them: no credentials of its own.
	get, err := st.PresignGet("listings", ref.SHA256, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(get) // #nosec G107 -- the test server's URL
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, content) {
		t.Fatalf("presigned GET: %s, %d bytes", resp.Status, len(got))
	}
	// A presigned PUT writes the content it was issued for only: another body of the same length, or another
	// length, is refused.
	output := []byte("an output")
	sum := sha256.Sum256(output)
	sha := hex.EncodeToString(sum[:])
	put, err := st.PresignPut("outputs", sha, int64(len(output)), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	presignedPut := func(body []byte) int {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, put, bytes.NewReader(body))
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set(artifacts.ChecksumHeader, artifacts.ChecksumSHA256(sha))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := presignedPut([]byte("an outpuT")); code < 400 {
		t.Fatalf("presigned PUT of another body of the same length: %d", code)
	}
	if code := presignedPut([]byte("an output, longer")); code < 400 {
		t.Fatalf("presigned PUT of another length: %d", code)
	}
	if ok, _ := st.Exists(ctx, "outputs", sha); ok {
		t.Fatal("a refused presigned PUT stored something")
	}
	if code := presignedPut(output); code != http.StatusOK {
		t.Fatalf("presigned PUT: %d", code)
	}
	if ok, _ := st.Exists(ctx, "outputs", sha); !ok {
		t.Fatal("the presigned PUT stored nothing")
	}
	// The artifact sweep's operations: the listing, Last-Modified, a touch (Put of a stored content too), deletes.
	files, err := st.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	for _, f := range files {
		listed[f.Pipeline+"/"+f.Name] = !f.ModTime.IsZero()
	}
	if len(files) != 3 || !listed["listings/"+ref.SHA256] || !listed["deals/"+ref.SHA256] || !listed["outputs/"+sha] {
		t.Fatalf("List = %+v", files)
	}
	before, err := st.ModTime(ctx, "listings", ref.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond) // Last-Modified counts seconds
	if _, err := st.Put(ctx, "listings", "again.txt", bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if after, err := st.ModTime(ctx, "listings", ref.SHA256); err != nil || !after.After(before) {
		t.Fatalf("ModTime after storing again = %v (before %v), %v", after, before, err)
	}
	if got, _ := st.Open(ctx, "listings", ref.SHA256); got == nil {
		t.Fatal("the touched object is gone")
	} else if b, err := io.ReadAll(got); err != nil || !bytes.Equal(b, content) {
		t.Fatalf("the touched object reads %d bytes, %v", len(b), err)
	}
	for range 2 { // the second time, already gone
		if err := st.Remove(ctx, "deals", ref.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Touch(ctx, "deals", ref.SHA256); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Touch of a removed object = %v", err)
	}

	// A tampered URL, and a wrong key, are refused by the server.
	resp, err = http.Get(strings.Replace(get, "listings", "deals", 1)) // #nosec G107 -- the test server's URL
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("tampered presigned GET: %s", resp.Status)
	}
	wrong, _ := New(Config{Bucket: testBucket, Endpoint: endpoint, AccessKeyID: testAccessKey,
		SecretAccessKey: "wrong", Prefix: "deploy"})
	if _, err := wrong.Exists(ctx, "listings", ref.SHA256); err == nil {
		t.Fatal("a wrong secret key was accepted")
	}
}
