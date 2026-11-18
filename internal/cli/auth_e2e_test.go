//go:build e2e

package cli

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// serve with auth.token and tls: the API answers over HTTPS only with the token, as a bearer token or the session
// cookie signing in sets, which the live streams accept too; /metrics takes the token; the CLI reaches it with
// MILLSTREAM_TOKEN; signing out ends the session.
func Test_serveWithTokenAndTLS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	pool := selfSigned(t, dir)
	envFile := filepath.Join(dir, "millstream.yaml")
	if err := os.WriteFile(envFile, []byte("store: { dsn: sqlite://state/millstream.db }\nlisten: 127.0.0.1:0\n"+
		"auth: { token: { env: MILLSTREAM_TOKEN }, user: ana }\ntls: { cert: cert.pem, key: key.pem }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	environ := map[string]string{"MILLSTREAM_TOKEN": testToken} // no $USER: the reviewer is auth.user
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	serving := regexp.MustCompile(`serving the UI and the API on https://(127\.0\.0\.1:\d+)/`)
	out := newWatchBuffer(func(s string) bool { return serving.MatchString(s) })
	errOut := newWatchBuffer(func(string) bool { return false })
	serveCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan int, 1)
	go func() {
		done <- Run(serveCtx, []string{"--env-file", envFile, "serve"}, IO{In: strings.NewReader(""), Out: out,
			Err: errOut}, "dev", func(rt *Runtime) {
			rt.Env = func(k string) (string, bool) { v, ok := environ[k]; return v, ok }
			rt.Launchers = testLaunchers()
		})
	}()
	select {
	case <-out.Done:
	case code := <-done:
		t.Fatalf("serve exited with %d before serving:\n%s", code, errOut.String())
	}
	base := "https://" + serving.FindStringSubmatch(out.String())[1]
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool,
		MinVersion: tls.VersionTLS12}}, Timeout: 10 * time.Second}
	call := func(method, path, body string, header ...string) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		for i := 0; i+1 < len(header); i += 2 {
			req.Header.Set(header[i], header[i+1])
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}
	if resp := call(http.MethodGet, "/api/v1/pipelines", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("without the token: %d", resp.StatusCode)
	}
	if resp := call(http.MethodGet, "/api/v1/pipelines", "", "Authorization", "Bearer "+testToken); resp.StatusCode != 200 {
		t.Fatalf("bearer: %d", resp.StatusCode)
	}
	resp := call(http.MethodGet, "/metrics", "", "Authorization", "Bearer "+testToken)
	if b, _ := io.ReadAll(resp.Body); resp.StatusCode != 200 || !bytes.Contains(b, []byte("millstream_writer_batch_size")) {
		t.Fatalf("metrics: %d %s", resp.StatusCode, b)
	}

	// The CLI sends MILLSTREAM_TOKEN; it trusts the certificate through SSL_CERT_FILE in real use, the test's
	// client here. The package is the fixture with its stub action, not the built fake action runPackage places:
	// nothing runs, and uploading a 4 MB binary under -race outlasts the client's timeout on a CI runner.
	var cliOut, cliErr bytes.Buffer
	code := Run(ctx, []string{"apply", fixture("run")}, IO{In: strings.NewReader(""), Out: &cliOut, Err: &cliErr}, "dev",
		func(rt *Runtime) {
			env := map[string]string{ServerEnv: base, TokenEnv: testToken}
			rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
			rt.HTTPClient = client
		})
	if code != 0 || !strings.HasPrefix(cliOut.String(), "listings revision 1 ") {
		t.Fatalf("apply --server: %d %q %q", code, cliOut.String(), cliErr.String())
	}

	// The UI's session: the cookie reaches the API and the live streams; signing out ends it.
	resp = call(http.MethodPost, "/api/v1/session", `{"token":"`+testToken+`"}`, "Origin", base)
	cookies := resp.Cookies()
	if resp.StatusCode != 200 || len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly {
		t.Fatalf("sign in: %d %v", resp.StatusCode, cookies)
	}
	cookie := cookies[0].Name + "=" + cookies[0].Value
	resp = call(http.MethodGet, "/api/v1/session", "", "Cookie", cookie)
	if b, _ := io.ReadAll(resp.Body); resp.StatusCode != 200 || !bytes.Contains(b, []byte(`"user":"ana"`)) {
		t.Fatalf("session: %d %s", resp.StatusCode, b)
	}
	resp = call(http.MethodGet, "/api/v1/live/pipelines/listings", "", "Cookie", cookie)
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("live stream on the cookie: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	_ = resp.Body.Close()
	if resp := call(http.MethodPost, "/api/v1/pipelines/listings/pause", "{}", "Cookie", cookie); resp.StatusCode !=
		http.StatusForbidden {
		t.Fatalf("a POST on the cookie without an origin: %d", resp.StatusCode)
	}
	if resp := call(http.MethodDelete, "/api/v1/session", "", "Cookie", cookie, "Origin", base); resp.StatusCode != 204 {
		t.Fatalf("sign out: %d", resp.StatusCode)
	}
	if resp := call(http.MethodGet, "/api/v1/pipelines", "", "Cookie", cookie); resp.StatusCode != 401 {
		t.Fatalf("after signing out: %d", resp.StatusCode)
	}
	stop()
	if code := <-done; code != 0 {
		t.Fatalf("serve exited with %d:\n%s", code, errOut.String())
	}
}

// selfSigned writes a certificate for 127.0.0.1 and its key as cert.pem and key.pem in dir, and returns a pool
// trusting it.
func selfSigned(t *testing.T, dir string) *x509.CertPool {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "millstream test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	for name, b := range map[string][]byte{
		"cert.pem": certPEM,
		"key.pem":  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return pool
}
