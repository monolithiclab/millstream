//go:build e2e

package e2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeCA writes a certificate authority and a certificate it signs for 127.0.0.1, localhost and the Compose engines
// (engine, engine-standby) into dir, valid for 30 days and readable by the soak's containers, and returns the paths
// of the CA's certificate, the certificate and its key.
func writeCA(t *testing.T, dir string) (caPath, certPath, keyPath string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "millstream test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(0, 0, 30), IsCA: true,
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "engine"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(0, 0, 30),
		DNSNames:    []string{"engine", "engine-standby", "localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage: x509.KeyUsageDigitalSignature}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, kind string, der []byte) string {
		p := filepath.Join(dir, name)
		// #nosec G306 -- a test's own key, read by the soak's containers under another uid
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	return write("ca.pem", "CERTIFICATE", caDER), write("cert.pem", "CERTIFICATE", leafDER),
		write("key.pem", "EC PRIVATE KEY", keyDER)
}

// TestNodeOverTLS: an engine serving HTTPS with tls takes a node that trusts its CA through --ca, over HTTP/2.
func TestNodeOverTLS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caPath, certPath, keyPath := writeCA(t, dir)
	f := &failover{t: t, db: dbPath(t), dir: dir, envFile: filepath.Join(dir, "millstream.yaml")}
	if err := os.WriteFile(f.envFile, []byte("store: { sqlite: { path: "+f.db+" } }\nlisten: 127.0.0.1:0\n"+
		"tls: { cert: "+certPath+", key: "+keyPath+" }\n"+
		"node: { embedded: false, token: { env: MILLSTREAM_NODE_TOKEN } }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	a := f.startEngine("a", f.db, withNodeToken)
	if !strings.HasPrefix(a.url, "https://") {
		t.Fatalf("engine URL %s", a.url)
	}
	n := f.startNodeCA("1", caPath, a.url)
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2: true}
	resp, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Get(a.url + "/node/v1/package")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("node endpoint over TLS: %s %d, want HTTP/2 and 401 without the token", resp.Proto, resp.StatusCode)
	}
	if r := f.client(a.url, "nodes"); !strings.Contains(r.stdout, n.id) {
		t.Fatalf("nodes: %s", r)
	}
	a.stop(t)
}
