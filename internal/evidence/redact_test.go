package evidence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// TestRedactionStableAndUnreadable: under one key a value hashes alike wherever it is found, so bundles compare; under
// another key, or without one, the hash differs, and it never holds the value. A value is replaced at its pointer
// whatever its type, and elsewhere when it is a long enough string or a composite, in payloads, edits and text alike.
func TestRedactionStableAndUnreadable(t *testing.T) {
	key, other := []byte("deployment-key-0123456789abcdef!"), []byte("another-deployment-key-987654321")
	ptrs := []string{"/seller/phone", "/seller/address", "/n", "/a~1b", "/tags/1", "/tags/9", "/tags/x", "/n/x", "/no/x"}
	src := []byte(`{"seller":{"phone":"06 \"12\"","address":{"city":"Lyon","street":"1 rue X","lines":["Bat A"]}},` +
		`"n":7,"a/b":"ok","tags":["keep","secret"]}`)
	redacted := func(k []byte) (*redactor, string) {
		r := newRedactor(k)
		r.collect(ptrs, src)
		r.finish()
		out, err := r.Payload(ptrs, src)
		if err != nil {
			t.Fatal(err)
		}
		return r, string(out)
	}
	r, got := redacted(key)
	phone := r.hashValue("06 \"12\"")
	unkeyed := sha256.Sum256([]byte(`"06 \"12\""`))
	if !strings.HasPrefix(phone, HashPrefix) || len(phone) != len(HashPrefix)+32 ||
		strings.Contains(phone, hex.EncodeToString(unkeyed[:])[:32]) {
		t.Fatalf("hash %q: want the keyed hash, cut to 128 bits", phone)
	}
	if _, again := redacted(key); again != got {
		t.Errorf("same key: %s, then %s", got, again)
	}
	if _, theirs := redacted(other); theirs == got {
		t.Error("another key hashes alike")
	}
	for _, leak := range []string{"06", "Lyon", "rue X", "Bat A", `"n":7`, `"ok"`, "secret"} {
		if strings.Contains(got, leak) {
			t.Errorf("redacted payload %s holds %q", got, leak)
		}
	}
	for _, c := range []struct{ name, in, want string }{
		{"downstream copy", `{"contact":"06 \"12\"","short":"7","place":{"city":"Lyon","lines":["Bat A"],` +
			`"street":"1 rue X"}}`, fmt.Sprintf(`{"contact":%q,"place":%q,"short":"7"}`, phone,
			r.values[`{"city":"Lyon","lines":["Bat A"],"street":"1 rue X"}`])},
		{"string in an array", `[["Bat A"]]`, fmt.Sprintf(`[[%q]]`, r.hashValue("Bat A"))},
		{"inside a string", `{"note":"call 06 \"12\" now"}`, fmt.Sprintf(`{"note":"call %s now"}`, phone)},
		{"string in a composite", `{"street":"1 rue X"}`, fmt.Sprintf(`{"street":%q}`, r.hashValue("1 rue X"))},
	} {
		out, err := r.Payload(nil, []byte(c.in))
		if err != nil || string(out) != c.want {
			t.Errorf("%s: %s, %v; want %s", c.name, out, err, c.want)
		}
	}
	bare := newRedactor(key)
	bare.finish()
	if out, err := bare.Payload(nil, []byte(`{"n":1}`)); err != nil || string(out) != `{"n":1}` || bare.String("06") != "06" {
		t.Errorf("a redactor with nothing to redact changed %s", out)
	}
	for _, bad := range []func() error{
		func() error { _, err := r.Payload(ptrs, []byte(`{`)); return err },
		func() error { _, err := r.Patch(ptrs, []byte(`{}`)); return err },
	} {
		if bad() == nil {
			t.Error("redacted a document that is not JSON")
		}
	}
	if text := r.String(`{"text":"06 \"12\""}`); text != `{"text":"`+phone+`"}` {
		t.Errorf("JSON-escaped phone in a transcript: %s", text)
	}
	patch, err := r.Patch(ptrs, []byte(`[{"op":"replace","path":"/seller","value":{"phone":"01 23 45 67"}},`+
		`{"op":"add","path":"/n","value":8},{"op":"remove","path":"/x"}]`))
	want := fmt.Sprintf(`[{"op":"replace","path":"/seller","value":{"phone":%q}},{"op":"add","path":"/n","value":%q},`+
		`{"op":"remove","path":"/x"}]`, r.hashValue("01 23 45 67"), r.hashValue(8))
	if err != nil || string(patch) != want {
		t.Errorf("patch = %s, %v; want %s", patch, err, want)
	}
}

// TestStratify: each outcome gets an equal share, one with fewer handing the rest to the others, and a stratum's
// picks are spread evenly over the order its deliveries finished; deliveries still running, or of another step than
// the one named, are left out.
func TestStratify(t *testing.T) {
	var ds []store.Delivery
	add := func(n int, step string, state store.DeliveryState) {
		for i := range n {
			ds = append(ds, store.Delivery{ID: fmt.Sprintf("%s-%s-%02d", step, state, i), Step: step, State: state,
				FinishedAt: t0.Add(time.Duration(i) * time.Minute)})
		}
	}
	add(10, "score", store.DeliveryDone)
	add(2, "score", store.DeliveryDead)
	add(3, "score", store.DeliveryPending)
	add(5, "gate", store.DeliveryDone)
	ds = append(ds, store.Delivery{ID: "gate-reviewed", Step: "gate", State: store.DeliveryFiltered})
	reviews := map[string]store.Review{"gate-reviewed": {Decision: store.DecisionReject}}
	ids := func(s []Sampled) []string {
		var out []string
		for _, x := range s {
			out = append(out, x.Stratum+":"+x.Delivery)
		}
		return out
	}
	for _, c := range []struct {
		step string
		n    int
		want []string
	}{
		{"score", 6, []string{"done:score-done-01", "done:score-done-03", "done:score-done-06", "done:score-done-08",
			"dead:score-dead-00", "dead:score-dead-01"}},
		{"gate", 2, []string{"done:gate-done-02", "rejected:gate-reviewed"}},
		{"nowhere", 3, []string{"done:score-done-03", "dead:score-dead-01", "rejected:gate-reviewed"}},
	} {
		if got := ids(stratify(ds, reviews, c.step, c.n)); !slices.Equal(got, c.want) {
			t.Errorf("stratify(%s, %d) = %v, want %v", c.step, c.n, got, c.want)
		}
	}
}

// TestDecodeRefusals: another version, an archive not opening with its manifest, an entry the format lacks and an
// archive without evidence are not bundles this build reads.
func TestDecodeRefusals(t *testing.T) {
	archive := func(version string, entries ...string) []byte {
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.HuffmanOnly) // the other levels allocate a compressor of 600 kB
		tw := tar.NewWriter(zw)
		for _, name := range entries {
			body := []byte(`{"format":"millstream-evidence","version":` + version + `,"id":"ev","pipeline":"p"}`)
			_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg})
			_, _ = tw.Write(body)
		}
		_ = tw.Close()
		_ = zw.Close()
		return buf.Bytes()
	}
	for name, data := range map[string][]byte{
		"newer version":    archive("2", manifestEntry, evidenceEntry),
		"evidence first":   archive("1", evidenceEntry, manifestEntry),
		"unexpected entry": archive("1", manifestEntry, "notes.txt"),
		"no evidence":      archive("1", manifestEntry),
		"not gzip":         []byte("plain"),
	} {
		if _, err := Decode(data); !errors.Is(err, ErrFormat) {
			t.Errorf("%s: %v, want ErrFormat", name, err)
		}
	}
}
