package s3

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The examples of the AWS documentation, "Authenticating Requests: Using the Authorization Header" and "Using Query
// Parameters", with their published signatures.
var (
	example     = signer{accessKey: "AKIAIOSFODNN7EXAMPLE", secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", region: "us-east-1"}
	exampleTime = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
)

func TestSignHeaderExamples(t *testing.T) {
	tests := []struct {
		name, target string
		header       http.Header
		want         string
	}{
		{"GET object", "https://examplebucket.s3.amazonaws.com/test.txt", http.Header{"Range": {"bytes=0-9"}},
			"Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
				"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
				"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"},
		{"GET bucket lifecycle", "https://examplebucket.s3.amazonaws.com/?lifecycle", nil,
			"SignedHeaders=host;x-amz-content-sha256;x-amz-date, " +
				"Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"},
		{"list objects", "https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J", nil,
			"SignedHeaders=host;x-amz-content-sha256;x-amz-date, " +
				"Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"},
	}
	for _, tt := range tests {
		req, err := http.NewRequest(http.MethodGet, tt.target, nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range tt.header {
			req.Header[k] = v
		}
		example.sign(req, emptySHA256, exampleTime)
		if got := req.Header.Get("Authorization"); !strings.HasPrefix(got, algorithm+" ") || !strings.Contains(got, tt.want) {
			t.Errorf("%s: Authorization = %q, want it to hold %q", tt.name, got, tt.want)
		}
	}
}

func TestPresignExample(t *testing.T) {
	u, _ := url.Parse("https://examplebucket.s3.amazonaws.com/test.txt")
	got := example.presign(http.MethodGet, u, nil, 24*time.Hour, exampleTime)
	want := "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z" +
		"&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
		"&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	if got != want {
		t.Fatalf("presigned URL\n got %s\nwant %s", got, want)
	}
}

// A presigned PUT signs the headers it is given beside the host: the canonical request is the documented one, its
// headers sorted by name, and a request sending another value of one of them no longer matches the signature.
func TestPresignSignedHeaders(t *testing.T) {
	u, _ := url.Parse("https://examplebucket.s3.amazonaws.com/test.txt")
	header := map[string]string{"x-amz-checksum-sha256": "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
		"content-length": "0"}
	got, _ := url.Parse(example.presign(http.MethodPut, u, header, time.Hour, exampleTime))
	q := got.Query()
	if q.Get("X-Amz-SignedHeaders") != "content-length;host;x-amz-checksum-sha256" {
		t.Fatalf("X-Amz-SignedHeaders = %q", q.Get("X-Amz-SignedHeaders"))
	}
	cr := "PUT\n/test.txt\nX-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z" +
		"&X-Amz-Expires=3600&X-Amz-SignedHeaders=content-length%3Bhost%3Bx-amz-checksum-sha256\n" +
		"content-length:0\nhost:examplebucket.s3.amazonaws.com\n" +
		"x-amz-checksum-sha256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\n\n" +
		"content-length;host;x-amz-checksum-sha256\nUNSIGNED-PAYLOAD"
	if want := example.signature(cr, exampleTime); q.Get("X-Amz-Signature") != want {
		t.Fatalf("X-Amz-Signature = %s, want %s", q.Get("X-Amz-Signature"), want)
	}
	header["content-length"] = "1"
	if other, _ := url.Parse(example.presign(http.MethodPut, u, header, time.Hour, exampleTime)); other.Query().Get(
		"X-Amz-Signature") == q.Get("X-Amz-Signature") {
		t.Fatal("the signature does not cover the signed headers' values")
	}
}

func TestURIEncode(t *testing.T) {
	if got := uriEncode("a b/c~d$é", false); got != "a%20b/c~d%24%C3%A9" {
		t.Errorf("uriEncode = %q", got)
	}
	if got := encodePath("/bucket/pre%20fix/x"); got != "/bucket/pre%20fix/x" {
		t.Errorf("encodePath = %q", got)
	}
}

// The credential scope's date is UTC's whatever the clock's zone: just after midnight in Paris is the day before.
func TestSignInAnotherZone(t *testing.T) {
	u, _ := url.Parse("https://examplebucket.s3.amazonaws.com/test.txt")
	paris := exampleTime.In(time.FixedZone("CEST", 2*3600))
	if example.presign(http.MethodGet, u, nil, 24*time.Hour, paris) != example.presign(http.MethodGet, u, nil, 24*time.Hour, exampleTime) {
		t.Fatal("the signature depends on the clock's zone")
	}
}
