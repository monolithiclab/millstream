package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// A minimal AWS Signature Version 4 for S3 (the requests this store makes, and presigned GET and PUT URLs): the
// algorithm of the AWS documentation, "Authenticating Requests (AWS Signature Version 4)", checked against its
// examples in the tests.

const (
	algorithm       = "AWS4-HMAC-SHA256"
	amzDateLayout   = "20060102T150405Z"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	// emptySHA256 is the SHA-256 of an empty body.
	emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// signer signs requests for one region with one key pair.
type signer struct {
	accessKey, secretKey, region string
}

func (s signer) scope(t time.Time) string {
	return t.Format("20060102") + "/" + s.region + "/s3/aws4_request"
}

func (s signer) key(t time.Time) []byte {
	k := hmacSHA256([]byte("AWS4"+s.secretKey), t.Format("20060102"))
	k = hmacSHA256(k, s.region)
	k = hmacSHA256(k, "s3")
	return hmacSHA256(k, "aws4_request")
}

// sign adds the Authorization header to req, signing its host, every x-amz-* header and those named in extra, with
// payloadHash as its x-amz-content-sha256, at t.
func (s signer) sign(req *http.Request, payloadHash string, t time.Time) {
	t = t.UTC() // the date of the credential scope is UTC's, as the timestamp
	req.Header.Set("X-Amz-Date", t.UTC().Format(amzDateLayout))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	names := []string{"host"}
	for k := range req.Header {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, "x-amz-") || lk == "range" || lk == "content-type" {
			names = append(names, lk)
		}
	}
	slices.Sort(names)
	var canon strings.Builder
	for _, n := range names {
		v := req.Host
		if n != "host" {
			v = strings.TrimSpace(req.Header.Get(n))
		} else if v == "" {
			v = req.URL.Host
		}
		canon.WriteString(n + ":" + v + "\n")
	}
	signed := strings.Join(names, ";")
	cr := strings.Join([]string{req.Method, encodePath(req.URL.EscapedPath()), canonicalQuery(req.URL.Query()),
		canon.String(), signed, payloadHash}, "\n")
	sig := s.signature(cr, t)
	req.Header.Set("Authorization", algorithm+" Credential="+s.accessKey+"/"+s.scope(t)+", SignedHeaders="+signed+
		", Signature="+sig)
}

// presign returns u with the query that authorizes method on it, for expires from t, its host the only signed
// header and its payload unsigned.
func (s signer) presign(method string, u *url.URL, expires time.Duration, t time.Time) string {
	t = t.UTC()
	q := u.Query()
	q.Set("X-Amz-Algorithm", algorithm)
	q.Set("X-Amz-Credential", s.accessKey+"/"+s.scope(t))
	q.Set("X-Amz-Date", t.UTC().Format(amzDateLayout))
	q.Set("X-Amz-Expires", strconv.FormatInt(int64(expires/time.Second), 10))
	q.Set("X-Amz-SignedHeaders", "host")
	cr := strings.Join([]string{method, encodePath(u.EscapedPath()), canonicalQuery(q), "host:" + u.Host + "\n",
		"host", unsignedPayload}, "\n")
	out := *u
	out.RawQuery = canonicalQuery(q) + "&X-Amz-Signature=" + s.signature(cr, t)
	return out.String()
}

func (s signer) signature(canonicalRequest string, t time.Time) string {
	sum := sha256.Sum256([]byte(canonicalRequest))
	toSign := algorithm + "\n" + t.UTC().Format(amzDateLayout) + "\n" + s.scope(t) + "\n" + hex.EncodeToString(sum[:])
	return hex.EncodeToString(hmacSHA256(s.key(t), toSign))
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// canonicalQuery is the query sorted by name then value, each name and value URI-encoded.
func canonicalQuery(q url.Values) string {
	var parts []string
	for k, vs := range q {
		for _, v := range vs {
			parts = append(parts, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}
	slices.Sort(parts)
	return strings.Join(parts, "&")
}

// encodePath is an escaped path re-encoded the way SigV4 wants it for S3: each segment decoded, then URI-encoded
// once.
func encodePath(escaped string) string {
	if escaped == "" {
		return "/"
	}
	segs := strings.Split(escaped, "/")
	for i, s := range segs {
		if d, err := url.PathUnescape(s); err == nil {
			s = d
		}
		segs[i] = uriEncode(s, true)
	}
	return strings.Join(segs, "/")
}

// uriEncode encodes every byte but the unreserved characters (A-Z, a-z, 0-9, '-', '.', '_', '~'), and '/' unless
// slash is set.
func uriEncode(s string, slash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9', c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && !slash:
			b.WriteByte(c)
		default:
			b.WriteString("%" + strings.ToUpper(hex.EncodeToString([]byte{c})))
		}
	}
	return b.String()
}
