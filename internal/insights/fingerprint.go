package insights

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

// normalisers replace, in order, what varies between occurrences of one error: URLs, paths, UUIDs, prefixed ids
// with a digit (it_…, dl_…, run_…), hex strings of 8 characters or more with a digit, then numbers (spec section
// 11.1).
var normalisers = []struct {
	re   *regexp.Regexp
	with string
}{
	{regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://\S+`), "<url>"},
	{regexp.MustCompile(`(?:[A-Za-z]:)?(?:[\w.~-]*/)+[\w.~-]*`), "<path>"},
	{regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`), "<id>"},
	{regexp.MustCompile(`\b[a-z]+_[\w-]*\d[\w-]*\b`), "<id>"},
	{regexp.MustCompile(`(?i)\b[0-9a-f]*\d[0-9a-f]*\b`), ""}, // hex or a number: see hexOrNumber
	{regexp.MustCompile(`\d+(?:\.\d+)?`), "<n>"},
}

// hexOrNumber is the replacement of a word of hex digits holding a digit: <hex> for 8 characters or more with a
// letter, else the word, for the number rule.
func hexOrNumber(w string) string {
	if len(w) >= 8 && strings.ContainsAny(strings.ToLower(w), "abcdef") {
		return "<hex>"
	}
	return w
}

var spaces = regexp.MustCompile(`\s+`)

// maxNormalised is the most bytes of a normalised message kept: the start of a message identifies its kind.
const maxNormalised = 200

// Normalise reduces an error message to what identifies its kind, so the occurrences of one error share a
// fingerprint: URLs, paths, ids, hex strings and numbers are replaced by placeholders, runs of white space by one
// space, and the result is cut to 200 bytes.
func Normalise(msg string) string {
	for _, n := range normalisers {
		if n.with == "" {
			msg = n.re.ReplaceAllStringFunc(msg, hexOrNumber)
		} else {
			msg = n.re.ReplaceAllString(msg, n.with)
		}
	}
	msg = strings.TrimSpace(spaces.ReplaceAllString(msg, " "))
	if len(msg) > maxNormalised {
		msg = strings.ToValidUTF8(msg[:maxNormalised], "")
	}
	return msg
}

// fingerprint is an insight's id: a hash of its kind, pipeline and what makes it distinct, so a recurring problem
// lands on the same row.
func fingerprint(kind, pipelineName string, parts ...string) string {
	h := sha256.New()
	for _, p := range append([]string{kind, pipelineName}, parts...) {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return "in_" + hex.EncodeToString(h.Sum(nil)[:8])
}
