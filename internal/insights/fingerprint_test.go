package insights

import (
	"strings"
	"testing"
)

func TestNormalise(t *testing.T) {
	t.Parallel()
	tests := []struct{ in, want string }{
		{"timeout after 30s", "timeout after <n>s"},
		{"retry 3 of 5 in 1.5 seconds", "retry <n> of <n> in <n> seconds"},
		{"GET https://api.example.test/v1/items/42?x=1 failed", "GET <url> failed"},
		{"open /var/lib/data/run-7/page.json: no such file", "open <path>: no such file"},
		{"read C:/data/x.txt", "read <path>"},
		{"delivery dl_0a1b2c3d failed for it_ff00aa11", "delivery <id> failed for <id>"},
		{"run run_20261015T0600Z_daily cancelled", "run <id> cancelled"},
		{"request 123e4567-e89b-12d3-a456-426614174000 refused", "request <id> refused"},
		{"digest deadbeef0123 mismatch", "digest <hex> mismatch"},
		{"code abc12 and cafe", "code abc<n> and cafe"},
		{"status 503", "status <n>"},
		{"  spaced \n\t out  ", "spaced out"},
		{"the lease held by embedded-4242 expired before it reported",
			"the lease held by embedded-<n> expired before it reported"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := Normalise(tt.in); got != tt.want {
			t.Errorf("Normalise(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
	long := Normalise(strings.Repeat("é", 150))
	if len(long) > maxNormalised {
		t.Errorf("normalised message of %d bytes", len(long))
	}
	if fingerprint("k", "p", "a", "b") == fingerprint("k", "p", "ab") {
		t.Error("fingerprint parts are ambiguous")
	}
}
