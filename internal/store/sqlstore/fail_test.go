package sqlstore

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

func Test_truncate(t *testing.T) {
	tests := []struct {
		in   string
		n    int
		want string
	}{
		{"short", 10, "short"},
		{"abcdef", 3, "abc"},
		{"aé", 2, "a"}, // é is two bytes: never split
		{"éé", 3, "é"},
	}
	for _, tt := range tests {
		if got := truncate(tt.in, tt.n); got != tt.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.in, tt.n, got, tt.want)
		}
	}
}

func Test_backoff(t *testing.T) {
	r := pipeline.Retry{MaxAttempts: 10, Backoff: 10 * time.Second, MaxBackoff: time.Minute}
	tests := []struct {
		attempt int
		jitter  float64
		want    time.Duration
	}{
		{1, 1, 10 * time.Second},
		{2, 1, 20 * time.Second},
		{3, 1, 40 * time.Second},
		{4, 1, time.Minute}, // capped
		{60, 1, time.Minute},
		{3, 0.25, 10 * time.Second},
		{1, 0, 0},
	}
	for _, tt := range tests {
		if got := backoff(r, tt.attempt, tt.jitter); got != tt.want {
			t.Errorf("backoff(attempt %d, jitter %v) = %v, want %v", tt.attempt, tt.jitter, got, tt.want)
		}
	}
}
