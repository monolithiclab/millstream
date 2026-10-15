package pipeline

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{in: "10s", want: 10 * time.Second},
		{in: "1h30m", want: 90 * time.Minute},
		{in: "90d", want: 90 * 24 * time.Hour},
		{in: "0s", wantErr: true},
		{in: "-1d", wantErr: true},
		{in: "xd", wantErr: true},
		{in: "999999999999d", wantErr: true},
		{in: "soon", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, err := ParseDuration(tt.in)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("ParseDuration(%q) = %v, %v; want %v, error %v", tt.in, got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestParseRate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		count   int
		per     time.Duration
		wantErr bool
	}{
		{in: "10/min", count: 10, per: time.Minute},
		{in: "1/s", count: 1, per: time.Second},
		{in: "5/hour", count: 5, per: time.Hour},
		{in: "100/day", count: 100, per: 24 * time.Hour},
		{in: "10", wantErr: true},
		{in: "0/min", wantErr: true},
		{in: "x/min", wantErr: true},
		{in: "10/week", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			count, per, err := ParseRate(tt.in)
			if (err != nil) != tt.wantErr || count != tt.count || per != tt.per {
				t.Errorf("ParseRate(%q) = %d, %v, %v", tt.in, count, per, err)
			}
		})
	}
}
