package pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseDuration parses a positive duration: Go's syntax (10s, 1h30m) plus whole days (90d), which retention and
// review timeouts use.
func ParseDuration(s string) (time.Duration, error) {
	var d time.Duration
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseInt(days, 10, 64)
		if err != nil || n > int64(1<<63-1)/int64(24*time.Hour) {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		d = time.Duration(n) * 24 * time.Hour
	} else {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
	}
	if d <= 0 {
		return 0, fmt.Errorf("duration %q must be positive", s)
	}
	return d, nil
}

// rateUnits maps the period names a rate accepts to their length.
var rateUnits = map[string]time.Duration{
	"s": time.Second, "sec": time.Second, "second": time.Second,
	"m": time.Minute, "min": time.Minute, "minute": time.Minute,
	"h": time.Hour, "hour": time.Hour,
	"d": 24 * time.Hour, "day": 24 * time.Hour,
}

// ParseRate parses a step rate such as 10/min into the number of deliveries allowed per period.
func ParseRate(s string) (count int, per time.Duration, err error) {
	n, unit, ok := strings.Cut(s, "/")
	if !ok {
		return 0, 0, fmt.Errorf("invalid rate %q: want <count>/<s|min|h|day>", s)
	}
	count, err = strconv.Atoi(n)
	if err != nil || count < 1 {
		return 0, 0, fmt.Errorf("invalid rate %q: count must be a positive integer", s)
	}
	per, ok = rateUnits[unit]
	if !ok {
		return 0, 0, fmt.Errorf("invalid rate %q: unknown period %q", s, unit)
	}
	return count, per, nil
}
