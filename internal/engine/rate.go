package engine

import (
	"math"
	"time"
)

// bucket is a step's token bucket (spec section 5.9): it holds up to count tokens, refilled continuously at count
// per period, and each lease takes one. It starts full, so a step's first period may use its whole rate at once.
type bucket struct {
	rate   string // as written, to notice a revision changing it
	count  float64
	per    time.Duration
	tokens float64
	at     time.Time // when tokens was last refilled
}

func newBucket(rate string, count int, per time.Duration, now time.Time) *bucket {
	return &bucket{rate: rate, count: float64(count), per: per, tokens: float64(count), at: now}
}

// available refills the bucket up to now and returns the whole tokens it holds.
func (b *bucket) available(now time.Time) int {
	if elapsed := now.Sub(b.at); elapsed > 0 {
		b.tokens = math.Min(b.count, b.tokens+b.count*elapsed.Seconds()/b.per.Seconds())
		b.at = now
	}
	return int(b.tokens)
}

// take spends one token.
func (b *bucket) take() { b.tokens = math.Max(b.tokens-1, 0) }
