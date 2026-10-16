package engine

import (
	"math"
	"time"
)

// Bucket is a token bucket (spec sections 4.6 and 5.9): a step's rate in the engine, a webhook trigger's in the API.
// It holds up to count tokens, refilled continuously at count per period, and each lease or request takes one. It
// starts full, so a first period may use its whole rate at once. It is not safe for concurrent use.
type Bucket struct {
	rate   string // as written, to notice a revision changing it
	count  float64
	per    time.Duration
	tokens float64
	at     time.Time // when tokens was last refilled
}

// NewBucket returns a full bucket of count tokens per period, rate being how the rate was written.
func NewBucket(rate string, count int, per time.Duration, now time.Time) *Bucket {
	return &Bucket{rate: rate, count: float64(count), per: per, tokens: float64(count), at: now}
}

// Rate returns the rate as written.
func (b *Bucket) Rate() string { return b.rate }

// Available refills the bucket up to now and returns the whole tokens it holds.
func (b *Bucket) Available(now time.Time) int {
	if elapsed := now.Sub(b.at); elapsed > 0 {
		b.tokens = math.Min(b.count, b.tokens+b.count*elapsed.Seconds()/b.per.Seconds())
		b.at = now
	}
	return int(b.tokens)
}

// Take spends one token.
func (b *Bucket) Take() { b.tokens = math.Max(b.tokens-1, 0) }

// TakeAt spends one token at now when there is one, and otherwise reports how long until there is.
func (b *Bucket) TakeAt(now time.Time) (time.Duration, bool) {
	if b.Available(now) == 0 {
		return time.Duration((1 - b.tokens) * float64(b.per) / b.count), false
	}
	b.Take()
	return 0, true
}
