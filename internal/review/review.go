// Package review holds what the review builtin needs besides the store (spec section 8): which auto-approved items
// become review samples, the JSON Patch edits a reviewer applies and their diff, and the webhook notifications of
// parked items. The review units themselves (Park, Review decision, Sample decision, Review timeout) live in the
// store; the engine parks deliveries and applies timeouts, and the inbox command and the HTTP API decide them.
package review

import (
	"crypto/sha256"
	"encoding/binary"
)

// Reviewers that are not people: an item autoApprove approved is recorded as approved by Rule, and one onTimeout
// decided as decided by Timeout. Events record Person for anyone else: metrics and insights never see who decided
// (spec section 11.6); the review keeps the name.
const (
	ReviewerRule    = "rule"
	ReviewerTimeout = "timeout"
	ReviewerPerson  = "person"
)

// Sampled reports whether the auto-approved item id becomes a review sample at rate: deterministic per item id, so a
// delivery retried or replayed is sampled alike, and uniform over ids (the first 8 bytes of the id's SHA-256 as a
// fraction of 2^64), so about rate of the items are.
func Sampled(itemID string, rate float64) bool {
	sum := sha256.Sum256([]byte(itemID))
	return float64(binary.BigEndian.Uint64(sum[:8]))/(1<<64) < rate
}
