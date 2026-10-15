// Package pool is the listing pool of the M2 fixture: the crawl of day d emits PerDay consecutive listings of a pool
// of Size, starting Stride further each day, so consecutive days overlap and seven days of crawls hold duplicates a
// dedupe step removes. It also holds the scoring rule of the fake model the fixture's agent recordings were made
// against, so the end-to-end tests compute the scores a replayed run must produce.
package pool

import (
	"regexp"
	"strconv"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
)

// The pool's generator parameters.
const (
	Seed   = 7
	Size   = 12
	PerDay = 4
	Stride = 1
)

// All returns the pool, L001 to L012.
func All() []listing.Listing { return listing.Generate(Seed, Size) }

// Day returns the listings crawled on day d, counting from 1: PerDay consecutive listings of the pool from the
// (d-1)*Stride-th, wrapping around its end.
func Day(d int) []listing.Listing {
	all := All()
	out := make([]listing.Listing, PerDay)
	for i := range out {
		out[i] = all[((d-1)*Stride+i)%Size]
	}
	return out
}

// Score is the fake model's score of a listing: the M1 fixture's rule, lowered by poorPenalty points for a listing
// in poor condition, clamped to 0..100. The penalty comes from the step's lessons (Penalty), 0 without them.
func Score(l listing.Listing, poorPenalty int) int {
	s := listing.Score(l)
	if l.Condition == listing.ConditionPoor {
		s -= poorPenalty
	}
	return min(100, max(0, s))
}

// penaltyRule matches the lessons rule the fake model obeys.
var penaltyRule = regexp.MustCompile(`lower its score by (\d+) points`)

// Penalty returns the poor-condition penalty a prompt's lessons ask for, 0 when they ask for none.
func Penalty(prompt string) int {
	m := penaltyRule.FindStringSubmatch(prompt)
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1]) // \d+ always parses, short of an absurd length
	return n
}
