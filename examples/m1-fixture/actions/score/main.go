// Command score is the M1 fixture's scorer: it rates each listing from its price per square metre against the
// city's market and its condition, deterministically. Run with --abstain-city=<city>, it abstains on the listings
// of that city instead (spec section 11.5): rules a fallback backs, as the crash suite's builtins variant runs it.
package main

import (
	"os"
	"strings"

	millstream "github.com/monolithiclab/millstream/sdk/go"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
)

func main() {
	var abstainCity string
	for _, arg := range os.Args[1:] {
		if v, ok := strings.CutPrefix(arg, "--abstain-city="); ok {
			abstainCity = v
		}
	}
	millstream.Map(func(c millstream.Context, l listing.Listing) ([]listing.Scored, error) {
		if abstainCity != "" && l.City == abstainCity {
			return nil, c.Abstain("no rule for the listings of " + l.City)
		}
		return []listing.Scored{{Listing: l, Score: listing.Score(l)}}, nil
	})
}
