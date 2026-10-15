// Command score is the M1 fixture's scorer: it rates each listing from its price per square metre against the
// city's market and its condition, deterministically.
package main

import (
	millstream "github.com/monolithiclab/millstream/sdk/go"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
)

func main() {
	millstream.Map(func(_ millstream.Context, l listing.Listing) ([]listing.Scored, error) {
		return []listing.Scored{{Listing: l, Score: listing.Score(l)}}, nil
	})
}
