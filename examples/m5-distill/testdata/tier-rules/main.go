// Command tier-rules sorts a listing into its price tier by rule, abstaining on a listing without a surface, which
// the agent keeps.
package main

import (
	millstream "github.com/monolithiclab/millstream/sdk/go"
)

// Listing is the tier step's input.
type Listing struct {
	ID        string `json:"id"`
	City      string `json:"city"`
	PriceEur  int    `json:"priceEur"`
	SurfaceM2 int    `json:"surfaceM2"`
}

// Tiered is its output: the listing as given, with its tier.
type Tiered struct {
	Listing
	Tier string `json:"tier"`
}

// tier is the rule the training pairs follow: low under 3,000 EUR per square metre, mid under 6,000, high above.
func tier(l Listing) (string, bool) {
	if l.SurfaceM2 <= 0 {
		return "", false
	}
	switch perM2 := l.PriceEur / l.SurfaceM2; {
	case perM2 < 3000:
		return "low", true
	case perM2 < 6000:
		return "mid", true
	}
	return "high", true
}

func main() {
	millstream.Map(func(c millstream.Context, in Listing) ([]Tiered, error) {
		t, ok := tier(in)
		if !ok {
			return nil, c.Abstain("no surface: no price per square metre")
		}
		return []Tiered{{Listing: in, Tier: t}}, nil
	})
}
