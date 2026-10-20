// Package tiers is the M5 distill fixture's data: synthetic listings, all distinct from one day to the next, and the
// rule the fake model applies when the end-to-end suite records the tier agent, which the distilled code must learn.
package tiers

import (
	"fmt"
	"hash/fnv"
)

// Listing is a synthetic listing.
type Listing struct {
	ID        string `json:"id"`
	City      string `json:"city"`
	PriceEur  int    `json:"priceEur"`
	SurfaceM2 int    `json:"surfaceM2"`
}

// Tiered is a listing with its tier, the agent's output.
type Tiered struct {
	Listing
	Tier string `json:"tier"`
}

var cities = []string{"Lille", "Lyon", "Nantes", "Paris"}

// Day returns day's count listings, D<day>-<n>: every 25th has no surface.
func Day(day, count int) []Listing {
	out := make([]Listing, count)
	for i := range out {
		id := fmt.Sprintf("D%d-%03d", day, i)
		h := fnv.New64a()
		_, _ = h.Write([]byte(id))
		x := h.Sum64()
		surface := 15 + int(x%120)
		if i%25 == 24 {
			surface = 0
		}
		perM2 := 1500 + int((x/120)%8000)
		out[i] = Listing{ID: id, City: cities[(x/1_000_000)%4], SurfaceM2: surface,
			PriceEur: max(surface, 20) * perM2 / 1000 * 1000}
	}
	return out
}

// Tier is the rule: low under 3,000 EUR per square metre, mid under 6,000, high otherwise or without a surface.
func Tier(l Listing) string {
	switch {
	case l.SurfaceM2 <= 0:
		return "high"
	case l.PriceEur/l.SurfaceM2 < 3000:
		return "low"
	case l.PriceEur/l.SurfaceM2 < 6000:
		return "mid"
	}
	return "high"
}
