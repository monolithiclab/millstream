// Package listing is the domain of the M1 fixture pipeline: synthetic real estate listings generated
// deterministically from a seed, and the scoring, pricing and reporting rules the fixture's actions apply. The
// actions share it so their outputs are reproducible, and the end-to-end tests use it to compute the counts a run
// must produce.
package listing

import (
	"errors"
	"fmt"
	"math"
)

// Condition grades a property's state; a poorer one costs more to renovate and scores higher as an opportunity.
const (
	ConditionGood = "good"
	ConditionFair = "fair"
	ConditionPoor = "poor"
)

// FailingIndex is the listing generated without a rent estimate: strict pricing refuses it, which the fixture uses
// as its designed non-retryable failure. It is generated cheap and in poor condition, so it is always shortlisted.
const FailingIndex = 13

// ErrNoRent is returned, wrapped, by Price in strict mode for a listing without a rent estimate.
var ErrNoRent = errors.New("no monthlyRentEur")

// Listing is a crawled listing.
type Listing struct {
	ID        string `json:"id"`
	City      string `json:"city"`
	PriceEur  int    `json:"priceEur"`
	SurfaceM2 int    `json:"surfaceM2"`
	Rooms     int    `json:"rooms"`
	Condition string `json:"condition"`
	// MonthlyRentEur is the estimated rent; nil when the source gave none.
	MonthlyRentEur *int `json:"monthlyRentEur,omitempty"`
}

// Scored is a listing with its score, 0 to 100.
type Scored struct {
	Listing
	Score int `json:"score"`
}

// Priced is a scored listing with its rentability.
type Priced struct {
	Scored
	CostEur       int     `json:"costEur"`
	ResaleEur     int     `json:"resaleEur"`
	MarginPct     float64 `json:"marginPct"`
	GrossYieldPct float64 `json:"grossYieldPct"`
}

// Report is the summary the report step emits for a priced listing.
type Report struct {
	ListingID string  `json:"listingId"`
	MarginPct float64 `json:"marginPct"`
	Selected  bool    `json:"selected"`
	// Artifact is the report file's path, relative to the action's working directory.
	Artifact string `json:"artifact"`
}

// market is a city's resale price per square metre.
type market struct {
	city      string
	eurPerM2  int
	rentPerM2 int // typical monthly rent per square metre, used when a listing has no estimate
}

var markets = []market{
	{"Lyon", 5200, 15}, {"Bordeaux", 4700, 14}, {"Nantes", 4100, 13}, {"Lille", 3600, 13}, {"Grenoble", 3300, 12},
}

var conditions = []string{ConditionGood, ConditionFair, ConditionPoor}

// ID returns the id of the i-th listing, counting from 1.
func ID(i int) string { return fmt.Sprintf("L%03d", i) }

// mix is splitmix64: a fixed, well-spread function of the seed and the index, the same on every platform.
func mix(seed, i uint64) uint64 {
	z := seed*0x9e3779b97f4a7c15 + i*0xbf58476d1ce4e5b9
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Generate returns the n listings of the seed, L001 to Ln.
func Generate(seed uint64, n int) []Listing {
	out := make([]Listing, 0, n)
	for i := 1; i <= n; i++ {
		h := mix(seed, uint64(i)) // #nosec G115 -- i is positive
		m := markets[h%uint64(len(markets))]
		surface := 25 + int((h>>8)%95)
		cond := conditions[(h>>16)%uint64(len(conditions))]
		pricePct := 55 + int((h>>24)%60) // asking price, 55% to 114% of the market
		rent := surface * (m.rentPerM2 - 2 + int((h>>32)%5))
		l := Listing{
			ID: ID(i), City: m.city, SurfaceM2: surface, Rooms: 1 + surface/25, Condition: cond, MonthlyRentEur: &rent,
		}
		if i == FailingIndex {
			l.Condition, pricePct, l.MonthlyRentEur = ConditionPoor, 60, nil
		}
		l.PriceEur = roundTo(surface*m.eurPerM2*pricePct/100, 1000)
		out = append(out, l)
	}
	return out
}

// marketOf returns the city's market; an unknown city gets the cheapest one.
func marketOf(city string) market {
	for _, m := range markets {
		if m.city == city {
			return m
		}
	}
	return markets[len(markets)-1]
}

// Score rates a listing as an opportunity: 50, plus the discount of its price per square metre to the market in
// points, plus 10 for a poor condition and 5 for a fair one, clamped to 0..100.
func Score(l Listing) int {
	if l.SurfaceM2 <= 0 {
		return 0
	}
	m := marketOf(l.City)
	discount := 100 - l.PriceEur*100/(l.SurfaceM2*m.eurPerM2)
	bonus := map[string]int{ConditionPoor: 10, ConditionFair: 5}[l.Condition]
	return min(100, max(0, 50+discount+bonus))
}

// renovationPerM2 is the renovation cost per square metre by condition.
var renovationPerM2 = map[string]int{ConditionGood: 200, ConditionFair: 600, ConditionPoor: 1100}

// Price computes a scored listing's rentability: purchase fees of 8%, renovation by condition, resale at the
// market price, and the gross rental yield. A listing without a rent estimate fails with ErrNoRent when strict, and
// otherwise uses the city's typical rent.
func Price(s Scored, strict bool) (Priced, error) {
	m := marketOf(s.City)
	rent := s.SurfaceM2 * m.rentPerM2
	switch {
	case s.MonthlyRentEur != nil:
		rent = *s.MonthlyRentEur
	case strict:
		return Priced{}, fmt.Errorf("listing %s: %w", s.ID, ErrNoRent)
	}
	cost := s.PriceEur + s.PriceEur*8/100 + s.SurfaceM2*renovationPerM2[s.Condition]
	resale := s.SurfaceM2 * m.eurPerM2
	p := Priced{Scored: s, CostEur: cost, ResaleEur: resale}
	if cost > 0 {
		p.MarginPct = round1(float64(resale-cost) * 100 / float64(cost))
		p.GrossYieldPct = round1(float64(rent*12) * 100 / float64(cost))
	}
	return p, nil
}

// Summarize builds a priced listing's report: selected when its margin clears minMarginPct.
func Summarize(p Priced, minMarginPct float64) Report {
	return Report{
		ListingID: p.ID, MarginPct: p.MarginPct, Selected: p.MarginPct >= minMarginPct,
		Artifact: "report-" + p.ID + ".txt",
	}
}

// Text is the report file's content.
func (r Report) Text() string {
	verdict := "below threshold"
	if r.Selected {
		verdict = "selected"
	}
	return fmt.Sprintf("listing %s\nmargin %.1f%%\n%s\n", r.ListingID, r.MarginPct, verdict)
}

func roundTo(v, unit int) int { return (v + unit/2) / unit * unit }

func round1(v float64) float64 { return math.Round(v*10) / 10 }
