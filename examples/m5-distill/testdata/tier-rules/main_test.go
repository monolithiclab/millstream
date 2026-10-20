package main

import "testing"

func TestTier(t *testing.T) {
	for _, c := range []struct {
		l    Listing
		want string
		ok   bool
	}{
		{Listing{PriceEur: 100000, SurfaceM2: 50}, "low", true},
		{Listing{PriceEur: 200000, SurfaceM2: 50}, "mid", true},
		{Listing{PriceEur: 300000, SurfaceM2: 50}, "high", true},
		{Listing{PriceEur: 300000}, "", false},
	} {
		if got, ok := tier(c.l); got != c.want || ok != c.ok {
			t.Errorf("%+v: %q %v", c.l, got, ok)
		}
	}
}
