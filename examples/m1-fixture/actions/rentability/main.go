// Command rentability is the M1 fixture's pricing step: fees, renovation, resale margin and rental yield. With
// config strict (the default), a listing without a rent estimate fails without retry, which dead-letters it; with
// strict false the city's typical rent stands in.
package main

import (
	millstream "github.com/monolithiclab/millstream/sdk/go"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
)

type config struct {
	Strict *bool `json:"strict"`
}

func main() {
	millstream.Map(func(c millstream.Context, s listing.Scored) ([]listing.Priced, error) {
		var cfg config
		if err := c.ConfigInto(&cfg); err != nil {
			return nil, err
		}
		p, err := listing.Price(s, cfg.Strict == nil || *cfg.Strict)
		if err != nil {
			return nil, err // deterministic: retrying cannot help
		}
		return []listing.Priced{p}, nil
	})
}
