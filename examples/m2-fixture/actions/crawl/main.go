// Command crawl is the M2 fixture's crawler: it emits the configured day's listings from the fixture's pool, keyed by
// listing id, so the listings of consecutive days overlap.
package main

import (
	"encoding/json"
	"errors"

	millstream "github.com/monolithiclab/millstream/sdk/go"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
	"github.com/monolithiclab/millstream/examples/m2-fixture/pool"
)

type config struct {
	Day int `json:"day"`
}

func main() {
	millstream.MapKeyed(func(c millstream.Context, _ json.RawMessage) ([]millstream.Keyed[listing.Listing], error) {
		var cfg config
		if err := c.ConfigInto(&cfg); err != nil {
			return nil, err
		}
		if cfg.Day < 1 {
			return nil, errors.New("config day must be 1 or more")
		}
		ls := pool.Day(cfg.Day)
		out := make([]millstream.Keyed[listing.Listing], len(ls))
		for i, l := range ls {
			out[i] = millstream.Keyed[listing.Listing]{Key: l.ID, Value: l}
		}
		c.Logger().Info("listings crawled", "day", cfg.Day, "count", len(out))
		return out, nil
	})
}
