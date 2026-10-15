// Command crawl is the M1 fixture's crawler: it emits the configured number of synthetic listings, keyed by listing
// id and generated deterministically from the configured seed, whatever trigger fired the run.
package main

import (
	"encoding/json"
	"errors"
	"time"

	millstream "github.com/monolithiclab/millstream/sdk/go"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
)

type config struct {
	Seed  uint64 `json:"seed"`
	Count int    `json:"count"`
	// DelayMs simulates a slow source: the crawl waits this long before emitting.
	DelayMs int `json:"delayMs"`
}

func main() {
	millstream.MapKeyed(func(c millstream.Context, _ json.RawMessage) ([]millstream.Keyed[listing.Listing], error) {
		var cfg config
		if err := c.ConfigInto(&cfg); err != nil {
			return nil, err
		}
		if cfg.Count <= 0 {
			return nil, errors.New("config count must be positive")
		}
		time.Sleep(time.Duration(cfg.DelayMs) * time.Millisecond)
		ls := listing.Generate(cfg.Seed, cfg.Count)
		out := make([]millstream.Keyed[listing.Listing], len(ls))
		for i, l := range ls {
			out[i] = millstream.Keyed[listing.Listing]{Key: l.ID, Value: l}
		}
		c.Logger().Info("listings crawled", "count", len(out))
		return out, nil
	})
}
