// Command list is the M5 distill fixture's source: it emits the configured day's synthetic listings, keyed by id.
package main

import (
	"encoding/json"
	"errors"

	millstream "github.com/monolithiclab/millstream/sdk/go"

	"github.com/monolithiclab/millstream/examples/m5-distill/tiers"
)

type config struct {
	Day   int `json:"day"`
	Count int `json:"count"`
}

func main() {
	millstream.MapKeyed(func(c millstream.Context, _ json.RawMessage) ([]millstream.Keyed[tiers.Listing], error) {
		var cfg config
		if err := c.ConfigInto(&cfg); err != nil {
			return nil, err
		}
		if cfg.Day < 1 || cfg.Count < 1 {
			return nil, errors.New("config day and count must be 1 or more")
		}
		ls := tiers.Day(cfg.Day, cfg.Count)
		out := make([]millstream.Keyed[tiers.Listing], len(ls))
		for i, l := range ls {
			out[i] = millstream.Keyed[tiers.Listing]{Key: l.ID, Value: l}
		}
		return out, nil
	})
}
