// Command report is the M1 fixture's report step: for each priced listing it writes a small text report under
// MILLSTREAM_WORKDIR, attaches it to its output as an artifact, and emits a summary naming that file, with whether
// the margin clears the configured threshold.
package main

import (
	"os"

	millstream "github.com/monolithiclab/millstream/sdk/go"

	"github.com/monolithiclab/millstream/examples/m1-fixture/listing"
)

type config struct {
	MinMarginPct float64 `json:"minMarginPct"`
}

func main() {
	millstream.Map(func(c millstream.Context, p listing.Priced) ([]listing.Report, error) {
		var cfg config
		if err := c.ConfigInto(&cfg); err != nil {
			return nil, err
		}
		r := listing.Summarize(p, cfg.MinMarginPct)
		if err := writeReport(os.Getenv("MILLSTREAM_WORKDIR"), r.Artifact, r.Text()); err != nil {
			return nil, millstream.Retryable(err)
		}
		if err := c.Artifact(r.Artifact, r.Artifact); err != nil {
			return nil, err
		}
		return []listing.Report{r}, nil
	})
}

// writeReport writes the file inside dir only: the name comes from the payload, so it is resolved through an
// os.Root, which refuses a path leading outside.
func writeReport(dir, name, text string) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.WriteFile(name, []byte(text), 0o600)
}
