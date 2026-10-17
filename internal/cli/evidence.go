package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// maxBundle bounds a bundle an export reads from the server.
const maxBundle = 1 << 30

// EvidenceCmd is "millstream evidence" (spec section 11.2).
type EvidenceCmd struct {
	Export EvidenceExportCmd `cmd:"" help:"Export an evidence bundle of a pipeline, in production."`
	Import EvidenceImportCmd `cmd:"" help:"Import an evidence bundle as read-only history, in development."`
}

// EvidenceExportCmd is "millstream evidence export".
type EvidenceExportCmd struct {
	Pipeline string `required:"" help:"The pipeline whose history the bundle samples."`
	Since    string `default:"30d" help:"Sample the deliveries created within this duration (7d, 12h)."`
	Insight  string `help:"Carry this insight alone and sample its step."`
	Out      string `type:"path" help:"Where to write the bundle (default: <bundle id>.tar.gz here)."`
}

// Run exports a bundle through the served engine's API with --server, else from this process, and writes it,
// readable by its owner alone: it holds production data.
func (c EvidenceExportCmd) Run(rt *Runtime) error {
	since, err := pipeline.ParseDuration(c.Since)
	if err != nil {
		return usageError("--since: " + err.Error())
	}
	var (
		data []byte
		id   string
	)
	err = changeState(rt, func(cl *apiClient) error {
		cl.user = identity(rt)
		res, body, err := cl.send(rt.Ctx, "POST", pipelinePath(c.Pipeline, "evidence"),
			api.EvidenceExportRequest{Since: c.Since, Insight: c.Insight}, maxBundle)
		data, id = body, res.Header.Get(api.BundleHeader)
		return err
	}, func(st store.Store) error {
		env, arts, err := localArtifacts(rt)
		if err != nil {
			return err
		}
		now := time.Now()
		var rec store.EvidenceExport
		data, rec, err = evidence.Export(rt.Ctx, st, arts, now, evidence.ExportRequest{Pipeline: c.Pipeline,
			Since: now.Add(-since), Insight: c.Insight, Sample: env.Improve.EvidenceSample, TTL: env.Improve.EvidenceTTL,
			By: identity(rt)})
		id = rec.ID
		return err
	})
	if err != nil {
		return err
	}
	b, err := evidence.Decode(data)
	if err != nil {
		return err
	}
	out := c.Out
	if out == "" {
		out = id + ".tar.gz"
	}
	if err := os.WriteFile(out, data, 0o600); err != nil {
		return err
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s: %s of %s (%d sampled), expires %s, written to %s\n", b.ID,
		plural(len(b.Items), "item"), b.Pipeline, len(b.Sample), b.ExpiresAt.Format(time.RFC3339), out)
	return err
}

// EvidenceImportCmd is "millstream evidence import".
type EvidenceImportCmd struct {
	Bundle string `arg:"" type:"existingfile" help:"The bundle evidence export wrote."`
}

// Run imports a bundle through the served engine's API with --server, else from this process; a production
// deployment refuses it. It reports when the development package diverged from the bundle's production revision.
func (c EvidenceImportCmd) Run(rt *Runtime) error {
	data, err := os.ReadFile(c.Bundle)
	if err != nil {
		return err
	}
	var im api.EvidenceImport
	fresh := false
	err = changeState(rt, func(cl *apiClient) error {
		status, err := cl.post(rt.Ctx, "/evidence/imports", api.EvidenceImportRequest{Bundle: data}, &im)
		fresh = created(status)
		return err
	}, func(st store.Store) error {
		env, arts, err := localArtifacts(rt)
		if err != nil {
			return err
		}
		got, err := evidence.Import(rt.Ctx, st, arts, time.Now(), env.EffectiveMode(pipeline.ModeDevelopment), data)
		im, fresh = api.ToEvidenceImport(got), got.Created
		return err
	})
	if err != nil {
		return err
	}
	suffix := ""
	if !fresh {
		suffix = " (imported already)"
	}
	if _, err := fmt.Fprintf(rt.IO.Out, "%s: %s of %s imported, expires %s%s\n", im.ID, plural(im.Items, "item"),
		im.Pipeline, im.ExpiresAt.Format(time.RFC3339), suffix); err != nil {
		return err
	}
	switch {
	case im.LocalRevision == "":
		_, err = fmt.Fprintf(rt.IO.Out, "%s is not applied here: the bundle was exported on revision %.12s\n",
			im.Pipeline, im.ProductionRevision)
	case im.Diverged:
		_, err = fmt.Fprintf(rt.IO.Out, "the development package (%.12s) has diverged from the production revision "+
			"(%.12s)\n", im.LocalRevision, im.ProductionRevision)
	}
	return err
}
