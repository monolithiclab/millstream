package evidence

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Import refusals. Each wraps store.ErrConflict, which the API answers 409.
var (
	ErrProduction = fmt.Errorf("a production deployment does not import evidence: %w", store.ErrConflict)
	ErrExpired    = fmt.Errorf("the bundle has expired: %w", store.ErrConflict)
)

// Imported is what an import did: the store's record, whether this import made it (false: the bundle was imported
// already), and the development deployment's active revision of the pipeline, its package hash ("" when the
// pipeline is not applied here), to compare with the production revision the bundle was exported on.
type Imported struct {
	store.EvidenceImport
	Created            bool
	ProductionRevision Revision
	LocalRevision      string
}

// Diverged reports whether the development package differs from the production revision the bundle holds.
func (i Imported) Diverged() bool { return i.LocalRevision != i.ProductionRevision.PackageHash }

// Import loads a bundle into a development deployment (mode is its environment's effective mode): the bundle is
// stored whole in the artifact store under its pipeline's prefix and recorded in the store's evidence imports. A
// production deployment and an expired bundle are refused; a bundle imported already is left as it is.
func Import(ctx context.Context, st store.Store, arts artifacts.Store, now time.Time, mode string, data []byte) (
	Imported, error,
) {
	if mode == pipeline.ModeProduction {
		return Imported{}, fmt.Errorf("evidence import: %w", ErrProduction)
	}
	b, err := Decode(data)
	if err != nil {
		return Imported{}, fmt.Errorf("evidence import: %w: %w", err, store.ErrInvalid)
	}
	if !b.ExpiresAt.After(now) {
		return Imported{}, fmt.Errorf("evidence import: bundle %s expired at %s: %w", b.ID,
			b.ExpiresAt.Format(time.RFC3339), ErrExpired)
	}
	ref, err := arts.Put(ctx, b.Pipeline, b.ID+".tar.gz", bytes.NewReader(data))
	if err != nil {
		return Imported{}, fmt.Errorf("evidence import: store the bundle: %w", err)
	}
	rec := store.EvidenceImport{ID: b.ID, Pipeline: b.Pipeline, RevisionHash: b.Revision.PackageHash,
		SHA256: ref.SHA256, Size: ref.Size, Items: len(b.Items), ExportedAt: b.ExportedAt, ExpiresAt: b.ExpiresAt}
	created, err := st.RecordImport(ctx, rec)
	if err != nil {
		return Imported{}, fmt.Errorf("evidence import: %w", err)
	}
	out := Imported{EvidenceImport: rec, Created: created, ProductionRevision: b.Revision}
	if id, err := st.ActiveRevision(ctx, b.Pipeline); err == nil {
		rev, err := st.Revision(ctx, id)
		if err != nil {
			return out, fmt.Errorf("evidence import: %w", err)
		}
		out.LocalRevision = rev.PackageHash
	} else if !errors.Is(err, store.ErrNotFound) {
		return out, fmt.Errorf("evidence import: %w", err)
	}
	return out, nil
}

// Load returns the bundles imported for a pipeline that have not expired at now, oldest import first: the history
// insights, backtests and the improve pipeline read beside the deployment's own.
func Load(ctx context.Context, st store.Store, arts artifacts.Store, pipelineName string, now time.Time) ([]*Bundle,
	error,
) {
	imports, err := st.EvidenceImports(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	var out []*Bundle
	for _, im := range imports {
		if !im.ExpiresAt.After(now) {
			continue
		}
		f, err := arts.Open(ctx, im.Pipeline, im.SHA256)
		if err != nil {
			return nil, fmt.Errorf("evidence bundle %s: %w", im.ID, err)
		}
		data, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("evidence bundle %s: %w", im.ID, err)
		}
		b, err := Decode(data)
		if err != nil {
			return nil, fmt.Errorf("evidence bundle %s: %w", im.ID, err)
		}
		out = append(out, b)
	}
	return out, nil
}

// StoreItems returns the bundle's items as the store's, in its pipeline: the shape local history has.
func (b *Bundle) StoreItems() []store.Item {
	out := make([]store.Item, len(b.Items))
	for i, it := range b.Items {
		out[i] = store.Item{ID: it.ID, Pipeline: b.Pipeline, RunID: it.Run, Stream: it.Stream, Step: it.Step,
			Key: it.Key, Parents: it.Parents, Generation: it.Generation, TS: it.TS, Payload: it.Payload,
			Artifacts: it.Artifacts, Tombstone: store.Tombstone(it.Tombstone)}
	}
	return out
}

// StoreDeliveries returns the bundle's deliveries as the store's.
func (b *Bundle) StoreDeliveries() []store.Delivery {
	out := make([]store.Delivery, len(b.Deliveries))
	for i, d := range b.Deliveries {
		out[i] = store.Delivery{ID: d.ID, Pipeline: b.Pipeline, RunID: d.Run, Step: d.Step, ItemID: d.Item,
			Generation: d.Generation, State: store.DeliveryState(d.State), Attempt: d.Attempt,
			ErrorClass: d.ErrorClass, ErrorMessage: d.Error, FilterReason: d.FilterReason, CostUSD: d.CostUSD,
			CreatedAt: d.CreatedAt, FinishedAt: d.FinishedAt, Wait: time.Duration(d.WaitMs) * time.Millisecond,
			Work: time.Duration(d.WorkMs) * time.Millisecond}
	}
	return out
}

// StoreReviews returns the bundle's reviews as the store's, their reviewer the decider (person, rule or timeout).
func (b *Bundle) StoreReviews() []store.Review {
	out := make([]store.Review, len(b.Reviews))
	for i, r := range b.Reviews {
		out[i] = store.Review{Pipeline: b.Pipeline, DeliveryID: r.Delivery, RunID: r.Run, Step: r.Step,
			State: store.ReviewState(r.State), Decision: r.Decision, Reviewer: r.Decider, Reason: r.Reason,
			Patch: r.Patch, ParkedAt: r.ParkedAt, DecidedAt: r.DecidedAt, Item: store.Item{ID: r.Item}}
	}
	return out
}

// StoreInsights returns the bundle's insights as the store's, in its pipeline.
func (b *Bundle) StoreInsights() []store.Insight {
	out := make([]store.Insight, len(b.Insights))
	for i, in := range b.Insights {
		out[i] = store.Insight{Pipeline: b.Pipeline, ID: in.ID, Kind: in.Kind, Scope: in.Scope,
			WindowStart: in.WindowStart, WindowEnd: in.WindowEnd, Severity: in.Severity, Evidence: in.Evidence,
			State: store.InsightState(in.State), OpenedAt: in.OpenedAt, UpdatedAt: in.UpdatedAt}
	}
	return out
}

// ImportedInsight is an insight as a development deployment lists it: its own (Bundle empty), or an open insight an
// imported bundle carries, with the bundle's id.
type ImportedInsight struct {
	store.Insight
	Bundle string
}

// ListInsights returns the insights f selects, the deployment's own (most recently updated first), then, when f
// selects open ones, the open insights the unexpired bundles imported for f's pipeline carry (every pipeline's when
// it names none) that the deployment does not record itself, the latest import's copy of each: production's
// insights listed beside the deployment's own, for people and the improve pipeline to act on (spec section 11.2).
// arts nil reads no bundle.
func ListInsights(ctx context.Context, st store.Store, arts artifacts.Store, f store.InsightFilter, now time.Time) (
	[]ImportedInsight, error,
) {
	all, err := st.Insights(ctx, store.InsightFilter{Pipeline: f.Pipeline})
	if err != nil {
		return nil, err
	}
	var out []ImportedInsight
	seen := map[[2]string]bool{}
	for _, in := range all {
		seen[[2]string{in.Pipeline, in.ID}] = true
		if f.State == "" || in.State == f.State {
			out = append(out, ImportedInsight{Insight: in})
		}
	}
	if arts == nil || (f.State != "" && f.State != store.InsightOpen) {
		return out, nil
	}
	bundles, err := Load(ctx, st, arts, f.Pipeline, now)
	if err != nil {
		return nil, err
	}
	for _, b := range slices.Backward(bundles) { // the latest import first
		for _, in := range b.StoreInsights() {
			k := [2]string{in.Pipeline, in.ID}
			if in.State == store.InsightOpen && !seen[k] {
				seen[k] = true
				out = append(out, ImportedInsight{Insight: in, Bundle: b.ID})
			}
		}
	}
	return out, nil
}

// Items indexes the items of the unexpired bundles imported for a pipeline, the first copy of each (bundles may
// overlap): the history the reads of an item the deployment does not hold fall back to.
type Items struct {
	ByID     map[string]store.Item
	Children map[string][]store.Item
}

// LoadItems reads the items of the unexpired bundles imported for a pipeline.
func LoadItems(ctx context.Context, st store.Store, arts artifacts.Store, pipelineName string, now time.Time) (
	*Items, error,
) {
	bundles, err := Load(ctx, st, arts, pipelineName, now)
	if err != nil {
		return nil, err
	}
	x := &Items{ByID: map[string]store.Item{}, Children: map[string][]store.Item{}}
	for _, b := range bundles {
		for _, it := range b.StoreItems() {
			if _, dup := x.ByID[it.ID]; dup {
				continue
			}
			x.ByID[it.ID] = it
			for _, p := range it.Parents {
				x.Children[p] = append(x.Children[p], it)
			}
		}
	}
	return x, nil
}
