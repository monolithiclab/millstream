package evidence

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// lineageChunk bounds the item ids one read names.
const lineageChunk = 200

// maxTranscript bounds a transcript an export reads into memory.
const maxTranscript = 64 << 20

// ExportRequest asks for a bundle of a pipeline: the deliveries finished since Since are sampled, Sample of them
// (DefaultSample when zero), and the bundle expires TTL after the export (DefaultTTL when zero). Insight, when set,
// keeps that insight alone and samples its step, its sample items' lineages included. By is who exports, recorded in
// the store, never in the bundle.
type ExportRequest struct {
	Pipeline string
	Since    time.Time
	Insight  string
	Sample   int
	TTL      time.Duration
	By       string
}

// Export builds a bundle of the pipeline's history and records it in the store's evidence exports. It returns the
// bundle's archive and what the store recorded. arts may be nil: the transcripts are then left out.
func Export(ctx context.Context, st store.Store, arts artifacts.Store, now time.Time, req ExportRequest) ([]byte,
	store.EvidenceExport, error,
) {
	req.Sample, req.TTL = cmp.Or(req.Sample, DefaultSample), cmp.Or(req.TTL, DefaultTTL)
	x := &exporter{st: st, arts: arts, req: req, items: map[string]store.Item{}}
	b, err := x.build(ctx, now)
	if err != nil {
		return nil, store.EvidenceExport{}, fmt.Errorf("evidence export: %w", err)
	}
	data, err := b.Encode()
	if err != nil {
		return nil, store.EvidenceExport{}, fmt.Errorf("evidence export: %w", err)
	}
	scope, _ := json.Marshal(b.Scope) // times, strings and a number: always encodes
	rec := store.EvidenceExport{ID: b.ID, Pipeline: b.Pipeline, ExportedBy: req.By, Scope: scope,
		RevisionHash: b.Revision.PackageHash, Items: len(b.Items), ExpiresAt: b.ExpiresAt}
	for _, it := range x.items {
		rec.Keys = append(rec.Keys, it.Key)
	}
	if err := st.RecordExport(ctx, rec); err != nil {
		return nil, rec, fmt.Errorf("evidence export: %w", err)
	}
	return data, rec, nil
}

// exporter gathers one bundle.
type exporter struct {
	st    store.Store
	arts  artifacts.Store
	req   ExportRequest
	items map[string]store.Item // the lineages, unredacted
}

func (x *exporter) build(ctx context.Context, now time.Time) (*Bundle, error) {
	name := x.req.Pipeline
	ps, err := x.st.PipelineState(ctx, name)
	if err != nil {
		return nil, err
	}
	rev, err := x.st.Revision(ctx, ps.ActiveRevision)
	if err != nil {
		return nil, err
	}
	pkg, err := x.st.Package(ctx, rev.PackageHash)
	if err != nil {
		return nil, err
	}
	key, err := x.st.EvidenceKey(ctx)
	if err != nil {
		return nil, err
	}
	pointers, err := x.pointers(ctx)
	if err != nil {
		return nil, err
	}
	insights, step, extra, err := x.insights(ctx)
	if err != nil {
		return nil, err
	}
	deliveries, err := x.st.Deliveries(ctx, store.DeliveryFilter{Pipeline: name, Since: x.req.Since})
	if err != nil {
		return nil, err
	}
	reviews, err := x.reviews(ctx)
	if err != nil {
		return nil, err
	}
	sample := stratify(deliveries, reviews, step, x.req.Sample)
	byID := map[string]store.Delivery{}
	for _, d := range deliveries {
		byID[d.ID] = d
	}
	roots := extra
	for _, s := range sample {
		roots = append(roots, byID[s.Delivery].ItemID)
	}
	if err := x.lineage(ctx, roots); err != nil {
		return nil, err
	}
	r := newRedactor(key)
	ids := slices.Sorted(maps.Keys(x.items))
	ptrs := x.itemPointers(pointers)
	for _, id := range ids {
		r.collect(ptrs[id], x.items[id].Payload)
	}
	r.finish()
	// An insight's evidence quotes payloads too (an error's example, a filter's condition): redacted as one.
	for i, in := range insights {
		ev, err := r.Payload(nil, in.Evidence)
		if err != nil {
			return nil, fmt.Errorf("insight %s: %w", in.ID, err)
		}
		insights[i].Evidence = ev
	}
	b := &Bundle{
		Manifest: Manifest{Format: Format, Version: Version, ID: newID(now), Pipeline: name,
			Revision:   Revision{ID: int64(rev.ID), PackageHash: rev.PackageHash, AppliedAt: rev.AppliedAt},
			ExportedAt: now.UTC(), ExpiresAt: now.Add(x.req.TTL).UTC(),
			Scope:  Scope{Since: x.req.Since.UTC(), Insight: x.req.Insight, Sample: x.req.Sample},
			Strata: map[string]int{}},
		Evidence: Evidence{Insights: insights, Sample: sample, Items: []Item{}, Deliveries: []Delivery{},
			Reviews: []Review{}, Transcripts: []Transcript{}},
		Package: pkg, Files: map[string][]byte{},
	}
	for _, s := range sample {
		b.Strata[s.Stratum]++
	}
	for _, id := range ids {
		it := x.items[id]
		payload, err := r.Payload(ptrs[id], it.Payload)
		if err != nil {
			return nil, fmt.Errorf("item %s: %w", id, err)
		}
		b.Items = append(b.Items, Item{ID: it.ID, Run: it.RunID, Stream: it.Stream, Step: it.Step,
			Key: r.String(it.Key), Parents: it.Parents, Generation: it.Generation, TS: it.TS, Payload: payload,
			Artifacts: it.Artifacts, Tombstone: string(it.Tombstone)})
	}
	for _, d := range deliveries {
		if _, ok := x.items[d.ItemID]; !ok {
			continue
		}
		b.Deliveries = append(b.Deliveries, Delivery{ID: d.ID, Run: d.RunID, Step: d.Step, Item: d.ItemID,
			Generation: d.Generation, State: string(d.State), Attempt: d.Attempt, ErrorClass: d.ErrorClass,
			Error: r.String(d.ErrorMessage), FilterReason: d.FilterReason, CostUSD: d.CostUSD,
			CreatedAt: d.CreatedAt, FinishedAt: d.FinishedAt, WaitMs: d.Wait.Milliseconds(),
			WorkMs: d.Work.Milliseconds()})
		if rv, ok := reviews[d.ID]; ok {
			patch, err := r.Patch(ptrs[d.ItemID], rv.Patch)
			if err != nil {
				return nil, fmt.Errorf("review of %s: %w", d.ID, err)
			}
			b.Reviews = append(b.Reviews, Review{Delivery: d.ID, Run: d.RunID, Step: d.Step, Item: d.ItemID,
				State: string(rv.State), Decision: rv.Decision, Decider: decider(rv.Reviewer), Reason: rv.Reason,
				Patch: patch, ParkedAt: rv.ParkedAt, DecidedAt: rv.DecidedAt})
		}
		if err := x.transcripts(ctx, b, r, d.ID); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// pointers returns the redact pointers of each stream, over every revision of the pipeline: a pointer a later
// revision dropped still covers the items published before.
func (x *exporter) pointers(ctx context.Context) (map[string][]string, error) {
	revs, err := x.st.Revisions(ctx, x.req.Pipeline)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, rev := range revs {
		p, err := rev.Decode()
		if err != nil {
			return nil, err
		}
		for name, decl := range p.Streams {
			for _, ptr := range decl.Redact {
				if !slices.Contains(out[name], ptr) {
					out[name] = append(out[name], ptr)
				}
			}
		}
	}
	return out, nil
}

// itemPointers returns the redact pointers of each item read: its stream's and its ancestors', which steps carry
// fields from.
func (x *exporter) itemPointers(byStream map[string][]string) map[string][]string {
	out := map[string][]string{}
	var of func(id string, depth int) []string
	of = func(id string, depth int) []string {
		if ptrs, ok := out[id]; ok || depth > len(x.items) {
			return ptrs
		}
		it, ok := x.items[id]
		if !ok {
			return nil
		}
		ptrs := slices.Clone(byStream[it.Stream])
		for _, parent := range it.Parents {
			for _, p := range of(parent, depth+1) {
				if !slices.Contains(ptrs, p) {
					ptrs = append(ptrs, p)
				}
			}
		}
		slices.Sort(ptrs)
		out[id] = ptrs
		return ptrs
	}
	for id := range x.items {
		of(id, 0)
	}
	return out
}

// insights returns the insights the bundle carries: the open ones, or the one named, with the step it scopes and
// the sample items its evidence lists.
func (x *exporter) insights(ctx context.Context) ([]Insight, string, []string, error) {
	f := store.InsightFilter{Pipeline: x.req.Pipeline, State: store.InsightOpen}
	if x.req.Insight != "" {
		f.State = ""
	}
	all, err := x.st.Insights(ctx, f)
	if err != nil {
		return nil, "", nil, err
	}
	out := []Insight{}
	for _, in := range all {
		if x.req.Insight == "" || in.ID == x.req.Insight {
			out = append(out, Insight{ID: in.ID, Kind: in.Kind, Scope: in.Scope, Severity: in.Severity,
				State: string(in.State), WindowStart: in.WindowStart, WindowEnd: in.WindowEnd, Evidence: in.Evidence,
				OpenedAt: in.OpenedAt, UpdatedAt: in.UpdatedAt})
		}
	}
	if x.req.Insight == "" {
		return out, "", nil, nil
	}
	if len(out) == 0 {
		return nil, "", nil, fmt.Errorf("insight %s of %s: %w", x.req.Insight, x.req.Pipeline, store.ErrNotFound)
	}
	var ev struct {
		Samples []string `json:"samples"`
	}
	_ = json.Unmarshal(out[0].Evidence, &ev) // evidence without samples samples nothing more
	return out, out[0].Scope, ev.Samples, nil
}

// reviews returns the pipeline's decided and timed-out reviews, by delivery.
func (x *exporter) reviews(ctx context.Context) (map[string]store.Review, error) {
	out := map[string]store.Review{}
	for _, state := range []store.ReviewState{store.ReviewDecided, store.ReviewTimedOut} {
		rs, err := x.st.Reviews(ctx, store.ReviewFilter{Pipeline: x.req.Pipeline, State: state})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			out[r.DeliveryID] = r
		}
	}
	return out, nil
}

// decider is who decided a review, without naming a person.
func decider(reviewer string) string {
	switch reviewer {
	case review.ReviewerRule:
		return DeciderRule
	case review.ReviewerTimeout:
		return DeciderTimeout
	}
	return DeciderPerson
}

// stratum is a delivery's outcome, "" for one that has not ended or was cancelled.
func stratum(d store.Delivery, reviews map[string]store.Review) string {
	if r, ok := reviews[d.ID]; ok {
		switch {
		case r.Decision == store.DecisionReject:
			return StratumRejected
		case len(r.Patch) > 0 && string(r.Patch) != "[]":
			return StratumEdited
		default:
			return StratumApproved
		}
	}
	switch d.State {
	case store.DeliveryDone:
		return StratumDone
	case store.DeliveryFiltered:
		return StratumFiltered
	case store.DeliveryDead:
		return StratumDead
	}
	return ""
}

// Stratify samples n of the deliveries that ended as an export does (spec section 11.2): backtests sample their
// past items so too (spec section 11.3).
func Stratify(ds []store.Delivery, reviews map[string]store.Review, n int) []Sampled {
	return stratify(ds, reviews, "", n)
}

// stratify samples n of the deliveries that ended (those of step alone when it names one of their steps): an equal
// share of n for each stratum, a stratum with fewer handing what it leaves to the others, and within a stratum
// deliveries evenly spaced in the order they finished, so the sample spans the window.
func stratify(ds []store.Delivery, reviews map[string]store.Review, step string, n int) []Sampled {
	if step != "" && !slices.ContainsFunc(ds, func(d store.Delivery) bool { return d.Step == step }) {
		step = ""
	}
	pop := map[string][]store.Delivery{}
	for _, d := range ds {
		if s := stratum(d, reviews); s != "" && (step == "" || d.Step == step) {
			pop[s] = append(pop[s], d)
		}
	}
	alloc := map[string]int{}
	open := slices.DeleteFunc(slices.Clone(Strata), func(s string) bool { return len(pop[s]) == 0 })
	for left := n; left > 0 && len(open) > 0; {
		share := max(left/len(open), 1)
		next := open[:0]
		for _, s := range open {
			take := min(share, len(pop[s])-alloc[s], left)
			alloc[s] += take
			left -= take
			if alloc[s] < len(pop[s]) {
				next = append(next, s)
			}
		}
		open = next
	}
	out := []Sampled{}
	for _, s := range Strata {
		in := pop[s]
		slices.SortFunc(in, func(a, b store.Delivery) int {
			return cmp.Or(a.FinishedAt.Compare(b.FinishedAt), strings.Compare(a.ID, b.ID))
		})
		k := alloc[s]
		for i := range k {
			out = append(out, Sampled{Delivery: in[(2*i+1)*len(in)/(2*k)].ID, Stratum: s})
		}
	}
	return out
}

// lineage reads the roots, their ancestors and their descendants in the pipeline into x.items.
func (x *exporter) lineage(ctx context.Context, roots []string) error {
	name := x.req.Pipeline
	roots = slices.Compact(slices.Sorted(slices.Values(roots)))
	// Ancestors: each level's parents, read by id.
	frontier := roots
	for len(frontier) > 0 {
		var missing []string
		for _, id := range frontier {
			if _, ok := x.items[id]; !ok {
				missing = append(missing, id)
			}
		}
		frontier = nil
		for chunk := range slices.Chunk(missing, lineageChunk) {
			found, err := x.st.Items(ctx, store.ItemFilter{Pipeline: name, IDs: chunk})
			if err != nil {
				return err
			}
			for _, it := range found {
				x.items[it.ID] = it
				if it.ParentPipeline == "" { // a copy's parents are the producer's, outside the pipeline
					frontier = append(frontier, it.Parents...)
				}
			}
		}
	}
	// Descendants: each level's children.
	frontier = roots
	for len(frontier) > 0 {
		var next []string
		for chunk := range slices.Chunk(frontier, lineageChunk) {
			found, err := x.st.Items(ctx, store.ItemFilter{Pipeline: name, Parents: chunk})
			if err != nil {
				return err
			}
			for _, it := range found {
				if _, ok := x.items[it.ID]; !ok {
					x.items[it.ID] = it
					next = append(next, it.ID)
				}
			}
		}
		frontier = next
	}
	return nil
}

// transcripts adds the artifacts the attempts of a delivery kept, redacted, to the bundle.
func (x *exporter) transcripts(ctx context.Context, b *Bundle, r *redactor, delivery string) error {
	if x.arts == nil {
		return nil
	}
	arts, err := x.st.AttemptArtifacts(ctx, x.req.Pipeline, delivery)
	if err != nil {
		return err
	}
	for _, a := range arts {
		text, err := x.readArtifact(ctx, a.ArtifactRef)
		if errors.Is(err, errGone) {
			continue
		}
		if err != nil {
			return fmt.Errorf("transcript %s of %s: %w", a.Name, delivery, err)
		}
		body := []byte(r.String(string(text)))
		sum := sha256.Sum256(body)
		sha := hex.EncodeToString(sum[:])
		b.Files[sha] = body
		b.Transcripts = append(b.Transcripts, Transcript{Delivery: delivery, Attempt: a.Attempt, Name: a.Name,
			SHA256: sha, Size: int64(len(body))})
	}
	return nil
}

// errGone is an artifact whose file is no longer in the store.
var errGone = errors.New("artifact gone")

func (x *exporter) readArtifact(ctx context.Context, ref store.ArtifactRef) ([]byte, error) {
	f, err := x.arts.Open(ctx, x.req.Pipeline, ref.SHA256)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, errGone
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	rd, err := artifacts.Decompressed(ref, f)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(io.LimitReader(rd, maxTranscript))
}

// newID names a bundle: when it was exported and a random part.
func newID(now time.Time) string {
	var b [6]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails
	return "evidence_" + now.UTC().Format("20060102T150405Z") + "_" + hex.EncodeToString(b[:])
}
