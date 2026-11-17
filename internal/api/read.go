package api

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"mime"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/insights"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Paging bounds: a page holds defaultLimit records unless the client asks for up to MaxLimit.
const (
	defaultLimit = 50
	MaxLimit     = 500
)

// Lineage bounds: how many generations a lineage walks by default and at most, and how many items each side holds
// (a variable, for tests).
const (
	defaultDepth = 10
	maxDepth     = 50
)

var maxLineageNodes = 500

// whereScanBound is the most items one page of a filtered item list reads: a filter that matches little answers a
// short page with a cursor rather than reading the whole stream.
const whereScanBound = 5000

// limit reads the limit query parameter.
func limit(r *http.Request) (int, error) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return defaultLimit, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > MaxLimit {
		return 0, invalidf("limit %q: want 1 to %d", v, MaxLimit)
	}
	return n, nil
}

// enumParam reads an enumerated query parameter: "" when absent, invalid when it is none of values.
func enumParam[T ~string](r *http.Request, name string, values ...T) (T, error) {
	v := T(r.URL.Query().Get(name))
	if v == "" || slices.Contains(values, v) {
		return v, nil
	}
	want := make([]string, len(values))
	for i, s := range values {
		want[i] = string(s)
	}
	return "", invalidf("%s %q: want one of %s", name, v, strings.Join(want, ", "))
}

// The values of the enumerated filters.
var (
	runStates = []store.RunState{store.RunRunning, store.RunWaitingReview, store.RunSucceeded,
		store.RunCompletedWithErrors, store.RunCancelled}
	deliveryStates = []store.DeliveryState{store.DeliveryPending, store.DeliveryLeased, store.DeliveryDone,
		store.DeliveryFiltered, store.DeliveryAwaitingReview, store.DeliveryDead, store.DeliveryCancelled}
	reviewStates  = []store.ReviewState{store.ReviewOpen, store.ReviewDecided, store.ReviewTimedOut, store.ReviewCancelled}
	sampleStates  = []store.SampleState{store.SampleOpen, store.SampleDecided, store.SampleUnreviewed}
	insightStates = []store.InsightState{store.InsightOpen, store.InsightAcknowledged, store.InsightResolved,
		store.InsightMuted}
)

// Cursors are opaque to clients: the position of the last record of a page, base64url-encoded with a prefix naming
// the list, so a cursor of one list is refused by another.
func encodeCursor(kind string, parts ...int64) string {
	s := kind
	for _, p := range parts {
		s += ":" + strconv.FormatInt(p, 10)
	}
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

// decodeCursor reads the cursor query parameter of a list of kind holding n numbers; no cursor is all zeros.
func decodeCursor(r *http.Request, kind string, n int) ([]int64, error) {
	v := r.URL.Query().Get("cursor")
	out := make([]int64, n)
	if v == "" {
		return out, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(v)
	parts := strings.Split(string(raw), ":")
	if err != nil || len(parts) != n+1 || parts[0] != kind {
		return nil, invalidf("cursor %q: not a cursor of this list", v)
	}
	for i, p := range parts[1:] {
		if out[i], err = strconv.ParseInt(p, 10, 64); err != nil {
			return nil, invalidf("cursor %q: not a cursor of this list", v)
		}
	}
	return out, nil
}

// activePipeline returns a pipeline's state and its active revision, record and decoded.
func activePipeline(ctx context.Context, st store.Store, name string) (store.PipelineState, store.Revision,
	*pipeline.Pipeline, error,
) {
	ps, err := st.PipelineState(ctx, name)
	if err != nil {
		return ps, store.Revision{}, nil, err
	}
	rev, p, err := decodedRevision(ctx, st, ps.ActiveRevision)
	return ps, rev, p, err
}

// decodedRevision reads a revision and decodes its pipeline.
func decodedRevision(ctx context.Context, st store.Store, id store.RevisionID) (store.Revision, *pipeline.Pipeline,
	error,
) {
	rev, err := st.Revision(ctx, id)
	if err != nil {
		return rev, nil, err
	}
	p, err := rev.Decode()
	return rev, p, err
}

// pageOf makes a page of the first n of rows, read n+1 at most: a row past n means a next page, whose cursor is
// the last row's.
func pageOf[T, U any](rows []T, n int, conv func(T) U, cursor func(T) string) Page[U] {
	page := Page[U]{Items: make([]U, 0, min(len(rows), n))}
	for _, row := range rows[:min(len(rows), n)] {
		page.Items = append(page.Items, conv(row))
	}
	if len(rows) > n {
		page.Next = cursor(rows[n-1])
	}
	return page
}

func (s *Server) pipeline(ctx context.Context, ps store.PipelineState, p *pipeline.Pipeline) (Pipeline, error) {
	g := graph(p, &ps)
	out := Pipeline{Name: ps.Name, Description: g.Description, State: "active", Revision: int64(ps.ActiveRevision),
		Steps: g.Steps, Triggers: g.Triggers, Imports: g.Imports, Streams: g.Streams}
	switch {
	case ps.Paused:
		out.State = "paused"
	case ps.Disabled:
		out.State = "disabled"
	}
	runs, err := s.opts.Store.Runs(ctx, store.RunFilter{Pipeline: ps.Name, Limit: 1})
	if err != nil {
		return out, err
	}
	if len(runs) == 1 {
		r := runs[0]
		out.LastRun = &RunSummary{ID: r.ID, State: string(r.State), StartedAt: r.StartedAt, SettledAt: timePtr(r.SettledAt)}
	}
	dls, err := s.opts.Store.DeadLetters(ctx, store.DeadLetterFilter{Pipeline: ps.Name, State: store.DeadLetterOpen})
	if err != nil {
		return out, err
	}
	reviews, err := s.opts.Store.Reviews(ctx, store.ReviewFilter{Pipeline: ps.Name})
	if err != nil {
		return out, err
	}
	out.OpenDeadLetters, out.OpenReviews = len(dls), len(reviews)
	return out, nil
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) error {
	states, err := s.opts.Store.Pipelines(r.Context())
	if err != nil {
		return err
	}
	out := make([]Pipeline, 0, len(states))
	for _, ps := range states {
		_, p, err := decodedRevision(r.Context(), s.opts.Store, ps.ActiveRevision)
		if err != nil {
			return err
		}
		pl, err := s.pipeline(r.Context(), ps, p)
		if err != nil {
			return err
		}
		out = append(out, pl)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request) error {
	ps, _, p, err := activePipeline(r.Context(), s.opts.Store, r.PathValue("pipeline"))
	if err != nil {
		return err
	}
	out, err := s.pipeline(r.Context(), ps, p)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) listRevisions(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	active, err := s.opts.Store.ActiveRevision(r.Context(), name)
	if err != nil {
		return err
	}
	revs, err := s.opts.Store.Revisions(r.Context(), name)
	if err != nil {
		return err
	}
	out := make([]Revision, 0, len(revs))
	for _, rev := range revs {
		out = append(out, toRevision(rev, active))
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getRevision(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	id, err := strconv.ParseInt(r.PathValue("revision"), 10, 64)
	if err != nil {
		return invalidf("revision %q: want a number", r.PathValue("revision"))
	}
	active, err := s.opts.Store.ActiveRevision(r.Context(), name)
	if err != nil {
		return err
	}
	rev, err := s.opts.Store.Revision(r.Context(), store.RevisionID(id))
	if err != nil {
		return err
	}
	if rev.Pipeline != name {
		return fmt.Errorf("revision %d of %s: %w", id, name, store.ErrNotFound)
	}
	p, err := rev.Decode()
	if err != nil {
		return err
	}
	g := graph(p, nil)
	out := toRevision(rev, active)
	out.Graph = &g
	writeJSON(w, http.StatusOK, out)
	return nil
}

// getRevisionFiles answers a revision's package files as text, for the improve pipeline's drafts (spec section
// 11.3): a file that is not UTF-8 text, a compiled action for one, is named under binary instead.
func (s *Server) getRevisionFiles(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	id, err := strconv.ParseInt(r.PathValue("revision"), 10, 64)
	if err != nil {
		return invalidf("revision %q: want a number", r.PathValue("revision"))
	}
	rev, err := s.opts.Store.Revision(r.Context(), store.RevisionID(id))
	if err != nil {
		return err
	}
	if rev.Pipeline != name {
		return fmt.Errorf("revision %d of %s: %w", id, name, store.ErrNotFound)
	}
	bundle, err := s.opts.Store.Package(r.Context(), rev.PackageHash)
	if err != nil {
		return err
	}
	files, err := pipeline.DecodeBundle(bundle)
	if err != nil {
		return fmt.Errorf("package %.12s: %w", rev.PackageHash, err)
	}
	out := RevisionFiles{PackageHash: rev.PackageHash, Files: map[string]string{}, Binary: []string{}}
	for _, p := range slices.Sorted(maps.Keys(files)) {
		if utf8.Valid(files[p]) {
			out.Files[p] = string(files[p])
		} else {
			out.Binary = append(out.Binary, p)
		}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) listStreams(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	_, _, p, err := activePipeline(r.Context(), s.opts.Store, name)
	if err != nil {
		return err
	}
	counts, err := s.opts.Store.StreamCounts(r.Context(), name, "")
	if err != nil {
		return err
	}
	streams := map[string]*Stream{}
	get := func(name string) *Stream {
		if streams[name] == nil {
			streams[name] = &Stream{Name: name, Producers: []string{}, Consumers: []string{}}
		}
		return streams[name]
	}
	g := graph(p, nil)
	for _, t := range g.Triggers {
		get(t.Stream).Producers = append(get(t.Stream).Producers, pipeline.TriggerStreamPrefix+t.Name)
	}
	for _, st := range g.Steps {
		for _, o := range append(slices.Clone(st.Out), st.DLQ) {
			if o != "" && !slices.Contains(get(o).Producers, st.Name) {
				get(o).Producers = append(get(o).Producers, st.Name)
			}
		}
		for _, in := range st.In {
			get(in).Consumers = append(get(in).Consumers, st.Name)
		}
	}
	for name, n := range counts { // a stream an earlier revision wrote stays listed while it holds items
		get(name).Items = n
	}
	out := make([]Stream, 0, len(streams))
	for _, name := range slices.Sorted(maps.Keys(streams)) {
		out = append(out, *streams[name])
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	if _, err := s.opts.Store.ActiveRevision(r.Context(), name); err != nil {
		return err
	}
	n, err := limit(r)
	if err != nil {
		return err
	}
	cur, err := decodeCursor(r, "runs", 2)
	if err != nil {
		return err
	}
	state, err := enumParam(r, "state", runStates...)
	if err != nil {
		return err
	}
	f := store.RunFilter{Pipeline: name, State: state, Limit: n + 1,
		After: store.RunPosition{StartedAt: time.Unix(0, cur[0]).UTC(), Seq: cur[1]}}
	runs, err := s.opts.Store.Runs(r.Context(), f)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, pageOf(runs, n, toRun, func(r store.Run) string {
		return encodeCursor("runs", r.StartedAt.UnixNano(), r.Seq)
	}))
	return nil
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) error {
	run, err := s.opts.Store.Run(r.Context(), r.PathValue("pipeline"), r.PathValue("run"))
	if err != nil {
		return err
	}
	counts, err := s.opts.Store.StreamCounts(r.Context(), run.Pipeline, run.ID)
	if err != nil {
		return err
	}
	dls, err := s.opts.Store.DeadLetters(r.Context(), store.DeadLetterFilter{Pipeline: run.Pipeline, RunID: run.ID,
		State: store.DeadLetterOpen})
	if err != nil {
		return err
	}
	out := toRun(run)
	out.Items = counts
	open := len(dls)
	out.OpenDeadLetters = &open
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getRunReport(w http.ResponseWriter, r *http.Request) error {
	rep, err := insights.BuildReport(r.Context(), s.opts.Store, r.PathValue("pipeline"), r.PathValue("run"), s.opts.Clock.Now())
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, rep)
	return nil
}

func (s *Server) listRunDeliveries(w http.ResponseWriter, r *http.Request) error {
	run, err := s.opts.Store.Run(r.Context(), r.PathValue("pipeline"), r.PathValue("run"))
	if err != nil {
		return err
	}
	return s.writeDeliveries(w, r, store.DeliveryFilter{Pipeline: run.Pipeline, RunID: run.ID})
}

func (s *Server) writeDeliveries(w http.ResponseWriter, r *http.Request, f store.DeliveryFilter) error {
	state, err := enumParam(r, "state", deliveryStates...)
	if err != nil {
		return err
	}
	f.Step, f.State = r.URL.Query().Get("step"), state
	ds, err := s.opts.Store.Deliveries(r.Context(), f)
	if err != nil {
		return err
	}
	out := make([]Delivery, 0, len(ds))
	for _, d := range ds {
		out = append(out, toDelivery(d))
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// itemFilter evaluates a list's where expression over items, with each item's run as vars and run.
type itemFilter struct {
	s     *Server
	where *expr.ItemExpr
	runs  map[string]itemRun
}

type itemRun struct {
	vars map[string]any
	run  expr.Run
}

func (f *itemFilter) keep(ctx context.Context, it store.Item) (bool, error) {
	if f.where == nil {
		return true, nil
	}
	ir, ok := f.runs[it.RunID]
	if !ok {
		run, err := f.s.opts.Store.Run(ctx, it.Pipeline, it.RunID)
		if err != nil {
			return false, err
		}
		rev, err := f.s.opts.Store.Revision(ctx, run.Revision)
		if err != nil {
			return false, err
		}
		ir = itemRun{vars: run.Vars, run: expr.Run{ID: run.ID, Trigger: run.Trigger, StartedAt: run.StartedAt,
			Revision: rev.PackageHash}}
		f.runs[it.RunID] = ir
	}
	v, err := pipeline.DecodeValue(string(it.Payload))
	if err != nil {
		return false, fmt.Errorf("item %s: %w", it.ID, err)
	}
	ok, err = f.where.Eval(v, expr.Envelope{ID: it.ID, Key: it.Key, TS: it.TS, Step: it.Step}, ir.vars, ir.run)
	// An expression that fails on one item (a field it lacks, a type it does not have) does not match it, as a
	// filter step would not pass it: one odd item must not make the whole list unreadable.
	return ok && err == nil, nil
}

func (s *Server) listItems(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	if _, err := s.opts.Store.ActiveRevision(r.Context(), name); err != nil {
		return err
	}
	n, err := limit(r)
	if err != nil {
		return err
	}
	cur, err := decodeCursor(r, "items", 1)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	filter := &itemFilter{s: s, runs: map[string]itemRun{}}
	if src := q.Get("where"); src != "" {
		if filter.where, err = expr.CompileItem(src); err != nil {
			return invalidf("where: %s", err.Error())
		}
	}
	f := store.ItemFilter{Pipeline: name, Stream: q.Get("stream"), RunID: q.Get("run"), After: cur[0], Limit: n + 1}
	if filter.where != nil {
		f.Limit = MaxLimit
	}
	page := Page[Item]{Items: []Item{}}
	for scanned := 0; ; {
		items, err := s.opts.Store.Items(r.Context(), f)
		if err != nil {
			return err
		}
		for _, it := range items {
			ok, err := filter.keep(r.Context(), it)
			if err != nil {
				return err
			}
			if !ok {
				f.After = it.Seq
				continue
			}
			if len(page.Items) == n { // one more matches: there is a next page
				page.Next = encodeCursor("items", f.After)
				writeJSON(w, http.StatusOK, page)
				return nil
			}
			page.Items = append(page.Items, toItem(it))
			f.After = it.Seq
		}
		scanned += len(items)
		if len(items) < f.Limit { // without a filter, a full read of n+1 has returned above
			break
		}
		if scanned >= whereScanBound {
			page.Next = encodeCursor("items", f.After)
			break
		}
	}
	writeJSON(w, http.StatusOK, page)
	return nil
}

// item returns the item the request names, of the deployment's own.
func (s *Server) item(ctx context.Context, pipelineName, id string) (store.Item, error) {
	items, err := s.opts.Store.Items(ctx, store.ItemFilter{Pipeline: pipelineName, IDs: []string{id}})
	if err != nil {
		return store.Item{}, err
	}
	if len(items) == 0 {
		return store.Item{}, fmt.Errorf("item %s: %w", id, store.ErrNotFound)
	}
	return items[0], nil
}

// readItem returns the item the request names: the deployment's own, else one of the evidence bundles imported for
// its pipeline (spec section 11.2), with those bundles' items, the history its lineage is read in (nil for its own).
func (s *Server) readItem(ctx context.Context, pipelineName, id string) (store.Item, *evidence.Items, error) {
	it, err := s.item(ctx, pipelineName, id)
	if !errors.Is(err, store.ErrNotFound) || s.opts.Artifacts == nil {
		return it, nil, err
	}
	imported, lerr := evidence.LoadItems(ctx, s.opts.Store, s.opts.Artifacts, pipelineName, s.opts.Clock.Now())
	if lerr != nil {
		return store.Item{}, nil, lerr
	}
	if it, ok := imported.ByID[id]; ok {
		return it, imported, nil
	}
	return store.Item{}, nil, err
}

func (s *Server) getItem(w http.ResponseWriter, r *http.Request) error {
	it, _, err := s.readItem(r.Context(), r.PathValue("pipeline"), r.PathValue("item"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toItem(it))
	return nil
}

func (s *Server) getLineage(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	it, imported, err := s.readItem(r.Context(), name, r.PathValue("item"))
	if err != nil {
		return err
	}
	// An imported item's lineage is read in the bundles that hold it, never in the deployment's own items.
	items := s.opts.Store.Items
	if imported != nil {
		items = func(_ context.Context, f store.ItemFilter) ([]store.Item, error) {
			var out []store.Item
			for _, id := range f.IDs {
				if it, ok := imported.ByID[id]; ok {
					out = append(out, it)
				}
			}
			for _, p := range f.Parents {
				out = append(out, imported.Children[p]...)
			}
			return out, nil
		}
	}
	depth := defaultDepth
	if v := r.URL.Query().Get("depth"); v != "" {
		if depth, err = strconv.Atoi(v); err != nil || depth < 1 || depth > maxDepth {
			return invalidf("depth %q: want 1 to %d", v, maxDepth)
		}
	}
	out := Lineage{Item: it.ID, Up: []LineageNode{}, Down: []LineageNode{}}
	// Items are named by pipeline and id: a copy published through an imported stream has its parent in the
	// producer's pipeline, and its children in the consumers' (spec section 4.12).
	type ref struct{ pipeline, id string }
	seen := map[ref]bool{{it.Pipeline, it.ID}: true}
	parentsOf := func(it store.Item) []ref {
		p := cmp.Or(it.ParentPipeline, it.Pipeline)
		refs := make([]ref, len(it.Parents))
		for i, id := range it.Parents {
			refs[i] = ref{p, id}
		}
		return refs
	}
	// byPipeline groups refs by pipeline, in the order the pipelines come.
	byPipeline := func(refs []ref) ([]string, map[string][]string) {
		groups := map[string][]string{}
		var order []string
		for _, r := range refs {
			if slices.Contains(groups[r.pipeline], r.id) {
				continue
			}
			if _, ok := groups[r.pipeline]; !ok {
				order = append(order, r.pipeline)
			}
			groups[r.pipeline] = append(groups[r.pipeline], r.id)
		}
		return order, groups
	}
	// Up: each generation's parents, read together per pipeline.
	frontier := parentsOf(it)
	for range depth {
		order, groups := byPipeline(slices.DeleteFunc(frontier, func(r ref) bool { return seen[r] }))
		if len(order) == 0 {
			break
		}
		var next []ref
		for _, p := range order {
			parents, err := items(r.Context(), store.ItemFilter{Pipeline: p, IDs: groups[p]})
			if err != nil {
				return err
			}
			for _, item := range parents {
				if len(out.Up) == maxLineageNodes {
					out.Truncated = true
					break
				}
				seen[ref{item.Pipeline, item.ID}] = true
				out.Up = append(out.Up, toLineageNode(item))
				next = append(next, parentsOf(item)...)
			}
		}
		frontier = next
		if out.Truncated { // the cap ends the walk, not just this generation
			break
		}
	}
	if len(frontier) > 0 && !out.Truncated {
		out.Truncated = slices.ContainsFunc(frontier, func(r ref) bool { return !seen[r] })
	}
	// Down: each generation's children, read together per pipeline: those in the item's pipeline, and the copies in
	// the pipelines importing its stream.
	level := []ref{{it.Pipeline, it.ID}}
	capped := false
	for d := 0; d < depth && len(level) > 0; d++ {
		var next []ref
		order, groups := byPipeline(level)
		for _, p := range order {
			children, err := items(r.Context(), store.ItemFilter{Parents: groups[p], ParentPipeline: p})
			if err != nil {
				return err
			}
			for _, c := range children {
				cr := ref{c.Pipeline, c.ID}
				if seen[cr] {
					continue
				}
				if len(out.Down) == maxLineageNodes {
					out.Truncated, capped = true, true
					break
				}
				seen[cr] = true
				out.Down = append(out.Down, toLineageNode(c))
				next = append(next, cr)
			}
			if capped {
				break
			}
		}
		level = next
		if capped {
			break
		}
		if d == depth-1 && len(level) > 0 {
			out.Truncated = true // there may be more below
		}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) listItemDeliveries(w http.ResponseWriter, r *http.Request) error {
	it, err := s.item(r.Context(), r.PathValue("pipeline"), r.PathValue("item"))
	if err != nil {
		return err
	}
	return s.writeDeliveries(w, r, store.DeliveryFilter{Pipeline: it.Pipeline, ItemID: it.ID})
}

// listItemAudits lists the fallback audits of the deliveries that read an item (spec section 11.5).
func (s *Server) listItemAudits(w http.ResponseWriter, r *http.Request) error {
	it, err := s.item(r.Context(), r.PathValue("pipeline"), r.PathValue("item"))
	if err != nil {
		return err
	}
	as, err := s.opts.Store.Audits(r.Context(), store.AuditFilter{Pipeline: it.Pipeline, ItemID: it.ID})
	if err != nil {
		return err
	}
	out := make([]Audit, len(as))
	for i, a := range as {
		out[i] = toAudit(a)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getItemArtifact(w http.ResponseWriter, r *http.Request) error {
	it, err := s.item(r.Context(), r.PathValue("pipeline"), r.PathValue("item"))
	if err != nil {
		return err
	}
	name := r.PathValue("name")
	i := slices.IndexFunc(it.Artifacts, func(a store.ArtifactRef) bool { return a.Name == name })
	if i < 0 {
		return fmt.Errorf("artifact %s of item %s: %w", name, it.ID, store.ErrNotFound)
	}
	return s.serveArtifact(w, r, it.Pipeline, it.ParentPipeline, it.Artifacts[i])
}

// delivery returns the delivery the request names.
func (s *Server) delivery(ctx context.Context, pipelineName, id string) (store.Delivery, error) {
	ds, err := s.opts.Store.Deliveries(ctx, store.DeliveryFilter{Pipeline: pipelineName, ID: id})
	if err != nil {
		return store.Delivery{}, err
	}
	if len(ds) == 0 {
		return store.Delivery{}, fmt.Errorf("delivery %s: %w", id, store.ErrNotFound)
	}
	return ds[0], nil
}

// attemptKinds are the events that start or end a delivery's attempt.
var attemptKinds = []string{
	store.EventDeliveryLeased, store.EventDeliveryDone, store.EventDeliveryRetry, store.EventDeliveryDead,
	store.EventDeliveryFiltered, store.EventDeliveryCancelled, store.EventLeaseExpired, store.EventReviewParked,
}

// attemptEvent is what an attempt reads of its events' data.
type attemptEvent struct {
	DeliveryID string  `json:"deliveryId"`
	Attempt    int     `json:"attempt"`
	Owner      string  `json:"owner"`
	Class      string  `json:"class"`
	Error      string  `json:"error"`
	CostUSD    float64 `json:"costUsd"`
	ChargedUSD float64 `json:"chargedUsd"`
	WorkMs     int64   `json:"workMs"`
}

// attempts reads a delivery's attempts from its run's events, oldest first, with the artifacts each kept.
func (s *Server) attempts(ctx context.Context, d store.Delivery) ([]Attempt, error) {
	evs, err := s.opts.Store.PipelineEvents(ctx, store.EventFilter{Pipeline: d.Pipeline, RunID: d.RunID, Kinds: attemptKinds})
	if err != nil {
		return nil, err
	}
	arts, err := s.opts.Store.AttemptArtifacts(ctx, d.Pipeline, d.ID)
	if err != nil {
		return nil, err
	}
	var out []Attempt
	byNumber := map[int]int{} // attempt number to index in out
	for _, ev := range evs {
		var a attemptEvent
		if err := json.Unmarshal(ev.Data, &a); err != nil {
			return nil, fmt.Errorf("event %d: %w", ev.Seq, err)
		}
		if a.DeliveryID != d.ID {
			continue
		}
		i, ok := byNumber[a.Attempt]
		if !ok { // attempt numbers are never reused, a dead-letter replay included
			out = append(out, Attempt{Attempt: a.Attempt, Outcome: "running", Artifacts: []store.ArtifactRef{}})
			i = len(out) - 1
			byNumber[a.Attempt] = i
		}
		at := &out[i]
		ts := ev.TS
		switch ev.Kind {
		case store.EventDeliveryLeased:
			at.Owner, at.LeasedAt = a.Owner, &ts
			continue
		case store.EventDeliveryDone:
			at.Outcome = "done"
		case store.EventDeliveryRetry:
			at.Outcome = "retry"
		case store.EventDeliveryDead:
			at.Outcome = "dead"
		case store.EventDeliveryFiltered:
			at.Outcome = "filtered"
		case store.EventDeliveryCancelled:
			at.Outcome = "cancelled"
		case store.EventLeaseExpired:
			at.Outcome, at.ErrorClass, a.CostUSD = "leaseExpired", store.ClassLeaseExpired, a.ChargedUSD
		case store.EventReviewParked:
			at.Outcome = "awaitingReview"
		}
		at.EndedAt, at.WorkMs = &ts, at.WorkMs+a.WorkMs
		at.CostUSD += a.CostUSD
		at.ErrorClass, at.Error = cmp.Or(a.Class, at.ErrorClass), cmp.Or(a.Error, at.Error)
	}
	for _, a := range arts {
		if i, ok := byNumber[a.Attempt]; ok {
			out[i].Artifacts = append(out[i].Artifacts, a.ArtifactRef)
		}
	}
	return out, nil
}

func (s *Server) getDelivery(w http.ResponseWriter, r *http.Request) error {
	d, err := s.delivery(r.Context(), r.PathValue("pipeline"), r.PathValue("delivery"))
	if err != nil {
		return err
	}
	out := toDelivery(d)
	if out.Attempts, err = s.attempts(r.Context(), d); err != nil {
		return err
	}
	out.Attempts = orEmpty(out.Attempts)
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getAttemptArtifact(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	d, err := s.delivery(r.Context(), name, r.PathValue("delivery"))
	if err != nil {
		return err
	}
	attempt, err := strconv.Atoi(r.PathValue("attempt"))
	if err != nil {
		return invalidf("attempt %q: want a number", r.PathValue("attempt"))
	}
	arts, err := s.opts.Store.AttemptArtifacts(r.Context(), name, d.ID)
	if err != nil {
		return err
	}
	for _, a := range arts {
		if a.Attempt == attempt && a.Name == r.PathValue("name") {
			return s.serveArtifact(w, r, name, "", a.ArtifactRef)
		}
	}
	return fmt.Errorf("artifact %s of attempt %d of delivery %s: %w", r.PathValue("name"), attempt, d.ID,
		store.ErrNotFound)
}

// inlineTypes are the media types an artifact is shown as in the browser; any other downloads. None of them runs
// script, and the sandbox policy keeps it so whatever a browser makes of the bytes.
var inlineTypes = []string{"application/json", "application/pdf", "image/gif", "image/jpeg", "image/png",
	"image/webp", "text/plain"}

// serveArtifact answers an artifact's content with its media type. Artifacts are what actions wrote: they are
// served under a sandbox policy, without sniffing, and as an attachment unless their type is a passive one.
func (s *Server) serveArtifact(w http.ResponseWriter, r *http.Request, pipelineName, from string,
	ref store.ArtifactRef,
) error {
	if s.opts.Artifacts == nil {
		return fmt.Errorf("artifacts: no artifact store: %w", errUnavailable)
	}
	f, err := engine.OpenArtifact(r.Context(), s.opts.Artifacts, pipelineName, from, ref)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("artifact %s: %w: %w", ref.Name, err, store.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("artifact %s: %w", ref.Name, err)
	}
	defer func() { _ = f.Close() }()
	if artifacts.Compressed(ref) {
		return serveDecompressed(w, ref, f)
	}
	setArtifactHeaders(w, ref, ref.Type)
	// Ranges, conditional requests on the ETag, and the length of the bytes on disk.
	http.ServeContent(w, r, "", time.Time{}, f)
	return nil
}

// setArtifactHeaders sets the headers every artifact is served with: its media type typ, inline only when passive,
// under the sandbox policy, cached by its content address.
func setArtifactHeaders(w http.ResponseWriter, ref store.ArtifactRef, typ string) {
	base, _, _ := strings.Cut(typ, ";")
	disposition := "attachment"
	if slices.Contains(inlineTypes, strings.TrimSpace(base)) {
		disposition = "inline"
	}
	h := w.Header()
	h.Set("Content-Type", typ)
	h.Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": ref.Name}))
	h.Set("Content-Security-Policy", artifactPolicy)
	h.Set("X-Frame-Options", "SAMEORIGIN")
	h.Set("Cache-Control", "private, max-age=31536000, immutable") // content-addressed
	h.Set("ETag", `"`+ref.SHA256+`"`)
}

// serveDecompressed answers an artifact stored compressed (a transcript, spec section 7.2) decompressed, its type
// read from its name, without ranges: the bytes on disk are not the bytes served.
func serveDecompressed(w http.ResponseWriter, ref store.ArtifactRef, f io.Reader) error {
	r, err := artifacts.Decompressed(ref, f)
	if err != nil {
		return err
	}
	setArtifactHeaders(w, ref, cmp.Or(mime.TypeByExtension(path.Ext(ref.Name)), "application/octet-stream"))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, r) // the status is sent: a failure mid-copy can only cut the body short
	return nil
}

func (s *Server) listDeadLetters(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	if _, err := s.opts.Store.ActiveRevision(r.Context(), name); err != nil {
		return err
	}
	f := store.DeadLetterFilter{Pipeline: name, State: store.DeadLetterOpen}
	switch st := r.URL.Query().Get("state"); st {
	case "":
	case "all":
		f.State = ""
	case string(store.DeadLetterOpen), string(store.DeadLetterReplayed), string(store.DeadLetterDiscarded):
		f.State = store.DeadLetterState(st)
	default:
		return invalidf("state %q: want open, replayed, discarded or all", st)
	}
	dls, err := s.opts.Store.DeadLetters(r.Context(), f)
	if err != nil {
		return err
	}
	groups := groupDeadLetters(dls)
	out := make([]DeadLetterGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// deadLetterFingerprint groups the dead letters of one step and kind whose errors are the same once what varies
// between occurrences (ids, numbers, paths) is taken out (insights.Normalise).
func deadLetterFingerprint(dl store.DeadLetter) (string, string) {
	norm := insights.Normalise(dl.Error)
	sum := sha256.Sum256([]byte(dl.OriginStep + "\x00" + string(dl.Kind) + "\x00" + norm))
	return hex.EncodeToString(sum[:8]), norm
}

// groupDeadLetters groups dead letters by fingerprint, largest group first, then by step and fingerprint.
func groupDeadLetters(dls []store.DeadLetter) []*DeadLetterGroup {
	byFP := map[string]*DeadLetterGroup{}
	var groups []*DeadLetterGroup
	for _, dl := range dls {
		fp, norm := deadLetterFingerprint(dl)
		g := byFP[fp]
		if g == nil {
			g = &DeadLetterGroup{Fingerprint: fp, Step: dl.OriginStep, Kind: string(dl.Kind), Error: norm,
				FirstSeen: dl.CreatedAt}
			byFP[fp] = g
			groups = append(groups, g)
		}
		g.Count++
		if dl.CreatedAt.Before(g.FirstSeen) {
			g.FirstSeen = dl.CreatedAt
		}
		if dl.CreatedAt.After(g.LastSeen) {
			g.LastSeen = dl.CreatedAt
		}
		g.DeadLetters = append(g.DeadLetters, DeadLetter{ID: dl.ID, RunID: dl.RunID, OriginDelivery: dl.OriginDelivery,
			Stream: dl.Stream, State: string(dl.State), Error: dl.Error, CreatedAt: dl.CreatedAt})
	}
	slices.SortStableFunc(groups, func(a, b *DeadLetterGroup) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.Step, b.Step), cmp.Compare(a.Fingerprint, b.Fingerprint))
	})
	return groups
}

func (s *Server) listLessons(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	_, rev, p, err := activePipeline(r.Context(), s.opts.Store, name)
	if err != nil {
		return err
	}
	bundle, err := s.opts.Store.Package(r.Context(), rev.PackageHash)
	if err != nil {
		return err
	}
	pkg := &pipeline.Package{Pipeline: p}
	if len(bundle) > 0 {
		if pkg.Files, err = pipeline.DecodeBundle(bundle); err != nil {
			return fmt.Errorf("package %s: %w", rev.PackageHash, err)
		}
	}
	out := []StepLessons{}
	for _, step := range slices.Sorted(maps.Keys(p.Steps)) {
		l, _, err := pipeline.StepLessons(pkg, p.Steps[step])
		if err != nil {
			return fmt.Errorf("step %s: %w", step, err)
		}
		if l != nil {
			out = append(out, StepLessons{Step: step, Rules: orEmpty(l.Rules), Examples: orEmpty(l.Examples)})
		}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getReview(w http.ResponseWriter, r *http.Request) error {
	rv, err := s.opts.Store.Review(r.Context(), r.PathValue("pipeline"), r.PathValue("delivery"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toReview(rv))
	return nil
}

func (s *Server) getSample(w http.ResponseWriter, r *http.Request) error {
	sm, err := s.opts.Store.ReviewSample(r.Context(), r.PathValue("pipeline"), r.PathValue("delivery"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toSample(sm))
	return nil
}

func (s *Server) listInbox(w http.ResponseWriter, r *http.Request) error {
	n, err := limit(r)
	if err != nil {
		return err
	}
	cur, err := decodeCursor(r, "reviews", 1)
	if err != nil {
		return err
	}
	state, err := enumParam(r, "state", reviewStates...)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	reviews, err := s.opts.Store.Reviews(r.Context(), store.ReviewFilter{Pipeline: q.Get("pipeline"), Inbox: q.Get("inbox"),
		Step: q.Get("step"), State: state, After: cur[0], Limit: n + 1})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, pageOf(reviews, n, toReview, func(r store.Review) string {
		return encodeCursor("reviews", r.Seq)
	}))
	return nil
}

func (s *Server) listSamples(w http.ResponseWriter, r *http.Request) error {
	n, err := limit(r)
	if err != nil {
		return err
	}
	cur, err := decodeCursor(r, "samples", 1)
	if err != nil {
		return err
	}
	state, err := enumParam(r, "state", sampleStates...)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	samples, err := s.opts.Store.ReviewSamples(r.Context(), store.SampleFilter{Pipeline: q.Get("pipeline"),
		Inbox: q.Get("inbox"), Step: q.Get("step"), State: state, After: cur[0], Limit: n + 1})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, pageOf(samples, n, toSample, func(s store.ReviewSample) string {
		return encodeCursor("samples", s.Seq)
	}))
	return nil
}

func (s *Server) listInsights(w http.ResponseWriter, r *http.Request) error {
	state, err := enumParam(r, "state", insightStates...)
	if err != nil {
		return err
	}
	ins, err := evidence.ListInsights(r.Context(), s.opts.Store, s.opts.Artifacts,
		store.InsightFilter{Pipeline: r.URL.Query().Get("pipeline"), State: state}, s.opts.Clock.Now())
	if err != nil {
		return err
	}
	out := make([]Insight, len(ins))
	for i, in := range ins {
		out[i] = InsightOf(in)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// InsightOf is an insight as the API and millstream insights --json show it.
func InsightOf(in evidence.ImportedInsight) Insight {
	return Insight{Pipeline: in.Pipeline, ID: in.ID, Kind: in.Kind, Scope: in.Scope, Severity: in.Severity,
		State: string(in.State), Summary: insights.Summary(in.Insight), WindowStart: in.WindowStart,
		WindowEnd: in.WindowEnd, OpenedAt: in.OpenedAt, UpdatedAt: in.UpdatedAt, Evidence: in.Evidence,
		Imported: in.Bundle}
}

// timeRange reads the from and to query parameters (RFC 3339), or since (a duration back from now): the last
// hour by default.
func (s *Server) timeRange(r *http.Request) (time.Time, time.Time, error) {
	q := r.URL.Query()
	now := s.opts.Clock.Now()
	from, to := now.Add(-time.Hour), now
	if v := q.Get("since"); v != "" {
		d, err := pipeline.ParseDuration(v)
		if err != nil {
			return from, to, invalidf("since %q: %s", v, err.Error())
		}
		from = now.Add(-d)
	}
	for _, p := range []struct {
		name string
		t    *time.Time
	}{{"from", &from}, {"to", &to}} {
		if v := q.Get(p.name); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return from, to, invalidf("%s %q: want an RFC 3339 time", p.name, v)
			}
			*p.t = t
		}
	}
	if to.Before(from) {
		return from, to, invalidf("to is before from")
	}
	return from, to, nil
}

func (s *Server) getMetrics(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	name := q.Get("name")
	if name == "" {
		return invalidf("name: the metric is required")
	}
	from, to, err := s.timeRange(r)
	if err != nil {
		return err
	}
	rollups, err := s.opts.Store.MetricRollups(r.Context(), store.MetricFilter{Pipeline: q.Get("pipeline"), Name: name,
		Since: from.Truncate(time.Minute)})
	if err != nil {
		return err
	}
	groupBy := q.Get("groupBy")
	type key struct {
		group  string
		minute time.Time
	}
	points := map[key]*MetricPoint{}
	groups := map[string][]time.Time{}
	for _, ru := range rollups {
		if ru.Minute.After(to) {
			continue
		}
		g := name
		if groupBy != "" {
			g = ru.Labels[groupBy]
		}
		k := key{g, ru.Minute}
		p := points[k]
		if p == nil {
			p = &MetricPoint{T: ru.Minute, Min: ru.Min, Max: ru.Max}
			points[k] = p
			groups[g] = append(groups[g], ru.Minute)
		}
		p.Count += ru.Count
		p.Sum += ru.Sum
		p.Min, p.Max = min(p.Min, ru.Min), max(p.Max, ru.Max)
	}
	out := make([]MetricSeries, 0, len(groups))
	for _, g := range slices.Sorted(maps.Keys(groups)) {
		minutes := groups[g]
		slices.SortFunc(minutes, func(a, b time.Time) int { return a.Compare(b) })
		series := MetricSeries{Name: name, Group: g, Points: make([]MetricPoint, 0, len(minutes))}
		for _, m := range minutes {
			series.Points = append(series.Points, *points[key{g, m}])
		}
		out = append(out, series)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) listMetricNames(w http.ResponseWriter, r *http.Request) error {
	from, _, err := s.timeRange(r)
	if err != nil {
		return err
	}
	rollups, err := s.opts.Store.MetricRollups(r.Context(), store.MetricFilter{Pipeline: r.URL.Query().Get("pipeline"),
		Since: from.Truncate(time.Minute)})
	if err != nil {
		return err
	}
	names := make([]string, 0, len(rollups))
	for _, ru := range rollups {
		names = append(names, ru.Name)
	}
	slices.Sort(names)
	names = slices.Compact(names)
	writeJSON(w, http.StatusOK, names)
	return nil
}

// environment is the deployment's environment: Options.Environment, development when unset.
func (s *Server) environment() string { return cmp.Or(s.opts.Environment, pipeline.ModeDevelopment) }

func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) error {
	leased, err := s.opts.Store.Deliveries(r.Context(), store.DeliveryFilter{State: store.DeliveryLeased})
	if err != nil {
		return err
	}
	held := map[string]int{}
	for _, d := range leased {
		held[d.LeaseOwner]++
	}
	id := s.opts.Identity
	self := EngineInfo{State: stateActive, ID: id.ID, Version: s.opts.Version, Since: timePtr(id.Since),
		Epoch: id.Epoch, Environment: s.environment()}
	if s.opts.Standby {
		self.State, self.Epoch = stateStandby, 0
	}
	out := Nodes{Engine: self, Nodes: []Node{}}
	if s.opts.NotificationFailures != nil {
		out.Engine.NotificationsFailed = s.opts.NotificationFailures()
	}
	if s.opts.Deployment != nil {
		if out.Engines, err = s.opts.Deployment.Engines(r.Context()); err != nil {
			return err
		}
	} else {
		out.Engines = []DeploymentEngine{{ID: self.ID, State: self.State, Version: self.Version, Since: self.Since,
			Epoch: self.Epoch}}
	}
	out.Engines = orEmpty(out.Engines)
	for _, n := range s.opts.Nodes {
		n.Leases = held[n.ID]
		n.Labels, n.Runtimes = nonNilMap(n.Labels), orEmpty(n.Runtimes)
		n.State, n.Health = string(store.NodeActive), engine.HealthHealthy
		out.Nodes = append(out.Nodes, n)
	}
	remote, err := s.opts.Store.Nodes(r.Context())
	if err != nil {
		return err
	}
	now := s.opts.Clock.Now()
	for _, n := range remote {
		out.Nodes = append(out.Nodes, Node{ID: n.ID, Version: n.Version, Behind: n.Version != s.opts.Version,
			Slots: n.Slots, Leases: held[n.ID], Labels: nonNilMap(n.Labels), Runtimes: orEmpty(n.Runtimes),
			State: string(n.State), Health: engine.Health(n.LastHeartbeat, now, cmp.Or(s.opts.LeaseTTL, engine.DefaultLeaseTTL)), LastHeartbeat: timePtr(n.LastHeartbeat)})
	}
	if s.opts.Engine != nil {
		un, err := s.opts.Engine.Unschedulable(r.Context())
		if err != nil {
			return err
		}
		out.Unschedulable = make([]UnschedulableStep, 0, len(un))
		for _, u := range un {
			out.Unschedulable = append(out.Unschedulable, UnschedulableStep{Pipeline: u.Pipeline, Step: u.Step,
				Pending: u.Pending, Needs: u.Placement.String()})
		}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getPurge(w http.ResponseWriter, r *http.Request) error {
	p, err := s.opts.Store.Purge(r.Context(), r.PathValue("purge"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, ToPurge(p))
	return nil
}
