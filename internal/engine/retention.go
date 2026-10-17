package engine

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/schema"
	"github.com/monolithiclab/millstream/internal/store"
)

// DefaultRetentionInterval paces the retention job without retention.every.
const DefaultRetentionInterval = time.Hour

// ArtifactGrace is how long the artifact sweep leaves a file nothing references after it was last stored, and a
// package directory or a temporary file after it was written: a delivery in flight may have stored the file and
// not yet reported. A purge does not wait.
const ArtifactGrace = 24 * time.Hour

// retentionLoop resumes the purges a crash interrupted, then runs the retention job every retention.every until ctx
// is cancelled.
func (e *Engine) retentionLoop(ctx context.Context) {
	if err := e.ResumePurges(ctx); err != nil && ctx.Err() == nil {
		e.log.Error("resume purges", "error", err.Error())
	}
	tick := time.NewTicker(e.opts.Retention.Every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if err := e.Retain(ctx); err != nil && ctx.Err() == nil {
			e.log.Error("retention", "error", err.Error())
		}
	}
}

// Retain runs the retention job now (spec section 9.3): it resumes the purges not done, applies the retention of
// every stream of the pipelines' active revisions and the environment file's retention of events, transcripts and
// rollups, a batch at a time, sweeps the artifact files nothing references, and prunes the package directory and the
// engine's caches of the revisions no longer active or used by an open run. Run calls it every retention.every;
// "run --once" calls it once its runs settled. A step that fails is logged and the others still run.
func (e *Engine) Retain(ctx context.Context) error {
	e.retainMu.Lock()
	defer e.retainMu.Unlock()
	var errs []error
	if err := e.ResumePurges(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := e.retain(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := e.sweepArtifacts(ctx, nil, ArtifactGrace); err != nil {
		errs = append(errs, fmt.Errorf("artifact sweep: %w", err))
	}
	if err := e.prune(ctx); err != nil {
		errs = append(errs, fmt.Errorf("prune: %w", err))
	}
	return errors.Join(errs...)
}

// retain applies the retention in passes of the store's unit until none is left with more to do.
func (e *Engine) retain(ctx context.Context) error {
	req := store.RetentionRequest{Events: e.opts.Retention.Events, Transcripts: e.opts.Retention.Transcripts,
		Rollups: e.opts.Retention.Rollups}
	pipelines, err := e.st.Pipelines(ctx)
	if err != nil {
		return fmt.Errorf("retention: %w", err)
	}
	for _, ps := range pipelines {
		rev, err := e.revision(ctx, ps.ActiveRevision)
		if err != nil {
			return fmt.Errorf("retention of %s: %w", ps.Name, err)
		}
		for _, name := range slices.Sorted(maps.Keys(rev.p.Streams)) {
			if age := rev.p.Streams[name].Retention; age > 0 {
				req.Streams = append(req.Streams, store.StreamRetention{Pipeline: ps.Name, Stream: name, Age: age})
			}
		}
	}
	var total store.RetentionResult
	for {
		res, err := e.st.Retain(ctx, req)
		if err != nil {
			return fmt.Errorf("retention: %w", err)
		}
		total.Add(res)
		if !res.More {
			break
		}
	}
	if total != (store.RetentionResult{}) {
		e.log.Info("retention", "items", total.Items, "events", total.Events, "transcripts", total.Transcripts,
			"rollups", total.Rollups)
	}
	return nil
}

// PurgeRequest asks for a purge (spec section 9.3): of the items with Key, or of the item Item, in Pipeline or in
// every pipeline. RequestID names it (generated when empty): the purge's id is ids.Purge of it, so a request
// repeated after a crash finds the purge it started.
type PurgeRequest struct {
	Key, Item, Pipeline, RequestID string
}

// Purge erases what req names: StartPurge fences the lineage, then the purge runs to the end, its artifact files
// swept. It returns the purge, and created false when the request id named one already started, which it finishes
// if it is not done.
func (e *Engine) Purge(ctx context.Context, req PurgeRequest) (store.Purge, bool, error) {
	id := ids.Purge(cmp.Or(req.RequestID, NewRequestID(e.opts.Now())))
	p, created, err := e.st.StartPurge(ctx, store.PurgeRequest{ID: id, Key: req.Key, Item: req.Item,
		Pipeline: req.Pipeline})
	if err != nil {
		return p, false, err
	}
	if created {
		e.log.Info("purge started", "purgeId", p.ID, "scope", string(p.Scope), "items", p.Items,
			"pipelines", strings.Join(p.Pipelines, ","))
	}
	p, err = e.runPurge(ctx, p)
	return p, created, err
}

// ResumePurges finishes every purge not done: those a crash or a stopped engine interrupted.
func (e *Engine) ResumePurges(ctx context.Context) error {
	ps, err := e.st.Purges(ctx)
	if err != nil {
		return fmt.Errorf("resume purges: %w", err)
	}
	var errs []error
	for _, p := range ps {
		if _, err := e.runPurge(ctx, p); err != nil {
			errs = append(errs, fmt.Errorf("purge %s: %w", p.ID, err))
		}
	}
	return errors.Join(errs...)
}

// runPurge takes a purge from where it stands to done: its erasing batches, then the sweep of the artifact files
// of its pipelines that nothing references any more, without grace, then its end.
func (e *Engine) runPurge(ctx context.Context, p store.Purge) (store.Purge, error) {
	var err error
	for p.State == store.PurgeErasing {
		if p, err = e.st.ErasePurge(ctx, p.ID); err != nil {
			return p, err
		}
	}
	if p.State != store.PurgeSweeping {
		return p, nil
	}
	only := map[string]bool{}
	for _, name := range p.Pipelines {
		only[name] = true
	}
	if err := e.sweepArtifacts(ctx, only, 0); err != nil {
		return p, fmt.Errorf("sweep: %w", err)
	}
	if p, err = e.st.FinishPurge(ctx, p.ID); err != nil {
		return p, err
	}
	e.log.Info("purge done", "purgeId", p.ID, "items", p.Erased)
	return p, nil
}

// files is the engine's artifact store as the sweep sees it, nil without one.
func (e *Engine) files() artifacts.Files {
	if e.opts.ArtifactFiles != nil {
		return e.opts.ArtifactFiles
	}
	f, _ := e.opts.Artifacts.(artifacts.Files)
	return f
}

// sweepArtifacts deletes the files of the artifact entries nothing references (of the pipelines in only, every one
// when nil), then the entries. A file another pipeline references without holding it, a copy an import made that
// was never opened (spec section 9.2), is copied into that pipeline's prefix first. With a grace, a file stored
// again within it is left for a later sweep, and the files no entry names (an attempt's outputs whose item already
// existed, a crash between a file's deletion and its entry's) are deleted once older than it.
func (e *Engine) sweepArtifacts(ctx context.Context, only map[string]bool, grace time.Duration) error {
	all, err := e.st.Artifacts(ctx, "")
	if err != nil {
		return err
	}
	files, now := e.files(), e.opts.Now()
	held := map[string][]string{} // pipelines referencing each hash
	known := map[artifacts.File]bool{}
	for _, a := range all {
		known[artifacts.File{Pipeline: a.Pipeline, Name: a.SHA256}] = true
		if a.Refcount > 0 {
			held[a.SHA256] = append(held[a.SHA256], a.Pipeline)
		}
	}
	var gone []store.ArtifactKey
	for _, a := range all {
		if a.Refcount > 0 || (only != nil && !only[a.Pipeline]) {
			continue
		}
		if files != nil {
			if grace > 0 {
				if t, err := files.ModTime(ctx, a.Pipeline, a.SHA256); err == nil && now.Sub(t) < grace {
					continue
				}
			}
			if err := keepReferenced(ctx, files, a, held[a.SHA256]); err != nil {
				return err
			}
			if err := files.Remove(ctx, a.Pipeline, a.SHA256); err != nil {
				return err
			}
		}
		gone = append(gone, store.ArtifactKey{Pipeline: a.Pipeline, SHA256: a.SHA256})
	}
	if _, err := e.st.DeleteArtifacts(ctx, gone); err != nil {
		return err
	}
	if files == nil || grace <= 0 {
		return nil
	}
	stored, err := files.List(ctx)
	if err != nil {
		return err
	}
	for _, f := range stored {
		if !known[artifacts.File{Pipeline: f.Pipeline, Name: f.Name}] && now.Sub(f.ModTime) >= grace {
			if err := files.Remove(ctx, f.Pipeline, f.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// keepReferenced copies an artifact about to be deleted into the prefix of each other pipeline referencing it that
// does not hold its file yet.
func keepReferenced(ctx context.Context, files artifacts.Files, a store.Artifact, holders []string) error {
	for _, to := range holders {
		if _, err := files.ModTime(ctx, to, a.SHA256); !errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err := files.Copy(ctx, a.Pipeline, to, a.SHA256); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// prune forgets what the engine holds of the revisions that are neither a pipeline's active one nor that of a run
// not yet settled: their decoded revisions, resolved actions, lessons, compiled builtins and trigger input schemas and, when the engine
// loads packages from the store, their packages and the directories it wrote them to. They come back from the store
// if a rollback or a replay needs them again.
func (e *Engine) prune(ctx context.Context) error {
	live, err := e.liveRevisions(ctx)
	if err != nil {
		return err
	}
	hashes := map[string]bool{}
	for _, h := range live {
		hashes[h] = true
	}
	e.mu.Lock()
	maps.DeleteFunc(e.revs, func(id store.RevisionID, _ *revision) bool { _, ok := live[id]; return !ok })
	maps.DeleteFunc(e.actions, func(k actionKey, _ *resolvedAction) bool { return !hashes[k.hash] })
	maps.DeleteFunc(e.lessonsC, func(k actionKey, _ lessonsEntry) bool { return !hashes[k.hash] })
	maps.DeleteFunc(e.builtins, func(k builtinKey, _ *builtin) bool { return !hashes[k.hash] })
	maps.DeleteFunc(e.triggerSchemas, func(k actionKey, _ *schema.Schema) bool { return !hashes[k.hash] })
	if e.opts.PackageDir != "" { // without it, a package evicted could not be loaded again
		maps.DeleteFunc(e.packages, func(h string, _ *pipeline.Package) bool { return !hashes[h] })
	}
	e.mu.Unlock()
	if e.opts.PackageDir == "" {
		return nil
	}
	entries, err := os.ReadDir(e.opts.PackageDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	now := e.opts.Now()
	for _, d := range entries {
		name := d.Name()
		if !d.IsDir() || hashes[name] {
			continue
		}
		if strings.HasPrefix(name, ".tmp-") {
			if info, err := d.Info(); err != nil || now.Sub(info.ModTime()) < ArtifactGrace {
				continue // another engine of this host may be writing it
			}
		}
		if err := os.RemoveAll(filepath.Join(e.opts.PackageDir, name)); err != nil {
			return err
		}
	}
	return nil
}

// liveRevisions returns the package hash of every pipeline's active revision and of every unsettled run's.
func (e *Engine) liveRevisions(ctx context.Context) (map[store.RevisionID]string, error) {
	pipelines, err := e.st.Pipelines(ctx)
	if err != nil {
		return nil, err
	}
	open, err := e.st.OpenRevisions(ctx)
	if err != nil {
		return nil, err
	}
	for _, ps := range pipelines {
		open = append(open, ps.ActiveRevision)
	}
	live := map[store.RevisionID]string{}
	for _, id := range open {
		if _, ok := live[id]; ok {
			continue
		}
		rev, err := e.st.Revision(ctx, id)
		if err != nil {
			return nil, err
		}
		live[id] = rev.PackageHash
	}
	return live, nil
}
