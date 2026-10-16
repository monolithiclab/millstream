package sqlstore

import (
	"errors"
	"fmt"
	"slices"

	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// FailpointImportCopy is hit inside the Stream import unit (part of Done, spec section 5.11), after the producer's
// item is inserted and before its copy is: a crash there must leave neither.
const FailpointImportCopy = "streamImport.copy"

// Failpoints names the failpoints hit inside units, besides each unit's beforeCommit and afterCommit (Units).
var Failpoints = []string{FailpointImportCopy}

// setImports records the streams p imports as its active revision's (spec section 4.12), replacing the rows of
// the revision it supersedes: every unit that changes a pipeline's active revision calls it.
func (w *wtx) setImports(p *pipeline.Pipeline) error {
	if _, err := w.exec(`DELETE FROM stream_imports WHERE consumer = ?`, p.Name); err != nil {
		return err
	}
	for _, name := range p.Imports() {
		imp := p.Streams[name].Import
		if _, err := w.exec(`INSERT INTO stream_imports (consumer, stream, producer, producer_stream)
			VALUES (?, ?, ?, ?)`, p.Name, name, imp.Pipeline, imp.Stream); err != nil {
			return err
		}
	}
	return nil
}

// importer is a stream of another pipeline importing a producer's stream.
type importer struct{ consumer, stream string }

// importCopies is the Stream import unit (spec sections 4.12 and 5.11): it copies item it, just inserted in the
// producer run, to every pipeline importing its stream, opening the consumer's run for that producer run on its first
// item. The copy's id is ids.ImportItem(stream, item id) and its parent the producer's item; it is published like any
// item, so it creates the consumer's deliveries and is copied on in turn when another pipeline imports the
// consumer's stream. A consumer run that was cancelled receives nothing more. w.chain holds the pipelines the copy
// went through in this unit: a cycle, which check refuses, ends rather than copying for ever.
func (w *wtx) importCopies(producer *runRow, it newItem) error {
	importers, err := w.importers(producer.pipeline, it.stream)
	if err != nil || len(importers) == 0 {
		return err
	}
	w.chain = append(w.chain, producer.pipeline)
	defer func() { w.chain = w.chain[:len(w.chain)-1] }()
	for _, imp := range importers {
		if slices.Contains(w.chain, imp.consumer) {
			continue
		}
		run, rev, err := w.importRun(imp, producer)
		if err != nil {
			return err
		}
		if run == nil {
			continue // cancelled
		}
		failpoint.Hit(FailpointImportCopy)
		if err := w.publish(rev, run, copyOf(imp.stream, producer.pipeline, it)); err != nil {
			return fmt.Errorf("import %s.%s into %s.%s: %w", producer.pipeline, it.stream, imp.consumer, imp.stream, err)
		}
	}
	return nil
}

// copyOf is the copy of a producer's item it on the consumer's imported stream: the id the import gives it, the
// producer's item as parent, its key, payload and artifacts.
func copyOf(stream, producer string, it newItem) newItem {
	return newItem{id: ids.ImportItem(stream, it.id), stream: stream, step: stream, key: it.key,
		parents: []string{it.id}, parentPipeline: producer, payload: it.payload, artifacts: it.artifacts}
}

// importers returns the pipelines importing stream from producer, by consumer then stream.
func (w *wtx) importers(producer, stream string) ([]importer, error) {
	rows, err := w.query(`SELECT consumer, stream FROM stream_imports WHERE producer = ? AND producer_stream = ?
		ORDER BY consumer, stream`, producer, stream)
	if err != nil {
		return nil, err
	}
	var out []importer
	for rows.Next() {
		var imp importer
		if err := rows.Scan(&imp.consumer, &imp.stream); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan stream import: %w", err)
		}
		out = append(out, imp)
	}
	return out, closeRows(rows)
}

// importRun returns the consumer's run for the producer run, ids.ImportRun(stream, producer run id), with its
// revision, opening it when this is its first item: on the consumer's active revision, with the variables resolved
// when that revision was applied, in generation 0 for a producer run of generation 0 and in the consumer's next
// generation otherwise (a producer replay gets a consumer run whose outputs are its own, as a replay's are). A
// cancelled run is nil; a settled one is reopened, since it receives more.
func (w *wtx) importRun(imp importer, producer *runRow) (*runRow, *revision, error) {
	id := ids.ImportRun(imp.stream, producer.id)
	run, err := w.run(imp.consumer, id)
	switch {
	case err == nil && run.state == store.RunCancelled:
		return nil, nil, nil
	case err == nil:
		if run.settled {
			if err := w.reopen(run, map[string]any{"producerRun": producer.id}); err != nil {
				return nil, nil, err
			}
		}
		rev, err := w.revision(run.revision)
		return run, rev, err
	case !errors.Is(err, store.ErrNotFound):
		return nil, nil, err
	}
	var revID store.RevisionID
	var vars, sources string
	if err := w.queryRow(`SELECT p.active_revision, r.vars, r.var_sources FROM pipelines p
		JOIN revisions r ON r.id = p.active_revision WHERE p.id = ?`, imp.consumer).Scan(&revID, &vars, &sources); err != nil {
		return nil, nil, notFound(err, "pipeline "+imp.consumer)
	}
	rev, err := w.revision(revID)
	if err != nil {
		return nil, nil, err
	}
	generation := int64(0)
	if producer.generation != 0 {
		if generation, err = w.nextGeneration(imp.consumer); err != nil {
			return nil, nil, err
		}
	}
	if _, err := w.exec(`INSERT INTO runs (pipeline, id, revision, trigger, trigger_key, trigger_source, kind,
		generation, vars, var_sources, state, started_at, producer)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		imp.consumer, id, revID, imp.stream, producer.id, store.SourceImport, store.RunKindImport, generation, vars,
		sources, store.RunRunning, w.ts(), producer.pipeline); err != nil {
		return nil, nil, err
	}
	if err := w.event(imp.consumer, id, store.EventRunStarted, map[string]any{
		"kind": store.RunKindImport, "trigger": imp.stream, "key": producer.id, "source": store.SourceImport,
		"revision": revID, "generation": generation, "producer": producer.pipeline, "producerRun": producer.id,
	}); err != nil {
		return nil, nil, err
	}
	if _, err := w.exec(`UPDATE runs SET consumers = consumers + 1 WHERE pipeline = ? AND id = ?`, producer.pipeline,
		producer.id); err != nil {
		return nil, nil, err
	}
	producer.consumers++
	v, err := pipeline.DecodeValue(vars)
	if err != nil {
		return nil, nil, fmt.Errorf("pipeline %s import vars: %w", imp.consumer, err)
	}
	run = &runRow{pipeline: imp.consumer, id: id, trigger: imp.stream, producer: producer.pipeline,
		producerRun: producer.id, revision: revID, generation: generation, state: store.RunRunning, startedAt: w.now}
	run.vars, _ = v.(map[string]any)
	return run, rev, nil
}

// nextGeneration takes a pipeline's next generation (spec section 5.7).
func (w *wtx) nextGeneration(pipelineName string) (int64, error) {
	var generation int64
	if err := w.queryRow(`INSERT INTO generations (pipeline, next) VALUES (?, 2)
		ON CONFLICT (pipeline) DO UPDATE SET next = generations.next + 1 RETURNING next - 1`, pipelineName).
		Scan(&generation); err != nil {
		return 0, fmt.Errorf("allocate a generation: %w", err)
	}
	return generation, nil
}

// producerHolds reports whether an import run's producer run is not settled yet, which holds the import run open
// since more items may arrive (spec section 5.4), and whether it waits only on reviews, so the import run, with
// nothing of its own left, waits on them too. A producer run that is gone holds nothing open.
func (w *wtx) producerHolds(run *runRow) (open, onlyReviews bool, err error) {
	if run.producer == "" {
		return false, false, nil
	}
	producer, err := w.run(run.producer, run.producerRun)
	if errors.Is(err, store.ErrNotFound) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return !producer.settled, producer.state == store.RunWaitingReview, nil
}

// runRef names a run of a pipeline.
type runRef struct{ pipeline, id string }

// openConsumers returns the import runs opened for a producer run and not settled, by pipeline and id.
func (w *wtx) openConsumers(producer *runRow) ([]runRef, error) {
	return w.runRefs(`SELECT pipeline, id FROM runs WHERE producer = ? AND trigger_key = ? AND producer <> ''
		AND settled_at IS NULL ORDER BY pipeline, id`, producer.pipeline, producer.id)
}

// settledConsumers returns the import runs opened for a producer run that settled and were not cancelled.
func (w *wtx) settledConsumers(producer *runRow) ([]runRef, error) {
	return w.runRefs(`SELECT pipeline, id FROM runs WHERE producer = ? AND trigger_key = ? AND producer <> ''
		AND settled_at IS NOT NULL AND state <> ? ORDER BY pipeline, id`, producer.pipeline, producer.id,
		store.RunCancelled)
}

func (w *wtx) runRefs(query string, args ...any) ([]runRef, error) {
	rows, err := w.query(query, args...)
	if err != nil {
		return nil, err
	}
	var out []runRef
	for rows.Next() {
		var ref runRef
		if err := rows.Scan(&ref.pipeline, &ref.id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan import run: %w", err)
		}
		out = append(out, ref)
	}
	return out, closeRows(rows)
}

// settleConsumers settles the import runs a producer run was holding open, once it settled, or moves them along
// with it between running and waitingReview.
func (w *wtx) settleConsumers(producer *runRow) error {
	if producer.consumers == 0 {
		return nil
	}
	refs, err := w.openConsumers(producer)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if err := w.settle(ref.pipeline, ref.id); err != nil {
			return err
		}
	}
	return nil
}

// reopen returns a settled run to running, recording runReopened with data, and reopens the import runs it had let
// settle: they stay open while their producer run is (spec section 5.4). A cancelled run stays cancelled.
func (w *wtx) reopen(run *runRow, data map[string]any) error {
	if !run.settled || run.state == store.RunCancelled {
		return nil
	}
	if _, err := w.exec(`UPDATE runs SET state = ?, settled_at = NULL WHERE pipeline = ? AND id = ?`,
		store.RunRunning, run.pipeline, run.id); err != nil {
		return err
	}
	if data == nil {
		data = map[string]any{}
	}
	data["from"] = run.state
	if err := w.event(run.pipeline, run.id, store.EventRunReopened, data); err != nil {
		return err
	}
	run.state, run.settled = store.RunRunning, false
	if run.consumers == 0 {
		return nil
	}
	refs, err := w.settledConsumers(run)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		consumer, err := w.run(ref.pipeline, ref.id)
		if err != nil {
			return err
		}
		if err := w.reopen(consumer, map[string]any{"producerRun": run.id}); err != nil {
			return err
		}
	}
	return nil
}
