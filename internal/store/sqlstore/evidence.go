package sqlstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/monolithiclab/millstream/internal/store"
)

// Evidence (spec section 11.2): the redaction key, the exports a production deployment records and the imports a
// development one keeps until they expire. An import's bundle is a file of the artifact store, counted in artifacts
// like an item's, so the artifact sweep deletes it once its import is gone.

// EvidenceKey returns the deployment's redaction key, made by the first call.
func (s *Store) EvidenceKey(ctx context.Context) ([]byte, error) {
	var b [32]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails
	var key string
	err := s.submit(ctx, unitEvidenceKey, func(w *wtx) error {
		if _, err := w.exec(`INSERT INTO evidence_key (id, key) VALUES (1, ?) ON CONFLICT (id) DO NOTHING`,
			hex.EncodeToString(b[:])); err != nil {
			return err
		}
		return w.queryRow(`SELECT key FROM evidence_key WHERE id = 1`).Scan(&key)
	})
	if err != nil {
		return nil, fmt.Errorf("evidence key: %w", err)
	}
	return hex.DecodeString(key)
}

// RecordExport records an evidence bundle exported, with its item keys.
func (s *Store) RecordExport(ctx context.Context, e store.EvidenceExport) error {
	if e.ID == "" || e.Pipeline == "" || e.ExpiresAt.IsZero() || !json.Valid(e.Scope) {
		return fmt.Errorf("record export: an id, a pipeline, an expiry and a JSON scope are required: %w",
			store.ErrInvalid)
	}
	keys := slices.DeleteFunc(slices.Compact(slices.Sorted(slices.Values(e.Keys))), func(k string) bool { return k == "" })
	return s.submit(ctx, unitRecordExport, func(w *wtx) error {
		n, err := w.affected(`INSERT INTO evidence_exports (id, pipeline, exported_by, exported_at, scope, revision_hash,
			items, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`, e.ID, e.Pipeline,
			e.ExportedBy, w.ts(), string(e.Scope), e.RevisionHash, e.Items, formatTime(e.ExpiresAt))
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("evidence bundle %s is recorded already: %w", e.ID, store.ErrConflict)
		}
		_, err = w.exec(`INSERT INTO evidence_export_keys (bundle, pipeline, item_key)
			SELECT CAST(? AS TEXT), CAST(? AS TEXT), value FROM (`+w.s.d.JSONParam(false)+`) l`,
			e.ID, e.Pipeline, jsonList(keys))
		return err
	})
}

// RecordImport records an evidence bundle imported and counts a reference to its file.
func (s *Store) RecordImport(ctx context.Context, e store.EvidenceImport) (bool, error) {
	if e.ID == "" || e.Pipeline == "" || e.SHA256 == "" || e.ExpiresAt.IsZero() {
		return false, fmt.Errorf("record import: an id, a pipeline, a file and an expiry are required: %w",
			store.ErrInvalid)
	}
	created := false
	err := s.submit(ctx, unitRecordImport, func(w *wtx) error {
		created = false
		if !e.ExpiresAt.After(w.now) {
			return fmt.Errorf("evidence bundle %s expired at %s: %w", e.ID, formatTime(e.ExpiresAt), store.ErrConflict)
		}
		n, err := w.affected(`INSERT INTO evidence_imports (id, pipeline, revision_hash, sha256, size, items,
			exported_at, imported_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`,
			e.ID, e.Pipeline, e.RevisionHash, e.SHA256, e.Size, e.Items, formatTime(e.ExportedAt), w.ts(),
			formatTime(e.ExpiresAt))
		if err != nil || n == 0 {
			return err
		}
		created = true
		_, err = w.exec(`INSERT INTO artifacts (pipeline, sha256, size, type, refcount, created_at)
			VALUES (?, ?, ?, ?, 1, ?) ON CONFLICT (pipeline, sha256) DO UPDATE SET refcount = artifacts.refcount + 1`,
			e.Pipeline, e.SHA256, e.Size, store.EvidenceType, w.ts())
		return err
	})
	return created, err
}

// ExpireEvidence deletes the evidence imports expired by the store clock and drops their references.
func (s *Store) ExpireEvidence(ctx context.Context) ([]store.EvidenceImport, error) {
	var out []store.EvidenceImport
	err := s.submit(ctx, unitExpireEvidence, func(w *wtx) error {
		var err error
		out, err = queryAll(w, scanImport, `SELECT `+importColumns+` FROM evidence_imports WHERE expires_at <= ?
			ORDER BY expires_at, id`, w.ts())
		if err != nil {
			return err
		}
		for _, e := range out {
			if _, err := w.exec(`DELETE FROM evidence_imports WHERE id = ?`, e.ID); err != nil {
				return err
			}
			if err := w.unref(e.Pipeline, e.SHA256); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

const importColumns = `id, pipeline, revision_hash, sha256, size, items, exported_at, imported_at, expires_at`

func scanImport(rows *sql.Rows) (store.EvidenceImport, error) {
	var (
		e                          store.EvidenceImport
		exported, imported, expiry TimeCol
	)
	if err := rows.Scan(&e.ID, &e.Pipeline, &e.RevisionHash, &e.SHA256, &e.Size, &e.Items, &exported, &imported,
		&expiry); err != nil {
		return e, fmt.Errorf("scan evidence import: %w", err)
	}
	e.ExportedAt, e.ImportedAt, e.ExpiresAt = exported.Time, imported.Time, expiry.Time
	return e, nil
}

// EvidenceImports returns the evidence imports of a pipeline, every pipeline when empty, oldest first.
func (s *Store) EvidenceImports(ctx context.Context, pipelineName string) ([]store.EvidenceImport, error) {
	var w where
	w.eq("pipeline", pipelineName)
	out, err := scan(ctx, s.read, `SELECT `+importColumns+` FROM evidence_imports`+w.String()+
		` ORDER BY imported_at, id`, w.args, scanImport)
	if err != nil {
		return nil, fmt.Errorf("evidence imports: %w", err)
	}
	return out, nil
}

// EvidenceExports returns the evidence exports of a pipeline, every pipeline when empty, oldest first, with the
// item keys they still hold.
func (s *Store) EvidenceExports(ctx context.Context, pipelineName string) ([]store.EvidenceExport, error) {
	var w where
	w.eq("pipeline", pipelineName)
	out, err := scan(ctx, s.read, `SELECT id, pipeline, exported_by, exported_at, scope, revision_hash, items,
		expires_at FROM evidence_exports`+w.String()+` ORDER BY exported_at, id`, w.args,
		func(rows *sql.Rows) (store.EvidenceExport, error) {
			var (
				e               store.EvidenceExport
				scope           string
				exported, until TimeCol
			)
			err := rows.Scan(&e.ID, &e.Pipeline, &e.ExportedBy, &exported, &scope, &e.RevisionHash, &e.Items, &until)
			e.Scope, e.ExportedAt, e.ExpiresAt, e.Keys = json.RawMessage(scope), exported.Time, until.Time, []string{}
			return e, err
		})
	if err != nil {
		return nil, fmt.Errorf("evidence exports: %w", err)
	}
	for i := range out {
		keys, err := scan(ctx, s.read, `SELECT item_key FROM evidence_export_keys WHERE bundle = ? ORDER BY item_key`,
			[]any{out[i].ID}, scanString)
		if err != nil {
			return nil, fmt.Errorf("evidence export keys: %w", err)
		}
		out[i].Keys = append(out[i].Keys, keys...)
	}
	return out, nil
}

// purgeBundles returns the evidence bundles holding the key of an item of the lineage, or the key a purge names,
// and forgets those keys: an erased key leaves no trace in the exports either (spec section 11.2).
func (w *wtx) purgeBundles(req store.PurgeRequest, byPipeline map[string][]string) ([]string, error) {
	type pk struct{ pipeline, key string }
	var keys []pk
	if req.Key != "" {
		var w2 where
		w2.eq("item_key", req.Key)
		w2.eq("pipeline", req.Pipeline)
		found, err := queryAll(w, scanPair, `SELECT DISTINCT pipeline, item_key FROM evidence_export_keys`+w2.String(),
			w2.args...)
		if err != nil {
			return nil, err
		}
		for _, f := range found {
			keys = append(keys, pk{f[0], f[1]})
		}
	}
	for name, ids := range byPipeline {
		for chunk := range slices.Chunk(ids, lineageChunk) {
			found, err := queryAll(w, scanPair, `SELECT DISTINCT pipeline, key FROM items WHERE pipeline = ?
				AND key <> '' AND id IN (`+w.s.d.JSONParam(false)+`)`, name, jsonList(chunk))
			if err != nil {
				return nil, err
			}
			for _, f := range found {
				keys = append(keys, pk{f[0], f[1]})
			}
		}
	}
	bundles := []string{}
	for _, k := range keys {
		found, err := queryAll(w, scanString, `DELETE FROM evidence_export_keys WHERE pipeline = ? AND item_key = ?
			RETURNING bundle`, k.pipeline, k.key)
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, found...)
	}
	return slices.Compact(slices.Sorted(slices.Values(bundles))), nil
}

// scanString scans a row of one text column.
func scanString(rows *sql.Rows) (string, error) {
	var v string
	return v, rows.Scan(&v)
}
