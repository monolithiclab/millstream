package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/pipeline"
)

// EvidenceExportRequest asks for an evidence bundle of a pipeline (spec section 11.2).
type EvidenceExportRequest struct {
	// Since samples the deliveries created within this duration (30d by default).
	Since   string `json:"since"`
	Insight string `json:"insight"`
}

// BundleHeader names the bundle an export answers.
const BundleHeader = "X-Millstream-Bundle"

// exportEvidence answers a new evidence bundle, recorded in the evidence exports under the request's user.
func (s *Server) exportEvidence(w http.ResponseWriter, r *http.Request) error {
	var req EvidenceExportRequest
	if err := s.decodeBody(w, r, &req); err != nil {
		return err
	}
	since := evidence.DefaultSince
	if req.Since != "" {
		var err error
		if since, err = pipeline.ParseDuration(req.Since); err != nil {
			return invalidf("since: %s", err.Error())
		}
	}
	who, err := s.user(r)
	if err != nil {
		return err
	}
	now := s.opts.Clock.Now()
	data, rec, err := evidence.Export(r.Context(), s.opts.Store, s.opts.Artifacts, now, evidence.ExportRequest{
		Pipeline: r.PathValue("pipeline"), Since: now.Add(-since), Insight: req.Insight,
		Sample: s.opts.Evidence.Sample, TTL: s.opts.Evidence.TTL, By: who})
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+rec.ID+`.tar.gz"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set(BundleHeader, rec.ID)
	w.WriteHeader(http.StatusCreated)
	// #nosec G705 -- a gzip archive sent as an attachment of type application/gzip, never sniffed (Handler)
	_, _ = w.Write(data) // the status is sent: a client gone mid-body has nothing to be told
	return nil
}

// EvidenceImportRequest carries a bundle to import, base64 in JSON: a POST is JSON (jsonContent).
type EvidenceImportRequest struct {
	Bundle []byte `json:"bundle"`
}

// EvidenceImport is an imported bundle (spec section 11.2), with the production revision it was exported on and the
// development deployment's active one: they differ when the package diverged.
type EvidenceImport struct {
	ID                 string    `json:"id"`
	Pipeline           string    `json:"pipeline"`
	Items              int       `json:"items"`
	ExportedAt         time.Time `json:"exportedAt"`
	ExpiresAt          time.Time `json:"expiresAt"`
	ProductionRevision string    `json:"productionRevision"`
	LocalRevision      string    `json:"localRevision"`
	Diverged           bool      `json:"diverged"`
}

// ToEvidenceImport is an import as the API answers it.
func ToEvidenceImport(im evidence.Imported) EvidenceImport {
	return EvidenceImport{ID: im.ID, Pipeline: im.Pipeline, Items: im.Items, ExportedAt: im.ExportedAt,
		ExpiresAt: im.ExpiresAt, ProductionRevision: im.ProductionRevision.PackageHash,
		LocalRevision: im.LocalRevision, Diverged: im.Diverged()}
}

// importEvidence imports a bundle into this deployment, refused in production.
func (s *Server) importEvidence(w http.ResponseWriter, r *http.Request) error {
	var req EvidenceImportRequest
	if err := s.decodeBodyMax(w, r, &req, s.opts.MaxPackagesBody); err != nil {
		return err
	}
	if len(req.Bundle) == 0 {
		return invalidf("bundle: required")
	}
	if s.opts.Artifacts == nil {
		return fmt.Errorf("evidence import: no artifact store: %w", errUnavailable)
	}
	im, err := evidence.Import(r.Context(), s.opts.Store, s.opts.Artifacts, s.opts.Clock.Now(), s.opts.Evidence.Mode,
		req.Bundle)
	if err != nil {
		return err
	}
	status := http.StatusOK
	if im.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, ToEvidenceImport(im))
	return nil
}
