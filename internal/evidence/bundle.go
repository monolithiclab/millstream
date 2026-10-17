// Package evidence moves production history to development (spec section 11.2). A production deployment exports an
// evidence bundle: its open insights, a stratified sample of items with every step's inputs and outputs on their
// lineage, review decisions without reviewer identities, agent transcripts and the production revision, with the
// values of each stream's redact pointers replaced by a hash keyed per deployment. A development deployment imports
// it as read-only history, kept whole in the artifact store until it expires; insights, backtests and the improve
// pipeline read it with Load, never mixed into the deployment's own items.
package evidence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// The bundle format: a gzip-compressed tar holding manifest.json first, then evidence.json, the production revision's
// package (package) and each transcript under transcripts/<sha256>. Version changes with any incompatible change, and
// a reader refuses a version it does not know.
const (
	Format  = "millstream-evidence"
	Version = 1
)

// Defaults of the environment file's improve.evidence (spec section 14) and of export's --since.
const (
	DefaultSample = 500
	DefaultTTL    = 30 * 24 * time.Hour
	DefaultSince  = 30 * 24 * time.Hour
)

// maxDecoded bounds what Decode decompresses: a bundle is read whole into memory.
const maxDecoded = 1 << 30

// Archive entries.
const (
	manifestEntry   = "manifest.json"
	evidenceEntry   = "evidence.json"
	packageEntry    = "package"
	transcriptsPath = "transcripts/"
)

// Strata are the outcomes the sample is stratified by: a delivery's terminal state, or its review's decision.
const (
	StratumDone     = "done"
	StratumFiltered = "filtered"
	StratumDead     = "dead"
	StratumApproved = "approved"
	StratumRejected = "rejected"
	StratumEdited   = "edited"
)

// strata lists them in the order allocation visits them.
var strata = []string{StratumDone, StratumFiltered, StratumDead, StratumApproved, StratumRejected, StratumEdited}

// Manifest describes a bundle.
type Manifest struct {
	Format     string    `json:"format"`
	Version    int       `json:"version"`
	ID         string    `json:"id"`
	Pipeline   string    `json:"pipeline"`
	Revision   Revision  `json:"revision"`
	ExportedAt time.Time `json:"exportedAt"`
	ExpiresAt  time.Time `json:"expiresAt"`
	Scope      Scope     `json:"scope"`
	// Strata counts the sampled deliveries by stratum.
	Strata map[string]int `json:"strata"`
}

// Revision is the production revision a bundle was exported on.
type Revision struct {
	ID          int64     `json:"id"`
	PackageHash string    `json:"packageHash"`
	AppliedAt   time.Time `json:"appliedAt"`
}

// Scope is what an export was asked for.
type Scope struct {
	Since   time.Time `json:"since"`
	Insight string    `json:"insight,omitempty"`
	Sample  int       `json:"sample"`
}

// Evidence is the history a bundle carries.
type Evidence struct {
	Insights []Insight `json:"insights"`
	// Sample lists the deliveries the stratified sample chose; the items, deliveries and reviews are their lineages.
	Sample      []Sampled    `json:"sample"`
	Items       []Item       `json:"items"`
	Deliveries  []Delivery   `json:"deliveries"`
	Reviews     []Review     `json:"reviews"`
	Transcripts []Transcript `json:"transcripts"`
}

// Insight is an insight as exported.
type Insight struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Scope       string          `json:"scope"`
	Severity    string          `json:"severity"`
	State       string          `json:"state"`
	WindowStart time.Time       `json:"windowStart"`
	WindowEnd   time.Time       `json:"windowEnd"`
	Evidence    json.RawMessage `json:"evidence"`
	OpenedAt    time.Time       `json:"openedAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
}

// Sampled is a delivery the sample chose, with its stratum.
type Sampled struct {
	Delivery string `json:"delivery"`
	Stratum  string `json:"stratum"`
}

// Item is an item on a sampled lineage, its payload redacted.
type Item struct {
	ID         string              `json:"id"`
	Run        string              `json:"run"`
	Stream     string              `json:"stream"`
	Step       string              `json:"step"`
	Key        string              `json:"key"`
	Parents    []string            `json:"parents"`
	Generation int64               `json:"generation"`
	TS         time.Time           `json:"ts"`
	Payload    json.RawMessage     `json:"payload"`
	Artifacts  []store.ArtifactRef `json:"artifacts,omitempty"`
	Tombstone  string              `json:"tombstone,omitempty"`
}

// Delivery is a step's delivery of an item on a sampled lineage: the step's input is its item, its outputs the items
// whose parents hold it, published by its step.
type Delivery struct {
	ID           string    `json:"id"`
	Run          string    `json:"run"`
	Step         string    `json:"step"`
	Item         string    `json:"item"`
	Generation   int64     `json:"generation"`
	State        string    `json:"state"`
	Attempt      int       `json:"attempt"`
	ErrorClass   string    `json:"errorClass,omitempty"`
	Error        string    `json:"error,omitempty"`
	FilterReason string    `json:"filterReason,omitempty"`
	CostUSD      float64   `json:"costUsd"`
	CreatedAt    time.Time `json:"createdAt"`
	FinishedAt   time.Time `json:"finishedAt"`
	WaitMs       int64     `json:"waitMs"`
	WorkMs       int64     `json:"workMs"`
}

// Deciders a review records in place of the reviewer's identity, never exported.
const (
	DeciderPerson  = "person"
	DeciderRule    = "rule"
	DeciderTimeout = "timeout"
)

// Review is a review decision: who decided is only person, rule or timeout; the free text is not exported.
type Review struct {
	Delivery  string          `json:"delivery"`
	Run       string          `json:"run"`
	Step      string          `json:"step"`
	Item      string          `json:"item"`
	State     string          `json:"state"`
	Decision  string          `json:"decision"`
	Decider   string          `json:"decider"`
	Reason    string          `json:"reason,omitempty"`
	Patch     json.RawMessage `json:"patch"`
	ParkedAt  time.Time       `json:"parkedAt"`
	DecidedAt time.Time       `json:"decidedAt"`
}

// Transcript is an artifact an attempt kept, an agent's transcript, stored redacted under transcripts/<SHA256>.
type Transcript struct {
	Delivery string `json:"delivery"`
	Attempt  int    `json:"attempt"`
	Name     string `json:"name"`
	SHA256   string `json:"sha256"`
	Size     int64  `json:"size"`
}

// Bundle is an evidence bundle: its manifest, its history, the production revision's package and the transcripts'
// contents by hash.
type Bundle struct {
	Manifest
	Evidence
	Package []byte
	Files   map[string][]byte
}

// Encode writes the bundle as its archive.
func (b *Bundle) Encode() ([]byte, error) {
	manifest, err := json.Marshal(b.Manifest)
	if err != nil {
		return nil, err
	}
	evidence, err := json.Marshal(b.Evidence)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	put := func(name string, data []byte) {
		if err == nil {
			err = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data)), ModTime: b.ExportedAt,
				Typeflag: tar.TypeReg})
		}
		if err == nil {
			_, err = tw.Write(data)
		}
	}
	put(manifestEntry, manifest)
	put(evidenceEntry, evidence)
	put(packageEntry, b.Package)
	for _, t := range b.Transcripts {
		put(transcriptsPath+t.SHA256, b.Files[t.SHA256])
	}
	if err == nil {
		err = tw.Close()
	}
	if err == nil {
		err = zw.Close()
	}
	return buf.Bytes(), err
}

// ErrFormat marks data that is not an evidence bundle this build reads.
var ErrFormat = errors.New("not an evidence bundle")

// Decode reads a bundle's archive, refusing another format or version.
func Decode(data []byte) (*Bundle, error) {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFormat, err)
	}
	tr := tar.NewReader(io.LimitReader(zr, maxDecoded))
	b := &Bundle{Files: map[string][]byte{}}
	seen := map[string]bool{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrFormat, err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrFormat, h.Name, err)
		}
		if !seen[manifestEntry] && h.Name != manifestEntry {
			return nil, fmt.Errorf("%w: the archive does not open with %s", ErrFormat, manifestEntry)
		}
		seen[h.Name] = true
		switch name := path.Clean(h.Name); {
		case name == manifestEntry:
			if err := json.Unmarshal(body, &b.Manifest); err != nil {
				return nil, fmt.Errorf("%w: %s: %w", ErrFormat, name, err)
			}
			if b.Format != Format || b.Version != Version {
				return nil, fmt.Errorf("%w: format %q version %d, this build reads %s version %d", ErrFormat,
					b.Format, b.Version, Format, Version)
			}
		case name == evidenceEntry:
			if err := json.Unmarshal(body, &b.Evidence); err != nil {
				return nil, fmt.Errorf("%w: %s: %w", ErrFormat, name, err)
			}
		case name == packageEntry:
			b.Package = body
		case path.Dir(name)+"/" == transcriptsPath:
			b.Files[path.Base(name)] = body
		default:
			return nil, fmt.Errorf("%w: unexpected entry %s", ErrFormat, h.Name)
		}
	}
	if !seen[manifestEntry] || !seen[evidenceEntry] || b.ID == "" || b.Pipeline == "" {
		return nil, fmt.Errorf("%w: no manifest, evidence, id or pipeline", ErrFormat)
	}
	return b, nil
}
