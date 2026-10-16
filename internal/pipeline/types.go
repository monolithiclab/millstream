// Package pipeline is the pipeline file format (spec section 4): parsing a package into a typed Pipeline with
// strict, path-carrying errors, hashing the package into its revision, resolving variables and secrets, and drawing
// the step graph. Parsing checks the shape of the file only; cross-references (unknown streams, cycles, schema
// compatibility) are diagnostics of Check, so one broken reference never hides the others.
package pipeline

import (
	"maps"
	"slices"
	"time"
)

// Pipeline is a parsed pipeline file. Maps are keyed by the names the user chose.
type Pipeline struct {
	Name        string
	Description string
	// Vars holds each variable's default. A nil default means the variable is required (spec section 4.4).
	// Numbers are int64 when integral in the file and float64 otherwise.
	Vars     map[string]any
	Secrets  []string
	Triggers map[string]Trigger
	Streams  map[string]StreamDecl
	Steps    map[string]Step
	Budget   Budget
	Limits   Limits
	// Placement is the pipeline-wide node placement; a step's own placement overrides it.
	Placement Placement
	// Insights and Improve are kept as written: M1 parses but does not interpret them (spec sections 11.1, 11.3).
	Insights map[string]any
	Improve  map[string]any
}

// Trigger is one trigger definition: exactly one of its kinds is set.
type Trigger struct {
	Cron    *CronSpec
	Manual  *ManualSpec
	Webhook *WebhookSpec
	// InputSchema is the schema of the trigger's stream, as written (a path or an inline schema).
	InputSchema any
}

// Catch-up policies for cron firings missed while no engine ran (spec section 4.6).
const (
	CatchupLatest = "latest"
	CatchupAll    = "all"
	CatchupNone   = "none"
)

// CronSpec is a cron trigger.
type CronSpec struct {
	Expr    string
	TZ      string
	Catchup string // latest (default), all or none
}

// ManualSpec is a manual trigger.
type ManualSpec struct{}

// WebhookSpec is a webhook trigger; M1 parses it and does not serve it.
type WebhookSpec struct {
	Path string
}

// StreamDecl is an entry under streams: either settings for a stream of this pipeline, or an import of another
// pipeline's stream (spec section 4.12), never both.
type StreamDecl struct {
	Schema    any // a path or an inline schema, as written
	Retention time.Duration
	Redact    []string
	Import    *StreamImport
}

// StreamImport names another pipeline's stream.
type StreamImport struct {
	Pipeline string
	Stream   string
}

// Step is one step definition (spec section 4.3).
type Step struct {
	Name   string
	Action string
	In     []string
	Out    Out
	DLQ    string // defaults to "<step>.dlq"
	Config map[string]any
	// Concurrency is the most deliveries in flight; defaults to 1.
	Concurrency int
	// Rate is the most deliveries started per period, as written (for example "10/min"); see ParseRate.
	Rate      string
	Retry     Retry
	Timeout   time.Duration
	Paused    bool
	Placement *Placement // nil means the pipeline's placement
	Fallback  *Fallback
	// Lessons is the step's lessons file, relative to the package; Load fills in lessons/<step>.yaml when that
	// file exists and the step sets none.
	Lessons string
}

// FirstManualTrigger returns the name of the pipeline's first manual trigger by name: the one fired when none is
// named (an API trigger, a package test). ok is false when it has none.
func (p *Pipeline) FirstManualTrigger() (name string, ok bool) {
	for _, n := range slices.Sorted(maps.Keys(p.Triggers)) {
		if p.Triggers[n].Manual != nil {
			return n, true
		}
	}
	return "", false
}

// Out is where a step's outputs go: a list of streams that each receive every output, or route objects where the
// first match wins. Exactly one is non-empty, or both are empty for a terminal step.
type Out struct {
	Streams []string
	Routes  []Route
}

// Route is a route object: outputs for which the CEL expression When holds go to stream Out.
type Route struct {
	When string
	Out  string
}

// Retry is a step's retry policy for retryable errors.
type Retry struct {
	MaxAttempts int
	Backoff     time.Duration
	MaxBackoff  time.Duration
}

// Placement restricts deliveries to nodes carrying all of Labels.
type Placement struct {
	Labels map[string]string
}

// Fallback is a second action for deliveries the first abstains on or fails (spec section 11.5).
type Fallback struct {
	Action    string
	When      []string // abstain (default), error
	AuditRate float64
}

// Budget holds the pipeline's cost guards in US dollars; zero means unset.
type Budget struct {
	RunUSD float64
	DayUSD float64
}

// Limits holds pipeline-wide limits; zero means unset.
type Limits struct {
	Concurrency int
}

// Step defaults from spec section 4.3.
const (
	DefaultConcurrency = 1
	DefaultMaxAttempts = 3
	DefaultBackoff     = 10 * time.Second
	DefaultMaxBackoff  = 10 * time.Minute
	DefaultTimeout     = 10 * time.Minute
)

// TriggerStreamPrefix is the reserved prefix of trigger streams: trigger x publishes to trigger.x.
const TriggerStreamPrefix = "trigger."
