// Package nodewire is the node protocol's wire format (spec sections 5.10 and 12.1): the JSON messages remote nodes
// and the active engine exchange under /node/v1, over HTTPS (HTTP/2) or plain HTTP to a loopback engine. Every
// request carries the deployment's node token as a bearer token; every request but Register names the node, which
// must have registered with the engine serving it (a new active engine after a failover answers CodeUnregistered,
// and the node registers again). Errors are {"error": {"code", "message"}}, as the HTTP API's.
//
// The protocol is versioned by Protocol, sent at registration: the engine refuses another version
// (CodeIncompatible).
package nodewire

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/store"
)

// Protocol is the node protocol's version.
const Protocol = 1

// Prefix is where the engine serves the node protocol (api.NodePrefix).
const Prefix = "/node/v1"

// The protocol's operations, each a POST of a JSON body answered with JSON (Done, Fail and Leave with an Ack), but
// Package (a GET answering the
// package's bundle) and Blob (the engine's artifact proxy: a GET of the content, or a PUT of it answering its
// reference).
const (
	PathRegister    = Prefix + "/register"
	PathPull        = Prefix + "/pull"
	PathHeartbeat   = Prefix + "/heartbeat"
	PathDone        = Prefix + "/done"
	PathFail        = Prefix + "/fail"
	PathLeave       = Prefix + "/leave"
	PathLogs        = Prefix + "/logs"
	PathPackage     = Prefix + "/package"
	PathArtifactURL = Prefix + "/artifacts/url"
	PathBlob        = Prefix + "/artifacts/blob"
)

// Error codes, with their HTTP statuses.
const (
	CodeUnauthorized = "unauthorized" // 401: no node token, or another one
	CodeRevoked      = "revoked"      // 403: the node id was revoked
	CodeNotHeld      = "notHeld"      // 403: the node holds no such lease (spec section 15.7)
	CodeUnregistered = "unregistered" // 409: the node must register (again) with this engine
	CodeStale        = "stale"        // 409: a Done or Fail under a lease the node no longer holds
	CodeIncompatible = "incompatible" // 400: another protocol version
	CodeInvalid      = "invalid"      // 400: a malformed request
	CodeNotFound     = "notFound"     // 404
	CodeUnavailable  = "unavailable"  // 503: a standby, or an engine between terms
	CodeInternal     = "internal"     // 500
)

// Error is an error answer's body.
type Error struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// Register is a node registering: its id, the protocol version it speaks, its version, labels and capacity.
type Register struct {
	Node     string            `json:"node"`
	Protocol int               `json:"protocol"`
	Version  string            `json:"version"`
	Labels   map[string]string `json:"labels"`
	Runtimes []string          `json:"runtimes"`
	Slots    int               `json:"slots"`
}

// Registered is the engine accepting a registration: who it is, and the lease duration nodes heartbeat within.
type Registered struct {
	Engine     string `json:"engine"`
	Epoch      int64  `json:"epoch,omitempty"`
	Version    string `json:"version"`
	LeaseTTLMs int64  `json:"leaseTtlMs"`
}

// Pull asks for up to Max deliveries, waiting up to WaitMs for one (a long poll; the engine caps the wait).
type Pull struct {
	Node   string `json:"node"`
	Max    int    `json:"max"`
	WaitMs int64  `json:"waitMs"`
}

// Ack is the engine accepting a Done, a Fail or a Leave. A node takes nothing else for an acceptance, an empty
// answer included: it sends the report again.
type Ack struct {
	Accepted bool `json:"accepted"`
}

// Check reports an answer that does not accept.
func (a *Ack) Check() error {
	if !a.Accepted {
		return errors.New("the engine's answer accepts nothing")
	}
	return nil
}

// Assignments are the deliveries a pull leased.
type Assignments struct {
	Assignments []Assignment `json:"assignments"`
}

// Assignment is one leased delivery: its lease, the action process as the engine resolved it, the secrets resolved
// for it (in memory only on the node), the item message and the step's timeout.
type Assignment struct {
	Lease     Lease             `json:"lease"`
	Launch    node.Launch       `json:"launch"`
	Secrets   map[string]string `json:"secrets,omitempty"`
	Item      protocol.Item     `json:"item"`
	TimeoutMs int64             `json:"timeoutMs,omitempty"`
}

// Lease is a lease as a node holds it.
type Lease struct {
	DeliveryID   string              `json:"deliveryId"`
	Pipeline     string              `json:"pipeline"`
	RunID        string              `json:"runId"`
	Revision     int64               `json:"revision"`
	Step         string              `json:"step"`
	Generation   int64               `json:"generation"`
	Attempt      int                 `json:"attempt"`
	FencingToken int64               `json:"fencingToken"`
	ExpiresAt    time.Time           `json:"expiresAt"`
	ItemID       string              `json:"itemId"`
	ItemKey      string              `json:"itemKey"`
	Artifacts    []store.ArtifactRef `json:"artifacts,omitempty"`
}

// LeaseRef names a lease: a delivery and the fencing token it was leased under.
type LeaseRef struct {
	Pipeline     string `json:"pipeline"`
	DeliveryID   string `json:"deliveryId"`
	Step         string `json:"step"`
	FencingToken int64  `json:"fencingToken"`
}

// Heartbeat renews the leases a node holds; the engine answers those it lost.
type Heartbeat struct {
	Node   string     `json:"node"`
	Leases []LeaseRef `json:"leases"`
}

// Lost are the leases a heartbeat found lost.
type Lost struct {
	Lost []LeaseRef `json:"lost"`
}

// Output is an output of a delivery done.
type Output struct {
	Key       string              `json:"key"`
	Payload   json.RawMessage     `json:"payload"`
	Artifacts []store.ArtifactRef `json:"artifacts,omitempty"`
}

// Done reports a delivery done, under its lease.
type Done struct {
	Node      string              `json:"node"`
	Lease     LeaseRef            `json:"lease"`
	Attempt   int                 `json:"attempt"`
	Outputs   []Output            `json:"outputs"`
	CostUSD   float64             `json:"costUsd,omitempty"`
	Artifacts []store.ArtifactRef `json:"artifacts,omitempty"`
	Metrics   []store.Metric      `json:"metrics,omitempty"`
}

// Fail reports a delivery failed, under its lease.
type Fail struct {
	Node       string              `json:"node"`
	Lease      LeaseRef            `json:"lease"`
	Attempt    int                 `json:"attempt"`
	Class      string              `json:"class"`
	Message    string              `json:"message"`
	Retryable  bool                `json:"retryable"`
	CostUSD    float64             `json:"costUsd,omitempty"`
	Unreported bool                `json:"unreported,omitempty"`
	Artifacts  []store.ArtifactRef `json:"artifacts,omitempty"`
	Metrics    []store.Metric      `json:"metrics,omitempty"`
}

// Leave is a node leaving: the engine ends the leases it still holds at once (each attempt consumed, as an expiry),
// and records it draining until it registers again.
type Leave struct {
	Node string `json:"node"`
}

// Logs relays a node's log lines about its deliveries, the actions' stderr among them (spec section 10.3), to the
// engine's log and live tail.
type Logs struct {
	Node  string    `json:"node"`
	Lines []LogLine `json:"lines"`
}

// LogLine is one log line: its time, level, message and attributes.
type LogLine struct {
	Time  time.Time      `json:"time"`
	Level string         `json:"level"`
	Msg   string         `json:"msg"`
	Attrs map[string]any `json:"attrs,omitempty"`
}

// ArtifactURL asks for a URL that moves one artifact of a delivery the node holds: Op get, an input artifact by
// SHA256; Op put, an output, its reference computed by the node.
type ArtifactURL struct {
	Node     string            `json:"node"`
	Lease    LeaseRef          `json:"lease"`
	Op       string            `json:"op"`
	Artifact store.ArtifactRef `json:"artifact"`
}

// Artifact URL operations.
const (
	OpGet = "get"
	OpPut = "put"
)

// URL is where to move the artifact: the S3-compatible store's presigned URL (Direct), or the engine's proxy, a
// path under Prefix with its signed query, reached with the node token. Exists, for a put, means the store holds the
// content already: nothing to send.
type URL struct {
	URL    string `json:"url"`
	Direct bool   `json:"direct,omitempty"`
	Exists bool   `json:"exists,omitempty"`
}

// ToRef is the store's lease reference of r.
func (r LeaseRef) ToRef() store.LeaseRef {
	return store.LeaseRef{Pipeline: r.Pipeline, DeliveryID: r.DeliveryID, Step: r.Step, FencingToken: r.FencingToken}
}

// FromRef is the wire lease reference of r.
func FromRef(r store.LeaseRef) LeaseRef {
	return LeaseRef{Pipeline: r.Pipeline, DeliveryID: r.DeliveryID, Step: r.Step, FencingToken: r.FencingToken}
}

// FromRefs and ToRefs convert lists of lease references.
func FromRefs(rs []store.LeaseRef) []LeaseRef {
	out := make([]LeaseRef, 0, len(rs))
	for _, r := range rs {
		out = append(out, FromRef(r))
	}
	return out
}

func ToRefs(rs []LeaseRef) []store.LeaseRef {
	out := make([]store.LeaseRef, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.ToRef())
	}
	return out
}

// FromLease is the wire lease of l.
func FromLease(l store.Lease) Lease {
	return Lease{DeliveryID: l.DeliveryID, Pipeline: l.Pipeline, RunID: l.RunID, Revision: int64(l.Revision),
		Step: l.Step, Generation: l.Generation, Attempt: l.Attempt, FencingToken: l.FencingToken,
		ExpiresAt: l.ExpiresAt, ItemID: l.Item.ID, ItemKey: l.Item.Key, Artifacts: l.Item.Artifacts}
}

// ToLease is the store lease of l, its item with what a node needs of it: id, key and input artifacts (the payload
// travels in the item message).
func (l Lease) ToLease() store.Lease {
	return store.Lease{DeliveryID: l.DeliveryID, Pipeline: l.Pipeline, RunID: l.RunID,
		Revision: store.RevisionID(l.Revision), Step: l.Step, Generation: l.Generation, Attempt: l.Attempt,
		FencingToken: l.FencingToken, ExpiresAt: l.ExpiresAt,
		Item: store.Item{ID: l.ItemID, Key: l.ItemKey, Pipeline: l.Pipeline, Artifacts: l.Artifacts}}
}

// FromOutputs and ToOutputs convert a delivery's outputs.
func FromOutputs(os []store.Output) []Output {
	out := make([]Output, 0, len(os))
	for _, o := range os {
		out = append(out, Output{Key: o.Key, Payload: o.Payload, Artifacts: o.Artifacts})
	}
	return out
}

func ToOutputs(os []Output) []store.Output {
	out := make([]store.Output, 0, len(os))
	for _, o := range os {
		out = append(out, store.Output{Key: o.Key, Payload: o.Payload, Artifacts: o.Artifacts})
	}
	return out
}
