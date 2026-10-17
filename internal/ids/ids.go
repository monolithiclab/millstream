// Package ids holds Millstream's deterministic identifier formulas (D9). Every id is a function of the facts that
// make the thing unique, never of time of creation or a random source, so a retried or replayed write lands on the
// same row and idempotency needs no exactly-once machinery. Changing a formula changes every id: it is a storage
// format, not an implementation detail.
package ids

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// Prefixes of the ids this package produces.
const (
	itemPrefix     = "it_"
	deliveryPrefix = "dl_"
	runPrefix      = "run_"
	auditPrefix    = "au_"
)

// hashLen is the number of hex characters kept from the SHA-256 digest.
const hashLen = 32

// digest returns the first hashLen hex characters of SHA-256 over the fields joined with NUL. NUL cannot appear in
// a step, stream or trigger name, so the join is unambiguous for those; payload-derived keys are hashed as given.
func digest(fields ...string) string {
	h := sha256.New()
	for i, f := range fields {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(f))
	}
	return hex.EncodeToString(h.Sum(nil)[:hashLen/2])
}

// Item is the id of an output item: hash(producing step, out stream, input item id, generation, key ?? output
// index). An empty key means the output is unkeyed and its index stands in, which is only stable for a delivery
// with a single output; the key rule (spec section 5.2) is enforced by the store, not here.
func Item(step, stream, inputItemID string, generation int64, key string, index int) string {
	k := key
	if k == "" {
		k = strconv.Itoa(index)
	}
	return itemPrefix + digest(step, stream, inputItemID, strconv.FormatInt(generation, 10), k)
}

// TriggerItem is the id of the item a trigger firing publishes: hash("trigger", name, key), where key is the
// scheduled time of a cron firing or the request id of a manual or webhook one.
func TriggerItem(trigger, key string) string {
	return itemPrefix + digest("trigger", trigger, key)
}

// Delivery is the id of a delivery: hash(step, item id, generation).
func Delivery(step, itemID string, generation int64) string {
	return deliveryPrefix + digest(step, itemID, strconv.FormatInt(generation, 10))
}

// Idempotency is the idempotency key an action receives with a delivery (docs/protocol.md): hash(step, item id,
// generation), the same on every attempt and new on a replay run.
func Idempotency(step, itemID string, generation int64) string {
	return digest(step, itemID, strconv.FormatInt(generation, 10))
}

// DeadLetter is the id of a dead-letter item: hash("dlq", dlq stream, origin delivery id, output id or "",
// failure sequence). outputID is empty for a dead delivery and the unrouted output's id for an unrouted dead letter;
// seq counts the origin delivery's earlier dead letters, so a delivery that dies again after a replay publishes a
// new dead letter rather than landing on the one already replayed.
func DeadLetter(dlqStream, originDelivery, outputID string, seq int) string {
	return itemPrefix + digest("dlq", dlqStream, originDelivery, outputID, strconv.Itoa(seq))
}

// ImportItem is the id of the copy of a producer's item published on a consumer's imported stream (spec section
// 4.12): hash("import", stream, producer item id), where stream is the consumer's name for the imported stream.
func ImportItem(stream, producerItemID string) string {
	return itemPrefix + digest("import", stream, producerItemID)
}

// ImportRun is the id of the run an imported stream opens in the consumer for one producer run (spec section 4.12):
// run_import_ and hash("import", stream, producer run id), stream being the consumer's name for it.
func ImportRun(stream, producerRunID string) string {
	return runPrefix + "import_" + digest("import", stream, producerRunID)
}

// CronKey formats a cron firing's scheduled time as the trigger item key and run id component: RFC 3339 truncated
// to the minute, in UTC, for example 2026-10-14T06:00Z.
func CronKey(scheduledAt time.Time) string {
	return scheduledAt.UTC().Format("2006-01-02T15:04Z")
}

// CronRun is the id of the run a cron firing opens: run_<scheduled minute>_<trigger>.
func CronRun(scheduledAt time.Time, trigger string) string { return run(CronKey(scheduledAt), trigger) }

// ManualRun is the id of the run a manual (or webhook) firing opens: run_<request id>_<trigger>.
func ManualRun(requestID, trigger string) string { return run(requestID, trigger) }

func run(key, trigger string) string { return runPrefix + key + "_" + trigger }

// UpperSnake converts a user-chosen name to the upper snake case used in environment variable names:
// homesiteToken → HOMESITE_TOKEN, min-score → MIN_SCORE, apiURL → API_URL, HTTPServer → HTTP_SERVER.
// Word boundaries are a lower-case letter or digit followed by an upper-case letter, the last capital of an
// acronym followed by a lower-case letter, and any character that is not a letter or digit.
func UpperSnake(name string) string {
	runes := []rune(name)
	var b strings.Builder
	pendingSep := false
	for i, r := range runes {
		if !isAlnum(r) {
			pendingSep = b.Len() > 0
			continue
		}
		if i > 0 && isUpper(r) && b.Len() > 0 {
			prev := runes[i-1]
			nextLower := i+1 < len(runes) && isLower(runes[i+1])
			if isLower(prev) || isDigit(prev) || (isUpper(prev) && nextLower) {
				pendingSep = true
			}
		}
		if pendingSep {
			b.WriteByte('_')
			pendingSep = false
		}
		b.WriteString(strings.ToUpper(string(r)))
	}
	return b.String()
}

func isUpper(r rune) bool { return r >= 'A' && r <= 'Z' }
func isLower(r rune) bool { return r >= 'a' && r <= 'z' }
func isDigit(r rune) bool { return r >= '0' && r <= '9' }
func isAlnum(r rune) bool { return isUpper(r) || isLower(r) || isDigit(r) }

// ReplayRun is the id of a replay run (spec section 5.7): run_replay_<request id>, so a replay request retried
// after a crash lands on the run it already started.
func ReplayRun(requestID string) string {
	return runPrefix + "replay_" + requestID
}

// Purge is the id of a purge (spec section 9.3): purge_<request id>, so a purge requested again after a crash lands
// on the purge it already started.
func Purge(requestID string) string {
	return "purge_" + requestID
}

// Audit is the id of the fallback audit of a delivery (spec section 11.5): au_ and hash("audit", delivery id). A
// delivery is audited at most once.
func Audit(deliveryID string) string {
	return auditPrefix + digest("audit", deliveryID)
}

// IsAudit reports whether id is an audit's id: a lease of one names it in place of a delivery's.
func IsAudit(id string) bool { return strings.HasPrefix(id, auditPrefix) }

// Sampled reports whether id falls in a sample at rate: deterministic per id, and uniform over ids (the first 8
// bytes of the id's SHA-256 as a fraction of 2^64), so about rate of them do.
func Sampled(id string, rate float64) bool {
	sum := sha256.Sum256([]byte(id))
	return float64(binary.BigEndian.Uint64(sum[:8]))/(1<<64) < rate
}
