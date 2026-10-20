# Getting started with Millstream

This tutorial takes you from nothing to a running pipeline with an agent step and a human review, on a laptop, in
about 40 minutes. Every command was run as written; the outputs are real, trimmed, and your ids and times will
differ.

## 1. What Millstream is

Millstream runs **pipelines**: graphs of **steps** connected by persisted **streams** of items. A step reads one or
more streams and writes others; steps never call each other, so inserting a step means pointing it at a stream. A
step runs an **action**, which is any program that speaks a small JSON-lines protocol over stdio, in any language,
or one of the builtins (`filter`, `dedupe`, `review`) that the engine runs itself. An **agent** is an action like any
other: Millstream renders its prompt, calls the model, validates the answer against a schema and records the
transcript and the cost. A `review` step parks items in an inbox until a person decides. Every item, attempt and
decision is stored, so you can replay a failed item, re-run a step over past items, and let the platform find what
is slow, failing or expensive, and propose fixes.

You will build `listings`, a small real estate pipeline:

```text
trigger.adhoc → crawl → dedupe → affordable ─┬→ listings.renovate → score (agent) → keep-good → analyst-review
                                              └→ listings.ready
```

You need Go (the version in the repository's `go.mod`, 1.27 at the time of writing), Git, and macOS or Linux. Docker
is optional: it builds the web UI and sandboxes agents, and this tutorial works without it. No model API key is
needed: the agent step runs against a small local stand-in for a model, then offline from recordings.

## 2. Install

Clone the repository and build the binary. Keep the checkout's path in a variable: the Go SDK is not published yet,
so actions point at it.

```sh
git clone git@github.com:monolithiclab/millstream.git
cd millstream
export MILLSTREAM_REPO="$PWD"
make ui      # optional, needs Docker: builds the web UI, embedded by the next build
make build   # writes build/millstream
export PATH="$MILLSTREAM_REPO/build:$PATH"
millstream version
```

```text
millstream 8f3b815
```

`go install ./cmd/millstream` works too (it prints `millstream dev` as its version). Without `make ui`, the binary
serves a page saying the UI was not built; the HTTP API and every CLI command work either way.

Work in a directory of your own, outside the checkout:

```sh
mkdir ~/millstream-tutorial && cd ~/millstream-tutorial
```

## 3. Scaffold a pipeline

```sh
millstream init pipeline listings
cd listings
```

```text
created /Users/you/millstream-tutorial/listings
next: millstream check /Users/you/millstream-tutorial/listings && millstream test pipeline /Users/you/millstream-tutorial/listings
```

The package holds everything the pipeline uses:

```text
.gitignore                     ignores bin/ (built actions) and .millstream/ (local state)
pipeline.yaml                  the pipeline file
actions/greet/action.yaml      the action's manifest: kind, input, output and config schemas, runtime
actions/greet/main.go          the action, written with the Go SDK
actions/greet/go.mod
actions/greet/tests/example.yaml   a fixture for `millstream test action`
actions/greet/evals/example.yaml   an eval case for `millstream eval`
tests/example.yaml             a package test for `millstream test pipeline`
```

`pipeline.yaml` declares a manual trigger and one step:

```yaml
pipeline: listings
description: Greets each name it is given

triggers:
  adhoc: { manual: {} }

steps:
  greet:
    action: ./actions/greet
    in: trigger.adhoc
    out: greetings
    config: { greeting: Hello }
```

A trigger named `adhoc` publishes one item per run to the stream `trigger.adhoc`, which the step reads like any
other stream. The step's outputs go to `greetings`, a stream declared by use. `action: ./actions/greet` is a local
action; its manifest says it is an `exec` action whose command is `greet`, which the engine finds at
`actions/greet/bin/<os>-<arch>/greet`. `main.go` is a function from an input to outputs:

```go
millstream.Map(func(c millstream.Context, in Input) ([]Output, error) {
	cfg := Config{Greeting: "Hello"}
	if err := c.ConfigInto(&cfg); err != nil {
		return nil, err // not retryable: the same config fails again
	}
	...
	return []Output{{Greeting: cfg.Greeting + ", " + in.Name + "!"}}, nil
})
```

The SDK reads items from stdin and writes answers to stdout; [the action protocol](protocol.md) is the whole
contract, if you would rather write actions without it.

## 4. Check, test and run

`check` validates the package: schemas, streams, expressions, actions. `graph` prints it as Mermaid (or DOT with
`--format dot`). `test pipeline` runs the package tests with their steps stubbed, so nothing needs building yet.

```sh
millstream check
millstream graph
millstream test pipeline
```

```text
warning steps.greet.out: stream greetings is written by step greet but no step reads it
listings: 0 errors, 1 warning

flowchart LR
  n0["greet"]
  n1[("greetings")]
  n2(["trigger.adhoc"])
  n0 -->|"greetings"| n1
  n2 -->|"trigger.adhoc"| n0

PASS one name greeted
listings: 1 passed, 0 failed
```

Millstream never builds local actions: you build them where the manifest finds them. Point the action's `go.mod` at
the SDK in your checkout, build it, then run its fixture, the protocol conformance suite and its eval:

```sh
cd actions/greet
go mod edit -replace github.com/monolithiclab/millstream/sdk/go=$MILLSTREAM_REPO/sdk/go
go mod tidy
go build -o bin/$(go env GOOS)-$(go env GOARCH)/greet .
millstream test action .
millstream test action . --conformance
millstream eval .
cd ../..
```

```text
PASS greets by name
listings/greet: 1 passed, 0 failed
PASS ready: ready in 4ms
...
PASS shutdown: exited with status 0
listings/greet: conformant
CASE     ASSERTIONS  JUDGE  COST     SCORE
example  1/1         -      $0.0000  1.00
TOTAL                                1.00
```

### One run

`run --once` applies the package, fires its manual triggers once, waits for the runs to settle, prints a summary and
exits. With no directory argument it runs the package in the current directory. The engine logs JSON lines on
stderr; they are hidden here.

```sh
millstream run --once 2>/dev/null
```

```text
listings run_20261020T162508Z-f9927400_adhoc succeeded
  greetings 1
  trigger.adhoc 1
```

`run --once` fires the trigger with an empty payload, `{}`. `greet`'s input schema makes `name` optional and the
action greets the world when there is none; an item that failed the schema would have gone to the step's
**dead-letter stream**, `greet.dlq`, to wait there to be replayed or discarded (section 6 shows one).

State lives in `.millstream/millstream.db`, a SQLite database in the current directory. Every command reads it,
whether or not an engine is running.

### The engine and the web UI

Fire the trigger with a payload. With no engine running, the run is recorded and waits:

```sh
millstream trigger listings adhoc --payload '{"name":"Ada"}'
```

```text
run_20261020T162512Z-a8bab96e_adhoc
```

Now start the engine in a second terminal (same directory, same `PATH`). `millstream run` without `--once` keeps
running, serves the API and the UI on 127.0.0.1:7070, and picks up the waiting run:

```sh
millstream run 2>../engine.log
```

```text
Millstream is serving the UI and the API on http://127.0.0.1:7070/
```

The log goes outside the package on purpose: every file of the package directory but `.git/`, `.millstream/` and
`millstream.yaml` is part of its content, so a log written there would make each restart a new revision.

Back in the first terminal:

```sh
millstream status
```

```text
PIPELINE  STATE   REVISION  PAUSED STEPS
listings  active  1         -

RUN                                  PIPELINE  STATE      STARTED               ITEMS  OPEN DEAD LETTERS
run_20261020T162512Z-a8bab96e_adhoc  listings  succeeded  2026-10-20T16:25:12Z  2      0
run_20261020T162508Z-f9927400_adhoc  listings  succeeded  2026-10-20T16:25:08Z  2      0
```

Open <http://127.0.0.1:7070/> and look at:

- **Graph** (`/p/listings/graph`): the steps as cards with what is in flight, throughput, p95 latency and error rate,
  the streams between them, and lenses for errors, latency and queues. The strip below sums up the latest run.
- **Runs**: open a run for its timeline, one bar per step, working versus waiting, and its report: critical path,
  slowest steps, failures grouped by cause, cost.
- **Streams and items**: browse a stream and open an item. Its page shows the item's journey (each step it went
  through, every attempt), its payload, artifacts and lineage up to the trigger and down to what it produced.
- **Dead letters**: grouped by error, with replay and discard.

Stop the engine with Ctrl-C: it drains what is in flight and exits.

## 5. Add steps

Replace the placeholder with a crawler, a deduplication, a price filter and routing.

### A crawler

`init action` writes the same Go template under a new name. Make it a crawler that emits four listings a day from a
pool of seven, so consecutive days overlap, keyed by listing id. One listing has no price ("price on request").

```sh
rm -r actions/greet
millstream init action crawl --dir actions
cd actions/crawl
rm -r evals
```

Replace `action.yaml`:

```yaml
name: listings/crawl
version: 0.1.0
description: Emits the day's listings from a small built-in pool, keyed by id
kind: exec
effects: none
input: { type: object }
output:
  type: object
  properties:
    id: { type: string }
    city: { type: string }
    priceEur: { type: integer }
    surfaceM2: { type: integer }
    condition: { enum: [good, fair, poor] }
  required: [id, city, surfaceM2, condition]
config:
  type: object
  properties:
    day: { type: integer, minimum: 1 }
  required: [day]
runtime: { exec: { command: [crawl] } }
```

Replace `main.go`. `MapKeyed` emits several outputs per input; each needs a distinct key, which also makes the
items' ids deterministic, so a retried delivery publishes the same items again rather than new ones.

```go
// Command crawl emits the listings of a day: four from a pool of seven, so consecutive days overlap.
package main

import (
	"encoding/json"

	millstream "github.com/monolithiclab/millstream/sdk/go"
)

type Listing struct {
	ID        string `json:"id"`
	City      string `json:"city"`
	PriceEur  int    `json:"priceEur,omitempty"` // 0: price on request, left out
	SurfaceM2 int    `json:"surfaceM2"`
	Condition string `json:"condition"`
}

var pool = []Listing{
	{"L1", "Lyon", 185000, 48, "poor"},
	{"L2", "Lyon", 420000, 95, "good"},
	{"L3", "Lyon", 240000, 62, "fair"},
	{"L4", "Lyon", 0, 70, "poor"},
	{"L5", "Lyon", 210000, 55, "poor"},
	{"L6", "Lyon", 150000, 31, "good"},
	{"L7", "Lyon", 275000, 80, "poor"},
}

type Config struct {
	Day int `json:"day"`
}

func main() {
	millstream.MapKeyed(func(c millstream.Context, _ json.RawMessage) ([]millstream.Keyed[Listing], error) {
		var cfg Config
		if err := c.ConfigInto(&cfg); err != nil {
			return nil, err
		}
		var out []millstream.Keyed[Listing]
		for i := cfg.Day - 1; i < cfg.Day+3 && i < len(pool); i++ {
			out = append(out, millstream.Keyed[Listing]{Key: pool[i].ID, Value: pool[i]})
		}
		return out, nil
	})
}
```

Replace `tests/example.yaml` with a fixture for day 2:

```yaml
name: day 2 starts at L2
input: {}
config: { day: 2 }
expect:
  outputs:
    - { key: L2, payload: { id: L2, city: Lyon, priceEur: 420000, surfaceM2: 95, condition: good } }
    - { key: L3, payload: { id: L3, city: Lyon, priceEur: 240000, surfaceM2: 62, condition: fair } }
    - { key: L4, payload: { id: L4, city: Lyon, surfaceM2: 70, condition: poor } }
    - { key: L5, payload: { id: L5, city: Lyon, priceEur: 210000, surfaceM2: 55, condition: poor } }
```

Build and test it:

```sh
go mod edit -replace github.com/monolithiclab/millstream/sdk/go=$MILLSTREAM_REPO/sdk/go
go mod tidy
go build -o bin/$(go env GOOS)-$(go env GOARCH)/crawl .
millstream test action .
cd ../..
```

```text
PASS day 2 starts at L2
listings/crawl: 1 passed, 0 failed
```

### Builtins, variables and routing

Replace `pipeline.yaml`:

```yaml
pipeline: listings
description: Sources renovation opportunities from the day's listings

vars:
  day: 1
  maxPrice: 300000

triggers:
  adhoc: { manual: {} }

steps:
  crawl:
    action: ./actions/crawl
    in: trigger.adhoc
    out: listings.raw
    config: { day: "${vars.day}" }

  dedupe:
    action: dedupe
    in: listings.raw
    out: listings.unique
    config: { key: item.id, window: 30d }

  affordable:
    action: filter
    in: listings.unique
    config: { expr: "item.priceEur <= vars.maxPrice" }
    out:
      - { when: "item.condition == 'poor'", out: listings.renovate }
      - { when: "true", out: listings.ready }
```

- `vars` are named values with defaults. `${vars.day}` in a config is interpolated once per run, and keeps the
  variable's type.
- `dedupe` and `filter` are builtins: their config is [CEL](https://cel.dev) evaluated per item, over `item` and
  `vars`. `dedupe` lets an item through unless one with the same key passed in the last 30 days. `filter` lets it
  through when `expr` holds and marks it filtered otherwise.
- `out` as a list of route objects sends each output to the first route whose `when` holds. An output that matches
  none goes to the step's dead-letter stream as unrouted, never silently lost; the last route here catches the rest.
- Thresholds such as `maxPrice` belong in `filter` steps, not in actions, so an action stays reusable and every
  threshold is visible in the pipeline file.

Replace `tests/example.yaml` with a package test whose crawl is stubbed:

```yaml
name: a cheap poor listing is routed to renovation
payload: {}
stubs:
  crawl:
    outputs:
      - { key: a, payload: { id: a, city: Lyon, priceEur: 180000, surfaceM2: 40, condition: poor } }
      - { key: b, payload: { id: b, city: Lyon, priceEur: 250000, surfaceM2: 60, condition: good } }
      - { key: c, payload: { id: c, city: Lyon, priceEur: 900000, surfaceM2: 200, condition: poor } }
expect: { listings.unique: 3, listings.renovate: 1, listings.ready: 1 }
```

```sh
millstream check
millstream test pipeline
millstream vars
```

```text
warning steps.affordable.out[0].out: stream listings.renovate is written by step affordable but no step reads it
warning steps.affordable.out[1].out: stream listings.ready is written by step affordable but no step reads it
listings: 0 errors, 2 warnings
PASS a cheap poor listing is routed to renovation
listings: 1 passed, 0 failed
NAME      VALUE   SOURCE   ENVIRONMENT
day       1       default  MILLSTREAM_VAR_DAY
maxPrice  300000  default  MILLSTREAM_VAR_MAX_PRICE
```

`millstream vars` shows where each value comes from. A value is overridden by `--var name=value`, else by the
environment variable it names, else the default applies.

### Run it, twice

```sh
millstream run --once 2>/dev/null
millstream run --once --var day=2 2>/dev/null
```

```text
listings run_20261020T162558Z-7559f3e7_adhoc completedWithErrors
  affordable.dlq 1
  listings.raw 4
  listings.ready 1
  listings.renovate 1
  listings.unique 4
  trigger.adhoc 1
listings run_20261020T162559Z-902b5020_adhoc succeeded
  listings.raw 4
  listings.renovate 1
  listings.unique 1
  trigger.adhoc 1
```

Day 1 crawled L1 to L4: L1 went to `listings.renovate`, L3 to `listings.ready`, L2 was filtered (over
`maxPrice`), and L4, with no price, failed the filter's expression and was dead-lettered. Day 2 crawled L2 to L5:
`dedupe` held back the three listings already seen, and only L5 went on. The run summary counts the items on each
stream; in the UI, an item's journey shows where it was filtered and why (`Filtered · filtered: exprFalse`).

## 6. Operate

Start the engine again in the second terminal (`millstream run 2>../engine.log`) and drive it from the first.
Commands that change state write to the SQLite store directly, and the running engine picks the change up.

### Status, triggers and events

```sh
millstream trigger listings adhoc --var day=3
millstream status --limit 1
```

```text
run_20261020T162612Z-7689e2d2_adhoc
PIPELINE  STATE   REVISION  PAUSED STEPS
listings  active  2         -

RUN                                  PIPELINE  STATE      STARTED               ITEMS  OPEN DEAD LETTERS
run_20261020T162612Z-7689e2d2_adhoc  listings  succeeded  2026-10-20T16:26:12Z  7      0
```

The revision is 2: the engine stores each distinct package content as a revision, and every run records the one it
used. There is no version field to bump.

Every state change is an event. `millstream tail` prints them as JSON lines, and `tail -f` keeps following:

```sh
millstream tail | grep runSettled | tail -1
```

```text
{"seq":117,"ts":"2026-10-20T16:26:17.508Z","pipeline":"listings","runId":"run_20261020T162614Z-960e8ee4_adhoc","kind":"runSettled","data":{"costUsd":0,"durationMs":2869,"state":"succeeded"}}
```

### Pause and resume

Pausing a step holds its deliveries; triggers still fire and upstream steps keep working.

```sh
millstream pause listings affordable
millstream trigger listings adhoc --var day=4
millstream status --limit 1
millstream resume listings affordable
```

```text
listings.affordable paused
run_20261020T162614Z-960e8ee4_adhoc
PIPELINE  STATE   REVISION  PAUSED STEPS
listings  active  2         affordable

RUN                                  PIPELINE  STATE    STARTED               ITEMS  OPEN DEAD LETTERS
run_20261020T162614Z-960e8ee4_adhoc  listings  running  2026-10-20T16:26:14Z  6      0
listings.affordable resumed
```

The run finishes once the step resumes. `millstream disable listings` stops the triggers too.

### Fix a dead letter and replay it

L4 is still a dead letter of the day 1 run. Fix the expression so that a listing without a price is filtered instead
of failing. In `pipeline.yaml`:

```yaml
    config: { expr: "has(item.priceEur) && item.priceEur <= vars.maxPrice" }
```

Apply the package, which makes it a new revision, then replay the run's dead letters with `--migrate`, which moves
the run to the active revision first. Without `--migrate` the replay runs on the run's own revision, and fails the
same way.

```sh
millstream apply .
millstream replay --pipeline listings --from dlq --run run_20261020T162558Z-7559f3e7_adhoc --migrate
millstream dlq list --state all
```

```text
listings revision 3 83b5897356a1
1 dead letter replayed
ID                                   PIPELINE  RUN                                  STEP        KIND  STATE      ERROR
it_88a91b75434734f59cc9057087b7f59d  listings  run_20261020T162558Z-7559f3e7_adhoc  affordable  dead  replayed   expr: filter of step affordable: evaluate "item.priceEur <= vars.maxPrice": no …
```

`replay` has two forms, `--from dlq` for dead letters and `--step` (next section), and takes one of them. The day 1
run is now `succeeded`, L4 filtered. `millstream rollback listings` reactivates the previous revision if a change
goes wrong.

### Replay a step over past items

`replay --step` starts a **replay run** of one step over the items its input streams received, here those of the last
hour, with a variable changed for this run only:

```sh
millstream replay --pipeline listings --step affordable --since 1h --var maxPrice=200000
millstream run report listings run_replay_20261020T162620Z-b9df9171
```

```text
run_replay_20261020T162620Z-b9df9171
Run run_replay_20261020T162620Z-b9df9171 of listings (replay): succeeded
Started 2026-10-20T16:26:20Z, settled 2026-10-20T16:26:21Z, 540ms, cost $0.0000
Items: listings.ready 1, listings.renovate 1

Critical path (540ms):
  STEP        DELIVERY                             LATENCY  WAIT   WORK  BACKOFF  SHARE
  affordable  dl_719ce262d713c5774527c64ea86e39e2  540ms    540ms  0s    0s       100%
...
```

At 200,000 EUR only L1 (renovate) and L6 (ready) pass. This is how you see what a threshold change would do before
you apply it, and how a new step catches up on past items, as the next section shows. Stop the engine with Ctrl-C.

## 7. Add an agent step

An agent step scores each listing to renovate. Its action has `kind: agent`: Millstream's agent adapter renders
`prompt.md` with the item, calls the model through a **harness** (`api` for a direct model call, `claude-code` for
Claude Code), asks for a JSON answer, validates it against the output schema, and records the transcript, tokens and
cost.

To stay offline, the agent talks to a stand-in for an OpenAI-compatible endpoint that scores listings by price per
square metre. You record its answers once, then replay them with no server and no key, which is how Millstream's own
tests and CI run agents. With a real provider, only the manifest's `runtime.agent` and the secret change (below).

### A stand-in model

Outside the package, in `~/millstream-tutorial/fakemodel/main.go`:

```go
// Command fakemodel is a stand-in for an OpenAI-compatible model: it scores the listing a prompt describes, so the
// tutorial's agent step can be recorded without an API key.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
)

var listing = regexp.MustCompile(`Listing (\w+) in (\w+): (\d+) m², condition (\w+), asking (\d+) EUR`)

func main() {
	http.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model    string `json:"model"`
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Messages) == 0 {
			http.Error(w, "not a chat completion", http.StatusBadRequest)
			return
		}
		m := listing.FindStringSubmatch(req.Messages[0].Content)
		if m == nil {
			http.Error(w, "no listing in the prompt", http.StatusBadRequest)
			return
		}
		surface, _ := strconv.Atoi(m[3])
		price, _ := strconv.Atoi(m[5])
		perM2 := price / surface
		score := max(0, min(100, 100-(perM2-3000)/20)) // cheaper per square metre scores higher
		answer, _ := json.Marshal(map[string]any{
			"id": m[1], "city": m[2], "surfaceM2": surface, "condition": m[4], "priceEur": price,
			"score":  score,
			"reason": fmt.Sprintf("%d EUR per square metre in %s.", perM2, m[2]),
		})
		json.NewEncoder(w).Encode(map[string]any{
			"model": req.Model,
			"choices": []map[string]any{{"index": 0, "finish_reason": "stop",
				"message": map[string]string{"role": "assistant", "content": "```json\n" + string(answer) + "\n```"}}},
			"usage": map[string]int{"prompt_tokens": 300, "completion_tokens": 60},
		})
	})
	log.Println("fake model on http://127.0.0.1:8099/v1")
	log.Fatal(http.ListenAndServe("127.0.0.1:8099", nil))
}
```

Start it in a third terminal:

```sh
cd ~/millstream-tutorial/fakemodel
go mod init fakemodel
go run .
```

```text
2026/10/20 18:26:53 fake model on http://127.0.0.1:8099/v1
```

### The agent action

In the package, `actions/score/action.yaml`:

```yaml
name: listings/score
version: 0.1.0
description: Scores a listing as a renovation opportunity, with an LLM
kind: agent
input:
  type: object
  properties:
    id: { type: string }
    city: { type: string }
    priceEur: { type: integer }
    surfaceM2: { type: integer }
    condition: { enum: [good, fair, poor] }
  required: [id, city, surfaceM2, condition]
output:
  type: object
  properties:
    id: { type: string }
    city: { type: string }
    priceEur: { type: integer }
    surfaceM2: { type: integer }
    condition: { enum: [good, fair, poor] }
    score: { type: integer, minimum: 0, maximum: 100 }
    reason: { type: string }
  required: [id, city, surfaceM2, condition, score, reason]
runtime:
  agent:
    harness: api
    provider: openai
    baseUrl: http://127.0.0.1:8099/v1
    model: fake-scorer
    apiKeySecret: modelKey
    network: [127.0.0.1]
    maxCostUsd: 0.01
```

`actions/score/prompt.md`, a Go template over the item's `.Payload` (also `.Config` and `.Vars`):

```text
You score real estate listings as renovation opportunities, from 0 to 100.

Listing {{ .Payload.id }} in {{ .Payload.city }}: {{ .Payload.surfaceM2 }} m², condition {{ .Payload.condition }}, asking {{ .Payload.priceEur }} EUR.

Answer with the listing as given, every field unchanged, plus `score` (an integer) and `reason` (one sentence).
```

The adapter appends the output schema and asks for a final fenced JSON block. It also appends the step's
**lessons**, rules and examples kept in `lessons/<step>.yaml` (this is where accepted improvements land, section 9).
Create `lessons/score.yaml`:

```yaml
rules:
  - text: A listing under 35 m² is hard to resell after works, so lower its score by 10 points.
    source: tutorial
    added: 2026-10-20
```

### The environment file

`millstream.yaml` at the package root says where the package runs; it is not part of the package. Agents run in a
container sandbox by default, which needs Docker and a Linux build of the binary. On a laptop, allow them to run as
child processes instead (logged at every start), and give the model a price so cost is computed from tokens:

```yaml
agents: { allowUnsandboxed: true }
prices: { fake-scorer: { inPerMtok: 3, outPerMtok: 15 } }
```

### The steps

Add to `pipeline.yaml`: a `minScore` variable and the secret under `vars`, then the agent step, a threshold filter
and a run budget.

```yaml
vars:
  day: 1
  maxPrice: 300000
  minScore: 50

secrets: [modelKey]
```

```yaml
  score:
    action: ./actions/score
    in: listings.renovate
    out: listings.scored
    concurrency: 2

  keep-good:
    action: filter
    in: listings.scored
    out: listings.shortlist
    config: { expr: "item.score >= vars.minScore" }

budget:
  runUsd: 1
```

A pipeline names its secrets; their values come from the environment. Without an environment file mapping them, a
secret reads `MILLSTREAM_SECRET_<NAME>`. `check`, `apply` and `test pipeline` need a secret declared (and, with an
environment file, mapped), not its value: the engine reads the value when it dispatches a delivery that needs it, and
a delivery that finds it unset fails, naming the variable to set.

```sh
millstream check
```

```text
warning steps.affordable.out[1].out: stream listings.ready is written by step affordable but no step reads it
warning steps.keep-good.out: stream listings.shortlist is written by step keep-good but no step reads it
listings: 0 errors, 2 warnings
```

Stub the agent in the package test too, so `test pipeline` never calls a model. Add to `tests/example.yaml`'s
`stubs`, and expect the shortlist:

```yaml
  score:
    outputs:
      - { payload: { id: a, city: Lyon, priceEur: 180000, surfaceM2: 40, condition: poor, score: 72, reason: cheap } }
expect: { listings.unique: 3, listings.renovate: 1, listings.ready: 1, listings.shortlist: 1 }
```

### Record

`MILLSTREAM_HARNESS_MODE` selects `live` (the default), `record` or `replay`, and `MILLSTREAM_HARNESS_DIR` where
recordings go; the engine passes both to the adapter, so set them where the engine runs. A recording is keyed by the
SHA-256 of the harness, the model, the rendered prompt and the output schema. Use an absolute directory. In the second
terminal:

```sh
export MILLSTREAM_SECRET_MODEL_KEY=unused MILLSTREAM_HARNESS_MODE=record MILLSTREAM_HARNESS_DIR=$PWD/recordings
millstream run 2>../engine.log
```

Every listing has been crawled already, and `dedupe` holds back the ones it has seen, so a new run would give the
agent nothing. Replay the new step over the past instead, from the first terminal:

```sh
millstream replay --pipeline listings --step score --since 1d
millstream run report listings run_replay_20261020T162704Z-cfc54e94
ls recordings
```

```text
run_replay_20261020T162704Z-cfc54e94
Run run_replay_20261020T162704Z-cfc54e94 of listings (replay): succeeded
Started 2026-10-20T16:27:04Z, settled 2026-10-20T16:27:06Z, 1.057s, cost $0.0072
Items: listings.scored 4, listings.shortlist 4
...
a08350c0f7b623d6ed61c298c56190d2a72dc7dc449b06043669c8d4512bc5e3.json
cfc429981652d5b310018d6366ab0756d2ea84f96a49218c924d91de879cc8e1.json
dcbf4f9a08a6a96837f7390becbe57fba8234aa4f7d6437cc90d2a85553055e8.json
```

Four items, three recordings: L1, L5 and L7 reached `listings.renovate`, L1 twice (in the day 1 run and in your
`--var maxPrice=200000` replay run), and the same prompt is one recording. Each call cost $0.0018 from the price
table. In the UI, open one of the items on `listings.scored` and expand the `score` row of its journey: the attempt
shows the model, tokens and cost, and the full prompt as sent, the lessons section included.

### Evals

An agent action can ship eval cases: an input and CEL assertions over `output`, `input` and `vars`, optionally an LLM
judge with a rubric. `actions/score/evals/cheap-flat.yaml`:

```yaml
input: { id: E1, city: Lyon, priceEur: 150000, surfaceM2: 50, condition: poor }
assert:
  - output.score >= 70
  - output.id == input.id
```

`eval` replays by default, from `actions/score/evals/recordings`; record the case once while the stand-in runs. The
`api` harness sends a key whenever it calls a model, so give it the placeholder:

```sh
MILLSTREAM_SECRET_MODEL_KEY=unused millstream eval actions/score --harness-mode record
```

```text
CASE        ASSERTIONS  JUDGE  COST     SCORE
cheap-flat  2/2         -      $0.0018  1.00
TOTAL                                   1.00
```

With `--db` (or an environment file naming the store), scores are recorded, and each run shows the case's previous
score: compare prompts and models before they ship.

### Replay offline

Stop the engine (Ctrl-C) and the stand-in model. Restart the engine in replay mode, without the key:

```sh
unset MILLSTREAM_SECRET_MODEL_KEY
export MILLSTREAM_HARNESS_MODE=replay MILLSTREAM_HARNESS_DIR=$PWD/recordings
millstream run 2>../engine.log
```

```sh
millstream replay --pipeline listings --step score --since 1d
millstream eval actions/score
```

```text
run_replay_20261020T162756Z-332259af
CASE        ASSERTIONS  JUDGE  COST     SCORE
cheap-flat  2/2         -      $0.0018  1.00
TOTAL                                   1.00
```

The replay run succeeds with no model running and no key: a replay never calls the model. A prompt that changed
misses its recording, and the delivery fails without retry, naming the key:

```text
cheap-flat: error: the action failed: replay: no recording 627ebf3b880e…3c6b for harness api and model fake-scorer in /Users/you/millstream-tutorial/listings/actions/score/evals/recordings: record it with MILLSTREAM_HARNESS_MODE=record
```

Lessons are rendered into the prompt, so editing them misses the old recordings too, rather than replaying a stale
answer. The recordings live in the package, so they ship with it and replay in CI.

### Budgets

`maxCostUsd` caps each delivery: an attempt that costs more fails with a `budget:` error, not retried. Lower it to
0.001 in the manifest and run the eval:

```text
cheap-flat: error: the action failed: budget: the attempt cost $0.0018, over the action's maxCostUsd of $0.001 (the attempt's transcript is its transcript.json artifact)
```

Put it back to 0.01. `budget.runUsd` and `budget.dayUsd` cap a run and a pipeline's day: once one is reached, agent
deliveries are held back and the pipeline's agent steps pause with the cause shown by `millstream status`, until
`millstream resume listings`. Under a budget every agent step must set `maxCostUsd`.

### With a real model

Recording against a real model costs money and needs its key. For Anthropic, drop `provider` and `baseUrl` (the
default provider is `anthropic`), name a real model, set `network: [api.anthropic.com]`, export the key as
`MILLSTREAM_SECRET_MODEL_KEY`, and add the model's price to `prices`. Run with `MILLSTREAM_HARNESS_MODE=record` to keep
the answers for replay, or leave the variable unset for live calls. `harness: claude-code` drives the `claude` CLI
instead, logged in on the machine.

## 8. Add a human review

The `review` builtin parks each item in an inbox until someone approves or rejects it. Add to `pipeline.yaml`, before
`budget`:

```yaml
  analyst-review:
    action: review
    in: listings.shortlist
    out: listings.approved
    config:
      inbox: analysts
      timeout: 72h
      editable: [/score]
      reasons: [overpriced, bad-area]
      display: "{{ .item.id }}: {{ .item.surfaceM2 }} m² at {{ .item.priceEur }} EUR, score {{ .item.score }}"
```

`editable` lists the JSON pointers a reviewer may change before approving, `reasons` the codes a rejection must give,
`display` a one-line template for the inbox. After `timeout` an item is rejected (`onTimeout` changes that), and
`autoApprove` takes a CEL expression for the items a rule may approve on its own, a sample of which still reaches a
person.

The engine is still running in replay mode. Apply the package and send the scored listings through again:

```sh
millstream test pipeline
millstream apply .
millstream replay --pipeline listings --step score --since 1d
millstream status --limit 1
millstream inbox
```

```text
PASS a cheap poor listing is scored and shortlisted
listings: 1 passed, 0 failed
listings revision 6 f7e8e74de2c9
run_replay_20261020T162814Z-cc272e1e
PIPELINE  STATE   REVISION  PAUSED STEPS
listings  active  6         -

RUN                                   PIPELINE  STATE          STARTED               ITEMS  OPEN DEAD LETTERS
run_replay_20261020T162814Z-cc272e1e  listings  waitingReview  2026-10-20T16:28:14Z  8      0
DELIVERY                             PIPELINE  STEP            INBOX     ITEM  PARKED                DEADLINE              DISPLAY
dl_319ef7f15ebfe412643004daef54835f  listings  analyst-review  analysts  L5    2026-10-20T16:28:15Z  2026-10-23T16:28:15Z  L5: 55 m² at 210000 EUR, score 60
dl_4e650b2631bf16871fbf3a806f0664b6  listings  analyst-review  analysts  L1    2026-10-20T16:28:15Z  2026-10-23T16:28:15Z  L1: 48 m² at 185000 EUR, score 58
dl_10e2b8dac5c2cda48768323670c8dc18  listings  analyst-review  analysts  L7    2026-10-20T16:28:15Z  2026-10-23T16:28:15Z  L7: 80 m² at 275000 EUR, score 79
dl_1a3bf49e9f6088ea853eb8d150e1cb52  listings  analyst-review  analysts  L1    2026-10-20T16:28:15Z  2026-10-23T16:28:15Z  L1: 48 m² at 185000 EUR, score 58
```

The run waits on people: `waitingReview`. A parked item holds no slot, and `run --once` stops waiting at this point
(and exits 1, since the run has not succeeded yet).

In the UI, **Inbox** lists the reviews oldest first: `j` and `k` move, the editable fields are inputs, `a` approves
and `r` rejects with a reason. From the CLI:

```sh
millstream inbox show dl_10e2b8dac5c2cda48768323670c8dc18
millstream inbox approve dl_10e2b8dac5c2cda48768323670c8dc18 --set /score=85 --text "Large flat, worth it"
millstream inbox reject dl_319ef7f15ebfe412643004daef54835f
millstream inbox reject dl_319ef7f15ebfe412643004daef54835f --reason bad-area
```

```text
Review of delivery dl_10e2b8dac5c2cda48768323670c8dc18 (listings/analyst-review), version 1: open
Inbox: analysts
...
Editable: /score
Reasons: overpriced, bad-area

L7: 80 m² at 275000 EUR, score 79
...
Deciding version 1 of the review, its current one: pass --version to decide the version you saw.
dl_10e2b8dac5c2cda48768323670c8dc18 approved (delivery done)
Deciding version 1 of the review, its current one: pass --version to decide the version you saw.
millstream: reviewDecision: a rejection needs a reason code: one of overpriced, bad-area: invalid request
Deciding version 1 of the review, its current one: pass --version to decide the version you saw.
dl_319ef7f15ebfe412643004daef54835f rejected (delivery filtered)
```

Decisions are recorded under `--as`, by default `$USER`. Approve the two L1 reviews, and the run settles:

```text
RUN                                   PIPELINE  STATE      STARTED               ITEMS  OPEN DEAD LETTERS
run_replay_20261020T162814Z-cc272e1e  listings  succeeded  2026-10-20T16:28:14Z  11     0
```

Its report shows three listings approved and the time the run spent waiting on the reviewer on its critical path.
Stop the engine with Ctrl-C.

## 9. Where to go next

- **Insights and run reports.** The engine analyses every pipeline on its own, with no LLM: slow steps, failure
  clusters, cost per approved item, review patterns. `millstream insights list`, `millstream run report`, and the
  Insights view show them; yours already lists the failed filter expression and the agent's cost:

  ```text
  ID                   PIPELINE  KIND            SCOPE       SEVERITY  STATE     UPDATED               SUMMARY
  in_81c1c912b20d93ff  listings  failureCluster  affordable  error     open      2026-10-20T16:28:09Z  1 failures (7% of attempts), 0% recovered on retry: expr: filter of step affordable: evaluate "item.priceEur <= vars.maxPrice": no such key: priceEur
  in_9fff78625564965e  listings  cost            score       info      open      2026-10-20T16:28:09Z  $0.0144 for 8 outputs ($0.0018 each, $- per approved item), 1 turns per delivery, 0 schema failures
  ```

  `millstream metrics` prints the rollups behind them, or a Prometheus exposition with `--prometheus`.
- **Self-improvement.** Production exports evidence (`millstream evidence export`), a development deployment imports
  it, and the `improve` pipeline drafts proposals from insights: new lessons, prompt and config changes, or code that
  replaces an agent step (`distill`), each validated by a backtest before a person accepts it (`millstream
  proposals`, the Improve view). It needs `improve.enabled` in a development environment file and a model for the
  drafting agent. Spec section 11 describes the loop and [the M5 exit](m5-exit.md) walks through it end to end.
- **Serving.** `millstream serve` runs an engine from an environment file, on Postgres, with token auth, a hot
  standby and remote nodes; `millstream apply` uploads packages to it. See [the deployment guide](deploy.md).
- **Other languages.** `millstream init action <name> --lang raw` scaffolds an action in Python 3 speaking the
  protocol with the standard library only, and `millstream test action . --conformance` checks any action against
  the protocol. [The action protocol](protocol.md) is the whole contract.
- **The design.** [The platform design](specs/20261014.platform-design.md) covers every key of the pipeline file
  (section 4), the execution model (5), actions (6), agents (7), review (8) and the CLI (13).

Command help: `millstream <command> --help`; `millstream run --help`, for one, lists `run [dir…]` with its flags and
`run report`.
