# Strand: platform design

Date: 2026-10-14. Status: draft for review. Working name: Strand.

Strand is a pipeline platform for processing work that mixes code, LLM agents and human decisions. A pipeline is
a graph of actions connected by persisted streams of items. It runs on a laptop with one binary, and unchanged on a
VPS or a Kubernetes cluster. Every pipeline is instrumented by default.

Decisions referenced as `Dn` are recorded in [`docs/decisions.md`](../../decisions.md).

## 1. Purpose

### 1.1 The problem

Most of the automation a forward-deployed engineering (FDE) team builds for a customer has the same shape: a chain
of `input → action → output` steps, where one step's output is the next step's input. Some steps are plain code
(crawl, compute, render), some are LLM agents (classify, score, extract, write), and some need a human to approve.

Built ad hoc, each project re-implements scheduling, fan-out, retries, error collection, cost tracking and a way to
see what happened. The result can't be reused at the next customer, and nobody can answer "how many listings did we
score yesterday, what did it cost, and which ones failed?" without reading logs.

### 1.2 What Strand is

- **A standard way to define pipelines:** a declarative file describing triggers, streams and steps, validated
  before it runs.
- **A standard action contract:** any program that speaks a small JSON-lines protocol is an action, whatever its
  language, and agents are actions like any other.
- **A durable, observable runtime:** every item is persisted, every state change is an event, and metrics, lineage,
  logs and a live view come for free.
- **One engine from laptop to cluster:** the same pipeline file runs with SQLite on a laptop and with Postgres on a
  VPS or Kubernetes.

It is used by engineers to build and run customer pipelines on customer infrastructure. The engine is open source;
a commercial layer adds what larger organisations pay for (section 16).

### 1.3 Reference example

The running example in this spec, and the end-to-end acceptance test of the platform:

1. Every day, crawl new real estate listings matching a set of filters, from several sources.
2. Score each listing.
3. For each listing scoring above a threshold, compute rentability: fees, renovation work, resale margin.
4. For each listing whose margin clears a threshold, have an analyst approve it.
5. Generate a PDF report for each approved listing.

Later, a deduplication step is inserted between crawling and scoring without touching the other steps.

### 1.4 Success criteria

- The reference pipeline runs end to end on a laptop with `strand run`, with no service to install.
- Killing the engine or any worker at any point loses no item and duplicates no item downstream.
- Inserting a step is a change of a few lines in the pipeline file, and past items can be replayed through it.
- For any pipeline, the UI answers: what is running now, how many items went through each step, what failed and
  why, what each agent step cost, and how often a human intervened.
- The same pipeline file runs on a laptop, a VPS and Kubernetes; only the environment file differs.

## 2. Scope

### 2.1 Goals for v1

- Declarative pipeline file with variables, secrets and CEL expressions; static validation.
- Durable item log with at-least-once delivery and idempotent item identity.
- Action runtimes: local executable, container, LLM agent (harness-agnostic), human review, builtins.
- Cron, manual and webhook triggers.
- Retries, dead letters, replay, pause and resume.
- Metrics, logs, lineage and traces built in; Prometheus and OpenTelemetry export.
- A web UI with a live pipeline graph, run timelines, item browsing, logs, metrics and a review inbox.
- Laptop, VPS and Kubernetes deployments.

### 2.2 Non-goals for v1

- A graphical pipeline editor. The pipeline file in git is the source of truth (D7).
- Multi-tenant SaaS. Each customer gets their own deployment (D6).
- Exactly-once side effects on external systems. Strand passes an idempotency key; the external system enforces it.
- High availability of the engine. One engine is active per deployment; leader election on Postgres comes later.
- Sub-second, high-throughput stream processing. The design target is 1,000 items per second per engine on
  Postgres and 300 on SQLite, far above what agent steps need.
- Streams shared across pipelines.

## 3. Core concepts

| Concept | Definition |
| --- | --- |
| **Item** | The unit of work: a JSON payload and an envelope (section 5.2). Binaries are stored as artifacts and referenced, never inlined. |
| **Stream** | A named, persisted, typed channel of items. Steps read from and write to streams; they never reference each other. |
| **Action** | A reusable, versioned implementation described by a manifest: input, output and config schemas, a kind and a runtime. |
| **Step** | One use of an action in a pipeline: its source (triggers or streams), output stream, config and execution policy. |
| **Trigger** | What starts a run: `cron`, `manual` or `webhook`. |
| **Run** | One firing of a trigger. Every item descends from exactly one run. |
| **Pipeline** | One file declaring variables, secrets, triggers, streams and steps. |

Because steps only know streams, the graph is defined by which steps write to and read from each stream:

- Several steps reading one stream each receive every item (fan-out).
- Several steps writing one stream merge their outputs (fan-in of producers, for example several crawlers).
- Inserting a step means pointing it at an existing stream and re-pointing one consumer to its output.

## 4. The pipeline file

### 4.1 Example

```yaml
pipeline: property-sourcing
description: Daily sourcing of renovation opportunities

vars:
  city: Lyon
  max_price: 400000
  min_score: 70
  min_margin_pct: 15

secrets:
  homesite_token: { env: HOMESITE_TOKEN }

triggers:
  daily: { cron: "0 6 * * *", tz: Europe/Paris }

streams:
  listings.raw: { retention: 90d }

steps:
  crawl-homesite:
    action: lab/crawl-homesite@1.2
    on: daily
    out: listings.raw
    config:
      filters: { city: "${vars.city}", max_price: "${vars.max_price}" }
      token: "${secrets.homesite_token}"
  crawl-classifieds:
    action: lab/crawl-classifieds@0.9
    on: daily
    out: listings.raw
    config:
      filters: { city: "${vars.city}", max_price: "${vars.max_price}" }

  dedupe:
    action: dedupe
    in: listings.raw
    out: listings.unique
    config: { key: "item.address_norm + ':' + string(item.surface_m2)", window: 30d }

  score:
    action: lab/score-listing@2
    in: listings.unique
    out: listings.scored
    concurrency: 8
  keep-good:
    action: filter
    in: listings.scored
    out: listings.shortlist
    config: { expr: "item.score >= vars.min_score" }

  rentability:
    action: ./actions/rentability
    in: listings.shortlist
    out: listings.priced
  keep-profitable:
    action: filter
    in: listings.priced
    out: listings.selected
    config: { expr: "item.margin_pct >= vars.min_margin_pct" }

  analyst-review:
    action: review
    in: listings.selected
    out: listings.approved
    config: { inbox: analysts, timeout: 72h, on_timeout: reject, editable: [/margin_pct, /notes] }

  report:
    action: lab/pdf-report@1
    in: listings.approved
    out: reports.pdf

budget:
  run_usd: 25
```

### 4.2 Top-level keys

| Key | Required | Meaning |
| --- | --- | --- |
| `pipeline` | yes | Identifier, unique in a deployment: lowercase letters, digits and `-`. |
| `description` | no | Shown in the UI. |
| `vars` | no | Named values usable in config interpolation and in CEL (section 4.4). |
| `secrets` | no | Named secret references (section 4.5). |
| `triggers` | yes | Map of trigger name to trigger definition (section 4.6). |
| `streams` | no | Per-stream settings: `schema` override, `retention`. Streams are otherwise declared implicitly by use. |
| `steps` | yes | Map of step name to step definition (section 4.3). |
| `budget` | no | Cost guards: `run_usd`, `day_usd` (section 7.5). |

A pipeline has no version field. The engine stores each distinct file content as a **revision** (its SHA-256, plus
the git commit when the file is in a clean git tree), and every run records the revision it used.

### 4.3 Step keys

| Key | Default | Meaning |
| --- | --- | --- |
| `action` | required | An action reference (section 6.2) or a builtin name. |
| `on` | — | Trigger name or list of names. A step has either `on` or `in`, never both. |
| `in` | — | Stream name or list of stream names. |
| `out` | — | Output stream. Omitted only for terminal steps. `route` declares its outputs in config instead. |
| `mode` | `map` | `map`: one item in, zero or more out, streamed. `collect`: all of a run's items in, emitted at the end. |
| `config` | `{}` | Validated against the action's config schema. Builtins take their parameters here too. |
| `concurrency` | `1` | Maximum deliveries of this step in flight. |
| `rate` | none | Maximum deliveries started per period, for example `10/min`. Shared by every step of the deployment that uses the same action. |
| `retry` | `{max_attempts: 3, backoff: 10s, max_backoff: 10m}` | Exponential backoff with jitter for retryable errors. |
| `timeout` | `10m` | Per delivery. On timeout the worker is killed and the error is retryable. |
| `resources` | from manifest | `memory`, `cpu`; enforced for containers, advisory for local executables (section 6.5). |
| `paused` | `false` | Starts the step paused. |

### 4.4 Variables and CEL

Expressions use [CEL](https://cel.dev) (D8): typed, side-effect free, bounded in cost, and with a mature Go
implementation.

- **Variables** are declared under `vars` with their default values. Values are resolved in this order, highest
  first: `--var name=value` on the command line, the environment overlay file named by the environment file
  (`vars.prod.yaml`), then the pipeline defaults. The resolved set is stored on each run.
- **Config interpolation.** A string in `config` may contain `${expr}`. The expression is CEL evaluated once when
  the run starts, over `vars`, `run` and `secrets`. A string made of a single `${expr}` takes the expression's type
  (`"${vars.max_price}"` is an integer); otherwise the result is spliced into the string.
- **Item expressions.** Builtins (`filter`, `route`, `dedupe`) evaluate CEL per item over `item`, `vars` and `run`.
  `item` is the payload; `envelope` gives `id`, `key`, `ts` and `attempt`.
- Action config never depends on `item`: the same config applies to every item of a run.
- Every expression is compiled and type-checked by `strand check`. Evaluation has a cost limit; exceeding it fails
  the item as non-retryable.

The CEL context:

| Name | Type | Available in |
| --- | --- | --- |
| `vars` | map | interpolation, item expressions |
| `run` | `{id, trigger, started_at, revision}` | interpolation, item expressions |
| `secrets` | map | interpolation only |
| `item` | the payload | item expressions only |
| `envelope` | `{id, key, ts, attempt, step}` | item expressions only |

### 4.5 Secrets

`secrets` maps a name to a source: `{ env: NAME }` or `{ file: path }` in v1; Vault and Kubernetes secrets later.
Secrets:

- are only usable in config interpolation, never in item expressions;
- are passed only to steps whose config references them;
- are redacted from logs, events, stored config and the UI, by value as well as by name.

### 4.6 Triggers

| Kind | Definition | Run input |
| --- | --- | --- |
| `cron` | `{cron: "<5-field spec>", tz: <IANA zone>}` | `{trigger, scheduled_at}` |
| `manual` | `{manual: {}}`, optional `input_schema` | The input given by `strand trigger` or the UI, validated |
| `webhook` | `{webhook: {path: /hooks/x}, input_schema}` | The request body, validated; authenticated by a per-trigger token |

Firing a trigger creates a run and one **trigger item** whose id is derived from the trigger name and the scheduled
time (cron) or a request id (manual, webhook). A cron trigger that fires twice for the same scheduled time, for
example after a restart, therefore creates one run, not two. Steps with `on: <trigger>` receive the trigger item.

A missed cron firing (engine down at the scheduled time) is fired once at startup if it is less than one period
late; older misses are recorded as skipped events.

### 4.7 Builtins

| Builtin | Config | Behaviour |
| --- | --- | --- |
| `filter` | `expr` | Emits the item unchanged when `expr` is true; otherwise marks the delivery `filtered`. |
| `route` | `routes: [{when, out}]`, `default` | Emits the item to the first matching route's stream, or to `default`, or filters it. |
| `dedupe` | `key`, `window` | Emits the item if no item with the same key passed this step within `window`; otherwise `filtered` with reason `duplicate`. The first one wins. |
| `collect` | none | Implies `mode: collect`: emits one item `{items: [...]}` per run. Used to hand a whole run to a non-collect action. |
| `review` | section 8 | Parks the item until a human decides. |
| `sink` | `to: file \| webhook \| artifacts`, target | Writes items out. A terminal step. |

Thresholds live in `filter` steps, not inside actions, so one scoring action serves every customer and every
threshold is visible in the pipeline file.

### 4.8 Validation

`strand check` runs before every `run`, `serve` reload and `apply`, and fails on:

- unknown actions, triggers or streams, or steps with neither or both of `on` and `in`;
- cycles in the step graph;
- streams that are read but never written, and `out` streams that nobody reads (warning only);
- config that does not match the action's config schema, after interpolation types are inferred;
- CEL expressions that do not compile or type-check;
- **schema incompatibility:** every property the consumer's input schema requires must be declared by every
  producer's output schema with the same JSON type, recursively. Extra producer properties are allowed. This
  structural check is deliberately simpler than full JSON Schema subsumption; every item is also validated against
  the consumer's input schema at runtime (section 5.5);
- an agent step that can reach a step whose action declares `effects: external` without a `review` or `filter`
  step in between (warning; section 14.4).

`strand graph` prints the resolved graph as Mermaid or DOT.

## 5. Execution model

### 5.1 Overview

The engine is a single process: a scheduler (triggers), a dispatcher (deliveries), a worker supervisor, the review
inbox, the HTTP API and the metrics projections, all over one **log store** (section 9). Workers are stateless and
disposable; the log store is the only state (D2).

### 5.2 Item identity and envelope

```json
{
  "id": "it_7f3a…",
  "key": "https://www.homesite.example/annonces/123",
  "run_id": "run_2026-10-14T06:00Z_daily",
  "stream": "listings.raw",
  "step": "crawl-homesite",
  "parents": ["it_19c2…"],
  "generation": 0,
  "ts": "2026-10-14T06:00:12.481Z",
  "payload": { "...": "..." },
  "artifacts": [{ "name": "photo-1.jpg", "sha256": "…", "size": 183402, "type": "image/jpeg" }]
}
```

Item ids are deterministic:

```text
id = hash(producing_step, input_item_id, generation, key ?? output_index)
```

- `key` is optional and set by the action on emit. An action that emits several items in a non-deterministic order
  (a crawler) must set `key` (the listing URL); the Go SDK requires it for multi-emit actions.
- When a delivery is retried, its re-emitted outputs get the same ids and are upserted, so downstream steps see
  each output once even though the action ran twice (D9).
- `generation` is bumped by a replay of that step, so a replay produces new items rather than colliding with the
  previous ones.

Outputs are committed as soon as they are emitted, not when the delivery completes, so a crawler's first listings
are scored while it is still crawling. Deterministic ids are what make this safe under retries.

### 5.3 Deliveries

A **delivery** is one (step, input item) pair. It is created when an item is appended to a stream the step reads,
or for a step added later, by a replay (section 5.7).

```text
pending → leased → done
                 → filtered          (builtin decided not to pass it on)
                 → awaiting_review   → done | filtered
                 → failed → pending  (retryable, attempts left, after backoff)
                          → dead     (non-retryable, or attempts exhausted)
```

- **Leases.** A leased delivery has an owner and an expiry, renewed every 10 seconds while the worker holds it.
  When the engine restarts or a remote worker disappears, expired leases return to `pending`. Lease duration
  defaults to 60 seconds.
- **Ordering.** Deliveries of one step are dispatched oldest first. No ordering is guaranteed across steps or across
  concurrent deliveries.
- **Dead letters.** A dead delivery's item is appended to the step's dead-letter stream `<step>.dlq` with the last
  error. Dead letters can be replayed (section 5.7).
- **Pause.** A paused step keeps receiving deliveries in `pending` and dispatches none.

### 5.4 Runs

A run is **settled** when every delivery of every item in the run is in a terminal state (`done`, `filtered`,
`dead`). Run states:

| State | Meaning |
| --- | --- |
| `running` | Some deliveries are pending, leased or failed-awaiting-retry. |
| `waiting_review` | Only deliveries in `awaiting_review` remain. |
| `succeeded` | Settled with no dead delivery. |
| `completed_with_errors` | Settled with at least one dead delivery. |
| `cancelled` | Cancelled by a user: pending deliveries are dropped, leased ones finish. |

### 5.5 Errors

| Class | Source | Handling |
| --- | --- | --- |
| Retryable | `{"type":"error","retryable":true}`, worker crash, timeout, agent output failing schema validation | Retry with backoff, then dead |
| Non-retryable | `retryable: false`, input failing the step's input schema, CEL cost limit exceeded | Dead immediately |
| Config | Interpolation failure at run start, missing secret | The run fails before any delivery; recorded as a run error |

Every error is stored with the attempt, the worker's last 200 log lines and, for agents, the transcript.

### 5.6 Collect steps

A `mode: collect` step on stream `S` fires once per run, when the run's part of the graph upstream of `S` is
settled: no item of that run is in a non-terminal delivery at any step that can, transitively, write to `S`. The
engine computes that upstream set from the graph. The action then receives the run's items on `S` as a batch
(section 6.3), paged from the log store, never all loaded in memory.

A collect step downstream of a `review` step therefore waits for every review of the run, bounded by the review
timeout.

### 5.7 Replay and topology changes

- **Applying a new revision** compares the step graph. New steps receive deliveries for items appended after the
  revision is applied. Removed steps stop receiving deliveries; their pending deliveries are dropped and counted.
  Changed steps (action, config) apply to deliveries dispatched after the change.
- **Replay** creates new deliveries, at a new generation, for a selection of items on a step:
  `strand replay --step score --since 7d`, `--from dlq`, `--run <id>`, `--where '<CEL>'`. This is how a newly
  inserted `dedupe` step is back-filled, and how last week's listings are re-scored with a new prompt.
- Replays are recorded as runs of kind `replay`, so their cost and results are tracked like any other run.

### 5.8 Backpressure

The log store absorbs bursts: a slow step builds a queue, visible as queue depth and oldest-pending age, and never
slows its producers. Throughput is bounded by step `concurrency`, action `rate` and an engine-wide worker cap
(`workers.max` in the environment file).

## 6. Actions

### 6.1 Manifest

Each action is a directory or OCI artifact containing `action.yaml`:

```yaml
name: lab/score-listing
version: 2.1.0
description: Scores a real estate listing for renovation potential
kind: agent               # exec | container | agent
effects: none             # none | external
input:  { $ref: schema/input.json }
output: { $ref: schema/output.json }
config: { $ref: schema/config.json }
lifecycle: per_item       # long (default for exec/container) | per_item (default for agent)
resources: { memory: 512Mi, cpu: "1" }
recycle: { max_items: 5000, max_rss: 400Mi }
runtime:
  agent:
    harness: claude-code   # claude-code | agent-sdk | api
    model: claude-sonnet-5-5
    prompt: prompt.md
    tools: [WebFetch]
    mcp: []
    network: [api.anthropic.com, "*.homesite.example"]
    max_turns: 20
    max_cost_usd: 0.50
```

Schemas are JSON Schema 2020-12. `effects: external` declares that the action changes something outside Strand
(sends email, writes to a CRM); it drives the safety check in section 4.8 and the idempotency key in section 6.3.

### 6.2 References and resolution

| Form | Example | Resolves to |
| --- | --- | --- |
| Local path | `./actions/rentability` | A directory next to the pipeline file |
| Registry | `lab/score-listing@2` | An OCI artifact; `@2` is a semver range, resolved to an exact version and digest |
| Builtin | `filter` | Compiled into the engine |

`strand lock` writes `strand.lock` next to the pipeline with each reference's exact version and digest; `run` and
`apply` use the lock and refuse a digest mismatch. The registry namespace maps to an OCI registry in the
environment file (`lab` → `ghcr.io/monolithiclab/strand-actions`).

### 6.3 The action protocol

Actions talk to the engine over stdin and stdout, one JSON object per line (D3). The protocol is published as its
own versioned document so third parties can implement it.

Engine to action:

```text
{"type":"hello","protocol":1,"action":"lab/score-listing","version":"2.1.0","config":{…},"vars":{…}}
{"type":"item","delivery":"dl_…","id":"it_…","key":"…","payload":{…},"artifacts":[…],"run":{…},"attempt":1,"idempotency_key":"…"}
{"type":"begin","delivery":"dl_…","run":{…}}          collect mode: start of a batch
{"type":"item",…}                                      collect mode: one per item, no delivery field
{"type":"end","delivery":"dl_…"}                       collect mode: end of the batch
{"type":"shutdown"}
```

Action to engine:

```text
{"type":"ready"}
{"type":"emit","delivery":"dl_…","key":"…","payload":{…},"artifacts":[{"name":"report.pdf","path":"out/report.pdf"}]}
{"type":"metric","delivery":"dl_…","name":"pages_fetched","value":12,"labels":{"source":"homesite"}}
{"type":"done","delivery":"dl_…"}
{"type":"error","delivery":"dl_…","message":"…","retryable":true}
```

- Anything written to stderr is a log line, tagged with the run, step, item and attempt.
- A long-lived worker handles one delivery at a time; concurrency comes from running several workers. This keeps
  actions simple to write.
- Artifacts are files written under the working directory the engine gives the worker
  (`STRAND_WORKDIR`); the engine moves them to the artifact store on `emit`. Input artifacts are materialised in the
  same directory before the `item` message.
- `idempotency_key` is `hash(step, item id, generation)`: stable across retries, new on replay.
- A protocol violation (malformed line, `done` for an unknown delivery) kills the worker; the delivery is retried.

### 6.4 Runtimes

| Kind | Laptop / VPS | Kubernetes |
| --- | --- | --- |
| `exec` | A child process | Not allowed: use `container` |
| `container` | `docker run -i` or Podman, stdio attached | A pod per worker; see below |
| `agent` | The agent adapter in a sandbox container (section 7) | Same, as a pod |
| builtins, `review` | In the engine | In the engine |

On Kubernetes the engine cannot attach to a pod's stdio. Each worker pod runs `strand worker` as its entrypoint: an
init container copies the static `strand` binary into a shared volume, and `strand worker` starts the action's own
command as a child, speaks the stdio protocol to it, and relays it to the engine over an authenticated HTTP/2
stream. Action images need no change.

### 6.5 Worker lifecycle and memory

- **Long-lived workers** (default for `exec` and `container`) handle deliveries one after another, which amortises
  start-up. Go actions are well suited to this.
- **Per-item workers** (default for agents) start fresh for each delivery, so no context leaks between items.
- **Recycling.** The supervisor samples each worker's RSS every 5 seconds. A worker that reaches `recycle.max_rss`
  or `recycle.max_items` is sent `shutdown` after its current delivery and replaced.
- **Hard limits.** Containers and pods get the manifest's memory and CPU limits. Local executables get
  `GOMEMLIMIT` set to 90% of `resources.memory` (honoured by Go SDK actions) and are killed above twice the value.
- Worker RSS, CPU, restarts and recycles are metrics, so a leak shows as a slope before it becomes a crash.
- **Engine memory.** The engine streams payloads from the log store and pages collect batches. Its RSS target is
  under 150 MB at the design throughput, independent of stream sizes, checked by a benchmark in CI.

### 6.6 SDKs

SDKs are thin wrappers over the protocol; writing an action without one is supported and documented.

```go
func main() {
    strand.Map(func(ctx strand.Context, in Listing) ([]Priced, error) {
        ctx.Metric("works_estimate_eur", float64(estimate))
        return []Priced{price(in, ctx.Config())}, nil
    })
}
```

- Go SDK in v1 (`strand.Map`, `strand.MapKeyed` for multi-emit with keys, `strand.Collect`), with schema
  generation from Go types.
- Python SDK after v1.

## 7. Agent actions

### 7.1 Harness-agnostic adapter

An agent action is run by the **agent adapter**, a program shipped with Strand that speaks the action protocol on
one side and drives an LLM harness on the other (D4). The engine never knows which model ran.

| Harness | How it runs |
| --- | --- |
| `claude-code` | Claude Code in headless mode, with the action's tools and MCP servers as an explicit allowlist |
| `agent-sdk` | The Claude Agent SDK, for actions that ship code around the agent loop |
| `api` | A direct model call with an adapter-run tool loop: Anthropic, Mistral, any OpenAI-compatible endpoint, or a local model |

For each delivery the adapter:

1. renders `prompt.md` (Go template) with the payload, config and vars;
2. runs the harness in the sandbox (section 7.3) with `max_turns` and `max_cost_usd`;
3. obtains structured output, using the harness's native structured-output support where it exists and a final
   fenced JSON block otherwise;
4. validates it against the output schema, and reports a retryable error if it does not match;
5. emits the result with metrics: `tokens_in`, `tokens_out`, `cost_usd`, `model`, `turns`, `duration_ms`.

Cost is taken from the harness when it reports it, otherwise computed from token counts and the price table in the
environment file.

### 7.2 Transcripts

The full transcript of each attempt (prompts, tool calls, results, final output) is stored as an artifact of the
delivery and shown in the item view. Secrets are redacted before storage.

### 7.3 Sandbox

Agents run in a container with a read-only filesystem except the working directory, a network allowlist taken from
`runtime.agent.network`, and only the declared tools and MCP servers. On a laptop without Docker, `claude-code`
and `api` agents may run unsandboxed only when the environment file sets `agents.allow_unsandboxed: true`, which is
logged at every start.

### 7.4 Evals

Each agent action may ship an `evals/` directory of cases: an input, and assertions on the output (CEL over the
output, or an LLM judge with a rubric). `strand eval <action>` runs them, reports a score per case, and stores the
result as metrics so prompt and model changes are compared before they ship. Record/replay of harness calls keeps
eval and test runs deterministic in CI (section 15).

### 7.5 Budgets

- `max_cost_usd` per delivery: the adapter stops the agent and reports a non-retryable error.
- `budget.run_usd` and `budget.day_usd` per pipeline: when reached, every agent step of the pipeline is paused, an
  event is recorded and the UI shows the cause. Resuming is a user action.

## 8. Human review

`review` is a builtin step that parks each item in an inbox until a person decides.

| Config | Meaning |
| --- | --- |
| `inbox` | Inbox name; access to it is controlled by RBAC in the commercial layer |
| `timeout` | How long an item may wait |
| `on_timeout` | `reject`, `approve` or `escalate: <inbox>` |
| `editable` | JSON pointers the reviewer may change before approving |
| `notify` | Slack webhook, email or generic webhook, with signed approve and reject links |
| `display` | Optional template rendering the item for the reviewer |

- **Approve** emits the payload, as edited, to `out`. **Reject** marks the delivery `filtered` with the reviewer's
  reason. Every decision records who, when, the reason and the diff of any edit.
- Reviews are made from the web inbox, `strand inbox`, or the signed notification links.
- Metrics: decisions by outcome, time to decision, edit rate, and **intervention rate** (reviewed items over items
  reaching the step), per step and per pipeline.

## 9. Storage

### 9.1 Log store

One interface, two implementations: SQLite (pure-Go driver, no cgo) for laptops, Postgres for servers (D2).

| Table | Content |
| --- | --- |
| `revisions` | Pipeline file content by hash, git commit, applied_at |
| `runs` | Id, pipeline, revision, trigger, kind (`trigger` or `replay`), resolved vars, state, timings, cost |
| `items` | Envelope and payload; immutable once written |
| `deliveries` | Step, item, generation, state, attempt, lease owner and expiry, next attempt at, last error |
| `events` | Append-only: every state change, with a monotonic sequence number |
| `reviews` | Decisions, reviewer, reason, edit diff |
| `dedupe_keys` | Step, key, first item, seen_at |
| `logs` | Worker log lines, tagged |
| `metric_rollups` | One-minute rollups per metric and label set |
| `artifacts` | Content hash, size, type, storage location, reference count |

Delivery leasing uses `SELECT … FOR UPDATE SKIP LOCKED` on Postgres and a single writer connection on SQLite.
Migrations are versioned and run at start-up. Before the first public release, the schema may change without
migrations (no compatibility shims before launch).

### 9.2 Artifacts

Artifacts are content-addressed (SHA-256) in a local directory or an S3-compatible bucket, and deduplicated by
hash.

### 9.3 Retention and erasure

- Retention is set per stream (payloads and artifacts), and globally for events, logs and rollups. Expired payloads
  are replaced by a tombstone; envelopes and lineage stay, so counts and lineage remain correct.
- `strand purge --key <key>` (or `--item`) erases an item, its descendants, their artifacts and logs, for GDPR
  erasure requests, and records a purge event without the erased data.

## 10. Observability

### 10.1 One source of truth

Every state change is an event in `events`. Metrics, lineage, the live view and the audit trail are projections of
that table, so they cannot disagree (D10).

### 10.2 Metrics

| Scope | Metrics |
| --- | --- |
| Step | items in, out, filtered, failed, dead; queue depth; oldest pending age; delivery latency (histogram); retries |
| Worker | RSS, CPU, starts, recycles, crashes |
| Agent | tokens in and out, cost, turns, schema-validation failures, by model |
| Review | decisions by outcome, time to decision, edit rate, intervention rate |
| Run | duration, items per stream, cost, state |
| Custom | any `metric` message from an action |

Metrics are rolled up per minute in the log store and queried by the UI, so a laptop needs nothing else. The engine
also exposes `/metrics` in Prometheus format.

### 10.3 Traces and logs

- OpenTelemetry traces follow lineage: a span per run, per item, per delivery attempt, exportable over OTLP.
- Logs are structured and stored with their tags; they can also be exported over OTLP.

## 11. API and web UI

### 11.1 HTTP API

A versioned REST API (`/api/v1`) with a published OpenAPI document. The UI and the CLI use only this API.

- Read: pipelines, revisions, runs, streams, items (with CEL filtering), item lineage, deliveries, logs, metrics,
  inbox.
- Actions: trigger a run, cancel a run, pause and resume a step, replay, review decisions, apply a revision.
- Live data over **server-sent events** (D5):
  - `/api/v1/live/pipelines/{p}`: per-step state (in flight, throughput, error rate, p95 latency, queue depth)
    coalesced at about 4 updates per second, whatever the item volume;
  - `/api/v1/live/runs/{r}`, `/api/v1/live/items/{i}` and `/api/v1/live/logs?…`: fine-grained events for one scope.
- Every SSE event carries the event sequence number, so a client reconnecting with `Last-Event-ID` resumes without
  gaps.

### 11.2 Web UI

A React and TypeScript single-page application, built with Vite and embedded in the binary (`go:embed`); `strand
run` serves it on localhost. Graphs use React Flow; time series use uPlot. The UI is localised in English and
French from the first commit and meets WCAG 2.2 AA.

| View | Content |
| --- | --- |
| Pipelines | List with state, last run, items today, cost today, dead letters |
| Pipeline graph | Steps and streams laid out automatically; live overlays: in flight, throughput, error rate, p95 latency, queue depth; edge thickness by volume; failing steps highlighted |
| Run timeline | A Gantt chart per step for one run: start, drain, waiting versus working time, what blocks the run |
| Streams and items | Browse a stream, filter with CEL; item detail with payload, artifacts preview (PDF, images), lineage tree up and down, every attempt with logs, agent transcript and cost |
| Logs | Live tail and search by run, step, item and level |
| Metrics explorer | Pick a metric, group by step, action, run or label, choose a range; saved views |
| Inbox | Review queue; approve, reject, edit then approve, with a diff of edits |
| Dead letters | Grouped by error; replay one, a selection or all |
| Revisions | Graph diff between two revisions; which revision each run used |

In v1 the UI changes nothing but runs, step pause state, replays and review decisions. Pipelines change through
their files.

## 12. CLI

| Command | Purpose |
| --- | --- |
| `strand init pipeline \| action <name>` | Scaffold a pipeline or an action (Go or raw protocol) |
| `strand check [file]` | Validate (section 4.8) |
| `strand graph [file]` | Print the graph as Mermaid or DOT |
| `strand run [file…]` | Run locally: engine, SQLite, UI on localhost; `--var`, `--once` (one run then exit) |
| `strand serve` | Run the engine as a server from the environment file |
| `strand apply <file…>` | Push a revision to a running engine |
| `strand trigger <pipeline> <trigger>` | Fire a manual trigger |
| `strand status`, `strand tail` | Pipelines and runs; live tail of events and logs |
| `strand replay` | Section 5.7 |
| `strand inbox` | List and decide reviews |
| `strand lock` | Write `strand.lock` |
| `strand test action \| pipeline` | Section 15 |
| `strand eval <action>` | Section 7.4 |
| `strand purge` | Section 9.3 |
| `strand worker` | Remote worker entrypoint (section 6.4) |

## 13. Deployment

The pipeline file is identical everywhere; the **environment file** `strand.yaml` describes where it runs:

```yaml
store: { postgres: { dsn: { env: STRAND_DSN } } }    # or { sqlite: { path: .strand/strand.db } }
artifacts: { s3: { bucket: strand-artifacts, endpoint: https://s3.fr-par.scw.cloud } }
runtime: { container: docker }                       # docker | podman | kubernetes
listen: 127.0.0.1:7070
auth: { token: { env: STRAND_TOKEN } }
workers: { max: 32 }
registries: { lab: ghcr.io/monolithiclab/strand-actions }
vars_overlay: vars.prod.yaml
retention: { events: 90d, logs: 30d, rollups: 400d }
prices: { claude-sonnet-5-5: { in_per_mtok: 3, out_per_mtok: 15 } }
```

| Target | Store | Workers | Packaging |
| --- | --- | --- | --- |
| Laptop | SQLite | Child processes, local Docker | `strand run`, defaults without an environment file |
| VPS | Postgres | Docker | Docker Compose file or systemd unit, shipped in the repo |
| Kubernetes | Postgres | Pods with `strand worker` | Helm chart: engine Deployment (one replica), RBAC to create worker pods |

## 14. Security

### 14.1 Tenancy

Single-tenant: one deployment per customer, on their infrastructure or on a dedicated VPS (D6). A deployment holds
several pipelines. Data never crosses deployments.

### 14.2 Access

- The API listens on localhost by default. Exposed, it requires a bearer token (open-source edition), or OIDC
  sessions with RBAC per pipeline and inbox (commercial edition).
- Webhook triggers each have their own token. Review notification links are signed and expire with the review.

### 14.3 Isolation

- `exec` actions run with the engine's privileges and are for trusted, local code only.
- Third-party and registry actions run as containers.
- Agents run in their sandbox (section 7.3).

### 14.4 Prompt injection

Crawled pages and documents reach agents, so agent output is treated as untrusted data:

- it is validated against the output schema before it goes anywhere;
- an agent gets only the tools, MCP servers and network destinations its manifest declares, and no secrets beyond
  its config;
- `strand check` warns when agent output can reach an `effects: external` step with no `review` or `filter`
  between them.

### 14.5 Supply chain

Registry actions are pinned by digest in `strand.lock`. Catalog actions are signed with cosign; the engine verifies
signatures on pull when `registries.<name>.verify` is set.

### 14.6 Personal data

Streams may hold personal data (a seller's name and phone number in a listing). Retention per stream, erasure by
key (section 9.3) and secret redaction are in the open-source core. Encryption at rest is delegated to the database
and the disk.

## 15. Testing

Testing is planned from the first commit and gated by `make ci`, which includes lint.

| Layer | Tests |
| --- | --- |
| Pipeline parsing and CEL | Unit and table tests; Go fuzzing of the parser and of interpolation |
| Delivery state machine | Property tests: random sequences of emit, done, error, crash, lease expiry, replay; invariants checked after each |
| Log store | The same suite against SQLite and Postgres (testcontainers) |
| Crash safety | Seeded runs of the reference pipeline with stub actions; `kill -9` of the engine and workers at random points; assert no item lost and none duplicated downstream |
| Action protocol | Conformance suite runnable against any action (`strand test action --conformance`) |
| Actions | `strand test action`: fixture inputs against golden outputs |
| Agent actions | Record/replay of harness calls in CI; `strand eval` for quality, run on demand and before releases |
| Pipelines | `strand test pipeline`: the graph with stubbed actions on in-memory SQLite, assertions on stream contents |
| Reference pipeline | End to end in CI with stub crawlers and a recorded agent |
| API | Contract tests against the OpenAPI document |
| UI | Vitest for components; Playwright end to end against a seeded engine, including the live SSE view; axe accessibility checks; a check that every string exists in English and French |
| Performance | Benchmarks for throughput (section 2.2) and engine RSS (section 6.5), compared against a saved baseline |

## 16. Packaging and editions

| Edition | Licence | Contents |
| --- | --- | --- |
| Open-source core | Apache-2.0 | Engine, CLI, Go SDK, builtins, agent adapter, the whole web UI (single user, token auth), Helm chart, protocol specification |
| Commercial | Commercial licence, separate repository | OIDC/SSO, RBAC per pipeline and inbox, audit export; a catalog of production actions and connectors (crawlers, CRM and ERP connectors, document generation, agent templates per vertical); a fleet control plane across customer deployments (inventory, upgrades, health); support |

The UI is in the core because it is what makes engineers adopt Strand and what makes it convincing to a customer.
Organisations pay for identity, governance, connectors and support (D11).

Distribution: release binaries and a Homebrew formula (goreleaser), a container image, the Helm chart, and actions
as OCI artifacts. A documentation site covers concepts, the pipeline file reference, the protocol specification, the
CLI and deployment guides.

## 17. Technology

| Area | Choice |
| --- | --- |
| Engine, CLI, adapter | Go, following the lab Go standard |
| Expressions | cel-go |
| SQLite | modernc.org/sqlite (pure Go, no cgo, cross-compiles) |
| Postgres | pgx |
| Cron | robfig/cron v3 |
| JSON Schema | santhosh-tekuri/jsonschema (2020-12) |
| Telemetry | OpenTelemetry Go SDK; Prometheus exposition |
| UI | React, TypeScript, Vite, React Flow, uPlot, Tailwind CSS, react-i18next |

Repository layout:

```text
cmd/strand/            CLI entrypoint
internal/pipeline/     file parsing, interpolation, validation, graph
internal/expr/         CEL environment
internal/store/        log store interface; sqlite/ and postgres/
internal/engine/       scheduler, dispatcher, run tracking, replay
internal/runtime/      exec/, container/, kubernetes/, worker supervisor
internal/agent/        agent adapter and harness drivers
internal/builtin/      filter, route, dedupe, collect, review, sink
internal/api/          REST, SSE, OpenAPI
internal/telemetry/    projections, rollups, Prometheus, OTLP
sdk/go/                Go SDK (separate module)
ui/                    web UI
deploy/                Compose file, systemd unit, Helm chart
docs/                  specs, plans, decisions, protocol specification
examples/property-sourcing/   the reference pipeline and its stub actions
```

## 18. Milestones

| # | Scope | Exit criterion |
| --- | --- | --- |
| M1 | Pipeline file, vars and CEL, `check`, `graph`, SQLite store, `map` steps, `exec` runtime, cron and manual triggers, retries and dead letters, Go SDK, `run`, `tail`, `status` | The reference pipeline with stub actions runs end to end on a laptop and passes the crash tests |
| M2 | Builtins, `collect` mode, agent adapter (`claude-code` and `api` with Anthropic, Mistral and OpenAI-compatible endpoints), transcripts, budgets, evals, metric rollups, replay, artifacts | Scoring runs with a real agent; inserting `dedupe` and replaying seven days works |
| M3 | Web UI: pipeline graph with live overlays, run timeline, streams and items with lineage, logs, metrics explorer, dead letters; `review` builtin, inbox and notifications | The full reference demo from the browser, in English and French, with no axe violation |
| M4 | Postgres store, `container` runtime, `serve`, `apply`, webhook triggers, token auth, Prometheus and OTLP, VPS packaging | A pipeline running unattended on a VPS for a week |
| M5 | Kubernetes runtime and Helm chart, `strand worker`, retention and purge, `strand.lock` and signature verification, documentation site, protocol specification | Open-source release |
| M6 | Commercial edition: OIDC and RBAC, first catalog actions, fleet control plane | First commercial deployment |

Each milestone gets its own implementation plan.

## 19. Prior art and positioning

| Tool | What it is | Why Strand differs |
| --- | --- | --- |
| Temporal | Durable execution of code-first workflows | Item streams and declarative graphs fit poorly; a laptop needs the dev server; our standard would be a layer on someone else's engine (D1) |
| Airflow, Dagster, Prefect | Python data orchestration | Batch and task oriented rather than item streaming; Python-centric; no agent or human step model |
| Argo Workflows | Kubernetes-native workflows | Kubernetes only |
| n8n | Low-code automation | Visual-first, sustainable-use licence, not built for engineers' code or durable item lineage |
| Redpanda Connect (Benthos) | Declarative stream processing in Go | Stream ETL, with no agents, humans, runs or lineage; part of it is now commercially licensed |
| Inngest, Trigger.dev | Event-driven functions | Function-centric, tied to their hosted models |
| LangGraph, CrewAI | In-process agent frameworks | They orchestrate inside one agent; they can run inside a Strand agent action |

Strand's position: declarative item-stream pipelines that mix code, agents and people in one graph; one binary from
laptop to Kubernetes; lineage, evals, cost and human-intervention rate built in; an open action protocol any
language can implement.

## 20. Key decisions

| Id | Decision |
| --- | --- |
| D1 | Build a Go engine rather than build on Temporal or assemble stream tools |
| D2 | A durable item log (SQLite or Postgres) is the only state; at-least-once delivery |
| D3 | Actions speak JSON lines over stdio |
| D4 | Agent actions are harness-agnostic, run by an adapter |
| D5 | Live UI data over server-sent events, not WebSocket |
| D6 | Single-tenant deployments in v1 |
| D7 | The pipeline file is the source of truth; no graphical editor in v1 |
| D8 | CEL for every expression |
| D9 | Deterministic item ids make retries idempotent downstream |
| D10 | One event table drives metrics, lineage, live view and audit |
| D11 | Open-source core includes the whole UI; the commercial edition sells identity, governance, connectors and fleet operations |
