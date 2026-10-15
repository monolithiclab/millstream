# Decisions

Each entry records what was decided, the evidence, and the cost if it turns out wrong. The design they belong to is
[the platform spec](specs/20261014.platform-design.md).

Until the first public release this log is kept current: entries are edited to match the design, and superseded
ones are removed. From the first release on, it becomes append-only, and a changed decision is superseded by a new
entry rather than edited.

## Foundations

### D1. Build our own Go engine (2026-10-14)

**Decision.** Millstream is its own engine in Go, over a durable item log. It is not built on Temporal, and not
assembled from stream tools such as Redpanda Connect.

**Evidence.** The pipeline file and the action protocol are the product, and they must be ours to standardise and
open-source. Temporal is shaped around code-first workflows: per-item streaming and fan-out are awkward and costly
there, and a laptop needs its dev server. Stream ETL tools have no model for agents, human review, runs or lineage.
The hard part we take on, durable leased deliveries, is a well-known pattern on SQLite and Postgres.

**Cost if wrong.** We own crash recovery and leasing bugs that Temporal would have solved. Mitigated by the crash
tests at every atomic boundary (spec section 16) from M1.

### D2. A durable item log is the only state (2026-10-14)

**Decision.** Every item between steps is persisted in SQLite (laptop) or Postgres (server). Delivery is
at-least-once. Workers and nodes are stateless.

**Evidence.** Replay of a step over past items, crash resume, lineage and audit all need persisted items. In-memory
channels lose in-flight work on a crash and make "insert a step and back-fill it" impossible.

**Cost if wrong.** Throughput is bounded by the database (D25). A deployment that needs much more would need a
different store behind the same interface.

### D3. Actions speak JSON lines over stdio (2026-10-14)

**Decision.** An action is any program that reads items on stdin and writes emits, metrics and results on stdout,
one JSON object per line; stderr is logs.

**Evidence.** Any language works with no SDK. Local processes and `docker run -i` need no network setup.

**Cost if wrong.** Large payloads go through pipes; artifacts are passed as files to keep that bounded. A gRPC
transport could be added later as a second protocol binding.

### D4. Harness-agnostic agent actions (2026-10-14)

**Decision.** Agent actions are run by an adapter that drives Claude Code headless, the Claude Agent SDK, or a
direct model API (Anthropic, Mistral, OpenAI-compatible, local). The engine never knows which model ran.

**Evidence.** Customers differ on which providers they accept; model choice per step, including sovereign or local
models, is a selling point. Agents then get the same retries, metrics and lineage as code.

**Cost if wrong.** Harness-specific features (subagents, skills) are reachable only through the `claude-code`
harness, and each harness needs its own driver and tests.

### D5. Server-sent events for live UI data (2026-10-14)

**Decision.** The live view uses SSE. User actions from the UI are plain REST calls.

**Evidence.** Live data only flows from server to browser. SSE reconnects natively, resumes with `Last-Event-ID`
over the event sequence, and passes through proxies and ingresses as plain HTTP.

**Cost if wrong.** A future need for two-way streaming (collaborative editing) would add WebSocket alongside.

### D6. Single-tenant deployments (2026-10-14)

**Decision.** One deployment per customer, on their infrastructure or a dedicated VPS. A deployment runs many
pipelines of that one organisation. No multi-tenant SaaS in v1.

**Evidence.** Forward-deployed work keeps customer data on customer infrastructure. Tenancy isolation, billing and
certifications are a product of their own.

**Cost if wrong.** A hosted offer later needs tenant isolation added to the store and API.

### D7. The pipeline package is the source of truth (2026-10-14)

**Decision.** Pipelines change through their packages, in git, applied with `millstream apply`. The v1 UI visualises
and operates; it does not edit pipelines.

**Evidence.** Files diff, review and version like code; a revision hash ties every run to the exact definition.

**Cost if wrong.** Less technical users need an engineer to change a pipeline until a graphical editor, writing
YAML back, exists.

### D8. CEL for every expression (2026-10-14)

**Decision.** Filters, route conditions, dedupe keys, config interpolation and item queries all use CEL.

**Evidence.** CEL is typed and checked before running, side-effect free, cost-bounded, and has a mature Go
implementation; one language everywhere means one thing to learn.

**Cost if wrong.** CEL is less familiar than JavaScript or JSONPath; documentation needs good examples.

### D9. Deterministic, immutable items (2026-10-14)

**Decision.** `id = hash(producing step, output stream, input item id, generation, key or output index)`. Items are
immutable and the first commit of an id wins: a re-emit with the same payload is a no-op, a different payload is
discarded and recorded as nondeterministic. A delivery with several outputs must key every one
of them.

**Evidence.** A retried delivery re-emits the same ids, so downstream steps see each output once. Replay allocates a
new generation to produce new items. Upserting would let a retry rewrite an item a consumer already processed.

**Cost if wrong.** An action whose output changes between attempts keeps its first answer; the nondeterminism
metric makes that visible.

### D10. One event table drives every projection (2026-10-14)

**Decision.** Every state change is appended to `events`; metrics, lineage, the live view and audit are derived
from it.

**Evidence.** Separate counters drift and disagree; one ordered source cannot.

**Cost if wrong.** The table grows fast; time partitioning, retention and rollups keep it bounded.

### D11. Open-source core includes the whole UI (2026-10-14)

**Decision.** Apache-2.0 core: engine, CLI, SDK, builtins, agent adapter and the full web UI for a single user.
Commercial edition: SSO and RBAC, audit export, the action and connector catalog, support.

**Evidence.** The UI is what makes engineers adopt the tool and makes it convincing to a customer; organisations
pay for identity, governance, connectors and someone to call.

**Cost if wrong.** If the core alone covers a customer's needs, revenue rests on the catalog and support.

### D12. The product is named Millstream (2026-10-14)

**Decision.** The platform is named Millstream; the CLI, environment file, lock file, environment variables and
registry namespace follow (`millstream`, `millstream.yaml`, `millstream.lock`, `MILLSTREAM_*`).

**Evidence.** The working name, Strand, collides with AWS Strands Agents, an AI agent SDK with the same audience.
Millstream is plain English, names the platform's core concept (streams driving the work), is unused in the
developer and AI space, and `millstream.dev` was free. The candidates are in
[the naming note](research/20261014.naming.md).

**Cost if wrong.** Small companies in unrelated markets use the name, one of them a UK software firm; if the
trademark search finds an obstacle, the rename is mechanical while there is no code and no public release.


## Pipeline model

### D13. The package is the unit of deployment and isolation (2026-10-14)

**Decision.** A pipeline is deployed as a package: a directory with the pipeline file, local actions, lessons,
evals, assets, vars overlays and the lock file. A revision is the hash of the whole package. Within one instance,
the pipeline id scopes data, secret grants, capacity limits, placement and lifecycle.

**Evidence.** One instance must run many pipelines of an organisation without one's backlog, secrets or rollout
affecting another. Hashing only the pipeline file would let a change to a local action or lesson slip past the
revision, breaking replay.

**Cost if wrong.** Packages are uploaded as bundles, so large assets make every apply heavier; content addressing
limits this to changed packages.

### D14. Triggers publish to trigger streams (2026-10-14)

**Decision.** A trigger `x` publishes its trigger item to the reserved stream `trigger.x`, and steps read it with
`in` like any stream; there is no `on` key. Only the trigger writes to its stream, and each item on it starts a run.

**Evidence.** Steps know only streams, and triggers were the one exception. With trigger streams, inserting a step
right after a trigger is the same few-line change as anywhere else, a step can be fed by a trigger and a stream
together, and validation, lineage, metrics and schemas treat trigger items like any item.

**Cost if wrong.** `in: trigger.daily` reads less directly than a dedicated key, and the engine must still treat
trigger streams as run roots, enforced by `check`.

### D15. Routing lives in out, with a dead-letter stream for gaps (2026-10-14)

**Decision.** Every step's `out` takes one of three shapes: a stream, a list of streams (each output goes to all), or
a list of route objects `{when, out}` evaluated in order when an output is published, first match wins. An output
matching no route goes to the step's `dlq` as an `unrouted` dead letter, so a step with route objects must name its
`dlq`. There is no routing builtin.

**Evidence.** A separate routing step costs one more delivery per item and would be the one builtin declaring outputs
outside `out`. Evaluating routes at publication makes routing free and available to every action, including catalog
actions that know nothing of the pipeline. A dead-letter stream, rather than an implicit default, makes a gap in the
routes visible and replayable; first match keeps classification short, with `when: "true"` as an explicit catch-all.

**Cost if wrong.** Conditional multicast (one output to several matching routes) is not expressible; unconditional
fan-out still is, with a list of streams.

### D16. Builtins are only the steps that need engine state (2026-10-14)

**Decision.** The builtins are `filter`, `dedupe` and `review`. Routing lives in `out` for every step (D15), and
writing items out to a file or a webhook is done by stock actions (`millstream/sink-file`, `millstream/sink-webhook`).
There is no run-wide aggregation in v1. Builtins take all their parameters under `config`.

**Evidence.** A builtin runs inside the single active engine: one that does outbound HTTP or writes to the engine's
disk puts side effects, timeouts and retries in the control plane, which actions already handle. Run-wide aggregation
needs barriers, empty-input rules and dead-letter blocking, and the reference pipeline never aggregates a run.

**Cost if wrong.** A sink step costs an action process start; a daily digest of a whole run waits for aggregation to
come back from the roadmap.

### D17. Static schema checking covers a defined subset (2026-10-14)

**Decision.** `check` proves compatibility within a stated subset of JSON Schema 2020-12 (types with `null`,
properties and required, single-schema items, enum and const, allOf, local `$ref`), requires producers to require
what consumers require, reports every construct outside the subset as not statically checked, and refuses remote
`$ref`.

**Evidence.** Letting an optional producer property satisfy a required consumer property misses the most common
real mismatch, and unions, references and arrays would otherwise be left to each implementer's guess.

**Cost if wrong.** Schemas using unions get only runtime validation for those parts; the warnings make that visible.

### D18. One naming rule for keys and values (2026-10-14)

**Decision.** Every key Millstream defines, in JSON (item envelope, action protocol, HTTP API, event payloads) and in
YAML (pipeline file, action manifests, environment file, lessons), and every multi-word enum or state value, is
lowerCamelCase, with acronyms written as words (`runId`, `maxCostUsd`, `awaitingReview`). Metric names follow
Prometheus, log-store tables and columns follow SQL, CLI flags are kebab-case, environment variables are upper snake
case, and user-chosen names keep their own grammar.

**Evidence.** lowerCamelCase is the common JSON convention and matches the UI's TypeScript. `config:` blocks are
passed to actions as JSON and validated by JSON Schema, and CEL reads JSON payloads, so any other YAML convention
would mix two styles in every pipeline file. Kubernetes manifests have made camelCase YAML familiar.

**Cost if wrong.** Slightly less readable YAML than snake_case; none to change before release.


## Execution

### D19. Outputs publish on done (2026-10-14)

**Decision.** A delivery's outputs are staged with the attempt and published, routed and delivered downstream in the
transaction that records `done`. An attempt that fails, loses its lease, ends dead or is cancelled publishes nothing.
There is no streaming publication in v1; a long crawl splits its work into several deliveries.

**Evidence.** Publishing as outputs are emitted lets a delivery that later dies leave partial outputs flowing
downstream, and a second publication mode doubles the paths through the delivery state machine, routing and the crash
tests, for the benefit of starting downstream work a few minutes earlier in daily pipelines.

**Cost if wrong.** Staging costs a write per output, and a crash redoes a whole delivery; streaming publication is in
the roadmap.

### D20. Runs are pinned to their revision (2026-10-14)

**Decision.** A run executes entirely on the revision it started with. `apply` and `rollback` affect new runs;
in-flight work changes only through a replay, or a dead-letter replay with `--migrate`, which records the change.

**Evidence.** Applying changes to pending deliveries would leave a run's recorded revision unable to explain its
results, and let a retry produce a new revision's output under an old output id.

**Cost if wrong.** A fix does not reach runs already in flight, such as runs waiting on long reviews, without an
explicit replay.

### D21. One active engine with a hot standby (2026-10-14)

**Decision.** One engine is active per deployment, elected by a Postgres session advisory lock held on its only write
connection; a second engine on the same store waits as a hot standby and takes over within about 30 seconds.
Concurrency slots, rates, limits and the round-robin across pipelines live in the active engine's
memory; budget reservations are
written on deliveries. An active-active control plane is in the roadmap.

**Evidence.** Several engines sharing one Postgres add no throughput, because the database is the bottleneck, and the
workload (daily triggers, agent calls of seconds, reviews of days) does not notice a 30-second failover that loses
nothing. An active-active design needs job leases, membership, an events outbox and shared counters in the store,
which is where most of an adversarial review's findings landed. Holding the lock on the write connection means an
engine that loses leadership cannot write, without relying on clocks or epochs.

**Cost if wrong.** Throughput is capped by one engine on one Postgres (D25), and failover takes tens of seconds.
Going active-active later brings back job leases and a sequencing scheme behind the same store interfaces.

### D22. A single writer loop sequences events (2026-10-14)

**Decision.** The active engine applies every state change through one writer loop that group-commits batches of
atomic units and assigns event sequence numbers in commit order, on Postgres and SQLite alike.

**Evidence.** With one writer, commit order is known in process, so live views resume without gaps with no outbox or
publisher job, and batching keeps the transaction rate well below the database's limits at the committed
throughput.

**Cost if wrong.** The writer loop is a serial bottleneck inside the engine; its batch size and latency are metrics
and part of the scale benchmark.

### D23. Nodes pull deliveries under fenced leases (2026-10-14)

**Decision.** Actions run on nodes that connect with the deployment's node token, advertise labels and runtimes, and
pull deliveries from the active engine, following it across a failover. Leases carry fencing tokens; nodes never
connect to the store. On a laptop, one node is embedded and pulls over an in-memory transport. Nodes run `exec` and
`container` actions; Kubernetes as a runtime is in the roadmap.

**Evidence.** Pull places work where the engine cannot reach: on-premises nodes behind NAT, GPU machines, several
hosts for crawling, more memory for sandboxed agents. It survives node loss by lease expiry, and fencing stops a
cut-off node from completing a delivery handed to another. One code path from laptop to server keeps M1 tests
meaningful for production.

**Cost if wrong.** A pull round trip adds latency per delivery compared with an engine-spawned child process;
negligible against agent steps, measurable for very short code steps, where nodes pull in batches.

### D24. One node token; leases grant access to data (2026-10-14)

**Decision.** Nodes authenticate with one token per deployment, over TLS. A node fetches packages and artifacts only
for deliveries it holds. Revoking a node id refuses it and expires its leases. Labels route work and are not a
security boundary. Scoped join tokens and node certificates are in the roadmap.

**Evidence.** In a single-tenant deployment every node is run by the same operator, so a certificate authority, scoped
tokens and label-based residency would defend against a threat v1 does not have. The lease check is cheap and still
stops a node from reading another pipeline's data by guessing a hash.

**Cost if wrong.** A compromised node can pull any pipeline's deliveries that match its labels; residency enforced
against an insider waits for scoped tokens.

### D25. v1 publishes a measured throughput ceiling (2026-10-14)

**Decision.** v1 measures its throughput on a documented benchmark workload, keeps it as a regression baseline in CI,
and publishes the ceiling, instead of committing to a figure. Nodes lease in batches and the writer group-commits.
Every table is keyed by pipeline id first, so pipelines can later be sharded across stores; event partitioning and
sharding are in the roadmap.

**Evidence.** A delivery costs about eight row writes (lease, completion, item, consumer delivery, events). Agent-heavy
workloads are bound by model latency far below any plausible database ceiling, and a release gate on a number would
force partitioning and tuning work no first customer needs.

**Cost if wrong.** No contractual throughput at launch; a customer with a hard requirement gets the measured figure
and the benchmark to reproduce it.

### D26. Millstream stores no logs (2026-10-15)

**Decision.** The engine and worker nodes log through Go's `log/slog` as JSON lines on standard error, and the
environment (journald, Docker, a log system) keeps them. Every line carries the keys that locate it
(`pipeline`, `runId`, `step`, `itemId`, `deliveryId`, `attempt`, `node`); nodes re-emit an action's stderr the same
way. The UI offers a live tail relayed over SSE, never stored. A failed delivery keeps only its error class and a
message truncated to 4 KB.

**Evidence.** Log storage is a solved problem that every target environment already provides. Storing logs, or
copying log lines into errors, would make them the largest and least valuable data in Postgres, competing with
deliveries for write throughput.

**Cost if wrong.** Historical logs are read with the environment's tools rather than in the Millstream UI.


## Improvement

### D27. Lessons are versioned per step (2026-10-14)

**Decision.** What a pipeline learns for an agent step lives in a lessons file per step, in the package, edited by
engineers, injected into the prompt by the agent adapter, and part of the revision hash. A change is checked by
replaying a past window and by evals, and reaches production through a normal release.

**Evidence.** Actions may be locked, shared or from the catalog, while what was learned is specific to one
pipeline's use of them. A file in the revision keeps replay deterministic; a live memory store would not.

**Cost if wrong.** Static examples use prompt space whether or not they apply to the item; selecting examples by
similarity is parked in the roadmap.

## Build

### D28. The UI builds without a host toolchain (2026-10-15)

**Decision.** The UI is installed, built, linted and tested in containers: the official `node` image for `npm ci`,
the build and Vitest, and the official Playwright image for end-to-end tests, both pinned by digest, locally and in
CI. A toolchain with no suitable public image gets an image of our own, published under `ghcr.io/monolithiclab/`.

**Evidence.** Installing Node and its packages on the host pollutes it and drifts between machines; pinned images give
every developer and CI the same toolchain, with nothing left behind.

**Cost if wrong.** UI commands need Docker, and the first run pulls the images.
