# Decisions

Append-only. Each entry records what was decided, the evidence, and the cost if it turns out wrong. Never edit an
entry; supersede it with a new one. The design they belong to is
[the platform spec](specs/20261014.platform-design.md).

## D1. Build our own Go engine (2026-10-14)

**Decision.** Millstream is its own engine in Go, over a durable item log. It is not built on Temporal, and not
assembled from stream tools such as Redpanda Connect.

**Evidence.** The pipeline file and the action protocol are the product, and they must be ours to standardise and
open-source. Temporal is shaped around code-first workflows: per-item streaming and fan-out are awkward and costly
there, and a laptop needs its dev server. Stream ETL tools have no model for agents, human review, runs or lineage.
The hard part we take on, durable leased deliveries, is a well-known pattern on SQLite and Postgres.

**Cost if wrong.** We own crash recovery and leasing bugs that Temporal would have solved. Mitigated by the crash
test suite (spec section 16) from M1.

## D2. A durable item log is the only state (2026-10-14)

**Decision.** Every item between steps is persisted in SQLite (laptop) or Postgres (server). Delivery is
at-least-once. Workers are stateless.

**Evidence.** Replay of a step over past items, crash resume, lineage and audit all need persisted items. In-memory
channels lose in-flight work on a crash and make "insert a step and back-fill it" impossible.

**Cost if wrong.** Throughput is bounded by the database (design target 1,000 items/s on Postgres). If a customer
needs much more, a NATS JetStream store would sit behind the same store interface.

## D3. Actions speak JSON lines over stdio (2026-10-14)

**Decision.** An action is any program that reads items on stdin and writes emits, metrics and results on stdout,
one JSON object per line; stderr is logs.

**Evidence.** Any language works with no SDK. Local processes and `docker run -i` need no network setup. The same
protocol is relayed to the engine by `millstream worker` on Kubernetes.

**Cost if wrong.** Large payloads go through pipes; artifacts are passed as files to keep that bounded. A gRPC
transport could be added later as a second protocol binding.

## D4. Harness-agnostic agent actions (2026-10-14)

**Decision.** Agent actions are run by an adapter that drives Claude Code headless, the Claude Agent SDK, or a
direct model API (Anthropic, Mistral, OpenAI-compatible, local). The engine never knows which model ran.

**Evidence.** Customers differ on which providers they accept; model choice per step, including sovereign or local
models, is a selling point. Agents then get the same retries, metrics and lineage as code.

**Cost if wrong.** Harness-specific features (subagents, skills) are reachable only through the `claude-code`
harness, and each harness needs its own driver and tests.

## D5. Server-sent events for live UI data (2026-10-14)

**Decision.** The live view uses SSE. User actions from the UI are plain REST calls.

**Evidence.** Live data only flows from server to browser. SSE reconnects natively, resumes with `Last-Event-ID`
over the event sequence, and passes through proxies and ingresses as plain HTTP.

**Cost if wrong.** A future need for two-way streaming (collaborative editing) would add WebSocket alongside.

## D6. Single-tenant deployments (2026-10-14)

**Decision.** One deployment per customer, on their infrastructure or a dedicated VPS. No multi-tenant SaaS in v1.

**Evidence.** Forward-deployed work keeps customer data on customer infrastructure. Tenancy isolation, billing and
certifications are a product of their own.

**Cost if wrong.** A hosted offer later needs tenant isolation added to the store and API.

## D7. The pipeline file is the source of truth (2026-10-14)

**Decision.** Pipelines change through their files, in git, applied with `millstream apply`. The v1 UI visualises and
operates; it does not edit pipelines.

**Evidence.** Files diff, review and version like code; a revision hash ties every run to the exact definition.

**Cost if wrong.** Less technical users need an engineer to change a pipeline until a graphical editor, writing
YAML back, exists.

## D8. CEL for every expression (2026-10-14)

**Decision.** Filters, routes, dedupe keys, config interpolation and item queries all use CEL.

**Evidence.** CEL is typed and checked before running, side-effect free, cost-bounded, and has a mature Go
implementation; one language everywhere means one thing to learn.

**Cost if wrong.** CEL is less familiar than JavaScript or JSONPath; documentation needs good examples.

## D9. Deterministic item ids (2026-10-14)

**Decision.** `id = hash(producing step, input item id, generation, key or output index)`, and emitted items are
upserted.

**Evidence.** Outputs are committed as they are emitted so downstream steps start early; a retried delivery
re-emits the same ids, so downstream steps still see each item once. Replay bumps the generation to produce new
items.

**Cost if wrong.** An action that emits several items in a non-deterministic order without setting `key` creates
duplicates on retry; the Go SDK requires a key for multi-emit actions.

## D10. One event table drives every projection (2026-10-14)

**Decision.** Every state change is appended to `events`; metrics, lineage, the live view and audit are derived
from it.

**Evidence.** Separate counters and logs drift and disagree; one ordered source cannot.

**Cost if wrong.** The table grows fast; retention and rollups (spec section 9.3) keep it bounded.

## D11. Open-source core includes the whole UI (2026-10-14)

**Decision.** Apache-2.0 core: engine, CLI, SDK, builtins, agent adapter and the full web UI for a single user.
Commercial edition: SSO and RBAC, audit export, the action and connector catalog, the fleet control plane, support.

**Evidence.** The UI is what makes engineers adopt the tool and makes it convincing to a customer; organisations
pay for identity, governance, connectors and someone to call.

**Cost if wrong.** If the core alone covers a customer's needs, revenue rests on the catalog and support.

## D12. The product is named Millstream (2026-10-14)

**Decision.** The platform is named Millstream; the CLI, environment file, lock file, environment variables and
registry namespace follow (`millstream`, `millstream.yaml`, `millstream.lock`, `MILLSTREAM_*`). It replaces the
working name Strand.

**Evidence.** Strand collides with AWS Strands Agents, an AI agent SDK with the same audience. Millstream is plain
English, names the platform's core concept (streams driving the work), is unused in the developer and AI space, and
`millstream.dev` is free. Two rounds of candidates are in [the naming note](research/20261014.naming.md).

**Cost if wrong.** Small companies in unrelated markets use the name, one of them a UK software firm; if the
trademark search finds an obstacle, the rename is mechanical while there is no code and no public release.

## D13. Learnings reach production only through accepted proposals (2026-10-14)

**Decision.** Insights, lessons, prompt changes, new filters and code that replaces an agent reach a pipeline only
as proposals that a person accepts, each validated by evals and a backtest against past items. Nothing applies on
its own, and agents do not read a live learnings store.

**Evidence.** A pipeline that changes itself unseen breaks revision hashes and replay, and leaves no one accountable
for a wrong change. A validated diff is cheap to review and keeps git as the source of truth (D7).

**Cost if wrong.** A pipeline improves only as fast as people accept proposals. Auto-applying low-risk classes can
be added later behind a policy without changing the proposal model.

## D14. Lessons are versioned per step (2026-10-14)

**Decision.** What a pipeline learns for an agent step lives in a lessons file per step, in the pipeline
repository, injected into the prompt by the agent adapter, and part of the revision hash.

**Evidence.** Actions may be locked, shared or from the catalog, while what was learned is specific to one
pipeline's use of them. A file in the revision keeps replay deterministic; a live memory store would not.

**Cost if wrong.** Static examples use prompt space whether or not they apply to the item; selecting examples by
similarity is parked in the roadmap.

## D15. Self-improvement runs as a Millstream pipeline (2026-10-14)

**Decision.** Insights are computed by the engine without an LLM. Drafting proposals is done by an `improve`
pipeline embedded in the binary, in the ordinary pipeline format; validation (check, evals, backtest, shadow) is done
by the engine.

**Evidence.** As a pipeline, the LLM work gets budgets, transcripts, replay and the UI for free, and the engine holds
no agent logic. Deterministic insights work on a laptop with no API key.

**Cost if wrong.** The `improve` pipeline needs engine hooks (backtest, shadow) that ordinary pipelines do not use;
if they grow, they become a builtin set of their own.

## D16. Forge integration goes through actions (2026-10-14)

**Decision.** Accepted proposals are published by an action chosen in `improve.publish`: `apply`, `patch-file`, or
`git-pr-gh` and `git-pr-glab`, which wrap the `gh` and `glab` CLIs with the credentials they already have.

**Evidence.** Millstream then holds no forge tokens or configuration, and a new forge is a new action, not an engine
change.

**Cost if wrong.** Pull requests depend on the CLIs being installed and authenticated in the action's environment.

## D17. The package is the unit of deployment and isolation (2026-10-14)

**Decision.** A pipeline is deployed as a package: a directory with the pipeline file, local actions, lessons,
evals, assets, vars overlays and the lock file. A revision is the hash of the whole package. Within one instance,
the pipeline id scopes data, secrets grants, capacity limits, placement and lifecycle.

**Evidence.** One instance must run many pipelines of an organisation without one's backlog, secrets or rollout
affecting another. Hashing only the pipeline file would let a change to a local action or lesson slip past the
revision, breaking replay and proposals.

**Cost if wrong.** Packages are uploaded as bundles, so large assets make every apply heavier; content addressing
limits this to changed packages.

## D18. Leaderless control-plane replicas over Postgres (2026-10-14)

**Decision.** `millstream serve` runs as several identical replicas over one Postgres. Dispatch is coordinated by
delivery leases; jobs that need a single owner (cron, collect settlement, projections, retention) run under fenced
job leases spread by rendezvous hashing. SQLite stays single-replica. This replaces the v1 non-goal of a single
active engine.

**Evidence.** Losing the engine must cost only lease expiry. `SKIP LOCKED` leasing already makes replicas safe for
dispatch, so a leader would add failover delay without adding safety.

**Cost if wrong.** Postgres becomes the shared bottleneck; D20 bounds the target and keeps sharding possible.

## D19. Worker nodes pull deliveries under fenced leases (2026-10-14)

**Decision.** Actions run on worker nodes that join with a token, receive a client certificate, advertise labels
and runtimes, and pull deliveries from any replica. Leases carry fencing tokens; nodes never connect to the store.
On a laptop, one worker is embedded and pulls over an in-memory transport.

**Evidence.** Pull scales out by adding nodes anywhere (VMs, GPU machines, on-premises, Kubernetes) without the
engine reaching into them, and survives node loss by lease expiry. Fencing stops a cut-off node from completing a
delivery handed to another. One code path from laptop to fleet keeps M1 tests meaningful for production.

**Cost if wrong.** A pull round trip adds latency per delivery compared with an engine-spawned child process;
negligible against agent steps, measurable for very short code steps, where nodes pull in batches.

## D20. Scale target on one Postgres, pipeline as partition key (2026-10-14)

**Decision.** v1 targets about 10,000 deliveries per second across the fleet on one well-sized Postgres. Every table
and job is keyed by pipeline id so that pipelines can later be sharded across stores.

**Evidence.** Agent-heavy workloads are bound by model latency far below this rate; sharding before a workload
needs it would multiply the operational and testing surface.

**Cost if wrong.** A deployment beyond the target needs the sharding work parked in the roadmap.

## D21. Outputs publish on done by default (2026-10-14)

**Decision.** A step's outputs are staged with the attempt and published atomically with `done`. Steps that need
streaming, such as crawlers, opt into `commit: emit`. Items are immutable and the first commit of an id wins; a
conflicting re-emit is discarded and recorded.

**Evidence.** Two adversarial reviews showed that publishing on emit lets a delivery that later dies leave partial
outputs flowing downstream, and that "upsert" contradicted immutable items. Publishing on done gives the simple
guarantee by default and keeps streaming where it pays.

**Cost if wrong.** Staging costs a write per output and delays downstream work until each delivery completes;
streaming steps must say so explicitly.

## D22. Runs are pinned to their revision (2026-10-14)

**Decision.** A run executes entirely on the revision it started with. `apply` and `rollback` affect new runs;
in-flight work changes only through a replay, or a dead-letter replay with `--migrate`, which records the change.

**Evidence.** Applying changes to pending deliveries made a run's recorded revision unable to explain its results,
and let a retry produce a new revision's output under an old output id.

**Cost if wrong.** A fix does not reach runs already in flight, such as runs waiting on long reviews, without an
explicit replay.

## D23. v1 commits to 2,000 deliveries per second, measured (2026-10-14)

**Decision.** Refines D20. v1 guarantees 2,000 deliveries per second on one Postgres on a documented benchmark
workload; about 10,000 per second remains the design goal. A feasibility benchmark opens M5, before replica work.
Leases are taken in batches, notifications coalesced per transaction, and events partitioned by time.

**Evidence.** A delivery costs about eight row writes (lease, completion, item, consumer delivery, events), so
10,000 per second means about 80,000 row writes per second before indexes and WAL, and 90 days of events at that
rate is hundreds of billions of rows. The target was asserted, not measured.

**Cost if wrong.** If the benchmark finds a lower ceiling, the commitment drops before release rather than after.

## D24. Events are published through an outbox (2026-10-14)

**Decision.** Events are inserted without a public sequence number; a publisher job per pipeline, under a job lease,
assigns contiguous sequence numbers to committed events. Live views and `Last-Event-ID` use that sequence.

**Evidence.** A Postgres sequence is assigned at insert, not at commit, and rolled-back transactions burn values, so
"advance up to the oldest transaction in flight" could neither be implemented exactly nor tell a gap from a delay.

**Cost if wrong.** Live views lag by the publisher's batch interval, and the publisher is one more job per pipeline.

## D25. One active engine with a hot standby (2026-10-14)

**Decision.** Supersedes D18. One engine is active per deployment, elected by a Postgres session advisory lock held
on its only write connection; a second engine on the same store waits as a hot standby and takes over within about
30 seconds. Concurrency slots, limits, fair queuing and rate pools live in the active engine's memory; budget
reservations are written on deliveries. Worker nodes (D19) connect to the active engine and follow it across a
failover. An active-active control plane is deferred to the roadmap.

**Evidence.** Replicas sharing one Postgres add no throughput, because the database is the bottleneck, and the
workload (daily triggers, agent calls of seconds, reviews of days) does not notice a 30-second failover that loses
nothing. The leaderless design carried most of the specification's complexity (job leases, membership, an events
outbox, shared counters in the store) and most of the adversarial reviews' findings. Holding the lock on the write
connection means an engine that loses leadership cannot write, without relying on clocks or epochs.

**Cost if wrong.** Throughput is capped by one engine on one Postgres (D23), and failover is tens of seconds, not
instant. Going active-active later reintroduces job leases and a sequencing scheme behind the same store interfaces.

## D26. A single writer loop sequences events (2026-10-14)

**Decision.** Supersedes D24. The active engine applies every state change through one writer loop that
group-commits batches of atomic units and assigns event sequence numbers in commit order, on Postgres and SQLite
alike.

**Evidence.** With one writer, commit order is known in process, so live views resume without gaps with no outbox or
publisher job, and batching keeps the transaction rate well below the database's limits at the committed
throughput.

**Cost if wrong.** The writer loop is a serial bottleneck inside the engine; its batch size and latency are metrics
and part of the scale benchmark.

## D27. Worker nodes are authorised per request (2026-10-14)

**Decision.** A node's certificate carries only its id. Its scope (pipelines, allowed labels) is copied from its join
token into its record and checked on every request. A node may advertise only allowed labels, leases only deliveries
of granted pipelines, and fetches packages and artifacts only for deliveries it holds. Revocation expires its leases
at once.

**Evidence.** Self-reported labels let a compromised node attract deliveries meant for a restricted site, and fetches
by hash would otherwise expose any pipeline's packages and artifacts to any certified node.

**Cost if wrong.** Every node request costs a lookup of the node's record, cached in the active engine.

## D28. Proposal patches are confined and published by hash (2026-10-14)

**Decision.** A proposal's patch may touch only the package paths its kind allows. The publish action runs in its
own container with only forge credentials, never builds or runs candidate code, disables git hooks, and publishes
only the patch whose hash was validated.

**Evidence.** Patches are drafted by an agent that read crawled, untrusted data; an injected instruction could add
CI workflows or build files that run with repository secrets as soon as a branch is pushed, or candidate code could
read forge credentials if they shared an environment.

**Cost if wrong.** Some legitimate improvements (a helper file next to an action, a CI tweak) must be made by hand.

## D29. Acceptance is conditional on the base revision (2026-10-14)

**Decision.** Accepting a proposal succeeds only if its base revision is still active, in one writer transaction
that also marks overlapping open proposals stale. On git routes, applied revisions are matched to proposals by commit
trailer, and a proposal merged onto another base triggers evals and a backtest of the new revision.

**Evidence.** Two proposals validated on the same base could otherwise be combined without ever being validated
together, and a pull request may merge after other commits.

**Cost if wrong.** Busy pipelines see more stale proposals and revalidation runs, charged to the improve budget.

## D30. Static schema checking covers a defined subset (2026-10-14)

**Decision.** `check` proves compatibility within a stated subset of JSON Schema 2020-12 (types with `null`,
properties and required, single-schema items, enum and const, allOf, local `$ref`), requires producers to require
what consumers require, reports every construct outside the subset as not statically checked, and refuses remote
`$ref`.

**Evidence.** The previous rule let an optional producer property satisfy a required consumer property, the most
common real mismatch, and left unions, references and arrays to each implementer's guess.

**Cost if wrong.** Schemas using unions get only runtime validation for those parts; the warnings make that visible.

## D31. Triggers publish to trigger streams (2026-10-14)

**Decision.** A trigger `x` publishes its trigger item to the reserved stream `trigger.x`; steps read it with `in`
like any stream, and the `on` step key is removed. Only the trigger writes to its stream, and each item on it starts a
run.

**Evidence.** Steps already know only streams; triggers were the one exception. With trigger streams, inserting a
step right after a trigger is the same few-line change as anywhere else, a step can be fed by a trigger and a stream
together, and validation, lineage, metrics and schemas treat trigger items like any item.

**Cost if wrong.** `in: trigger.daily` reads less directly than `on: daily`, and the engine must still treat trigger
streams as run roots, enforced by `check`.

## D32. Collect is one builtin that emits an artifact (2026-10-14)

**Decision.** `collect` is the only way to gather a run's items. It is a builtin run by the engine: when the run's
barrier closes, it streams the items into a JSON Lines artifact and emits one item referencing it, read by an
ordinary map action downstream. The `mode` step key, the collect messages of the action protocol (`begin`, `end`,
items without a delivery) and the SDK's `Collect` are removed. Its parameters, like every builtin's, live under
`config`; output streams are declared only in `out`.

**Evidence.** Two mechanisms (`mode: collect` on any action and a `collect` builtin emitting `{items: [...]}`)
duplicated one concept, let a step pair a mode with an action that could not run in it, and put a whole run inside
one payload, contradicting the paging guarantee.

**Cost if wrong.** A run's items are copied into the artifact store, and aggregation takes two steps instead of one.

## D33. Routing lives in out, with a dead-letter stream for gaps (2026-10-14)

**Decision.** Every step's `out` takes one of three shapes: a stream, a list of streams (each output goes to all), or
a list of route objects `{when, out}` evaluated in order when an output is published, first match wins. An output
matching no route goes to the step's `dlq` as an `unrouted` dead letter; a step with route objects must name its
`dlq`. The `route` builtin is removed, and item ids include the output stream.

**Evidence.** A separate `route` step cost one more delivery per item and was the one builtin that declared outputs
outside `out`. Evaluating routes at publication makes routing free and available to every action, including
catalog actions that know nothing of the pipeline. Sending unmatched outputs to a dead-letter stream, rather than to
an implicit default, makes a gap in the routes visible and replayable; first match keeps classification short, with
an explicit `when: "true"` as a catch-all.

**Cost if wrong.** Conditional multicast (one output to several matching routes) is not expressible; unconditional
fan-out still is, with a list of streams.

## D34. JSON keys are lowerCamelCase (2026-10-14)

**Decision.** Every JSON document Millstream defines (item envelope, action protocol, HTTP API, event payloads) uses
lowerCamelCase keys (`runId`, `idempotencyKey`, `producedBy`). Metric names follow Prometheus snake_case, and
user-chosen identifiers (pipeline, step, stream, trigger names) keep their own grammar.

**Evidence.** lowerCamelCase is the common JSON convention, matching JavaScript, the web UI's TypeScript and most
public JSON APIs, so clients map keys to fields without renaming.

**Cost if wrong.** None before release; changing it later would break every protocol client.

## D35. One naming rule for keys and values (2026-10-14)

**Decision.** Extends D34. Keys Millstream defines in YAML (pipeline file, action manifests, environment file,
lessons) and its multi-word enum and state values (`awaitingReview`, `completedWithErrors`, `slowStep`) are
lowerCamelCase, acronyms written as words (`maxCostUsd`). Metric names, log-store tables and columns, CLI flags,
environment variables and user-chosen names keep their own conventions. Names used in earlier entries of this log
are as they were when written.

**Evidence.** `config:` blocks are passed to actions as JSON and validated by JSON Schema, and CEL reads JSON
payloads, so any other YAML convention would mix two styles in every pipeline file. Kubernetes manifests have made
camelCase YAML familiar to the engineers who write pipelines.

**Cost if wrong.** Slightly less readable YAML than snake_case; none to change before release.

## D36. Logs live in files, not in the database (2026-10-14)

**Decision.** Log lines are JSON objects tagged with pipeline, run, step, item, delivery, attempt and node, written by
nodes into one segment per attempt and shipped to the artifact store (plain files on a laptop). The database keeps
only an index of segments. Errors store a class and a truncated message, and refer to the attempt's segment and
transcript instead of copying them.

**Evidence.** Storing every worker log line, plus 200 lines copied into each error, would make logs the largest and
least valuable data in Postgres, competing with deliveries for write throughput. Tagged JSON lines in files are
cheap to keep, expire as whole segments, and can be read with `grep` and `jq`.

**Cost if wrong.** No full-text search across all logs without exporting to a log system over OTLP; reading logs for
a delivery costs a fetch from the artifact store.
