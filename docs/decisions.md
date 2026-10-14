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
