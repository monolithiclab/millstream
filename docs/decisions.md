# Decisions

Append-only. Each entry records what was decided, the evidence, and the cost if it turns out wrong. Never edit an
entry; supersede it with a new one. The design they belong to is
[the platform spec](superpowers/specs/2026-10-14-platform-design.md).

## D1. Build our own Go engine (2026-10-14)

**Decision.** Strand is its own engine in Go, over a durable item log. It is not built on Temporal, and not
assembled from stream tools such as Redpanda Connect.

**Evidence.** The pipeline file and the action protocol are the product, and they must be ours to standardise and
open-source. Temporal is shaped around code-first workflows: per-item streaming and fan-out are awkward and costly
there, and a laptop needs its dev server. Stream ETL tools have no model for agents, human review, runs or lineage.
The hard part we take on, durable leased deliveries, is a well-known pattern on SQLite and Postgres.

**Cost if wrong.** We own crash recovery and leasing bugs that Temporal would have solved. Mitigated by the crash
test suite (spec section 15) from M1.

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
protocol is relayed to the engine by `strand worker` on Kubernetes.

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

**Decision.** Pipelines change through their files, in git, applied with `strand apply`. The v1 UI visualises and
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
