# Roadmap

Parked ideas and follow-ups. Committed scope and milestones are in the
[platform spec](docs/specs/20261014.platform-design.md), section 19. Items under *icebox* headings were designed and
then cut from v1 in the final adversarial review, for adding more complexity than a first customer needs; their
earlier designs are in the git history of the spec.

## Before implementation

- Trademark search for Millstream (INPI, EUIPO, UKIPO, USPTO; classes 9 and 42); see
  [the naming note](docs/research/20261014.naming.md).

## Self-improvement

- Auto-applying low-risk proposal classes behind a policy (D29).
- Lessons: selecting examples per item by similarity instead of including them all (D28).
- Evidence bundles pulled directly from a production deployment by a development one, instead of export and import
  files.
- `reviewPattern` filter candidates test one field at a time, on fields every decided item holds; conjunctions of
  fields, fields some items lack (a `has()` guard) and thresholds that allow `improve.filter.maxFalseDrop` false
  drops on numbers are left to the `improve` agent and the backtest.
- The insights projector reads the lineage of every item discarded or decided in the window, with payloads: on a
  large pipeline, sample them (as `distillable` samples 50 deliveries) once a profile shows the pass is slow.
- Insight evidence is JSON with HTML escaping (`\u003e` for `>`, `\u003curl\u003e`); readers decode it, but the raw
  rows would read better without it.

## Follow-ups

- Backtests: the improve budget they charge is a per-process `backtest.Ledger` against `improve.budget.dayUsd`,
  while the improve pipeline's own agents charge its `budget.dayUsd` (the same figure) in the store: charge the
  backtests to the pipeline's `budget_days` too, so both share one day budget across restarts (the store has no
  per-pipeline charge outside deliveries yet). Backtests
  live in the engine's memory (the last 100); persist them if proposals need their reports after a restart.
  Replays of runs a replay or an import opened, and of evidence (which carries no run variables, resolved anew), are
  left out or approximated.
- Distill (spec section 11.5): validation builds the candidate and runs its evals and the holdout, not its unit
  tests (`go test` in the sandbox would); the holdout run gives the code an empty config and no variables, so a step
  whose agent reads its config is measured without it; an accepted Go action reaches production as any exec action
  does, built into its `bin/<os>-<arch>/` (nothing builds it on `apply` yet); the patch sets no `auditRate`, though
  `effects: none` on the code allows one when the agent declares it too. A retention or erasure pass between the
  gathering and the validation that removes an entity's oldest pair can move it to the holdout: record the training
  entities if that ever matters. Python and other `improve.distill.language` values wait for a sandbox image each.
- Proposals: the evals of a `prompt` proposal replay the action's recordings, which a changed prompt misses: the
  `improve` package validates in its harness's live or record mode.
- The improve package: its actions call the API over plain HTTP on the address the engine serves; with `tls`, a
  self-signed certificate would fail them (trust the configured certificate, or a loopback listener for them). Only the
  embedded `improve` pipeline gets the `improveApi` and `improveToken` secrets: a fork (`init pipeline --from improve`)
  maps them itself, and minting a token for it replaces the embedded one's. The weekly run hands every accepted proposal
  to the publisher again: publishers recognise what they published (Task 8). `run --once` does not run the improve
  pipeline (it serves no API).
- Publishing (M5 Task 8): the improve package's publish step reaches the publish actions through
  `POST /proposals/{id}/publish`; there is no CLI command for it, and a person's acceptance does not publish by
  itself (the next improve run does). The git actions need an image with git and `gh` or `glab`, which
  the project does not ship: publish one, pinned. `millstream apply` reads the trailers of the last commit touching
  the package only, so a proposal merged under later commits is not recorded applied. Whether a revision carries a
  proposal in a validated combination lives in `proposalApplied` events, not in the proposal's row the API serves.
- Candidate sandbox: Go actions only, built against the standard library and the SDK (no other module, no vendor
  directory); each validation builds again (no cache of builds by action digest); remote nodes never run proposed
  code, the engine's own process does.

- Audits: an audit whose run's revision is neither active nor has unsettled runs leases without a budget
  reservation (the engine's reservations cover those revisions only); and audits keep their outputs' payloads, not
  their artifacts or the fallback's transcript.

- Insights: deployment-wide `insights` defaults from the environment file, and a CLI to reopen a muted insight.
- Go SDK: per-output artifacts for `MapKeyed` (today `Context.Artifact` attaches a file to every output of the
  delivery).
- Registry actions: listing a remote registry's tags to resolve ranges, and pulling the pinned digest on a node that
  lacks it (M2 resolves against local images only).
- Container actions: enforcing the manifest's network destinations (M2 grants the default network to any declared
  list).
- Agent harnesses: the `api` harness's tool loop over declared tools, and MCP servers for both harnesses (M2
  resolves either as unavailable).
- The agent adapter reads its action (manifest, prompt, schemas) from the package directory on disk, like an exec
  action's command: an edit there while a run is open changes what later deliveries of that revision run. Serving
  the revision's files from the engine would close this for both.
- Shared streams: `millstream test pipeline` needs a trigger to fire, so a package whose only entry is an imported
  stream cannot be tested on its own yet; a test could name an imported stream and the items to copy onto it.
- Shared streams: the UI's demo dataset has no importing pipeline (the Vitest tests add one), so `?demo` and the
  Playwright screens do not show an import entry on the graph or a lineage crossing pipelines.
- Evals: container actions, a step's lessons applied to a case, and the judge's own cost in the printed cost (M2
  evaluates agent and exec actions, without lessons, and prints only the action's cost).
- Agent sandbox image: pin `agents.image` by digest in `millstream.lock`, as container actions' images are.
- The unsandboxed `claude-code` harness reads the user's own `~/.claude` settings (hooks, permissions, MCP
  servers); isolate it with `--setting-sources` (or an empty `CLAUDE_CONFIG_DIR`) so a run does not depend on them.
- `sqlite.Memory`'s pool of closed stores' databases is built for tests (and `millstream test`): it keeps up to
  eight emptied in-memory databases, with their prepared statements, alive for the life of the process, and empties
  each table row by row on close. A long-running process opening Memory stores, or one with large ones, would need
  it bounded by size or turned off.
- Review notifications: sign webhook bodies so a receiver can tell them from forgeries.
- Review: inbox access control (RBAC, commercial layer) and reviewers named per person (the open-source edition is
  single-user: with `auth.token` every decision is `auth.user`'s); escalation chains longer than one inbox (M3 rejects
  an escalated review that times out again); edit diffs element by element for arrays (M3 replaces a changed array
  whole); attributing the cost per approved item to the steps on the item's lineage (M3 divides a step's cost by every
  approval of the pipeline's review steps).
- Container mounts: `--mount` refuses a path holding a comma, and never creates a missing source; quote such paths
  if Docker and Podman both accept a quoted CSV field.
- HTTP API: a pipeline's items and cost today are not computed yet.
- Apply (M4 Task 2): each package of one apply is its own unit, so a store failure part-way leaves the earlier ones
  applied; one unit for the whole apply would make it atomic. Apply re-checks every other pipeline's active package,
  so a pipeline broken by a change of the deployment (a secret unmapped) blocks every apply until it is fixed or
  applied again; `--force` does not bypass check errors.
- CLI through `--server`: `trigger` and `replay --step` refuse `--var` (the API takes no variables), the inbox
  decisions and insight states need `--pipeline`, and `eval` records scores only from its own process (no API).
- HTTP API and purge: an item's descendants are found by scanning its pipeline's items' parents (`json_each`), the
  lineage view and a purge's fence alike; an index of parent to child (or reading the deliveries of the item) before
  volumes grow.
- HTTP API reads to tighten before volumes grow: count queries instead of reading open dead letters and reviews to
  count them (pipelines list), a delivery's attempts filtered in SQL (a `delivery_id` column on events) and decoded
  by one reader shared with `insights`, an index on `deliveries (pipeline, finished_at)` for the live step stats,
  and a dead-letter replay across runs as one unit (the API applies one unit per run, so a failure part-way leaves
  the earlier runs replayed). Dead-letter fingerprints could share `insights`' error grouping.
- Web UI screens of M3 Task 4: the Step resource has no action kind, so the graph draws agent steps as actions (no
  agent mark, no cost per step); a dead letter carries neither its attempt count nor its original item's id (the
  screen links the dead-letter item, whose parent is the original); dead letters have no CEL filter; the step panel
  has no queue list (oldest waiting items) or throughput sparkline; every channel animates, not only those in view.
- Web UI: the inbox reads its queue again every 15 seconds because the API has no inbox stream (a `/live/inbox` of
  review events would make it live); the item list's filtered pages read on only when asked (no automatic scan to the
  next match).
- Token auth (M4 Task 7): failed sign-ins are limited per remote address in memory (ten a minute), which behind a
  reverse proxy is the proxy's for every client; a wrong bearer token on the API is not counted; sessions live in the
  engine process's memory, kept across its own terms (standby, then active again), so a restart or a failover to another
  engine signs the UI out (shared sessions in the store would survive it); `/metrics` reads every rollup series on each
  scrape, aggregated in SQL (an incremental total would serve a store holding many months of rollups more cheaply).
- Remote nodes (M4 Task 4): the node token is rotated by restarting the engines (a token read again from its file source
  would rotate it live); a node's package cache is never pruned (Task 8's bounded caches); placement reads a step's
  action kind from the newest revision defining the step, so a delivery of an older revision whose step changed kind may
  land on a node lacking its runtime and fail `unsupported`; agents on a remote node read the `agents` and `runtime`
  keys of an environment file beside the node. The Compose smoke test (Postgres, engines and nodes from `deploy/compose`
  on `millstream:dev`) is not written: the e2e node chaos suite covers the same processes; the standby names the active
  engine by its bound `listen` address, so engines on `0.0.0.0` need every engine listed in `--join` (an advertised
  address key would fix it).
- Live events: the broker wakes on this process's commits and polls every 15 seconds for other processes' writes on
  the same SQLite file; Postgres (M4) can wake it with `LISTEN`/`NOTIFY` instead. The live log tail relays the
  embedded node's lines and those remote nodes relay every second.
- Lease renewal: a pull returns once a third of the lease has passed (engine.Pull), because the node heartbeats
  between pulls and builtins run inside them. Heartbeats on a goroutine of their own in the node, or builtins on an
  engine worker rather than in Pull, would remove the bound and its tie to the heartbeat interval.
- Test budget: `make test-times` was 562 ms after M5 Tasks 4–5, under the 750 ms budget and above the 600 ms target
  (raised from 500/400 ms during M5). Unit execution in the store
  (about 100 µs a unit through the writer goroutine) dominates; running a unit inline when the connection is free
  would help every store-backed package, and would let the store tests drop their one-processor setting.
- Failover (M4 Task 3): the leadership row records the engine's bound `listen` address, which a standby names to
  clients; a deployment whose engines listen on `0.0.0.0` behind a proxy needs an advertised address key (with
  remote nodes, Task 4). `run` on Postgres takes the lock but has no standby loop: it exits when its store is lost.
  The standby serves no live streams (its broker would wake only on its 15-second poll); its UI shows them
  unavailable until it leads.
- Postgres store: its indexes mirror SQLite's, plus a queue index for Lease; tune them on the section 16 benchmark,
  with a profile. The writer's statements rely on pgx's statement cache rather than preparing them as on SQLite.
- Postgres out of resources (SQLSTATE class 53: disk full, out of memory, too many connections) reaches API clients
  as `internal error`, the cause only in the engine's log (how `apply --server` failed on a full Docker disk); mapping
  class 53 to `unavailable` with a generic reason would point an operator at the database.
- Retention and purge (M4 Task 8): retention covers payloads, artifacts, events, transcripts and rollups; the tables
  that grow more slowly (`dedupe_keys` past their window, `skipped_firings`, `eval_results`, resolved `insights`,
  done `purges` and their `purge_items`) are kept for ever. A purge sweeps its files without the periodic sweep's
  grace: a delivery in flight storing the very same content in the same pipeline at that moment could lose the file
  it references.
- Evidence (M5 Task 3): a purge lists the bundles that held an erased key but cannot reach their files; erasing the
  matching items inside development imports (rewriting the stored bundle) would close it. Bundles leave out the
  files of the items' own artifacts (photos, pages), the review samples and the deliveries of lineage items created
  before `--since`; an export reads the transcripts and lineages one delivery at a time, fine for a sample of 500.
  The redaction of free text (transcripts, error messages) replaces the redacted strings it finds verbatim or
  JSON-escaped, not other spellings of them. A development deployment lists the open insights of its imported
  bundles but does not record them, so they fire no insight trigger (`millstream improve --insight` drafts from
  them by hand), cannot be acknowledged or muted, and the projector does not recompute them from the bundles; each
  read of an imported item decodes its pipeline's bundles again (fine for a few bundles of 500 items). A distill
  draft whose outputs are right but unkeyed (the SDK's `Map`) fails its holdout with no hint that only the keys differ:
  the report could say so, or `validate` refuse such a draft before the sandbox runs it.
- Crash suite on Postgres: the failpoint matrices (run and commands) run on both stores; the seeded random kill
  points, the M2 agent path and the review fixtures still run on SQLite only.

## Execution (icebox)

- Streaming publication (`commit: emit`): outputs visible as they are emitted, for long crawls (D20).
- Run-wide aggregation (`collect`): a barrier per run and a JSON Lines artifact of the run's items, then grouping by
  key or by time window (D16).
- Weighted priorities between pipelines, and rate pools shared across pipelines.
- Action process resource limits, memory monitoring and recycling (memory and CPU limits for containers, RSS
  sampling, recycling by memory or item count).

## Deployment (icebox)

- Kubernetes: nodes that launch action pods through a relay, a Helm chart, and queue-depth metrics for autoscaling.
- Scoped join tokens, node certificates, and labels enforced for data residency (D25).
- Event partitioning by time, and a contractual throughput figure (D26).
- OpenTelemetry traces over OTLP.
- Signed approve and reject links in notifications, and email notifications.
- A control plane across customer deployments (commercial edition).
- The Claude Agent SDK as an agent harness.
- The release workflow, at the first public release: tag push, `goreleaser release` publishing the archives and the
  image to GHCR, cosign keyless signing (the `.goreleaser.yaml` sections are written, snapshots skip them), a Homebrew
  formula in `monolithiclab/homebrew-tap`, and Dependabot for the `docker` ecosystem so the pinned digests refresh.
- An image variant for nodes that run interpreted or container actions (a shell, Python, the Docker CLI): the
  distroless image runs static exec binaries only.

## After v1

- Graphical pipeline editor writing YAML back (D7).
- Active-active control plane: several engines dispatching at once, with job leases and an events outbox (D22, D23).
- Sharding pipelines across several Postgres stores (D26).
- Federation of independent instances with their own stores, for data residency.
- Streams shared across instances.
- Python SDK.
- Vault and Kubernetes secret sources.
- NATS JetStream store for high-throughput deployments (D2).
- Alerting rules in the engine (v1 relies on Prometheus alerting).
- Webhook triggers (M4 Task 6): verifying a sender's signature (GitHub's `X-Hub-Signature-256`, Stripe's) as an
  alternative to the bearer token, for senders that sign rather than authenticate; a deployment-wide default `rate`
  in the environment file; the rate buckets live in the active engine's memory, so a failover or a restart starts
  each one full; the apply's tokens and its revision are two units, so an apply interrupted between them leaves a
  webhook trigger without a token until the same apply runs again or the token is rotated.
- Flaky under load: `TestFailoverAtFailpoints` (`internal/e2e`) failed twice in the full race suite on 2026-10-21
  ("audit … of delivery … is pending after the run", at `endAudit.beforeCommit` and `park.beforeCommit`) and passed
  run alone, on the branch and on its base. If it recurs, log the audit's state and the takeover times of the run
  that leaves it pending, and check whether the clean-outcome wait ends before the new leader resumes audits.
