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

## Follow-ups

- Insights: deployment-wide `insights` defaults from the environment file, a CLI to reopen a muted insight, and
  retention of `metric_rollups` and of the events the projectors read (M4).
- Go SDK: per-output artifacts for `MapKeyed` (today `Context.Artifact` attaches a file to every output of the
  delivery).
- Registry actions: listing a remote registry's tags to resolve ranges, and pulling the pinned digest on a node that
  lacks it (M2 resolves against local images only).
- Container actions: enforcing the manifest's network destinations (M2 grants the default network to any declared
  list).
- Artifact files orphaned by a retry whose item already existed (first commit wins) are only reclaimed by a sweep
  of unreferenced hashes, which arrives with retention (M4).
- Agent harnesses: the `api` harness's tool loop over declared tools, and MCP servers for both harnesses (M2
  resolves either as unavailable).
- The agent adapter reads its action (manifest, prompt, schemas) from the package directory on disk, like an exec
  action's command: an edit there while a run is open changes what later deliveries of that revision run. Serving
  the revision's files from the engine would close this for both.
- Transcripts are stored uncompressed until retention (M4) brings compression and `retention.transcripts`.
- Evals: container actions, a step's lessons applied to a case, and the judge's own cost in the printed cost (M2
  evaluates agent and exec actions, without lessons, and prints only the action's cost).
- Evals: expose `eval_results` as the metric `eval_score{action,case}` with the Prometheus endpoint (M4).
- Engine memory before the M4 server: bound the revision cache and the per-revision maps (resolved actions, lessons,
  packages) a long-running engine accumulates, evicting revisions with no open run.
- Agent sandbox image: pin `agents.image` by digest in `millstream.lock`, as container actions' images are.
- The unsandboxed `claude-code` harness reads the user's own `~/.claude` settings (hooks, permissions, MCP
  servers); isolate it with `--setting-sources` (or an empty `CLAUDE_CONFIG_DIR`) so a run does not depend on them.
- `sqlite.Memory`'s pool of closed stores' databases is built for tests (and `millstream test`): it keeps up to
  eight emptied in-memory databases, with their prepared statements, alive for the life of the process, and empties
  each table row by row on close. A long-running process opening Memory stores, or one with large ones, would need
  it bounded by size or turned off.
- Review notifications: expose the `review_notifications_failed` counter with the Prometheus endpoint (M4; M3 keeps
  it in memory and logs each failure), and sign webhook bodies so a receiver can tell them from forgeries.
- Review: inbox access control (RBAC, commercial layer) and authenticated reviewers (M4; M3 takes `--as`/`$USER`);
  escalation chains longer than one inbox (M3 rejects an escalated review that times out again); edit diffs element
  by element for arrays (M3 replaces a changed array whole); attributing the cost per approved item to the steps on
  the item's lineage (M3 divides a step's cost by every approval of the pipeline's review steps).
- Container mounts: `--mount` refuses a path holding a comma, and never creates a missing source; quote such paths
  if Docker and Podman both accept a quoted CSV field.

- HTTP API: rollback, disable and node drain or revoke arrive with the M4 commands they share a unit with; a
  pipeline's items and cost today, and unschedulable deliveries on the nodes view, are not computed yet.
- HTTP API: an item's descendants are found by scanning its pipeline's items' parents (`json_each`); an index of
  parent to child (or reading the deliveries of the item) before volumes grow.
- Live events: the broker wakes on this process's commits and polls every 15 seconds for other processes' writes on
  the same SQLite file; Postgres (M4) can wake it with `LISTEN`/`NOTIFY` instead. The live log tail relays the
  embedded node only; remote nodes stream their lines with M4's node endpoint. Event retention (M4) is what makes a
  cursor too old besides the 10,000-event resume limit.

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
