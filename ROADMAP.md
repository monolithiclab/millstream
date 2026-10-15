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

- Go SDK: per-output artifacts for `MapKeyed` (today `Context.Artifact` attaches a file to every output of the
  delivery).
- Artifact files orphaned by a retry whose item already existed (first commit wins) are only reclaimed by a sweep
  of unreferenced hashes, which arrives with retention (M4).

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
