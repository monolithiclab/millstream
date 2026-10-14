# Roadmap

Parked ideas and follow-ups. Committed scope and milestones are in the
[platform spec](docs/specs/20261014.platform-design.md), section 19.

## Before implementation

- Trademark search for Millstream (INPI, EUIPO, UKIPO, USPTO; classes 9 and 42); see
  [the naming note](docs/research/20261014.naming.md).

## After v1

- Graphical pipeline editor writing YAML back (D7).
- Sharding pipelines across several Postgres stores (D20).
- Federation of independent instances with their own stores, for data residency.
- Streams shared across pipelines.
- Python SDK.
- Vault and Kubernetes secret sources.
- NATS JetStream store for high-throughput deployments (D2).
- Alerting rules in the engine (v1 relies on Prometheus alerting).
- Lessons: select examples per item by similarity instead of including them all (D14).
- Self-improvement: auto-apply low-risk proposal classes behind a policy (D13).
