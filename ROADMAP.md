# Roadmap

Parked ideas and follow-ups. Committed scope and milestones are in the
[platform spec](docs/superpowers/specs/2026-10-14-platform-design.md), section 18.

## Before implementation

- Trademark search for Millstream (INPI, EUIPO, UKIPO, USPTO; classes 9 and 42), then register `millstream.dev` and
  `getmillstream.com`; see [the naming note](docs/research/2026-10-14-naming.md).
- Create the `monolithiclab/millstream` remote and a CI workflow running `make ci`.

## After v1

- Graphical pipeline editor writing YAML back (D7).
- Engine high availability: leader election on Postgres.
- Streams shared across pipelines.
- Python SDK.
- Vault and Kubernetes secret sources.
- NATS JetStream store for high-throughput deployments (D2).
- Alerting rules in the engine (v1 relies on Prometheus alerting).
