# Millstream

A pipeline platform for work that mixes code, LLM agents and human decisions. A pipeline is a graph of actions
connected by persisted streams of items: a crawler streams listings, an agent scores each one, a filter keeps the
good ones, an analyst approves, and a report is generated. One binary runs it on a laptop, and the same pipeline
package runs on a server, with a standby engine and remote nodes. Every pipeline is instrumented by default:
metrics, lineage, agent cost, insights and a live view in the browser.

```yaml
steps:
  score:
    action: lab/score-listing@2      # an agent action
    in: listings.unique
    out: listings.scored
  keep-good:
    action: filter
    in: listings.scored
    out: listings.shortlist
    config: { expr: "item.score >= vars.min_score" }
```

**Status:** a proof of concept, not yet released. The five milestones are built, each on its branch: M1 the core
engine, M2 agents, containers, budgets and evals, M3 review and the web UI, M4 server deployment with remote nodes
and failover, M5 insights and self-improvement (`m5-improve` holds them all). [Getting started](docs/tutorial.md)
walks through what works today, and [the post-mortem](docs/20261020.postmortem.md) says what building it taught. The
name is chosen, pending a trademark search ([naming note](docs/research/20261014.naming.md)).

## Documentation

| Document | Content |
| --- | --- |
| [Getting started](docs/tutorial.md) | A tutorial: from nothing to a running pipeline with an agent step and a review, on a laptop |
| [Platform design](docs/specs/20261014.platform-design.md) | The full specification: concepts, pipeline file, execution model, actions, agents, UI, deployment, milestones |
| [Decisions](docs/decisions.md) | Key architecture decisions and their reasoning |
| [Action protocol](docs/protocol.md) | How the engine and an action process talk, for writing actions without an SDK |
| [Deployment](docs/deploy.md) | The container image, Docker Compose, systemd units, backups and upgrades |
| [UI design brief](docs/design/20261015.ui-brief.md) | The web UI's look and feel brief, for the design work |
| [Roadmap](ROADMAP.md) | Parked ideas and follow-ups |

## Development

`make help` lists every command; `make ci` is the gate (lint, tests, Markdown docs check) and needs `shellcheck`.

Build: `make build` writes `build/millstream`; `make ci` runs the gate.

## License

Apache-2.0, see [LICENSE](LICENSE). A commercial edition will be distributed separately.
