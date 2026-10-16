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

**Status:** under construction, not yet released. M1 (the core engine: pipeline files, the SQLite store, the `exec`
runtime, the embedded node, crash-safe runs) is complete; M2 (agents, containers, budgets, evals, lessons, dedupe,
replay, insights) is implemented and in review. The web UI (M3) and server deployment (M4) come next. The name is
chosen, pending a trademark search ([naming note](docs/research/20261014.naming.md)).

## Documentation

| Document | Content |
| --- | --- |
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
