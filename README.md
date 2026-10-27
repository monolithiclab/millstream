# Millstream

A pipeline platform for work that mixes code, LLM agents and human decisions. A pipeline is a graph of actions
connected by persisted streams of items: a crawler streams listings, an agent scores each one, a filter keeps the
good ones, an analyst approves, and a report is generated. One binary runs it on a laptop, and the same pipeline
package runs on a server, with a standby engine and remote nodes. Every pipeline is instrumented by default:
metrics, lineage, agent cost, insights and a live view in the browser. Each step runs the cheapest thing that does
the job: a rule, code, an agent or a person. As evidence accumulates, the platform proposes moving a step down that
list, such as code replacing an agent, with the agent kept as its fallback.

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
and failover, M5 insights and self-improvement (`m5-improve` holds them all).
[Getting started](docs/guides/getting-started.md) walks through what works today, and
[the post-mortem](docs/reviews/20261020.postmortem.md) says what building it taught. The name is chosen, pending a
trademark search ([naming note](docs/research/20261014.naming.md)).

## Documentation

The [documentation index](docs/README.md) lists every page by what you want to do. The usual entry points:

| Document | Content |
| --- | --- |
| [Getting started](docs/guides/getting-started.md) | A tutorial: from nothing to a running pipeline with an agent step and a review, on a laptop |
| [Agents in production](docs/guides/agents.md) | Validated answers, bounded cost, transcripts, evals and proposals for agent steps |
| [Deployment](docs/guides/deployment.md) | The container image, Docker Compose, systemd units, remote nodes, backups and upgrades |
| [Action protocol](docs/reference/action-protocol.md) | How the engine and an action process talk, for writing actions without an SDK |
| [Platform design](docs/specs/20261014.platform-design.md) | The full specification: concepts, pipeline file, execution model, actions, agents, UI, deployment, milestones |
| [Architecture](docs/development/architecture.md) | The package map and the path an item takes through the code |
| [Contributing](docs/development/contributing.md) | Requirements, everyday commands and how a change lands |
| [Roadmap](ROADMAP.md) | Parked ideas and follow-ups |

## Development

`make help` lists every command; `make build` writes `build/millstream`, and `make ci` is the gate (lint, tests,
Markdown docs check). [Contributing](docs/development/contributing.md) lists the requirements and the workflow.

## License

Apache-2.0, see [LICENSE](LICENSE). A commercial edition will be distributed separately.
