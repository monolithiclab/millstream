# Strand

A pipeline platform for work that mixes code, LLM agents and human decisions. A pipeline is a graph of actions
connected by persisted streams of items: a crawler streams listings, an agent scores each one, a filter keeps the
good ones, an analyst approves, and a report is generated. One binary runs it on a laptop, and the same pipeline
file runs on a VPS or Kubernetes. Every pipeline is instrumented by default: metrics, lineage, logs, agent cost and
a live view in the browser.

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

**Status:** design. The platform specification is written and under review; no code yet. Strand is a working name.

## Documentation

| Document | Content |
| --- | --- |
| [Platform design](docs/superpowers/specs/2026-10-14-platform-design.md) | The full specification: concepts, pipeline file, execution model, actions, agents, UI, deployment, milestones |
| [Decisions](docs/decisions.md) | Key architecture decisions and their reasoning |
| [Roadmap](ROADMAP.md) | Parked ideas and follow-ups |

## Development

`make help` lists every command; `make ci` is the gate. Until the engine exists, the gate checks the Markdown docs
(links and 120-column wrapping) and needs `shellcheck`.

## License

Apache-2.0, see [LICENSE](LICENSE). A commercial edition will be distributed separately.
