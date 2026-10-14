# AGENTS.md

Strand is a Monolithic Lab pipeline platform (Go engine, React UI) in its design phase: the repo holds the
specification and decisions only. See [README.md](README.md) for what it is, `docs/` (table below) for the
reasoning, and `make help` for every command.

## Hard rules

- The repo is meant to go open source: no customer, prospect or partner names anywhere, in docs, examples or
  commit messages. The reference example is the generic real estate pipeline.
- No code before the platform spec is approved and an implementation plan exists for the milestone.
- A change to the pipeline file format, the action protocol or the HTTP API updates the spec in the same commit:
  these are public contracts.

## Constraints that look like bugs

- Pipelines have no version field on purpose: the engine identifies revisions by content hash (spec section 4.2).
- Thresholds belong in `filter` steps, not in actions, so actions stay reusable (spec section 4.7).
- At-least-once delivery is deliberate; idempotency comes from deterministic item ids (D9), not from exactly-once
  machinery.

## Where things go

- Architecture decisions: a new D-entry in `docs/decisions.md`, never an edit of an old one.
- Specs and plans: `docs/superpowers/specs/` and `docs/superpowers/plans/`, dated.
- Parked ideas and follow-ups: `ROADMAP.md`.

## Documentation

| Document | Owns |
| --- | --- |
| `docs/superpowers/specs/2026-10-14-platform-design.md` | The platform design: concepts, file format, execution model, protocol, UI, deployment, milestones |
| `docs/decisions.md` | Architecture decisions D1 onward |
| `ROADMAP.md` | Parked ideas and follow-ups |

## Workflow

1. Minimal change, with tests, plus the owning doc in the same change
2. `make ci` green
3. Commit with the session's trailers; push only when asked
