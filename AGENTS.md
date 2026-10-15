# AGENTS.md

Millstream is a Monolithic Lab pipeline platform (Go engine, React UI) under construction: the repo holds the
specification, decisions and the Go engine as it is built. See [README.md](README.md) for what it is, `docs/` (table
below) for the reasoning, and `make help` for every command.

## Hard rules

- The repo is meant to go open source: no customer, prospect or partner names anywhere, in docs, examples or
  commit messages. The reference example is the generic real estate pipeline.
- Each milestone is implemented from its plan in `docs/plans/`; a change to a public contract updates the spec in the
  same commit.
- A change to the pipeline file format, the action protocol or the HTTP API updates the spec in the same commit:
  these are public contracts.

## Constraints that look like bugs

- Pipelines have no version field on purpose: the engine identifies revisions by content hash (spec section 4.2).
- Thresholds belong in `filter` steps, not in actions, so actions stay reusable (spec section 4.7).
- At-least-once delivery is deliberate; idempotency comes from deterministic item ids (D9), not from exactly-once
  machinery.

## Layout

- `cmd/millstream/`: the binary's `main`.
- `internal/`: `cli`, `pipeline`, `ids`, `expr`, `schema`, `protocol`, `store`, `failpoint`, `engine`, `node`,
  `redact` and the other packages named in the M1 plan's file structure.
- `sdk/go/`: the Go action SDK, its own module.
- `examples/`: fixture and example pipeline packages.
- `make ci` is the gate: lint (golangci-lint, govulncheck, tidy, pins, shellcheck), tests with `-race`, and the docs
  check, for the main module and the SDK module alike.

## Agent recordings and the live smoke test

- Tests never reach a model: agent calls replay from recordings made against a fake model server. `make recordings`
  rewrites them (`internal/agent/testdata/recordings`, `examples/m2-fixture/recordings`) after a change to a prompt,
  a lessons file, an output schema or the fake model.
- The live smoke test runs the M2 fixture's score action through the `claude-code` harness against the real `claude`
  CLI (installed, logged in) on two listings, recording into a temporary directory, and spends a few cents:
  `MILLSTREAM_LIVE=1 go test -tags e2e -count=1 -v -run TestLiveScore ./internal/e2e` (`MILLSTREAM_LIVE_MODEL` picks
  the model, `haiku` by default). It is never part of `make ci`; never commit what it records.
- The M2 end-to-end and crash tests sit behind the `e2e` build tag: `go test -tags e2e ./internal/e2e`.

## Where things go

- Architecture decisions live in `docs/decisions.md`. Until the first public release the log is kept current: edit
  an entry to match the design and remove superseded ones. From the first release on, it is append-only.
- Specs and plans: `docs/specs/YYYYMMDD.<topic>-design.md` and `docs/plans/YYYYMMDD.<topic>.md`.
- Parked ideas and follow-ups: `ROADMAP.md`.

## Documentation

| Document | Owns |
| --- | --- |
| `docs/specs/20261014.platform-design.md` | The platform design: concepts, file format, execution model, protocol, UI, deployment, milestones |
| `docs/decisions.md` | Architecture decisions D1 onward |
| `docs/protocol.md` | The action protocol: messages, framing, ordering, errors, for action authors and SDKs |
| `docs/design/20261015.ui-brief.md` | Design brief for the web UI, written for Claude Design |
| `docs/design/canvas/` | Snapshot of the Claude Design canvas for the web UI, with its version id |
| `ROADMAP.md` | Parked ideas and follow-ups |

## Workflow

1. Minimal change, with tests, plus the owning doc in the same change
2. `make ci` green
3. Commit with the session's trailers; push only when asked
