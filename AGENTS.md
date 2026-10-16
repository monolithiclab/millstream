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
- `ui/`: the web UI, built and tested only in containers (see Web UI).
- `examples/`: fixture and example pipeline packages.
- `make ci` is the gate: lint (golangci-lint over the e2e-tagged build, which holds every file, govulncheck, tidy,
  pins, shellcheck), the default test suite without `-race`, and the docs check, for the main module and the SDK
  module alike.

## Test suites

- `make test` runs the default build: in-process unit tests, without `-race`, within a budget measured without
  `-race` by `make test-times` (`scripts/test-times`): at most 500 ms of serial-equivalent test time in total (the
  suite run with `-p 1 -parallel 1`, each top-level test timed from the `go test -json` event timestamps), no
  top-level test over 20 ms, and no package's wall time (`-p 1`, `t.Parallel` in effect) more than 100 ms above its
  test binary's floor (the same binary running no test: process start, initialisation, `TestMain`). It prints the
  slowest tests and exits 1 over budget. Compilation and binary start are not counted.
- What keeps it there: actions run in-process (`exec.FuncLauncher` with `exectest.Serve`, wired through `fakeexec`
  and `cli.Runtime.Launchers`); stores are `sqlite.Memory`, whose template loads from an embedded image and whose
  closed connections, emptied, serve the next store with their statements already prepared (`Clone` is slower: it
  prepares them all again); fixtures (packages, parsed pipelines, applied runs, insight histories) are built once
  per process; time is a fake clock or an injected timer. No `go build`, no process, no Docker, no sleep or
  polling, no file database but the one that tests opening a file: a test that needs one, or drives the CLI or the
  engine end to end, goes behind the `e2e` build tag, in a `*_e2e_test.go` file. The store and insights tests run
  serially: the transpiled SQLite allocates under one process-wide lock, so parallel tests contend instead of
  overlapping.
- `make test-e2e` (`go test -race -count=1 -tags e2e ./...`, and the SDK module's) runs everything, tagged and
  untagged, under the race detector, and prints the whole suite's coverage (`make test` checks the default build's
  against `COVERAGE_MIN`): the `internal/e2e` package (crash matrix, M1 and M2 scenarios), the end-to-end CLI and
  engine tests, the real-process twins of the in-process tests (under the tag, `fakeexec` runs the built fake
  binary), Docker tests, and the long property runs. It is slow, is not part of `make ci`, and runs in CI as its own
  job; run it before closing a milestone and after touching the exec runtime, the node, the CLI's run path, the
  store's concurrency or the crash recovery.

## Web UI

- `ui/`: the React 18 + TypeScript (strict) + Vite + Tailwind CSS single-page app of spec section 12.2. `src/shell/`
  is the app shell (navigation, pipeline switcher, command palette, shortcut help, live mark), `src/routes.ts` the
  screens and their URLs, `src/api/` the HTTP API client, `src/live/` the server-sent events hook, `src/i18n/` the
  catalogues and `Intl` formatting, `src/styles/` the tokens, `e2e/` the Playwright tests.
- No host toolchain (D35): npm, Vite, Vitest and Playwright run only in containers, from images pinned by digest in
  `ui/images.mk`. Never install Node on the host. `node_modules` and the npm cache live in the named volumes
  `millstream-ui-node-modules` and `millstream-ui-npm-cache`; containers run as the host uid.
- `make ui` builds into `internal/ui/dist/`, which `internal/ui` embeds (`//go:embed all:dist`) and serves as an SPA
  (index.html for client routes, immutable caching for `assets/`). The build is not committed; the committed
  `internal/ui/dist/placeholder.html` is served until it exists, so `go build` never needs npm.
- `make test-ui`: typecheck, ESLint (typescript-eslint, jsx-a11y, react-hooks), Prettier check, and Vitest with
  coverage in jsdom: fake timers and a `FakeApi`, never a network or a wall-clock wait. `make test-ui-e2e` builds,
  then runs Playwright with axe (WCAG 2.2 AA) on the shell in both themes, both languages, at 375 and 1280 px,
  reaching the build through `internal/ui/uiserve` (the Go handler, cross-compiled for the container).
  `make ui-shell` opens a shell in the node container (`npm run dev` serves on port 5173). Both test targets run in
  CI as their own jobs, not in `make ci`.
- Tokens: `ui/src/styles/tokens.css` holds every colour, font, radius, shadow and motion token of the canvas identity
  board as `--ms-*` custom properties, colours as `light-dark(light, dark)`. The theme follows
  `prefers-color-scheme` unless `<html data-theme="light|dark">` overrides it. `src/styles/index.css` maps the tokens
  into Tailwind and clears Tailwind's own palette, so only token utilities exist (`bg-surface`, `text-ink-2`).
  `tokens.test.ts` checks the WCAG contrast of every text (4.5:1) and UI (3:1) pair from the file itself.
- i18n: react-i18next with `src/i18n/en.json` and `fr.json`; `t()` accepts only keys of the English catalogue.
  `i18n.test.ts` fails on a key missing in either language, a key no source file uses (a key is used when it
  appears as a quoted literal: keep keys literal, no built-up names), and French typography (no-break space before
  `: ; ! ? »`). Numbers, money, durations and relative times go through `src/i18n/format.ts`.
- API: `src/api/api.ts` is the `Api` interface, `http.ts` its fetch implementation, `fake.ts` the in-memory one for
  tests and `?demo`. `src/api/schema.ts` holds the resource types by hand until `internal/api/openapi.yaml` exists;
  then they become aliases of the types generated from it.

## Agent recordings and the live smoke test

- Tests never reach a model: agent calls replay from recordings made against a fake model server. `make recordings`
  rewrites them (`internal/agent/testdata/recordings`, `examples/m2-fixture/recordings`) after a change to a prompt,
  a lessons file, an output schema or the fake model.
- The live smoke test runs the M2 fixture's score action through the `claude-code` harness against the real `claude`
  CLI (installed, logged in) on two listings, recording into a temporary directory, and spends a few cents:
  `MILLSTREAM_LIVE=1 go test -tags e2e -count=1 -v -run TestLiveScore ./internal/e2e` (`MILLSTREAM_LIVE_MODEL` picks
  the model, `haiku` by default). It is never part of `make ci`; never commit what it records.
- The end-to-end and crash tests sit behind the `e2e` build tag: `make test-e2e`, or `go test -tags e2e ./internal/e2e`.

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
