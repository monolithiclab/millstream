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
- A pipeline that imports a stream may declare no trigger, and its import runs take the variables resolved when its
  revision was applied, not when they open: they open inside the producer's Done unit (spec section 4.12). A copy is
  made only when the producer's item is newly inserted; earlier items reach a new importer through
  `replay --stream`.
- An item whose payload is gone keeps its row: `items.tombstone` says why (`expired` by retention, `erased` by a
  purge), its payload is `null` and its artifacts `[]` (spec section 9.3). Test the tombstone, never a `null`
  payload, which an action may publish. Retention skips what unfinished work needs; a purge fences its lineage in one
  unit before it erases anything, and a leased delivery of a fenced item is cancelled when it reports
  (`sqlstore.discards`). The engine's retention job (`internal/engine/retention.go`) drives both, with the artifact
  sweep and the pruning of the package cache and the engine's caches.

## Layout

- `cmd/millstream/`: the binary's `main`.
- `internal/`: `cli`, `pipeline`, `ids`, `expr`, `schema`, `protocol`, `store`, `failpoint`, `engine`, `node`,
  `redact` and the other packages named in the M1 plan's file structure.
- `internal/store/`: the log store interface; `sqlstore` is its implementation on `database/sql` (the writer loop,
  every unit, the reads), shared by `sqlite` and `postgres`, which open, migrate and hold the connections and give a
  `sqlstore.Dialect` for what the databases spell differently; `storetest` is the conformance suite both pass;
  `stores` opens the one the environment file names, in one of two modes: `Open` for an engine or a command writing
  from its own process (on Postgres it takes the leadership lock), `OpenReader` for a command that only reads (on
  Postgres read-only, without the lock, its units refused with `store.ErrReadOnly`).
- CLI writes (`internal/cli`): a command that changes state goes through `changeState`, which calls the served
  engine's API when `--server`/`MILLSTREAM_SERVER` names one (`remote.go`), else `withWriter` (SQLite, or Postgres
  with `--direct`); reads use `withStore`, engines (`run`, `serve`) `withStoreMode(engineStore)`. A new command that
  changes state follows the same split.
- `sdk/go/`: the Go action SDK, its own module.
- `ui/`: the web UI, built and tested only in containers (see Web UI).
- `examples/`: fixture and example pipeline packages.
- `deploy/`: the Compose deployment and the systemd units (see Packaging).
- `make ci` is the gate: lint (golangci-lint over the e2e-tagged build, which holds every file, govulncheck, tidy,
  pins, shellcheck), the default test suite without `-race`, and the docs check, for the main module and the SDK
  module alike.

## Test suites

- `make test` runs the default build: in-process unit tests, without `-race`, within a budget measured without
  `-race` by `make test-times` (`scripts/test-times`). It runs the suite serially (`-p 1 -parallel 1`) three times
  (five with `CI=1`), times each top-level test from the `go test -json` event timestamps and takes its median: the
  sum of the medians is at most 500 ms, no test's median is over 20 ms, and the slowest run of each listed test is
  printed beside it. It runs the suite three more times at `-p 1` (`t.Parallel` in effect): no package's median wall
  time, from its first test's start to its last test's end, is over 100 ms. Compilation, binary start, package
  initialisation and `TestMain` are not counted. It says "noisy machine" when its slowest serial run took over 1.2
  times its fastest, and exits 1 over budget.
- The working target is 400 ms, which the script warns about: a change that takes the median total above it makes
  room in the same change.
- What keeps it there: actions run in-process (`exec.FuncLauncher` with `exectest.Serve`, wired through `fakeexec` and
  `cli.Runtime.Launchers`); stores are `sqlite.Memory`, whose template loads from an embedded image and whose closed
  connections, emptied, serve the next store with their statements already prepared (`Clone` is slower: it prepares them
  all again); fixtures (packages, parsed pipelines, applied runs, insight histories) are built once per process, and the
  files a test only reads are committed under `testdata` (the CLI's shared packages are those directories themselves,
  never copies; the store images `internal/store/sqlite/template.db` and `internal/insights/testdata/history.db` are
  rebuilt with `-update` after a schema change); time is a fake clock or an injected timer. No `go build`, no process,
  no Docker, no sleep or polling, no file database but the one that tests opening a file: a test that needs one, or
  drives the CLI or the engine end to end, goes behind the `e2e` build tag, in a `*_e2e_test.go` file. The store, engine
  and insights tests run serially: the transpiled SQLite allocates under one process-wide lock, so parallel tests
  contend instead of overlapping. A table's subtests that take microseconds do not call `t.Parallel` either: pausing and
  resuming one costs more than running it.
- The store tests are the conformance suite `internal/store/storetest`: exported functions taking an opener, one per
  behaviour, run against SQLite in `internal/store/sqlstore`'s tests (one top-level wrapper each in
  `conformance_test.go`, so test-times and coverage count them where the code lives) and against Postgres in
  `internal/store/postgres` under the tag. A new store test goes in storetest, listed in `storetest.Tests` and
  wrapped in sqlstore; `TestTestsListed` and `TestConformanceListed` name what is missing. A test that must reach
  behind the units uses the store's `Exec`, `Unit` or `QueryRow`, written with `?` placeholders.
- Postgres tests (`internal/store/postgres`, `stores`, and the crash suite's Postgres half in `internal/e2e`) run
  under the e2e tag in Docker: `pgtest` starts one container of `pgtest.Image` (the official `postgres:17`, pinned by
  digest) per test binary on first use, its data directory a tmpfs (no volume outlives it), migrates a template
  database once and copies a new database from it for each test, dropped when the test ends; `TestMain` calls
  `pgtest.Stop`. Without Docker they skip, saying so. Run them alone with
  `go test -tags e2e ./internal/store/postgres/...`.
- `make test-e2e` (`go test -race -count=1 -tags e2e ./...`, and the SDK module's) runs everything, tagged and
  untagged, under the race detector, and prints the whole suite's coverage (`make test` checks the default build's
  against `COVERAGE_MIN`): the `internal/e2e` package (crash matrix on SQLite and Postgres, run with a second pipeline
  importing the fixture's stream so that it covers a Done that imports, the failover chaos suite of
  `failover_test.go` on the same two pipelines with token auth on, two `serve` processes on Postgres, M1 and M2
  scenarios), the end-to-end CLI and engine tests, the real-process twins of the in-process tests (under the tag,
  `fakeexec` runs the built fake binary), Docker and Postgres tests, and the long property runs. It is slow, is not
  part of `make ci`, and runs in CI as its own job; run it before closing a milestone and after touching the exec
  runtime, the node, the CLI's run path, the store's concurrency or the crash recovery.

## Web UI

- `ui/`: the React 18 + TypeScript (strict) + Vite + Tailwind CSS single-page app of spec section 12.2. `src/shell/` is
  the app shell (navigation, pipeline switcher, command palette, shortcut help, live mark), `src/routes.ts` the screens
  and their URLs, `src/screens/` one folder per screen, `src/common/` the pieces screens share, one module per concept
  (status glyphs and badges, loading, empty and error states, confirmation dialog, notices, ids, the segmented toggle,
  the pipeline filter, JSON viewer, log tail, class lists in `styles.ts`, `useResource` for every API read,
  `useLoadMore` for its next pages, `useThrottle`, `arrowKeys` for tab lists and toggles, `useFormat`, `useNow`),
  `src/api/` the HTTP API client, `src/auth/` the session gate (the sign-in screen whenever the API answers 401, on
  a server with `auth.token`; every API method goes through its guard) and `useSession` (the shell's Sign out),
  `src/live/` the server-sent events hub (one reference-counted connection per URL, held
  in the API context and shared by every subscriber) and its hook, `src/i18n/` the catalogues and `Intl` formatting,
  `src/styles/` the tokens, `e2e/` the Playwright tests. The pipeline graph is React Flow laid out by dagre; jsdom gets
  the browser APIs React Flow needs from `src/test/flow.ts`, installed for every test file.
- No host toolchain (D35): npm, Vite, Vitest and Playwright run only in containers, from images pinned by digest in
  `ui/images.mk`. Never install Node on the host. `node_modules` and the npm cache live in the named volumes
  `millstream-ui-node-modules` and `millstream-ui-npm-cache`; containers run as the host uid.
- `make ui` builds into `internal/ui/dist/`, which `internal/ui` embeds (`//go:embed all:dist`) and serves as an SPA
  (index.html for client routes, immutable caching for `assets/`). The build is not committed; the committed
  `internal/ui/dist/placeholder.html` is served until it exists, so `go build` never needs npm.
- `make test-ui`: typecheck, ESLint (typescript-eslint, jsx-a11y, react-hooks), Prettier check, and Vitest with coverage
  in jsdom: fake timers and a `FakeApi`, never a network or a wall-clock wait. `make test-ui-e2e` builds, then runs
  Playwright with axe (WCAG 2.2 AA) on the shell in both themes, both languages, at 375 and 1280 px, reaching the build
  through `millstream run` (cross-compiled for the container with the UI embedded) serving the API and the UI over
  `ui/e2e/fixture`, a package of builtin steps. `e2e/global.setup.ts` seeds runs through the API first: four listings
  parked in the analysts and triage inboxes, one filtered out so a run settles, and one the fixture's `price-check`
  dead-letters (its surface is 0). `e2e/screens.spec.ts` runs axe and the clipping check of `e2e/checks.ts` on every
  built screen, then the flows across screens (graph, item, replayed dead letter; the items list's stream and CEL
  filter; the reject sheet; the density strip; the metrics explorer); `e2e/inbox-flow.spec.ts` decides reviews from the
  keyboard in French; `e2e/smoke.spec.ts` covers the shell on `?demo`. Then `make test-ui-e2e` runs
  `e2e/auth.spec.ts` (`playwright.auth.config.ts`) on a `millstream run` of the fixture with `auth.token`
  (`e2e/auth/millstream.yaml`): the sign-in screen in both themes, languages and widths, then signing in, a
  decision recorded under `auth.user`, and signing out. Then it runs the M3 reference
  demo, `e2e/demo.spec.ts` (`playwright.demo.config.ts`; `make test-ui-demo` runs it alone), once in English and light,
  once in French and dark, each on a `millstream run` of its own over the package make assembles in `.build/demo`:
  `e2e/demo-fixture/pipeline.yaml` (the M2 fixture with a `review` step after `score`), the M2 fixture's actions with
  their Go exec actions compiled for the container, and its recordings replayed offline but L002's, withheld so that
  listing dead-letters; `e2e/demo-fixture/millstream.yaml` lets the agent run unsandboxed. It fires the run from the
  Runs screen, watches the graph, reads a transcript, decides the reviews from the keyboard, checks the run settles and
  the cost per approved item, replays the dead letter and opens the metrics, with axe on every state. `make ui-shell`
  opens a shell in the node container (`npm run dev` serves on port 5173). Both test targets run in CI as their own
  jobs, not in `make ci`.
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
  tests and `?demo`, over the one demo dataset of `demo.ts` (every screen's data, linked: an inbox card, a timeline bar
  or an insight sample opens an item the item screen has). `src/demo.ts` wires them for `?demo`, and `main.tsx` loads it
  with a dynamic import only in demo mode, so neither is in the main bundle; the shell then shows a Demo data badge with
  Exit demo. `src/api/openapi.d.ts` is generated from `internal/api/openapi.yaml` by `make ui-types` and committed;
  `src/api/schema.ts` aliases its schemas, so the rest of the UI imports resource types from there.

## HTTP API

- `internal/api` is the HTTP API of spec section 12.1: `routes` in `api.go` lists every operation, `read.go` and
  `actions.go` the handlers (reads from the store, actions through the store's units or the engine, the same code
  paths as the CLI), `dto.go` the JSON resources, `broker.go` the live event broker, `logs.go` the live log ring and
  `sse.go` the streams. `millstream run` serves it with the UI (`internal/cli/serve.go`).
- `internal/api/openapi.yaml` is the contract, embedded and served at `/api/v1/openapi.yaml`. A change to a route or
  a resource changes the document and spec section 12.1 in the same commit. The tests enforce it: every route is
  documented and called by a test, the document has no operation the server lacks, and every response a handler test
  sees validates against the document's schemas (`internal/schema.CompileDocument`).
- Access (`auth.go`): without `Options.Auth` the API serves loopback clients only; with it (`auth.token`, wired by
  `internal/cli`'s `apiAuth`) every request carries the token as a bearer token or the UI's session cookie, the Host
  guard also accepts `ui.baseUrl`'s and `listen`'s hosts, and the reviewer is `auth.user`. Every state-changing
  request is checked for a foreign `Origin`, with or without the token. `/metrics` (`prometheus.go`) is served
  beside the API by the same `Server`, outside the OpenAPI document; `metrics.Exposition` renders the store's
  `MetricTotals` and the engine's own families, and its tests parse the output as the Prometheus text format.
- Handler tests call the server through `httptest.NewRecorder` on the shared fixture, seeded once per process in
  `TestMain` (`sharedFixture`), or on a clone of it for a test that changes state; live tests drive the broker by
  hand (`pump`, `flush`) with a fake clock, never a listener or a wall-clock wait. The real listener and SSE client
  are in `internal/cli`'s e2e tests.
- After changing the document, regenerate the UI's types: `make ui-types` writes `ui/src/api/openapi.d.ts`
  (openapi-typescript in the node container), committed; `ui/src/api/schema.ts` aliases them.

## Agent recordings and the live smoke test

- Tests never reach a model: agent calls replay from recordings made against a fake model server. `make recordings`
  rewrites them (`internal/agent/testdata/recordings`, `examples/m2-fixture/recordings`) after a change to a prompt,
  a lessons file, an output schema or the fake model.
- The live smoke test runs the M2 fixture's score action through the `claude-code` harness against the real `claude`
  CLI (installed, logged in) on two listings, recording into a temporary directory, and spends a few cents:
  `MILLSTREAM_LIVE=1 go test -tags e2e -count=1 -v -run TestLiveScore ./internal/e2e` (`MILLSTREAM_LIVE_MODEL` picks
  the model, `haiku` by default). It is never part of `make ci`; never commit what it records.
- The end-to-end and crash tests sit behind the `e2e` build tag: `make test-e2e`, or `go test -tags e2e ./internal/e2e`.

## Packaging

- `Dockerfile` builds the image from source: the UI in the node image of `ui/images.mk` (keep its digest equal to
  `UI_NODE_IMAGE`), the binary with the UI embedded in the golang image of go.mod's toolchain, then
  `gcr.io/distroless/static-debian13:nonroot`. Every `FROM` is pinned by digest (`lint-pins`). `Dockerfile.release`
  is GoReleaser's: the same runtime around the binaries it built; keep the two runtime stages in step.
- `make image` builds `millstream:dev`; `make test-image` checks it prints its version and that `millstream run`
  in it serves the built UI (not the placeholder) and the API over `ui/e2e/fixture`, from a node container sharing
  its network namespace (run listens on loopback only). `make test-deploy` runs `docker compose config` on
  `deploy/compose` and `systemd-analyze verify` and `security` on `deploy/systemd` in a pinned ubi-init image. None
  of them is in `make ci`; CI's `image` job runs them.
- `make release-snapshot` runs GoReleaser (`.goreleaser.yaml`) with `--snapshot --skip=sign`: archives, an SBOM
  each, checksums and local images in `dist/`, nothing signed or pushed. GoReleaser and syft are pinned in their own
  tools module, `tools/release/go.mod`, so their dependencies never move the linters' in `tools/go.mod`; bump them
  with `go get -tool -modfile=tools/release/go.mod <module>@<version>`.
- `deploy/compose/millstream.yaml` and the units reference `node --join` (M4 Task 4) before it exists: a task that
  changes that command or the environment file's keys updates `deploy/` and `docs/deploy.md` in the same commit.
  `TestDeployEnvironmentParses` (`internal/pipeline`) parses the Compose environment file strictly.

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
| `docs/deploy.md` | Deployment: the image, Compose, systemd, security, backups and upgrades |
| `docs/design/20261015.ui-brief.md` | Design brief for the web UI, written for Claude Design |
| `docs/design/canvas/` | Snapshot of the Claude Design canvas for the web UI, with its version id |
| `ROADMAP.md` | Parked ideas and follow-ups |

## Workflow

1. Minimal change, with tests, plus the owning doc in the same change
2. `make ci` green
3. Commit with the session's trailers; push only when asked
