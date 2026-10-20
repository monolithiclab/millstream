# AGENTS.md

Millstream is a Monolithic Lab pipeline platform (Go engine, React UI) under construction. See [README.md](README.md)
for what it is, the documents below for the reasoning, [docs/testing.md](docs/testing.md) for how the suites work,
and `make help` for every command.

## Hard rules

- The repo is meant to go open source: no customer, prospect or partner names anywhere, in docs, examples or
  commit messages. The reference example is the generic real estate pipeline.
- Each milestone is implemented from its plan in `docs/plans/`.
- The pipeline file format, the action protocol and the HTTP API are public contracts: a change to one updates the
  spec in the same commit. For the API that is `internal/api/openapi.yaml` and spec section 12.1 together; the tests
  fail on an undocumented route, a documented operation the server lacks, or a response that does not validate.
- A change to the `node --join` command or the environment file's keys updates `deploy/` and `docs/deploy.md` in the
  same commit.

## Constraints that look like bugs

- Pipelines have no version field on purpose: the engine identifies revisions by content hash (spec section 4.2).
- Thresholds belong in `filter` steps, not in actions, so actions stay reusable (spec section 4.7).
- At-least-once delivery is deliberate; idempotency comes from deterministic item ids (D9), not from exactly-once
  machinery.
- A pipeline that imports a stream may declare no trigger, and its import runs take the variables resolved when its
  revision was applied, not when they open: they open inside the producer's Done unit (spec section 4.12). A copy is
  made only when the producer's item is newly inserted; earlier items reach a new importer through
  `replay --stream`.
- The environment (`production` or `development`, spec section 11.2) is the environment file's key, else the command's
  default: `serve` production, `run` development. Production refuses `improve.enabled` and `improve.publish: apply`
  (`pipeline.Environment.CheckMode`, `ErrProduction`), and every proposal: created, revalidated, or its token minted
  (`Engine.developmentOnly`); a command that runs the `improve` pipeline calls `Environment.RequireDevelopment`.
- An insight trigger fires only for the insights of other pipelines (`engine/insight.go`), keyed by the insight and
  its `insightOpened` or `insightEscalated` event: a pipeline never feeds itself, and reading an event again finds
  the run it fired.
- An item whose payload is gone keeps its row: `items.tombstone` says why (`expired` by retention, `erased` by a
  purge), its payload is `null` and its artifacts `[]` (spec section 9.3). Test the tombstone, never a `null`
  payload, which an action may publish. Retention skips what unfinished work needs; a purge fences its lineage in one
  unit before it erases anything, and a leased delivery of a fenced item is cancelled when it reports
  (`sqlstore.discards`). Both run from the retention job, `internal/engine/retention.go`.
- `reviewDecided` events say whether a reason text came (`text`), never the text.
- Imported evidence bundles live whole in the artifact store, recorded in `evidence_imports`, never in the
  deployment's own items.
- Proposals (spec section 11.3) are accepted on their base revision only, and an accepted one makes overlapping open
  proposals stale; apply and rollback make proposals on another base stale. The proposals-only token is checked in
  `ServeHTTP` against `proposalRoutes` before routing (`internal/api/proposals.go`). Backtest jobs live in memory, not
  the store.
- The `improve` pipeline (spec section 11.3) is an ordinary package embedded in `internal/improve/package`, loaded by
  `run` (while serving) and `serve` when `improve.enabled` is set; its exec steps run the binary itself (the
  program name `millstream` is the running binary, `millstream improve-action <step>`) and reach the API through
  the `improveApi` and `improveToken` secrets the engine supplies. The engine holds no drafting logic.
- A distill's holdout (spec section 11.5) is the engine's alone: `distill-training` serves the training side, and
  validation recomputes the holdout from the `holdoutFrom` in the action's `distill.yaml`, never earlier than the
  boundary the engine served. Nothing the improve package holds may name a holdout item; the split is by entity (item,
  key, lineage root), never by delivery.
- Proposed exec code (files a proposal's patch touches) runs only through `internal/runtime/sandbox`, in backtests
  and evals alike; without a container runtime it is refused, never run on the host. The publish actions
  (`internal/publish`) re-check the patch's hash and path rules themselves, whatever the caller checked.
- Webhook triggers (`POST /hooks/{pipeline}/{trigger}`, `internal/api/hooks.go`) sit outside `/api/v1`, before the
  Host guard and outside `auth.token` and the Origin check, on purpose: the trigger's own token authenticates them.
  Trigger tokens are stored hashed and never logged.
- Without `auth.token` the API serves loopback clients only; every state-changing request is checked for a foreign
  `Origin`, with or without the token.

## Where things go

- A CLI command that changes state goes through `changeState` (`internal/cli`), which calls the served engine's API
  when `--server`/`MILLSTREAM_SERVER` names one, else `withWriter`; reads use `withStore`. A command that only reads
  opens the store with `stores.OpenReader` (on Postgres: read-only, no leadership lock).
- CLI help is `internal/cli/help.go` (three levels, colored on a terminal only) and the `millstream help` guide is
  `internal/cli/guide.md`; a new command takes a `group:""` tag and a one-line `help:""`, examples go in a `Help()`
  method, and `go test ./internal/cli -run Test_help -update` rewrites the golden files.
- A new store test goes in `internal/store/storetest`, listed in `storetest.Tests` and wrapped in the
  `conformance_test.go` of the `internal/store/sqlstore/conformance/` package of its area (`delivery`, `review` or
  `retention`; a package nearing 70 ms wall splits again); `TestTestsListed` and `TestConformanceListed` name what is
  missing, or wrapped twice.
- A test that needs a process, `go build`, Docker, a file database or drives the CLI or engine end to end goes behind
  the `e2e` build tag, in a `*_e2e_test.go` file.
- Architecture decisions live in `docs/decisions.md`. Until the first public release the log is kept current: edit
  an entry to match the design and remove superseded ones. From the first release on, it is append-only.
- Specs and plans: `docs/specs/YYYYMMDD.<topic>-design.md` and `docs/plans/YYYYMMDD.<topic>.md`.
- Parked ideas and follow-ups: `ROADMAP.md`.

## Testing

- `make test` (the default build, no `-race`) has a time budget checked by `make test-times`: medians summing to at
  most 750 ms, no test over 20 ms, no package over 100 ms. The working target is 600 ms: a change that takes the
  median total above it makes room in the same change. What keeps it there: [docs/testing.md](docs/testing.md).
- Tiers: while working, run `make ci` and `make test-e2e PKG=./internal/foo/...` for the packages a change touches.
  Run the full `make test-e2e` (race, every tag, Docker, Postgres, S3, chaos) once when the change is done, before
  closing a milestone, and after touching the exec runtime, the node, the CLI's run path, the store's concurrency or
  crash recovery. Run `make soak` (docs/soak.md) after touching failover, the node or the Compose files. None of these
  is in `make ci`; CI runs `test-e2e`, the UI suites and the image checks as separate jobs.
- `make test-e2e`, `make test-ui-e2e` and `make soak` queue behind a machine-wide lock (`scripts/e2elock`); `PKG=`
  runs take none. Never run their `*-locked` twins while another run may be going: the failover tests time out.
- After a schema change, rebuild the committed store images (`internal/store/sqlite/template.db`,
  `internal/insights/testdata/history.db`) with `-update`.
- Tests never reach a model: agent calls replay from recordings. `make recordings` rewrites them after a change to a
  prompt, a lessons file, an output schema or the fake model. The live smoke test (`MILLSTREAM_LIVE=1`, see
  docs/testing.md) spends money and is never part of `make ci`; never commit what it records.

## Web UI

- No host toolchain (D35): npm, Vite, Vitest and Playwright run only in containers, from images pinned by digest in
  `ui/images.mk`. Never install Node on the host.
- `make ui` builds into `internal/ui/dist/`, embedded by `internal/ui`. The build is not committed; the committed
  `internal/ui/dist/placeholder.html` is served until it exists, so `go build` never needs npm.
- Only token utilities exist: `ui/src/styles/index.css` clears Tailwind's palette, and every colour, font, radius,
  shadow and motion comes from `ui/src/styles/tokens.css`, whose contrast `tokens.test.ts` checks.
- i18n: `t()` accepts only keys of `src/i18n/en.json`; keep keys literal (no built-up names), or `i18n.test.ts`
  reports them unused. French copy needs a no-break space before `: ; ! ? »`. Numbers, money, durations and relative
  times go through `src/i18n/format.ts`.
- After changing `internal/api/openapi.yaml`, run `make ui-types` and commit `ui/src/api/openapi.d.ts`; the UI
  imports resource types from `src/api/schema.ts`, never from the generated file.
- Unit tests use fake timers and the `FakeApi`, never a network or a wall-clock wait. `make test-ui` and
  `make test-ui-e2e` (Playwright and axe, WCAG 2.2 AA) are not in `make ci`.

## Packaging

- `Dockerfile`'s node stage digest equals `UI_NODE_IMAGE` in `ui/images.mk`; its runtime stage and
  `Dockerfile.release`'s stay in step. Every `FROM` is pinned by digest (`lint-pins`).
- GoReleaser and syft are pinned in `tools/release/go.mod`, apart from the linters' `tools/go.mod`; bump them with
  `go get -tool -modfile=tools/release/go.mod <module>@<version>`.
- `make test-image` and `make test-deploy` are not in `make ci`; CI's `image` job runs them.

## Documentation

| Document | Owns |
| --- | --- |
| `docs/specs/20261014.platform-design.md` | The platform design: concepts, file format, execution model, protocol, UI, deployment, milestones |
| `docs/decisions.md` | Architecture decisions D1 onward |
| `docs/protocol.md` | The action protocol, for action authors and SDKs |
| `docs/deploy.md` | Deployment: the image, Compose, systemd, security, backups and upgrades |
| `docs/tutorial.md` | The getting-started tutorial: every command and output in it is run for real |
| `docs/testing.md` | The test suites: budget mechanics, fixtures, Postgres, end-to-end and UI suites |
| `docs/soak.md` | The soak test |
| `docs/design/` | The web UI's design brief and the Claude Design canvas snapshot |

## Workflow

1. Minimal change, with tests, plus the owning doc in the same change
2. `make ci` green
3. Commit with the session's trailers; push only when asked
