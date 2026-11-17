# Testing

How Millstream's test suites work and what keeps the default one fast. AGENTS.md holds the rules; this page holds the
detail behind them.

## The suites

| Command | What it runs | In `make ci` |
| --- | --- | --- |
| `make test` | The default build: in-process unit tests, without `-race`, within the time budget | Yes |
| `make test-times` | The budget check (`scripts/test-times`): six suite runs, so at milestone close only | No |
| `make test-e2e PKG=./internal/foo/...` | The iteration tier: those packages only, tag `e2e`, no `-race`, no coverage | No |
| `make test-e2e` | Everything but `internal/e2e`, tagged and untagged, under `-race`, with the suite's coverage | No (own CI job) |
| `make test-scenarios` | `internal/e2e` under `-race`: the crash matrix, the chaos suites, the milestone scenarios | No (manual workflow) |
| `make test-exit` | The M5 exit scenario alone, verbose, under the e2e lock, see [m5-exit.md](m5-exit.md) | No (in `test-scenarios`) |
| `make soak` | The Compose deployment under chaos for `SOAK_DURATION` (30m), see [soak.md](soak.md) | No |
| `make test-ui`, `make test-ui-e2e` | The web UI's unit and Playwright suites, in containers | No (own CI jobs) |
| `make test-image`, `make test-deploy` | The image and the deployment files | No (CI's `image` job) |

While working, run `make ci` and `make test-e2e PKG=...` for the packages a change touches. Run the full
`make test-e2e` once, when the change is done, and before closing a milestone. Run `make test-scenarios` before
closing a milestone and after touching the exec runtime, the node, the CLI's run path, the store's concurrency or the
crash recovery. Run `make soak` after touching failover, the
node or the Compose files.

## The time budget

`make test` runs the default build without `-race`, within a budget that `make test-times` measures at milestone
close, not for each change or branch: it runs the suite six times. The script runs
the suite serially (`-p 1 -parallel 1`) three times (five with `CI=1`), times each top-level test from the
`go test -json` event timestamps and takes its median. The sum of the medians is at most 750 ms, and no test's median
is over 20 ms. The slowest run of each listed test is printed beside its median. The script then runs the suite three
more times at `-p 1` with `t.Parallel` in effect: no package's median wall time, from its first test's start to its
last test's end, is over 100 ms.

Compilation, binary start, package initialisation and `TestMain` are not counted. The script says "noisy machine" when
its slowest serial run took over 1.2 times its fastest: the medians still decide, but read the figures with care. It
exits 1 over budget. The figures are `BUDGET_TOTAL_MS`, `BUDGET_TEST_MS` and `BUDGET_PKG_MS` in `scripts/test-times`,
with `TARGET_TOTAL_MS` for the working target.

The working target is 600 ms, which the script warns about when the median total goes above it. A milestone over the
target makes room before it closes.

### What keeps the default suite fast

- Actions run in-process: `exec.FuncLauncher` with `exectest.Serve`, wired through `fakeexec` and
  `cli.Runtime.Launchers`.
- Stores are `sqlite.Memory`. Its template loads from an embedded image, and closed connections, once emptied, serve
  the next store with their statements already prepared (`Clone` is slower: it prepares them all again).
- Fixtures (packages, parsed pipelines, applied runs, insight histories) are built once per process. The files a test
  only reads are committed under `testdata`: the CLI's shared packages are those directories themselves, never
  copies. The store images `internal/store/sqlite/template.db` and `internal/insights/testdata/history.db` are rebuilt
  with `-update` after a schema change.
- What a process does once is done in `TestMain`, outside the budget: SQLite's start and the template's load, the
  common units' first statements, the CEL environments, the JSON Schema compiler, the shared seeded stores.
  `sqlitetest.Warm` serves the packages that open Memory stores; a package's own fixtures sit beside it.
- Time is a fake clock or an injected timer. There is no `go build`, no process, no Docker, no sleep or polling, and
  no file database except the one test that opens a file.
- The store, engine and insights tests run serially: the transpiled SQLite allocates under one process-wide lock, so
  parallel tests contend instead of overlapping. A table's subtests that take microseconds do not call `t.Parallel`
  either: pausing and resuming one costs more than running it.

A test that needs a process, `go build`, Docker or a file database, or that drives the CLI or the engine end to end,
goes behind the `e2e` build tag, in a `*_e2e_test.go` file.

## The store conformance suite

`internal/store/storetest` holds exported functions that take an opener, one per behaviour. They run against SQLite
Memory stores in the default build and against Postgres in `internal/store/postgres` under the tag.

The default build runs them from three packages by area, each with its own warmed process (`sqlitetest.Main` as
`TestMain`, `sqlitetest.Memory` as the opener), so that no package nears the 100 ms limit:

- `internal/store/sqlstore/conformance/delivery`: revisions, firings, leases, done and fail, built-in steps, budgets,
  dead letters, replays, the writer.
- `internal/store/sqlstore/conformance/review`: reviews, audits and fallbacks, imports, insights, evals, proposals, the
  API reads.
- `internal/store/sqlstore/conformance/retention`: retention, purges, rollups, evidence, nodes, missing tables.

Each package has one top-level wrapper per test in its `conformance_test.go`, so `make test-times` measures each test.
The e2e-only tests are wrapped in `internal/store/sqlstore/conformance_e2e_test.go`.

A new store test goes in storetest, listed in `storetest.Tests` and wrapped in the package of its area. A package
nearing 70 ms wall splits again. `TestTestsListed` and `TestConformanceListed` name what is missing, or wrapped
twice. A test that must reach behind the units uses the store's `Exec`, `Unit` or `QueryRow`, written with `?`
placeholders.

### Coverage

`make test` and `make test-e2e` pass `-coverpkg=./...`, so coverage is cross-package: every test credits the
statements it runs in any package, which is how the conformance packages count in `internal/store/sqlstore`.
`make test` checks the default build's coverage against `COVERAGE_MIN`; `make test-e2e` prints the whole suite's.

## End-to-end tests

`make test-e2e` runs `go test -race -count=1 -tags e2e` on every package but `internal/e2e`, and the SDK module's. It
is slow and runs in CI as its own job. `make test-scenarios` runs `internal/e2e`, which takes about ten minutes on ten
cores and many times that on a CI runner, so CI leaves it out (decided 2026-10-27): the Scenarios workflow runs it
when started by hand (`gh workflow run scenarios.yml`). Together they cover:
 - The `internal/e2e` package (`test-scenarios`): the crash matrix on SQLite and Postgres, run with a second pipeline
  importing the fixture's stream so that it covers a Done that imports; the failover chaos suite (`failover_test.go`) on
  the same two pipelines with token auth on, two `serve` processes on Postgres; the node chaos suite (`nodes_test.go`:
  remote node processes killed, cut off by a proxy past their lease, revoked, refused a rotated token, following a
  failover) and
  `nodes_tls_test.go` (a node over HTTPS and HTTP/2 with `--ca`); the M1 and M2 scenarios.
- The S3 store against a real S3-compatible server (SeaweedFS pinned by digest: MinIO publishes no server image any
  more).
- The end-to-end CLI and engine tests, and the real-process twins of the in-process tests (under the tag, `fakeexec`
  runs the built fake binary).
- Docker and Postgres tests, and the long property runs. The candidate sandbox's tests build Go actions in
  `sandbox.DefaultImage`, pinned by digest and pulled once when missing (the backtest's sandbox test skips without
  it).
- The test images (Postgres, SeaweedFS) are Docker Hub images pulled through `mirror.gcr.io`, Google's
  anonymous Docker Hub cache, under the same digests. GitHub's runners share their addresses, and so their anonymous
  quotas: Postgres came from ECR Public until its 500 GB a month ran out on 2026-10-26 (`toomanyrequests: Data limit
  exceeded`). `sandbox.DefaultImage` stays on Docker Hub, since it is the product's default, not a test image.

`make test-e2e PKG=./internal/foo/...` is the iteration tier: `go test -tags e2e -count=1` of those packages alone,
tagged and untagged, without `-race` or coverage.

### The machine-wide lock

`make test-e2e`, `make test-scenarios`, `make test-ui-e2e` and `make soak` take a machine-wide lock first:
`scripts/e2elock` takes `flock` on `$TMPDIR/millstream-e2e.lock` (macOS has no `flock(1)`). Runs from several checkouts
or agents queue, the waiting one printing "waiting for the e2e lock held by <pid> (<checkout>)", instead of loading the
machine together, under which the failover and partition tests time out. Each target runs its `*-locked` twin under
the lock; call that twin directly only when nothing else can be running. `PKG=` runs take no lock.

### Postgres

The Postgres tests (`internal/store/postgres`, `stores`, and the crash suite's Postgres half in `internal/e2e`) run
under the e2e tag in Docker. `pgtest` starts one container of `pgtest.Image` (the official `postgres:17`, pinned by
digest) per test binary on first use, with a tmpfs data directory so that no volume outlives it. It migrates a template
database once and copies a new database from it for each test, dropped when the test ends; `TestMain` calls
`pgtest.Stop`. Without Docker the tests skip, saying so. Run them alone with
`go test -tags e2e ./internal/store/postgres/...`.

### Soak

`make soak` runs the Compose deployment of `deploy/compose` from `make image` under chaos for `SOAK_DURATION`, then
checks the store's invariants and writes `build/soak/report.md`. It is `TestSoak` in `internal/e2e`, skipped unless
`SOAK_DURATION` is set, so neither `make ci` nor `make test-e2e` runs it. It touches only its own Compose project,
`millstream-soak`. See [soak.md](soak.md).

## Agent recordings and the live smoke test

Tests never reach a model: agent calls replay from recordings made against a fake model server. `make recordings`
rewrites them (`internal/agent/testdata/recordings`, `examples/m2-fixture/recordings`, and the listings example's
`internal/scaffold/listings/recordings`, recorded against `examples/fakemodel`) after a change to a prompt, a lessons
file, an output schema or the fake model. `Test_initListings` (`internal/cli`, e2e build) replays the listings
example's: a prompt they no longer match dead-letters its score step there.

The improve pipeline's draft agent replays `internal/improve/testdata/recordings` in `TestImprove`
(`internal/e2e/improve_test.go`): the M2 fixture with a review step, analysts lowering the scores of poor listings,
`millstream improve --insight` on the `reviewPattern` it yields, a `lessons` proposal validated through a real
backtest. Its prompt is the brief `gather-evidence` writes from that run, so a change to the brief, the draft prompt
or schema, the improve package's variables or the fixture misses the recording: `make recordings` (or the test with
`-update`) runs the scenario once, records the fake drafter's answer (the M2 fixture's second lessons version) for
the brief it finds, and runs it again. The default build tests the package's files, check and triggers, and each
step against an in-process fake API (`internal/improve`). `TestDistill` (`internal/e2e/distill_test.go`) does the
same for a `distill` proposal on the M5 distill fixture (`examples/m5-distill`, two days of 270 listings through an
agent tiering them by price per square metre): it records the tier agent at test time against a fake model applying
`tiers.Tier`, replays the drafter's answer (the code in `examples/m5-distill/testdata/tier-rules`) from
`internal/improve/testdata/recordings/distill`, and validates the proposal in the container sandbox on the holdout;
it skips without Docker. `make recordings` and `-update` rewrite that recording too.

`TestM5Exit` (`internal/e2e/m5_exit_test.go`, `make test-exit`, [m5-exit.md](m5-exit.md)) chains both across two
deployments: production runs the review and distill fixtures and exports their evidence, development imports it and
drafts, validates, accepts and publishes both proposals, then runs a day of new listings on the distilled code. It
replays the same draft recordings (the briefs drawn from imported evidence are the local ones), so it needs no
recording of its own; it skips without Docker.

The live smoke test runs the M2 fixture's score action through the `claude-code` harness against the real `claude` CLI
(installed, logged in) on two listings, recording into a temporary directory, and spends a few cents:

```sh
MILLSTREAM_LIVE=1 go test -tags e2e -count=1 -v -run TestLiveScore ./internal/e2e
```

`MILLSTREAM_LIVE_MODEL` picks the model (`haiku` by default). It is never part of `make ci`; never commit what it
records.

## Web UI tests

Everything runs in containers (see AGENTS.md); the images are pinned in `ui/images.mk`.

`make test-ui` runs the typecheck, ESLint (typescript-eslint, jsx-a11y, react-hooks), the Prettier check, and Vitest
with coverage in jsdom: fake timers and a `FakeApi`, never a network or a wall-clock wait. jsdom gets the browser APIs
React Flow needs from `src/test/flow.ts`, installed for every test file. `tokens.test.ts` checks the WCAG contrast of
every text (4.5:1) and UI (3:1) pair from `tokens.css` itself. `i18n.test.ts` fails on a key missing in either
language, a key no source file uses (a key is used when it appears as a quoted literal), and French typography.

While working, `make test-ui FILES='src/screens/improve src/api'` runs the typecheck, then ESLint, Prettier and
Vitest on those paths alone, without coverage; run the whole `make test-ui` once the change is done.

`make test-ui-e2e` builds, then runs Playwright with axe (WCAG 2.2 AA) on the shell in both themes, both languages, at
375 and 1280 px. It reaches the build through `millstream run` (cross-compiled for the container with the UI embedded)
serving the API and the UI over `ui/e2e/fixture`, a package of builtin steps. In order:

1. `e2e/global.setup.ts` seeds runs through the API: four listings parked in the analysts and triage inboxes, one
   filtered out so a run settles, and one the fixture's `price-check` dead-letters (its surface is 0).
2. `e2e/screens.spec.ts` runs axe and the clipping check of `e2e/checks.ts` on every built screen, then the flows
   across screens (graph, item, replayed dead letter; the items list's stream and CEL filter; the reject sheet; the
   density strip; the metrics explorer). `e2e/inbox-flow.spec.ts` decides reviews from the keyboard in French, and
   `e2e/smoke.spec.ts` covers the shell on `?demo`.
3. `e2e/auth.spec.ts` (`playwright.auth.config.ts`) runs on a `millstream run` of the fixture with `auth.token`
   (`e2e/auth/millstream.yaml`): the sign-in screen in both themes, languages and widths, then signing in, a decision
   recorded under `auth.user`, and signing out.
4. The M3 reference demo, `e2e/demo.spec.ts` (`playwright.demo.config.ts`; `make test-ui-demo` runs it alone), once in
   English and light, once in French and dark, each on a `millstream run` of its own over the package make assembles
   in `.build/demo`: `e2e/demo-fixture/pipeline.yaml` (the M2 fixture with a `review` step after `score`), the M2
   fixture's actions with their Go exec actions compiled for the container, and its recordings replayed offline except
   L002's, withheld so that listing dead-letters. `e2e/demo-fixture/millstream.yaml` lets the agent run unsandboxed.
   The spec fires the run from the Runs screen, watches the graph, reads a transcript, decides the reviews from the
   keyboard, checks the run settles and the cost per approved item, replays the dead letter and opens the metrics,
   with axe on every state.

`make ui-shell` opens a shell in the node container (`npm run dev` serves on port 5173).
