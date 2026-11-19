# Contributing

How to build Millstream, check a change, and land it. [Architecture](architecture.md) maps the code;
[Testing](testing.md) explains the suites in depth; `make help` lists every command.

## Requirements

- **Go**, at the version `go.mod` names. The linters are pinned in `tools/go.mod` and run through `go tool`, so
  nothing else needs installing for Go work.
- **shellcheck**, which `make ci` runs over `scripts/`.
- **Docker** for everything outside the Go build: the web UI (npm, Vite, Vitest and Playwright run only in containers
  pinned by digest in `ui/images.mk`; Node is never installed on the host), the end-to-end suites (Postgres, S3,
  container actions, the sandbox), the image and the soak.
- Network access for `make lint`, whose vulnerability scan fetches the Go vulnerability database.

## Everyday commands

| Command | Does |
| --- | --- |
| `make build` | Builds `build/millstream` |
| `make ui` | Builds the web UI into `internal/ui/dist/`, embedded by the next build; without it the binary serves a placeholder page |
| `make ci` | The gate: lint, the default test suite and the docs check. CI runs exactly this |
| `make lint-fix` | Applies every auto-fix (`go fix`, `gofmt -s`) |
| `make test-e2e PKG=./internal/foo/...` | The end-to-end tests of the packages a change touches |
| `make test-ui` | The web UI's typecheck, lint and unit tests |
| `make docs` | Checks every Markdown link and the 120-column wrap |

The slow suites (`make test-e2e` in full, `make test-scenarios`, `make test-ui-e2e`, `make soak`, `make test-times`)
run once a change is done or a milestone closes, not on every edit; [Testing](testing.md) says when each one is due.

## How a change lands

1. **Design first, for anything beyond a fix.** A spec goes in `docs/specs/YYYYMMDD.<topic>-design.md` and its plan
   in `docs/plans/YYYYMMDD.<topic>.md`. A plan ends with execution notes recording what was built, the rulings made
   along the way and what was deferred. An architecture decision gets an entry in [decisions.md](../decisions.md).
2. **Code, tests and the owning doc together.** Each page under `docs/` owns one area (the [index](../README.md)
   lists them); a change to that area updates the page in the same commit. The pipeline file format, the action
   protocol and the HTTP API are public contracts: a change to one updates the spec, and for the API
   `internal/api/openapi.yaml`, in the same commit.
3. **`make ci` green**, then the targeted end-to-end tests of the touched packages.
4. **Commit** as `type(scope): summary`, with the reason in the body. Types are `feat`, `fix`, `perf`, `refactor`,
   `docs`, `test`, `build`, `ci` and `chore`; the scope is the package or directory as spelled in the code. A hook
   enforces the format.

Parked ideas and follow-ups go in the [roadmap](../../ROADMAP.md), not in code comments.

## Cutting a release

Only the owner pushes tags. A `v*` tag starts the release workflow (`.github/workflows/release.yml`); `make
test-release` runs the same build as a snapshot in CI first.

1. If `sdk/go` changed, tag `sdk/go/vX.Y.Z` and require it in `internal/scaffold/templates/action/go.mod.tmpl`.
2. Tag `vX.Y.Z-rc.N`: a prerelease, with no Homebrew formula and no `latest` image tag.
3. Check the workflow's `verify` job: signatures, the install script, a scaffolded action built against the proxy.
4. First release only: set the GHCR package `millstream` public after the rc, so the image pulls without a login.
5. Tag `vX.Y.Z`, which publishes the formula and moves `latest`.

## Writing documentation

- Each page has one reader and one job: a guide walks a user through a task, a reference page states a contract, a
  development page explains how the code or the suites work. Link to the owner rather than repeating it.
- The [getting-started guide](../guides/getting-started.md) is run for real: a change to a command it shows means
  running it again and pasting the new output.
- Markdown wraps at 120 columns; tables, code blocks and lines holding a URL are exempt. `make docs` checks the wrap
  and every relative link.
- The repository is meant to go open source: no customer, prospect or partner names, in docs, examples or commit
  messages.
