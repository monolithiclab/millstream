# Architecture

How Millstream's code is organised, for someone about to change it. The concepts and the reasoning behind them are
in the [platform design](../specs/20261014.platform-design.md) (section 3 for the vocabulary, section 5 for the
execution model); this page maps them onto the repository. Each package's doc comment (`go doc ./internal/<name>`)
is the authoritative one-paragraph summary, with the spec section it implements.

## The shape of the system

One binary, `millstream` (`cmd/millstream`), is the engine, the CLI, a remote node and the agent adapter. The web UI
is a React application built into the binary.

- The **engine** is the control plane: it applies packages as revisions, fires runs, creates deliveries, runs the
  builtin steps, and serves the HTTP API and the UI. One engine is active per deployment; a standby takes over when
  it dies.
- **Nodes** pull leased deliveries from the engine and run actions as child processes or containers. On a laptop
  one node is embedded in the engine; on a server remote nodes join it over HTTPS.
- The **log store** (SQLite or Postgres) is the only state. Nodes and action processes are stateless and disposable.
- The **artifact store** holds files, content-addressed, in a directory or an S3-compatible bucket.

## The path of an item

A run follows the same path whether it is started by `millstream run` on a laptop or by a served engine:

1. **Apply.** `internal/pipeline` parses a package into a typed pipeline, checks it, and hashes it into a revision
   (there is no version field: the content hash identifies a revision). `internal/schema` checks that each
   producer's output schema fits its consumers, and `internal/expr` compiles the CEL expressions.
2. **Fire.** `internal/engine` fires a trigger (cron, manual, webhook, insight, or an imported stream): it opens a
   run, resolves its variables, and publishes the trigger's item to the stream `trigger.<name>`.
3. **Deliver.** Publishing an item creates one delivery per step that reads its stream. The engine runs the `filter`
   and `dedupe` builtins itself; every other delivery waits for a node.
4. **Run the action.** `internal/node` leases a delivery, and `internal/runtime/exec` starts the action process and
   speaks the [action protocol](../reference/action-protocol.md) (`internal/protocol`) on its stdin and stdout.
   `internal/runtime/container` runs the same process under Docker or Podman. An agent step runs
   `millstream agent` (`internal/agent`), which drives an LLM harness and answers in the same protocol.
5. **Publish.** The node reports `done` with the lease's fencing token. In one atomic unit of the store, the
   outputs become items with deterministic ids (`internal/ids`), are routed, and create the next deliveries. A
   retried delivery republishes the same ids, so at-least-once delivery stays idempotent.
6. **Review.** A `review` step parks items in the inbox until a person decides (`internal/review`, the inbox screen
   and `millstream inbox`).
7. **Observe.** Every state change is an event in the log. Metrics (`internal/metrics`) and insights
   (`internal/insights`) are projections of those events, so the run reports, the metrics and the live view agree.

## Packages

### Engine and execution

| Package | Role |
| --- | --- |
| `internal/engine` | The active engine: revisions, triggers, runs, replay, builtins, leasing, retention |
| `internal/node` | The node: pulls deliveries, runs them on the process pool, heartbeats, reports results |
| `internal/nodeapi`, `internal/nodewire` | The node protocol between remote nodes and the engine (`/node/v1`), and its wire format |
| `internal/runtime/exec` | The action process runtime: child processes speaking the protocol, timeouts, process groups |
| `internal/runtime/container` | Actions run as containers, and the image pinning `millstream lock` uses |
| `internal/runtime/sandbox` | Where proposed, not yet accepted code runs: always a container, never the host |
| `internal/protocol` | The action protocol's messages and framing |
| `internal/conformance` | The protocol conformance suite any action must pass |
| `internal/agent` | The agent adapter: prompt rendering, harnesses, structured output, budgets |
| `internal/review` | Review sampling, reviewer edits and the notifications of parked items |

### Pipelines and data

| Package | Role |
| --- | --- |
| `internal/pipeline` | The pipeline file format: parsing, checking, revision hashing, variables and secrets, the step graph |
| `internal/expr` | The CEL environment: config interpolation, filter and route expressions, eval assertions |
| `internal/schema` | JSON Schema validation and the producer/consumer compatibility check |
| `internal/ids` | Deterministic identifiers for items and idempotency keys |
| `internal/store` | The log store contract: its records and its atomic units |
| `internal/store/sqlstore` | The store over `database/sql`, shared by both databases: the writer loop, units and reads |
| `internal/store/sqlite`, `internal/store/postgres` | The two databases; `internal/store/stores` opens the one a DSN names |
| `internal/store/storetest` | The store conformance suite both databases pass |
| `internal/artifacts` | The artifact store: a local directory, or an S3-compatible bucket (`artifacts/s3`) |
| `internal/redact` | Removes secret values from anything logged or stored |

### Observability and self-improvement

| Package | Role |
| --- | --- |
| `internal/metrics` | Metric names and their per-minute rollups from the event log |
| `internal/insights` | The insights projector and run reports: deterministic analysis, no LLM |
| `internal/eval` | An action's evals: cases scored by CEL assertions or an LLM judge |
| `internal/evidence` | Evidence bundles that carry production history into a development deployment |
| `internal/improve` | The `improve` pipeline, an ordinary embedded package that drafts proposals from insights |
| `internal/distill` | The rules of a distill proposal: training and holdout split, thresholds the candidate code meets |
| `internal/proposals`, `internal/patch` | Proposal rules and the unified-diff patch format they carry |
| `internal/backtest` | Replays a patch over past items and compares before and after |
| `internal/publish` | What an accepted proposal does: apply a revision, write a patch, or open a pull request |

### Interfaces

| Package | Role |
| --- | --- |
| `internal/cli` | Every command, on Kong; state-changing commands go through the API when `--server` names an engine |
| `internal/api` | The HTTP API under `/api/v1` ([OpenAPI](../../internal/api/openapi.yaml)), webhooks and server-sent events |
| `internal/ui` | Serves the web UI embedded from `internal/ui/dist` |
| `internal/scaffold` | The templates `millstream init` writes |
| `sdk/go` | The Go SDK for actions, its own module |
| `ui/` | The React web UI (Vite, Tailwind with the project's tokens only), built in containers by `make ui` |

Support packages for tests (`internal/testdir`, `internal/failpoint`, the `*test` packages) and small utilities
(`internal/memo`) are described by their doc comments.

## Other directories

| Path | Holds |
| --- | --- |
| `examples/` | The fixture packages the tests and the tutorial build on: `m1-fixture`, `m2-fixture`, `m5-distill` |
| `deploy/` | Docker Compose and systemd files, described by the [deployment guide](../guides/deployment.md) |
| `scripts/` | The scripts behind make targets: docs check, test-time budget, image and deploy checks, the e2e lock |
| `tools/` | Pinned linters (`tools/go.mod`) and release tooling (`tools/release/go.mod`) |
| `Dockerfile`, `Dockerfile.release` | The image built from source (`make image`) and the one GoReleaser packages |
