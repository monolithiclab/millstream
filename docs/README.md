# Millstream documentation

Start with the page that matches what you want to do. Every page is plain Markdown and reads on GitHub or in an
editor; `make docs` checks that every link here resolves.

## Use Millstream

| Page | Read it to |
| --- | --- |
| [Getting started](guides/getting-started.md) | Go from nothing to a running pipeline with an agent step and a human review, on a laptop, in about 40 minutes |
| [Agents in production](guides/agents.md) | Run agent steps whose answers are validated, whose cost is bounded and whose behaviour improves: schemas, abstain and fallback, sandbox, budgets, transcripts, evals, proposals |
| [Deployment](guides/deployment.md) | Run Millstream on a server: the container image, Docker Compose, systemd, remote nodes, failover, security, retention, backups and upgrades |
| `millstream help` | Read the CLI guide and its topics in the terminal (source: [internal/cli/guide.md](../internal/cli/guide.md)) |

## Reference

| Page | Covers |
| --- | --- |
| [Action protocol](reference/action-protocol.md) | How the engine and an action process talk: write an action in any language, without an SDK |
| [Pipeline file](specs/20261014.platform-design.md#4-the-pipeline-file) | Every key of `pipeline.yaml`, action manifests, variables, secrets, triggers and streams (spec section 4) |
| [HTTP API](../internal/api/openapi.yaml) | The OpenAPI document of `/api/v1`, also served by a running engine at `/api/v1/openapi.yaml` |
| [CLI](specs/20261014.platform-design.md#13-cli) | Every command and its flags (spec section 13); `millstream <command> --help` prints the same |

## Understand the design

| Page | Covers |
| --- | --- |
| [Architecture](development/architecture.md) | How the code is organised: the packages, and the path an item takes through them |
| [Platform design](specs/20261014.platform-design.md) | The full specification: concepts, execution model, agents, review, storage, self-improvement, UI, deployment |
| [Decisions](decisions.md) | Architecture decisions D1 onward, each with its reasoning and the cost if it turns out wrong |
| [UI design brief](design/20261015.ui-brief.md) | The web UI's look and feel, with the [design canvas snapshot](design/canvas/README.md) |

## Work on Millstream

| Page | Covers |
| --- | --- |
| [Contributing](development/contributing.md) | Requirements, the everyday commands, the gate, and how a change moves from spec to commit |
| [Testing](development/testing.md) | The test suites, the time budget, the store conformance suite, end-to-end, recordings and UI tests |
| [Soak test](development/soak.md) | The compressed week-long run of the Compose deployment under chaos, and its results |
| [M5 exit scenario](development/m5-exit.md) | The end-to-end scenario that closes milestone M5: production evidence to accepted proposals |

## History and research

| Folder | Holds |
| --- | --- |
| [specs/](specs/) | Design specs, one per piece of work, dated `YYYYMMDD.<topic>-design.md` |
| [plans/](plans/) | Implementation plans, each paired with its spec and ending with execution notes on what was built |
| [reviews/](reviews/) | Reviews and retrospectives, such as the [post-mortem of the proof of concept](reviews/20261020.postmortem.md) |
| [research/](research/) | External reference material: the [naming note](research/20261014.naming.md), a [competitive analysis](research/20261022.convex-workflow.md) of Convex Workflow, an [analysis of agentic harnesses](research/20261026.agentic-harnesses.md) and an [analysis of self-improving software](research/20261105.self-improving-software.md) |
| [design/](design/) | The UI design brief and the design canvas snapshot |

Parked ideas and follow-ups are in the [roadmap](../ROADMAP.md).
