# CLI reference

<!-- Generated from the commands by `go test ./internal/cli -run Test_cliReference -update`. -->

Pipelines of actions connected by persisted streams. The same text is printed by `millstream <command> --help`, and
`millstream help` prints the guide.

## Global flags

Every command takes these.

| Flag | Help |
| --- | --- |
| `--db=DSN` | Store DSN, overriding store.dsn: sqlite://<path> (created with its directory) or postgres://host/db (default: sqlite://.millstream/millstream.db). |
| `--env-file=ENV_FILE` | Environment file (default: $MILLSTREAM_ENV_FILE, else ./millstream.yaml when it exists); millstream init environment writes one. |
| `--server=URL` | Change state through the API of the engine serving at this URL (default: $MILLSTREAM_SERVER; token: $MILLSTREAM_TOKEN). |
| `--color=auto\|always\|never` | Color the help: auto (on a terminal, unless NO_COLOR or TERM=dumb), always or never (default: $MILLSTREAM_COLOR, else auto). |

## Develop

### `init`

Scaffold a pipeline package or an action.

```
millstream init <command> [flags]
```

#### `init pipeline`

Scaffold a pipeline package: a manual trigger, one Go action step, a fixture, an eval and a package test.

```
millstream init pipeline <name> [flags]
```

| Argument | Help |
| --- | --- |
| `<name>` | The pipeline's name: lowercase letters, digits and single dashes. |

| Flag | Help |
| --- | --- |
| `--dir=DIR` | The directory to create the package in (default: the current one). |
| `--from=improve\|listings` | Copy a package embedded in the binary: listings, the example that runs offline; improve, to fork it. |

#### `init action`

Scaffold an action: Go with the SDK, or raw protocol in Python 3 (a POSIX sh JSON implementation is not realistic).

```
millstream init action <name> [flags]
```

| Argument | Help |
| --- | --- |
| `<name>` | The action's name: lowercase letters, digits and single dashes. |

| Flag | Help |
| --- | --- |
| `--dir=DIR` | The directory to create the action in (default: the current one). |
| `--lang=go\|raw` | go: the Go SDK; raw: the protocol spoken directly, in Python 3 (default: go). |

#### `init environment`

Write millstream.yaml, the environment file, with every key commented out at its default.

```
millstream init environment [flags]
```

Every key is commented out at its default, or at an example when it has none, and each line names the
MILLSTREAM_* variable that sets it. Uncomment and edit the keys to change.

**Examples:**

```
millstream init environment
millstream init environment --dir /etc/millstream
```

| Flag | Help |
| --- | --- |
| `--dir=DIR` | The directory to write millstream.yaml in (default: the current one). |

### `check`

Validate pipeline packages, alone and against each other.

```
millstream check [<dirs> ...] [flags]
```

| Argument | Help |
| --- | --- |
| `[<dirs> ...]` | Package directories (default: the current one); several are also checked against each other: imports, contracts and cycles across pipelines. |

### `vars`

Print a package's variables, their values and sources.

```
millstream vars [<dir>] [flags]
```

| Argument | Help |
| --- | --- |
| `[<dir>]` | Package directory (default: the current one). |

| Flag | Help |
| --- | --- |
| `--var=KEY=VALUE;...` | Set a variable (name=value). |

### `graph`

Print a package's graph as Mermaid or DOT.

```
millstream graph [<dir>] [flags]
```

| Argument | Help |
| --- | --- |
| `[<dir>]` | Package directory (default: the current one). |

| Flag | Help |
| --- | --- |
| `--format=mermaid\|dot` | Output format: mermaid or dot (default: mermaid). |

### `test`

Test a package against stubbed actions, or an action against golden outputs.

```
millstream test <command> [flags]
```

#### `test pipeline`

Run a package's tests/*.yaml against stubbed actions.

```
millstream test pipeline [<dir>] [flags]
```

| Argument | Help |
| --- | --- |
| `[<dir>]` | Package directory (default: the current one). |

| Flag | Help |
| --- | --- |
| `--timeout=DURATION` | How long each test may take to settle (default: 60s). |

#### `test action`

Run an action's tests/*.yaml against golden outputs, or the protocol conformance suite.

```
millstream test action <dir> [flags]
```

| Argument | Help |
| --- | --- |
| `<dir>` | The action directory, holding tests/*.yaml. |

| Flag | Help |
| --- | --- |
| `--package=PACKAGE` | The package directory (default: the nearest directory above the action holding pipeline.yaml; without one, the action runs alone, unless it is an agent). |
| `--harness-mode=replay\|record\|live` | Harness mode of an agent action's calls: replay, record or live (default: replay). |
| `--harness-dir=HARNESS_DIR` | Recordings directory (default: <action>/tests/recordings). |
| `--conformance` | Run the protocol conformance suite instead of the fixtures (exec and container actions). |
| `--timeout=DURATION` | How long each conformance check waits for the action (default: 5s). |

### `eval`

Run an action's evals and score them.

```
millstream eval <action> [flags]
```

| Argument | Help |
| --- | --- |
| `<action>` | The action directory, holding evals/*.yaml. |

| Flag | Help |
| --- | --- |
| `--package=PACKAGE` | The package directory (default: the nearest directory above the action holding pipeline.yaml). |
| `--harness-mode=replay\|record\|live` | Harness mode of the agent's and the judge's calls: replay, record or live (default: replay). |
| `--harness-dir=HARNESS_DIR` | Recordings directory (default: <action>/evals/recordings). |
| `--case=CASE` | Run only this case. |

### `lock`

Pin a package's container and registry actions by image digest.

```
millstream lock [<dir>] [flags]
```

| Argument | Help |
| --- | --- |
| `[<dir>]` | Package directory (default: the current one). |

## Run

### `run`

Run packages locally with an embedded engine and node.

```
millstream run [<dirs> ...] [flags]
```

**Examples:**

```
# Apply the package in the current directory, fire its manual triggers once, print a summary
millstream run --once
# Serve the engine, the API and the UI on 127.0.0.1:7070 until interrupted, logs on stderr
millstream run 2>engine.log
# Run two packages, one importing the other's stream, with a variable set
millstream run ./crawl ./score --var region=lyon
```

| Argument | Help |
| --- | --- |
| `[<dirs> ...]` | Package directories (default: the current one). |

| Flag | Help |
| --- | --- |
| `--var=KEY=VALUE;...` | Set a variable (name=value). |
| `--once` | Fire each pipeline's manual triggers (or its first cron trigger) once, wait for every run to settle or wait only on reviews, print a summary and exit. |
| `--slots=N` | Deliveries the embedded node runs at once (default: 4). |
| `--request-id=REQUEST_ID` | With --once, the request id of the manual firings, so a rerun resumes the same runs. |
| `--lease-ttl=DURATION` | Lease duration; a crashed run's leases are retried after it (default: 60s). |
| `--listen=HOST:PORT` | Serve the API and the web UI on this loopback address (default: ui.listen, else listen, else 127.0.0.1:7070); not with --once. |

### `trigger`

Fire a trigger by hand, or replace a webhook trigger's token.

```
millstream trigger <pipeline> <trigger> [flags]
millstream trigger <command> [flags]
```

#### `trigger fire`

Fire a trigger by hand.

```
millstream trigger <pipeline> <trigger> [flags]
```

**Examples:**

```
millstream trigger listings adhoc --payload '{"city":"Lyon"}'
# A retry with the same request id fires nothing new
millstream trigger listings adhoc --request-id nightly-2026-10-21
```

| Argument | Help |
| --- | --- |
| `<pipeline>` | Pipeline name. |
| `<trigger>` | Trigger name. |

| Flag | Help |
| --- | --- |
| `--payload=PAYLOAD` | Trigger input, as JSON (default: {}). |
| `--var=KEY=VALUE;...` | Set a variable (name=value). |
| `--request-id=REQUEST_ID` | Request id of the firing; firing the same id again is a no-op. |

#### `trigger token`

Replace a webhook trigger's token (--rotate) and print the new one, once.

```
millstream trigger token <pipeline> <trigger> [flags]
```

| Argument | Help |
| --- | --- |
| `<pipeline>` | Pipeline name. |
| `<trigger>` | Webhook trigger name. |

| Flag | Help |
| --- | --- |
| `--rotate` | Replace the token: the old one stops working at once. Required: the store keeps only a hash, so the token in use cannot be shown again. |

### `cancel`

Cancel a run.

```
millstream cancel <pipeline> <run> [flags]
```

| Argument | Help |
| --- | --- |
| `<pipeline>` | Pipeline name. |
| `<run>` | Run id. |

### `replay`

Replay a step over past items, or back-fill an imported stream.

```
millstream replay --pipeline=PIPELINE [flags]
```

Two forms, exclusive: --step or --stream. Dead letters replay with "millstream dead-letters replay".

**Examples:**

```
# Rerun a step over the items published in the last day, in a new replay run
millstream replay --pipeline listings --step score --since 1d
# Back-fill an imported stream from the producer's retained items
millstream replay --pipeline scoring --stream listings.ready
```

| Flag | Help |
| --- | --- |
| `--step=STEP` | Start a replay run of this step over past items on its input streams. Exclusive with --stream; one of the two is required. |
| `--stream=STREAM` | Start a replay run back-filling this imported stream from the producer's retained items on it. |
| `--pipeline=PIPELINE` | Pipeline name. |
| `--run=RUN` | With --step, replay only this run's items. |
| `--since=SINCE` | Replay only the items published within this duration (7d, 12h). |
| `--where=WHERE` | Replay only the items for which this CEL expression holds. |
| `--revision=N` | The revision to run on (default: the active one). |
| `--request-id=REQUEST_ID` | The request id naming the replay run; repeating it starts nothing new. |
| `--var=KEY=VALUE;...` | Set a variable (name=value). |

### `dead-letters`

List, replay and discard dead letters.

```
millstream dead-letters (dlq) <command> [flags]
```

#### `dead-letters list`

List dead letters.

```
millstream dead-letters (dlq) list [flags]
```

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | Only this pipeline's dead letters. |
| `--run=RUN` | Only this run's dead letters. |
| `--state=open\|replayed\|discarded\|all` | State to list: open, replayed, discarded or all (default: open). |

#### `dead-letters replay`

Replay a run's dead letters into that run, reopening it.

```
millstream dead-letters (dlq) replay --pipeline=PIPELINE --run=RUN [<id> ...] [flags]
```

**Examples:**

```
# Retry a run's open dead letters in that run
millstream dead-letters replay --pipeline listings --run <run>
# Retry one, on the pipeline's active revision
millstream dead-letters replay --pipeline listings --run <run> --migrate <id>
```

| Argument | Help |
| --- | --- |
| `[<id> ...]` | The dead letters to replay; all of the run's open ones when none. |

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | Pipeline name. |
| `--run=RUN` | The run whose dead letters to replay. |
| `--migrate` | Move the run to the pipeline's active revision first. |

#### `dead-letters discard`

Discard dead letters, accepting the loss.

```
millstream dead-letters (dlq) discard --pipeline=PIPELINE <id> ... [flags]
```

| Argument | Help |
| --- | --- |
| `<id> ...` | Dead letters to discard. |

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | Pipeline name. |

### `inbox`

List and decide reviews and review samples.

```
millstream inbox <command> [flags]
```

#### `inbox list`

List the open reviews (the default).

```
millstream inbox list [flags]
```

| Flag | Help |
| --- | --- |
| `--inbox=INBOX` | Only this inbox's reviews. |
| `--pipeline=PIPELINE` | Only this pipeline's reviews. |
| `--json` | Print each review with its item as a JSON line. |

#### `inbox show`

Show a review: its item, its display and what may be edited.

```
millstream inbox show <delivery> [flags]
```

| Argument | Help |
| --- | --- |
| `<delivery>` | Delivery id. |

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | The delivery's pipeline, when several pipelines have a delivery with this id. |
| `--json` | Print the review as JSON. |

#### `inbox approve`

Approve a review, optionally editing the item first.

```
millstream inbox approve <delivery> [flags]
```

| Argument | Help |
| --- | --- |
| `<delivery>` | Delivery id. |

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | The delivery's pipeline, when several pipelines have a delivery with this id. |
| `--as=AS` | Who decides (default: $USER, else the account's name). |
| `--version=N` | The review's version you saw, as inbox show prints it (default: its current one, printed). |
| `--edit=PATCH` | Apply this JSON Patch file (RFC 6902) to the item before approving. |
| `--set=/PTR=VALUE,...` | Set a value before approving (repeatable): JSON, or else a string. |
| `--reason=REASON` | Reason code. |
| `--text=TEXT` | Free-text reason. |

#### `inbox reject`

Reject a review, with a reason code when the step lists some.

```
millstream inbox reject <delivery> [flags]
```

| Argument | Help |
| --- | --- |
| `<delivery>` | Delivery id. |

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | The delivery's pipeline, when several pipelines have a delivery with this id. |
| `--as=AS` | Who decides (default: $USER, else the account's name). |
| `--version=N` | The review's version you saw, as inbox show prints it (default: its current one, printed). |
| `--reason=REASON` | Reason code: required when the step lists reason codes. |
| `--text=TEXT` | Free-text reason. |

#### `inbox samples`

List the open review samples.

```
millstream inbox samples [flags]
```

| Flag | Help |
| --- | --- |
| `--inbox=INBOX` | Only this inbox's samples. |
| `--pipeline=PIPELINE` | Only this pipeline's samples. |
| `--json` | Print each sample with its item as a JSON line. |

#### `inbox sample`

Decide a review sample: agree or disagree with the rule that approved it.

```
millstream inbox sample <delivery> <decision> [flags]
```

| Argument | Help |
| --- | --- |
| `<delivery>` | Delivery id. |
| `<decision>` | agree or disagree with the rule that approved the item. |

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | The delivery's pipeline, when several pipelines have a delivery with this id. |
| `--as=AS` | Who decides (default: $USER, else the account's name). |
| `--reason=REASON` | Why, as a reason code or free text. |

## Observe

### `status`

Show pipelines and recent runs.

```
millstream status [flags]
```

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | Only this pipeline. |
| `--limit=N` | Most recent runs to show (default: 10). |

### `tail`

Print events as JSON lines.

```
millstream tail [flags]
```

| Flag | Help |
| --- | --- |
| `--from=N` | Print events after this sequence number (default: 0). |
| `-f, --follow` | Keep polling for new events until interrupted. |
| `--interval=DURATION` | Polling interval with --follow (default: 500ms). |

### `metrics`

Print metric rollups, or their Prometheus exposition.

```
millstream metrics [flags]
```

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | Only this pipeline's metrics. |
| `--name=NAME` | Only this metric. |
| `--since=SINCE` | The minutes since this long ago (10m, 1h, 7d), or all (default: 1h). |
| `--prometheus` | Print the Prometheus text exposition, aggregated over the period, instead of the rollups. |

### `insights`

List, acknowledge and mute insights.

```
millstream insights <command> [flags]
```

#### `insights list`

List insights (the default).

```
millstream insights list [flags]
```

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | Only this pipeline's insights. |
| `--state=open\|acknowledged\|resolved\|muted\|all` | Only insights in this state: open, acknowledged, resolved, muted or all (default: all). |
| `--json` | Print each insight with its evidence as a JSON line. |

#### `insights acknowledge`

Acknowledge an insight: the projector keeps updating it and resolves it when it clears.

```
millstream insights acknowledge (ack) <id> [flags]
```

| Argument | Help |
| --- | --- |
| `<id>` | Insight id. |

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | The insight's pipeline, when several pipelines have an insight with this id. |

#### `insights mute`

Mute an insight: it stays muted whatever the projector finds.

```
millstream insights mute <id> [flags]
```

| Argument | Help |
| --- | --- |
| `<id>` | Insight id. |

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | The insight's pipeline, when several pipelines have an insight with this id. |

### `report`

Print a run's report: critical path, slowest steps, failures, cost, comparison.

```
millstream report <pipeline> <run> [flags]
```

| Argument | Help |
| --- | --- |
| `<pipeline>` | Pipeline name. |
| `<run>` | Run id. |

| Flag | Help |
| --- | --- |
| `--json` | Print the report as JSON. |

## Operate

### `serve`

Run an engine from the environment file, or its standby.

```
millstream serve [flags]
```

| Flag | Help |
| --- | --- |
| `--listen=HOST:PORT` | Serve the API and the web UI on this address (default: listen, else ui.listen, else 127.0.0.1:7070). |
| `--slots=N` | Deliveries the embedded node runs at once (node.embedded) (default: 4). |
| `--lease-ttl=DURATION` | Lease duration; a lost node's leases are retried after it (default: 60s). |

### `apply`

Check packages against the deployment and upload them as new revisions.

```
millstream apply <dirs> ... [flags]
```

| Argument | Help |
| --- | --- |
| `<dirs> ...` | Package directories. |

| Flag | Help |
| --- | --- |
| `--force` | Apply even when a package breaks the contract of a stream another pipeline imports. |
| `--message=MESSAGE` | Why: kept with each revision. |

### `rollback`

Reactivate an earlier revision of a pipeline.

```
millstream rollback <pipeline> [<revision>] [flags]
```

| Argument | Help |
| --- | --- |
| `<pipeline>` | Pipeline name. |
| `[<revision>]` | The revision to reactivate (default: the one before the active one). |

### `pause`

Pause a pipeline or one of its steps, or a pipeline's triggers too.

```
millstream pause <pipeline> [<step>] [flags]
```

| Argument | Help |
| --- | --- |
| `<pipeline>` | Pipeline name. |
| `[<step>]` | Step name; the whole pipeline when omitted. |

| Flag | Help |
| --- | --- |
| `--triggers` | Stop the pipeline's triggers too: it is disabled until resume. |

### `resume`

Resume a pipeline or one of its steps.

```
millstream resume <pipeline> [<step>] [flags]
```

| Argument | Help |
| --- | --- |
| `<pipeline>` | Pipeline name. |
| `[<step>]` | Step name; the whole pipeline when omitted. |

### `purge`

Erase items by key or id, with their descendants and copies.

```
millstream purge (--key=KEY | --item=ITEM) [flags]
```

| Flag | Help |
| --- | --- |
| `--key=KEY` | Erase every item with this key. |
| `--item=ITEM` | Erase the item with this id. |
| `--pipeline=PIPELINE` | Only in this pipeline (the copies imports made are followed all the same). |
| `--request-id=REQUEST_ID` | Names the purge: repeating it after a crash finishes the purge it started. |

### `node`

Run a remote node that joins an engine.

```
millstream node --join=URL[,URL…],... [flags]
```

The node authenticates with $MILLSTREAM_NODE_TOKEN; SIGTERM or "millstream nodes drain" drains it.

**Examples:**

```
millstream node --join https://engine-a:7070,https://engine-b:7070 --labels zone=par --slots 8
```

| Flag | Help |
| --- | --- |
| `--join=URL[,URL…],...` | The engines' URLs, the active one and its standbys: https, or http to a loopback engine. The node follows the active one across a failover. |
| `--labels=K=V[,K=V…]` | Labels steps place their deliveries by (placement.labels). |
| `--allow-exec` | Run exec actions, and unsandboxed agents, as children with this node's privileges: trusted code only (spec section 15.3). |
| `--slots=N` | Deliveries the node runs at once (default: 4). |
| `--ca=FILE` | PEM bundle of the certificate authority the engine's certificate is checked against, besides the system's. |
| `--drain-timeout=DURATION` | On drain or SIGTERM, how long in-flight deliveries may run before the node releases them (default: 30s). |
| `--state-dir=STATE_DIR` | The node's id, its package cache and its pid file (default: .millstream/node). |

### `nodes`

List the remote nodes, drain the one running here, or revoke one.

```
millstream nodes <command> [flags]
```

#### `nodes list`

List the remote nodes: state, health, version, slots, leases held, labels and runtimes.

```
millstream nodes list [flags]
```

| Flag | Help |
| --- | --- |
| `--lease-ttl=DURATION` | The engines' lease duration, which health is measured against (default: 60s). |

#### `nodes drain`

Drain the node running from the state directory: it stops pulling, finishes or releases its deliveries within its drain
timeout, and exits.

```
millstream nodes drain [flags]
```

| Flag | Help |
| --- | --- |
| `--state-dir=STATE_DIR` | The state directory of the node to drain (default: .millstream/node). |
| `--wait=DURATION` | How long to wait for the node to exit (default: 2m). |

#### `nodes revoke`

Revoke a node id: refused from then on, its leases ended at once.

```
millstream nodes revoke <node> [flags]
```

| Argument | Help |
| --- | --- |
| `<node>` | Node id. |

## Improve

### `evidence`

Export an evidence bundle in production; import it in development.

```
millstream evidence <command> [flags]
```

#### `evidence export`

Export an evidence bundle of a pipeline, in production.

```
millstream evidence export --pipeline=PIPELINE [flags]
```

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | The pipeline whose history the bundle samples. |
| `--since=SINCE` | Sample the deliveries created within this duration (7d, 12h) (default: 30d). |
| `--insight=INSIGHT` | Carry this insight alone and sample its step. |
| `--out=OUT` | Where to write the bundle (default: <bundle id>.tar.gz here). |

#### `evidence import`

Import an evidence bundle as read-only history, in development.

```
millstream evidence import <bundle> [flags]
```

| Argument | Help |
| --- | --- |
| `<bundle>` | The bundle evidence export wrote. |

### `improve`

Run the improve pipeline on one insight or every open one; development only.

```
millstream improve [flags]
```

| Flag | Help |
| --- | --- |
| `--insight=INSIGHT` | Propose a fix for this insight (default: every open opportunity, and other open insights at or above improve.minSeverity). |
| `--pipeline=PIPELINE` | Only this pipeline's insights. |
| `--slots=N` | Deliveries the embedded node runs at once (default: 4). |
| `--lease-ttl=DURATION` | Lease duration (default: 60s). |

### `proposals`

List, show and decide proposals.

```
millstream proposals <command> [flags]
```

#### `proposals list`

List proposals (the default).

```
millstream proposals list [flags]
```

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | Only this pipeline's proposals. |
| `--state=drafting\|validating\|ready\|accepted\|rejected\|stale\|all` | Only proposals in this state (default: all). |
| `--json` | Print each proposal as a JSON line. |

#### `proposals show`

Show a proposal: its patch and its validation report.

```
millstream proposals show <id> [flags]
```

| Argument | Help |
| --- | --- |
| `<id>` | The proposal. |

| Flag | Help |
| --- | --- |
| `--json` | Print the proposal as JSON. |

#### `proposals accept`

Accept a ready proposal on its base revision.

```
millstream proposals accept <id> [flags]
```

| Argument | Help |
| --- | --- |
| `<id>` | The proposal. |

| Flag | Help |
| --- | --- |
| `--patch-sha256=PATCH_SHA256` | The hash of the patch you reviewed, as proposals show prints it (default: its current one, printed). |
| `--as=AS` | Who decides (default: $USER, else the account's name). |

#### `proposals reject`

Reject a proposal with a reason, given to later drafts.

```
millstream proposals reject --reason=REASON <id> [flags]
```

| Argument | Help |
| --- | --- |
| `<id>` | The proposal. |

| Flag | Help |
| --- | --- |
| `--reason=REASON` | Why: recorded and given to later drafts. |
| `--as=AS` | Who decides (default: $USER, else the account's name). |

### `backtest`

Validate a patch against past items, before and after.

```
millstream backtest <patch> [flags]
```

| Argument | Help |
| --- | --- |
| `<patch>` | The patch: a unified diff against a revision's package, its base named by a Millstream-Base trailer. |

| Flag | Help |
| --- | --- |
| `--pipeline=PIPELINE` | The pipeline (default: the one with the patch's base revision, else the only one). |
| `--sample=N` | Past deliveries to sample (default: improve.backtest.sample, else 200). |
| `--json` | Print the report as JSON. |

## Help

### `help`

Print the guide, a topic, or a command's help.

```
millstream help [<topic-or-command> ...] [flags]
```

| Argument | Help |
| --- | --- |
| `[<topic-or-command> ...]` | A topic (help topics lists them), or a command: help dead-letters replay prints dead-letters replay --help. |

### `version`

Print the version.

```
millstream version [flags]
```
