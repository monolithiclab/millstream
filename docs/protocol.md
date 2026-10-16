# Millstream action protocol, version 1

How the engine and an action process talk. This is the minimal document of milestone M1: enough to write an
action in any language without an SDK. The design and its reasoning are in the
[platform design](specs/20261014.platform-design.md), section 6.3; the Go SDK (`sdk/go`) implements this document.

## Purpose

An action is a program that turns one input item into zero or more output items. Millstream starts it as a child
process, hands it items one at a time, and collects what it emits. The protocol is the whole contract between the
two: an action needs no Millstream library, no network access to the engine and no knowledge of the log store.

## Transport

- The engine writes to the action's **stdin** and reads its **stdout**. Nothing else may be written to stdout.
- **stderr** is free-form logging. The node re-emits each line with the run, step, item, delivery and attempt.
  Writing JSON log lines there is recommended; writing payload values there is not.
- The process starts with its working directory set by the node, and these environment variables:

| Variable | Meaning |
| --- | --- |
| `MILLSTREAM_WORKDIR` | A directory for this process: input artifacts are placed in it, output artifacts are written to it |
| `MILLSTREAM_ASSETS` | The package's `assets/` directory, read-only, when the package has one |

A container action sees the same: its working directory is mounted at the same path inside the container, which
receives no other variable of the node's environment.

## Framing

- One JSON object per line, UTF-8, terminated by `\n`. A line is at most **8 MiB**, newline excluded.
- Every object has a `type`. Keys are lowerCamelCase.
- Receivers ignore keys they do not know, so a later revision of protocol 1 may add optional fields. A new message
  type, or a change in meaning, needs a new protocol version, announced in `hello`.

## Messages

### Engine to action

**`hello`** is the first message a process receives. It names the action and gives the step's config, already
interpolated for the run, and the run's variables. A process only ever receives items with this same action
digest, config, vars and lessons, and the same environment and mounts it started with, so it may prepare once (open
clients, load models) and reuse that for every item.

```json
{"type":"hello","protocol":1,"action":"lab/score-listing","version":"2.1.0","config":{"minRooms":2},"vars":{"city":"Lyon"}}
```

An agent step's `hello` may also carry `lessons`, the step's lessons file as JSON: `rules` (each with `text`, and
optionally `source` and `added`) and `examples` (each with `input` and `output`, and optionally `why` and
`source`). The engine's agent adapter reads it; any other action may ignore it.

**`item`** hands the process one delivery: one item for one step.

```json
{"type":"item","delivery":"dl_3f…","id":"it_9a…","key":"listing-42","payload":{"url":"https://example.com/42"},"artifacts":[{"name":"page.html","path":"in/page.html"}],"run":{"id":"run_2026-10-14T06:00Z_daily","trigger":"daily","startedAt":"2026-10-14T06:00:00Z","revision":"5c1d…"},"attempt":1,"idempotencyKey":"7e2b…"}
```

| Field | Meaning |
| --- | --- |
| `delivery` | The delivery id; every answer about this item carries it |
| `id`, `key` | The item's id and key; `key` may be empty |
| `payload` | The item, any JSON value valid against the action's input schema |
| `artifacts` | Input files, already placed under `MILLSTREAM_WORKDIR` at `path` |
| `run` | The run the item belongs to |
| `attempt` | 1 on the first try, then 2, 3… on retries |
| `idempotencyKey` | Stable across retries of this delivery, new on replay (see below) |

**`shutdown`** asks the process to exit. It is only sent between deliveries. The process exits with status 0; the
end of stdin means the same.

```json
{"type":"shutdown"}
```

### Action to engine

**`ready`** answers `hello` once the process can take items.

```json
{"type":"ready"}
```

**`emit`** stages one output of the current delivery. `key` is optional for a single output and required on every
output when a delivery has several, each output with its own key. `artifacts` lists files the action wrote under
`MILLSTREAM_WORKDIR`, by path relative to it.

```json
{"type":"emit","delivery":"dl_3f…","key":"listing-42","payload":{"url":"https://example.com/42","score":81},"artifacts":[{"name":"report.pdf","path":"out/report.pdf"}]}
```

**`metric`** records a measurement for the current delivery. Names are Prometheus snake_case. A metric that
arrives after the delivery's `done` or `error`, before the next `item` is answered, is ignored rather than treated
as a violation. The engine records an attempt's metrics with the attempt and rolls them up per minute, labelled
with the step; at most 256 per attempt are kept, and a metric without a name or with a value that is not a finite
number is dropped. A name the engine's own metrics use (`items_out`, `queue_depth`…) is stored as `action_<name>`.
One name has a meaning of its own: the `cost_usd` metrics of an attempt add up to its cost in US dollars, recorded on
the delivery and its run and counted against the pipeline's budgets (platform design, sections 5.9 and 7.5). A late
`cost_usd` is ignored like any late metric, so report it before `done` or `error`.

```json
{"type":"metric","delivery":"dl_3f…","name":"pages_fetched","value":12,"labels":{"source":"portal"}}
```

**`done`** ends the delivery successfully. Its staged outputs are published now, all together. The optional
`artifacts` lists the attempt's own files (see Artifacts below), such as an agent's transcript.

```json
{"type":"done","delivery":"dl_3f…","artifacts":[{"name":"transcript.json","path":"transcript.json"}]}
```

**`error`** ends the delivery in failure. `retryable: true` asks for a retry with backoff; `false` sends the item to
the step's dead-letter stream at once. The optional `artifacts` lists the failed attempt's own files, kept so the
failure can be investigated.

```json
{"type":"error","delivery":"dl_3f…","message":"portal answered 503","retryable":true}
```

## Ordering

1. The engine sends `hello`; the process answers `ready` within 10 seconds.
2. The engine sends one `item`. The process answers with any number of `emit` and `metric` messages, then exactly
   one `done` or `error` for that delivery.
3. Only then does the engine send the next `item`, or `shutdown`. A process handles **one delivery at a time**;
   concurrency comes from running several processes.

A process may be long-lived (the default for `exec` and `container` actions: it serves item after item) or
per-item (the default for agents: one `hello`, one `item`, then `shutdown`), as the manifest's `lifecycle` says.
An action written to this document works under both.

## Errors and retries

- Outputs are staged by `emit` and published only on `done`. A delivery that ends in `error`, in a crash or in a
  timeout publishes nothing, so a retry starts from a clean slate.
- A retryable error, a crash, a timeout (the step's `timeout`, 10 minutes by default; the process is killed) and a
  protocol violation are retried with exponential backoff and jitter, up to the step's `retry.maxAttempts`; the
  item then becomes a dead letter.
- A non-retryable error, an input that fails the action's input schema, and several outputs not all keyed or
  sharing a key dead-letter the item at once.
- Delivery is at least once: an item may be handed to an action again after a crash, even when the action had
  already acted on it. The idempotency key makes that safe.

## Idempotency key

`idempotencyKey` is a hash of the step, the item id and the item's generation. It is the same on every attempt of
a delivery and changes when the item is replayed. An action with external effects (`effects: external` in its
manifest: sending an email, writing to a CRM) passes it to the system it calls, so a retried request is recognised
and not applied twice.

## Artifacts

Files go through `MILLSTREAM_WORKDIR`, never through the protocol itself. The directory is emptied before every
`item`.

- **Inputs.** Before an `item`, the node places each of the item's artifacts at `in/<name>` and lists them in
  `artifacts` with that path, relative to the working directory.
- **Outputs.** An action writes its files anywhere under the working directory and names them in the `artifacts`
  of an `emit`: `name` is how downstream steps see the file, `path` where it is, relative to the working directory.
  On `done`, the node moves every named file to the artifact store, then reports the outputs with their artifacts;
  the files of a delivery that ends in `error` are dropped.
- **Attempt artifacts.** `done` and `error` may name files of the attempt itself in their own `artifacts`. They
  belong to the attempt, by its number, never to an output, and are kept for a failed attempt too. On `error`, an
  attempt artifact the node cannot take is dropped with a warning and the action's own error is reported.
- **Names** are file names: not empty, at most 255 bytes, not `.` or `..`, without `/`, `\` or a NUL byte, and
  distinct among one output's artifacts, or among the attempt's.
- **Paths** must stay inside the working directory, pass through no symbolic link and end at a regular file.

A name or path that breaks these rules fails the delivery at once, not retryable (a protocol error that does not
kill the process, since retrying would refuse it again). An artifact store that cannot take the file fails it as a
retryable error. The same content is stored once per pipeline, whatever its name.

## Protocol violations

The engine kills the process and retries the delivery when it receives:

- a line that is not a JSON object, or longer than 8 MiB;
- a `type` it does not know, or a message out of order (an `emit` before `ready`, a second `done`);
- a message for a delivery other than the current one, or any message between deliveries other than a late
  `metric` of the delivery just answered.

On a violation, a timeout or a lost lease, the node kills the action's whole process group, so children the action
started go with it.

An `item` the engine cannot fit in one line (a payload over 8 MiB) is never sent: the delivery fails at once, not
retryable, and the process is kept for the next item.

Several outputs for one delivery where any has no key, or two share a key, is not a protocol violation but a
key-rule error: it is deterministic, so the item is dead-lettered at once instead of retried.

An action that receives something it does not understand should exit with a non-zero status, after logging why on
stderr.

## Conformance

`millstream test action <dir> --conformance [--timeout 5s]` checks an exec or container action against this document,
whatever it computes. It starts the action's process itself, with a temporary `MILLSTREAM_WORKDIR`, reads the raw
lines of its stdout and reports each check as passed, failed (with the reason) or skipped (when a check it needs
failed). The item's payload, config and vars come from the action's first `tests/*.yaml` fixture, else the payload is
a minimal value of the input schema: its required properties, each empty, zero or its first enum value.

| Check | Passes when |
| --- | --- |
| `ready` | `hello` is answered by `ready`, first, within the timeout |
| `unknown-keys` | A `hello` and an `item` carrying a key the action cannot know are answered as usual |
| `framing` | Every line, in every process of the suite, is a JSON object of a type an action writes, under 8 MiB |
| `item-answer` | An `item` gets `done` or `error` within the timeout, every `emit` and `metric` for its delivery |
| `keyed-outputs` | Several outputs of a delivery carry distinct keys (skipped with fewer than two) |
| `idle-silence` | Nothing is written after the answer, over a short window, but a late `metric` of that delivery |
| `malformed-config` | With a `null` config, then with every config value of an unexpected type, the process takes `hello` and answers the item with `done` or `error`, instead of exiting |
| `shutdown` | After `shutdown`, the process exits with status 0 within the timeout, writing nothing more |

The command exits 1 when any check fails. Agent actions are the engine's own adapter and are not checked this way.

## Not in version 1 yet

`abstain` (hand a delivery to the step's fallback) and fallback audits (`audit` on `item`) arrive with
self-improvement in milestone M5, as described in the platform design, section 11.5.
