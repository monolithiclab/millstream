# Using Millstream

Millstream runs pipelines of actions connected by persisted streams. This guide is what `millstream help`
prints; `millstream <command> --help` (or `millstream help <command>`) shows a command's arguments and
flags, and `millstream help <topic>` one of the topics at the end.

## Concepts

- A **package** is a directory holding a `pipeline.yaml`, its actions, fixtures and tests.
- A **pipeline** is a graph of **steps**, fired by **triggers**: by hand, on a schedule, by a webhook.
- Steps read and write **streams** of persisted, keyed items; they never call each other.
- A step runs an **action**: a program speaking a JSON-lines protocol on stdio, a container, an agent (a
  model call checked against a schema), or a builtin (`filter`, `dedupe`, `review`).
- A **run** is one firing of a trigger and the deliveries it causes. Every attempt and decision is stored,
  so items can be replayed and failures investigated.

## Everyday workflow

```sh
millstream init pipeline listings     # scaffold a package with a manual trigger, adhoc
cd listings
millstream check                      # validate it
millstream test pipeline              # run its tests against stubbed actions
millstream run --once                 # apply it, fire each manual trigger once, print a summary
millstream run                        # or serve the engine, the API and the UI on 127.0.0.1:7070
millstream trigger listings adhoc     # fire a run, from another shell
millstream status                     # pipelines and recent runs
millstream tail -f                    # events as JSON lines
millstream dead-letters replay --pipeline listings --run <run>   # retry a run's dead letters
```

## Where state lives

- `.millstream/millstream.db`: the SQLite state database (`store.dsn`, or `--db sqlite://<path>`), with `artifacts/`
  beside it.
- `millstream.yaml`: the environment file, read from the current directory (`--env-file` names another).
  Its `store.dsn` can name Postgres instead; `millstream help environment` lists its keys and variables.
- `.millstream/node/`: a remote node's id, package cache and pid file.

## Scripting

- Exit codes: 0 success, 1 failure (the command says why on stderr), 2 usage error.
- `--json` on `inbox`, `insights`, `proposals`, `report` and `backtest` prints JSON, one object per line
  for lists; `tail` always prints JSON lines.
- Millstream never prompts: every input is an argument, a flag, a file or an environment variable.
- Output and help go to stdout; errors, usage after an error and logs (JSON lines) go to stderr.
- `--server <url>` (or `MILLSTREAM_SERVER`) sends the commands that change state through a running
  engine's API, with `MILLSTREAM_TOKEN` as its token.
- Help is colored on a terminal only: `--color=never`, `MILLSTREAM_COLOR=never` or `NO_COLOR` turn it off.
- `millstream help scripting` says how an agent or a script should drive Millstream.

## Topics

`millstream help <topic>` prints one of the sections below; `millstream help topics` lists them.

### scripting

How an agent, or a script, should drive Millstream.

1. Read `millstream help` once, then `millstream <command> --help` before using a command.
2. Prefer `--json` where a command offers it, and parse that, never the tables.
3. Check every exit code: 0 success, 1 failure (read stderr), 2 a usage error to fix, not to retry.
4. Against a running engine, pass `--server <url>` or set `MILLSTREAM_SERVER` (and `MILLSTREAM_TOKEN`
   when it has `auth.token`): writes then go through the engine, which stays the only writer.
5. Never wait for a prompt: there is none. Give `trigger`, `replay` and `run --once` a `--request-id`, so a
   retry after a crash resumes the same run instead of starting another.
6. Decide reviews with `inbox list --json` and `inbox show <delivery> --json`, then `inbox approve` or
   `inbox reject`, naming the decider with `--as`.
7. Long-running commands (`run`, `serve`, `node`, `tail -f`) stop on SIGINT or SIGTERM; prefer
   `run --once` for a run that ends on its own.
8. The listings leave out what the improve package and maintenance use: `proposals create`, `revalidate` and
   `token`, and the global `--direct` (on Postgres, write from this process while no engine runs). Their
   `--help` still documents them.

### environment

The environment file and the variables Millstream reads.

The environment file, `millstream.yaml` by default, says where the packages run: `environment`
(`production` or `development`; `serve` defaults to production, `run` to development), `store` (SQLite or
Postgres), `artifacts`, `listen`, `auth`, `tls`, `node`, `secrets`, `retention`, `ui` and `improve`. The
platform design (section 14) and the deployment guide describe every key.

Three layers set every key, the last one winning: the environment file, then a `MILLSTREAM_` variable, then a
flag. A variable is `MILLSTREAM_<SECTION>_<KEY>` in upper snake case, down the key's path: `ui.baseUrl` is
`MILLSTREAM_UI_BASE_URL`, `improve.patchFile.dir` is `MILLSTREAM_IMPROVE_PATCH_FILE_DIR`. `millstream init
environment` writes the file with every key commented out at its default, each line naming its variable, but for
`improve`, a single line pointing here.

`improve` (development only, spec sections 11.2 to 11.7): `millstream help environment` ends with its keys, each
with its doc, its default or an example, and its variable, read from the code that reads them.

- `MILLSTREAM_SERVER`, `MILLSTREAM_TOKEN`: the engine API that commands changing state go through, and
  its bearer token.
- `MILLSTREAM_VAR_<NAME>`: a pipeline variable, its name in upper snake case (`minScore` reads
  `MILLSTREAM_VAR_MIN_SCORE`); `--var name=value` overrides it.
- `MILLSTREAM_SECRET_<NAME>`: a secret, when the environment file's `secrets` key maps none.
- `MILLSTREAM_NODE_TOKEN`: the token a remote node joins the engine with.
- `MILLSTREAM_COLOR`: `auto`, `always` or `never`, as `--color`; `NO_COLOR` or `TERM=dumb` turn color off
  in auto.
- `USER`: the decider of reviews and proposals unless `--as` names one.

### exit-codes

Every command exits with one of three codes.

- 0: success, including `--help` and `help`.
- 1: failure. The command says why on stderr: an invalid package for `check`, a run that did not succeed
  for `run --once`, a failing case for `test`, an error from the store or the server.
- 2: usage error: an unknown command or flag, a missing argument, flags that exclude each other. The
  usage follows on stderr. `test action` also exits 2 when there is no case to run.
