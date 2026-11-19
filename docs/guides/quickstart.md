# Quickstart

Run the reference pipeline on a laptop in five minutes, offline, with no model key: a crawler emits sample real
estate listings, a dedupe drops the ones already seen, an agent scores each one, a filter keeps the good scores, and
you approve or reject what is left in the web UI. Every command was run as written; the outputs are real, and your
ids, times and paths will differ.

You need macOS or Linux and Python 3, for the example's crawler. No Go, Git or Docker.

## 1. Install

With Homebrew:

```sh
brew install monolithiclab/tap/millstream
millstream version
```

Or with the install script, which checks the release's checksums (and its signature when `cosign` is installed) and
writes the binary to `/usr/local/bin` (else `~/.local/bin`), or the directory `MILLSTREAM_INSTALL_DIR` names:

```sh
curl -fsSL https://raw.githubusercontent.com/monolithiclab/millstream/main/scripts/install.sh | sh
millstream version
```

```text
millstream 2847931
```

The container image is for servers ([deployment guide](deployment.md)): its runtime has no Python for the example's
crawler.

## 2. Scaffold the example

Work in a directory of your own:

```sh
mkdir ~/millstream-quickstart && cd ~/millstream-quickstart
millstream init pipeline listings --from listings
cd listings
millstream check
```

```text
created /Users/you/millstream-quickstart/listings, the listings example
next: cd /Users/you/millstream-quickstart/listings && MILLSTREAM_HARNESS_MODE=replay MILLSTREAM_HARNESS_DIR=recordings millstream run --once
warning steps.review.out: stream listings.approved is written by step review but no step reads it
listings: 0 errors, 1 warning
```

The package holds:

```text
pipeline.yaml                   crawl → dedupe → score → keep-good → review
actions/crawl/                  the crawler: Python 3, standard library only, emitting listings.json
actions/score/                  the agent: its manifest (schemas, model, cost cap) and prompt.md
recordings/                     the agent's recorded answers, one file per prompt
millstream.yaml                 the environment file: runs the agent as a child process, prices its model
```

The agent replays its answers from `recordings/` when `MILLSTREAM_HARNESS_MODE=replay` and `MILLSTREAM_HARNESS_DIR`
are set: no model is called and no key is needed. Set them for the rest of this page:

```sh
export MILLSTREAM_HARNESS_MODE=replay MILLSTREAM_HARNESS_DIR=recordings
```

## 3. Run it once

`run --once` applies the package, fires its manual trigger, waits for the run and prints what each stream received.
The engine's JSON logs go to stderr, kept out of the package directory here.

```sh
millstream run --once 2>../once.log
millstream inbox
```

```text
listings run_20261117T102859Z-19d734e3_adhoc waitingReview
  listings.raw 6
  listings.scored 6
  listings.shortlist 3
  listings.unique 6
  trigger.adhoc 1
DELIVERY                             PIPELINE  STEP    INBOX     ITEM   PARKED                DEADLINE              DISPLAY
dl_39efc1f181763817d37ca01b681788e2  listings  review  analysts  L1002  2026-11-17T10:28:59Z  2026-11-20T10:28:59Z  L1002: 70 m² in Nantes at 210000 EUR, score 100
dl_c32e6f8be019183605aff4b7bfcd967e  listings  review  analysts  L1004  2026-11-17T10:28:59Z  2026-11-20T10:28:59Z  L1004: 45 m² in Lille at 150000 EUR, score 84
dl_5a32a39a522153dd5aa37b8c4afeaeb9  listings  review  analysts  L1006  2026-11-17T10:28:59Z  2026-11-20T10:28:59Z  L1006: 80 m² in Lyon at 275000 EUR, score 79
```

Six listings were crawled and scored, three scored at least `minScore` (60), and those three wait for a person: the
run is `waitingReview`, so `run --once` stops there and exits 1. State lives in `.millstream/millstream.db`.

## 4. Review in the web UI

Start the engine. It serves the API and the UI until Ctrl-C:

```sh
millstream run 2>../engine.log
```

```text
Millstream is serving the UI and the API on http://127.0.0.1:7070/
```

Open <http://127.0.0.1:7070/inbox>. The first listing, L1002, shows its fields and its score, editable before you
decide. Press **Approve** (or `a`): the inbox moves to the next one, with `u` to undo. `j` and `k` move between items,
and **Reject…** (`r`) asks for one of the pipeline's reason codes.

The CLI decides too. From a second terminal in the same directory:

```sh
millstream inbox approve dl_c32e6f8be019183605aff4b7bfcd967e --set /score=90
millstream inbox reject dl_5a32a39a522153dd5aa37b8c4afeaeb9 --reason overpriced
millstream status
```

```text
Deciding version 1 of the review, its current one: pass --version to decide the version you saw.
dl_c32e6f8be019183605aff4b7bfcd967e approved (delivery done)
Deciding version 1 of the review, its current one: pass --version to decide the version you saw.
dl_5a32a39a522153dd5aa37b8c4afeaeb9 rejected (delivery filtered)
PIPELINE  STATE   REVISION  PAUSED STEPS
listings  active  1         -

RUN                                  PIPELINE  STATE      STARTED               ITEMS  OPEN DEAD LETTERS
run_20261117T102859Z-19d734e3_adhoc  listings  succeeded  2026-11-17T10:28:59Z  24     0
```

With every review decided, the run succeeds. Its report shows the critical path, with the time spent waiting on you
as `REVIEW`, and the agent's cost from the recorded tokens:

```sh
millstream report listings run_20261117T102859Z-19d734e3_adhoc
```

```text
Run run_20261117T102859Z-19d734e3_adhoc of listings (trigger): succeeded
Started 2026-11-17T10:28:59Z, settled 2026-11-17T10:29:50Z, 50.731s, cost $0.0108
Items: listings.approved 2, listings.raw 6, listings.scored 6, listings.shortlist 3, listings.unique 6, trigger.adhoc 1

Critical path (50.731s):
  STEP       DELIVERY                             LATENCY  WAIT  WORK  BACKOFF  REVIEW   SHARE
  crawl      dl_be37e4629320635b66cd8c1becf8a917  39ms     4ms   35ms  0s       0s       0%
  dedupe     dl_3f3c2fc759519cc463c327470fe187cf  12ms     11ms  1ms   0s       0s       0%
  score      dl_0ff6bf28d0eabb9da1d5594e77fb8570  91ms     69ms  22ms  0s       0s       0%
  keep-good  dl_d0c51aeda5d58a9c54f86cd18a75b30e  2ms      1ms   1ms   0s       0s       0%
  review     dl_5a32a39a522153dd5aa37b8c4afeaeb9  50.587s  1ms   0s    0s       50.586s  100%
...
```

Fire the trigger again while the engine runs. The crawler emits the same six listings, and the dedupe holds them all
back, so nothing reaches the agent:

```sh
millstream trigger listings adhoc
millstream status --limit 1
```

```text
run_20261117T102953Z-d3748db7_adhoc
PIPELINE  STATE   REVISION  PAUSED STEPS
listings  active  1         -

RUN                                  PIPELINE  STATE      STARTED               ITEMS  OPEN DEAD LETTERS
run_20261117T102953Z-d3748db7_adhoc  listings  succeeded  2026-11-17T10:29:53Z  7      0
```

In the UI, **Graph** shows the steps with what flowed through them, **Runs** each run's timeline, and an item's page
its journey through every step, the agent's prompt, tokens and cost included. Stop the engine with Ctrl-C.

## Next

- [Getting started](getting-started.md) builds a pipeline like this one from scratch: actions in Go, routing,
  operations, recording an agent against a stand-in model, evals and budgets.
- To score with a real model, change `runtime.agent` in `actions/score/action.yaml`, export the model's key as
  `MILLSTREAM_SECRET_SCORER_KEY` (the pipeline's `scorerKey` secret, which a replay does not need) and unset
  `MILLSTREAM_HARNESS_MODE`; getting started's "With a real model" says how.
- `millstream help` is a short guide to the CLI, and [the documentation index](../README.md) lists every page.
