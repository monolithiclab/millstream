# The soak

The M4 exit criterion (spec section 19) asks for several pipelines, one importing another's stream, to run
unattended for a week with a standby engine and two node hosts. `make soak` is that week compressed: the Compose
deployment of [`deploy/compose/`](../deploy/compose/) under chaos for 30 minutes, with triggers every minute standing
in for the reference schedule. A real seven-day run is out of scope for this proof of concept; the same command runs
it:

```sh
make soak SOAK_DURATION=168h            # a week; SOAK_CHAOS=2m by default
```

## What it does

`TestSoak` (`internal/e2e/soak_test.go`, behind the `e2e` tag, skipped unless `SOAK_DURATION` is set, so neither
`make ci` nor `make test-e2e` runs it):

1. Builds `millstream:dev` (`make image`) and brings up `deploy/compose/compose.yaml` as the Compose project
   `millstream-soak`: Postgres, the active engine, the standby engine and two nodes, exactly as deployed. An override
   (`build/soak/_work/compose.soak.yaml`) only publishes the ports on loopback ports Docker picks, swaps in a
   certificate made for the run, the scorer's secret and the fake model's price (`MILLSTREAM_PRICES`), and gives the
   nodes the same price, unsandboxed agents and the M2 recordings their harness replays: no network call leaves the
   deployment.
2. Applies three packages with the image's own CLI: the M1 fixture with its builtins (a filter and an auto-approving
   review, the dead-lettering `rentability` kept) fired by a cron trigger every minute and by a webhook trigger; a
   pipeline importing its crawled listings; and the M2 fixture, whose score agent replays its recordings, fired every
   minute. Neither keeps its dedupe, which would drop every listing after the first run.
3. For `SOAK_DURATION`, a chaos driver acts every `SOAK_CHAOS` on average (exponential intervals, seeded by
   `SOAK_SEED`): it kills the active engine (`docker kill`, started again 2 to 30 seconds later) or restarts it
   gracefully (`docker restart`), kills a node, or disconnects a node from the project's network for 5 to 90
   seconds, past its 60-second lease at times. Each takeover is measured from the standby's log: from the active
   engine's end to `leading the deployment`, and to its `first dispatch`. The webhook is called every
   `SOAK_WEBHOOK` (45 s) on whichever engine accepts it.
4. At the end: every run started more than `SOAK_STUCK` (5 minutes) before the chaos ended must have settled;
   within `SOAK_STUCK`, every run must settle and the triggered pipelines are disabled between two minutes' cron
   firings (a disabled pipeline's deliveries wait, so disabling one with a run in flight would hold that run);
   `/metrics` and the UI must answer; then, the engines and nodes stopped, the store must hold the crash suite's
   invariants (`readSnapshot`: runs settled, deliveries terminal, no item published twice, no budget reservation
   left, budget days, runs and deliveries agreeing on spend, dedupe keys sound) and every import run must have
   settled after its producer run. When a run stays stuck, `build/soak/stuck.txt` lists the deliveries not
   terminal, and the engines and nodes dump their goroutines into their logs.
5. Writes `build/soak/report.md` (runs, items, deliveries, kills, partitions, failovers, takeover times, the event
   log, the checks) and each service's log under `build/soak/logs/`, then removes the project with
   `docker compose down -v`. It touches no other container, image or volume.

## Results

On 2026-10-17, `millstream:dev` at the M4 branch's head, Docker Desktop on an arm64 Mac:

| Run | Chaos | Runs | Items | Deliveries | Kills | Partitions | Takeovers (end to lock; to first dispatch) | Checks |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `make soak` (30 min) | every 2 min on average | 168 (69 of them imports) | 15,837 | 13,776 | 3 engines | 3 nodes | 29–30 ms; 5.0–6.6 s | all pass |
| `SOAK_DURATION=5m SOAK_CHAOS=20s` | every 20 s on average | 27 (11 imports) | 2,528 | 2,199 | 2 engines, 4 nodes | 1 node | 30–31 ms; 19.0–20.8 s | all pass |

Every run settled, every delivery was terminal, no item was published twice, no reservation was left, spend agreed
across budget days, runs and deliveries, every import run settled after its producer run, and `/metrics` and the UI
answered on both engines. The 69 dead letters of the long run are the M1 fixture's designed one, once per run. The
standby takes the lock within tens of milliseconds of the active engine's death (its session ends with the process);
the first dispatch then waits for the nodes to find the new engine and for work to be due.

What the soak found:

- A node killed in its container never came back: the restarted container ran the new node as pid 1 again, which
  took the stale `node.pid` for a live node and exited in a loop. Fixed by holding a lock on the pid file instead
  (`fix(node)`, `Test_lockStateDirOwnPid`).
- A disabled pipeline's deliveries wait (spec section 17): disabling a pipeline with a run in flight holds that run.
  The soak's first version disabled the triggers as soon as the chaos ended and saw runs stuck; it now disables them
  between two minutes' firings, once every run settled.
