# The M5 exit

The M5 row of the platform design (spec section 19) closes on this criterion:

> Evidence exported from a production deployment of the reference pipeline yields, in development, an accepted
> `lessons` proposal and an accepted `distill` proposal that cuts agent cost with holdout agreement of at least 0.98.

`TestM5Exit` (`internal/e2e/m5_exit_test.go`) is that scenario, end to end through the binary, offline and
deterministic: every agent call replays a recording. `make test-exit` runs it alone under the e2e lock and prints what
it measured; it needs Docker, since proposed code runs only in the container sandbox (it skips without).

## The scenario

Two deployments, each its own SQLite store and environment file.

**Production** (`environment: production`, `improve.evidence.sample: 1000`) runs the two reference pipelines:

- `m2-review`, the M2 fixture with a review step after `score`. Every listing passes once; the analysts lower the
  score of each poor listing by the amount the second lessons version asks for. The insights projector opens a
  `reviewPattern` on `review` whose hint is a `lessons` proposal on `score`.
- `m5-distill`, two days of 270 synthetic listings through the `tier` agent, whose answers follow a rule (the price
  per square metre). 540 deliveries with a bounded output make `tier` `distillable`.

Production refuses what only development does: `millstream improve` fails ("runs in development deployments only"),
and an environment file with `improve.publish: apply` is refused when it is read. `millstream evidence export` writes
one bundle per pipeline (71 items of `m2-review`, 1,082 of `m5-distill`, every tier delivery sampled). Production then
runs a third day of listings on the agent: the cost the distilled code is measured against.

**Development** (`environment: development`, `improve.enabled`, `improve.publish: apply`) applies the same two
packages and imports both bundles. It has no history of its own: everything below reads the imported evidence.

1. `millstream insights list` shows the two production insights beside the deployment's own, each naming its bundle
   in `imported`.
2. `millstream improve --insight <reviewPattern>` gathers the evidence (the reviewers' edits are read from the bundle
   through the API), the recorded draft agent answers the lessons file, and the engine validates the proposal by a
   backtest of `score` on the bundle's sample. It is `ready`.
3. `millstream improve --insight <distillable>` reads the training half of the engine's split of the imported pairs,
   the recorded draft agent answers Go code keyed and abstaining as the step needs, and the engine builds and runs it
   in the sandbox on the holdout half. It is `ready`.
4. A served development engine accepts both (`proposals accept --patch-sha256`, the hash of the patch read) and
   publishes them with `apply` (`POST /proposals/{id}/publish`): two development revisions.
5. The `m5-distill` revision, materialized from the API's files with the code built on the `PATH` as its manifest's
   bare command asks, runs the third day: the code tiers what it covers, the agent under `fallback` the rest.

## What it measured

| Measure | Value |
| --- | --- |
| Lessons backtest | 46 sampled deliveries from the bundle; agreement with the reviewers 0.58 before, 1.00 after |
| Distill holdout | 270 items, 260 covered (coverage 0.963), 260 agreeing (agreement 1.000) |
| Agent cost over the holdout | 0.227238 USD, of which the code saves 0.218868 USD |
| Day 3 in production | 270 agent calls, 0.227307 USD |
| Day 3 in development | 260 tiered by the code, 10 by the agent (the listings without a surface), 0.008385 USD |

Development's day costs 3.7% of production's, every tier is the rule's, and the run is on the published revision.
The test asserts each of these: thresholds where the spec sets one (agreement at least 0.98, coverage at least 0.6, at
least 200 covered of at least 250 holdout items), exact counts elsewhere, and a development cost under a tenth of
production's.

## What the exit found

- **Imported insights were invisible.** The improve pipeline reads insights and items through the API, which served
  only the deployment's own: a development deployment could import a bundle but not act on its insights. The API and
  `millstream insights` now list the open insights of the imported bundles that the deployment does not record
  itself, and an item the deployment lacks is read, with its lineage, in its pipeline's bundles (spec sections 11.2
  and 12.1).
- **Unkeyed distilled outputs passed validation.** The holdout check treated an output without a key as keyed with
  its input's, but the engine publishes it unkeyed, under another id: the code the draft wrote (the SDK's `Map`)
  agreed on the holdout and then published every listing without its key, which an audit would have counted as a
  disagreement. Agreement now compares keys as they are, and the brief asks for `MapKeyed` with the input's key, as
  the agent keys its output.

## Not covered

- The Improve view: the Playwright suite runs on its own fixture deployment (`ui/e2e/fixture`, seeded by
  `global.setup.ts`), which has no review history, distill fixture or sandbox; driving this scenario's development
  deployment from it would need a second harness. The view's proposals, diff and validation tables are covered by
  `ui/e2e/screens.spec.ts` on that fixture.
- The insight trigger: development runs `millstream improve --insight` by hand. Imported insights are listed, not
  recorded, so they fire no `insightOpened` event.
