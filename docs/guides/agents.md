# Agents in production

How to run LLM agents in a Millstream pipeline so that their answers can be trusted, their cost bounded and their
behaviour improved. The [getting started tutorial](getting-started.md) builds an agent step; this guide says why each
safeguard exists and which key sets it. The contracts are in the
[platform design](../specs/20261014.platform-design.md), sections 7 (agent actions), 8 (review), 11
(self-improvement) and 15.4 (prompt injection).

The guide follows four practices that hold for any agent system: treat the model's output as untrusted input, keep
planning apart from execution, record what happens, and improve from what was recorded. It starts with the choice
that comes before them.

## 1. Choose the cheapest step that works

Every step of a pipeline runs one of four things. Pick the first that does the job:

| Step | Cost per item | Use it when |
| --- | --- | --- |
| A builtin (`filter`, `dedupe`, route objects) | Nothing: CEL in the engine | The decision is a condition over fields you already have |
| Code (an `exec` or `container` action) | A process | The rule is known, even if long |
| An agent (`kind: agent`) | Tokens, and seconds | The rule is not known, or the input is free text |
| A person (`review`) | Someone's time | The decision needs judgement, or carries a consequence you must authorise |

The choice is not final. An agent step can start where the rules are not known yet, and as reviews and deliveries
accumulate, the `improve` pipeline proposes moving it down this table (section 5 below).

## 2. Treat the output as untrusted

A model's answer is data from outside the system, and a crawled page or a document in its prompt can steer it.

- **The output schema.** Every agent action declares `output`, a JSON Schema. The adapter validates each answer
  before anything is published. An answer that does not match goes back to the model with the validator's errors,
  as a repair turn inside the same attempt: `runtime.agent.repairTurns` of them (1 by default, 0 for none), within
  `maxTurns` and `maxCostUsd`. One that still does not match is a retryable error, retried under the step's `retry`
  like a crash. The attempt keeps its transcript, every turn in it, and the `schema_validation_failures` metric
  counts it.
  Make the schema tight: enums over free strings, `minimum` and `maximum` on numbers, `required` on every field a
  later step reads.
- **Abstaining.** An agent that cannot answer should say so rather than guess. If the output schema admits
  `{"abstain": "<why>"}` (an `anyOf` alternative) and the prompt says when to use it, an abstention reaches the
  step's `fallback` instead of a downstream step.
- **Allowlists.** An agent gets only the `tools`, `mcp` servers and `network` destinations its manifest lists, and no
  secret beyond its config. In the sandbox its filesystem is read-only except its working directory. On a laptop
  without Docker, agents run unsandboxed only when the environment file sets `agents.allowUnsandboxed: true`.
- **Effects need a person.** Declare `effects: external` on any action that changes something outside Millstream:
  an email, a CRM write, a payment. `millstream check` warns when an agent's output can reach such a step with no
  `review` without `autoApprove` between them. A `filter` does not count, since its condition says nothing about
  whether the effect is authorised. The warning is advisory, so treat it as a design review.
- **Thresholds in filters.** Keep the model's answer a measurement (`score`, `tier`, `category`) and the decision a
  `filter` in the pipeline file. The threshold is then visible, reviewable and changed without touching the prompt.

## 3. Keep planning apart from execution

An agent never changes the pipeline it runs in. It answers one item, inside one step, under limits it cannot
raise:

- `maxTurns` (10 by default) and `maxCostUsd` per delivery; the adapter stops the agent at the limit and the error
  is not retried.
- `budget.runUsd` and `budget.dayUsd` per pipeline, enforced by reservation before a delivery is leased. A pipeline
  whose budget is reached pauses its agent steps until `millstream resume`.
- `concurrency` and `rate` per step.

Changes to the pipeline itself (a prompt, a lessons file, a threshold, a new filter, code replacing an agent) are
planned elsewhere: drafted as proposals in a development deployment, validated against past items, accepted by a
person and released like any other change (section 5). Runs are pinned to the content-hash revision they started on,
so a change never alters work in flight, and `millstream rollback` reactivates the previous revision.

## 4. Record everything

- **Transcripts.** Each attempt stores its full transcript (prompt, tool calls, results, final output) as a
  gzip-compressed artifact, with every secret value of the pipeline redacted, and the item view in the UI shows it.
  `retention.transcripts` sets how long transcripts are kept; `millstream purge` erases them with the item.
- **Metrics.** Every answer reports `tokens_in`, `tokens_out`, `cost_usd`, `turns` and `duration_ms`, labelled with
  the model. Cost per approved item and the intervention rate come from the same events. `/metrics` serves them to
  Prometheus.
- **Lineage.** Every item names its parents, so an output can be traced to the input, the revision and the attempt
  that produced it.
- **Recordings.** With `MILLSTREAM_HARNESS_MODE=record`, agent calls are recorded, keyed by the harness, the model,
  the rendered prompt, the output schema and any repair turns before it. In `replay` mode, tests and evals run
  offline and deterministically, without an API key. A changed prompt or lessons file misses its old recordings
  instead of replaying a stale answer.

## 5. Improve from what was recorded

- **Evals.** An agent action ships `evals/*.yaml`: an input, CEL assertions over the output, and optionally an LLM
  judge with a rubric. `millstream eval <action>` scores them, from recordings by default, so a prompt or model change
  is compared before it ships.
- **Reviews as evidence.** A `review` step with `reasons` and `editable` fields records why a person rejected or
  corrected an answer. Edits are attributed, through lineage, to the step that produced the value.
- **Insights.** Computed from the event log with no model call: slow steps, failure clusters, cost, review patterns,
  and agent steps predictable enough to replace (`distillable`).
- **Proposals.** In a development deployment, the `improve` pipeline turns insights into proposals:
  - `lessons`: rules and examples taken from reviewers' edits, rendered into the prompt;
  - `config`: retries, thresholds, an `autoApprove` rule;
  - `topology`: a filter moved upstream of the most expensive step it can precede;
  - `distill`: code replacing an agent.

  Each is backtested against past items. `millstream backtest` validates a hand-written patch the same way.

  `distillable` and `reviewPattern` insights start these drafts whatever their severity, usually `info`, since they say
  what could improve rather than how bad something is. `improve.minSeverity` governs the others: failures, slow steps.
- **Code that replaces an agent.** A `distill` proposal writes a code action from past answers and checks it on a
  holdout the drafting agent never saw. The agent stays as the step's `fallback` for the items the code abstains on.
  `fallback.auditRate` keeps sending a sample of covered items to the agent, and disagreements raise a `drift` insight.
  The `distill` patch sets it from `improve.distill.auditRate` (default 2%) when the agent declares `effects: none`, as
  audits require; declare it on your agent steps, or the patch says in a comment that nothing will check the code. In
  the platform's own acceptance scenario, on synthetic listings whose answers follow a rule, the code covered 260 of 270
  held-out items with full agreement, and the next day cost 5.2% of the agent's, audits included ([M5
  exit](../development/m5-exit.md)).

## Checklist

Before an agent step goes to production:

- [ ] The output schema is tight, and every field a later step reads is `required`.
- [ ] The schema admits `abstain`, the prompt says when, and the step has a `fallback`.
- [ ] `tools`, `mcp` and `network` list only what the agent needs; it runs in the sandbox.
- [ ] Every action with `effects: external` downstream of the agent has a `review` before it.
- [ ] Thresholds live in `filter` steps.
- [ ] `maxCostUsd` is set, and the pipeline has `budget.dayUsd`.
- [ ] The action has evals, with recordings committed, and they pass in `replay` mode.
- [ ] Reviews have `reasons`, and `editable` lists the fields people correct.
- [ ] `retention.transcripts` matches how long you need to explain an answer.
