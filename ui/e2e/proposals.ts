import { readFileSync } from 'node:fs';

// The proposals global.setup.ts creates, for the Improve view's screens and flow (screens.spec.ts).

// A config proposal retitling the fixture's pipeline, as git diff writes it: a description changes no step, so
// its backtest samples nothing and it comes back ready.
export function descriptionPatch(description: string): string {
  const lines = readFileSync('e2e/fixture/pipeline.yaml', 'utf8').split('\n');
  const at = lines.findIndex((l) => l.startsWith('description:'));
  return [
    'diff --git a/pipeline.yaml b/pipeline.yaml',
    'index 1111111..2222222 100644',
    '--- a/pipeline.yaml',
    '+++ b/pipeline.yaml',
    `@@ -${at},3 +${at},3 @@`,
    ` ${lines[at - 1]}`,
    `-${lines[at]}`,
    `+description: ${description}`,
    ` ${lines[at + 1]}`,
    '',
  ].join('\n');
}

// The flow rejects the first and accepts the second.
export const toReject = {
  fingerprint: 'e2e-reject',
  description: 'Listings shortlisted, reviewed and triaged',
  rationale: 'Retitle the pipeline for the triage inbox.',
};
export const toAccept = {
  fingerprint: 'e2e-accept',
  description: 'Listings shortlisted for the analysts',
  rationale: 'Name the analysts in the pipeline’s description.',
};
export const proposals = [toReject, toAccept];
