import { readFileSync } from 'node:fs';

// The proposals global.setup.ts creates, for the Improve view's screens and flow (screens.spec.ts).

// A config proposal giving the triage inbox another reject reason, as git diff writes it: a review's reasons are
// what a config proposal may change, and no output moves, so its backtest holds and it comes back ready.
export function reasonsPatch(reasons: string): string {
  const lines = readFileSync('e2e/fixture/pipeline.yaml', 'utf8').split('\n');
  const inbox = lines.findIndex((l) => l.trim() === 'inbox: triage');
  const at = lines.findIndex((l, i) => i > inbox && l.trim().startsWith('reasons:'));
  const line = lines[at] ?? '';
  const indent = line.slice(0, line.indexOf('reasons:'));
  return [
    'diff --git a/pipeline.yaml b/pipeline.yaml',
    'index 1111111..2222222 100644',
    '--- a/pipeline.yaml',
    '+++ b/pipeline.yaml',
    `@@ -${at},3 +${at},3 @@`,
    ` ${lines[at - 1]}`,
    `-${line}`,
    `+${indent}reasons: ${reasons}`,
    ` ${lines[at + 1]}`,
    '',
  ].join('\n');
}

// The flow rejects the first and accepts the second.
export const toReject = {
  fingerprint: 'e2e-reject',
  reasons: '[overpriced, stale, duplicate, wrong-area]',
  rationale: 'Let triage reject a listing in the wrong area.',
};
export const toAccept = {
  fingerprint: 'e2e-accept',
  reasons: '[overpriced, stale, duplicate, below-margin]',
  rationale: 'Let triage reject a listing below the margin.',
};
export const proposals = [toReject, toAccept];
