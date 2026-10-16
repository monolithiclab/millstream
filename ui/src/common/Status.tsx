import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';

// The canvas status set (Components board): every state is a shape, a word and a colour, never colour alone.
export const SH = {
  circle: 'M6 1.5a4.5 4.5 0 1 1 0 9a4.5 4.5 0 1 1 0-9Z',
  half: 'M6 1.5a4.5 4.5 0 0 0 0 9Z',
  diamond: 'M6 1.2L10.8 6L6 10.8L1.2 6Z',
  tri: 'M6 1.3L11 10.4H1Z',
  square: 'M2 2H10V10H2Z',
  check: 'M1.8 6.4L4.8 9.3L10.2 2.9',
  slash: 'M2.9 9.1L9.1 2.9',
  pause: 'M3.8 2.5V9.5M8.2 2.5V9.5',
  x: 'M4.2 4.2L7.8 7.8M7.8 4.2L4.2 7.8',
  replay: 'M2.3 6a3.7 3.7 0 1 0 1.1-2.6M2.2 1.6V4.3H4.9',
};

type Tone = 'accent' | 'ok' | 'warn' | 'bad' | 'human' | 'mute';

// Literal class names, so Tailwind sees each one.
const toneText: Record<Tone, string> = {
  accent: 'text-accent',
  ok: 'text-ok',
  warn: 'text-warn',
  bad: 'text-bad',
  human: 'text-human',
  mute: 'text-mute',
};
const toneBadge: Record<Tone, string> = {
  accent: 'bg-accent-soft text-accent',
  ok: 'bg-ok-soft text-ok',
  warn: 'bg-warn-soft text-warn',
  bad: 'bg-bad-soft text-bad',
  human: 'bg-human-soft text-human',
  mute: 'bg-mute-soft text-mute',
};

interface Glyph {
  tone: Tone;
  // The main shape, filled or stroked in the tone; an optional second mark over it.
  d: string;
  filled?: boolean;
  mark?: string;
  // The mark is filled (the half disc), or stroked in the surface colour over a filled shape (the cross).
  markFill?: boolean;
}

export const statuses = {
  running: { tone: 'accent', d: SH.circle, filled: true },
  leased: { tone: 'accent', d: SH.circle, filled: true },
  pending: { tone: 'mute', d: SH.circle },
  awaitingReview: { tone: 'human', d: SH.circle, mark: SH.half, markFill: true },
  waitingReview: { tone: 'human', d: SH.circle, mark: SH.half, markFill: true },
  done: { tone: 'ok', d: SH.check },
  succeeded: { tone: 'ok', d: SH.check },
  active: { tone: 'ok', d: SH.check },
  passed: { tone: 'ok', d: SH.diamond, filled: true },
  filtered: { tone: 'mute', d: SH.diamond },
  failed: { tone: 'warn', d: SH.tri },
  completedWithErrors: { tone: 'warn', d: SH.tri, filled: true },
  dead: { tone: 'bad', d: SH.square, filled: true, mark: SH.x },
  unrouted: { tone: 'warn', d: SH.square },
  open: { tone: 'bad', d: SH.square, filled: true },
  cancelled: { tone: 'mute', d: SH.circle, mark: SH.slash },
  discarded: { tone: 'mute', d: SH.circle, mark: SH.slash },
  replayed: { tone: 'accent', d: SH.replay },
  paused: { tone: 'mute', d: SH.pause },
  disabled: { tone: 'mute', d: SH.square, filled: true },
  failing: { tone: 'bad', d: SH.tri, filled: true },
  slow: { tone: 'warn', d: SH.tri },
  unschedulable: { tone: 'bad', d: SH.square },
  acknowledged: { tone: 'mute', d: SH.check },
  resolved: { tone: 'ok', d: SH.check },
  muted: { tone: 'mute', d: SH.circle, mark: SH.slash },
  sample: { tone: 'human', d: SH.diamond },
  // A node: its state, and its health from its heartbeats.
  draining: { tone: 'warn', d: SH.circle, mark: SH.half, markFill: true },
  revoked: { tone: 'bad', d: SH.circle, mark: SH.slash },
  healthy: { tone: 'ok', d: SH.check },
  late: { tone: 'warn', d: SH.tri },
  lost: { tone: 'bad', d: SH.tri, filled: true },
} satisfies Record<string, Glyph>;

export type StatusKey = keyof typeof statuses;

// Written out so the i18n test sees each key as a quoted literal.
const labels: Record<StatusKey, `status.${StatusKey}`> = {
  running: 'status.running',
  leased: 'status.leased',
  pending: 'status.pending',
  awaitingReview: 'status.awaitingReview',
  waitingReview: 'status.waitingReview',
  done: 'status.done',
  succeeded: 'status.succeeded',
  active: 'status.active',
  passed: 'status.passed',
  filtered: 'status.filtered',
  failed: 'status.failed',
  completedWithErrors: 'status.completedWithErrors',
  dead: 'status.dead',
  unrouted: 'status.unrouted',
  open: 'status.open',
  cancelled: 'status.cancelled',
  discarded: 'status.discarded',
  replayed: 'status.replayed',
  paused: 'status.paused',
  disabled: 'status.disabled',
  failing: 'status.failing',
  slow: 'status.slow',
  unschedulable: 'status.unschedulable',
  acknowledged: 'status.acknowledged',
  resolved: 'status.resolved',
  muted: 'status.muted',
  sample: 'status.sample',
  draining: 'status.draining',
  revoked: 'status.revoked',
  healthy: 'status.healthy',
  late: 'status.late',
  lost: 'status.lost',
};

export function isStatus(value: string): value is StatusKey {
  return Object.hasOwn(statuses, value);
}

export function useStatusLabel() {
  const { t } = useTranslation();
  return (status: StatusKey) => t(labels[status]);
}

// The state's shape in its tone; tone={false} draws it in the surrounding colour (a dark toast).
export function StatusGlyph({ status, size = 12, tone = true }: { status: StatusKey; size?: number; tone?: boolean }) {
  const g: Glyph = statuses[status];
  const stroked = g.d === SH.check || g.d === SH.pause || g.d === SH.replay;
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 12 12"
      aria-hidden="true"
      className={`flex-none ${tone ? toneText[g.tone] : ''}`}
    >
      <path
        d={g.d}
        fill={g.filled ? 'currentColor' : 'none'}
        stroke={g.filled ? 'none' : 'currentColor'}
        strokeWidth={stroked ? 1.8 : 1.6}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {g.mark !== undefined && (
        <path
          d={g.mark}
          fill={g.markFill ? 'currentColor' : 'none'}
          stroke={g.markFill ? 'none' : g.filled ? 'var(--ms-surface)' : 'currentColor'}
          strokeWidth="1.4"
          strokeLinecap="round"
        />
      )}
    </svg>
  );
}

// A pill: shape, word and colour. label replaces the state's own word ("Awaiting review at analyst-review").
export function StatusBadge({ status, label }: { status: StatusKey; label?: ReactNode }) {
  const statusLabel = useStatusLabel();
  return (
    <span
      className={`inline-flex max-w-full items-center gap-1.5 rounded-pill py-0.5 pr-2.5 pl-2 text-tiny font-bold ${toneBadge[statuses[status].tone]}`}
    >
      <StatusGlyph status={status} />
      <span className="min-w-0">{label ?? statusLabel(status)}</span>
    </span>
  );
}

// A glyph and the word, without the pill: table cells and dense rows.
export function StatusText({ status, label }: { status: StatusKey; label?: string }) {
  const statusLabel = useStatusLabel();
  return (
    <span className={`inline-flex items-center gap-1.5 font-bold ${toneText[statuses[status].tone]}`}>
      <StatusGlyph status={status} />
      {label ?? statusLabel(status)}
    </span>
  );
}
