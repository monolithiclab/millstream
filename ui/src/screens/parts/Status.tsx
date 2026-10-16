import type { ReactNode } from 'react';

// The status glyphs of the canvas components board: every state has a shape as well as a colour and a word.
const shape = {
  circle: 'M6 1.5a4.5 4.5 0 1 1 0 9a4.5 4.5 0 1 1 0-9Z',
  half: 'M6 1.5a4.5 4.5 0 0 0 0 9Z',
  diamond: 'M6 1.2L10.8 6L6 10.8L1.2 6Z',
  tri: 'M6 1.3L11 10.4H1Z',
  square: 'M2 2H10V10H2Z',
  check: 'M1.8 6.4L4.8 9.3L10.2 2.9',
  slash: 'M2.9 9.1L9.1 2.9',
  pause: 'M3.8 2.5V9.5M8.2 2.5V9.5',
  x: 'M4.2 4.2L7.8 7.8M7.8 4.2L4.2 7.8',
};

export type Tone = 'accent' | 'mute' | 'human' | 'ok' | 'warn' | 'bad';

interface Glyph {
  tone: Tone;
  d: string;
  filled?: boolean;
  // A second mark drawn over the first.
  over?: { d: string; filled?: boolean; ground?: boolean };
}

export const glyphs = {
  run: { tone: 'accent', d: shape.circle, filled: true },
  pend: { tone: 'mute', d: shape.circle },
  human: { tone: 'human', d: shape.circle, over: { d: shape.half, filled: true } },
  ok: { tone: 'ok', d: shape.check },
  filt: { tone: 'mute', d: shape.diamond },
  retry: { tone: 'warn', d: shape.tri },
  warnFill: { tone: 'warn', d: shape.tri, filled: true },
  dead: { tone: 'bad', d: shape.square, filled: true, over: { d: shape.x, ground: true } },
  open: { tone: 'bad', d: shape.square, filled: true },
  stopped: { tone: 'mute', d: shape.circle, over: { d: shape.slash } },
  paused: { tone: 'mute', d: shape.pause },
  failing: { tone: 'bad', d: shape.tri, filled: true },
  unsched: { tone: 'bad', d: shape.square },
  ack: { tone: 'mute', d: shape.check },
  sample: { tone: 'human', d: shape.diamond },
} satisfies Record<string, Glyph>;

export type GlyphName = keyof typeof glyphs;

const ink: Record<Tone, string> = {
  accent: 'text-accent',
  mute: 'text-ink-3',
  human: 'text-human',
  ok: 'text-ok',
  warn: 'text-warn',
  bad: 'text-bad',
};

const soft: Record<Tone, string> = {
  accent: 'bg-accent-soft',
  mute: 'bg-mute-soft',
  human: 'bg-human-soft',
  ok: 'bg-ok-soft',
  warn: 'bg-warn-soft',
  bad: 'bg-bad-soft',
};

export function StatusGlyph({ name, size = 11 }: { name: GlyphName; size?: number }) {
  const g: Glyph = glyphs[name];
  return (
    <svg width={size} height={size} viewBox="0 0 12 12" aria-hidden="true" className="flex-none">
      <path
        d={g.d}
        fill={g.filled ? 'currentColor' : 'none'}
        stroke={g.filled ? 'none' : 'currentColor'}
        strokeWidth="1.7"
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      {g.over && (
        <path
          d={g.over.d}
          fill={g.over.filled ? 'currentColor' : 'none'}
          stroke={g.over.filled ? 'none' : g.over.ground ? 'var(--ms-surface)' : 'currentColor'}
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      )}
    </svg>
  );
}

// A pill: glyph and word on the state's soft ground.
export function StatusBadge({ name, children }: { name: GlyphName; children: ReactNode }) {
  const tone = glyphs[name].tone;
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-1 px-2 py-px text-tiny font-bold whitespace-nowrap ${ink[tone]} ${soft[tone]}`}
    >
      <StatusGlyph name={name} />
      {children}
    </span>
  );
}
