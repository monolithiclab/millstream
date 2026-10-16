import { describe, expect, it } from 'vitest';
import type { Delivery } from '../../api/schema';
import { buildTimeline, density, position, ticks } from './timeline';

const t0 = Date.parse('2026-10-15T06:00:00Z');
const at = (minutes: number) => new Date(t0 + minutes * 60_000).toISOString();

function d(step: string, extra: Partial<Delivery>): Delivery {
  return {
    id: `dl_${step}_${Math.random()}`,
    pipeline: 'p',
    runId: 'r',
    step,
    itemId: 'it',
    generation: 0,
    state: 'done',
    attempt: 1,
    costUsd: 0,
    createdAt: at(0),
    waitMs: 0,
    workMs: 0,
    ...extra,
  };
}

describe('buildTimeline', () => {
  const deliveries = [
    d('fetch', { createdAt: at(0), leasedAt: at(0), finishedAt: at(10) }),
    d('score', { createdAt: at(10), leasedAt: at(20), finishedAt: at(30) }),
    d('score', { createdAt: at(12), state: 'dead', leasedAt: at(30), finishedAt: at(32) }),
    d('review', { createdAt: at(30), state: 'awaitingReview', leasedAt: at(30) }),
    d('render', { createdAt: at(35), state: 'pending' }),
  ];

  it('lays each step out as working, waiting and waiting-on-people spans, with drains and dead letters', () => {
    const now = t0 + 40 * 60_000;
    const eta = t0 + 60 * 60_000;
    const tl = buildTimeline(
      ['fetch', 'score', 'review', 'render', 'idle'],
      new Set(['review']),
      deliveries,
      t0,
      null,
      now,
      eta,
    );
    expect(tl).toMatchObject({ from: t0, to: eta, end: now });
    const [fetch, score, review, render, idle] = tl.rows;
    expect(fetch!.segments.map((s) => s.kind)).toEqual(['work']);
    expect(fetch!.drained).toBe(t0 + 10 * 60_000);
    expect(fetch!.shares.work).toBe(1);
    expect(score!.segments.map((s) => s.kind)).toEqual(['wait', 'work']);
    expect(score!.dead).toEqual([t0 + 32 * 60_000]);
    expect(review!.segments.map((s) => s.kind)).toEqual(['people', 'projected']);
    expect(review!.drained).toBeNull();
    expect(render!.segments.map((s) => s.kind)).toEqual(['wait', 'projected']);
    expect(idle).toMatchObject({ start: null, drained: null, segments: [], shares: { work: 0, people: 0, wait: 0 } });
  });

  it('ends at the settlement and projects nothing once settled', () => {
    const settled = t0 + 33 * 60_000;
    const tl = buildTimeline(['score'], new Set(), deliveries.slice(1, 3), t0, settled, settled + 1e6, settled + 1e7);
    expect(tl.to).toBe(settled);
    expect(tl.rows[0]!.segments.some((s) => s.kind === 'projected')).toBe(false);
  });

  it('gives a fresh run a minute of width', () => {
    expect(buildTimeline([], new Set(), [], t0, null, t0, null).to).toBe(t0 + 60_000);
  });
});

describe('ticks, density and position', () => {
  it('picks round intervals', () => {
    expect(ticks(t0, t0 + 60 * 60_000).map((t) => (t - t0) / 60_000)).toEqual([0, 10, 20, 30, 40, 50, 60]);
    expect(ticks(t0, t0 + 3 * 60_000)).toHaveLength(4);
    expect(ticks(t0, t0 + 30 * 86_400_000).length).toBeLessThanOrEqual(31);
  });

  it('bins deliveries by when they finished, else were leased or created', () => {
    const bins = density(
      [
        d('s', { finishedAt: at(1) }),
        d('s', { leasedAt: at(59) }),
        d('s', { createdAt: at(30) }),
        d('s', { createdAt: at(90) }),
      ],
      t0,
      t0 + 60 * 60_000,
      6,
    );
    expect(bins.bins.map((b) => b.length)).toEqual([1, 0, 0, 1, 0, 2]);
    expect(bins.width).toBe(10 * 60_000);
  });

  it('places times in percent', () => {
    expect(position(t0 + 30, t0, t0 + 60)).toBe(50);
    expect(position(t0, t0, t0)).toBe(0);
  });
});
