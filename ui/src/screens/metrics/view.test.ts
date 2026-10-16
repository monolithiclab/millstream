import { describe, expect, it } from 'vitest';
import { bucket, bucketSize, defaultStat, queryOf, readView, summary, windowOf, writeView } from './view';

const now = Date.parse('2026-10-15T06:31:00Z');

describe('the view in the URL', () => {
  it('reads defaults and round-trips what differs from them', () => {
    const empty = readView(new URLSearchParams());
    expect(empty).toEqual({
      metric: '',
      pipeline: '',
      by: 'step',
      range: '24h',
      from: '',
      to: '',
      stat: 'sum',
      marks: true,
      hidden: [],
    });
    expect(writeView(empty).toString()).toBe('');
    const url =
      'm=delivery_latency_seconds&p=listings-daily&by=reason&range=custom&from=a&to=b&stat=max&marks=0&hide=x%2Cy';
    const view = readView(new URLSearchParams(url));
    expect(view).toMatchObject({ by: 'reason', range: 'custom', stat: 'max', marks: false, hidden: ['x', 'y'] });
    expect(writeView(view).toString()).toBe(url);
    expect(readView(new URLSearchParams('range=bogus&stat=bogus&m=items_in'))).toMatchObject({
      range: '24h',
      stat: 'sum',
    });
    expect(writeView({ ...empty, by: '' }).toString()).toBe('by=');
  });

  it('defaults durations, costs and gauges to the mean', () => {
    expect(['delivery_latency_seconds', 'delivery_cost_usd', 'queue_depth', 'items_in'].map(defaultStat)).toEqual([
      'mean',
      'mean',
      'mean',
      'sum',
    ]);
  });

  it('turns the view into a window and a query', () => {
    const view = readView(new URLSearchParams('m=items_in&p=p&range=7d'));
    expect(windowOf(view, now)).toEqual({ from: now - 7 * 86_400_000, to: now });
    expect(queryOf(view, now)).toEqual({ name: 'items_in', pipeline: 'p', groupBy: 'step', since: '7d' });
    const custom = readView(
      new URLSearchParams('m=m&by=&range=custom&from=2026-10-14T00:00:00Z&to=2026-10-14T06:00:00Z'),
    );
    expect(queryOf(custom, now)).toEqual({
      name: 'm',
      from: '2026-10-14T00:00:00.000Z',
      to: '2026-10-14T06:00:00.000Z',
    });
    const open = readView(new URLSearchParams('range=custom&from=2026-10-15T00:00:00Z'));
    expect(windowOf(open, now).to).toBe(now);
    const bad = readView(new URLSearchParams('range=custom&from=nope'));
    expect(windowOf(bad, now)).toEqual({ from: now - 86_400_000, to: now });
  });
});

describe('bucketing', () => {
  it('sizes about 120 buckets on whole minutes', () => {
    expect(bucketSize(0, 3_600_000)).toBe(60_000);
    expect(bucketSize(0, 86_400_000)).toBe(720_000);
  });

  it('rolls minute points up with each statistic and leaves empty buckets null', () => {
    const from = Date.parse('2026-10-15T00:00:00Z');
    const to = from + 3 * 3_600_000;
    const pt = (minute: number, count: number, sum: number, max: number) => ({
      t: new Date(from + minute * 60_000).toISOString(),
      count,
      sum,
      min: 0,
      max,
    });
    const series = [
      { name: 'm', group: 'a', points: [pt(0, 2, 4, 3), pt(1, 2, 8, 6), pt(200, 1, 1, 1), pt(-5, 1, 1, 1)] },
    ];
    const size = bucketSize(from, to);
    expect(size).toBe(120_000);
    const sum = bucket(series, 'sum', from, to);
    expect(sum.times).toHaveLength(90);
    expect(sum.series[0]!.values[0]).toBe(12);
    expect(sum.series[0]!.values[1]).toBeNull();
    expect(bucket(series, 'mean', from, to).series[0]!.values[0]).toBe(3);
    expect(bucket(series, 'max', from, to).series[0]!.values[0]).toBe(6);
    expect(bucket(series, 'count', from, to).series[0]!.values[0]).toBe(4);
    const zero = [{ name: 'm', group: 'z', points: [pt(0, 0, 0, 0)] }];
    expect(bucket(zero, 'mean', from, to).series[0]!.values[0]).toBeNull();
  });

  it('summarises a series', () => {
    expect(summary([null, 3, 1, null, 2])).toEqual({ last: 2, median: 2, max: 3 });
    expect(summary([4, 1, 3, 2])).toEqual({ last: 2, median: 2.5, max: 4 });
    expect(summary([null])).toEqual({ last: null, median: null, max: null });
  });
});
