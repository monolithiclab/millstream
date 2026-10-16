import type { MetricsQuery } from '../../api/api';
import type { MetricSeries } from '../../api/schema';

// The explorer's view, kept in the URL so a link reopens it: /metrics?m=delivery_latency_seconds&by=step
// &p=listings-daily&range=7d&stat=mean&hide=dedupe. Custom ranges carry from and to (RFC 3339).

export const ranges = ['1h', '24h', '7d', '30d', 'custom'] as const;
export type Range = (typeof ranges)[number];
export const stats = ['sum', 'mean', 'max', 'count'] as const;
export type Stat = (typeof stats)[number];

export interface View {
  metric: string;
  pipeline: string;
  // '' is one series per label set; 'step' or another label groups by it.
  by: string;
  range: Range;
  from: string;
  to: string;
  stat: Stat;
  marks: boolean;
  hidden: string[];
}

const pick = <T extends string>(list: readonly T[], value: string | null, fallback: T): T =>
  list.find((x) => x === value) ?? fallback;

// Counters add up per bucket; durations, costs and gauges read best as a mean.
export function defaultStat(metric: string): Stat {
  return /(_seconds|_usd|_depth)$/.test(metric) ? 'mean' : 'sum';
}

export function readView(params: URLSearchParams): View {
  const metric = params.get('m') ?? '';
  return {
    metric,
    pipeline: params.get('p') ?? '',
    by: params.get('by') ?? 'step',
    range: pick(ranges, params.get('range'), '24h'),
    from: params.get('from') ?? '',
    to: params.get('to') ?? '',
    stat: pick(stats, params.get('stat'), defaultStat(metric)),
    marks: params.get('marks') !== '0',
    hidden: (params.get('hide') ?? '').split(',').filter((x) => x !== ''),
  };
}

// Only what differs from the defaults goes into the URL.
export function writeView(view: View): URLSearchParams {
  const out = new URLSearchParams();
  if (view.metric) out.set('m', view.metric);
  if (view.pipeline) out.set('p', view.pipeline);
  if (view.by !== 'step') out.set('by', view.by);
  if (view.range !== '24h') out.set('range', view.range);
  if (view.range === 'custom') {
    if (view.from) out.set('from', view.from);
    if (view.to) out.set('to', view.to);
  }
  if (view.stat !== defaultStat(view.metric)) out.set('stat', view.stat);
  if (!view.marks) out.set('marks', '0');
  if (view.hidden.length > 0) out.set('hide', view.hidden.join(','));
  return out;
}

const MINUTE = 60_000;
const spans: Record<Exclude<Range, 'custom'>, number> = {
  '1h': 60 * MINUTE,
  '24h': 1440 * MINUTE,
  '7d': 7 * 1440 * MINUTE,
  '30d': 30 * 1440 * MINUTE,
};

// The window the view covers, ending now for the fixed ranges.
export function windowOf(view: View, now: number): { from: number; to: number } {
  if (view.range === 'custom') {
    const from = Date.parse(view.from);
    const to = view.to ? Date.parse(view.to) : now;
    return Number.isNaN(from) || Number.isNaN(to) || to <= from ? { from: now - spans['24h'], to: now } : { from, to };
  }
  return { from: now - spans[view.range], to: now };
}

export function queryOf(view: View, now: number): MetricsQuery {
  const q: MetricsQuery = { name: view.metric };
  if (view.pipeline) q.pipeline = view.pipeline;
  if (view.by) q.groupBy = view.by;
  if (view.range === 'custom') {
    const { from, to } = windowOf(view, now);
    q.from = new Date(from).toISOString();
    q.to = new Date(to).toISOString();
  } else {
    q.since = view.range;
  }
  return q;
}

// About 120 buckets over the window, on whole minutes: one per minute over an hour, per 12 minutes over a day.
export function bucketSize(from: number, to: number): number {
  return Math.max(MINUTE, Math.round((to - from) / 120 / MINUTE) * MINUTE);
}

export interface Bucketed {
  // Bucket starts, epoch ms.
  times: number[];
  series: { group: string; values: (number | null)[] }[];
}

// The server's per-minute points, rolled up into buckets with the chosen statistic; a bucket without points is null.
export function bucket(series: MetricSeries[], stat: Stat, from: number, to: number): Bucketed {
  const size = bucketSize(from, to);
  const start = Math.floor(from / size) * size;
  const count = Math.max(1, Math.ceil((to - start) / size));
  const times = Array.from({ length: count }, (_, i) => start + i * size);
  return {
    times,
    series: series.map((s) => {
      const acc = times.map(() => ({ n: 0, sum: 0, max: -Infinity, seen: false }));
      for (const p of s.points) {
        const i = Math.floor((Date.parse(p.t) - start) / size);
        const a = acc[i];
        if (a === undefined) continue;
        a.seen = true;
        a.n += p.count;
        a.sum += p.sum;
        a.max = Math.max(a.max, p.max);
      }
      const values = acc.map((a) => {
        if (!a.seen) return null;
        if (stat === 'sum') return a.sum;
        if (stat === 'count') return a.n;
        if (stat === 'max') return a.max;
        return a.n === 0 ? null : a.sum / a.n;
      });
      return { group: s.group, values };
    }),
  };
}

// The last, median and largest of a series' values.
export function summary(values: (number | null)[]): { last: number | null; median: number | null; max: number | null } {
  const present = values.filter((v): v is number => v !== null);
  if (present.length === 0) return { last: null, median: null, max: null };
  const sorted = [...present].sort((a, b) => a - b);
  const at = (i: number) => sorted[i] ?? 0;
  const mid = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 1 ? at(mid) : (at(mid - 1) + at(mid)) / 2;
  return { last: present[present.length - 1] ?? null, median, max: at(sorted.length - 1) };
}
