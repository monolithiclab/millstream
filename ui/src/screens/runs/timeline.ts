import type { Delivery } from '../../api/schema';

// The run timeline's layout, from the run's deliveries: per step, when it started and drained, and over time
// whether it was working (a delivery leased), waiting on people (a review parked), waiting for input (deliveries
// queued, none leased), or is projected to run until the ETA.

export type SegmentKind = 'work' | 'wait' | 'people' | 'projected';

export interface Segment {
  kind: SegmentKind;
  from: number;
  to: number;
}

export interface StepRow {
  step: string;
  deliveries: Delivery[];
  start: number | null;
  // When the step drained: its last delivery finished and none is left.
  drained: number | null;
  segments: Segment[];
  dead: number[];
  // Shares of the step's span spent working, waiting on people and waiting for input.
  shares: Record<'work' | 'people' | 'wait', number>;
}

export interface Timeline {
  from: number;
  to: number;
  // The end of what happened: the settlement, else now.
  end: number;
  rows: StepRow[];
}

const BINS = 240;
const terminal = new Set<Delivery['state']>(['done', 'filtered', 'dead', 'cancelled']);
const ms = (iso: string | undefined) => (iso === undefined ? null : Date.parse(iso));

export function buildTimeline(
  steps: string[],
  reviewSteps: Set<string>,
  deliveries: Delivery[],
  started: number,
  settled: number | null,
  now: number,
  eta: number | null,
): Timeline {
  const end = Math.max(settled ?? now, started + 60_000);
  const to = settled === null && eta !== null && eta > end ? eta : end;
  const width = (end - started) / BINS;
  const byStep = new Map<string, Delivery[]>();
  for (const d of deliveries) {
    const list = byStep.get(d.step);
    if (list) list.push(d);
    else byStep.set(d.step, [d]);
  }

  const rows = steps.map((step): StepRow => {
    const list = byStep.get(step) ?? [];
    const people = reviewSteps.has(step);
    // Each delivery's spans: queued (created to leased) and then working or parked (leased to finished).
    const spans = list.map((d) => {
      const created = ms(d.createdAt) ?? started;
      const leased = ms(d.leasedAt);
      const finished = ms(d.finishedAt) ?? (terminal.has(d.state) ? (leased ?? created) : end);
      const busy: SegmentKind = people || d.state === 'awaitingReview' ? 'people' : 'work';
      return { created, leased, finished, busy };
    });
    const kinds: (SegmentKind | null)[] = [];
    for (let i = 0; i < BINS; i++) {
      const b0 = started + i * width;
      const b1 = b0 + width;
      let kind: SegmentKind | null = null;
      for (const s of spans) {
        if (s.leased !== null && s.leased < b1 && s.finished > b0) {
          if (s.busy === 'work') {
            kind = 'work';
            break;
          }
          kind = 'people';
        } else if (kind === null && s.created < b1 && (s.leased ?? s.finished) > b0) {
          kind = 'wait';
        }
      }
      kinds.push(kind);
    }
    const segments: Segment[] = [];
    kinds.forEach((kind, i) => {
      if (kind === null) return;
      const from = started + i * width;
      const last = segments.at(-1);
      if (last && last.kind === kind && Math.abs(last.to - from) < 1) last.to = from + width;
      else segments.push({ kind, from, to: from + width });
    });
    const open = list.some((d) => !terminal.has(d.state));
    const finishedTimes = spans.map((s) => s.finished);
    const drained = list.length > 0 && !open ? Math.max(...finishedTimes) : null;
    if (settled === null && open && eta !== null && eta > end) segments.push({ kind: 'projected', from: end, to: eta });
    const counted = kinds.filter((k) => k !== null);
    const share = (k: SegmentKind) =>
      counted.length === 0 ? 0 : counted.filter((x) => x === k).length / counted.length;
    return {
      step,
      deliveries: list,
      start: list.length === 0 ? null : Math.min(...spans.map((s) => s.created)),
      drained,
      segments,
      dead: list.filter((d) => d.state === 'dead').map((d) => ms(d.finishedAt) ?? end),
      shares: { work: share('work'), people: share('people'), wait: share('wait') },
    };
  });
  return { from: started, to, end, rows };
}

// Tick marks at a round interval, at most `max` of them.
export function ticks(from: number, to: number, max = 7): number[] {
  const minutes = [1, 2, 5, 10, 15, 30, 60, 120, 240, 360, 720, 1440];
  const span = to - from;
  const step = (minutes.find((m) => span / (m * 60_000) <= max - 1) ?? 1440) * 60_000;
  const out: number[] = [];
  for (let t = Math.ceil(from / step) * step; t <= to; t += step) out.push(t);
  return out;
}

// The step's deliveries counted into `bins` bins of the run by when each finished (else was leased, else created):
// the density strip, with each bin's width.
export function density(
  deliveries: Delivery[],
  from: number,
  end: number,
  bins = 48,
): { bins: Delivery[][]; width: number } {
  const out: Delivery[][] = Array.from({ length: bins }, () => []);
  const width = Math.max(1, (end - from) / bins);
  for (const d of deliveries) {
    const t = ms(d.finishedAt) ?? ms(d.leasedAt) ?? ms(d.createdAt) ?? from;
    const i = Math.min(bins - 1, Math.max(0, Math.floor((t - from) / width)));
    out[i]?.push(d);
  }
  return { bins: out, width };
}

// Where a time sits on the timeline, in percent.
export const position = (t: number, from: number, to: number) => ((t - from) / Math.max(1, to - from)) * 100;
