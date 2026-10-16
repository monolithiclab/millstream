import type {
  Delivery,
  Insight,
  Item,
  MetricSeries,
  Review,
  Revision,
  Run,
  RunReport,
  Sample,
  StepLessons,
} from './schema';

// Demo data for the inbox, run timeline, metrics, insights, lessons and logs: the canvas's listings example on
// 15 October 2026 at 06:31, while the 06:00 run is under way. main.tsx shifts it to the present for ?demo.
export const DEMO_NOW = '2026-10-15T06:31:00Z';
const day = '2026-10-15';
const at = (hhmm: string) => `${day}T${hhmm}:00Z`;
const minutesBefore = (minutes: number) => new Date(Date.parse(DEMO_NOW) - minutes * 60_000).toISOString();

const PIPELINE = 'listings-daily';

function listingItem(id: string, key: string, payload: Record<string, unknown>, ts: string): Item {
  return {
    id,
    pipeline: PIPELINE,
    runId: 'run_0600',
    stream: 'scored',
    step: 'score',
    key,
    parents: [],
    generation: 1,
    ts,
    payload,
    artifacts: [],
  };
}

interface Listing {
  delivery: string;
  item: string;
  title: string;
  where: string;
  price: number;
  surface: number;
  rooms: number;
  score: number;
  margin: number;
  waited: number;
}

const listings: Listing[] = [
  {
    delivery: 'dl_4b2e',
    item: 'it_7f3a9c21',
    title: 'T3 · 68 m² · Paris 19e',
    where: 'Rue de Crimée, 75019 Paris · 4th floor, no lift',
    price: 412000,
    surface: 68,
    rooms: 3,
    score: 82,
    margin: 17.8,
    waited: 14,
  },
  {
    delivery: 'dl_51a0',
    item: 'it_2b90d7e4',
    title: 'T2 · 44 m² · Pantin',
    where: 'Avenue Jean Lolive, 93500 Pantin · 2nd floor',
    price: 268000,
    surface: 44,
    rooms: 2,
    score: 77,
    margin: 16.2,
    waited: 13,
  },
  {
    delivery: 'dl_7c1d',
    item: 'it_5e7f0a12',
    title: 'T4 · 91 m² · Montreuil',
    where: 'Rue de Paris, 93100 Montreuil · ground floor',
    price: 489000,
    surface: 91,
    rooms: 4,
    score: 74,
    margin: 15.4,
    waited: 11,
  },
  {
    delivery: 'dl_88f2',
    item: 'it_88a1c3f0',
    title: 'Studio · 19 m² · Paris 20e',
    where: 'Rue des Pyrénées, 75020 Paris · 6th floor',
    price: 159000,
    surface: 19,
    rooms: 1,
    score: 88,
    margin: 21,
    waited: 9,
  },
  {
    delivery: 'dl_9a03',
    item: 'it_9d02e6b3',
    title: 'T3 · 70 m² · Saint-Denis',
    where: 'Rue Gabriel Péri, 93200 Saint-Denis · 3rd floor',
    price: 251000,
    surface: 70,
    rooms: 3,
    score: 71,
    margin: 19.6,
    waited: 8,
  },
  {
    delivery: 'dl_a61b',
    item: 'it_c41e0b88',
    title: 'T3 · 62 m² · Bagnolet',
    where: 'Rue Sadi Carnot, 93170 Bagnolet · 1st floor',
    price: 338000,
    surface: 62,
    rooms: 3,
    score: 73,
    margin: 16.8,
    waited: 4,
  },
  {
    delivery: 'dl_b7d4',
    item: 'it_4c11ab90',
    title: 'T2 · 47 m² · Ivry-sur-Seine',
    where: 'Rue Marat, 94200 Ivry-sur-Seine · 5th floor, lift',
    price: 279000,
    surface: 47,
    rooms: 2,
    score: 76,
    margin: 17.3,
    waited: 2,
  },
  {
    delivery: 'dl_c2e9',
    item: 'it_70e2d5c3',
    title: 'Maison · 104 m² · Aubervilliers',
    where: 'Rue du Moutier, 93300 Aubervilliers · garden',
    price: 455000,
    surface: 104,
    rooms: 5,
    score: 70,
    margin: 15.9,
    waited: 1,
  },
];

function payloadOf(l: Listing): Record<string, unknown> {
  return {
    title: l.title,
    priceEur: l.price,
    surfaceM2: l.surface,
    rooms: l.rooms,
    score: l.score,
    marginPct: l.margin,
    rentEstimateEur: Math.round((l.price * 0.048) / 12),
    notes: null,
    url: `https://listings.example/annonces/${l.item.slice(3)}`,
  };
}

export const demoReviews: Review[] = listings.map((l) => ({
  pipeline: PIPELINE,
  deliveryId: l.delivery,
  runId: 'run_0600',
  step: 'analyst-review',
  inbox: 'analysts',
  state: 'open',
  version: 1,
  parkedAt: minutesBefore(l.waited),
  deadline: minutesBefore(l.waited - 72 * 60),
  editable: ['/marginPct', '/notes'],
  reasons: ['overpriced', 'wrong-data', 'duplicate', 'out-of-criteria', 'other'],
  display: `${l.title}\n${l.where}`,
  item: listingItem(l.item, l.item.slice(3), payloadOf(l), minutesBefore(l.waited + 1)),
}));

// Deciding this review answers 409, as when another reviewer got there first.
export const demoConflicts: Record<string, string> = {
  dl_7c1d: 'review of delivery dl_7c1d was already decided by claire at 06:29',
};

const sampled: Listing = {
  delivery: 'dl_d3f5',
  item: 'it_b8f6031e',
  title: 'T2 · 39 m² · Paris 18e',
  where: 'Rue Ordener, 75018 Paris · auto-approved by the rule',
  price: 322000,
  surface: 39,
  rooms: 2,
  score: 79,
  margin: 15.1,
  waited: 6,
};

export const demoSamples: Sample[] = [
  {
    pipeline: PIPELINE,
    deliveryId: sampled.delivery,
    runId: 'run_0600',
    step: 'analyst-review',
    inbox: 'analysts',
    state: 'open',
    display: `${sampled.title}\n${sampled.where}`,
    createdAt: minutesBefore(sampled.waited),
    item: listingItem(sampled.item, sampled.item.slice(3), payloadOf(sampled), minutesBefore(7)),
  },
];

const run = (id: string, start: string, extra: Partial<Run>): Run => ({
  id,
  pipeline: PIPELINE,
  revision: 14,
  kind: 'trigger',
  ...(extra.kind === 'replay' ? {} : { trigger: 'daily' }),
  triggerKey: start,
  triggerSource: 'cron',
  scheduledAt: start,
  generation: 0,
  state: 'succeeded',
  startedAt: start,
  costUsd: 0,
  vars: { city: 'paris' },
  varSources: { city: 'pipeline' },
  ...extra,
});

export const demoRuns: Run[] = [
  run('run_0600_1014', '2026-10-14T06:00:00Z', { settledAt: '2026-10-14T06:52:00Z', costUsd: 13.9 }),
  run('run_0600_1013', '2026-10-13T06:00:00Z', {
    state: 'completedWithErrors',
    revision: 13,
    settledAt: '2026-10-13T07:01:00Z',
    costUsd: 14.4,
  }),
  run('run_1430_1012', '2026-10-12T14:30:00Z', {
    kind: 'replay',
    triggerSource: 'manual',
    revision: 13,
    state: 'cancelled',
    settledAt: '2026-10-12T14:36:00Z',
    costUsd: 0.42,
  }),
];

// Deliveries of the 06:00 run, step by step: [step, how many, first created, minutes between, work minutes, wait
// minutes]. Some steps are still draining at 06:31; score has two dead letters; the review step waits on people.
type Plan = [step: string, count: number, from: number, every: number, work: number, wait: number];
const plans: Plan[] = [
  ['fetch', 6, 0, 1.5, 1.4, 0],
  ['parse', 6, 1.5, 1.5, 0.6, 0.2],
  ['dedupe', 6, 2.5, 1.5, 0.1, 0.4],
  ['geocode', 6, 3, 1.6, 1.2, 0.8],
  ['enrich', 6, 5, 1.8, 2.6, 1.5],
  ['score', 10, 6, 2.6, 2.2, 3],
  ['analyst-review', 9, 16, 1.4, 0, 0],
  ['render', 2, 20, 4, 1.5, 0.5],
];

function delivery(step: string, n: number, created: number, extra: Partial<Delivery>): Delivery {
  const createdAt = new Date(Date.parse(at('06:00')) + created * 60_000).toISOString();
  return {
    id: `dl_${step}_${n}`,
    pipeline: PIPELINE,
    runId: 'run_0600',
    step,
    itemId: `it_${step}_${n}`,
    generation: 0,
    state: 'done',
    attempt: 1,
    costUsd: step === 'score' ? 0.019 : 0,
    createdAt,
    waitMs: 0,
    workMs: 0,
    ...extra,
  };
}

const minute = (m: number) => new Date(Date.parse(at('06:00')) + m * 60_000).toISOString();
const nowMinute = 31;

export const demoDeliveries: Delivery[] = plans.flatMap(([step, count, from, every, work, wait]) =>
  Array.from({ length: count }, (_, n) => {
    const created = from + n * every;
    if (step === 'analyst-review') {
      return delivery(step, n, created, { state: 'awaitingReview', leasedAt: minute(created) });
    }
    const leased = created + wait;
    const finished = leased + work;
    if (leased > nowMinute) return delivery(step, n, created, { state: 'pending', waitMs: 0 });
    if (finished > nowMinute) {
      return delivery(step, n, created, { state: 'leased', leasedAt: minute(leased), waitMs: wait * 60_000 });
    }
    const dead = step === 'score' && (n === 2 || n === 6);
    return delivery(step, n, created, {
      state: dead ? 'dead' : 'done',
      attempt: dead ? 3 : 1,
      ...(dead ? { errorClass: 'schema', errorMessage: 'output schema: "score" missing' } : {}),
      leasedAt: minute(leased),
      finishedAt: minute(finished),
      waitMs: Math.round(wait * 60_000),
      workMs: Math.round(work * 60_000),
    });
  }),
);

export const demoReports: Record<string, Partial<RunReport>> = {
  run_0600: {
    durationSeconds: 31 * 60,
    costUsd: 9.84,
    items: { raw: 1632, scored: 402 },
    criticalPath: {
      totalSeconds: 31 * 60,
      steps: [
        {
          step: 'fetch',
          delivery: 'dl_fetch_5',
          latencySeconds: 540,
          waitSeconds: 0,
          workSeconds: 540,
          backoffSeconds: 0,
          share: 0.29,
        },
        {
          step: 'enrich',
          delivery: 'dl_enrich_5',
          latencySeconds: 300,
          waitSeconds: 120,
          workSeconds: 180,
          backoffSeconds: 0,
          share: 0.16,
        },
        {
          step: 'score',
          delivery: 'dl_score_9',
          latencySeconds: 660,
          waitSeconds: 300,
          workSeconds: 300,
          backoffSeconds: 60,
          share: 0.35,
        },
        {
          step: 'analyst-review',
          delivery: 'dl_analyst-review_0',
          latencySeconds: 360,
          waitSeconds: 360,
          workSeconds: 0,
          backoffSeconds: 0,
          share: 0.2,
        },
      ],
    },
    steps: [
      {
        step: 'score',
        deliveries: 10,
        failedAttempts: 4,
        p50Seconds: 130,
        p95Seconds: 290,
        maxSeconds: 320,
        meanWaitSeconds: 180,
        meanWorkSeconds: 132,
        costUsd: 7.62,
      },
      {
        step: 'enrich',
        deliveries: 6,
        failedAttempts: 0,
        p50Seconds: 240,
        p95Seconds: 252,
        maxSeconds: 260,
        meanWaitSeconds: 90,
        meanWorkSeconds: 156,
        costUsd: 1.9,
      },
      {
        step: 'fetch',
        deliveries: 6,
        failedAttempts: 1,
        p50Seconds: 84,
        p95Seconds: 96,
        maxSeconds: 102,
        meanWaitSeconds: 0,
        meanWorkSeconds: 84,
        costUsd: 0.32,
      },
    ],
    failures: [
      {
        step: 'score',
        class: 'schema',
        message: 'output schema: "score" missing',
        example: 'it_9d02e6b3',
        count: 2,
        recovered: 0,
      },
      {
        step: 'fetch',
        class: 'timeout',
        message: 'timeout after 30 s',
        example: 'it_1a8d4f02',
        count: 1,
        recovered: 1,
      },
    ],
    comparison: {
      runs: 10,
      durationSeconds: { value: 31 * 60, median: 52 * 60 },
      costUsd: { value: 9.84, median: 14.1 },
      items: { value: 1632, median: 1590 },
      failedAttempts: { value: 5, median: 2 },
    },
    eta: at('06:58'),
  },
};

export const demoRevisions: Revision[] = [
  { id: 14, pipeline: PIPELINE, packageHash: 'sha256:9f2c41d07a', appliedAt: '2026-10-13T06:40:00Z', active: true },
  { id: 13, pipeline: PIPELINE, packageHash: 'sha256:51c0e8d4b3', appliedAt: '2026-10-02T09:12:00Z', active: false },
];

const insight = (id: string, extra: Partial<Insight> & Pick<Insight, 'kind' | 'scope' | 'evidence'>): Insight => ({
  pipeline: PIPELINE,
  id,
  severity: 'warning',
  state: 'open',
  summary: '',
  windowStart: '2026-10-08T06:00:00Z',
  windowEnd: DEMO_NOW,
  openedAt: '2026-10-13T06:40:00Z',
  updatedAt: minutesBefore(3),
  ...extra,
});

export const demoInsights: Insight[] = [
  insight('in_slow_enrich', {
    kind: 'slowStep',
    scope: 'enrich',
    summary: '34% of the critical path (queue 40%, work 60%, backoff 0%); p95 4.1s, baseline 1.3s',
    evidence: {
      runs: 3,
      criticalPathShare: 0.34,
      queueWaitShare: 0.4,
      workShare: 0.6,
      backoffShare: 0,
      slowestPart: 'work',
      p95Seconds: 4.1,
      baselineP95Seconds: 1.3,
      samples: ['it_88a1c3f0', 'it_2b90d7e4', 'it_5e7f0a12'],
    },
  }),
  insight('in_fail_score', {
    kind: 'failureCluster',
    scope: 'score',
    severity: 'error',
    openedAt: at('06:28'),
    summary: '2 failures (1% of attempts), 0% recovered on retry: schema: output schema: "score" missing',
    evidence: {
      class: 'schema',
      message: 'output schema: "score" missing',
      example: 'it_9d02e6b3',
      count: 2,
      rate: 0.011,
      firstSeen: at('06:12'),
      lastSeen: at('06:22'),
      deliveries: 2,
      dead: 2,
      retrySuccessShare: 0,
      samples: ['it_9d02e6b3', 'it_c41e0b88'],
    },
  }),
  insight('in_review_analyst', {
    kind: 'reviewPattern',
    scope: 'analyst-review',
    severity: 'info',
    state: 'acknowledged',
    openedAt: '2026-10-14T07:10:00Z',
    summary: '10% rejected, 30% edited, 62% auto-approved, sampled agreement 95%, intervention 38%',
    evidence: {
      decisions: { approved: 18, rejected: 2, autoApproved: 33, timedOut: 0 },
      rejectionsByReason: { overpriced: 2 },
      rejectionRate: 0.1,
      autoApprovalRate: 0.62,
      editRate: 0.3,
      editHotSpots: [{ pointer: '/marginPct', edits: 6 }],
      timeToDecisionP50Seconds: 95,
      timeToDecisionP95Seconds: 410,
      samplesReviewed: { agree: 19, disagree: 1 },
      sampledAgreement: 0.95,
      interventionRate: 0.38,
      samples: ['it_4c11ab90', 'it_70e2d5c3', 'it_b8f6031e'],
    },
  }),
  insight('in_cost_score', {
    kind: 'cost',
    scope: 'score',
    severity: 'info',
    state: 'resolved',
    summary: '$7.62 for 402 outputs ($0.019 each, $0.42 per approved item), 2.1 turns per delivery, 0 schema failures',
    evidence: {
      costUsd: 7.62,
      outputs: 402,
      costPerOutput: 0.019,
      costPerApprovedItem: 0.42,
      baselineCostPerOutput: 0.018,
      turnsPerDelivery: 2.1,
      schemaFailures: 0,
      samples: [],
    },
  }),
  insight('in_unsched_render', {
    kind: 'unschedulable',
    scope: 'render',
    severity: 'error',
    state: 'muted',
    summary: '3 attempts no runtime can take: unsupported: no node offers runtime docker',
    evidence: {
      class: 'unsupported',
      example: 'no node offers runtime docker',
      count: 3,
      deliveries: 3,
      firstSeen: '2026-10-12T06:20:00Z',
      lastSeen: '2026-10-12T06:24:00Z',
      samples: ['it_0f3e9a71'],
    },
  }),
];

export const demoLessons: Record<string, StepLessons[]> = {
  [PIPELINE]: [
    {
      step: 'score',
      rules: [
        {
          text: 'Listings without a surface area are not scored; return score 0 and reason "missingSurface".',
          source: 'proposal/p-0142',
          added: '2026-10-03',
        },
        {
          text: 'Above the 3rd floor without a lift, lower the expected resale value by 4 %.',
          source: 'insight/in_review_analyst',
          added: '2026-10-14',
        },
        { text: 'Ignore the agency fee when it is not stated in the listing.' },
      ],
      examples: [
        {
          input: { title: 'T2 · 41 m² · Paris 11e', floor: 0, street: 'Boulevard Voltaire' },
          output: { score: 58, reason: 'groundFloorMainRoad' },
          why: 'Reviewer lowered the score: ground-floor flats on a main road.',
          source: 'review/dl_4b2e',
        },
      ],
    },
    { step: 'enrich', rules: [{ text: 'Prefer the cadastral surface over the advertised one.' }], examples: [] },
  ],
};

export const demoMetricNames = [
  'delivery_cost_usd',
  'delivery_latency_seconds',
  'delivery_wait_seconds',
  'delivery_work_seconds',
  'items_in',
  'items_out',
  'queue_depth',
  'review_decisions',
  'review_time_to_decision_seconds',
];

// A deterministic series per step: a level, a wobble, and for enrich a jump when r14 was applied.
export function demoMetrics(name: string, groupBy: string | undefined, from: Date, to: Date): MetricSeries[] {
  const groups = groupBy === 'step' ? ['enrich', 'score', 'dedupe'] : groupBy ? ['a', 'b'] : [name];
  const span = to.getTime() - from.getTime();
  const step = Math.max(60_000, Math.round(span / 120 / 60_000) * 60_000);
  const jump = Date.parse('2026-10-13T06:40:00Z');
  return groups.map((group, g) => {
    const level = [1.3, 2.4, 0.03][g] ?? 1;
    const points = [];
    for (let t = Math.ceil(from.getTime() / step) * step, i = 0; t <= to.getTime(); t += step, i += 1) {
      const wobble = Math.sin(i * 1.7 + g) * 0.12 + Math.cos(i * 0.9) * 0.07;
      const value = Math.max(0, level * (group === 'enrich' && t >= jump ? 3.1 : 1) * (1 + wobble));
      const count = 10 + ((i * 7 + g * 3) % 9);
      points.push({ t: new Date(t).toISOString(), count, sum: value * count, min: value * 0.4, max: value * 1.6 });
    }
    return { name, group, points };
  });
}

// Moves every timestamp of the demo data by the same offset, so ?demo reads as happening now.
export function shiftTimes<T>(value: T, offsetMs: number): T {
  const iso = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;
  const walk = (v: unknown): unknown => {
    if (typeof v === 'string' && iso.test(v)) return new Date(Date.parse(v) + offsetMs).toISOString();
    if (Array.isArray(v)) return v.map(walk);
    if (v !== null && typeof v === 'object') {
      return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, walk(x)]));
    }
    return v;
  };
  return walk(value) as T;
}
