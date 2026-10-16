import type { FakeArtifact, FakeDeadLetter, Fixtures } from './fake';
import type {
  Delivery,
  Insight,
  Item,
  MetricSeries,
  Pipeline,
  PipelineSnapshot,
  Review,
  Revision,
  Run,
  RunReport,
  Sample,
  Step,
  StepLessons,
  Stream,
} from './schema';

// The demo dataset of tests and ?demo: the canvas's real estate example on 15 October 2026 at 06:31, while the
// 06:00 run of listings-daily is under way. One set of listings, items, deliveries, dead letters and insights serves
// every screen, so a link from one screen (an inbox card, a density strip, an insight sample) opens what the other
// screen shows. main.tsx shifts it to the present for ?demo.
export const DEMO_NOW = '2026-10-15T06:31:00Z';
const day = '2026-10-15';
const at = (time: string) => `${day}T${time.length === 5 ? `${time}:00` : time}Z`;
const minutesBefore = (minutes: number) => new Date(Date.parse(DEMO_NOW) - minutes * 60_000).toISOString();
const minute = (m: number) => new Date(Date.parse(at('06:00')) + m * 60_000).toISOString();

const PIPELINE = 'listings-daily';

// Pipelines

function step(name: string, action: string, input: string[], out: string[], extra: Partial<Step> = {}): Step {
  return { name, action, builtin: false, in: input, out, dlq: '', concurrency: 4, paused: false, ...extra };
}

// A 14-step pipeline, so the graph shows its minimap (above 12 steps).
function longPipelineSteps(): Step[] {
  const names = [
    'ingest',
    'validate',
    'normalise',
    'dedupe',
    'geocode',
    'zoning',
    'enrich',
    'classify',
    'score',
    'keep-good',
    'analyst-review',
    'render',
    'archive',
    'notify',
  ];
  const builtins: Record<string, string> = { dedupe: 'dedupe', 'keep-good': 'filter', 'analyst-review': 'review' };
  return names.map((name, i) => ({
    name,
    action: builtins[name] ?? `permits.${name}`,
    builtin: name in builtins,
    in: [i === 0 ? 'trigger' : `s${i}`],
    out: i === names.length - 1 ? [] : [`s${i + 1}`],
    dlq: `${name}.dlq`,
    concurrency: 2,
    paused: false,
  }));
}

const listingSteps = [
  step('fetch', 'http.fetch', ['trigger'], ['raw']),
  step('parse', 'listing.parse', ['raw'], ['parsed']),
  step('dedupe', 'dedupe', ['parsed'], ['unique'], { builtin: true }),
  step('geocode', 'geo.lookup', ['unique'], ['located']),
  step('enrich', 'listing.enrich', ['located'], ['enriched']),
  step('score', 'agent.score', ['enriched'], ['scored'], { concurrency: 2 }),
  step('analyst-review', 'review', ['scored'], ['approved'], { builtin: true }),
  step('render', 'pdf.render', ['approved'], ['pdf']),
  step('publish', 'drive.upload', ['pdf'], []),
];

const permitSteps = longPipelineSteps();

const demoPipelines: Pipeline[] = [
  {
    name: PIPELINE,
    description: 'Paris listings → scored, reviewed, PDF',
    state: 'active',
    revision: 14,
    steps: listingSteps,
    triggers: [
      { name: 'daily', kind: 'cron', schedule: '0 6 * * *', stream: 'trigger' },
      { name: 'listing', kind: 'webhook', stream: 'trigger', path: `/hooks/${PIPELINE}/listing`, rate: '60/min' },
    ],
    streams: ['trigger', 'raw', 'parsed', 'unique', 'located', 'enriched', 'scored', 'approved', 'pdf'],
    lastRun: { id: 'run_0600', state: 'running', startedAt: at('06:00') },
    openDeadLetters: 3,
    openReviews: 8,
  },
  {
    name: 'photo-tagging',
    description: 'Listing photos → room labels',
    state: 'active',
    revision: 2,
    steps: [
      step('fetch', 'http.fetch', ['trigger'], ['photos']),
      step('resize', 'image.resize', ['photos'], ['thumbs']),
      step('tag', 'agent.tag', ['thumbs'], ['tagged']),
      step('store', 'drive.upload', ['tagged'], []),
    ],
    triggers: [{ name: 'hourly', kind: 'cron', schedule: '30 * * * *', stream: 'trigger' }],
    streams: ['trigger', 'photos', 'thumbs', 'tagged'],
    lastRun: { id: 'run_0530', state: 'succeeded', startedAt: at('05:30'), settledAt: at('05:52') },
    openDeadLetters: 0,
    openReviews: 0,
  },
  {
    name: 'rentals-weekly',
    description: 'Rental index, Mondays',
    state: 'paused',
    revision: 6,
    steps: [
      step('fetch', 'http.fetch', ['trigger'], ['rentals']),
      step('index', 'rentals.index', ['rentals'], ['index']),
      step('publish', 'drive.upload', ['index'], []),
    ],
    triggers: [{ name: 'by-hand', kind: 'manual', stream: 'trigger' }],
    streams: ['trigger', 'rentals', 'index'],
    openDeadLetters: 0,
    openReviews: 0,
  },
  {
    name: 'city-permits',
    description: 'Building permits → zoning, scored, reviewed',
    state: 'active',
    revision: 3,
    steps: permitSteps,
    triggers: [{ name: 'weekly', kind: 'cron', schedule: '0 5 * * 1', stream: 'trigger' }],
    streams: ['trigger', ...permitSteps.flatMap((s) => s.out)],
    openDeadLetters: 0,
    openReviews: 0,
  },
];

// Runs

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

const demoRuns: Run[] = [
  run('run_0600', at('06:00'), {
    state: 'running',
    costUsd: 9.84,
    items: { raw: 1632, scored: 402 },
    openDeadLetters: 3,
  }),
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

// Items: the listings of the 06:00 run, from the fetched page to the review, and its dead letters

function item(id: string, fields: Partial<Item> & Pick<Item, 'stream' | 'step'>): Item {
  return {
    id,
    pipeline: PIPELINE,
    runId: 'run_0600',
    key: id,
    parents: [],
    generation: 0,
    ts: at('06:04:12'),
    payload: {},
    artifacts: [],
    ...fields,
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

// The listings parked in the analysts inbox, oldest first.
const listings: [Listing, ...Listing[]] = [
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
    item: 'it_3e8b52d6',
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
    item: 'it_6d47c0a9',
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

// Auto-approved by the rule, then drawn as a sample for a second look.
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

const scoredItem = (l: Listing, ts: string): Item =>
  item(l.item, { stream: 'scored', step: 'score', key: l.item.slice(3), generation: 1, ts, payload: payloadOf(l) });

// The first listing in full: its journey from the fetched page, its artifacts and its agent transcript.
const firstPayload = {
  ...payloadOf(listings[0]),
  title: 'T3 lumineux, cour calme',
  zip: '75019',
  floor: 4,
  lift: false,
  photos: ['living.jpg', 'kitchen.jpg', 'bedroom.jpg'],
  sources: [{ site: 'homesite' }, { site: 'classifieds' }],
};
const firstListing: Item = item('it_7f3a9c21', {
  stream: 'scored',
  step: 'score',
  key: 'paris-19-t3-68',
  parents: ['it_51c0e8d4'],
  generation: 1,
  ts: at('06:12:00'),
  payload: firstPayload,
  artifacts: [
    { name: 'living.svg', sha256: 'a1', size: 412, type: 'image/svg+xml' },
    { name: 'brochure.pdf', sha256: 'b2', size: 498, type: 'application/pdf' },
    { name: 'notes.json', sha256: 'c3', size: 64, type: 'application/json' },
    { name: 'raw.html', sha256: 'd4', size: 18200, type: 'text/html' },
  ],
});

const schemaError =
  'output.schema: validation failed at $.result: required property "score" missing; got {"assessment": "This listing ' +
  'appears undervalued relative to comparable properties in the 18th arrondissement. The asking price per square ' +
  'metre of €6,059 is well below the median of recent sales, and the renovation needs are limited to the kitchen ' +
  'and the bathroom, which an investor can budget at about €25,000 with some confidence…"}';

const namedItems: Item[] = [
  item('it_1a8d4f02', { stream: 'raw', step: 'fetch', key: 'homesite-2210417', payload: { site: 'homesite' } }),
  item('it_51c0e8d4', {
    stream: 'enriched',
    step: 'enrich',
    key: 'paris-19-t3-68',
    parents: ['it_1a8d4f02'],
    ts: at('06:09:40'),
    payload: { ...firstPayload, score: undefined, rentEstimateEur: undefined, marginPct: undefined },
  }),
  firstListing,
  ...listings.slice(1).map((l) => scoredItem(l, minutesBefore(l.waited + 1))),
  scoredItem(sampled, minutesBefore(7)),
  // The two listings score dies on (the failure cluster's samples).
  item('it_9d02e6b3', {
    stream: 'enriched',
    step: 'enrich',
    key: 'paris-18-t2-41',
    payload: { title: 'T2 · 41 m² · Paris 18e', priceEur: 248000 },
  }),
  item('it_c41e0b88', {
    stream: 'enriched',
    step: 'enrich',
    key: 'paris-19-t3-66',
    payload: { title: 'T3 · 66 m² · Paris 19e', priceEur: 455000 },
  }),
  item('it_e2f81a07', { stream: 'trigger', step: 'trigger', key: 'classifieds-2487310245', payload: {} }),
  item('it_dl_9d02', {
    stream: 'score.dlq',
    step: 'score',
    key: '',
    parents: ['it_9d02e6b3'],
    ts: at('06:27:00'),
    payload: { kind: 'dead', originStep: 'score', originDelivery: 'dv_score_9d02', error: schemaError },
  }),
  item('it_dl_c41e', {
    stream: 'score.dlq',
    step: 'score',
    key: '',
    parents: ['it_c41e0b88'],
    ts: at('06:12:30'),
    payload: { kind: 'dead', originStep: 'score', originDelivery: 'dv_score_c41e', error: schemaError },
  }),
  item('it_dl_e2f8', {
    stream: 'fetch.dlq',
    step: 'fetch',
    key: '',
    parents: ['it_e2f81a07'],
    ts: at('06:14:00'),
    payload: { kind: 'dead', originStep: 'fetch', originDelivery: 'dv_fetch_e2f8', error: 'timeout' },
  }),
  // The listing render could not place on 12 October (the unschedulable insight's sample).
  item('it_0f3e9a71', {
    stream: 'approved',
    step: 'analyst-review',
    runId: 'run_1430_1012',
    ts: '2026-10-12T06:20:00Z',
    payload: { title: 'T2 · 52 m² · Vincennes' },
  }),
  // Tombstones of 13 October (spec section 9.3): a fetched page past the raw stream's retention, and its enriched
  // listing, erased by a purge.
  item('it_3b7e2c90', {
    stream: 'raw',
    step: 'fetch',
    runId: 'run_0600_1013',
    key: 'homesite-2198034',
    ts: '2026-10-13T06:03:10Z',
    payload: null,
    tombstone: 'expired',
  }),
  item('it_8c4d1f6e', {
    stream: 'enriched',
    step: 'enrich',
    runId: 'run_0600_1013',
    key: '',
    parents: ['it_3b7e2c90'],
    ts: '2026-10-13T06:08:40Z',
    payload: null,
    tombstone: 'erased',
  }),
];

// Reviews and samples

const itemOf = (l: Listing): Item => namedItems.find((i) => i.id === l.item) ?? scoredItem(l, DEMO_NOW);

const demoReviews: Review[] = listings.map((l) => ({
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
  item: itemOf(l),
}));

// Deciding this review answers 409, as when another reviewer got there first.
const demoConflicts: Record<string, string> = {
  dl_7c1d: 'review of delivery dl_7c1d was already decided by claire at 06:29',
};

const demoSamples: Sample[] = [
  {
    pipeline: PIPELINE,
    deliveryId: sampled.delivery,
    runId: 'run_0600',
    step: 'analyst-review',
    inbox: 'analysts',
    state: 'open',
    display: `${sampled.title}\n${sampled.where}`,
    createdAt: minutesBefore(sampled.waited),
    item: itemOf(sampled),
  },
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
  ['analyst-review', listings.length, 16, 1.4, 0, 0],
  ['render', 2, 20, 4, 1.5, 0.5],
];
const nowMinute = 31;
// The score deliveries that die, by their place in the plan: the failure cluster's two listings.
const deadScores: Record<number, { id: string; itemId: string }> = {
  2: { id: 'dv_score_c41e', itemId: 'it_c41e0b88' },
  6: { id: 'dv_score_9d02', itemId: 'it_9d02e6b3' },
};

function delivery(id: string, fields: Partial<Delivery> & Pick<Delivery, 'step' | 'itemId'>): Delivery {
  return {
    id,
    pipeline: PIPELINE,
    runId: 'run_0600',
    generation: 0,
    state: 'done',
    attempt: 1,
    costUsd: 0,
    createdAt: at('06:04:12'),
    waitMs: 0,
    workMs: 0,
    ...fields,
  };
}

function planned([step, count, from, every, work, wait]: Plan): Delivery[] {
  return Array.from({ length: count }, (_, n) => {
    const created = from + n * every;
    const base = { step, createdAt: minute(created), costUsd: step === 'score' ? 0.019 : 0 };
    const review = step === 'analyst-review' ? listings[n] : undefined;
    if (review) {
      return delivery(review.delivery, {
        ...base,
        itemId: review.item,
        state: 'awaitingReview',
        leasedAt: minute(created),
      });
    }
    const leased = created + wait;
    const finished = leased + work;
    const named = { id: `dl_${step}_${n}`, itemId: `it_${step}_${n}` };
    if (leased > nowMinute) return delivery(named.id, { ...base, itemId: named.itemId, state: 'pending' });
    if (finished > nowMinute) {
      return delivery(named.id, {
        ...base,
        itemId: named.itemId,
        state: 'leased',
        leasedAt: minute(leased),
        waitMs: wait * 60_000,
      });
    }
    const dead = step === 'score' ? deadScores[n] : undefined;
    return delivery(dead?.id ?? named.id, {
      ...base,
      itemId: dead?.itemId ?? named.itemId,
      state: dead ? 'dead' : 'done',
      attempt: dead ? 2 : 1,
      ...(dead ? { errorClass: 'schema', errorMessage: schemaError } : {}),
      leasedAt: minute(leased),
      finishedAt: minute(finished),
      waitMs: Math.round(wait * 60_000),
      workMs: Math.round(work * 60_000),
    });
  });
}

// What the item screen shows of some deliveries beyond the timeline's figures: their attempts.
const detailed: Delivery[] = [
  delivery('dv_enrich_1a8d', { step: 'enrich', itemId: 'it_1a8d4f02', workMs: 410 }),
  delivery('dv_score_51c0', {
    step: 'score',
    itemId: 'it_51c0e8d4',
    attempt: 2,
    costUsd: 0.011,
    createdAt: at('06:09:40'),
    finishedAt: at('06:09:47'),
    workMs: 3400,
    attempts: [
      {
        attempt: 1,
        outcome: 'retry',
        errorClass: 'timeout',
        error: 'attempt timed out after 30 s',
        costUsd: 0,
        workMs: 30000,
        artifacts: [],
      },
      {
        attempt: 2,
        outcome: 'done',
        costUsd: 0.011,
        workMs: 3400,
        endedAt: at('06:09:47'),
        artifacts: [{ name: 'transcript.json', sha256: 'e5', size: 2400, type: 'application/json' }],
      },
    ],
  }),
  delivery('dv_score_9d02', {
    step: 'score',
    itemId: 'it_9d02e6b3',
    state: 'dead',
    attempt: 2,
    costUsd: 0.008,
    errorClass: 'schema',
    errorMessage: schemaError,
    workMs: 2100,
    attempts: [
      {
        attempt: 1,
        outcome: 'retry',
        errorClass: 'schema',
        error: 'output.schema: required property "score" missing at $.result',
        costUsd: 0.007,
        workMs: 1900,
        artifacts: [],
      },
      {
        attempt: 2,
        outcome: 'dead',
        errorClass: 'schema',
        error: schemaError,
        costUsd: 0.008,
        workMs: 2100,
        artifacts: [{ name: 'transcript.json', sha256: 'f6', size: 900, type: 'application/json' }],
      },
    ],
  }),
];

// The planned deliveries, with the detailed ones laid over those of the same id (timing from the plan).
const demoDeliveries: Delivery[] = (() => {
  const all = plans.flatMap(planned);
  for (const d of detailed) {
    const i = all.findIndex((p) => p.id === d.id);
    const planned = all[i];
    if (planned === undefined) all.push(d);
    else all[i] = { ...planned, ...d, createdAt: planned.createdAt };
  }
  return all;
})();

// Every delivery's item exists: the planned ones without a listing get a bare item of the stream the step reads.
const demoItems: Item[] = [
  ...namedItems,
  ...demoDeliveries
    .filter((d) => !namedItems.some((i) => i.id === d.itemId))
    .map((d) => {
      const input = listingSteps.find((s) => s.name === d.step)?.in[0] ?? 'trigger';
      const producer = listingSteps.find((s) => s.out.includes(input))?.name ?? 'trigger';
      return item(d.itemId, { stream: input, step: producer, ts: d.createdAt, payload: {} });
    }),
];

// Run reports

const demoReports: Record<string, Partial<RunReport>> = {
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
          delivery: 'dl_4b2e',
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

const demoRevisions: Revision[] = [
  {
    id: 14,
    pipeline: PIPELINE,
    packageHash: 'sha256:9f2c41d07a',
    appliedAt: '2026-10-13T06:40:00Z',
    message: 'score: stricter output schema',
    active: true,
  },
  { id: 13, pipeline: PIPELINE, packageHash: 'sha256:51c0e8d4b3', appliedAt: '2026-10-02T09:12:00Z', active: false },
  {
    id: 12,
    pipeline: PIPELINE,
    packageHash: 'sha256:c7e19a05f2',
    appliedAt: '2026-09-18T14:05:00Z',
    message: 'geocode: switch to the BAN API',
    active: false,
  },
];

// Dead letters: the two listings score died on, a fetch timeout, and yesterday's, settled.

const deadLetter = (
  id: string,
  fields: Omit<FakeDeadLetter, 'id' | 'pipeline' | 'runId' | 'state'> & Partial<FakeDeadLetter>,
): FakeDeadLetter => ({ id, pipeline: PIPELINE, runId: 'run_0600', state: 'open', ...fields });

const scorePattern = 'output.schema: validation failed at <path>: required property "score" missing';

const demoDeadLetters: FakeDeadLetter[] = [
  deadLetter('it_dl_9d02', {
    step: 'score',
    kind: 'dead',
    fingerprint: 'fp3c9a0d17',
    pattern: scorePattern,
    originDelivery: 'dv_score_9d02',
    stream: 'score.dlq',
    error: schemaError,
    createdAt: at('06:27:00'),
  }),
  deadLetter('it_dl_c41e', {
    step: 'score',
    kind: 'dead',
    fingerprint: 'fp3c9a0d17',
    pattern: scorePattern,
    originDelivery: 'dv_score_c41e',
    stream: 'score.dlq',
    error: schemaError,
    createdAt: at('06:12:30'),
  }),
  deadLetter('it_dl_e2f8', {
    step: 'fetch',
    kind: 'dead',
    fingerprint: 'fp81b2e44c',
    pattern: 'context deadline exceeded: GET <url> (<n> s, <n> attempts)',
    originDelivery: 'dv_fetch_e2f8',
    stream: 'fetch.dlq',
    error:
      'context deadline exceeded: GET https://www.example.com/ad/ventes_immobilieres/2487310245 (30.0 s, 3 attempts)',
    createdAt: at('06:14:00'),
  }),
  deadLetter('it_dl_old1', {
    step: 'geocode',
    kind: 'unrouted',
    fingerprint: 'fp0aa1bb2c',
    pattern: 'no route matched the output',
    runId: 'run_0600_1014',
    originDelivery: 'dv_geocode_old1',
    stream: 'geocode.dlq',
    state: 'replayed',
    error: 'no route matched the output',
    createdAt: '2026-10-14T06:20:00Z',
  }),
  deadLetter('it_dl_old2', {
    step: 'parse',
    kind: 'dead',
    fingerprint: 'fp7c7c7c7c',
    pattern: 'parse: unexpected token at <n>',
    runId: 'run_0600_1014',
    originDelivery: 'dv_parse_old2',
    stream: 'parse.dlq',
    state: 'discarded',
    error: 'parse: unexpected token at 1042',
    createdAt: '2026-10-14T06:10:00Z',
  }),
];

// Streams and the live snapshot of the graph

const demoStreams: Record<string, Stream[]> = {
  [PIPELINE]: (
    [
      ['trigger', 1],
      ['raw', 1632],
      ['parsed', 1630],
      ['unique', 1108],
      ['located', 1108],
      ['enriched', 1102],
      ['scored', 402],
      ['approved', 22],
      ['pdf', 20],
    ] as const
  ).map(([name, items]) => ({ name, producers: [], consumers: [], items })),
};

type SnapshotRow = [
  step: string,
  queued: number,
  inFlight: number,
  awaiting: number,
  perMin: number,
  err: number,
  p95: number,
];
const snapshotRows: SnapshotRow[] = [
  ['fetch', 0, 0, 0, 0, 0, 1100],
  ['parse', 0, 2, 0, 64, 0, 12],
  ['dedupe', 0, 4, 0, 64, 0, 8],
  ['geocode', 7, 3, 0, 9, 0, 4100],
  ['enrich', 2, 4, 0, 40, 0, 900],
  ['score', 186, 12, 0, 38, 0.011, 2400],
  ['analyst-review', 0, 0, listings.length, 0, 0, 0],
  ['render', 0, 2, 0, 4, 0, 1900],
  ['publish', 3, 0, 0, 0, 0, 0],
];

const demoSnapshots: PipelineSnapshot[] = [
  {
    pipeline: PIPELINE,
    at: at('06:31:00'),
    seq: 4210,
    paused: false,
    steps: snapshotRows.map(([step, queued, inFlight, awaitingReview, throughputPerMin, errorRate, p95LatencyMs]) => ({
      step,
      paused: step === 'publish',
      queued,
      inFlight,
      awaitingReview,
      throughputPerMin,
      errorRate,
      p95LatencyMs,
    })),
  },
];

// Insights

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

const demoInsights: Insight[] = [
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
      lastSeen: at('06:27'),
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

// Lessons

const demoLessons: Record<string, StepLessons[]> = {
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

// Metrics

const demoMetricNames = [
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
function demoMetrics(name: string, groupBy: string | undefined, from: Date, to: Date): MetricSeries[] {
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

// Artifacts: the first listing's photo, brochure and notes, and two agent transcripts

const photo = `data:image/svg+xml,${encodeURIComponent(
  '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 120"><rect width="160" height="120" fill="#e6f0ec"/>' +
    '<path d="M0 90L50 50L90 80L120 60L160 90V120H0Z" fill="#0e6b66" opacity=".55"/>' +
    '<circle cx="124" cy="30" r="12" fill="#e5b056"/></svg>',
)}`;

// A one-page PDF ("Listing brochure").
const pdf = `data:application/pdf;base64,${btoa(
  '%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n' +
    '3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 300 144]/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>endobj\n' +
    '4 0 obj<</Length 44>>stream\nBT /F1 18 Tf 24 72 Td (Listing brochure) Tj ET\nendstream endobj\n' +
    '5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\ntrailer<</Root 1 0 R>>\n%%EOF\n',
)}`;

// An agent attempt's transcript.json (internal/agent.Transcript), with claude-code stream-json turns.
const prompt =
  'Score this listing for a buy-to-let investor. T3, 68 m², 4th floor without lift, rue de Crimée, Paris 19e. ' +
  'Asking €412,000. Needs kitchen and bathroom renovation.';
const transcript = {
  delivery: 'dv_score_51c0',
  item: 'it_51c0e8d4',
  attempt: 2,
  harness: 'claude-code',
  model: 'claude-sonnet',
  prompt,
  turns: [
    { role: 'user', text: prompt },
    {
      role: 'assistant',
      data: {
        type: 'assistant',
        message: {
          content: [
            { type: 'text', text: 'I will compare it with recent sales nearby.' },
            { type: 'tool_use', id: 'tu_1', name: 'get_comparables', input: { zip: '75019', rooms: 3 } },
          ],
        },
      },
    },
    {
      role: 'user',
      data: {
        type: 'user',
        message: {
          content: [
            { type: 'tool_result', tool_use_id: 'tu_1', content: '14 comparables · median €9,120/m² · p25 €8,400/m²' },
          ],
        },
      },
    },
    { role: 'result', data: { type: 'result', subtype: 'success', num_turns: 2 } },
  ],
  output: {
    score: 82,
    reasons: ['€6,059/m² vs €9,120 median (−34 %)', 'renovation needed: kitchen, bathroom'],
    confidence: 0.74,
  },
  tokensIn: 2310,
  tokensOut: 184,
  costUsd: 0.011,
  numTurns: 2,
  durationMs: 3400,
};

// A failed attempt's transcript: the agent answered prose instead of JSON.
const invalidTranscript = {
  delivery: 'dv_score_9d02',
  item: 'it_9d02e6b3',
  attempt: 2,
  harness: 'api',
  model: 'claude-sonnet',
  prompt: 'Score this listing for a buy-to-let investor. T2, 41 m², Paris 18e.',
  turns: [
    { role: 'user', text: 'Score this listing for a buy-to-let investor. T2, 41 m², Paris 18e.' },
    {
      role: 'assistant',
      text: 'This listing appears undervalued relative to comparable properties in the 18th arrondissement.',
    },
  ],
  error: 'output.schema: required property "score" missing at $.result',
  tokensIn: 1980,
  tokensOut: 96,
  costUsd: 0.008,
  numTurns: 1,
  durationMs: 2100,
};

const demoArtifacts: FakeArtifact[] = [
  { pipeline: PIPELINE, owner: { item: 'it_7f3a9c21' }, name: 'living.svg', content: photo },
  { pipeline: PIPELINE, owner: { item: 'it_7f3a9c21' }, name: 'brochure.pdf', content: pdf },
  {
    pipeline: PIPELINE,
    owner: { item: 'it_7f3a9c21' },
    name: 'notes.json',
    content: '{"agent": "Agence du Canal", "visit": "2026-10-17"}',
  },
  {
    pipeline: PIPELINE,
    owner: { delivery: 'dv_score_51c0', attempt: 2 },
    name: 'transcript.json',
    content: JSON.stringify(transcript),
  },
  {
    pipeline: PIPELINE,
    owner: { delivery: 'dv_score_9d02', attempt: 2 },
    name: 'transcript.json',
    content: JSON.stringify(invalidTranscript),
  },
];

export const demoFixtures: Fixtures = {
  pipelines: demoPipelines,
  runs: demoRuns,
  inbox: demoReviews,
  samples: demoSamples,
  nodes: {
    engine: {
      state: 'active',
      id: 'mill-01',
      version: '0.0.0',
      since: minutesBefore(2000),
      epoch: 3,
      notificationsFailed: 0,
    },
    engines: [
      { id: 'mill-01', state: 'active', version: '0.0.0', since: minutesBefore(2000), epoch: 3 },
      { id: 'mill-02', state: 'standby', version: '0.0.0', since: minutesBefore(2000) },
    ],
    nodes: [
      {
        id: 'mill-01',
        embedded: true,
        version: '0.0.0',
        behind: false,
        slots: 8,
        leases: 2,
        labels: {},
        runtimes: ['exec', 'agent:api'],
        state: 'active',
        health: 'healthy',
      },
      {
        id: 'gpu-01',
        embedded: false,
        version: '0.0.0',
        behind: false,
        slots: 4,
        leases: 3,
        labels: { gpu: 'true', zone: 'eu-west' },
        runtimes: ['exec', 'container', 'agent:api', 'agent:claude-code'],
        state: 'active',
        health: 'healthy',
        lastHeartbeat: new Date(Date.parse(DEMO_NOW) - 12_000).toISOString(),
      },
      {
        id: 'worker-03',
        embedded: false,
        version: '0.9.8',
        behind: true,
        slots: 4,
        leases: 1,
        labels: { zone: 'eu-west' },
        runtimes: ['exec', 'container'],
        state: 'draining',
        health: 'late',
        lastHeartbeat: minutesBefore(2),
      },
    ],
    // No node offers arm64: photo-tagging's resize waits.
    unschedulable: [
      { pipeline: 'photo-tagging', step: 'resize', pending: 12, needs: 'runtime container, labels arch=arm64' },
    ],
  },
  items: demoItems,
  deliveries: demoDeliveries,
  deadLetters: demoDeadLetters,
  streams: demoStreams,
  snapshots: demoSnapshots,
  artifacts: demoArtifacts,
  reports: demoReports,
  revisions: demoRevisions,
  insights: demoInsights,
  lessons: demoLessons,
  metricNames: demoMetricNames,
  metrics: demoMetrics,
  conflicts: demoConflicts,
};

// The log stream ?demo plays: the first listing's score attempt, then a few lines at 06:21, a lost connection, and
// the lines after it. A scoped stream (?item=, ?step=) plays the lines of its scope.
const logLine = (ts: string, level: string, message: string, step: string, itemId: string, attempt = 1) => ({
  ts: at(ts),
  level,
  message,
  pipeline: PIPELINE,
  runId: 'run_0600',
  step,
  itemId,
  deliveryId: '',
  attempt,
});
export const demoLogEvents: [type: 'log' | 'gap', data: Record<string, unknown>][] = [
  ['log', logLine('06:09:44', 'info', 'leased by mill-01 slot 4', 'score', 'it_51c0e8d4', 2)],
  [
    'log',
    logLine('06:09:45', 'info', 'tool get_comparables zip=75019 → 14 results (0.9 s)', 'score', 'it_51c0e8d4', 2),
  ],
  ['log', logLine('06:09:47', 'info', 'done in 3.4 s, $0.011', 'score', 'it_51c0e8d4', 2)],
  ['log', logLine('06:21:01.204', 'info', 'it_2b90d7e4 leased by mill-01 slot 4', 'score', 'it_2b90d7e4')],
  [
    'log',
    logLine('06:21:02.881', 'info', 'tool get_comparables zip=93500 → 11 results (0.7 s)', 'score', 'it_2b90d7e4'),
  ],
  ['log', logLine('06:21:03.950', 'warn', 'it_9d02e6b3 output.schema failed, retry 2/2', 'score', 'it_9d02e6b3')],
  ['gap', { lastEventId: '6' }],
  ['log', logLine('06:21:19.402', 'info', 'it_5e7f0a12 done in 2.1 s, $0.009', 'score', 'it_5e7f0a12')],
  ['log', logLine('06:21:20.017', 'error', 'it_9d02e6b3 dead: output.schema "score" missing', 'score', 'it_9d02e6b3')],
  ['log', logLine('06:21:20.533', 'info', 'it_88a1c3f0 leased by mill-01 slot 1', 'enrich', 'it_88a1c3f0')],
];

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
