import type { DeadLetter, Delivery, Insight, Item, PipelineSnapshot, Step, Stream } from './schema';

// Fixture data for the pipeline graph, item detail and dead letters screens, in the generic real estate example of
// the canvas: fake.ts folds it into demoFixtures, so ?demo shows every state those screens have.

const today = '2026-10-15';
const at = (time: string) => `${today}T${time}Z`;

// A dead letter as the FakeApi keeps it: the record, plus what the server groups by.
export interface FakeDeadLetter extends DeadLetter {
  pipeline: string;
  step: string;
  kind: 'dead' | 'unrouted';
  fingerprint: string;
  // The error with what varies taken out, as the server's DeadLetterGroup.error.
  pattern: string;
}

// An artifact's content: text for text and JSON, a data URL for images and PDFs.
export interface FakeArtifact {
  pipeline: string;
  owner: { item: string } | { delivery: string; attempt: number };
  name: string;
  content: string;
}

export function item(id: string, fields: Partial<Item> & Pick<Item, 'stream' | 'step'>): Item {
  return {
    id,
    pipeline: 'listings-daily',
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

function delivery(id: string, fields: Partial<Delivery> & Pick<Delivery, 'step' | 'itemId' | 'state'>): Delivery {
  return {
    id,
    pipeline: 'listings-daily',
    runId: 'run_0600',
    generation: 0,
    attempt: 1,
    costUsd: 0,
    createdAt: at('06:04:12'),
    waitMs: 0,
    workMs: 0,
    ...fields,
  };
}

const listingPayload = {
  title: 'T3 lumineux, cour calme',
  priceEur: 412000,
  surfaceM2: 68,
  rooms: 3,
  zip: '75019',
  floor: 4,
  lift: false,
  url: 'https://www.example.com/annonces/achat/appartement/paris-19eme-75/2210417.htm',
  photos: ['living.jpg', 'kitchen.jpg', 'bedroom.jpg'],
  score: 82,
  rentEstimate: 1640,
  marginPct: 17.8,
  sources: [{ site: 'homesite' }, { site: 'classifieds' }],
};

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
const transcript = {
  delivery: 'dv_score_51c0',
  item: 'it_51c0e8d4',
  attempt: 2,
  harness: 'claude-code',
  model: 'claude-sonnet',
  prompt:
    'Score this listing for a buy-to-let investor. T3, 68 m², 4th floor without lift, rue de Crimée, Paris 19e. ' +
    'Asking €412,000. Needs kitchen and bathroom renovation.',
  turns: [
    {
      role: 'user',
      text:
        'Score this listing for a buy-to-let investor. T3, 68 m², 4th floor without lift, rue de Crimée, Paris 19e. ' +
        'Asking €412,000. Needs kitchen and bathroom renovation.',
    },
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
            {
              type: 'tool_result',
              tool_use_id: 'tu_1',
              content: '14 comparables · median €9,120/m² · p25 €8,400/m²',
            },
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

const schemaError =
  'output.schema: validation failed at $.result: required property "score" missing; got {"assessment": "This listing ' +
  'appears undervalued relative to comparable properties in the 18th arrondissement. The asking price per square ' +
  'metre of €6,059 is well below the median of recent sales, and the renovation needs are limited to the kitchen ' +
  'and the bathroom, which an investor can budget at about €25,000 with some confidence…"}';

export const screensFixtures = {
  items: [
    item('it_1a8d4f02', { stream: 'raw', step: 'fetch', key: 'homesite-2210417', payload: { site: 'homesite' } }),
    item('it_51c0e8d4', {
      stream: 'enriched',
      step: 'enrich',
      key: 'paris-19-t3-68',
      parents: ['it_1a8d4f02'],
      ts: at('06:09:40'),
      payload: { ...listingPayload, score: undefined, rentEstimate: undefined, marginPct: undefined },
    }),
    // The review's listing (demoFixtures.inbox[0].item) with its artifacts.
    item('it_7f3a9c21', {
      stream: 'scored',
      step: 'score',
      key: 'paris-19-t3-68',
      parents: ['it_51c0e8d4'],
      generation: 1,
      ts: at('06:12:00'),
      payload: listingPayload,
      artifacts: [
        { name: 'living.svg', sha256: 'a1', size: 412, type: 'image/svg+xml' },
        { name: 'brochure.pdf', sha256: 'b2', size: 498, type: 'application/pdf' },
        { name: 'notes.json', sha256: 'c3', size: 64, type: 'application/json' },
        { name: 'raw.html', sha256: 'd4', size: 18200, type: 'text/html' },
      ],
    }),
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
  ] satisfies Item[],

  deliveries: [
    delivery('dv_enrich_1a8d', { step: 'enrich', itemId: 'it_1a8d4f02', state: 'done', workMs: 410 }),
    delivery('dv_score_51c0', {
      step: 'score',
      itemId: 'it_51c0e8d4',
      state: 'done',
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
    delivery('dl_4b2e', {
      step: 'analyst-review',
      itemId: 'it_7f3a9c21',
      state: 'awaitingReview',
      createdAt: at('06:14:00'),
      leasedAt: at('06:14:00'),
    }),
    delivery('dv_score_9d02', {
      step: 'score',
      itemId: 'it_9d02e6b3',
      state: 'dead',
      attempt: 2,
      costUsd: 0.008,
      errorClass: 'schema',
      errorMessage: schemaError,
      finishedAt: at('06:27:00'),
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
  ] satisfies Delivery[],

  deadLetters: [
    {
      id: 'it_dl_9d02',
      pipeline: 'listings-daily',
      step: 'score',
      kind: 'dead',
      fingerprint: 'fp3c9a0d17',
      pattern: 'output.schema: validation failed at <path>: required property "score" missing',
      runId: 'run_0600',
      originDelivery: 'dv_score_9d02',
      stream: 'score.dlq',
      state: 'open',
      error: schemaError,
      createdAt: at('06:27:00'),
    },
    {
      id: 'it_dl_c41e',
      pipeline: 'listings-daily',
      step: 'score',
      kind: 'dead',
      fingerprint: 'fp3c9a0d17',
      pattern: 'output.schema: validation failed at <path>: required property "score" missing',
      runId: 'run_0600',
      originDelivery: 'dv_score_c41e',
      stream: 'score.dlq',
      state: 'open',
      error: schemaError,
      createdAt: at('06:12:30'),
    },
    {
      id: 'it_dl_e2f8',
      pipeline: 'listings-daily',
      step: 'fetch',
      kind: 'dead',
      fingerprint: 'fp81b2e44c',
      pattern: 'context deadline exceeded: GET <url> (<n> s, <n> attempts)',
      runId: 'run_0600',
      originDelivery: 'dv_fetch_e2f8',
      stream: 'fetch.dlq',
      state: 'open',
      error:
        'context deadline exceeded: GET https://www.example.com/ad/ventes_immobilieres/2487310245 (30.0 s, 3 attempts)',
      createdAt: at('06:14:00'),
    },
    {
      id: 'it_dl_old1',
      pipeline: 'listings-daily',
      step: 'geocode',
      kind: 'unrouted',
      fingerprint: 'fp0aa1bb2c',
      pattern: 'no route matched the output',
      runId: 'run_0530',
      originDelivery: 'dv_geocode_old1',
      stream: 'geocode.dlq',
      state: 'replayed',
      error: 'no route matched the output',
      createdAt: '2026-10-14T06:20:00Z',
    },
    {
      id: 'it_dl_old2',
      pipeline: 'listings-daily',
      step: 'parse',
      kind: 'dead',
      fingerprint: 'fp7c7c7c7c',
      pattern: 'parse: unexpected token at <n>',
      runId: 'run_0530',
      originDelivery: 'dv_parse_old2',
      stream: 'parse.dlq',
      state: 'discarded',
      error: 'parse: unexpected token at 1042',
      createdAt: '2026-10-14T06:10:00Z',
    },
  ] satisfies FakeDeadLetter[],

  streams: {
    'listings-daily': [
      ['trigger', 1],
      ['raw', 1632],
      ['parsed', 1630],
      ['unique', 1108],
      ['located', 1108],
      ['enriched', 1102],
      ['scored', 402],
      ['approved', 22],
      ['pdf', 20],
    ].map(([name, items]) => ({ name: name as string, producers: [], consumers: [], items: items as number })),
  } satisfies Record<string, Stream[]>,

  snapshots: [
    {
      pipeline: 'listings-daily',
      at: at('06:31:00'),
      seq: 4210,
      paused: false,
      steps: [
        ['fetch', 0, 0, 0, 0, 0, 1100],
        ['parse', 0, 2, 0, 64, 0, 12],
        ['dedupe', 0, 4, 0, 64, 0, 8],
        ['geocode', 7, 3, 0, 9, 0, 4100],
        ['enrich', 2, 4, 0, 40, 0, 900],
        ['score', 186, 12, 0, 38, 0.011, 2400],
        ['analyst-review', 0, 0, 9, 0, 0, 0],
        ['render', 0, 2, 0, 4, 0, 1900],
        ['publish', 3, 0, 0, 0, 0, 0],
      ].map(([step, queued, inFlight, awaitingReview, throughputPerMin, errorRate, p95LatencyMs]) => ({
        step: step as string,
        paused: step === 'publish',
        queued: queued as number,
        inFlight: inFlight as number,
        awaitingReview: awaitingReview as number,
        throughputPerMin: throughputPerMin as number,
        errorRate: errorRate as number,
        p95LatencyMs: p95LatencyMs as number,
      })),
    },
  ] satisfies PipelineSnapshot[],

  insights: [
    {
      pipeline: 'listings-daily',
      id: 'in_cluster_score',
      kind: 'failureCluster',
      scope: 'score',
      severity: 'warning',
      state: 'open',
      summary: '2 items died with the same schema error since 06:12. Both outputs miss score.',
      windowStart: at('00:00:00'),
      windowEnd: at('06:30:00'),
      openedAt: at('06:27:30'),
      updatedAt: at('06:27:30'),
      evidence: {},
    },
    {
      pipeline: 'listings-daily',
      id: 'in_slow_geocode',
      kind: 'slowStep',
      scope: 'geocode',
      severity: 'warning',
      state: 'open',
      summary: 'geocode is the slowest step: p95 4.1 s, three times its weekly median.',
      windowStart: at('00:00:00'),
      windowEnd: at('06:30:00'),
      openedAt: at('06:20:00'),
      updatedAt: at('06:30:00'),
      evidence: {},
    },
  ] satisfies Insight[],

  artifacts: [
    { pipeline: 'listings-daily', owner: { item: 'it_7f3a9c21' }, name: 'living.svg', content: photo },
    { pipeline: 'listings-daily', owner: { item: 'it_7f3a9c21' }, name: 'brochure.pdf', content: pdf },
    {
      pipeline: 'listings-daily',
      owner: { item: 'it_7f3a9c21' },
      name: 'notes.json',
      content: '{"agent": "Agence du Canal", "visit": "2026-10-17"}',
    },
    {
      pipeline: 'listings-daily',
      owner: { delivery: 'dv_score_51c0', attempt: 2 },
      name: 'transcript.json',
      content: JSON.stringify(transcript),
    },
    {
      pipeline: 'listings-daily',
      owner: { delivery: 'dv_score_9d02', attempt: 2 },
      name: 'transcript.json',
      content: JSON.stringify(invalidTranscript),
    },
  ] satisfies FakeArtifact[],
};

// A 14-step pipeline, so ?demo shows the graph's minimap (above 12 steps).
export function longPipelineSteps(): Step[] {
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
  return names.map((name, i) => ({
    name,
    action: name === 'dedupe' || name === 'keep-good' ? (name === 'dedupe' ? 'dedupe' : 'filter') : `permits.${name}`,
    builtin: name === 'dedupe' || name === 'keep-good' || name === 'analyst-review',
    in: [i === 0 ? 'trigger' : `s${i}`],
    out: i === names.length - 1 ? [] : [`s${i + 1}`],
    dlq: `${name}.dlq`,
    concurrency: 2,
    paused: false,
    ...(name === 'analyst-review' ? { action: 'review' } : {}),
  }));
}

// The log lines ?demo streams on /live/logs, a gap among them.
export const demoLogEvents: [type: string, data: object][] = [
  ['log', { ts: at('06:09:44'), level: 'info', message: 'leased by mill-01 slot 4', step: 'score' }],
  [
    'log',
    {
      ts: at('06:09:45'),
      level: 'info',
      message: 'tool get_comparables zip=75019 → 14 results (0.9 s)',
      step: 'score',
    },
  ],
  ['gap', { lastEventId: '41' }],
  ['log', { ts: at('06:09:47'), level: 'warn', message: 'attempt 1 timed out, retrying', step: 'score' }],
  ['log', { ts: at('06:09:47'), level: 'info', message: 'done in 3.4 s, $0.011', step: 'score' }],
].map(([type, data]) => [
  type as string,
  {
    pipeline: 'listings-daily',
    runId: 'run_0600',
    itemId: 'it_51c0e8d4',
    deliveryId: '',
    attempt: 2,
    step: '',
    ...(data as object),
  },
]);
