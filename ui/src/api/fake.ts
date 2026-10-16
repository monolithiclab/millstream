import { ApiError, type Api } from './api';
import type { Item, Nodes, Pipeline, Review, Run, RunReport, Sample, Step } from './schema';

// Fixture data for tests and ?demo: the generic real estate example of the canvas.
export interface Fixtures {
  pipelines: Pipeline[];
  runs: Run[];
  inbox: Review[];
  samples: Sample[];
  nodes: Nodes;
}

const today = '2026-10-15';

function step(name: string, action: string, input: string[], out: string[], extra: Partial<Step> = {}): Step {
  return { name, action, builtin: false, in: input, out, dlq: '', concurrency: 4, paused: false, ...extra };
}

const listing: Item = {
  id: 'it_7f3a9c21',
  pipeline: 'listings-daily',
  runId: 'run_0600',
  stream: 'scored',
  step: 'score',
  key: 'paris-19-t3-68',
  parents: ['it_51c0e8d4'],
  generation: 1,
  ts: `${today}T06:12:00Z`,
  payload: { title: 'T3 · 68 m² · Paris 19e', priceEur: 489000, score: 82 },
  artifacts: [],
};

export const demoFixtures: Fixtures = {
  pipelines: [
    {
      name: 'listings-daily',
      description: 'Paris listings → scored, reviewed, PDF',
      state: 'active',
      revision: 14,
      steps: [
        step('fetch', 'http.fetch', ['trigger'], ['raw']),
        step('parse', 'listing.parse', ['raw'], ['parsed']),
        step('dedupe', 'dedupe', ['parsed'], ['unique'], { builtin: true }),
        step('geocode', 'geo.lookup', ['unique'], ['located']),
        step('enrich', 'listing.enrich', ['located'], ['enriched']),
        step('score', 'agent.score', ['enriched'], ['scored'], { concurrency: 2 }),
        step('analyst-review', 'review', ['scored'], ['approved'], { builtin: true }),
        step('render', 'pdf.render', ['approved'], ['pdf']),
        step('publish', 'drive.upload', ['pdf'], []),
      ],
      triggers: [{ name: 'daily', kind: 'cron', schedule: '0 6 * * *', stream: 'trigger' }],
      streams: ['trigger', 'raw', 'parsed', 'unique', 'located', 'enriched', 'scored', 'approved', 'pdf'],
      lastRun: { id: 'run_0600', state: 'running', startedAt: `${today}T06:00:00Z` },
      openDeadLetters: 3,
      openReviews: 1,
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
      lastRun: {
        id: 'run_0530',
        state: 'succeeded',
        startedAt: `${today}T05:30:00Z`,
        settledAt: `${today}T05:52:00Z`,
      },
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
  ],
  runs: [
    {
      id: 'run_0600',
      pipeline: 'listings-daily',
      revision: 14,
      kind: 'trigger',
      trigger: 'daily',
      triggerKey: `${today}T06:00:00Z`,
      triggerSource: 'cron',
      scheduledAt: `${today}T06:00:00Z`,
      generation: 0,
      state: 'running',
      startedAt: `${today}T06:00:00Z`,
      costUsd: 9.84,
      vars: { city: 'paris' },
      varSources: { city: 'pipeline' },
      items: { raw: 1632, scored: 402 },
      openDeadLetters: 3,
    },
  ],
  inbox: [
    {
      pipeline: 'listings-daily',
      deliveryId: 'dl_4b2e',
      runId: 'run_0600',
      step: 'analyst-review',
      inbox: 'analysts',
      state: 'open',
      version: 1,
      parkedAt: `${today}T06:14:00Z`,
      editable: ['/score'],
      reasons: ['overpriced', 'duplicate'],
      display: 'T3 · 68 m² · Paris 19e',
      item: listing,
    },
  ],
  samples: [],
  nodes: {
    engine: { state: 'active', version: '0.0.0' },
    nodes: [
      {
        id: 'mill-01',
        embedded: true,
        version: '0.0.0',
        slots: 8,
        leases: 2,
        labels: {},
        runtimes: ['process'],
      },
    ],
  },
};

export interface FakeCall {
  method: string;
  args: unknown[];
}

function report(run: Run): RunReport {
  return {
    pipeline: run.pipeline,
    run: run.id,
    kind: run.kind,
    state: run.state,
    startedAt: run.startedAt,
    settledAt: run.settledAt ?? null,
    durationSeconds: 0,
    costUsd: run.costUsd,
    items: run.items ?? {},
    criticalPath: { totalSeconds: 0, steps: null },
    steps: null,
    failures: null,
    comparison: null,
    eta: null,
  };
}

// In-memory Api: reads answer from the fixtures, actions are recorded in calls and resolve.
export class FakeApi implements Api {
  readonly calls: FakeCall[] = [];

  constructor(private readonly fixtures: Fixtures = demoFixtures) {}

  private record<T>(method: string, args: unknown[], result: T): Promise<T> {
    this.calls.push({ method, args });
    return Promise.resolve(result);
  }

  private notFound(what: string): Promise<never> {
    return Promise.reject(new ApiError(404, 'notFound', `${what} not found`));
  }

  private found<T>(value: T | undefined, what: string): Promise<T> {
    return value === undefined ? this.notFound(what) : Promise.resolve(value);
  }

  private run(pipeline: string, id: string): Promise<Run> {
    return this.found(
      this.fixtures.runs.find((r) => r.pipeline === pipeline && r.id === id),
      `run ${pipeline}/${id}`,
    );
  }

  private newRun(pipeline: string, kind: Run['kind']): Run {
    return {
      id: `run_${this.calls.length + 1}`,
      pipeline,
      revision: 1,
      kind,
      triggerKey: '',
      triggerSource: 'manual',
      generation: 0,
      state: 'running',
      startedAt: new Date(0).toISOString(),
      costUsd: 0,
      vars: {},
      varSources: {},
    };
  }

  listPipelines: Api['listPipelines'] = () => Promise.resolve(this.fixtures.pipelines);
  getPipeline: Api['getPipeline'] = (name) =>
    this.found(
      this.fixtures.pipelines.find((p) => p.name === name),
      `pipeline ${name}`,
    );
  listRevisions: Api['listRevisions'] = () => Promise.resolve([]);
  getRevision: Api['getRevision'] = (p, revision) => this.notFound(`revision ${p}/${revision}`);
  listStreams: Api['listStreams'] = () => Promise.resolve([]);
  listRuns: Api['listRuns'] = (pipeline) =>
    Promise.resolve({ items: this.fixtures.runs.filter((r) => r.pipeline === pipeline) });
  getRun: Api['getRun'] = (p, id) => this.run(p, id);
  getRunReport: Api['getRunReport'] = async (p, id) => report(await this.run(p, id));
  listRunDeliveries: Api['listRunDeliveries'] = () => Promise.resolve([]);
  listItems: Api['listItems'] = () => Promise.resolve({ items: [] });
  getItem: Api['getItem'] = (p, id) => this.notFound(`item ${p}/${id}`);
  getLineage: Api['getLineage'] = (_p, id) => Promise.resolve({ item: id, up: [], down: [], truncated: false });
  listItemDeliveries: Api['listItemDeliveries'] = () => Promise.resolve([]);
  getDelivery: Api['getDelivery'] = (p, id) => this.notFound(`delivery ${p}/${id}`);
  listDeadLetters: Api['listDeadLetters'] = () => Promise.resolve([]);
  listLessons: Api['listLessons'] = () => Promise.resolve([]);
  getReview: Api['getReview'] = (p, id) =>
    this.found(
      this.fixtures.inbox.find((r) => r.pipeline === p && r.deliveryId === id),
      `review ${p}/${id}`,
    );
  getSample: Api['getSample'] = (p, id) =>
    this.found(
      this.fixtures.samples.find((s) => s.pipeline === p && s.deliveryId === id),
      `sample ${p}/${id}`,
    );
  listInbox: Api['listInbox'] = ({ pipeline, inbox, step, state = 'open' } = {}) =>
    Promise.resolve({
      items: this.fixtures.inbox.filter(
        (r) =>
          r.state === state &&
          (pipeline === undefined || r.pipeline === pipeline) &&
          (inbox === undefined || r.inbox === inbox) &&
          (step === undefined || r.step === step),
      ),
    });
  listSamples: Api['listSamples'] = () => Promise.resolve({ items: this.fixtures.samples });
  listInsights: Api['listInsights'] = () => Promise.resolve([]);
  getMetrics: Api['getMetrics'] = () => Promise.resolve([]);
  listMetricNames: Api['listMetricNames'] = () => Promise.resolve([]);
  listNodes: Api['listNodes'] = () => Promise.resolve(this.fixtures.nodes);

  triggerRun: Api['triggerRun'] = (...args) => this.record('triggerRun', args, this.newRun(args[0], 'trigger'));
  cancelRun: Api['cancelRun'] = (...args) => this.record('cancelRun', args, undefined);
  pause: Api['pause'] = (...args) => this.record('pause', args, undefined);
  resume: Api['resume'] = (...args) => this.record('resume', args, undefined);
  replay: Api['replay'] = (...args) => this.record('replay', args, this.newRun(args[0], 'replay'));
  replayDeadLetters: Api['replayDeadLetters'] = (...args) =>
    this.record('replayDeadLetters', args, { count: args[1].ids?.length ?? 0 });
  discardDeadLetters: Api['discardDeadLetters'] = (...args) =>
    this.record('discardDeadLetters', args, { count: args[1].ids?.length ?? 0 });
  decideReview: Api['decideReview'] = async (...args) => {
    const review = await this.getReview(args[0], args[1]);
    return this.record('decideReview', args, { ...review, ...args[2], state: 'decided' });
  };
  decideSample: Api['decideSample'] = async (...args) => {
    const sample = await this.getSample(args[0], args[1]);
    return this.record('decideSample', args, { ...sample, ...args[2], state: 'decided' });
  };
  acknowledgeInsight: Api['acknowledgeInsight'] = (...args) => this.record('acknowledgeInsight', args, undefined);
  muteInsight: Api['muteInsight'] = (...args) => this.record('muteInsight', args, undefined);
}
