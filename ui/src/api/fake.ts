import { ApiError, type Api, type ArtifactLocation } from './api';
import { longPipelineSteps, screensFixtures, type FakeArtifact, type FakeDeadLetter } from './fakeData';
import type {
  DeadLetterGroup,
  DeadLetterSelection,
  Delivery,
  Insight,
  Item,
  LineageNode,
  Nodes,
  Pipeline,
  PipelineSnapshot,
  Review,
  Run,
  RunReport,
  Sample,
  Step,
  Stream,
} from './schema';

// Fixture data for tests and ?demo: the generic real estate example of the canvas.
export interface Fixtures {
  pipelines: Pipeline[];
  runs: Run[];
  inbox: Review[];
  samples: Sample[];
  nodes: Nodes;
  // The graph, item and dead letters screens (fakeData.ts).
  items: Item[];
  deliveries: Delivery[];
  deadLetters: FakeDeadLetter[];
  streams: Record<string, Stream[]>;
  snapshots: PipelineSnapshot[];
  insights: Insight[];
  artifacts: FakeArtifact[];
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

const permitSteps = longPipelineSteps();

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
  ...screensFixtures,
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

  private readonly fixtures: Fixtures;

  // A copy: actions change the FakeApi's own records (a replayed dead letter, a paused step), never the fixtures.
  constructor(fixtures: Fixtures = demoFixtures) {
    this.fixtures = structuredClone(fixtures);
  }

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

  private pipeline(name: string): Pipeline | undefined {
    return this.fixtures.pipelines.find((p) => p.name === name);
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
  getPipeline: Api['getPipeline'] = (name) => this.found(this.pipeline(name), `pipeline ${name}`);
  listRevisions: Api['listRevisions'] = () => Promise.resolve([]);
  getRevision: Api['getRevision'] = (p, revision) => this.notFound(`revision ${p}/${revision}`);
  listStreams: Api['listStreams'] = (p) => Promise.resolve(this.fixtures.streams[p] ?? []);
  listRuns: Api['listRuns'] = (pipeline) =>
    Promise.resolve({ items: this.fixtures.runs.filter((r) => r.pipeline === pipeline) });
  getRun: Api['getRun'] = (p, id) => this.run(p, id);
  getRunReport: Api['getRunReport'] = async (p, id) => report(await this.run(p, id));
  listRunDeliveries: Api['listRunDeliveries'] = () => Promise.resolve([]);
  listItems: Api['listItems'] = (p, { stream, run } = {}) =>
    Promise.resolve({
      items: this.fixtures.items.filter(
        (i) =>
          i.pipeline === p && (stream === undefined || i.stream === stream) && (run === undefined || i.runId === run),
      ),
    });
  getItem: Api['getItem'] = (p, id) =>
    this.found(
      this.fixtures.items.find((i) => i.pipeline === p && i.id === id),
      `item ${p}/${id}`,
    );
  getLineage: Api['getLineage'] = async (p, id, { depth = 10 } = {}) => {
    await this.getItem(p, id);
    const items = this.fixtures.items.filter((i) => i.pipeline === p);
    const node = ({ id, runId, stream, step, key, parents, ts }: Item): LineageNode => ({
      id,
      runId,
      stream,
      step,
      key,
      parents,
      ts,
    });
    let truncated = false;
    // Breadth first from the item, nearest first, depth levels at most.
    const walk = (next: (from: Item) => Item[]) => {
      const out: LineageNode[] = [];
      const seen = new Set([id]);
      let level = items.filter((i) => i.id === id);
      for (let d = 0; level.length > 0; d++) {
        const following = level.flatMap(next).filter((i) => !seen.has(i.id));
        if (d === depth) {
          truncated ||= following.length > 0;
          break;
        }
        following.forEach((i) => seen.add(i.id));
        out.push(...following.map(node));
        level = following;
      }
      return out;
    };
    const up = walk((from) => items.filter((i) => from.parents.includes(i.id)));
    const down = walk((from) => items.filter((i) => i.parents.includes(from.id)));
    return { item: id, up, down, truncated };
  };
  listItemDeliveries: Api['listItemDeliveries'] = (p, id, { step, state } = {}) =>
    Promise.resolve(
      this.fixtures.deliveries
        .filter(
          (d) =>
            d.pipeline === p &&
            d.itemId === id &&
            (step === undefined || d.step === step) &&
            (state === undefined || d.state === state),
        )
        // Attempts come with one delivery only.
        .map((d) => {
          const copy = { ...d };
          delete copy.attempts;
          return copy;
        }),
    );
  getDelivery: Api['getDelivery'] = (p, id) =>
    this.found(
      this.fixtures.deliveries.find((d) => d.pipeline === p && d.id === id),
      `delivery ${p}/${id}`,
    ).then((d) => ({ ...d, attempts: d.attempts ?? [] }));
  listDeadLetters: Api['listDeadLetters'] = (p, { state = 'open' } = {}) =>
    Promise.resolve(
      groupDeadLetters(
        this.fixtures.deadLetters.filter((d) => d.pipeline === p && (state === 'all' || d.state === state)),
      ),
    );
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
  listInsights: Api['listInsights'] = ({ pipeline, state } = {}) =>
    Promise.resolve(
      this.fixtures.insights.filter(
        (i) => (pipeline === undefined || i.pipeline === pipeline) && (state === undefined || i.state === state),
      ),
    );
  getMetrics: Api['getMetrics'] = () => Promise.resolve([]);
  listMetricNames: Api['listMetricNames'] = () => Promise.resolve([]);
  listNodes: Api['listNodes'] = () => Promise.resolve(this.fixtures.nodes);
  artifactUrl: Api['artifactUrl'] = (location) => this.artifact(location)?.content ?? 'about:blank';
  readArtifact: Api['readArtifact'] = (location) =>
    this.found(this.artifact(location), `artifact ${location.name}`).then((a) => a.content);

  private artifact({ pipeline, name, ...owner }: ArtifactLocation): FakeArtifact | undefined {
    return this.fixtures.artifacts.find(
      (a) => a.pipeline === pipeline && a.name === name && JSON.stringify(a.owner) === JSON.stringify(owner),
    );
  }

  // The open dead letters a selection names, as the server reads exactly one of ids, fingerprint and all.
  private selected(pipeline: string, selection: DeadLetterSelection): FakeDeadLetter[] {
    return this.fixtures.deadLetters.filter(
      (d) =>
        d.pipeline === pipeline &&
        d.state === 'open' &&
        (selection.all === true ||
          (selection.ids?.includes(d.id) ?? false) ||
          (selection.fingerprint !== undefined && d.fingerprint === selection.fingerprint)),
    );
  }

  private settle(pipeline: string, selection: DeadLetterSelection, state: 'replayed' | 'discarded') {
    const letters = this.selected(pipeline, selection);
    letters.forEach((d) => (d.state = state));
    const p = this.pipeline(pipeline);
    if (p) p.openDeadLetters = Math.max(0, p.openDeadLetters - letters.length);
    return { count: letters.length };
  }

  private setPaused({ pipeline, step }: { pipeline: string; step?: string }, paused: boolean) {
    const p = this.pipeline(pipeline);
    if (!p) return;
    if (step === undefined) p.state = paused ? 'paused' : 'active';
    for (const s of p.steps) if (step === undefined || s.name === step) s.paused = paused;
  }

  triggerRun: Api['triggerRun'] = (...args) => this.record('triggerRun', args, this.newRun(args[0], 'trigger'));
  cancelRun: Api['cancelRun'] = (...args) => this.record('cancelRun', args, undefined);
  pause: Api['pause'] = (...args) => {
    this.setPaused(args[0], true);
    return this.record('pause', args, undefined);
  };
  resume: Api['resume'] = (...args) => {
    this.setPaused(args[0], false);
    return this.record('resume', args, undefined);
  };
  replay: Api['replay'] = (...args) => this.record('replay', args, this.newRun(args[0], 'replay'));
  replayDeadLetters: Api['replayDeadLetters'] = (...args) =>
    this.record('replayDeadLetters', args, this.settle(args[0], args[1], 'replayed'));
  discardDeadLetters: Api['discardDeadLetters'] = (...args) =>
    this.record('discardDeadLetters', args, this.settle(args[0], args[1], 'discarded'));
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

// Dead letters grouped as the server does: by fingerprint, largest group first, then by step and fingerprint.
function groupDeadLetters(letters: FakeDeadLetter[]): DeadLetterGroup[] {
  const groups = new Map<string, DeadLetterGroup>();
  for (const {
    step,
    kind,
    fingerprint,
    pattern,
    id,
    runId,
    originDelivery,
    stream,
    state,
    error,
    createdAt,
  } of letters) {
    const letter = { id, runId, originDelivery, stream, state, error, createdAt };
    const group = groups.get(fingerprint) ?? {
      fingerprint,
      step,
      kind,
      error: pattern,
      count: 0,
      firstSeen: letter.createdAt,
      lastSeen: letter.createdAt,
      deadLetters: [],
    };
    group.count += 1;
    if (letter.createdAt < group.firstSeen) group.firstSeen = letter.createdAt;
    if (letter.createdAt > group.lastSeen) group.lastSeen = letter.createdAt;
    group.deadLetters.push(letter);
    groups.set(fingerprint, group);
  }
  return [...groups.values()].sort(
    (a, b) => b.count - a.count || a.step.localeCompare(b.step) || a.fingerprint.localeCompare(b.fingerprint),
  );
}

// The live pipeline snapshot ?demo streams, when the fixtures have one.
export function demoSnapshot(pipeline: string): PipelineSnapshot | undefined {
  return demoFixtures.snapshots.find((s) => s.pipeline === pipeline);
}
