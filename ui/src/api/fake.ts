import { ApiError, type Api } from './api';
import type { InboxEntry, NodeInfo, Pipeline, Run } from './schema';

// Fixture data for tests and ?demo: the generic real estate example of the canvas.
export interface Fixtures {
  pipelines: Pipeline[];
  runs: Run[];
  inbox: InboxEntry[];
  nodes: NodeInfo[];
}

const today = '2026-10-15';

export const demoFixtures: Fixtures = {
  pipelines: [
    {
      name: 'listings-daily',
      description: 'Paris listings → scored, reviewed, PDF',
      state: 'active',
      revision: 'r14',
      stepCount: 9,
      lastRun: { id: 'run_0600', state: 'running', startedAt: `${today}T06:00:00Z` },
      itemsToday: 1632,
      costTodayUsd: 9.84,
      openDeadLetters: 3,
    },
    {
      name: 'photo-tagging',
      description: 'Listing photos → room labels',
      state: 'active',
      revision: 'r2',
      stepCount: 4,
      lastRun: { id: 'run_0530', state: 'succeeded', startedAt: `${today}T05:30:00Z`, endedAt: `${today}T05:52:00Z` },
      itemsToday: 1108,
      costTodayUsd: 1.92,
      openDeadLetters: 0,
    },
    {
      name: 'rentals-weekly',
      description: 'Rental index, Mondays',
      state: 'paused',
      revision: 'r6',
      stepCount: 5,
      itemsToday: 0,
      costTodayUsd: 0,
      openDeadLetters: 0,
    },
  ],
  runs: [
    {
      id: 'run_0600',
      pipeline: 'listings-daily',
      revision: 'r14',
      state: 'running',
      trigger: 'schedule',
      startedAt: `${today}T06:00:00Z`,
      itemsIn: 1632,
      itemsOut: 402,
      deadLetters: 3,
      costUsd: 9.84,
    },
  ],
  inbox: [
    {
      id: 'rv_1',
      itemId: 'it_7f3a9c21',
      pipeline: 'listings-daily',
      step: 'analyst-review',
      inbox: 'analysts',
      title: 'T3 · 68 m² · Paris 19e',
      createdAt: `${today}T06:14:00Z`,
      sample: false,
      editable: ['/score'],
      reasons: [{ code: 'overpriced', label: 'Overpriced for the area' }],
    },
  ],
  nodes: [
    {
      id: 'mill-01',
      role: 'engine',
      state: 'active',
      version: '0.0.0',
      labels: {},
      slots: 8,
      leases: 2,
    },
  ],
};

export interface FakeCall {
  method: string;
  args: unknown[];
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

  listPipelines: Api['listPipelines'] = () => Promise.resolve(this.fixtures.pipelines);
  getPipeline: Api['getPipeline'] = (name) => {
    const found = this.fixtures.pipelines.find((p) => p.name === name);
    return found ? Promise.resolve(found) : this.notFound(`pipeline ${name}`);
  };
  listRevisions: Api['listRevisions'] = () => Promise.resolve([]);
  listRuns: Api['listRuns'] = (pipeline) => {
    const items = this.fixtures.runs.filter((r) => r.pipeline === pipeline);
    return Promise.resolve({ items, total: items.length });
  };
  getRun: Api['getRun'] = (id) => {
    const found = this.fixtures.runs.find((r) => r.id === id);
    return found ? Promise.resolve(found) : this.notFound(`run ${id}`);
  };
  getRunReport: Api['getRunReport'] = async (id) => ({ run: await this.getRun(id), steps: [] });
  listStreams: Api['listStreams'] = () => Promise.resolve([]);
  listItems: Api['listItems'] = () => Promise.resolve({ items: [], total: 0 });
  getItem: Api['getItem'] = (id) => this.notFound(`item ${id}`);
  getLineage: Api['getLineage'] = (id) => Promise.resolve({ item: id, parents: [], children: [] });
  listDeliveries: Api['listDeliveries'] = () => Promise.resolve([]);
  listDeadLetters: Api['listDeadLetters'] = () => Promise.resolve([]);
  getMetrics: Api['getMetrics'] = () => Promise.resolve([]);
  listInbox: Api['listInbox'] = () => Promise.resolve(this.fixtures.inbox.filter((e) => !e.sample));
  listSamples: Api['listSamples'] = () => Promise.resolve(this.fixtures.inbox.filter((e) => e.sample));
  listInsights: Api['listInsights'] = () => Promise.resolve([]);
  listLessons: Api['listLessons'] = () => Promise.resolve([]);
  listNodes: Api['listNodes'] = () => Promise.resolve(this.fixtures.nodes);

  triggerRun: Api['triggerRun'] = (pipeline) => {
    const run: Run = {
      id: `run_${this.calls.length + 1}`,
      pipeline,
      revision: 'r1',
      state: 'running',
      trigger: 'manual',
      startedAt: new Date(0).toISOString(),
      itemsIn: 0,
      itemsOut: 0,
      deadLetters: 0,
      costUsd: 0,
    };
    return this.record('triggerRun', [pipeline], run);
  };
  cancelRun: Api['cancelRun'] = (...args) => this.record('cancelRun', args, undefined);
  pause: Api['pause'] = (...args) => this.record('pause', args, undefined);
  resume: Api['resume'] = (...args) => this.record('resume', args, undefined);
  rollback: Api['rollback'] = (...args) => this.record('rollback', args, undefined);
  replay: Api['replay'] = (...args) => this.record('replay', args, undefined);
  replayDeadLetters: Api['replayDeadLetters'] = (...args) =>
    this.record('replayDeadLetters', args, { count: args[1].itemIds?.length ?? 0 });
  discardDeadLetters: Api['discardDeadLetters'] = (...args) =>
    this.record('discardDeadLetters', args, { count: args[1].itemIds?.length ?? 0 });
  decideReview: Api['decideReview'] = (...args) => this.record('decideReview', args, undefined);
  decideSample: Api['decideSample'] = (...args) => this.record('decideSample', args, undefined);
  acknowledgeInsight: Api['acknowledgeInsight'] = (...args) => this.record('acknowledgeInsight', args, undefined);
  muteInsight: Api['muteInsight'] = (...args) => this.record('muteInsight', args, undefined);
}
